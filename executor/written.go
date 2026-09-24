package executor

import (
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

/*
TriggerPluginDispatcher hands written records to trigger plugins.

Triggers run on a fixed pool of worker goroutines. Each (trigger, bucket) pair
always goes to the same worker, so:

  - concurrency, and therefore the number of OS threads parked in blocking
    file I/O, is bounded by the pool size. Previously every bucket in a flush
    started its own goroutine per matching trigger; at the market open or
    during a startup backfill that is tens of thousands of goroutines, each
    doing disk reads or waiting on a WAL flush, and the runtime creates an OS
    thread for every one blocked in a syscall. Production reached 4,418
    threads; Go aborts the process at 10,000.
  - fires for one bucket run in the order they were written. The 1Sec ->
    1Min cascade rewrites the current bar every second, and with a goroutine
    per fire an older version could be processed after a newer one.

Per-worker queues are unbounded, so dispatch never blocks. That is required,
not just convenient: a trigger that writes (ondiskagg calls WriteCSM) waits
for a WAL flush, and the flush dispatches the next round of records. If
dispatch blocked on a full queue, a worker would wait on a flush that waits
on that worker.
*/
type TriggerPluginDispatcher struct {
	c               chan writtenRecords
	done            chan struct{}
	mu              sync.Mutex
	m               map[string][]trigger.Record
	triggerMatchers []*trigger.Matcher
	triggerWg       *sync.WaitGroup
	workers         []*fireQueue
	workersWg       sync.WaitGroup
}

type writtenRecords struct {
	key     string
	records []trigger.Record
}

type fireJob struct {
	trig    trigger.Trigger
	key     string
	records []trigger.Record
}

// fireQueue is an unbounded FIFO drained by one worker goroutine.
type fireQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	jobs   []fireJob
	head   int
	closed bool
}

func newFireQueue() *fireQueue {
	q := &fireQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *fireQueue) push(j fireJob) {
	q.mu.Lock()
	q.jobs = append(q.jobs, j)
	q.mu.Unlock()
	q.cond.Signal()
}

// pop blocks until a job is available. ok is false once the queue is closed
// and drained.
func (q *fireQueue) pop() (j fireJob, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.head == len(q.jobs) && !q.closed {
		q.cond.Wait()
	}
	if q.head == len(q.jobs) {
		return fireJob{}, false
	}
	j = q.jobs[q.head]
	q.jobs[q.head] = fireJob{} // release records for GC
	q.head++
	if q.head == len(q.jobs) {
		// Drained: reuse the backing array from the start.
		q.jobs = q.jobs[:0]
		q.head = 0
	}
	return j, true
}

func (q *fireQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

// DefaultTriggerWorkers is the pool size used when none is configured.
// Triggers are I/O-bound (disk reads, waiting on WAL flushes), so the pool is
// a small multiple of GOMAXPROCS, clamped so tiny and huge hosts stay sane.
func DefaultTriggerWorkers() int {
	const (
		perProc = 4
		minimum = 8
		maximum = 256
	)
	n := perProc * runtime.GOMAXPROCS(0)
	if n < minimum {
		n = minimum
	}
	if n > maximum {
		n = maximum
	}
	return n
}

// StartNewTriggerPluginDispatcher starts a dispatcher with the default
// worker pool size.
func StartNewTriggerPluginDispatcher(triggerMatchers []*trigger.Matcher) *TriggerPluginDispatcher {
	return StartNewTriggerPluginDispatcherWithWorkers(triggerMatchers, 0)
}

// StartNewTriggerPluginDispatcherWithWorkers starts a dispatcher whose
// triggers run on at most workers goroutines. workers <= 0 selects
// DefaultTriggerWorkers.
func StartNewTriggerPluginDispatcherWithWorkers(triggerMatchers []*trigger.Matcher, workers int) *TriggerPluginDispatcher {
	if workers <= 0 {
		workers = DefaultTriggerWorkers()
	}
	tpd := &TriggerPluginDispatcher{
		c:               make(chan writtenRecords, WriteChannelCommandDepth),
		done:            make(chan struct{}),
		m:               nil,
		triggerMatchers: triggerMatchers,
		triggerWg:       &sync.WaitGroup{},
		workers:         make([]*fireQueue, workers),
	}
	for i := range tpd.workers {
		q := newFireQueue()
		tpd.workers[i] = q
		tpd.workersWg.Add(1)
		go tpd.work(q)
	}
	go tpd.run()

	return tpd
}

func (tpd *TriggerPluginDispatcher) run() {
	defer func() {
		for _, q := range tpd.workers {
			q.close()
		}
		tpd.workersWg.Wait()
		tpd.done <- struct{}{}
	}()

	for wr := range tpd.c {
		for i, tmatcher := range tpd.triggerMatchers {
			if tmatcher.Match(wr.key) {
				tpd.triggerWg.Add(1)
				q := tpd.workers[shard(i, wr.key, len(tpd.workers))]
				q.push(fireJob{trig: tmatcher.Trigger, key: wr.key, records: wr.records})
			}
		}
	}
}

func (tpd *TriggerPluginDispatcher) work(q *fireQueue) {
	defer tpd.workersWg.Done()
	for {
		j, ok := q.pop()
		if !ok {
			return
		}
		tpd.fire(j.trig, j.key, j.records)
	}
}

// shard maps a (matcher, key) pair to a worker with FNV-1a, so one bucket's
// fires for one trigger are always serialized on the same worker.
func shard(matcher int, key string, n int) int {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	h ^= uint32(matcher)
	h *= prime32
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return int(h % uint32(n))
}

// AppendRecord collects the record from the serialized buffer.
func (tpd *TriggerPluginDispatcher) AppendRecord(keyPath string, record []byte) {
	tpd.mu.Lock()
	defer tpd.mu.Unlock()

	if tpd.m == nil {
		tpd.m = make(map[string][]trigger.Record)
	}

	tpd.m[keyPath] = append(tpd.m[keyPath], record)
}

// DispatchRecords hands every collected bucket to the matching triggers.
// It never waits for triggers to run.
func (tpd *TriggerPluginDispatcher) DispatchRecords() {
	tpd.mu.Lock()
	m := tpd.m
	tpd.m = nil // for GC
	tpd.mu.Unlock()

	for key, records := range m {
		tpd.c <- writtenRecords{key: key, records: records}
	}
}

// fire runs one trigger and recovers from its panics, so a faulty trigger
// cannot take down its worker.
func (tpd *TriggerPluginDispatcher) fire(trig trigger.Trigger, key string, records []trigger.Record) {
	defer func() {
		tpd.triggerWg.Done()
		if r := recover(); r != nil {
			log.Error("recovering from %v\n%s", r, string(debug.Stack()))
		}
	}()
	trig.Fire(key, records)
}
