package executor_test

import (
	"fmt"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// blockingTrigger holds its OS thread in a real blocking syscall, the way a
// trigger's file reads and WAL fsync waits do. time.Sleep would not: it parks
// the goroutine without pinning a thread.
type blockingTrigger struct {
	d           time.Duration
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	fired       atomic.Int64
	wg          sync.WaitGroup
}

func (b *blockingTrigger) Fire(_ string, _ []trigger.Record) {
	defer b.wg.Done()
	n := b.inFlight.Add(1)
	for {
		m := b.maxInFlight.Load()
		if n <= m || b.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	ts := syscall.NsecToTimespec(b.d.Nanoseconds())
	_ = syscall.Nanosleep(&ts, nil)
	b.inFlight.Add(-1)
	b.fired.Add(1)
}

func fakeRecord(t testing.TB) []byte {
	t.Helper()
	buf, ok := io.SwapSliceData([]int64{0, 5}, byte(0)).([]byte)
	require.True(t, ok)
	return wal.OffsetIndexBuffer(buf).IndexAndPayload()
}

// TestTriggerDispatcherBoundsThreads reproduces the production incident: a
// flush that touches thousands of buckets used to start one goroutine per
// bucket per trigger, and every one blocked in a syscall got its own OS
// thread (4,418 threads observed; Go aborts the process at 10,000).
//
// Not parallel: it measures the process-wide thread-creation count.
func TestTriggerDispatcherBoundsThreads(t *testing.T) {
	const (
		workers = 8
		keys    = 2000
	)
	trig := &blockingTrigger{d: 2 * time.Millisecond}
	trig.wg.Add(keys)
	tpd := executor.StartNewTriggerPluginDispatcherWithWorkers(
		[]*trigger.Matcher{trigger.NewMatcher(trig, "*/1Sec/OHLCV")}, workers)

	rec := fakeRecord(t)
	threadsBefore := pprof.Lookup("threadcreate").Count()

	for i := 0; i < keys; i++ {
		tpd.AppendRecord(fmt.Sprintf("S%04d/1Sec/OHLCV/2026.bin", i), rec)
	}
	tpd.DispatchRecords()
	trig.wg.Wait()

	created := pprof.Lookup("threadcreate").Count() - threadsBefore
	t.Logf("fired=%d maxInFlight=%d threadsCreated=%d", trig.fired.Load(), trig.maxInFlight.Load(), created)

	assert.EqualValues(t, keys, trig.fired.Load())
	assert.LessOrEqual(t, trig.maxInFlight.Load(), int64(workers), "concurrent fires must be bounded by the pool")
	assert.Less(t, created, 4*workers, "OS threads created must be bounded, not proportional to buckets")
}

// orderTrigger records the order in which batches for each key are seen.
type orderTrigger struct {
	mu   sync.Mutex
	seen map[string][]int64
	wg   sync.WaitGroup
}

func (o *orderTrigger) Fire(key string, recs []trigger.Record) {
	defer o.wg.Done()
	o.mu.Lock()
	o.seen[key] = append(o.seen[key], recs[0].Index())
	o.mu.Unlock()
}

// TestTriggerDispatcherPreservesPerKeyOrder: successive flushes of the same
// bucket must reach a trigger in flush order. The 1Sec -> 1Min cascade
// rewrites the current bar every second; with a goroutine per fire an older
// rewrite could land after a newer one.
func TestTriggerDispatcherPreservesPerKeyOrder(t *testing.T) {
	t.Parallel()
	const (
		keys   = 50
		rounds = 200
	)
	trig := &orderTrigger{seen: map[string][]int64{}}
	trig.wg.Add(keys * rounds)
	tpd := executor.StartNewTriggerPluginDispatcherWithWorkers(
		[]*trigger.Matcher{trigger.NewMatcher(trig, "*/1Min/OHLCV")}, 4)

	for r := 0; r < rounds; r++ {
		for k := 0; k < keys; k++ {
			buf, _ := io.Serialize(nil, int64(r))
			buf = append(buf, make([]byte, 8)...)
			tpd.AppendRecord(fmt.Sprintf("K%02d/1Min/OHLCV/2026.bin", k), buf)
		}
		tpd.DispatchRecords()
	}
	trig.wg.Wait()

	for key, order := range trig.seen {
		require.Len(t, order, rounds, key)
		for i := range order {
			require.EqualValues(t, i, order[i], "key %s fired out of order: %v", key, order)
		}
	}
}

// reentrantTrigger dispatches more records from inside Fire, as a trigger
// that calls WriteCSM does (WriteCSM -> flush -> DispatchRecords). With a
// bounded queue that blocks when full this deadlocks; dispatch must never
// block on busy workers.
type reentrantTrigger struct {
	tpd   *executor.TriggerPluginDispatcher
	depth atomic.Int64
	wg    sync.WaitGroup
	rec   []byte
}

func (r *reentrantTrigger) Fire(key string, _ []trigger.Record) {
	defer r.wg.Done()
	if r.depth.Add(1) > 3000 {
		return
	}
	r.wg.Add(1)
	r.tpd.AppendRecord(key, r.rec)
	r.tpd.DispatchRecords()
}

func TestTriggerDispatcherReentrantDispatchDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	trig := &reentrantTrigger{rec: fakeRecord(t)}
	trig.tpd = executor.StartNewTriggerPluginDispatcherWithWorkers(
		[]*trigger.Matcher{trigger.NewMatcher(trig, "*/1D/OHLCV")}, 1)

	trig.wg.Add(1)
	trig.tpd.AppendRecord("X/1D/OHLCV/2026.bin", trig.rec)
	trig.tpd.DispatchRecords()

	done := make(chan struct{})
	go func() { trig.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("dispatcher deadlocked on re-entrant dispatch from a trigger")
	}
}

// panicTrigger panics on every fire.
type panicTrigger struct{ wg sync.WaitGroup }

func (p *panicTrigger) Fire(_ string, _ []trigger.Record) {
	defer p.wg.Done()
	panic("boom")
}

// A panicking trigger must not kill its worker: later fires still run.
func TestTriggerDispatcherSurvivesPanics(t *testing.T) {
	t.Parallel()
	trig := &panicTrigger{}
	const n = 20
	trig.wg.Add(n)
	tpd := executor.StartNewTriggerPluginDispatcherWithWorkers(
		[]*trigger.Matcher{trigger.NewMatcher(trig, "*/1Min/OHLCV")}, 1)
	rec := fakeRecord(t)
	for i := 0; i < n; i++ {
		tpd.AppendRecord("A/1Min/OHLCV/2026.bin", rec)
		tpd.DispatchRecords()
	}

	done := make(chan struct{})
	go func() { trig.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("worker died after a trigger panic")
	}
}
