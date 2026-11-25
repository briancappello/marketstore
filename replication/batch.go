package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// BatchOptions bounds a replay batch. Zero values select the defaults.
type BatchOptions struct {
	MaxEntries  int
	MaxBytes    int64
	MaxCommands int
	// Wait is the longest a batch stays open after its first entry, so a
	// lone entry is applied within Wait plus one flush.
	Wait time.Duration
}

const (
	DefaultReplayBatchMax      = 1000
	DefaultReplayBatchBytes    = 64 << 20
	DefaultReplayBatchCommands = 250000
	DefaultReplayBatchWait     = 50 * time.Millisecond
)

func (o BatchOptions) withDefaults() BatchOptions {
	if o.MaxEntries <= 0 {
		o.MaxEntries = DefaultReplayBatchMax
	}
	if o.MaxBytes <= 0 {
		o.MaxBytes = DefaultReplayBatchBytes
	}
	if o.MaxCommands <= 0 {
		o.MaxCommands = DefaultReplayBatchCommands
	}
	if o.Wait <= 0 {
		o.Wait = DefaultReplayBatchWait
	}
	return o
}

type batchItem struct {
	seq        uint64
	wallclock  int64
	tg         []byte
	cursorOnly bool // SetCursor: seq is the new cursor, possibly 0
}

// BatchApplier is the replica's Applier. It applies received transaction
// groups in batches: every write of a batch, plus the batch's last sequence as
// the new cursor, is committed as one WAL transaction group with one fsync.
// After a crash WAL replay restores a batch and its cursor together or not at
// all, so the replica resumes exactly after the last batch it committed.
//
// Apply only queues: a single goroutine (Run) builds and commits batches, in
// sequence order.
type BatchApplier struct {
	catDir  *catalog.Directory
	wal     *executor.WALFileType
	rootDir string
	opts    BatchOptions
	in      chan batchItem

	mu      sync.Mutex
	cursor  uint64 // durable
	ok      bool
	failed  error
	stats   BatchStats
	onBatch func(BatchStats)
	sizes   prometheus.Histogram // nil until RegisterMetrics
}

// BatchStats describes the batches applied so far.
type BatchStats struct {
	Batches     int
	Entries     int
	LastEntries int
	LastApplied time.Time
	// NewestWallclock is the master log time (unix ns) of the newest entry
	// applied so far; 0 when the master does not send it.
	NewestWallclock int64
}

func NewBatchApplier(catDir *catalog.Directory, wal *executor.WALFileType, rootDir string, opts BatchOptions,
) (*BatchApplier, error) {
	opts = opts.withDefaults()
	cursor, ok, err := ReadCursor(catDir)
	if err != nil {
		return nil, err
	}
	return &BatchApplier{
		catDir: catDir, wal: wal, rootDir: rootDir, opts: opts,
		in:     make(chan batchItem, opts.MaxEntries),
		cursor: cursor, ok: ok,
	}, nil
}

// Cursor is the sequence of the last committed batch.
func (a *BatchApplier) Cursor() (uint64, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cursor, a.ok, a.failed
}

// Stats returns batch statistics.
func (a *BatchApplier) Stats() BatchStats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stats
}

// Apply queues one transaction group. It returns the error that stopped the
// applier, if any: a batch that fails to apply is not retried.
func (a *BatchApplier) Apply(ctx context.Context, e Received) error {
	return a.enqueue(ctx, batchItem{seq: e.Seq, wallclock: e.Wallclock, tg: e.TG})
}

// SetCursor queues a cursor-only write, ordered after everything queued so far.
func (a *BatchApplier) SetCursor(ctx context.Context, seq uint64) error {
	return a.enqueue(ctx, batchItem{seq: seq, cursorOnly: true})
}

func (a *BatchApplier) enqueue(ctx context.Context, it batchItem) error {
	if err := a.err(); err != nil {
		return err
	}
	select {
	case a.in <- it:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *BatchApplier) err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.failed
}

// Run builds and commits batches until ctx is done or a batch fails. On
// return everything queued before ctx was done has been committed.
func (a *BatchApplier) Run(ctx context.Context) error {
	var (
		batch    []batchItem
		bytes    int64
		commands int
		timer    *time.Timer
		deadline <-chan time.Time
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if timer != nil {
			timer.Stop()
			timer, deadline = nil, nil
		}
		err := a.commit(batch)
		batch, bytes, commands = batch[:0], 0, 0
		if err != nil {
			a.mu.Lock()
			a.failed = err
			a.mu.Unlock()
			log.Error("replication: %v", err)
		}
		return err
	}
	for {
		select {
		case it := <-a.in:
			n := len(it.tg)
			cmds := tgCommandCount(it.tg)
			// Keep a batch within its bounds, but always take at least one
			// entry: a single oversized transaction group is its own batch.
			if len(batch) > 0 && (int64(n)+bytes > a.opts.MaxBytes || cmds+commands > a.opts.MaxCommands) {
				if err := flush(); err != nil {
					return err
				}
			}
			batch = append(batch, it)
			bytes += int64(n)
			commands += cmds
			if len(batch) == 1 {
				timer = time.NewTimer(a.opts.Wait)
				deadline = timer.C
			}
			if len(batch) >= a.opts.MaxEntries || bytes >= a.opts.MaxBytes || commands >= a.opts.MaxCommands {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-deadline:
			timer, deadline = nil, nil
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			// Commit what was already queued: the receiver counted it as
			// delivered.
		drain:
			for {
				select {
				case it := <-a.in:
					batch = append(batch, it)
				default:
					break drain
				}
			}
			return flush()
		}
	}
}

// tgCommandCount reads WTCount from a serialized transaction group's header.
func tgCommandCount(tg []byte) int {
	if len(tg) < 16 {
		return 0
	}
	return int(io.ToInt64(tg[8:16]))
}

// commit applies one batch: every write set of every transaction group in
// order, then the cursor, as one WAL transaction group.
func (a *BatchApplier) commit(batch []batchItem) error {
	bw, err := executor.NewBatchWriter(a.catDir, a.wal)
	if err != nil {
		return err
	}
	var last uint64
	var newest int64
	haveLast := false
	entries := 0
	for _, it := range batch {
		if it.wallclock > newest {
			newest = it.wallclock
		}
		// seq 0 on a data entry means an unsequenced master: no cursor.
		if it.cursorOnly || it.seq != 0 {
			last, haveLast = it.seq, true
		}
		if it.cursorOnly {
			continue
		}
		entries++
		_, wtSets := executor.ParseTGData(it.tg, a.rootDir)
		for i := range wtSets {
			csm, err := WTSetToCSM(&wtSets[i])
			if err != nil {
				return fmt.Errorf("replay batch: seq %d: convert write set: %w", it.seq, err)
			}
			// Each set's own record type: a transaction group can mix fixed
			// and variable writes.
			if err := bw.QueueCSM(csm, wtSets[i].RecordType == io.VARIABLE); err != nil {
				return fmt.Errorf("replay batch: seq %d: %w", it.seq, err)
			}
		}
	}
	if haveLast {
		if err := bw.QueueCSM(CursorCSM(last), false); err != nil {
			return fmt.Errorf("replay batch: cursor %d: %w", last, err)
		}
	}
	if err := bw.Commit(); err != nil {
		return fmt.Errorf("replay batch ending at %d: commit: %w", last, err)
	}

	a.mu.Lock()
	if haveLast {
		a.cursor, a.ok = last, true
	}
	a.stats.Batches++
	a.stats.Entries += entries
	a.stats.LastEntries = entries
	a.stats.LastApplied = time.Now()
	if newest > a.stats.NewestWallclock {
		a.stats.NewestWallclock = newest
	}
	stats, onBatch, sizes := a.stats, a.onBatch, a.sizes
	a.mu.Unlock()
	if sizes != nil {
		sizes.Observe(float64(entries))
	}
	if onBatch != nil {
		onBatch(stats)
	}
	return nil
}

// OnBatch registers fn to be called after each committed batch.
func (a *BatchApplier) OnBatch(fn func(BatchStats)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.onBatch = fn
}

// RegisterMetrics exposes replay progress on reg: entries and batches applied
// (rate() of the first is the drain rate), batch sizes, the replica's cursor
// and the age of the newest applied entry, measured against the master's log
// time. That age returns to near zero when the replica has caught up, and
// grows while the master writes nothing.
func (a *BatchApplier) RegisterMetrics(reg prometheus.Registerer, now func() time.Time) error {
	if now == nil {
		now = time.Now
	}
	opts := func(name, help string) prometheus.Opts {
		return prometheus.Opts{Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help}
	}
	stat := func(f func(BatchStats) float64) func() float64 {
		return func() float64 { return f(a.Stats()) }
	}
	sizes := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name:    "replication_replay_batch_entries",
		Help:    "Transaction groups per replay batch.",
		Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 5000},
	})
	collectors := []prometheus.Collector{
		sizes,
		prometheus.NewCounterFunc(prometheus.CounterOpts(opts("replication_replay_applied_total",
			"Transaction groups applied by the replica.")),
			stat(func(s BatchStats) float64 { return float64(s.Entries) })),
		prometheus.NewCounterFunc(prometheus.CounterOpts(opts("replication_replay_batches_total",
			"Replay batches committed by the replica.")),
			stat(func(s BatchStats) float64 { return float64(s.Batches) })),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts("replication_replay_newest_applied_age_seconds",
			"Age of the newest applied transaction group, by the master's log time (0: unknown).")),
			stat(func(s BatchStats) float64 {
				if s.NewestWallclock == 0 {
					return 0
				}
				return now().Sub(time.Unix(0, s.NewestWallclock)).Seconds()
			})),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts("replication_replica_cursor",
			"Sequence of the last transaction group the replica committed.")),
			func() float64 {
				c, _, _ := a.Cursor()
				return float64(c)
			}),
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	a.mu.Lock()
	a.sizes = sizes
	a.mu.Unlock()
	return nil
}
