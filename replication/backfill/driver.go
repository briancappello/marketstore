package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Driver reconciles the whole bucket set against the master.
type Driver struct {
	api          MasterAPI
	readLocal    ReadFunc
	write        WriteFunc
	wm           *Watermarks
	parallelism  int
	lookback     time.Duration
	healInterval time.Duration
	isVariable   func(tbk string) bool

	mu        sync.Mutex
	lastHeal  int64 // unix seconds of the last deep pass; 0 = never
	forceHeal bool  // set by RequestDeepHeal, cleared by the next pass

	// healAnchor is the start time (unix seconds) of the last deep pass that
	// completed for every bucket; 0 = unknown. A deep pass reaches back to
	// healAnchor - lookback, not just watermark - lookback. See bucketLookback.
	// Persisted in healAnchorPath when TrackDeepHeals was called, so a restart
	// (or an outage) cannot skip the corrections made since that pass.
	healAnchor     int64
	healAnchorPath string
}

// healAnchorFile is the persisted form of Driver.healAnchor.
type healAnchorFile struct {
	LastDeepHealStart int64 `json:"last_deep_heal_start"`
}

// TrackDeepHeals loads the last complete deep pass's start time from path and
// persists it there after every complete deep pass. A missing file is not an
// error: the first deep pass then reaches back from the watermarks only.
func (d *Driver) TrackDeepHeals(path string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.healAnchorPath = path
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read deep heal state %s: %w", path, err)
	}
	var f healAnchorFile
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("parse deep heal state %s: %w", path, err)
	}
	d.healAnchor = f.LastDeepHealStart
	return nil
}

// completeDeepPass records that the deep pass started at start reached every
// bucket, and persists it when tracking.
func (d *Driver) completeDeepPass(start int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.healAnchor = start
	if d.healAnchorPath == "" {
		return nil
	}
	data, err := json.Marshal(healAnchorFile{LastDeepHealStart: start})
	if err != nil {
		return err
	}
	tmp := d.healAnchorPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write deep heal state: %w", err)
	}
	return os.Rename(tmp, d.healAnchorPath)
}

// bucketLookback returns the lookback for one bucket on a deep pass.
//
// A deep pass exists to pick up epochs the master revised after we already
// held them. With the lookback measured back from the watermark alone, a
// correction to an epoch that is already about one lookback old by the next
// deep pass is never seen: taichi's overnight fill of the previous day's first
// premarket minutes lands roughly 24h after their epochs, so a deep pass some
// hours later started past them, and they stayed missing on the replica.
//
// Measuring back from the start of the previous complete deep pass instead
// guarantees that any correction made with an epoch within one lookback of
// when it was made is caught by the next complete deep pass, however long
// the replica was down in between. It costs at most one heal interval of
// extra reading per deep pass (the watermark trails "now", so watermark -
// anchor is about the time since the last deep pass); writes are unchanged,
// since only rows that differ are written.
func bucketLookback(lookback time.Duration, watermark, anchor int64) time.Duration {
	if lookback <= 0 || anchor <= 0 || watermark+1 <= anchor {
		return lookback
	}
	return lookback + time.Duration(watermark+1-anchor)*time.Second
}

// defaultHealInterval is used when the configured interval is unset. It must
// never be 0: a zero interval makes every pass a deep pass, which is exactly
// the write-amplification bug this cadence exists to prevent.
const defaultHealInterval = 24 * time.Hour

// NewDriver builds the reconciler. readLocal may be nil, which disables the
// deep-pass unchanged-data check and makes every deep pass write.
func NewDriver(api MasterAPI, readLocal ReadFunc, write WriteFunc, wm *Watermarks, parallelism int,
	lookback, healInterval time.Duration, isVariable func(tbk string) bool,
) *Driver {
	if parallelism <= 0 {
		parallelism = 8
	}
	if healInterval <= 0 {
		healInterval = defaultHealInterval
	}
	return &Driver{
		api: api, readLocal: readLocal, write: write, wm: wm, parallelism: parallelism,
		lookback: lookback, healInterval: healInterval, isVariable: isVariable,
	}
}

// RequestDeepHeal marks the next reconcile as a deep pass, so it re-pulls the
// full lookback window. Called when the live replication stream reconnects: the
// master may have revised epochs at or below our watermark while we were away,
// and only a lookback pass reaches back far enough to see them.
func (d *Driver) RequestDeepHeal() {
	d.mu.Lock()
	d.forceHeal = true
	d.mu.Unlock()
}

// parseWriteBytes extracts the write_bytes counter (bytes this process caused to
// be sent to storage) from the contents of /proc/<pid>/io. Returns 0 if absent.
func parseWriteBytes(procIO []byte) int64 {
	for _, line := range strings.Split(string(procIO), "\n") {
		v, ok := strings.CutPrefix(line, "write_bytes:")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0
		}
		return n
	}
	return 0
}

// selfWriteBytes reports this process's cumulative bytes-to-storage, or 0 where
// unavailable (non-Linux). It is process-wide, so a pass measured with it also
// includes concurrent live-stream writes -- which is what we want, since the
// question is always "what is this server actually costing the disk right now".
func selfWriteBytes() int64 {
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0
	}
	return parseWriteBytes(b)
}

// lookbackFor decides whether this pass is a deep heal, and returns the lookback
// to apply. A pass is deep when it is the first since startup, when
// RequestDeepHeal was called (live-stream reconnect), or when healInterval has
// elapsed since the last deep pass.
//
// Every other pass returns 0, so BackfillBucket queries [watermark+1, now].
// That range already covers both new data and any gap left by a live-stream
// outage, because the watermark only advances on a successful backfill. The
// lookback's only unique job is reaching BELOW the watermark to pick up
// master-side corrections, and that does not need doing every few minutes.
func (d *Driver) lookbackFor(now int64) time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()

	due := d.lastHeal == 0 || now-d.lastHeal >= int64(d.healInterval.Seconds())
	if !d.forceHeal && !due {
		return 0
	}
	d.forceHeal = false
	d.lastHeal = now
	return d.lookback
}

// Reconcile enumerates every bucket on the master and backfills each from its
// watermark up to now, concurrently. Per-bucket errors are logged, not fatal:
// a transient failure is retried on the next reconcile.
//
// The lookback window is applied only on a deep pass; see lookbackFor.
func (d *Driver) Reconcile(ctx context.Context, now int64) error {
	tbks, err := d.api.ListTBKs(ctx)
	if err != nil {
		return fmt.Errorf("enumerate buckets: %w", err)
	}
	lookback := d.lookbackFor(now)
	pass := "shallow"
	if lookback > 0 {
		pass = "deep"
	}
	d.mu.Lock()
	anchor := d.healAnchor
	d.mu.Unlock()
	// Log both ends of the pass. Without the completion line there is no way to
	// tell a long-running pass from a finished one, which makes write-rate
	// regressions in here effectively undiagnosable from the outside.
	if lookback > 0 && anchor > 0 {
		log.Info("[replication-backfill] %s pass starting: %d buckets, lookback=%s before the last complete deep pass (%s)",
			pass, len(tbks), lookback, time.Unix(anchor, 0).UTC().Format(time.RFC3339))
	} else {
		log.Info("[replication-backfill] %s pass starting: %d buckets, lookback=%s", pass, len(tbks), lookback)
	}
	started := time.Now()

	// stuckRows counts rows written whose epochs we already covered, i.e. work
	// that will be repeated identically on every future pass. It is the tell for
	// replica write amplification and cannot be seen from the watermark file or
	// the data directory size.
	var wroteRows, stuckRows, stuckBuckets, failedBuckets int64
	startBytes := selfWriteBytes()
	defer func() {
		const mib = 1 << 20
		log.Info("[replication-backfill] %s pass finished in %s: %d rows written, "+
			"%d rows in %d buckets rewritten without advancing the watermark, "+
			"%d MiB written to disk (process-wide), compare=%v",
			pass, time.Since(started), wroteRows, stuckRows, stuckBuckets,
			(selfWriteBytes()-startBytes)/mib, DrainSkipReasons())

		// Row-level accounting. The reason tally above says how many BUCKETS
		// took each path; this says how many ROWS were touched, which is what
		// write amplification actually is. Without it the only evidence is the
		// disk-byte total, which cannot distinguish "the data changed" from
		// "we rewrote data that had not changed".
		if rs := DrainRowStats(); rs.Compared > 0 || rs.Schema > 0 {
			var pctWritten float64
			if rs.Compared > 0 {
				pctWritten = 100 * float64(rs.Written()) / float64(rs.Compared)
			}
			log.Info("[replication-backfill] %s pass rows: compared=%d identical=%d "+
				"missing=%d revised=%d written=%d (%.2f%%) local-only=%d schema-skipped=%d",
				pass, rs.Compared, rs.Identical, rs.Missing, rs.Revised,
				rs.Written(), pctWritten, rs.LocalOnly, rs.Schema)
		}

		// Name a few buckets behind each reason. The counts alone say how much
		// disagreed but not what, so there is nothing to investigate from them.
		// Logged separately from the summary so the summary stays greppable.
		for reason, tbks := range DrainSkipSamples() {
			log.Info("[replication-backfill] %s pass: reason %q e.g. %v", pass, reason, tbks)
		}
	}()

	wp := worker.NewWorkerPool(ctx, d.parallelism)
	for _, tbk := range tbks {
		tbk := tbk
		// Variable-length (tick) buckets are excluded from backfill: their
		// writes append rather than overwrite by epoch (executor/writer.go),
		// so re-pulling an overlapping range would duplicate rows unbounded.
		// Ticks rely on the best-effort live stream only. See the design doc
		// §9 (Non-goals) for the bootstrap-detection caveat.
		if d.isVariable(tbk) {
			log.Debug("[replication-backfill] skipping variable-length bucket %s (live-stream only)", tbk)
			continue
		}
		wp.Do(func() {
			lb := bucketLookback(lookback, d.wm.Get(tbk), anchor)
			rows, advanced, err := BackfillBucket(ctx, d.api, d.readLocal, d.write, d.wm, tbk, now, lb, false)
			if err != nil {
				atomic.AddInt64(&failedBuckets, 1)
				log.Warn("[replication-backfill] %s: %v", tbk, err)
				return
			}
			atomic.AddInt64(&wroteRows, int64(rows))
			if rows > 0 && !advanced {
				atomic.AddInt64(&stuckRows, int64(rows))
				atomic.AddInt64(&stuckBuckets, 1)
			}
		})
	}
	wp.CloseAndWait()

	// Persist every watermark this pass advanced, once. Watermarks.Set is
	// memory-only precisely so this is a single ~1 MB write instead of one per
	// advanced bucket (~10k of them, i.e. ~9.7 GB, per pass).
	if err := d.wm.Flush(); err != nil {
		return fmt.Errorf("persist watermarks: %w", err)
	}

	// Only a deep pass that reached every bucket moves the anchor. After a
	// failed bucket or a cancelled pass the next deep pass keeps measuring
	// from the older anchor, so the failed range is still covered.
	if lookback > 0 && ctx.Err() == nil {
		if failed := atomic.LoadInt64(&failedBuckets); failed > 0 {
			log.Warn("[replication-backfill] deep pass incomplete (%d buckets failed); "+
				"the next deep pass reaches back to the previous complete one", failed)
		} else if err := d.completeDeepPass(now); err != nil {
			return fmt.Errorf("persist deep heal state: %w", err)
		}
	}
	return nil
}

// Run performs an immediate reconcile (bootstrap), then reconciles once per
// interval until ctx is cancelled. This one loop guarantees eventual
// completeness regardless of live-stream drops or disconnects.
func (d *Driver) Run(ctx context.Context, interval time.Duration, nowFn func() int64) {
	if err := d.Reconcile(ctx, nowFn()); err != nil && ctx.Err() == nil {
		log.Warn("[replication-backfill] bootstrap reconcile: %v", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.Reconcile(ctx, nowFn()); err != nil && ctx.Err() == nil {
				log.Warn("[replication-backfill] periodic reconcile: %v", err)
			}
		}
	}
}
