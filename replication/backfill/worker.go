package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// ReadFunc reads a local range, mirroring MasterAPI.QueryRange so a deep pass
// can compare what the master has against what is already on disk. May be nil,
// in which case no comparison is made and every deep pass writes.
type ReadFunc func(ctx context.Context, tbk string, start, end int64) (io.ColumnSeriesMap, error)

// WriteFunc writes a ColumnSeriesMap locally. Mirrors the replayer's write seam
// (executor.WriteCSM / GetDefaultWriter().WriteCSM).
type WriteFunc func(csm io.ColumnSeriesMap, isVariableLength bool) error

// lastClosedBarEpoch returns the newest epoch whose bar has finished for the
// timeframe encoded in tbk, i.e. the newest epoch safe to persist at time now.
//
// It reports ok=false when the timeframe cannot be determined, in which case
// the caller must not filter: silently dropping every row because a key failed
// to parse would look exactly like "the master has no data".
func lastClosedBarEpoch(tbk string, now int64) (cutoff int64, ok bool) {
	key := io.NewTimeBucketKey(tbk)
	if key == nil {
		return 0, false
	}
	tf, err := key.GetTimeFrame()
	if err != nil || tf == nil || tf.Duration <= 0 {
		return 0, false
	}
	return now - int64(tf.Duration.Seconds()), true
}

// BackfillBucket queries [watermark+1−lookback, now] for one bucket, writes
// what it gets, and advances the watermark to the newest epoch written. The
// lookback re-pulls a trailing window to heal master-side corrections to
// epochs at or below the watermark; corrections older than the lookback
// require a deep resync (delete the watermark file). A no-data result is a
// no-op. Writes are idempotent (WriteCSM overwrites by epoch), so overlap and
// re-running are always safe for CORRECTNESS -- but they are not free, so the
// caller controls how often the lookback is applied (see Driver.lookbackFor).
//
// It reports the number of rows written and whether the watermark advanced.
// rows > 0 with advanced == false means the master returned data we already
// cover: it was rewritten to no effect and will be rewritten again next pass,
// which is invisible in both the watermark file and the on-disk data size.
func BackfillBucket(
	ctx context.Context, api MasterAPI, readLocal ReadFunc, write WriteFunc, wm *Watermarks,
	tbk string, now int64, lookback time.Duration, isVariable bool,
) (rows int, advanced bool, err error) {
	prev := wm.Get(tbk)
	start := prev + 1 - int64(lookback.Seconds())
	if start < 1 {
		start = 1
	}
	if start > now {
		return 0, false, nil
	}
	csm, err := api.QueryRange(ctx, tbk, start, now)
	if err != nil {
		return 0, false, err
	}
	if len(csm) == 0 {
		return 0, false, nil
	}

	// Persist only bars whose period has closed.
	//
	// A bar stamped E covers [E, E+timeframe), so it is still being written on
	// the master until now >= E+timeframe. Backfilling an open bar copies a
	// half-formed value that the master will keep revising, which showed up
	// three ways: the deep pass reported the newest bar as a "correction" on
	// every single pass (dominated by Close and Volume, the two columns that
	// keep moving while Open/High/Low have settled), each of those triggered a
	// full-window rewrite, and the watermark could never advance past it -- the
	// "rewritten without advancing the watermark" buckets.
	//
	// A replica may lag, so the fix is simply not to take the open bar. The
	// cost is bounded at one timeframe period of freshness; the gain is that a
	// reported correction now means the master actually revised history.
	cutoff, haveCutoff := lastClosedBarEpoch(tbk, now)
	closed := func(e int64) bool { return e <= cutoff }
	if haveCutoff {
		csm = FilterCSM(csm, closed)
	}

	newest := int64(0)
	for _, cs := range csm {
		epochs := cs.GetEpoch()
		if len(epochs) == 0 {
			continue
		}
		rows += len(epochs)
		if last := epochs[len(epochs)-1]; last > newest {
			newest = last
		}
	}
	if newest == 0 {
		return 0, false, nil // rows present but no Epoch column — treat as no-op
	}

	// Shallow pass: we asked for (watermark, now] but the master returned
	// nothing newer than the watermark -- typically the bar CONTAINING start,
	// because a range query resolves to bar boundaries. Writing it back changes
	// no data, cannot advance the watermark, and re-fires every trigger bound to
	// this bucket. Left in, it repeats identically on every future pass.
	//
	// A deep pass (lookback > 0) is deliberately asking for epochs at or below
	// the watermark to pick up master-side corrections, so it must not skip.
	if lookback == 0 && newest <= prev {
		return 0, false, nil
	}

	// Deep pass: re-pull the whole lookback window in case the master revised
	// something, then write only the epochs that actually disagree.
	//
	// The window is compared row by row rather than as a whole. Deciding at
	// window granularity meant one revised bar rewrote every bar in the window:
	// on a 1Sec bucket that is ~86400 records pushed through the WAL and back
	// out to index-addressed file space to correct one of them.
	//
	// There is deliberately no split at the watermark any more. Rows above it
	// used to be written blind on the assumption that they were new, but the
	// live replication stream writes these same buckets independently, so they
	// are frequently already correct on disk. Comparing everything and writing
	// the difference makes "new" and "revised" the same case.
	if lookback > 0 && readLocal != nil {
		local, rerr := readLocal(ctx, tbk, start, now)
		if rerr != nil {
			// The error itself was previously discarded, leaving only an
			// aggregate count with no bucket and no cause -- nothing an
			// operator could act on. A failed read falls through to the write
			// below, so this is not fatal, but it does mean the pass rewrote
			// the bucket blind.
			recordSkipReason("local-read-error", tbk)
			log.Warn("[replication-backfill] local read failed for %s, rewriting the whole window blind: %v",
				tbk, rerr)
		} else {
			// Compare like with like. The master side above was reduced to
			// closed bars, so the local side must be too, or an open bar that
			// an earlier build already persisted makes local one row longer
			// than master -- a difference that would be "repaired" on every
			// pass until that bar finally closes.
			if haveCutoff {
				local = FilterCSM(local, closed)
			}

			diffs := CSMRowDiff(csm, local)

			needed := make(map[io.TimeBucketKey]map[int64]struct{}, len(diffs))
			var toWrite int
			inSync := true
			for key, d := range diffs {
				recordRowDiff(d)
				recordSkipReason(diffReason(d), tbk)

				if d.SchemaIssue != "" {
					// Writing cannot reconcile a schema disagreement: the write
					// either fails the column check or is coerced straight back
					// to the local type. Report it and leave the bucket alone
					// rather than rewriting it on every pass forever.
					log.Warn("[replication-backfill] %s: cannot compare with master (%s); "+
						"leaving it untouched, this needs the bucket schema reconciled",
						tbk, d.SchemaIssue)
					continue
				}
				if len(d.LocalOnly) > 0 {
					log.Warn("[replication-backfill] %s: holds %d row(s) the master did not return "+
						"(e.g. epoch %d); a write cannot remove rows",
						tbk, len(d.LocalOnly), d.LocalOnly[0])
				}
				epochs := d.NeedsWrite()
				if len(epochs) == 0 {
					continue
				}
				inSync = false
				set := make(map[int64]struct{}, len(epochs))
				for _, e := range epochs {
					set[e] = struct{}{}
				}
				needed[key] = set
				toWrite += len(epochs)
			}

			if inSync {
				// Nothing to write. The watermark still advances so the next
				// pass starts from here.
				if err := wm.Set(tbk, newest); err != nil {
					return 0, false, err
				}
				return 0, newest > prev, nil
			}

			csm = FilterCSMByBucket(csm, func(key io.TimeBucketKey, e int64) bool {
				set, ok := needed[key]
				if !ok {
					return false
				}
				_, ok = set[e]
				return ok
			})
			rows = CSMRows(csm)
		}
	}

	if err := write(csm, isVariable); err != nil {
		return 0, false, fmt.Errorf("write %s: %w", tbk, err)
	}
	if err := wm.Set(tbk, newest); err != nil {
		return rows, false, err
	}
	return rows, newest > prev, nil
}
