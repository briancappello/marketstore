package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// WriteFunc writes a ColumnSeriesMap locally. Mirrors the replayer's write seam
// (executor.WriteCSM / GetDefaultWriter().WriteCSM).
type WriteFunc func(csm io.ColumnSeriesMap, isVariableLength bool) error

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
	ctx context.Context, api MasterAPI, write WriteFunc, wm *Watermarks,
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

	if err := write(csm, isVariable); err != nil {
		return 0, false, fmt.Errorf("write %s: %w", tbk, err)
	}
	if err := wm.Set(tbk, newest); err != nil {
		return rows, false, err
	}
	return rows, newest > prev, nil
}
