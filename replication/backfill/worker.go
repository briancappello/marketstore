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
// recent epochs that were missed while disconnected; corrections older than
// the lookback require a deep resync (delete the watermark file). A no-data
// result is a no-op. Writes are idempotent (WriteCSM overwrites by epoch), so
// overlap and re-running are always safe.
func BackfillBucket(
	ctx context.Context, api MasterAPI, write WriteFunc, wm *Watermarks,
	tbk string, now int64, lookback time.Duration, isVariable bool,
) error {
	start := wm.Get(tbk) + 1 - int64(lookback.Seconds())
	if start < 1 {
		start = 1
	}
	if start > now {
		return nil
	}
	csm, err := api.QueryRange(ctx, tbk, start, now)
	if err != nil {
		return err
	}
	if len(csm) == 0 {
		return nil
	}

	newest := int64(0)
	for _, cs := range csm {
		epochs := cs.GetEpoch()
		if len(epochs) == 0 {
			continue
		}
		if last := epochs[len(epochs)-1]; last > newest {
			newest = last
		}
	}
	if newest == 0 {
		return nil // rows present but no Epoch column — treat as no-op
	}

	if err := write(csm, isVariable); err != nil {
		return fmt.Errorf("write %s: %w", tbk, err)
	}
	return wm.Set(tbk, newest)
}
