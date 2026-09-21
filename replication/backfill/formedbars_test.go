package backfill_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// barsCSM builds a one-bucket ColumnSeriesMap with the given epochs.
func barsCSM(tbk string, epochs []int64) io.ColumnSeriesMap {
	k := io.NewTimeBucketKey(tbk)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	closes := make([]float32, len(epochs))
	for i := range closes {
		closes[i] = float32(i) + 1
	}
	cs.AddColumn("Close", closes)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*k, cs)
	return csm
}

func writtenEpochs(m io.ColumnSeriesMap) []int64 {
	for _, cs := range m {
		return cs.GetEpoch()
	}
	return nil
}

// TestBackfillBucketPersistsOnlyClosedBars covers the decision that a replica
// stores only completed bars.
//
// A bar stamped E covers [E, E+timeframe), so until now reaches E+timeframe the
// master is still revising it. Persisting it copied a half-formed value: the
// deep pass then reported that bar as a "correction" on every pass, each one
// triggering a full-window rewrite, and the watermark could never move past it.
// Accepting up to one period of lag removes all three symptoms.
func TestBackfillBucketPersistsOnlyClosedBars(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	// 1Min bars at t, t+60, t+120. "now" sits inside the third bar's period, so
	// only the first two have closed.
	base := int64(1_700_000_000)
	now := base + 120 + 30 // 30s into the bar stamped base+120

	api := &fakeAPI{ret: barsCSM(tbk, []int64{base, base + 60, base + 120})}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)

	var wrote io.ColumnSeriesMap
	write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

	rows, advanced, err := backfill.BackfillBucket(
		context.Background(), api, nil, write, wm, tbk, now, 0, false)
	require.Nil(t, err)

	assert.Equal(t, []int64{base, base + 60}, writtenEpochs(wrote),
		"the bar still being formed must not be persisted")
	assert.Equal(t, 2, rows)
	assert.True(t, advanced)
	assert.Equal(t, base+60, wm.Get(tbk),
		"the watermark must stop at the last CLOSED bar, so the open one is re-fetched once it closes")
}

// TestBackfillBucketBarClosesExactlyAtNow pins the boundary: a bar is complete
// the instant now reaches E+timeframe, so it must be included rather than held
// back for another whole period.
func TestBackfillBucketBarClosesExactlyAtNow(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	base := int64(1_700_000_000)
	now := base + 60 // the bar stamped base has just closed

	api := &fakeAPI{ret: barsCSM(tbk, []int64{base})}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)

	var wrote io.ColumnSeriesMap
	write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

	rows, _, err := backfill.BackfillBucket(
		context.Background(), api, nil, write, wm, tbk, now, 0, false)
	require.Nil(t, err)
	assert.Equal(t, []int64{base}, writtenEpochs(wrote))
	assert.Equal(t, 1, rows)
}

// TestBackfillBucketAllBarsOpenIsNoOp guards the degenerate case: when nothing
// has closed yet there is nothing to persist, and it must be a clean no-op
// rather than a write of zero rows that still moves the watermark.
func TestBackfillBucketAllBarsOpenIsNoOp(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1D/OHLCV"
	base := int64(1_700_000_000)

	api := &fakeAPI{ret: barsCSM(tbk, []int64{base})}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)

	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	rows, advanced, err := backfill.BackfillBucket(
		context.Background(), api, nil, write, wm, tbk, base+3600, 0, false)
	require.Nil(t, err)

	assert.False(t, called, "an open bar alone is nothing to write")
	assert.Zero(t, rows)
	assert.False(t, advanced)
	assert.Zero(t, wm.Get(tbk), "the watermark must not advance past an unformed bar")
}

// TestBackfillBucketUnparsableTimeframeStillWrites makes sure a key whose
// timeframe cannot be read falls back to writing everything. Silently dropping
// every row because a key failed to parse would be indistinguishable from "the
// master has no data" -- a silent data-loss mode.
func TestBackfillBucketUnparsableTimeframeStillWrites(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/NotATimeframe/OHLCV"
	base := int64(1_700_000_000)

	api := &fakeAPI{ret: barsCSM(tbk, []int64{base})}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)

	var wrote io.ColumnSeriesMap
	write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

	rows, _, err := backfill.BackfillBucket(
		context.Background(), api, nil, write, wm, tbk, base+1, 0, false)
	require.Nil(t, err)
	assert.Equal(t, []int64{base}, writtenEpochs(wrote),
		"an unreadable timeframe must not silently discard data")
	assert.Equal(t, 1, rows)
}

// TestBackfillBucketDeepPassIgnoresStaleOpenBarOnDisk is the regression test
// for a trap created by the closed-bars rule itself.
//
// The master side is reduced to closed bars. If the local side is not reduced
// the same way, an open bar that an earlier build already persisted leaves
// local one row longer than master. CSMDiff then reports a row-count
// "correction" and the whole lookback window is rewritten -- on every pass,
// until that bar finally closes. For a daily bucket that is a full day of
// recurring full-window rewrites: precisely the write amplification the rule
// was introduced to remove, reintroduced by the rule.
//
// Both sides must be compared on the same closed-bar basis.
func TestBackfillBucketDeepPassIgnoresStaleOpenBarOnDisk(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	base := int64(1_700_000_000)
	// Bars at base and base+60 are closed; base+120 is still forming.
	now := base + 120 + 30

	master := barsCSM(tbk, []int64{base, base + 60, base + 120})
	// Local already holds the open bar, persisted by an earlier build.
	local := barsCSM(tbk, []int64{base, base + 60, base + 120})

	api := &fakeAPI{ret: master}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	require.Nil(t, wm.Set(tbk, base+120))

	readLocal := func(_ context.Context, _ string, _, _ int64) (io.ColumnSeriesMap, error) {
		return local, nil
	}

	wrote := false
	write := func(io.ColumnSeriesMap, bool) error { wrote = true; return nil }

	_, _, err = backfill.BackfillBucket(
		context.Background(), api, readLocal, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)

	assert.False(t, wrote,
		"closed history matches, so a stale open bar on disk must not trigger a full-window rewrite")
}

// TestBackfillBucketDeepPassAlsoWithholdsOpenBar confirms the rule is not
// bypassed by a deep pass. A deep pass exists to heal master-side corrections
// to CLOSED history; the open bar is not a correction, it is simply unfinished,
// and treating it as one is what produced the recurring full-window rewrites.
func TestBackfillBucketDeepPassAlsoWithholdsOpenBar(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	base := int64(1_700_000_000)
	now := base + 120 + 30

	api := &fakeAPI{ret: barsCSM(tbk, []int64{base, base + 60, base + 120})}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	require.Nil(t, wm.Set(tbk, base+60))

	var wrote io.ColumnSeriesMap
	write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

	_, _, err = backfill.BackfillBucket(
		context.Background(), api, nil, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)

	assert.NotContains(t, writtenEpochs(wrote), base+120,
		"a deep pass must not persist the still-forming bar either")
}
