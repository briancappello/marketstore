package backfill_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// diskSim is a tiny stand-in for the local store: it answers reads with what
// has been written so far, keyed by epoch, so a pass can be run twice and the
// second pass sees the first pass's effect.
type diskSim struct {
	tbk  *io.TimeBucketKey
	rows map[int64]float32
}

func newDiskSim(tbk string) *diskSim {
	return &diskSim{tbk: io.NewTimeBucketKey(tbk), rows: map[int64]float32{}}
}

func (d *diskSim) read(context.Context, string, int64, int64) (io.ColumnSeriesMap, error) {
	out := io.NewColumnSeriesMap()
	if len(d.rows) == 0 {
		return out, nil
	}
	epochs := make([]int64, 0, len(d.rows))
	for e := range d.rows {
		epochs = append(epochs, e)
	}
	// Deliberately unsorted-ish order is fine: the diff matches on epoch.
	closes := make([]float32, len(epochs))
	for i, e := range epochs {
		closes[i] = d.rows[e]
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Close", closes)
	out.AddColumnSeries(*d.tbk, cs)
	return out, nil
}

// write applies a CSM the way WriteCSM does: overwrite by epoch.
func (d *diskSim) write(csm io.ColumnSeriesMap, _ bool) error {
	for _, cs := range csm {
		epochs := cs.GetEpoch()
		closes, _ := cs.GetColumn("Close").([]float32)
		for i, e := range epochs {
			d.rows[e] = closes[i]
		}
	}
	return nil
}

func masterCSM(tbk string, epochs []int64, closes []float32) io.ColumnSeriesMap {
	k := io.NewTimeBucketKey(tbk)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Close", closes)
	m := io.NewColumnSeriesMap()
	m.AddColumnSeries(*k, cs)
	return m
}

// TestReconcileConverges_SecondPassWritesNothing is THE test for write
// amplification. Everything else in this area is a detail of how the diff is
// computed; this is the property that actually matters.
//
// Against an unchanged master, a pass that has already run must find nothing
// to do. Before the row-level diff this failed by construction: any difference
// anywhere in the window -- including the still-forming bar -- put the bucket
// on the "correction" path, which rewrote every row in the window, every pass,
// forever.
func TestReconcileConverges_SecondPassWritesNothing(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	base := int64(1_700_000_000)
	epochs := []int64{base, base + 60, base + 120, base + 180}
	closes := []float32{1, 2, 3, 4}
	now := base + 180 + 60 // every bar above has closed

	disk := newDiskSim(tbk)
	api := &fakeAPI{ret: masterCSM(tbk, epochs, closes)}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)

	writes := 0
	write := func(m io.ColumnSeriesMap, isVar bool) error {
		writes += backfill.CSMRows(m)
		return disk.write(m, isVar)
	}

	// First pass: nothing on disk, so everything is written.
	rows, _, err := backfill.BackfillBucket(
		context.Background(), api, disk.read, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)
	assert.Equal(t, 4, rows, "an empty bucket must be fully populated")
	assert.Equal(t, 4, writes)

	// Second pass, identical master, nothing changed anywhere.
	writes = 0
	rows, _, err = backfill.BackfillBucket(
		context.Background(), api, disk.read, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)

	assert.Zero(t, writes,
		"a second pass against an unchanged master must write NOTHING; "+
			"anything else is write amplification by definition")
	assert.Zero(t, rows)
}

// TestReconcileConverges_AfterCorrectionOnlyThatRowIsRewritten checks that a
// real master-side revision costs exactly one row, and that the pass after it
// is silent again.
func TestReconcileConverges_AfterCorrectionOnlyThatRowIsRewritten(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	base := int64(1_700_000_000)
	epochs := []int64{base, base + 60, base + 120, base + 180}
	now := base + 180 + 60

	disk := newDiskSim(tbk)
	api := &fakeAPI{ret: masterCSM(tbk, epochs, []float32{1, 2, 3, 4})}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)

	writes := 0
	write := func(m io.ColumnSeriesMap, isVar bool) error {
		writes += backfill.CSMRows(m)
		return disk.write(m, isVar)
	}

	_, _, err = backfill.BackfillBucket(
		context.Background(), api, disk.read, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)

	// The master revises one historical bar.
	api.ret = masterCSM(tbk, epochs, []float32{1, 2, 99, 4})

	writes = 0
	rows, _, err := backfill.BackfillBucket(
		context.Background(), api, disk.read, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)
	assert.Equal(t, 1, writes, "one revised bar must cost one row, not the whole window")
	assert.Equal(t, 1, rows)
	assert.InDelta(t, 99, float64(disk.rows[base+120]), 0.001, "the correction must actually land")

	// And the pass after that is silent again.
	writes = 0
	_, _, err = backfill.BackfillBucket(
		context.Background(), api, disk.read, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)
	assert.Zero(t, writes, "the correction must converge, not repeat")
}

// TestReconcileConverges_WithNaNRows is the convergence property in the
// presence of absent bar values. Without NaN-aware equality these rows are
// "revised" on every pass and rewritten forever.
func TestReconcileConverges_WithNaNRows(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	base := int64(1_700_000_000)
	nan := float32(math.NaN())
	epochs := []int64{base, base + 60, base + 120}
	now := base + 120 + 60

	disk := newDiskSim(tbk)
	api := &fakeAPI{ret: masterCSM(tbk, epochs, []float32{1, nan, 3})}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)

	writes := 0
	write := func(m io.ColumnSeriesMap, isVar bool) error {
		writes += backfill.CSMRows(m)
		return disk.write(m, isVar)
	}

	_, _, err = backfill.BackfillBucket(
		context.Background(), api, disk.read, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)
	require.Equal(t, 3, writes)

	writes = 0
	_, _, err = backfill.BackfillBucket(
		context.Background(), api, disk.read, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)
	assert.Zero(t, writes,
		"a bar with no value must not be rewritten on every pass forever")
}

// TestReconcileConverges_SchemaDivergenceNeverChurns is the behavioural guard
// against the worst possible outcome of a row-level diff.
//
// If a bucket's local schema disagrees with the master's, no write can
// reconcile it: WriteCSM either rejects the column set or coerces the incoming
// values straight back to the local type. A diff that reports those rows as
// "differing" therefore rewrites the whole bucket on every pass, forever --
// strictly worse than the whole-window rewriting this work set out to remove.
//
// The bucket must be left alone and reported instead.
func TestReconcileConverges_SchemaDivergenceNeverChurns(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	k := io.NewTimeBucketKey(tbk)
	base := int64(1_700_000_000)
	epochs := []int64{base, base + 60}
	now := base + 60 + 60

	// Master carries Close as float64; local holds it as float32.
	master := io.NewColumnSeriesMap()
	mcs := io.NewColumnSeries()
	mcs.AddColumn("Epoch", epochs)
	mcs.AddColumn("Close", []float64{1, 2})
	master.AddColumnSeries(*k, mcs)

	local := io.NewColumnSeriesMap()
	lcs := io.NewColumnSeries()
	lcs.AddColumn("Epoch", epochs)
	lcs.AddColumn("Close", []float32{1, 2})
	local.AddColumnSeries(*k, lcs)

	api := &fakeAPI{ret: master}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)

	readLocal := func(context.Context, string, int64, int64) (io.ColumnSeriesMap, error) {
		return local, nil
	}
	writes := 0
	write := func(m io.ColumnSeriesMap, _ bool) error {
		writes += backfill.CSMRows(m)
		return nil
	}

	// Several consecutive passes must all be silent.
	for i := 0; i < 3; i++ {
		_, _, err = backfill.BackfillBucket(
			context.Background(), api, readLocal, write, wm, tbk, now, time.Hour, false)
		require.Nil(t, err)
	}

	assert.Zero(t, writes,
		"a schema disagreement cannot be repaired by writing, so it must never "+
			"be answered with a write -- doing so rewrites the bucket on every pass forever")
}

// TestReconcileConverges_DoesNotRewriteRowsTheLiveStreamAlreadyWrote covers the
// blind-write problem that the watermark split used to create.
//
// Rows above the watermark were assumed new and written without comparison.
// The live replication stream writes these same buckets independently, so they
// are often already correct on disk.
func TestReconcileConverges_DoesNotRewriteRowsTheLiveStreamAlreadyWrote(t *testing.T) {
	t.Parallel()

	const tbk = "AAPL/1Min/OHLCV"
	base := int64(1_700_000_000)
	epochs := []int64{base, base + 60, base + 120}
	closes := []float32{1, 2, 3}
	now := base + 120 + 60

	disk := newDiskSim(tbk)
	// The live stream already delivered every one of these rows.
	for i, e := range epochs {
		disk.rows[e] = closes[i]
	}

	api := &fakeAPI{ret: masterCSM(tbk, epochs, closes)}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	// Watermark still at zero: backfill believes it has written nothing.
	require.Zero(t, wm.Get(tbk))

	writes := 0
	write := func(m io.ColumnSeriesMap, isVar bool) error {
		writes += backfill.CSMRows(m)
		return disk.write(m, isVar)
	}

	_, _, err = backfill.BackfillBucket(
		context.Background(), api, disk.read, write, wm, tbk, now, time.Hour, false)
	require.Nil(t, err)

	assert.Zero(t, writes,
		"rows the live stream already wrote correctly must not be rewritten just "+
			"because the watermark has not caught up")
	assert.Equal(t, base+120, wm.Get(tbk), "the watermark must still advance")
}
