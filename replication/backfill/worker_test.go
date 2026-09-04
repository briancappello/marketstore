package backfill_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type fakeAPI struct {
	gotStart, gotEnd int64
	ret              io.ColumnSeriesMap
}

func (f *fakeAPI) ListTBKs(_ context.Context) ([]string, error) { return nil, nil }
func (f *fakeAPI) QueryRange(_ context.Context, _ string, s, e int64) (io.ColumnSeriesMap, error) {
	f.gotStart, f.gotEnd = s, e
	return f.ret, nil
}

func TestBackfillBucketQueriesFromWatermarkAndAdvances(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1Min/OHLCV")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{100, 200, 300})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	api := &fakeAPI{ret: csm}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	require.Nil(t, wm.Set("AAPL/1Min/OHLCV", 100))

	var wrote io.ColumnSeriesMap
	write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

	rows, advanced, err := backfill.BackfillBucket(context.Background(), api, nil, write, wm, "AAPL/1Min/OHLCV", 999, 0, false)
	require.Nil(t, err)
	assert.Equal(t, 3, rows)
	assert.True(t, advanced)

	// Queried from just after the watermark to now.
	assert.Equal(t, int64(101), api.gotStart)
	assert.Equal(t, int64(999), api.gotEnd)
	// Wrote the returned data and advanced the watermark to the newest epoch.
	assert.NotNil(t, wrote)
	assert.Equal(t, int64(300), wm.Get("AAPL/1Min/OHLCV"))
}

func TestBackfillBucketLookbackWidensStart(t *testing.T) {
	api := &fakeAPI{ret: io.NewColumnSeriesMap()} // empty; we only check the range
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Min/OHLCV", 100)

	_, _, err := backfill.BackfillBucket(context.Background(), api, nil, nil, wm, "AAPL/1Min/OHLCV", 999, 60*time.Second, false)
	require.Nil(t, err)

	// Start reaches back behind the watermark by the lookback: 100+1-60 = 41.
	assert.Equal(t, int64(41), api.gotStart)
	assert.Equal(t, int64(999), api.gotEnd)

	// Lookback never produces a start below 1.
	_ = wm.Set("X/1Min/OHLCV", 10)
	_, _, err = backfill.BackfillBucket(context.Background(), api, nil, nil, wm, "X/1Min/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	assert.Equal(t, int64(1), api.gotStart)
}

func TestBackfillBucketNoDataLeavesWatermark(t *testing.T) {
	api := &fakeAPI{ret: io.NewColumnSeriesMap()} // empty
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("X/1Min/OHLCV", 500)
	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	rows, advanced, err := backfill.BackfillBucket(context.Background(), api, nil, write, wm, "X/1Min/OHLCV", 999, 0, false)
	require.Nil(t, err)
	assert.False(t, called, "must not write when there is no data")
	assert.Equal(t, int64(500), wm.Get("X/1Min/OHLCV"))
	assert.Equal(t, 0, rows)
	assert.False(t, advanced)
}

// The pathological case behind replica write amplification: the master returns
// rows whose newest epoch is at or below our watermark, so they are written but
// the watermark cannot advance. The next pass then requests the same range and
// rewrites exactly the same rows -- forever, at zero informational gain.
//
// BackfillBucket must report this (rows > 0 && !advanced) so Reconcile can
// count it, because it is invisible in both the watermark file and the on-disk
// data size.
func TestBackfillBucketReportsWriteThatCannotAdvanceWatermark(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1D/OHLCV")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{100}) // older than the watermark below
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	api := &fakeAPI{ret: csm}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1D/OHLCV", 500)

	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	// A deep pass (lookback > 0) is asking for corrections on purpose, so data
	// at or below the watermark is exactly what it wants: it must be written.
	rows, advanced, err := backfill.BackfillBucket(context.Background(), api, nil, write, wm, "AAPL/1D/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	assert.True(t, called, "a deep pass must write corrections below the watermark")
	assert.Equal(t, 1, rows)
	assert.False(t, advanced, "watermark cannot advance past data it already covers")
	assert.Equal(t, int64(500), wm.Get("AAPL/1D/OHLCV"))
}

// A shallow pass asks for (watermark, now]. If the master still hands back only
// rows at or below the watermark -- it returns the bar CONTAINING start, so a
// bucket at a bar boundary yields the bar we already hold -- then writing them
// achieves nothing and will repeat identically on every future pass.
//
// Measured on the p1 replica: ~17.6k of 35.2k buckets did this every 5 minutes,
// each rewrite also re-firing the watchlist trigger. Skip the write entirely.
func TestBackfillBucketShallowPassSkipsDataItAlreadyCovers(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1D/OHLCV")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{500}) // exactly the watermark
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	api := &fakeAPI{ret: csm}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1D/OHLCV", 500)

	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	rows, advanced, err := backfill.BackfillBucket(context.Background(), api, nil, write, wm, "AAPL/1D/OHLCV", 999, 0, false)
	require.Nil(t, err)
	assert.False(t, called, "shallow pass must not rewrite data at or below the watermark")
	assert.Equal(t, 0, rows)
	assert.False(t, advanced)
	assert.Equal(t, int64(500), wm.Get("AAPL/1D/OHLCV"))
}

// Guard the mixed case: if the master returns some rows we hold and some we do
// not, the write is needed and must still happen in full.
func TestBackfillBucketShallowPassWritesWhenAnyRowIsNew(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1Min/OHLCV")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{500, 560}) // 500 held, 560 new
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	api := &fakeAPI{ret: csm}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Min/OHLCV", 500)

	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	rows, advanced, err := backfill.BackfillBucket(context.Background(), api, nil, write, wm, "AAPL/1Min/OHLCV", 999, 0, false)
	require.Nil(t, err)
	assert.True(t, called)
	assert.Equal(t, 2, rows)
	assert.True(t, advanced)
	assert.Equal(t, int64(560), wm.Get("AAPL/1Min/OHLCV"))
}

// The deep pass exists to catch master-side corrections, but corrections are
// rare: nearly every bar it re-pulls is already byte-identical on disk. Writing
// them back cost 5.6 GB per pass across 11,615 1Sec buckets, because a bar sits
// in index-addressed space and a 32 KiB block carries 1,365 one-second slots.
// Compare first, and write only when something actually differs.
func TestBackfillBucketDeepPassSkipsWriteWhenLocalMatches(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1Sec/OHLCV")
	mk := func() io.ColumnSeriesMap {
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", []int64{100, 200, 300})
		cs.AddColumn("Close", []float32{1, 2, 3})
		csm := io.NewColumnSeriesMap()
		csm.AddColumnSeries(*tbk, cs)
		return csm
	}
	api := &fakeAPI{ret: mk()}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Sec/OHLCV", 300)

	readLocal := func(context.Context, string, int64, int64) (io.ColumnSeriesMap, error) { return mk(), nil }
	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	_, _, err := backfill.BackfillBucket(context.Background(), api, readLocal, write, wm,
		"AAPL/1Sec/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	assert.False(t, called, "unchanged deep-pass data must not be rewritten")
}

func TestBackfillBucketDeepPassWritesWhenLocalDiffers(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1Sec/OHLCV")
	master := io.NewColumnSeries()
	master.AddColumn("Epoch", []int64{100, 200})
	master.AddColumn("Close", []float32{1, 99}) // corrected bar
	mcsm := io.NewColumnSeriesMap()
	mcsm.AddColumnSeries(*tbk, master)

	stale := io.NewColumnSeries()
	stale.AddColumn("Epoch", []int64{100, 200})
	stale.AddColumn("Close", []float32{1, 2})
	lcsm := io.NewColumnSeriesMap()
	lcsm.AddColumnSeries(*tbk, stale)

	api := &fakeAPI{ret: mcsm}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Sec/OHLCV", 200)

	readLocal := func(context.Context, string, int64, int64) (io.ColumnSeriesMap, error) { return lcsm, nil }
	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	_, _, err := backfill.BackfillBucket(context.Background(), api, readLocal, write, wm,
		"AAPL/1Sec/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	assert.True(t, called, "a real correction must still be written")
}

// If the local read fails we must not silently drop a correction: fall back to
// writing, which is the pre-existing behaviour.
func TestBackfillBucketDeepPassWritesWhenLocalReadFails(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1Sec/OHLCV")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{100})
	cs.AddColumn("Close", []float32{1})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	api := &fakeAPI{ret: csm}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Sec/OHLCV", 100)

	readLocal := func(context.Context, string, int64, int64) (io.ColumnSeriesMap, error) {
		return nil, errors.New("no files returned from query parse")
	}
	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	_, _, err := backfill.BackfillBucket(context.Background(), api, readLocal, write, wm,
		"AAPL/1Sec/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	assert.True(t, called, "a failed local read must fall back to writing")
}

// The common deep-pass case is not "everything changed" -- it is "the history
// matches and the master has a few rows we have not received yet". Measured on
// p1: master 422 rows vs local 417. Rewriting all 422 to add 5 is what kept the
// deep pass at ~6 GB. Write only the tail.
func TestBackfillBucketDeepPassWritesOnlyNewTailWhenHistoryMatches(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1Sec/OHLCV")
	mkcs := func(ep []int64, cl []float32) io.ColumnSeriesMap {
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", ep)
		cs.AddColumn("Close", cl)
		m := io.NewColumnSeriesMap()
		m.AddColumnSeries(*tbk, cs)
		return m
	}
	// Watermark 300: history is 100..300, and the master has two newer bars.
	master := mkcs([]int64{100, 200, 300, 400, 500}, []float32{1, 2, 3, 4, 5})
	local := mkcs([]int64{100, 200, 300}, []float32{1, 2, 3})

	api := &fakeAPI{ret: master}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Sec/OHLCV", 300)

	readLocal := func(context.Context, string, int64, int64) (io.ColumnSeriesMap, error) { return local, nil }
	var wrote io.ColumnSeriesMap
	write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

	_, _, err := backfill.BackfillBucket(context.Background(), api, readLocal, write, wm,
		"AAPL/1Sec/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	require.NotNil(t, wrote, "the new bars must still be written")
	assert.Equal(t, []int64{400, 500}, wrote[*tbk].GetEpoch(),
		"only bars newer than the watermark should be written when history matches")
}

// If the history does NOT match, the master has corrected something below the
// watermark and the whole window must be written.
func TestBackfillBucketDeepPassWritesFullWindowOnCorrection(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1Sec/OHLCV")
	mkcs := func(ep []int64, cl []float32) io.ColumnSeriesMap {
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", ep)
		cs.AddColumn("Close", cl)
		m := io.NewColumnSeriesMap()
		m.AddColumnSeries(*tbk, cs)
		return m
	}
	master := mkcs([]int64{100, 200, 300, 400}, []float32{1, 99, 3, 4}) // 200 corrected
	local := mkcs([]int64{100, 200, 300}, []float32{1, 2, 3})

	api := &fakeAPI{ret: master}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Sec/OHLCV", 300)

	readLocal := func(context.Context, string, int64, int64) (io.ColumnSeriesMap, error) { return local, nil }
	var wrote io.ColumnSeriesMap
	write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

	_, _, err := backfill.BackfillBucket(context.Background(), api, readLocal, write, wm,
		"AAPL/1Sec/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	require.NotNil(t, wrote)
	assert.Equal(t, []int64{100, 200, 300, 400}, wrote[*tbk].GetEpoch(),
		"a correction below the watermark must rewrite the whole window")
}
