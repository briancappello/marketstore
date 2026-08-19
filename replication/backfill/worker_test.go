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

	err = backfill.BackfillBucket(context.Background(), api, write, wm, "AAPL/1Min/OHLCV", 999, 0, false)
	require.Nil(t, err)

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

	err := backfill.BackfillBucket(context.Background(), api, nil, wm, "AAPL/1Min/OHLCV", 999, 60*time.Second, false)
	require.Nil(t, err)

	// Start reaches back behind the watermark by the lookback: 100+1-60 = 41.
	assert.Equal(t, int64(41), api.gotStart)
	assert.Equal(t, int64(999), api.gotEnd)

	// Lookback never produces a start below 1.
	_ = wm.Set("X/1Min/OHLCV", 10)
	err = backfill.BackfillBucket(context.Background(), api, nil, wm, "X/1Min/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	assert.Equal(t, int64(1), api.gotStart)
}

func TestBackfillBucketNoDataLeavesWatermark(t *testing.T) {
	api := &fakeAPI{ret: io.NewColumnSeriesMap()} // empty
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("X/1Min/OHLCV", 500)
	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	err := backfill.BackfillBucket(context.Background(), api, write, wm, "X/1Min/OHLCV", 999, 0, false)
	require.Nil(t, err)
	assert.False(t, called, "must not write when there is no data")
	assert.Equal(t, int64(500), wm.Get("X/1Min/OHLCV"))
}
