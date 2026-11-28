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

// rangeAPI serves fixed rows and returns only those inside the queried range.
type rangeAPI struct {
	key    string
	epochs []int64
	gotEnd int64
}

func (a *rangeAPI) ListTBKs(context.Context) ([]string, error) { return []string{a.key}, nil }
func (a *rangeAPI) QueryRange(_ context.Context, _ string, s, e int64) (io.ColumnSeriesMap, error) {
	a.gotEnd = e
	var epochs []int64
	var factors []float64
	for _, ep := range a.epochs {
		if ep >= s && ep <= e {
			epochs = append(epochs, ep)
			factors = append(factors, 0.1)
		}
	}
	csm := io.NewColumnSeriesMap()
	if len(epochs) == 0 {
		return csm, nil
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("PriceFactor", factors)
	csm.AddColumnSeries(*io.NewTimeBucketKey(a.key), cs)
	return csm, nil
}

// A split announced for a future date (RC's 1:10 reverse split, effective
// 2026-12-08, written on the master 2026-11-24) must reach a replica that
// missed the write, on the next pass, not after the effective day ends.
func TestBackfillCopiesFutureDatedSplits(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	now := time.Date(2026, 11, 28, 6, 0, 0, 0, ny).Unix()
	effective := time.Date(2026, 12, 8, 0, 0, 0, 0, ny).Unix()
	const key = "RC/1D/SPLITS"

	for _, lookback := range []time.Duration{0, 24 * time.Hour} { // shallow and deep pass
		api := &rangeAPI{key: key, epochs: []int64{effective}}
		wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
		require.NoError(t, err)
		noBucket := func(context.Context, string, int64, int64) (io.ColumnSeriesMap, error) {
			return nil, errors.New("no files")
		}
		var wrote io.ColumnSeriesMap
		write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

		rows, _, err := backfill.BackfillBucket(context.Background(), api, noBucket, write, wm, key, now, lookback, false)
		require.NoError(t, err)
		assert.Equal(t, 1, rows, "lookback %v", lookback)
		if assert.NotNil(t, wrote) {
			assert.Equal(t, []int64{effective}, wrote[*io.NewTimeBucketKey(key)].GetEpoch())
		}
		assert.LessOrEqual(t, wm.Get(key), now, "the watermark never moves past now")

		// Once the replica holds it, the next pass writes nothing.
		have := func(ctx context.Context, tbk string, s, e int64) (io.ColumnSeriesMap, error) {
			return api.QueryRange(ctx, tbk, s, e)
		}
		wrote = nil
		rows, _, err = backfill.BackfillBucket(context.Background(), api, have, write, wm, key, now, lookback, false)
		require.NoError(t, err)
		assert.Equal(t, 0, rows)
		assert.Nil(t, wrote)
	}
}

// The change ledger's heal must not drop future-dated events either.
func TestHealRangeKeepsFutureDatedSplits(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	now := time.Date(2026, 11, 28, 6, 0, 0, 0, ny).Unix()
	effective := time.Date(2026, 12, 8, 0, 0, 0, 0, ny).Unix()
	api := &rangeAPI{key: "RC/1D/SPLITS", epochs: []int64{effective}}
	write := func(io.ColumnSeriesMap, bool) error { return nil }

	rows, err := backfill.HealRange(context.Background(), api, nil, write, "RC/1D/SPLITS", effective, effective, now)
	require.NoError(t, err)
	assert.Equal(t, 1, rows)

	// Bars keep the closed-bar rule: an open 1D bar is not healed.
	bars := &rangeAPI{key: "RC/1D/OHLCV", epochs: []int64{now}}
	rows, err = backfill.HealRange(context.Background(), bars, nil, write, "RC/1D/OHLCV", now, now, now)
	require.NoError(t, err)
	assert.Equal(t, 0, rows)
}
