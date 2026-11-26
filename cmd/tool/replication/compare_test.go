package replication_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	tool "github.com/alpacahq/marketstore/v4/cmd/tool/replication"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// fakeAPI serves fixed buckets: key -> epoch -> close.
type fakeAPI map[string]map[int64]float32

func (f fakeAPI) ListTBKs(context.Context) ([]string, error) {
	var out []string
	for k := range f {
		out = append(out, k)
	}
	return out, nil
}

func (f fakeAPI) QueryRange(_ context.Context, tbk string, start, end int64) (io.ColumnSeriesMap, error) {
	var epochs []int64
	var closes []float32
	for e := start; e <= end; e += 60 {
		if c, ok := f[tbk][e]; ok {
			epochs = append(epochs, e)
			closes = append(closes, c)
		}
	}
	if len(epochs) == 0 {
		return nil, errors.New("no results returned from query")
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Close", closes)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(tbk), cs)
	return csm, nil
}

func TestCompareFindsMissingDifferingAndExtraRows(t *testing.T) {
	const e = 1795600020
	master := fakeAPI{
		"A/1Min/OHLCV": {e: 1, e + 60: 2, e + 120: 3},
		"B/1Min/OHLCV": {e: 1},
		"C/1Sec/OHLCV": {e: 1}, // not a compared suffix
	}
	spec := tool.CompareSpec{Suffixes: []string{"1Min/OHLCV"}, Start: time.Unix(e-600, 0), End: time.Unix(e+600, 0), Examples: 10}

	same := fakeAPI{"A/1Min/OHLCV": {e: 1, e + 60: 2, e + 120: 3}, "B/1Min/OHLCV": {e: 1}, "X/1Min/OHLCV": {e: 9}}
	res, err := tool.Compare(context.Background(), master, same, spec)
	require.NoError(t, err)
	require.True(t, res.OK())
	require.Equal(t, 2, res.Buckets)
	require.Equal(t, 4, res.Identical)
	require.Equal(t, 1, res.OnlyOnReplica)

	off := fakeAPI{"A/1Min/OHLCV": {e: 1, e + 60: 2.5, e + 180: 4}}
	res, err = tool.Compare(context.Background(), master, off, spec)
	require.NoError(t, err)
	require.False(t, res.OK())
	require.Equal(t, 1, res.Identical)
	require.Equal(t, 2, res.Missing, "A at e+120, and all of B")
	require.Equal(t, 1, res.Differing)
	var out bytes.Buffer
	res.Write(&out)
	require.Contains(t, out.String(), "DIFFERENT")
}

func TestTradingDaysStartSkipsHolidaysAndWeekends(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	// Thanksgiving 2026: the last 5 trading days are 11/19, 11/20, 11/23, 11/24, 11/25.
	got := tool.TradingDaysStart(time.Date(2026, 11, 26, 9, 0, 0, 0, ny), 5)
	require.Equal(t, time.Date(2026, 11, 19, 0, 0, 0, 0, ny), got)
	// On a trading day, today counts.
	got = tool.TradingDaysStart(time.Date(2026, 11, 30, 21, 0, 0, 0, ny), 2)
	require.Equal(t, time.Date(2026, 11, 27, 0, 0, 0, 0, ny), got)
}
