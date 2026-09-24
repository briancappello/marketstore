package framework

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func requireSameRow(t *testing.T, want, got *io.ColumnSeries, msg string) {
	t.Helper()
	require.NotNil(t, want, msg)
	require.NotNil(t, got, msg)
	require.Equal(t, want.GetColumnNames(), got.GetColumnNames(), "%s: columns", msg)
	for _, name := range want.GetColumnNames() {
		require.Equal(t, want.GetColumn(name), got.GetColumn(name), "%s: column %s", msg, name)
	}
}

// TestLatestRowMatchesDiskRead checks the record decoder against the disk
// read-back it replaces, using the records the real WAL dispatches: single
// bars, multi-row batches (possibly out of time order), rewrites of the
// current bar, and several column types.
func TestLatestRowMatchesDiskRead(t *testing.T) {
	capture := setupCapturingInstance(t)
	tz := utils.InstanceConfig.Timezone
	rng := rand.New(rand.NewSource(7))

	type bucket struct {
		key   string
		tf    string
		build func(epochs []int64) *io.ColumnSeries
	}
	ohlcv := func(epochs []int64) *io.ColumnSeries {
		n := len(epochs)
		o, h, l, c := make([]float32, n), make([]float32, n), make([]float32, n), make([]float32, n)
		v := make([]int64, n)
		for i := range epochs {
			o[i], h[i], l[i], c[i] = rng.Float32()*500, rng.Float32()*500, rng.Float32()*500, rng.Float32()*500
			v[i] = rng.Int63n(1e9)
		}
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", epochs)
		cs.AddColumn("Open", o)
		cs.AddColumn("High", h)
		cs.AddColumn("Low", l)
		cs.AddColumn("Close", c)
		cs.AddColumn("Volume", v)
		return cs
	}
	mixed := func(epochs []int64) *io.ColumnSeries {
		n := len(epochs)
		p, q := make([]float64, n), make([]int32, n)
		b, u := make([]uint8, n), make([]uint64, n)
		for i := range epochs {
			p[i], q[i], b[i], u[i] = rng.NormFloat64(), rng.Int31(), uint8(rng.Intn(256)), rng.Uint64()
		}
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", epochs)
		cs.AddColumn("Price", p)
		cs.AddColumn("Count", q)
		cs.AddColumn("Flag", b)
		cs.AddColumn("Big", u)
		return cs
	}
	buckets := []bucket{
		{"AAPL/1Sec/OHLCV", "1Sec", ohlcv},
		{"AAPL/1Min/OHLCV", "1Min", ohlcv},
		{"MIX/1Min/STATS", "1Min", mixed},
	}

	base := time.Date(2026, 9, 24, 9, 30, 0, 0, tz)
	fallbacksBefore := diskFallbacks.Load()
	for _, b := range buckets {
		tbk := io.NewTimeBucketKey(b.key)
		tf := utils.NewTimeframe(b.tf).Duration
		keyPath := b.key + "/2026.bin"
		fire := 0
		for round := 0; round < 60; round++ {
			var epochs []int64
			switch round % 4 {
			case 0: // single new bar
				epochs = []int64{base.Add(time.Duration(round) * tf).Unix()}
			case 1: // rewrite the previous bar
				epochs = []int64{base.Add(time.Duration(round-1) * tf).Unix()}
			case 2: // batch, in order
				for k := 0; k < 3; k++ {
					epochs = append(epochs, base.Add(time.Duration(round+k)*tf).Unix())
				}
			case 3: // batch, newest first
				for k := 2; k >= 0; k-- {
					epochs = append(epochs, base.Add(time.Duration(round+k)*tf).Unix())
				}
			}
			csm := io.NewColumnSeriesMap()
			csm.AddColumnSeries(*tbk, b.build(epochs))
			require.NoError(t, executor.WriteCSM(csm, false))

			recs := capture.next(t, keyPath, fire)
			fire++

			got, err := latestRow(tbk, tf, 2026, recs)
			require.NoError(t, err)
			newest := newestRecord(recs)
			want, err := readLatestFromDisk(tbk, io.IndexToTime(newest.Index(), tf, 2026))
			require.NoError(t, err)
			requireSameRow(t, want, got, b.key)
		}
		l, err := layoutFor(tbk)
		require.NoError(t, err)
		require.False(t, l.variable)
	}
	require.Equal(t, fallbacksBefore, diskFallbacks.Load(),
		"fixed-length buckets must be decoded from records, not read back from disk")
}

// TestLatestRowVariableLengthFallsBack: variable-length buckets pack several
// rows per index, so they must take the disk path and still be correct.
func TestLatestRowVariableLengthFallsBack(t *testing.T) {
	capture := setupCapturingInstance(t)
	tz := utils.InstanceConfig.Timezone
	tbk := io.NewTimeBucketKey("AAPL/1Sec/TICKS")
	ts := time.Date(2026, 9, 24, 10, 0, 0, 0, tz)

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{ts.Unix(), ts.Unix()})
	cs.AddColumn("Price", []float64{101.25, 101.5})
	cs.AddColumn("Size", []float64{10, 20})
	cs.AddColumn("Nanoseconds", []int32{1000, 2000})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)
	require.NoError(t, executor.WriteCSM(csm, true))

	recs := capture.next(t, "AAPL/1Sec/TICKS/2026.bin", 0)
	l, err := layoutFor(tbk)
	require.NoError(t, err)
	require.True(t, l.variable, "bucket should be variable-length")

	fallbacksBefore := diskFallbacks.Load()
	got, err := latestRow(tbk, time.Second, 2026, recs)
	require.Equal(t, fallbacksBefore+1, diskFallbacks.Load(), "variable-length must use the disk path")
	require.NoError(t, err)
	want, err := readLatestFromDisk(tbk, ts)
	require.NoError(t, err)
	requireSameRow(t, want, got, "variable")
}
