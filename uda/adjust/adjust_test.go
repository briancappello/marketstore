package adjust_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/corpaction"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/uda/adjust"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/functions"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var ny, _ = time.LoadLocation("America/New_York")

func et(y int, m time.Month, d, hh, mm int) time.Time { return time.Date(y, m, d, hh, mm, 0, 0, ny) }

func startInstance(t *testing.T) *catalog.Directory {
	t.Helper()
	prev := utils.InstanceConfig
	t.Cleanup(func() { utils.InstanceConfig = prev })
	adjust.ResetCache()
	t.Cleanup(adjust.ResetCache)

	cfg := utils.NewDefaultConfig(t.TempDir())
	cfg.BackgroundSync = false
	cfg.Timezone = ny
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)
	walFile, err := executor.NewWALFile(c.GetAbsRootDir(), c.GetInitInstanceID(),
		&executor.NopReplicationSender{}, false, &sync.WaitGroup{},
		executor.StartNewTriggerPluginDispatcherWithWorkers(nil, 1), executor.NewTransactionPipe())
	require.NoError(t, err)
	executor.NewInstanceSetup(c.GetCatalogDir(), walFile)
	return c.GetCatalogDir()
}

func writeSplit(t *testing.T, sym string, y int, m time.Month, d int, from, to float64) {
	t.Helper()
	st := corpaction.ForwardSplit
	if to < from {
		st = corpaction.ReverseSplit
	}
	e, err := corpaction.NewSplit(sym, corpaction.EffectiveDate(y, m, d), from, to, st)
	require.NoError(t, err)
	csm, err := corpaction.EncodeCSM([]corpaction.Event{e})
	require.NoError(t, err)
	require.NoError(t, executor.WriteCSM(csm, false))
}

// run applies adjust(args...) to cs as if it were a query result of tbk,
// with the clock fixed at now.
func run(t *testing.T, dir *catalog.Directory, tbk string, now time.Time, cs *io.ColumnSeries,
	args ...interface{},
) *io.ColumnSeries {
	t.Helper()
	proto := &adjust.Adjust{CatalogDir: dir, Now: func() time.Time { return now }}
	am := functions.NewArgumentMap(proto.GetRequiredArgs(), proto.GetOptionalArgs()...)
	agg, err := proto.New(am, args...)
	require.NoError(t, err)
	out, err := agg.Accum(*io.NewTimeBucketKeyFromString(tbk), am, cs)
	require.NoError(t, err)
	return out
}

func ohlcv(epochs []time.Time, closes []float32, vols []int64) *io.ColumnSeries {
	ep := make([]int64, len(epochs))
	for i, e := range epochs {
		ep[i] = e.Unix()
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", ep)
	cs.AddColumn("Open", append([]float32(nil), closes...))
	cs.AddColumn("High", append([]float32(nil), closes...))
	cs.AddColumn("Low", append([]float32(nil), closes...))
	cs.AddColumn("Close", closes)
	cs.AddColumn("Volume", vols)
	return cs
}

var today = et(2026, 11, 23, 12, 0)

func TestNoEventsReturnsInputUnchanged(t *testing.T) {
	dir := startInstance(t)
	in := ohlcv([]time.Time{et(2024, 6, 7, 9, 30)}, []float32{400}, []int64{1000})
	out := run(t, dir, "MSFT/1Min/OHLCV", today, in, "split")
	assert.Equal(t, []float32{400}, out.GetColumn("Close"))
	assert.Equal(t, []int64{1000}, out.GetColumn("Volume"))
}

func TestICEBucketIsIgnored(t *testing.T) {
	dir := startInstance(t)
	ice := io.NewColumnSeries()
	ice.AddColumn("Epoch", []int64{et(2020, 8, 31, 0, 0).Unix()})
	ice.AddColumn("Rate", []float64{4})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKeyFromString("AAPL/1D/ACTIONS"), ice)
	require.NoError(t, executor.WriteCSM(csm, true))

	in := ohlcv([]time.Time{et(2020, 8, 28, 9, 30)}, []float32{400}, []int64{1000})
	out := run(t, dir, "AAPL/1Min/OHLCV", today, in, "split")
	assert.Equal(t, []float32{400}, out.GetColumn("Close"))
}

func TestFloat32OHLCVAndIntVolume(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "AAPL", 2020, 8, 31, 1, 4)
	in := ohlcv([]time.Time{et(2020, 8, 28, 0, 0)}, []float32{400}, []int64{1000})
	out := run(t, dir, "AAPL/1D/OHLCV", today, in, "split")
	for _, c := range []string{"Open", "High", "Low", "Close"} {
		assert.Equal(t, []float32{100}, out.GetColumn(c), c)
	}
	assert.Equal(t, []int64{4000}, out.GetColumn("Volume"))
	assert.Equal(t, []string{"Epoch", "Open", "High", "Low", "Close", "Volume"}, out.GetColumnNames())
}

func TestTradeSizeIsMultiplied(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "AAPL", 2020, 8, 31, 1, 4)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{et(2020, 8, 28, 10, 0).Unix()})
	cs.AddColumn("Nanoseconds", []int32{123})
	cs.AddColumn("Price", []float64{400})
	cs.AddColumn("Size", []float64{10})
	cs.AddColumn("Exchange", []byte{7})
	cs.AddColumn("Cond1", []byte{3})
	out := run(t, dir, "AAPL/1Sec/TRADE", today, cs, "split")
	assert.Equal(t, []float64{100}, out.GetColumn("Price"))
	assert.Equal(t, []float64{40}, out.GetColumn("Size"))
	assert.Equal(t, []byte{7}, out.GetColumn("Exchange"))
	assert.Equal(t, []byte{3}, out.GetColumn("Cond1"))
	assert.Equal(t, []int32{123}, out.GetColumn("Nanoseconds"))
}

func TestQuoteUint64Sizes(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "AAPL", 2020, 8, 31, 1, 4)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{et(2020, 8, 28, 10, 0).Unix()})
	cs.AddColumn("BidPrice", []float64{399})
	cs.AddColumn("AskPrice", []float64{401})
	cs.AddColumn("BidSize", []uint64{100})
	cs.AddColumn("AskSize", []uint64{200})
	out := run(t, dir, "AAPL/1Sec/QUOTE", today, cs, "split")
	assert.Equal(t, []float64{99.75}, out.GetColumn("BidPrice"))
	assert.Equal(t, []float64{100.25}, out.GetColumn("AskPrice"))
	assert.Equal(t, []uint64{400}, out.GetColumn("BidSize"))
	assert.Equal(t, []uint64{800}, out.GetColumn("AskSize"))
}

func TestReverseSplitRoundsSizes(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "XYZ", 2024, 3, 1, 10, 1)
	in := ohlcv([]time.Time{et(2024, 2, 29, 0, 0)}, []float32{1.5}, []int64{1005})
	out := run(t, dir, "XYZ/1D/OHLCV", today, in, "split")
	assert.Equal(t, []int64{101}, out.GetColumn("Volume"))
	assert.Equal(t, []float32{15}, out.GetColumn("Close"))
}

func TestBoundaryAtExecutionDate(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "NVDA", 2024, 6, 10, 1, 4)
	in := ohlcv([]time.Time{et(2024, 6, 7, 19, 59), et(2024, 6, 10, 4, 0)},
		[]float32{400, 100}, []int64{10, 40})
	out := run(t, dir, "NVDA/1Min/OHLCV", today, in, "split")
	assert.Equal(t, []float32{100, 100}, out.GetColumn("Close"))
	assert.Equal(t, []int64{40, 40}, out.GetColumn("Volume"))
}

func TestTwoSplitsCompound(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "XYZ", 2020, 1, 2, 1, 2)
	writeSplit(t, "XYZ", 2022, 1, 3, 1, 3)
	in := ohlcv([]time.Time{et(2019, 6, 3, 0, 0), et(2021, 6, 1, 0, 0), et(2022, 6, 1, 0, 0)},
		[]float32{60, 30, 10}, []int64{1, 1, 1})
	out := run(t, dir, "XYZ/1D/OHLCV", today, in, "split")
	assert.Equal(t, []float32{10, 10, 10}, out.GetColumn("Close"))
	assert.Equal(t, []int64{6, 3, 1}, out.GetColumn("Volume"))
}

func TestAnnouncedSplitDoesNotApplyYet(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "XYZ", 2026, 12, 1, 1, 2)
	in := ohlcv([]time.Time{et(2026, 11, 20, 0, 0)}, []float32{50}, []int64{100})
	out := run(t, dir, "XYZ/1D/OHLCV", today, in, "split")
	assert.Equal(t, []float32{50}, out.GetColumn("Close"))

	// Once the effective date has started, it applies.
	out = run(t, dir, "XYZ/1D/OHLCV", et(2026, 12, 1, 4, 0),
		ohlcv([]time.Time{et(2026, 11, 20, 0, 0)}, []float32{50}, []int64{100}), "split")
	assert.Equal(t, []float32{25}, out.GetColumn("Close"))
}

func TestArgumentsSelectKinds(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "AAPL", 2020, 8, 31, 1, 4)
	mk := func() *io.ColumnSeries {
		return ohlcv([]time.Time{et(2020, 8, 28, 0, 0)}, []float32{400}, []int64{1000})
	}
	assert.Equal(t, []float32{400}, run(t, dir, "AAPL/1D/OHLCV", today, mk(), "dividend").GetColumn("Close"))
	assert.Equal(t, []float32{100}, run(t, dir, "AAPL/1D/OHLCV", today, mk(), "SPLIT").GetColumn("Close"))
	assert.Equal(t, []float32{100}, run(t, dir, "AAPL/1D/OHLCV", today, mk()).GetColumn("Close"))
	assert.Equal(t, []float32{100},
		run(t, dir, "AAPL/1D/OHLCV", today, mk(), []string{"split", "dividend"}).GetColumn("Close"))
}

func TestFloatPricesRoundToFourDecimals(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "XYZ", 2024, 3, 1, 1, 3)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{et(2024, 2, 29, 10, 0).Unix()})
	cs.AddColumn("Price", []float64{10})
	out := run(t, dir, "XYZ/1Sec/TRADE", today, cs, "split")
	assert.Equal(t, []float64{3.3333}, out.GetColumn("Price"))
}

func TestNewEventVisibleAfterTTL(t *testing.T) {
	dir := startInstance(t)
	prevTTL := adjust.CacheTTL
	adjust.CacheTTL = 50 * time.Millisecond
	t.Cleanup(func() { adjust.CacheTTL = prevTTL })

	mk := func() *io.ColumnSeries {
		return ohlcv([]time.Time{et(2020, 8, 28, 0, 0)}, []float32{400}, []int64{1000})
	}
	assert.Equal(t, []float32{400}, run(t, dir, "AAPL/1D/OHLCV", today, mk(), "split").GetColumn("Close"))
	writeSplit(t, "AAPL", 2020, 8, 31, 1, 4)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, []float32{100}, run(t, dir, "AAPL/1D/OHLCV", today, mk(), "split").GetColumn("Close"))
}

func TestConcurrentAccum(t *testing.T) {
	dir := startInstance(t)
	writeSplit(t, "AAPL", 2020, 8, 31, 1, 4)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				adjust.ResetCache()
				out := run(t, dir, "AAPL/1D/OHLCV", today,
					ohlcv([]time.Time{et(2020, 8, 28, 0, 0)}, []float32{400}, []int64{1000}), "split")
				assert.Equal(t, []float32{100}, out.GetColumn("Close"))
			}
		}()
	}
	wg.Wait()
}
