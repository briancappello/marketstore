package main

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/corpaction"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func startSplitInstance(t *testing.T) *catalog.Directory {
	t.Helper()
	prev := utils.InstanceConfig
	t.Cleanup(func() { utils.InstanceConfig = prev })
	cfg := utils.NewDefaultConfig(t.TempDir())
	cfg.BackgroundSync = false
	cfg.Timezone = calendar.Nasdaq.Tz()
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)
	walFile, err := executor.NewWALFile(c.GetAbsRootDir(), c.GetInitInstanceID(),
		&executor.NopReplicationSender{}, false, &sync.WaitGroup{},
		executor.StartNewTriggerPluginDispatcherWithWorkers(nil, 1), executor.NewTransactionPipe())
	require.NoError(t, err)
	executor.NewInstanceSetup(c.GetCatalogDir(), walFile)
	return c.GetCatalogDir()
}

func stubSplits(t *testing.T, fn func() ([]api.Split, error)) *int {
	t.Helper()
	calls := 0
	orig := listSplits
	listSplits = func(*http.Client, api.SplitsQuery) ([]api.Split, error) {
		calls++
		return fn()
	}
	t.Cleanup(func() { listSplits = orig })
	return &calls
}

func writeBar(t *testing.T, tbk string) {
	t.Helper()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{time.Date(2026, 9, 1, 0, 0, 0, 0, calendar.Nasdaq.Tz()).Unix()})
	cs.AddColumn("Open", []float32{1})
	cs.AddColumn("High", []float32{1})
	cs.AddColumn("Low", []float32{1})
	cs.AddColumn("Close", []float32{1})
	cs.AddColumn("Volume", []int64{1})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKeyFromString(tbk), cs)
	require.NoError(t, executor.WriteCSM(csm, false))
}

func TestSyncSplitsWritesTrackedSymbols(t *testing.T) {
	dir := startSplitInstance(t)
	// TSLA is not configured but has data (e.g. from dynamic ticks).
	writeBar(t, "TSLA/1D/OHLCV")
	stubSplits(t, func() ([]api.Split, error) {
		return []api.Split{
			{Ticker: "NVDA", ExecutionDate: "2024-06-10", SplitFrom: 1, SplitTo: 10, AdjustmentType: "forward_split"},
			{Ticker: "TSLA", ExecutionDate: "2022-08-25", SplitFrom: 1, SplitTo: 3, AdjustmentType: "forward_split"},
			{Ticker: "ZZZZ", ExecutionDate: "2022-08-25", SplitFrom: 1, SplitTo: 3, AdjustmentType: "forward_split"},
			{Ticker: "NVDA", ExecutionDate: "2021-07-20", SplitFrom: 0, SplitTo: 4, AdjustmentType: "forward_split"},
		}, nil
	})
	mf := testFetcher(nil, "NVDA")

	res, err := mf.syncSplits(nil)
	require.NoError(t, err)
	assert.Equal(t, 2, res.written)
	assert.Equal(t, 1, res.skipped)

	nvda, err := corpaction.Load(dir, "NVDA", corpaction.Splits)
	require.NoError(t, err)
	require.Len(t, nvda, 1)
	assert.Equal(t, 10.0, nvda[0].PriceFactor)
	tsla, err := corpaction.Load(dir, "TSLA", corpaction.Splits)
	require.NoError(t, err)
	assert.Len(t, tsla, 1)
	_, err = dir.GetLatestTimeBucketInfoFromKey(corpaction.TimeBucketKey("ZZZZ", corpaction.Splits))
	assert.Error(t, err, "untracked symbols get no bucket")

	// Re-sync writes nothing new.
	res, err = mf.syncSplits(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, res.written)
	assert.Equal(t, 2, res.unchanged)
}

func TestSyncSplitsNeutralizesWithdrawn(t *testing.T) {
	dir := startSplitInstance(t)
	results := []api.Split{
		{Ticker: "AAPL", ExecutionDate: "2020-08-31", SplitFrom: 1, SplitTo: 4, AdjustmentType: "forward_split"},
		{Ticker: "AAPL", ExecutionDate: "2021-01-04", SplitFrom: 1, SplitTo: 2, AdjustmentType: "forward_split"},
	}
	stubSplits(t, func() ([]api.Split, error) { return results, nil })
	mf := testFetcher(nil, "AAPL")
	_, err := mf.syncSplits(nil)
	require.NoError(t, err)

	results = results[:1]
	res, err := mf.syncSplits(nil)
	require.NoError(t, err)
	assert.Equal(t, 1, res.neutralized)
	got, err := corpaction.Load(dir, "AAPL", corpaction.Splits)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, 4.0, got[0].PriceFactor)
}

func TestSyncSplitsWildcard(t *testing.T) {
	dir := startSplitInstance(t)
	stubSplits(t, func() ([]api.Split, error) {
		return []api.Split{{Ticker: "ZZZZ", ExecutionDate: "2022-08-25", SplitFrom: 1, SplitTo: 3, AdjustmentType: "forward_split"}}, nil
	})
	mf := testFetcher(nil, "*")
	_, err := mf.syncSplits(nil)
	require.NoError(t, err)
	got, err := corpaction.Load(dir, "ZZZZ", corpaction.Splits)
	require.NoError(t, err)
	assert.Len(t, got, 1)
}

func TestWriteSplitBatchesGroupsBySymbol(t *testing.T) {
	var writes []int
	orig := writeSplitsCSM
	writeSplitsCSM = func(csm io.ColumnSeriesMap) error { writes = append(writes, len(csm)); return nil }
	t.Cleanup(func() { writeSplitsCSM = orig })

	var events []corpaction.Event
	for i := 0; i < splitWriteBatch+3; i++ {
		for _, d := range []int{1, 2} {
			e, err := corpaction.NewSplit(fmt.Sprintf("S%04d", i), corpaction.EffectiveDate(2020, 1, d), 1, 2, corpaction.ForwardSplit)
			require.NoError(t, err)
			events = append(events, e)
		}
	}
	require.NoError(t, writeSplitBatches(events))
	assert.Equal(t, []int{splitWriteBatch, 3}, writes)
}

func TestNextSplitSync(t *testing.T) {
	et := calendar.Nasdaq.Tz()
	assert.Equal(t, time.Date(2026, 11, 24, 3, 30, 0, 0, et),
		nextSplitSync(time.Date(2026, 11, 23, 18, 0, 0, 0, et), 3, 30))
	assert.Equal(t, time.Date(2026, 11, 23, 3, 30, 0, 0, et),
		nextSplitSync(time.Date(2026, 11, 23, 1, 0, 0, 0, et), 3, 30))
	assert.Equal(t, time.Date(2026, 11, 24, 3, 30, 0, 0, et),
		nextSplitSync(time.Date(2026, 11, 23, 3, 30, 0, 0, et), 3, 30))
}

// The loop syncs at startup, keeps going after failures (retrying at the
// next scheduled time), and stops on shutdown.
func TestSplitSyncLoopSchedule(t *testing.T) {
	startSplitInstance(t)
	et := calendar.Nasdaq.Tz()
	clock := time.Date(2026, 11, 23, 18, 0, 0, 0, et)
	errs := []error{fmt.Errorf("HTTP 500: %w", fmt.Errorf("boom")), fmt.Errorf("x: %w", api.ErrAuthFailed), nil}
	calls := stubSplits(t, func() ([]api.Split, error) {
		err := errs[0]
		if len(errs) > 1 {
			errs = errs[1:]
		}
		return nil, err
	})

	mf := testFetcher(nil, "AAPL")
	waits := make(chan time.Duration, 10)
	ticks := make(chan time.Time)
	after := func(d time.Duration) <-chan time.Time {
		waits <- d
		return ticks
	}
	mf.wg.Add(1)
	done := make(chan struct{})
	go func() { mf.runSplitSyncLoop(func() time.Time { return clock }, after); close(done) }()

	// Startup sync failed with HTTP 500; next run is at 03:30 tomorrow.
	assert.Equal(t, 9*time.Hour+30*time.Minute, <-waits)
	assert.Equal(t, 1, *calls)
	ticks <- clock
	// Auth failure: also waits for the next scheduled time.
	<-waits
	assert.Equal(t, 2, *calls)
	ticks <- clock
	<-waits
	assert.Equal(t, 3, *calls)

	mf.cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not stop on shutdown")
	}
}
