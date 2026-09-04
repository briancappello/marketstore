package backfill_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type listAPI struct {
	tbks    []string
	mu      sync.Mutex
	queried []string
	starts  []int64
}

func (l *listAPI) ListTBKs(_ context.Context) ([]string, error) { return l.tbks, nil }
func (l *listAPI) QueryRange(_ context.Context, tbk string, s, _ int64) (io.ColumnSeriesMap, error) {
	l.mu.Lock()
	l.queried = append(l.queried, tbk)
	l.starts = append(l.starts, s)
	l.mu.Unlock()
	tk := io.NewTimeBucketKey(tbk)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{10})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tk, cs)
	return csm, nil
}

func TestDriverReconcileBackfillsEveryBucket(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV", "MSFT/1D/OHLCV"}}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	write := func(io.ColumnSeriesMap, bool) error { return nil }

	d := backfill.NewDriver(api, write, wm, 4, 0, 0, func(string) bool { return false })
	require.Nil(t, d.Reconcile(context.Background(), 1000))

	sort.Strings(api.queried)
	assert.Equal(t, []string{"AAPL/1Min/OHLCV", "MSFT/1D/OHLCV"}, api.queried)
	assert.Equal(t, int64(10), wm.Get("AAPL/1Min/OHLCV"))
	assert.Equal(t, int64(10), wm.Get("MSFT/1D/OHLCV"))
}

func TestDriverReconcileSkipsVariableBuckets(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV", "AAPL/1Sec/TRADE"}}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	write := func(io.ColumnSeriesMap, bool) error { return nil }

	// Report the TRADE bucket as variable-length; it must be skipped so its
	// append-only writes are never duplicated by re-pull.
	isVar := func(tbk string) bool { return tbk == "AAPL/1Sec/TRADE" }
	d := backfill.NewDriver(api, write, wm, 4, 0, 0, isVar)
	require.Nil(t, d.Reconcile(context.Background(), 1000))

	assert.Equal(t, []string{"AAPL/1Min/OHLCV"}, api.queried, "variable bucket must not be queried")
	assert.Equal(t, int64(0), wm.Get("AAPL/1Sec/TRADE"), "variable bucket watermark must stay unset")
}

// The lookback is the correction-healing window: it reaches back BEHIND the
// watermark to re-pull epochs the master may have revised. Gaps do not need it
// -- the watermark is only advanced by a successful backfill (worker.go), never
// by the live stream, so [watermark+1, now] already spans any outage.
//
// Applying it on every reconcile therefore buys nothing and costs
// lookback/reconcile_interval times the write volume (24h/5m = 288x in prod).
// It belongs on a slow "deep heal" cadence instead.
func TestDriverAppliesLookbackOnlyOnDeepPass(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	// listAPI returns epoch 10, which never regresses this watermark.
	require.Nil(t, wm.Set("AAPL/1Min/OHLCV", 5000))

	d := backfill.NewDriver(api, func(io.ColumnSeriesMap, bool) error { return nil },
		wm, 4, time.Hour, 24*time.Hour, func(string) bool { return false })

	// First pass after construction is a deep heal: reach back a full lookback.
	require.Nil(t, d.Reconcile(context.Background(), 10_000))
	assert.Equal(t, int64(1401), api.starts[0], "first pass must apply the lookback (5000+1-3600)")

	// Second pass, well inside the heal interval, must be watermark-only.
	require.Nil(t, d.Reconcile(context.Background(), 10_600))
	assert.Equal(t, int64(5001), api.starts[1], "steady-state pass must not re-pull the lookback window")
}

func TestDriverRepeatsDeepPassAfterHealInterval(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Min/OHLCV", 5000)

	d := backfill.NewDriver(api, func(io.ColumnSeriesMap, bool) error { return nil },
		wm, 4, time.Hour, 24*time.Hour, func(string) bool { return false })

	require.Nil(t, d.Reconcile(context.Background(), 10_000)) // deep
	require.Nil(t, d.Reconcile(context.Background(), 10_600)) // shallow
	// One heal interval later the deep sweep is due again.
	require.Nil(t, d.Reconcile(context.Background(), 10_000+86_400))

	assert.Equal(t, []int64{1401, 5001, 1401}, api.starts)
}

func TestDriverRequestDeepHealForcesNextPass(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Min/OHLCV", 5000)

	d := backfill.NewDriver(api, func(io.ColumnSeriesMap, bool) error { return nil },
		wm, 4, time.Hour, 24*time.Hour, func(string) bool { return false })

	require.Nil(t, d.Reconcile(context.Background(), 10_000)) // deep (first pass)
	require.Nil(t, d.Reconcile(context.Background(), 10_600)) // shallow

	// A live-stream reconnect asks for a deep sweep before the interval is due.
	d.RequestDeepHeal()
	require.Nil(t, d.Reconcile(context.Background(), 10_700))
	assert.Equal(t, int64(1401), api.starts[2], "RequestDeepHeal must force a lookback pass")

	// The request is one-shot: the pass after it is shallow again.
	require.Nil(t, d.Reconcile(context.Background(), 10_800))
	assert.Equal(t, int64(5001), api.starts[3], "deep heal request must not latch on")
}

// A zero heal interval must not silently mean "deep every pass" -- that is the
// 288x amplification bug. Fall back to the 24h default, as parallelism does.
func TestDriverDefaultsHealIntervalWhenUnset(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Min/OHLCV", 5000)

	d := backfill.NewDriver(api, func(io.ColumnSeriesMap, bool) error { return nil },
		wm, 4, time.Hour, 0, func(string) bool { return false })

	require.Nil(t, d.Reconcile(context.Background(), 10_000)) // deep (first pass)
	require.Nil(t, d.Reconcile(context.Background(), 10_600)) // shallow
	// Still inside the defaulted 24h window.
	require.Nil(t, d.Reconcile(context.Background(), 10_000+86_000))

	assert.Equal(t, []int64{1401, 5001, 5001}, api.starts)
}

func TestDriverRunReconcilesImmediatelyThenStops(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	d := backfill.NewDriver(api, func(io.ColumnSeriesMap, bool) error { return nil }, wm, 2, 0, 0, func(string) bool { return false })

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx, time.Hour, func() int64 { return 1000 }) // long interval: only the immediate pass runs
	// Give the immediate reconcile time to happen, then stop.
	assert.Eventually(t, func() bool { return wm.Get("AAPL/1Min/OHLCV") == 10 }, 2*time.Second, 10*time.Millisecond)
	cancel()
}
