package backfill_test

import (
	"context"
	"errors"
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

	d := backfill.NewDriver(api, nil, write, wm, 4, 0, 0, func(string) bool { return false })
	// now must be at least one timeframe period past the returned epoch, or the
	// bar counts as still open and is correctly withheld. A 1D bucket needs
	// now >= epoch+86400; 1000 would leave it unformed.
	require.Nil(t, d.Reconcile(context.Background(), 10_000_000))

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
	d := backfill.NewDriver(api, nil, write, wm, 4, 0, 0, isVar)
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

	d := backfill.NewDriver(api, nil, func(io.ColumnSeriesMap, bool) error { return nil },
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

	d := backfill.NewDriver(api, nil, func(io.ColumnSeriesMap, bool) error { return nil },
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

	d := backfill.NewDriver(api, nil, func(io.ColumnSeriesMap, bool) error { return nil },
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

	d := backfill.NewDriver(api, nil, func(io.ColumnSeriesMap, bool) error { return nil },
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
	d := backfill.NewDriver(api, nil, func(io.ColumnSeriesMap, bool) error { return nil }, wm, 2, 0, 0, func(string) bool { return false })

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx, time.Hour, func() int64 { return 1000 }) // long interval: only the immediate pass runs
	// Give the immediate reconcile time to happen, then stop.
	assert.Eventually(t, func() bool { return wm.Get("AAPL/1Min/OHLCV") == 10 }, 2*time.Second, 10*time.Millisecond)
	cancel()
}

// failingAPI fails the query for one bucket and otherwise behaves as listAPI.
type failingAPI struct {
	listAPI
	fail string
}

func (f *failingAPI) QueryRange(ctx context.Context, tbk string, s, e int64) (io.ColumnSeriesMap, error) {
	if tbk == f.fail {
		return nil, errors.New("boom")
	}
	return f.listAPI.QueryRange(ctx, tbk, s, e)
}

// The scenario that left p1 without the first premarket minutes of a day: the
// master revises an epoch about one lookback after it, a deep pass runs a few
// hours later, and its watermark-based window has already moved past it.
// Anchored at the previous complete deep pass, the window still covers it.
func TestDriverDeepPassReachesBackToPreviousDeepPass(t *testing.T) {
	const day = int64(86_400)
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	d := backfill.NewDriver(api, nil, func(io.ColumnSeriesMap, bool) error { return nil },
		wm, 1, 24*time.Hour, 24*time.Hour, func(string) bool { return false })

	t0 := 100 * day
	_ = wm.Set("AAPL/1Min/OHLCV", t0)
	require.Nil(t, d.Reconcile(context.Background(), t0)) // deep: anchor = t0

	// The live stream keeps the watermark current; 7h later the next deep
	// pass runs (forced, as on a restart or stream reconnect).
	t1 := t0 + 7*3600
	_ = wm.Set("AAPL/1Min/OHLCV", t1)
	d.RequestDeepHeal()
	require.Nil(t, d.Reconcile(context.Background(), t1))

	// Watermark-only would start at t1+1-24h; anchored, it starts at t0-24h,
	// which covers an epoch revised just under 24h old at any time since t0.
	assert.Equal(t, t0-day, api.starts[1])
}

// The anchor survives a restart: a new driver tracking the same file starts
// its first deep pass from the last complete deep pass of the previous run,
// however long the replica was down.
func TestDriverDeepHealAnchorPersistsAcrossRestart(t *testing.T) {
	const day = int64(86_400)
	dir := t.TempDir()
	newDriver := func(api backfill.MasterAPI, wm *backfill.Watermarks) *backfill.Driver {
		d := backfill.NewDriver(api, nil, func(io.ColumnSeriesMap, bool) error { return nil },
			wm, 1, 24*time.Hour, 24*time.Hour, func(string) bool { return false })
		require.Nil(t, d.TrackDeepHeals(dir+"/heal.json"))
		return d
	}

	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	wm, _ := backfill.NewWatermarks(dir + "/wm.json")
	t0 := 100 * day
	_ = wm.Set("AAPL/1Min/OHLCV", t0)
	require.Nil(t, newDriver(api, wm).Reconcile(context.Background(), t0))

	// Down for 5 days; the watermark had advanced a few hours past t0.
	_ = wm.Set("AAPL/1Min/OHLCV", t0+3*3600)
	api2 := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	require.Nil(t, newDriver(api2, wm).Reconcile(context.Background(), t0+5*day))
	assert.Equal(t, t0-day, api2.starts[0])
}

// A deep pass where a bucket failed must not move the anchor, or the failed
// bucket's correction window would be skipped by every later pass.
func TestDriverIncompleteDeepPassKeepsAnchor(t *testing.T) {
	const day = int64(86_400)
	dir := t.TempDir()
	api := &failingAPI{listAPI: listAPI{tbks: []string{"AAPL/1Min/OHLCV", "MSFT/1Min/OHLCV"}}}
	wm, _ := backfill.NewWatermarks(dir + "/wm.json")
	d := backfill.NewDriver(api, nil, func(io.ColumnSeriesMap, bool) error { return nil },
		wm, 1, 24*time.Hour, 24*time.Hour, func(string) bool { return false })
	require.Nil(t, d.TrackDeepHeals(dir+"/heal.json"))

	t0 := 100 * day
	_ = wm.Set("AAPL/1Min/OHLCV", t0)
	require.Nil(t, d.Reconcile(context.Background(), t0)) // complete: anchor = t0

	api.fail = "MSFT/1Min/OHLCV"
	d.RequestDeepHeal()
	require.Nil(t, d.Reconcile(context.Background(), t0+day)) // incomplete

	api.fail = ""
	api.starts = nil
	api.queried = nil
	_ = wm.Set("AAPL/1Min/OHLCV", t0+2*day)
	d.RequestDeepHeal()
	require.Nil(t, d.Reconcile(context.Background(), t0+2*day))
	for i, tbk := range api.queried {
		if tbk == "AAPL/1Min/OHLCV" {
			assert.Equal(t, t0-day, api.starts[i], "anchor must still be t0")
		}
	}
}
