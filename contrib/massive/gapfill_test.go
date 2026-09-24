package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
	"github.com/alpacahq/marketstore/v4/contrib/massive/ws"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// Today's incident: a startup backfill followed by two reconnects produced
// three concurrent full backfills. With the runner, the reconnects wait and
// are merged into a single run.
func TestBackfillRunnerSerializesAndCoalesces(t *testing.T) {
	t.Parallel()

	var (
		mu          sync.Mutex
		runs        []backfillRequest
		inFlight    atomic.Int32
		maxInFlight atomic.Int32
	)
	release := make(chan struct{})
	started := make(chan struct{}, 10)
	var wg sync.WaitGroup
	r := newBackfillRunner(func(req backfillRequest) {
		n := inFlight.Add(1)
		if n > maxInFlight.Load() {
			maxInFlight.Store(n)
		}
		started <- struct{}{}
		<-release
		mu.Lock()
		runs = append(runs, req)
		mu.Unlock()
		inFlight.Add(-1)
	}, &wg)

	base := time.Date(2026, 9, 24, 11, 0, 0, 0, calendar.Nasdaq.Tz())
	r.submit(backfillRequest{full: true})
	<-started // the full backfill is now running

	r.submit(backfillRequest{gap: &outage{from: base.Add(2 * time.Minute), to: base.Add(3 * time.Minute)}})
	r.submit(backfillRequest{gap: &outage{from: base, to: base.Add(time.Minute)}})
	r.submit(backfillRequest{gap: &outage{from: base.Add(time.Minute), to: base.Add(5 * time.Minute)}})

	release <- struct{}{} // finish the full backfill
	<-started             // the merged gap fill starts
	release <- struct{}{}
	wg.Wait()

	require.Len(t, runs, 2)
	assert.True(t, runs[0].full)
	assert.Nil(t, runs[0].gap)
	assert.False(t, runs[1].full)
	require.NotNil(t, runs[1].gap)
	assert.Equal(t, base, runs[1].gap.from, "merged window starts at the earliest outage")
	assert.Equal(t, base.Add(5*time.Minute), runs[1].gap.to, "merged window ends at the latest outage")
	assert.EqualValues(t, 1, maxInFlight.Load(), "backfills must never overlap")

	// Idle again: a new request starts a new run.
	r.submit(backfillRequest{full: true})
	<-started
	release <- struct{}{}
	wg.Wait()
	assert.Len(t, runs, 3)
}

func TestBackfillRunnerNilIsNoop(t *testing.T) {
	t.Parallel()
	var r *backfillRunner
	r.submit(backfillRequest{full: true}) // must not panic
}

func TestPlanReconnectBackfill(t *testing.T) {
	t.Parallel()
	et := calendar.Nasdaq.Tz()
	at := func(h, m, s int) time.Time { return time.Date(2026, 9, 24, h, m, s, 0, et) }

	tests := []struct {
		name      string
		lastData  time.Time
		reconnect time.Time
		wantFull  bool
		wantFrom  time.Time
		wantTo    time.Time
	}{
		{
			// The 11:38 ET i/o timeout: data stopped ~30s before the drop.
			name: "short intraday outage", lastData: at(11, 37, 38), reconnect: at(11, 38, 14),
			wantFrom: at(11, 36, 0), wantTo: at(11, 38, 19),
		},
		{
			name: "outage at the edge of the limit", lastData: at(9, 30, 0), reconnect: at(11, 28, 0),
			wantFrom: at(9, 29, 0), wantTo: at(11, 28, 5),
		},
		{name: "long outage", lastData: at(9, 30, 0), reconnect: at(12, 0, 0), wantFull: true},
		{
			name:     "overnight (nightly provider drop)",
			lastData: time.Date(2026, 9, 23, 20, 0, 0, 0, et), reconnect: at(3, 58, 0), wantFull: true,
		},
		{name: "never received data", reconnect: at(10, 0, 0), wantFull: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := planReconnectBackfill(tt.lastData, tt.reconnect)
			assert.Equal(t, tt.wantFull, req.full)
			if tt.wantFull {
				assert.Nil(t, req.gap)
				return
			}
			require.NotNil(t, req.gap)
			assert.Equal(t, tt.wantFrom, req.gap.from)
			assert.Equal(t, tt.wantTo, req.gap.to)
		})
	}
}

type barsCall struct {
	symbol, tf string
	from, to   time.Time
}

// stubFetchBars replaces the REST call for the duration of a test. Tests that
// use it must not run in parallel with each other.
func stubFetchBars(t *testing.T, fn func(c barsCall) (int, error)) *[]barsCall {
	t.Helper()
	var (
		mu    sync.Mutex
		calls []barsCall
	)
	orig := fetchBarsWindow
	fetchBarsWindow = func(_ context.Context, _ *http.Client, sym, tf string, from, to time.Time,
		_ int, _ bool, _ backfill.Writer,
	) (int, error) {
		c := barsCall{sym, tf, from, to}
		mu.Lock()
		calls = append(calls, c)
		mu.Unlock()
		return fn(c)
	}
	t.Cleanup(func() { fetchBarsWindow = orig })
	return &calls
}

func testFetcher(dataTypes []string, symbols ...string) *MassiveFetcher {
	ctx, cancel := context.WithCancel(context.Background())
	mf := &MassiveFetcher{ctx: ctx, cancel: cancel, wsDataTypes: map[string]struct{}{}}
	for _, dt := range dataTypes {
		mf.wsDataTypes[dt] = struct{}{}
	}
	for _, s := range symbols {
		mf.config.SymbolInfos = append(mf.config.SymbolInfos, massiveconfig.SymbolInfo{Symbol: s})
	}
	mf.config.BackfillParallelism = 4
	return mf
}

func TestFillOutageFetchesStreamedBarsForEverySymbol(t *testing.T) {
	calls := stubFetchBars(t, func(c barsCall) (int, error) {
		if c.symbol == "BAD" {
			return 0, errors.New("HTTP 500")
		}
		return 3, nil
	})
	// Production streams 1Sec plus tick types; only bars are filled.
	mf := testFetcher([]string{"1Sec", "trades", "quotes"}, "AAPL", "*", "BAD", "SPY")

	from := time.Now().Add(-3 * time.Minute)
	to := time.Now().Add(time.Minute) // in the future: must be clamped
	mf.fillOutage(outage{from: from, to: to})

	require.Len(t, *calls, 3, "one request per real symbol; the wildcard is skipped")
	seen := map[string]bool{}
	for _, c := range *calls {
		seen[c.symbol] = true
		assert.Equal(t, "1Sec", c.tf)
		assert.Equal(t, from, c.from)
		assert.True(t, c.to.Before(time.Now().Add(-gapFillSettle+time.Second)),
			"window must stop short of the second in progress")
	}
	assert.Equal(t, map[string]bool{"AAPL": true, "BAD": true, "SPY": true}, seen,
		"a failing symbol must not stop the others")
}

func TestFillOutageFinestTimeframeFirst(t *testing.T) {
	calls := stubFetchBars(t, func(barsCall) (int, error) { return 1, nil })
	mf := testFetcher([]string{"1Min", "1Sec"}, "AAPL")
	mf.fillOutage(outage{from: time.Now().Add(-time.Minute), to: time.Now().Add(-10 * time.Second)})
	require.Len(t, *calls, 2)
	assert.Equal(t, "1Sec", (*calls)[0].tf)
	assert.Equal(t, "1Min", (*calls)[1].tf)
}

func TestFillOutageAuthFailureStops(t *testing.T) {
	stubFetchBars(t, func(barsCall) (int, error) { return 0, api.ErrAuthFailed })
	mf := testFetcher([]string{"1Sec"}, "AAPL")
	mf.fillOutage(outage{from: time.Now().Add(-time.Minute), to: time.Now().Add(-10 * time.Second)})
	assert.Error(t, mf.ctx.Err(), "an auth failure must cancel the fetcher, as the full backfill does")
}

func TestFillOutageNoBarStreamsIsNoop(t *testing.T) {
	calls := stubFetchBars(t, func(barsCall) (int, error) { return 1, nil })
	mf := testFetcher([]string{"trades"}, "AAPL")
	mf.fillOutage(outage{from: time.Now().Add(-time.Minute), to: time.Now().Add(-10 * time.Second)})
	assert.Empty(t, *calls)
}

func TestRequestGapFill(t *testing.T) {
	t.Parallel()
	et := calendar.Nasdaq.Tz()
	reconnect := time.Date(2026, 9, 24, 11, 38, 14, 0, et)

	capture := func(mf *MassiveFetcher) *[]backfillRequest {
		var got []backfillRequest
		var wg sync.WaitGroup
		mf.backfills = newBackfillRunner(func(r backfillRequest) { got = append(got, r) }, &wg)
		t.Cleanup(wg.Wait)
		return &got
	}

	t.Run("short outage queues a window fill", func(t *testing.T) {
		mf := testFetcher([]string{"1Sec"}, "AAPL")
		got := capture(mf)
		mf.lastDataAt.Store(reconnect.Add(-40 * time.Second).UnixNano())
		mf.requestGapFill(reconnect)
		mf.backfills.wg.Wait()
		require.Len(t, *got, 1)
		assert.False(t, (*got)[0].full)
		require.NotNil(t, (*got)[0].gap)
	})
	t.Run("full backfill needs query_start", func(t *testing.T) {
		mf := testFetcher([]string{"1Sec"}, "AAPL")
		got := capture(mf)
		mf.requestGapFill(reconnect) // no data ever received -> full, but no query_start
		mf.backfills.wg.Wait()
		assert.Empty(t, *got)
	})
	t.Run("long outage with query_start queues the full backfill", func(t *testing.T) {
		mf := testFetcher([]string{"1Sec"}, "AAPL")
		mf.config.QueryStart = map[string]string{"1Min": "2025-06-01"}
		got := capture(mf)
		mf.lastDataAt.Store(reconnect.Add(-5 * time.Hour).UnixNano())
		mf.requestGapFill(reconnect)
		mf.backfills.wg.Wait()
		require.Len(t, *got, 1)
		assert.True(t, (*got)[0].full)
	})
}

// Messages already queued when the socket drops are complete data and must
// be written, not discarded with the connection.
func TestDrainOutputDispatchesQueuedMessages(t *testing.T) {
	t.Parallel()
	var hits int
	r := newMessageRouter([]streamTopic{
		{dataType: "1Sec", topic: ws.StocksSecAggs, handler: func(_ []byte) { hits++ }},
	})
	out := make(chan json.RawMessage, 10)
	for i := 0; i < 3; i++ {
		out <- json.RawMessage(`{"ev":"A","sym":"AAPL"}`)
	}
	mf := testFetcher(nil)
	mf.drainOutput(out, r)
	assert.Equal(t, 3, hits)
	assert.NotZero(t, mf.lastDataAt.Load())
	assert.Empty(t, out)
}

func stubLastTimestamp(t *testing.T, byKey map[string]time.Time) *[]string {
	t.Helper()
	var asked []string
	orig := lastTimestampOf
	lastTimestampOf = func(tbk *io.TimeBucketKey) time.Time {
		asked = append(asked, tbk.GetItemKey())
		return byKey[tbk.GetItemKey()]
	}
	t.Cleanup(func() { lastTimestampOf = orig })
	return &asked
}

// A deploy during market hours: the previous process streamed until 11:36:47
// ET and this one starts 13s later. The startup backfill must include a fill
// of that window, measured from the newest reference bar on disk.
func TestStartupBackfillFillsRestartOutage(t *testing.T) {
	et := calendar.Nasdaq.Tz()
	stopped := time.Date(2026, 9, 24, 11, 36, 47, 0, et)
	asked := stubLastTimestamp(t, map[string]time.Time{
		"SPY/1Sec/OHLCV":  stopped.Add(-2 * time.Second),
		"AAPL/1Sec/OHLCV": stopped, // newest wins
	})
	mf := testFetcher([]string{"1Sec", "trades"}, "AAPL", "SPY", "KOSS")
	mf.config.QueryStart = map[string]string{"1Min": "2025-06-01"}

	req := mf.startupBackfill(stopped.Add(13 * time.Second))

	assert.True(t, req.full, "the full startup backfill still runs")
	require.NotNil(t, req.gap)
	assert.Equal(t, time.Date(2026, 9, 24, 11, 35, 0, 0, et), req.gap.from)
	assert.Equal(t, stopped.Add(18*time.Second), req.gap.to)
	assert.ElementsMatch(t, []string{"SPY/1Sec/OHLCV", "AAPL/1Sec/OHLCV"}, *asked,
		"only configured reference symbols are consulted")
}

func TestStartupBackfillAfterOvernightIsFullOnly(t *testing.T) {
	et := calendar.Nasdaq.Tz()
	stubLastTimestamp(t, map[string]time.Time{"SPY/1Sec/OHLCV": time.Date(2026, 9, 23, 19, 59, 59, 0, et)})
	mf := testFetcher([]string{"1Sec"}, "SPY")
	mf.config.QueryStart = map[string]string{"1Min": "2025-06-01"}
	req := mf.startupBackfill(time.Date(2026, 9, 24, 4, 0, 0, 0, et))
	assert.True(t, req.full)
	assert.Nil(t, req.gap)
}

func TestStartupBackfillNoDataOnDisk(t *testing.T) {
	stubLastTimestamp(t, nil)
	mf := testFetcher([]string{"1Sec"}, "SPY")
	req := mf.startupBackfill(time.Now())
	assert.True(t, req.empty(), "no query_start and nothing streamed before: nothing to do")
}
