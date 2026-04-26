package framework_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack"

	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// ---------------------------------------------------------------------------
// Mock Curator and WatchlistStrategy
// ---------------------------------------------------------------------------

// mockCurator is a test Curator that curates symbols in its allowed map.
type mockCurator struct {
	allowed map[string]bool
}

func (m *mockCurator) Init(states map[string]*framework.SymbolState) {}

func (m *mockCurator) Evaluate(symbol string, state *framework.SymbolState) bool {
	if m.allowed == nil {
		return true
	}
	return m.allowed[symbol]
}

// dynamicCurator curates based on a threshold on the symbol's state.
type dynamicCurator struct {
	minDollarVolRate float64
	minPrice         float64
}

func (d *dynamicCurator) Init(states map[string]*framework.SymbolState) {}

func (d *dynamicCurator) Evaluate(symbol string, state *framework.SymbolState) bool {
	return state.LastPrice >= d.minPrice && state.DollarVolumeRate >= d.minDollarVolRate
}

// mockWatchlist is a test WatchlistStrategy that returns a static ranking.
type mockWatchlist struct {
	name   string
	rankFn func(map[string]*framework.SymbolState) []framework.RankedSymbol
}

func (m *mockWatchlist) Name() string                                  { return m.name }
func (m *mockWatchlist) Configure(config map[string]interface{}) error { return nil }
func (m *mockWatchlist) Rank(curated map[string]*framework.SymbolState) []framework.RankedSymbol {
	if m.rankFn != nil {
		return m.rankFn(curated)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

type testHarness struct {
	t        *testing.T
	trigger  *framework.WatchlistTrigger
	worker   *framework.WatchlistWorker
	wsServer *httptest.Server
	wsURL    string
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()

	rootDir := t.TempDir()
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	cfg.Timezone, _ = time.LoadLocation("America/New_York")
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	stream.Initialize()

	// Reset the global framework state.
	framework.ResetRegistry()
	framework.Manager = framework.NewSymbolStateManager()

	// Create trigger with minimal config.
	trig, err := framework.NewTrigger(map[string]interface{}{})
	require.NoError(t, err)

	// Create worker.
	worker, err := framework.NewBgWorker(map[string]interface{}{
		"ranking_interval_ms": 100000, // very long -- we trigger manually
	})
	require.NoError(t, err)

	wsServer := httptest.NewServer(http.HandlerFunc(stream.Handler))
	u, _ := url.Parse(wsServer.URL + "/ws")
	u.Scheme = "ws"

	h := &testHarness{
		t:        t,
		trigger:  trig.(*framework.WatchlistTrigger),
		worker:   worker.(*framework.WatchlistWorker),
		wsServer: wsServer,
		wsURL:    u.String(),
	}

	t.Cleanup(func() {
		stream.Shutdown()
		wsServer.Close()
		framework.Manager = nil
	})

	return h
}

// writeOHLCVAndFire writes a single 1Min OHLCV bar to disk and fires the trigger.
func (h *testHarness) writeOHLCVAndFire(symbol string, epoch time.Time, o, hi, lo, cl float32, vol int64) {
	h.t.Helper()

	tbk := io.NewTimeBucketKey(symbol + "/1Min/OHLCV")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch.Unix()})
	cs.AddColumn("Open", []float32{o})
	cs.AddColumn("High", []float32{hi})
	cs.AddColumn("Low", []float32{lo})
	cs.AddColumn("Close", []float32{cl})
	cs.AddColumn("Volume", []int64{vol})

	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)
	err := executor.WriteCSM(csm, false)
	require.NoError(h.t, err)

	// Build the keyPath and records to simulate what the WAL dispatch does.
	// We use the query-from-disk approach, so we just need a valid keyPath
	// with a Record that has the correct index.
	yearStr := epoch.Format("2006")
	keyPath := symbol + "/1Min/OHLCV/" + yearStr + ".bin"

	// Build a minimal record with the correct index.
	idx := io.TimeToIndex(epoch, time.Minute)
	idxBytes, _ := io.Serialize(nil, idx)
	// Pad with enough payload bytes for the OHLCV columns.
	// Open(4) + High(4) + Low(4) + Close(4) + Volume(8) = 24 bytes
	payload := make([]byte, 24)
	record := trigger.Record(append(idxBytes, payload...))

	h.trigger.Fire(keyPath, []trigger.Record{record})
}

// connectAndSubscribe creates a WS client and subscribes.
func (h *testHarness) connectAndSubscribe(keys ...string) (*websocket.Conn, chan map[string]interface{}) {
	h.t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial(h.wsURL, nil)
	require.NoError(h.t, err)

	buf, err := msgpack.Marshal(stream.SubscribeMessage{Action: "subscribe", TBKs: keys})
	require.NoError(h.t, err)
	require.NoError(h.t, conn.WriteMessage(websocket.BinaryMessage, buf))

	// Read ack.
	_, buf, err = conn.ReadMessage()
	require.NoError(h.t, err)
	var ack stream.SubscribedMessage
	require.NoError(h.t, msgpack.Unmarshal(buf, &ack))
	assert.Equal(h.t, "subscribed", ack.Action)

	msgCh := make(chan map[string]interface{}, 100)
	go func() {
		for {
			_, buf, err := conn.ReadMessage()
			if err != nil {
				close(msgCh)
				return
			}
			var payload stream.Payload
			if err := msgpack.Unmarshal(buf, &payload); err != nil {
				continue
			}
			// The Data field is a map[string]interface{} (our envelope).
			if data, ok := payload.Data.(map[string]interface{}); ok {
				data["_key"] = payload.Key
				msgCh <- data
			}
		}
	}()

	return conn, msgCh
}

// collectMessages drains the channel for the given duration.
func collectMessages(ch chan map[string]interface{}, timeout time.Duration) []map[string]interface{} {
	var msgs []map[string]interface{}
	timer := time.NewTimer(timeout)
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return msgs
			}
			msgs = append(msgs, msg)
		case <-timer.C:
			return msgs
		}
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Test 1: Curated symbol broadcasts to wildcard subscribers.
func TestCuratedBroadcastToWildcard(t *testing.T) {
	h := newTestHarness(t)

	// Set curator: AAPL is curated.
	framework.Manager.SetCurator(&mockCurator{allowed: map[string]bool{"AAPL": true}})

	conn, ch := h.connectAndSubscribe("*/1Min/OHLCV")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	epoch := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	h.writeOHLCVAndFire("AAPL", epoch, 150, 151, 149, 150.5, 100000)

	msgs := collectMessages(ch, 500*time.Millisecond)
	require.Equal(t, 1, len(msgs))
	assert.Equal(t, "AAPL/1Min/OHLCV", msgs[0]["_key"])
	assert.Equal(t, "bar", msgs[0]["msg_type"])
}

// Test 2: Non-curated symbol does NOT broadcast to wildcard subscribers.
func TestNonCuratedNoWildcard(t *testing.T) {
	h := newTestHarness(t)

	// Set curator: only AAPL is curated, PENNY is not.
	framework.Manager.SetCurator(&mockCurator{allowed: map[string]bool{"AAPL": true}})

	conn, ch := h.connectAndSubscribe("*/1Min/OHLCV")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	epoch := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	h.writeOHLCVAndFire("PENNY", epoch, 0.05, 0.06, 0.04, 0.055, 1000)

	msgs := collectMessages(ch, 300*time.Millisecond)
	assert.Equal(t, 0, len(msgs), "wildcard subscriber should NOT receive non-curated symbol")
}

// Test 3: Non-curated symbol DOES deliver to direct subscribers.
func TestNonCuratedDirectSubscriber(t *testing.T) {
	h := newTestHarness(t)

	framework.Manager.SetCurator(&mockCurator{allowed: map[string]bool{"AAPL": true}})

	connWild, chWild := h.connectAndSubscribe("*/1Min/OHLCV")
	defer connWild.Close()

	connDirect, chDirect := h.connectAndSubscribe("PENNY/1Min/OHLCV")
	defer connDirect.Close()
	time.Sleep(50 * time.Millisecond)

	epoch := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	h.writeOHLCVAndFire("PENNY", epoch, 0.05, 0.06, 0.04, 0.055, 1000)

	// Direct subscriber should receive it.
	msgsDirect := collectMessages(chDirect, 500*time.Millisecond)
	require.Equal(t, 1, len(msgsDirect))
	assert.Equal(t, "PENNY/1Min/OHLCV", msgsDirect[0]["_key"])

	// Wildcard subscriber should NOT.
	msgsWild := collectMessages(chWild, 200*time.Millisecond)
	assert.Equal(t, 0, len(msgsWild))
}

// Test 4: Direct subscriber for curated symbol also receives.
func TestCuratedDirectAlsoReceives(t *testing.T) {
	h := newTestHarness(t)

	framework.Manager.SetCurator(&mockCurator{allowed: map[string]bool{"AAPL": true}})

	connWild, chWild := h.connectAndSubscribe("*/1Min/OHLCV")
	defer connWild.Close()

	connDirect, chDirect := h.connectAndSubscribe("AAPL/1Min/OHLCV")
	defer connDirect.Close()
	time.Sleep(50 * time.Millisecond)

	epoch := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	h.writeOHLCVAndFire("AAPL", epoch, 150, 151, 149, 150.5, 100000)

	msgsWild := collectMessages(chWild, 500*time.Millisecond)
	require.Equal(t, 1, len(msgsWild))

	msgsDirect := collectMessages(chDirect, 500*time.Millisecond)
	require.Equal(t, 1, len(msgsDirect))
}

// Test 5: Dynamic curation -- symbol wakes up when volume increases.
func TestDynamicCurationWakeUp(t *testing.T) {
	h := newTestHarness(t)

	// Dynamic curator: needs price >= $1 and dollar vol rate >= $100/sec.
	framework.Manager.SetCurator(&dynamicCurator{minPrice: 1.0, minDollarVolRate: 100})

	conn, ch := h.connectAndSubscribe("*/1Min/OHLCV")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	baseTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	// First tick: low volume, price is fine but dollar vol rate will be low.
	h.writeOHLCVAndFire("SMCI", baseTime, 50, 51, 49, 50, 10)
	msgs1 := collectMessages(ch, 200*time.Millisecond)
	assert.Equal(t, 0, len(msgs1), "low volume should not be curated")

	// Second tick: high volume surge.
	h.writeOHLCVAndFire("SMCI", baseTime.Add(time.Minute), 50, 52, 49, 51, 1000000)
	msgs2 := collectMessages(ch, 500*time.Millisecond)
	assert.Equal(t, 1, len(msgs2), "volume surge should trigger curation")
}

// Test 7: Watchlist ranking updates.
func TestWatchlistRanking(t *testing.T) {
	h := newTestHarness(t)

	// Use noop curator (all curated).
	framework.Manager.SetCurator(&mockCurator{allowed: nil})

	// Register a simple watchlist that ranks by PctChange descending.
	framework.Manager.AddStrategy(&mockWatchlist{
		name: "TEST_GAINERS",
		rankFn: func(curated map[string]*framework.SymbolState) []framework.RankedSymbol {
			type e struct {
				sym string
				pct float64
			}
			var entries []e
			for sym, state := range curated {
				if state.PctChange > 0 {
					entries = append(entries, e{sym, state.PctChange})
				}
			}
			// Sort manually.
			for i := 0; i < len(entries); i++ {
				for j := i + 1; j < len(entries); j++ {
					if entries[j].pct > entries[i].pct {
						entries[i], entries[j] = entries[j], entries[i]
					}
				}
			}
			result := make([]framework.RankedSymbol, len(entries))
			for i, e := range entries {
				result[i] = framework.RankedSymbol{
					Symbol: e.sym,
					Fields: map[string]interface{}{"pct_change": e.pct},
				}
			}
			return result
		},
	})

	conn, ch := h.connectAndSubscribe("WATCHLISTS/1Min/*")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	baseTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	// Set prior closes so PctChange is computed.
	aaplState := framework.Manager.GetOrCreate("AAPL")
	aaplState.PriorClose = 100
	nvdaState := framework.Manager.GetOrCreate("NVDA")
	nvdaState.PriorClose = 200

	// Write bars that produce known percent changes.
	// AAPL: high=105, prior_close=100 -> +5%
	h.writeOHLCVAndFire("AAPL", baseTime, 101, 105, 100, 104, 50000)
	// NVDA: high=208, prior_close=200 -> +4%
	h.writeOHLCVAndFire("NVDA", baseTime, 201, 208, 199, 206, 30000)

	// Trigger ranking manually.
	h.worker.TriggerRanking()

	msgs := collectMessages(ch, 500*time.Millisecond)
	// We should get at least 1 watchlist update.
	found := false
	for _, msg := range msgs {
		if msg["msg_type"] == "watchlist_update" {
			found = true
			payload, _ := msg["payload"].(map[string]interface{})
			assert.Equal(t, "TEST_GAINERS", payload["name"])
			symbols, _ := payload["symbols"].([]interface{})
			if len(symbols) >= 2 {
				first, _ := symbols[0].(map[string]interface{})
				assert.Equal(t, "AAPL", first["symbol"])
			}
		}
	}
	assert.True(t, found, "should receive at least one watchlist_update message")
}

// Test 10: PushDirect with wildcard in non-symbol position.
func TestPushDirectPartialWildcard(t *testing.T) {
	h := newTestHarness(t)

	framework.Manager.SetCurator(&mockCurator{allowed: map[string]bool{"AAPL": true}})

	// Subscribe with PENNY/1Min/* -- wildcard in attrgroup, symbol is concrete.
	conn, ch := h.connectAndSubscribe("PENNY/1Min/*")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	epoch := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	h.writeOHLCVAndFire("PENNY", epoch, 0.05, 0.06, 0.04, 0.055, 1000)

	// Should receive because the symbol position is concrete.
	msgs := collectMessages(ch, 500*time.Millisecond)
	require.Equal(t, 1, len(msgs))
	assert.Equal(t, "PENNY/1Min/OHLCV", msgs[0]["_key"])
}

// Test 11: Message format consistency.
func TestMessageFormatEnvelope(t *testing.T) {
	h := newTestHarness(t)

	framework.Manager.SetCurator(&mockCurator{allowed: nil}) // all curated

	conn, ch := h.connectAndSubscribe("*/1Min/OHLCV")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	epoch := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	h.writeOHLCVAndFire("AAPL", epoch, 150, 151, 149, 150.5, 100000)

	msgs := collectMessages(ch, 500*time.Millisecond)
	require.Equal(t, 1, len(msgs))

	// Every message must have msg_type and payload.
	msg := msgs[0]
	assert.Contains(t, msg, "msg_type")
	assert.Contains(t, msg, "payload")
	assert.Equal(t, "bar", msg["msg_type"])

	payload, ok := msg["payload"].(map[string]interface{})
	require.True(t, ok)
	assert.Contains(t, payload, "symbol")
	assert.Equal(t, "AAPL", payload["symbol"])
}

// Test: SymbolState running state accumulates correctly.
func TestSymbolStateAccumulation(t *testing.T) {
	h := newTestHarness(t)

	framework.Manager.SetCurator(&mockCurator{allowed: nil}) // all curated

	// Set prior close for derived metrics.
	state := framework.Manager.GetOrCreate("TEST")
	state.PriorClose = 100

	conn, _ := h.connectAndSubscribe("TEST/1Min/OHLCV")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	baseTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	// Tick 1: open=101, high=103, low=100, close=102, vol=1000
	h.writeOHLCVAndFire("TEST", baseTime, 101, 103, 100, 102, 1000)

	assert.Equal(t, float64(101), state.DayOpen)
	assert.Equal(t, float64(103), state.HighOfDay)
	assert.InDelta(t, 100, state.LowOfDay, 0.01)
	assert.Equal(t, int64(1000), state.CumulativeVolume)
	assert.InDelta(t, 2.0, state.PctChange, 0.01) // (close 102 - prior 100)/100 * 100

	// Tick 2: higher high, more volume.
	h.writeOHLCVAndFire("TEST", baseTime.Add(time.Minute), 102, 106, 101, 105, 2000)

	assert.Equal(t, float64(101), state.DayOpen) // should not change
	assert.Equal(t, float64(106), state.HighOfDay)
	assert.Equal(t, int64(3000), state.CumulativeVolume) // accumulated
	assert.InDelta(t, 5.0, state.PctChange, 0.01)        // (close 105 - prior 100)/100 * 100
}

// Test: Curation change detection.
func TestCurationChangeDetection(t *testing.T) {
	h := newTestHarness(t)

	// Dynamic curator.
	framework.Manager.SetCurator(&dynamicCurator{minPrice: 1.0, minDollarVolRate: 0})

	conn, ch := h.connectAndSubscribe("CURATION/1Min/CHANGES")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	baseTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	// Write a tick for AAPL at $150 -- will be curated.
	h.writeOHLCVAndFire("AAPL", baseTime, 150, 151, 149, 150, 100000)

	// Trigger ranking to detect the change.
	h.worker.TriggerRanking()

	msgs := collectMessages(ch, 500*time.Millisecond)
	found := false
	for _, msg := range msgs {
		if msg["msg_type"] == "curation_change" {
			found = true
			payload, _ := msg["payload"].(map[string]interface{})
			added, _ := payload["added"].([]interface{})
			assert.True(t, len(added) > 0, "AAPL should be in added list")

			// Check that AAPL is in the added list.
			foundAAPL := false
			for _, entry := range added {
				if e, ok := entry.(map[string]interface{}); ok {
					if e["symbol"] == "AAPL" {
						foundAAPL = true
					}
				}
			}
			assert.True(t, foundAAPL, "AAPL should be in added entries")
		}
	}
	assert.True(t, found, "should receive a curation_change message")
}

// Test: Negative PctChange is computed correctly when price drops below PriorClose.
func TestNegativePctChange(t *testing.T) {
	h := newTestHarness(t)

	framework.Manager.SetCurator(&mockCurator{allowed: nil}) // all curated

	state := framework.Manager.GetOrCreate("LOSER")
	state.PriorClose = 100

	conn, _ := h.connectAndSubscribe("LOSER/1Min/OHLCV")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	baseTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	// Price drops: close=90, well below PriorClose=100
	h.writeOHLCVAndFire("LOSER", baseTime, 98, 99, 88, 90, 5000)

	assert.InDelta(t, -10.0, state.PctChange, 0.01) // (90 - 100) / 100 * 100
	assert.Equal(t, 90.0, state.LastPrice)

	// Second tick: price drops further
	h.writeOHLCVAndFire("LOSER", baseTime.Add(time.Minute), 89, 91, 80, 82, 3000)

	assert.InDelta(t, -18.0, state.PctChange, 0.01) // (82 - 100) / 100 * 100
}

// Test: Day-boundary reset clears running state and updates PriorClose.
func TestDayBoundaryReset(t *testing.T) {
	h := newTestHarness(t)

	framework.Manager.SetCurator(&mockCurator{allowed: nil})

	state := framework.Manager.GetOrCreate("DAYRESET")
	state.PriorClose = 100
	state.MedianVolume50D = 50000

	conn, _ := h.connectAndSubscribe("DAYRESET/1Min/OHLCV")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	day1 := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	// Day 1: two ticks
	h.writeOHLCVAndFire("DAYRESET", day1, 101, 105, 100, 104, 10000)
	h.writeOHLCVAndFire("DAYRESET", day1.Add(time.Minute), 104, 108, 103, 107, 20000)

	assert.Equal(t, 101.0, state.DayOpen)
	assert.Equal(t, 108.0, state.HighOfDay)
	assert.Equal(t, int64(30000), state.CumulativeVolume)
	assert.InDelta(t, 7.0, state.PctChange, 0.01) // (107 - 100) / 100 * 100
	assert.Equal(t, int64(2), state.TickCount)

	// Day 2: first tick on a new calendar day triggers ResetDaily.
	day2 := time.Date(2025, 1, 16, 9, 30, 0, 0, time.UTC)
	h.writeOHLCVAndFire("DAYRESET", day2, 110, 112, 109, 111, 5000)

	// PriorClose should now be the last close from day 1 (107), not the
	// original PriorClose (100).
	assert.Equal(t, 107.0, state.PriorClose)

	// Running state should be reset for the new day.
	assert.Equal(t, 110.0, state.DayOpen)
	assert.Equal(t, 112.0, state.HighOfDay)
	assert.Equal(t, 109.0, state.LowOfDay)
	assert.Equal(t, int64(5000), state.CumulativeVolume)
	assert.Equal(t, int64(1), state.TickCount)

	// PctChange should use the new PriorClose (107).
	// (111 - 107) / 107 * 100 ≈ 3.74
	assert.InDelta(t, 3.738, state.PctChange, 0.01)
}

// Test: Curation removal is detected when a symbol drops below thresholds.
func TestCurationRemoval(t *testing.T) {
	h := newTestHarness(t)

	// Curator requires price >= 5.0
	framework.Manager.SetCurator(&dynamicCurator{minPrice: 5.0, minDollarVolRate: 0})

	conn, ch := h.connectAndSubscribe("CURATION/1Min/CHANGES")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	baseTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

	// Tick 1: price $10 -- passes curation.
	h.writeOHLCVAndFire("DROPPER", baseTime, 10, 11, 9, 10, 100000)
	h.worker.TriggerRanking()

	// Consume the "added" curation change.
	msgs := collectMessages(ch, 500*time.Millisecond)
	addedFound := false
	for _, msg := range msgs {
		if msg["msg_type"] == "curation_change" {
			addedFound = true
		}
	}
	assert.True(t, addedFound, "DROPPER should be added to curation")
	assert.True(t, framework.Manager.IsCurated("DROPPER"))

	// Tick 2: price drops to $2 -- below minPrice=5.0, should be removed.
	h.writeOHLCVAndFire("DROPPER", baseTime.Add(time.Minute), 3, 4, 1, 2, 50000)
	h.worker.TriggerRanking()

	msgs2 := collectMessages(ch, 500*time.Millisecond)
	removedFound := false
	for _, msg := range msgs2 {
		if msg["msg_type"] == "curation_change" {
			payload, _ := msg["payload"].(map[string]interface{})
			removed, _ := payload["removed"].([]interface{})
			for _, entry := range removed {
				if e, ok := entry.(map[string]interface{}); ok {
					if e["symbol"] == "DROPPER" {
						removedFound = true
					}
				}
			}
		}
	}
	assert.True(t, removedFound, "DROPPER should be in removed list")
	assert.False(t, framework.Manager.IsCurated("DROPPER"))
}

// Test: Key naming for watchlist updates follows the WATCHLISTS/TimeFrame/NAME convention.
func TestWatchlistKeyNaming(t *testing.T) {
	h := newTestHarness(t)

	framework.Manager.SetCurator(&mockCurator{allowed: nil})

	framework.Manager.AddStrategy(&mockWatchlist{
		name: "MY_LIST",
		rankFn: func(curated map[string]*framework.SymbolState) []framework.RankedSymbol {
			return []framework.RankedSymbol{{Symbol: "TEST", Fields: map[string]interface{}{}}}
		},
	})

	conn, ch := h.connectAndSubscribe("WATCHLISTS/1Min/MY_LIST")
	defer conn.Close()
	time.Sleep(50 * time.Millisecond)

	// Write a tick to populate state.
	baseTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	h.writeOHLCVAndFire("TEST", baseTime, 10, 11, 9, 10.5, 5000)

	h.worker.TriggerRanking()

	msgs := collectMessages(ch, 500*time.Millisecond)
	found := false
	for _, msg := range msgs {
		key := msg["_key"]
		if keyStr, ok := key.(string); ok && strings.HasPrefix(keyStr, "WATCHLISTS/") {
			found = true
			assert.Equal(t, "WATCHLISTS/1Min/MY_LIST", keyStr)
		}
	}
	assert.True(t, found, "should receive watchlist update with correct key naming")
}
