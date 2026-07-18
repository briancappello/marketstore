package main

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/subscription"
	"github.com/alpacahq/marketstore/v4/contrib/massive/ws"
)

// TestDataTypeToTopicAgreesWithWSDataTypeToTopic ensures the two topic mapping
// tables cannot drift for the tick data types.
func TestDataTypeToTopicAgreesWithWSDataTypeToTopic(t *testing.T) {
	t.Parallel()
	assert.Equal(t, wsDataTypeToTopic["trades"], dataTypeToTopic[subscription.Trades])
	assert.Equal(t, wsDataTypeToTopic["quotes"], dataTypeToTopic[subscription.Quotes])
	assert.Equal(t, wsDataTypeToTopic["trades"], topicFor(subscription.Trades))
	assert.Equal(t, wsDataTypeToTopic["quotes"], topicFor(subscription.Quotes))
}

// TestSubscriptionControllerLifecycle exercises the dynamic SubscriptionController
// implementation end-to-end at the manager level (no live WS connection).
func TestSubscriptionControllerLifecycle(t *testing.T) {
	t.Parallel()

	worker, err := NewBgWorker(map[string]interface{}{
		"api_key":             "test-key",
		"ws_data_types":       []string{"1Sec", "trades", "quotes"},
		"dynamic_ticks":       true,
		"max_dynamic_symbols": 2,
	})
	require.NoError(t, err)

	mf, ok := worker.(*MassiveFetcher)
	require.True(t, ok)

	// Subscribe AAPL for both tick types.
	require.NoError(t, mf.Subscribe("AAPL", []string{"trades", "quotes"}))
	active := mf.ActiveSubscriptions()
	assert.ElementsMatch(t, []string{"trades", "quotes"}, active["AAPL"])

	// Unknown data type errors.
	assert.Error(t, mf.Subscribe("MSFT", []string{"bogus"}))

	// Cap is per-DataType: AAPL + MSFT trades = 2 (ok), GOOG trades = 3 (over cap).
	require.NoError(t, mf.Subscribe("MSFT", []string{"trades"}))
	assert.Error(t, mf.Subscribe("GOOG", []string{"trades"}))

	// Unsubscribe AAPL trades; quotes remains.
	require.NoError(t, mf.Unsubscribe("AAPL", []string{"trades"}))
	active = mf.ActiveSubscriptions()
	assert.ElementsMatch(t, []string{"quotes"}, active["AAPL"])
}

// TestStaticModeNoSubscriptionManager verifies that without dynamic_ticks the
// manager is nil and the controller methods refuse.
func TestStaticModeNoSubscriptionManager(t *testing.T) {
	t.Parallel()

	worker, err := NewBgWorker(map[string]interface{}{
		"api_key":       "test-key",
		"ws_data_types": []string{"trades"},
		"symbols":       []string{"AAPL"},
	})
	require.NoError(t, err)

	mf, ok := worker.(*MassiveFetcher)
	require.True(t, ok)
	assert.Nil(t, mf.subMgr)
	assert.Error(t, mf.Subscribe("AAPL", []string{"trades"}))
}

func TestWSDataTypeToTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dataType string
		topic    ws.Topic
	}{
		{"1Min", ws.StocksMinAggs},
		{"1Sec", ws.StocksSecAggs},
		{"trades", ws.StocksTrades},
		{"quotes", ws.StocksQuotes},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.dataType, func(t *testing.T) {
			t.Parallel()
			topic, ok := wsDataTypeToTopic[tt.dataType]
			assert.True(t, ok, "expected %q to be in wsDataTypeToTopic", tt.dataType)
			assert.Equal(t, tt.topic, topic)
		})
	}

	// Verify no extra entries.
	assert.Len(t, wsDataTypeToTopic, 4)
}

func TestNewBgWorker_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError string
	}{
		{
			name: "symbols_dsn without symbols_query returns error",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"ws_data_types": []string{"1Min"},
				"symbols_dsn":   "postgres://localhost/test",
			},
			expectError: "symbols_query is required when symbols_dsn is set",
		},
		{
			name: "symbols_dsn with empty symbols_query returns error",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"ws_data_types": []string{"1Min"},
				"symbols_dsn":   "postgres://localhost/test",
				"symbols_query": "",
			},
			expectError: "symbols_query is required when symbols_dsn is set",
		},
		{
			name: "invalid dsn returns connection error",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"ws_data_types": []string{"1Min"},
				"symbols_dsn":   "postgres://invalid:5432/nonexistent?connect_timeout=1",
				"symbols_query": "SELECT id, symbol, listed FROM symbols",
			},
			expectError: "fetch symbols from database: connect to postgres:",
		},
		{
			name: "query_start with dsn but missing sync_queries returns error",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"symbols_dsn":   "postgres://localhost/test",
				"symbols_query": "SELECT id, ticker, listed FROM asset",
				"query_start":   map[string]interface{}{"1Min": "2024-01-01"},
			},
			expectError: "sync_queries entry required for query_start key",
		},
		{
			name: "static symbols without dsn works",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"ws_data_types": []string{"1Min"},
				"symbols":       []string{"AAPL", "MSFT"},
			},
			expectError: "",
		},
		{
			name: "no ws_data_types defaults to 1Min",
			config: map[string]interface{}{
				"api_key": "test-key",
				"symbols": []string{"AAPL"},
			},
			expectError: "",
		},
		{
			name: "invalid ws_data_type returns error",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"ws_data_types": []string{"invalid"},
				"symbols":       []string{"AAPL"},
			},
			expectError: "invalid ws_data_type",
		},
		{
			name: "valid ws_data_types",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"ws_data_types": []string{"1Min", "1Sec", "quotes", "trades"},
				"symbols":       []string{"AAPL"},
			},
			expectError: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			worker, err := NewBgWorker(tt.config)

			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
				assert.Nil(t, worker)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, worker)
			}
		})
	}
}

func TestIsUpToDate(t *testing.T) {
	t.Parallel()

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// Helper to build ET times concisely.
	et := func(year, month, day, hour, min, sec int) time.Time {
		return time.Date(year, time.Month(month), day, hour, min, sec, 0, ny)
	}

	// effectiveStart is only used when lastTS is zero (no data on disk).
	// For most tests it can be an arbitrary old date.
	oldStart := et(2024, 1, 1, 0, 0, 0)

	// A future effectiveStart (listing date hasn't happened yet).
	futureStart := et(2027, 6, 1, 0, 0, 0)

	zero := time.Time{} // no data on disk

	tests := []struct {
		name           string
		lastTS         time.Time
		effectiveStart time.Time
		end            time.Time
		dataType       string
		want           bool // true = up to date (skip backfill)
	}{
		// ---------------------------------------------------------------
		// A. Intraday bars: tolerance = timeframe duration
		// ---------------------------------------------------------------
		{
			name:           "1Min up to date",
			lastTS:         et(2026, 4, 2, 19, 59, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Min",
			want:           true,
		},
		{
			name:           "1Min one bar behind",
			lastTS:         et(2026, 4, 2, 19, 58, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Min",
			want:           false,
		},
		{
			name:           "5Min up to date",
			lastTS:         et(2026, 4, 2, 19, 55, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "5Min",
			want:           true,
		},
		{
			name:           "5Min one bar behind",
			lastTS:         et(2026, 4, 2, 19, 50, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "5Min",
			want:           false,
		},
		{
			name:           "1H up to date",
			lastTS:         et(2026, 4, 2, 19, 0, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1H",
			want:           true,
		},
		{
			name:           "1Sec up to date",
			lastTS:         et(2026, 4, 2, 19, 59, 59),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Sec",
			want:           true,
		},
		{
			name:           "1Min early close day with extended hours",
			lastTS:         et(2026, 11, 27, 16, 59, 0), // day after Thanksgiving, early close 13:00 + 4h = 17:00
			effectiveStart: oldStart,
			end:            et(2026, 11, 27, 17, 0, 0),
			dataType:       "1Min",
			want:           true,
		},
		{
			name:           "1Min no data on disk",
			lastTS:         zero,
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Min",
			want:           false,
		},

		// ---------------------------------------------------------------
		// B. Daily+ bars: calendar-date comparison in market TZ
		// ---------------------------------------------------------------
		{
			name:           "1D up to date same date",
			lastTS:         et(2026, 4, 2, 0, 0, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0),
			dataType:       "1D",
			want:           true,
		},
		{
			name:           "1D missing latest day",
			lastTS:         et(2026, 4, 1, 0, 0, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0),
			dataType:       "1D",
			want:           false,
		},
		{
			name:           "1D weekend restart (Fri holiday, data through Thu)",
			lastTS:         et(2026, 4, 2, 0, 0, 0), // Thursday
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0), // LatestMarketTimeRegular on Saturday walks back to Thursday
			dataType:       "1D",
			want:           true,
		},
		{
			name:           "1D early close day",
			lastTS:         et(2026, 11, 27, 0, 0, 0), // day after Thanksgiving
			effectiveStart: oldStart,
			end:            et(2026, 11, 27, 13, 0, 0), // early close at 13:00
			dataType:       "1D",
			want:           true,
		},
		{
			name:           "1D no data on disk",
			lastTS:         zero,
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0),
			dataType:       "1D",
			want:           false,
		},
		{
			name:           "1W same ISO week",
			lastTS:         et(2026, 3, 30, 0, 0, 0), // Monday of week 14
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0), // Thursday of same week
			dataType:       "1W",
			want:           true,
		},
		{
			name:           "1W previous week",
			lastTS:         et(2026, 3, 23, 0, 0, 0), // Monday of week 13
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0), // Thursday of week 14
			dataType:       "1W",
			want:           false,
		},
		{
			name:           "1M same month",
			lastTS:         et(2026, 4, 1, 0, 0, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0),
			dataType:       "1M",
			want:           true,
		},
		{
			name:           "1M previous month",
			lastTS:         et(2026, 3, 1, 0, 0, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0),
			dataType:       "1M",
			want:           false,
		},

		// ---------------------------------------------------------------
		// C. Tick data: tolerance = 1 minute
		// ---------------------------------------------------------------
		{
			name:           "trades within tolerance",
			lastTS:         et(2026, 4, 2, 19, 59, 30),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "trades",
			want:           true,
		},
		{
			name:           "trades outside tolerance",
			lastTS:         et(2026, 4, 2, 19, 58, 30),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "trades",
			want:           false,
		},
		{
			name:           "quotes within tolerance",
			lastTS:         et(2026, 4, 2, 19, 59, 45),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "quotes",
			want:           true,
		},
		{
			name:           "trades no data on disk",
			lastTS:         zero,
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "trades",
			want:           false,
		},

		// ---------------------------------------------------------------
		// D. Edge cases
		// ---------------------------------------------------------------
		{
			name:           "lastTS exactly equals end",
			lastTS:         et(2026, 4, 2, 20, 0, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Min",
			want:           true,
		},
		{
			name:           "lastTS after end",
			lastTS:         et(2026, 4, 2, 20, 1, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Min",
			want:           true,
		},
		{
			// A daily bar stored as 2026-04-02T04:00:00Z = 2026-04-02T00:00:00 ET.
			// The end is April 2 16:00 ET. Same date in ET → up to date.
			name:           "1D UTC epoch maps to same ET date",
			lastTS:         time.Date(2026, 4, 2, 4, 0, 0, 0, time.UTC),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0),
			dataType:       "1D",
			want:           true,
		},
		{
			// A daily bar stored as 2026-04-02T03:00:00Z = 2026-04-01T23:00:00 ET.
			// The end is April 2 16:00 ET. Different dates in ET → not up to date.
			name:           "1D UTC epoch maps to previous ET date",
			lastTS:         time.Date(2026, 4, 2, 3, 0, 0, 0, time.UTC),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 16, 0, 0),
			dataType:       "1D",
			want:           false,
		},
		{
			// Boundary: lastTS is exactly at end - tolerance for 1Min.
			// end - 1min = 19:59:00, lastTS = 19:59:00 → lastTS >= end - tolerance → up to date.
			name:           "1Min boundary exactly at tolerance",
			lastTS:         et(2026, 4, 2, 19, 59, 0),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Min",
			want:           true,
		},
		{
			// Boundary: lastTS is 1ns before the tolerance threshold.
			// end - 1min = 19:59:00, lastTS = 19:58:59.999999999 → not up to date.
			name:           "1Min boundary 1ns before tolerance",
			lastTS:         et(2026, 4, 2, 19, 59, 0).Add(-time.Nanosecond),
			effectiveStart: oldStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Min",
			want:           false,
		},
		{
			name:           "no data but effectiveStart is in the future (listing date)",
			lastTS:         zero,
			effectiveStart: futureStart,
			end:            et(2026, 4, 2, 20, 0, 0),
			dataType:       "1Min",
			want:           true, // effectiveStart > end → nothing to fetch
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := isUpToDate(tt.lastTS, tt.effectiveStart, tt.end, tt.dataType, ny)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsDailyOrLonger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tf       string
		expected bool
	}{
		{"1Sec", false},
		{"1Min", false},
		{"5Min", false},
		{"15Min", false},
		{"1H", false},
		{"4H", false},
		{"1D", true},
		{"1W", true},
		{"1M", true},
		{"1Y", true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.tf, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, isDailyOrLonger(tt.tf), "isDailyOrLonger(%q)", tt.tf)
		})
	}
}

// TestMessageRouterDispatch verifies that the single multiplexed stream routes
// each message to the handler matching its "ev" field, and ignores events for
// channels that are not configured.
func TestMessageRouterDispatch(t *testing.T) {
	t.Parallel()

	var got []string
	mk := func(label string) func([]byte) {
		return func(_ []byte) { got = append(got, label) }
	}

	topics := []streamTopic{
		{dataType: "1Sec", topic: ws.StocksSecAggs, handler: mk("A")},
		{dataType: "1Min", topic: ws.StocksMinAggs, handler: mk("AM")},
		{dataType: "trades", topic: ws.StocksTrades, handler: mk("T")},
		{dataType: "quotes", topic: ws.StocksQuotes, handler: mk("Q")},
	}
	r := newMessageRouter(topics)

	r.dispatch([]byte(`{"ev":"AM","sym":"AAPL"}`))
	r.dispatch([]byte(`{"ev":"T","sym":"AAPL"}`))
	r.dispatch([]byte(`{"ev":"Q","sym":"AAPL"}`))
	r.dispatch([]byte(`{"ev":"A","sym":"AAPL"}`))
	// Unconfigured / status event types are ignored.
	r.dispatch([]byte(`{"ev":"status","status":"success"}`))
	r.dispatch([]byte(`{"ev":"XYZ"}`))
	// Malformed JSON is ignored (no panic).
	r.dispatch([]byte(`not-json`))

	assert.Equal(t, []string{"AM", "T", "Q", "A"}, got)
}

// TestMessageRouterPartialTopics verifies that only configured channels route;
// an event for an unconfigured channel (e.g., quotes when only 1Min is enabled)
// is dropped.
func TestMessageRouterPartialTopics(t *testing.T) {
	t.Parallel()

	var hits int
	topics := []streamTopic{
		{dataType: "1Min", topic: ws.StocksMinAggs, handler: func(_ []byte) { hits++ }},
	}
	r := newMessageRouter(topics)

	r.dispatch([]byte(`{"ev":"AM","sym":"AAPL"}`)) // routed
	r.dispatch([]byte(`{"ev":"Q","sym":"AAPL"}`))  // dropped (not configured)
	r.dispatch([]byte(`{"ev":"T","sym":"AAPL"}`))  // dropped

	assert.Equal(t, 1, hits)
}

func TestErrConnectionLimit(t *testing.T) {
	t.Parallel()

	// ws.ErrConnectionLimit should be matchable with errors.Is through wrapping.
	err := fmt.Errorf("something went wrong: %w", ws.ErrConnectionLimit)
	assert.True(t, errors.Is(err, ws.ErrConnectionLimit))
}

func TestFlatFileAvailableThrough(t *testing.T) {
	t.Parallel()

	ny := calendar.Nasdaq.Tz()

	// Helper: midnight UTC date for comparison.
	utcDate := func(year, month, day int) time.Time {
		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	}

	tests := []struct {
		name string
		now  time.Time
		want time.Time
	}{
		{
			name: "before noon ET: available through day-before-yesterday",
			now:  time.Date(2026, 4, 24, 6, 0, 0, 0, ny), // Fri 6 AM ET
			want: utcDate(2026, 4, 22),                   // Wednesday
		},
		{
			name: "at 11:59 AM ET: still before cutoff",
			now:  time.Date(2026, 4, 24, 11, 59, 0, 0, ny),
			want: utcDate(2026, 4, 22),
		},
		{
			name: "at noon ET: yesterday is available",
			now:  time.Date(2026, 4, 24, 12, 0, 0, 0, ny), // Fri noon ET
			want: utcDate(2026, 4, 23),                    // Thursday
		},
		{
			name: "after noon ET: yesterday is available",
			now:  time.Date(2026, 4, 24, 15, 0, 0, 0, ny), // Fri 3 PM ET
			want: utcDate(2026, 4, 23),                    // Thursday
		},
		{
			name: "Saturday after noon: Friday is available",
			now:  time.Date(2026, 4, 25, 13, 0, 0, 0, ny), // Sat 1 PM ET
			want: utcDate(2026, 4, 24),                    // Friday
		},
		{
			name: "Saturday before noon: Thursday is available",
			now:  time.Date(2026, 4, 25, 8, 0, 0, 0, ny), // Sat 8 AM ET
			want: utcDate(2026, 4, 23),                   // Thursday
		},
		{
			name: "Sunday after noon: Saturday (flat file dates are calendar, not market)",
			now:  time.Date(2026, 4, 26, 14, 0, 0, 0, ny), // Sun 2 PM ET
			want: utcDate(2026, 4, 25),                    // Saturday
		},
		{
			name: "midnight ET: before noon, 2 days back",
			now:  time.Date(2026, 4, 24, 0, 0, 0, 0, ny),
			want: utcDate(2026, 4, 22),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := flatFileAvailableThrough(tt.now)
			assert.Equal(t, tt.want, got, "flatFileAvailableThrough(%s)", tt.now.Format(time.RFC3339))
		})
	}
}

func TestNextPreMarketOpen(t *testing.T) {
	t.Parallel()

	ny := calendar.Nasdaq.Tz()

	tests := []struct {
		name     string
		now      time.Time
		expected time.Time
	}{
		{
			name:     "before 3:58 AM on a trading day returns today",
			now:      time.Date(2026, 4, 7, 2, 0, 0, 0, ny),  // Tuesday 2:00 AM
			expected: time.Date(2026, 4, 7, 3, 58, 0, 0, ny), // Tuesday 3:58 AM
		},
		{
			name:     "at 3:57 AM on a trading day returns today",
			now:      time.Date(2026, 4, 7, 3, 57, 0, 0, ny), // Tuesday 3:57 AM
			expected: time.Date(2026, 4, 7, 3, 58, 0, 0, ny), // Tuesday 3:58 AM
		},
		{
			name:     "at 3:58 AM on a trading day returns next trading day",
			now:      time.Date(2026, 4, 7, 3, 58, 0, 0, ny), // Tuesday 3:58 AM (not Before)
			expected: time.Date(2026, 4, 8, 3, 58, 0, 0, ny), // Wednesday 3:58 AM
		},
		{
			name:     "after market hours on a trading day returns next trading day",
			now:      time.Date(2026, 4, 7, 21, 0, 0, 0, ny), // Tuesday 9 PM
			expected: time.Date(2026, 4, 8, 3, 58, 0, 0, ny), // Wednesday 3:58 AM
		},
		{
			name:     "Friday after hours returns Monday",
			now:      time.Date(2026, 4, 10, 21, 0, 0, 0, ny), // Friday 9 PM
			expected: time.Date(2026, 4, 13, 3, 58, 0, 0, ny), // Monday 3:58 AM
		},
		{
			name:     "Saturday returns Monday",
			now:      time.Date(2026, 4, 11, 12, 0, 0, 0, ny), // Saturday noon
			expected: time.Date(2026, 4, 13, 3, 58, 0, 0, ny), // Monday 3:58 AM
		},
		{
			name:     "Sunday returns Monday",
			now:      time.Date(2026, 4, 12, 12, 0, 0, 0, ny), // Sunday noon
			expected: time.Date(2026, 4, 13, 3, 58, 0, 0, ny), // Monday 3:58 AM
		},
		{
			name:     "Sunday early morning returns Monday",
			now:      time.Date(2026, 4, 12, 1, 0, 0, 0, ny),  // Sunday 1 AM (before 3:58 but not a market day)
			expected: time.Date(2026, 4, 13, 3, 58, 0, 0, ny), // Monday 3:58 AM
		},
		{
			name:     "before holiday returns day after holiday",
			now:      time.Date(2026, 7, 2, 21, 0, 0, 0, ny), // Thursday July 2 evening
			expected: time.Date(2026, 7, 6, 3, 58, 0, 0, ny), // Monday July 6 (July 3 = holiday, 4-5 = weekend)
		},
		{
			name:     "early morning on a holiday returns next trading day",
			now:      time.Date(2026, 7, 3, 2, 0, 0, 0, ny),  // July 3 (holiday) 2 AM
			expected: time.Date(2026, 7, 6, 3, 58, 0, 0, ny), // Monday July 6
		},
		{
			name:     "early morning on early-close day returns today (still a market day)",
			now:      time.Date(2026, 11, 27, 2, 0, 0, 0, ny),  // Day after Thanksgiving 2 AM (early close)
			expected: time.Date(2026, 11, 27, 3, 58, 0, 0, ny), // Same day 3:58 AM
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := nextPreMarketOpen(tt.now)
			assert.Equal(t, tt.expected, result)
		})
	}
}
