package main

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

func TestIsLocalHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		server string
		want   bool
	}{
		{"localhost ws", "ws://localhost:8765", true},
		{"localhost wss", "wss://localhost:8765", true},
		{"127.0.0.1 ws", "ws://127.0.0.1:8765", true},
		{"127.0.0.1 wss", "wss://127.0.0.1:8765", true},
		{"localhost no port", "ws://localhost", true},
		{"remote host", "wss://socket.massive.com", false},
		{"other IP", "ws://192.168.1.1:8765", false},
		{"empty string", "", false},
		{"invalid URL", "://bad", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isLocalHost(tt.server))
		})
	}
}

func TestBuildSubScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		prefix   Prefix
		symbols  []string
		expected string
	}{
		{"single symbol", PrefixAggMinute, []string{"AAPL"}, "AM.AAPL"},
		{"multiple symbols", PrefixTrade, []string{"AAPL", "MSFT"}, "T.AAPL,T.MSFT"},
		{"wildcard", PrefixQuote, []string{"*"}, "Q.*"},
		{"nil defaults to wildcard", PrefixAggMinute, nil, "AM.*"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, buildSubScope(tt.prefix, tt.symbols))
		})
	}
}

func TestConnectURLPath(t *testing.T) {
	t.Parallel()

	// Test the URL path logic used in connect(). We can't call connect()
	// directly without a real WebSocket server, but we can verify the URL
	// construction logic.
	tests := []struct {
		name         string
		server       string
		expectedPath string
	}{
		{"no path appends default", "wss://socket.massive.com", "/stocks"},
		{"root path appends default", "wss://socket.massive.com/", "/stocks"},
		{"custom path preserved", "wss://delayed.massive.com/stocks", "/stocks"},
		{"different custom path", "wss://custom.massive.com/custom/path", "/custom/path"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			u, err := url.Parse(tt.server)
			assert.Nil(t, err)

			// Mirror the logic from connect()
			if u.Path == "" || u.Path == "/" {
				u.Path = defaultWSPath
			}

			assert.Equal(t, tt.expectedPath, u.Path)
		})
	}
}

func TestNewBgWorker_SymbolsDSNValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      map[string]interface{}
		expectError string
	}{
		{
			name: "symbols_dsn without symbols_query returns error",
			config: map[string]interface{}{
				"api_key":     "test-key",
				"data_types":  []string{"bars"},
				"symbols_dsn": "postgres://localhost/test",
			},
			expectError: "symbols_query is required when symbols_dsn is set",
		},
		{
			name: "symbols_dsn with empty symbols_query returns error",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"data_types":    []string{"bars"},
				"symbols_dsn":   "postgres://localhost/test",
				"symbols_query": "",
			},
			expectError: "symbols_query is required when symbols_dsn is set",
		},
		{
			name: "invalid dsn returns connection error",
			config: map[string]interface{}{
				"api_key":       "test-key",
				"data_types":    []string{"bars"},
				"symbols_dsn":   "postgres://invalid:5432/nonexistent?connect_timeout=1",
				"symbols_query": "SELECT symbol FROM symbols",
			},
			expectError: "fetch symbols from database: connect to postgres:",
		},
		{
			name: "static symbols without dsn works",
			config: map[string]interface{}{
				"api_key":    "test-key",
				"data_types": []string{"bars"},
				"symbols":    []string{"AAPL", "MSFT"},
			},
			expectError: "",
		},
		{
			name: "no data_types returns error",
			config: map[string]interface{}{
				"api_key": "test-key",
				"symbols": []string{"AAPL"},
			},
			expectError: "at least one valid data_type is required",
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

func TestConfigStartNeedsBackfill(t *testing.T) {
	t.Parallel()

	ny := calendar.Nasdaq.Tz()

	tests := []struct {
		name          string
		configStart   time.Time
		firstTS       time.Time
		dailyOrLonger bool
		expected      bool
	}{
		{
			name:          "no existing data returns false",
			configStart:   time.Date(2025, 1, 1, 0, 0, 0, 0, ny),
			firstTS:       time.Time{}, // zero time
			dailyOrLonger: false,
			expected:      false,
		},
		{
			name:          "intraday: holiday config start with first market day data is up-to-date",
			configStart:   time.Date(2025, 1, 1, 0, 0, 0, 0, ny),  // New Year's Day (holiday)
			firstTS:       time.Date(2025, 1, 2, 9, 30, 0, 0, ny), // First bar on Jan 2
			dailyOrLonger: false,
			expected:      false, // Should NOT need earlier data
		},
		{
			name:          "intraday: config start before first data needs backfill",
			configStart:   time.Date(2025, 1, 2, 0, 0, 0, 0, ny),  // Jan 2
			firstTS:       time.Date(2025, 1, 3, 9, 30, 0, 0, ny), // First bar on Jan 3
			dailyOrLonger: false,
			expected:      true, // Missing Jan 2 data
		},
		{
			name:          "intraday: same market day is up-to-date",
			configStart:   time.Date(2025, 1, 2, 0, 0, 0, 0, ny),
			firstTS:       time.Date(2025, 1, 2, 14, 0, 0, 0, ny), // Same day, afternoon
			dailyOrLonger: false,
			expected:      false,
		},
		{
			name:          "intraday: weekend config start with Monday data is up-to-date",
			configStart:   time.Date(2025, 1, 4, 0, 0, 0, 0, ny),  // Saturday
			firstTS:       time.Date(2025, 1, 6, 9, 30, 0, 0, ny), // Monday market open
			dailyOrLonger: false,
			expected:      false, // Monday is first market day after Saturday
		},
		{
			name:          "daily: config date before first data needs backfill",
			configStart:   time.Date(2025, 1, 2, 0, 0, 0, 0, ny),
			firstTS:       time.Date(2025, 1, 3, 0, 0, 0, 0, ny),
			dailyOrLonger: true,
			expected:      true,
		},
		{
			name:          "daily: same date is up-to-date",
			configStart:   time.Date(2025, 1, 2, 0, 0, 0, 0, ny),
			firstTS:       time.Date(2025, 1, 2, 0, 0, 0, 0, ny),
			dailyOrLonger: true,
			expected:      false,
		},
		{
			name:          "intraday: UTC listing date with local first data (same calendar day)",
			configStart:   time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), // From DB as UTC
			firstTS:       time.Date(2025, 1, 2, 9, 30, 0, 0, ny),      // ET market open
			dailyOrLonger: false,
			expected:      false, // Same calendar day in ET
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := configStartNeedsBackfill(tt.configStart, tt.firstTS, tt.dailyOrLonger)
			assert.Equal(t, tt.expected, result, "configStart=%v, firstTS=%v", tt.configStart, tt.firstTS)
		})
	}
}
