package main

import (
	"testing"

	massivews "github.com/massive-com/client-go/v3/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWSDataTypeToTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dataType string
		topic    massivews.Topic
	}{
		{"1Min", massivews.StocksMinAggs},
		{"1Sec", massivews.StocksSecAggs},
		{"trades", massivews.StocksTrades},
		{"quotes", massivews.StocksQuotes},
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

func TestWSLogger(t *testing.T) {
	t.Parallel()

	// Verify wsLogger satisfies the upstream Logger interface.
	var _ massivews.Logger = &wsLogger{}
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
				"symbols_query": "SELECT symbol FROM symbols",
			},
			expectError: "fetch symbols from database: connect to postgres:",
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
