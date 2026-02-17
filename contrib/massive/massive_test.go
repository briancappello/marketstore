package main

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
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
