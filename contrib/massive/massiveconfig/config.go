package massiveconfig

// FetcherConfig defines the configuration for the Massive data fetcher plugin.
type FetcherConfig struct {
	// APIKey is the Massive API key for authenticating with WebSocket and REST APIs.
	APIKey string `json:"api_key"`
	// BaseURL is the REST API base URL (defaults to "https://api.massive.com").
	BaseURL string `json:"base_url"`
	// WSServer is the WebSocket server URL (defaults to "wss://socket.massive.com").
	WSServer string `json:"ws_server"`
	// DataTypes is a list of data types to subscribe to (bars, quotes, trades).
	DataTypes []string `json:"data_types"`
	// Symbols is a list of stock ticker symbols to subscribe to.
	// Use ["*"] to subscribe to all tickers.
	Symbols []string `json:"symbols"`
	// QueryStart is a mapping of data type/frequency to start date (YYYY-MM-DD).
	// For bars, keys should be timeframe strings (e.g., "1Min", "5Min", "1H", "1D").
	// For trades and quotes, use "trades" and "quotes" as keys.
	// Only frequencies listed in BarFrequencies will be backfilled for bars.
	// On subsequent restarts, backfill resumes from the last written timestamp.
	// If empty, no automatic backfill is performed.
	// Example: {"1Min": "2024-01-01", "1D": "2020-01-01", "trades": "2024-06-01"}
	QueryStart map[string]string `json:"query_start"`
	// BackfillBatchSize is the pagination limit for REST API backfill requests.
	// Defaults to 50000 if not set.
	BackfillBatchSize int `json:"backfill_batch_size"`
	// BackfillAdjusted controls whether backfilled bars are split-adjusted.
	// Defaults to true.
	BackfillAdjusted *bool `json:"backfill_adjusted"`
	// WSQueryStart is an optional date (YYYY-MM-DD) sent in the WebSocket
	// subscribe message to request historical replay from the server.
	// This is only sent when the ws_server host is localhost or 127.0.0.1.
	// If empty, the stream starts from real-time.
	WSQueryStart string `json:"ws_query_start"`
	// BarFrequencies is a list of bar timeframes to backfill (e.g., ["1Min", "5Min", "1H", "1D"]).
	// If empty, defaults to ["1Min"].
	BarFrequencies []string `json:"bar_frequencies"`
}
