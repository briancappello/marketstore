package massiveconfig

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	// DBQueryTimeout is the timeout for database queries.
	DBQueryTimeout = 30 * time.Second
	// DateFormat is the expected format for date strings.
	DateFormat = "2006-01-02"
)

// SymbolInfo holds a ticker symbol and optional listing date.
// When ListingDate is set, it overrides the global query_start for backfilling
// if the listing date is more recent than the configured start.
type SymbolInfo struct {
	// Symbol is the ticker symbol (e.g., "AAPL").
	Symbol string
	// ListingDate is the optional date when the symbol started trading.
	// If set and more recent than the global query_start, backfill starts from this date.
	// If nil, the global query_start is used.
	ListingDate *time.Time
}

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
	// The keys determine which data types/frequencies are backfilled.
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
	// WSFrequencies is a list of bar timeframes for WebSocket streaming subscriptions
	// (e.g., ["1Min"]). This only affects real-time data, not backfilling.
	// If empty, defaults to ["1Min"].
	WSFrequencies []string `json:"ws_frequencies"`
	// SymbolsDSN is an optional PostgreSQL connection string for fetching symbols
	// dynamically at startup. If set, symbols are queried from the database and
	// the static Symbols field is ignored.
	// Example: "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
	SymbolsDSN string `json:"symbols_dsn"`
	// SymbolsQuery is the SQL query to execute when SymbolsDSN is set.
	// The query can return either:
	//   - 1 column: symbol strings (e.g., "SELECT symbol FROM tracked_symbols")
	//   - 2 columns: symbol and nullable listing date (e.g., "SELECT symbol, listing_date FROM tracked_symbols")
	// When listing dates are provided, they override the global query_start for backfilling
	// if the listing date is more recent than the configured start.
	// Example: "SELECT symbol, listing_date FROM tracked_symbols WHERE active = true"
	SymbolsQuery string `json:"symbols_query"`
	// SymbolInfos is populated at runtime from either Symbols (converted to SymbolInfo with nil dates)
	// or from the database query results. This field is not parsed from config.
	SymbolInfos []SymbolInfo `json:"-"`
}

// FetchSymbolsFromDB queries PostgreSQL for the list of symbols to track.
// The query can return either:
//   - 1 column: symbol strings
//   - 2 columns: symbol and nullable listing date
//
// Supported date formats: DATE, TIMESTAMP, TIMESTAMPTZ, or TEXT in YYYY-MM-DD format.
// For datetime values, only the date portion is used (time and timezone are ignored).
func FetchSymbolsFromDB(dsn, query string) ([]SymbolInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DBQueryTimeout)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	// Detect column count from field descriptions.
	fieldDescs := rows.FieldDescriptions()
	colCount := len(fieldDescs)
	if colCount == 0 {
		return nil, fmt.Errorf("query returned no columns")
	}
	if colCount > 2 {
		return nil, fmt.Errorf("query returned %d columns, expected 1 or 2", colCount)
	}

	var symbols []SymbolInfo
	for rows.Next() {
		var info SymbolInfo

		if colCount == 1 {
			// Single column: just the symbol.
			var symbol string
			if err := rows.Scan(&symbol); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
			info.Symbol = symbol
		} else {
			// Two columns: symbol and nullable listing date.
			var symbol string
			var listingDate interface{}
			if err := rows.Scan(&symbol, &listingDate); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
			info.Symbol = symbol

			if listingDate != nil {
				parsedDate, err := parseListingDate(listingDate)
				if err != nil {
					log.Warn("[massive] failed to parse listing date for %s: %v, using nil", symbol, err)
				} else {
					info.ListingDate = &parsedDate
				}
			}
		}

		symbols = append(symbols, info)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return symbols, nil
}

// parseListingDate converts various date formats to time.Time.
// Supported: time.Time (DATE, TIMESTAMP, TIMESTAMPTZ), string (YYYY-MM-DD).
// For datetime values, only the date portion is used.
func parseListingDate(val interface{}) (time.Time, error) {
	switch v := val.(type) {
	case time.Time:
		// Extract just the date portion, ignoring time and timezone.
		return time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, time.UTC), nil
	case string:
		// Parse as YYYY-MM-DD.
		t, err := time.Parse(DateFormat, v)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid date string %q: %w", v, err)
		}
		return t, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported date type %T", val)
	}
}

// EffectiveBackfillStart returns the effective start date for backfilling a symbol.
// If listingDate is set and is more recent than configStart, use listingDate.
// Otherwise, use configStart.
func EffectiveBackfillStart(configStart time.Time, listingDate *time.Time) time.Time {
	if listingDate == nil {
		return configStart
	}
	// Compare calendar dates, not timestamps. Both dates should represent
	// "the start of day X" but may be in different timezones.
	// Extract year/month/day and compare those directly.
	listingY, listingM, listingD := listingDate.Date()
	configY, configM, configD := configStart.Date()

	// Create comparable dates (both at midnight UTC for fair comparison)
	listingDay := time.Date(listingY, listingM, listingD, 0, 0, 0, 0, time.UTC)
	configDay := time.Date(configY, configM, configD, 0, 0, 0, 0, time.UTC)

	log.Info("[massive] EffectiveBackfillStart: listingDate=%s, configStart=%s, listingDay=%s, configDay=%s, after=%v",
		listingDate.Format("2006-01-02 MST"), configStart.Format("2006-01-02 MST"),
		listingDay.Format("2006-01-02"), configDay.Format("2006-01-02"),
		listingDay.After(configDay))

	if listingDay.After(configDay) {
		return *listingDate
	}
	return configStart
}
