package massiveconfig

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	// DBQueryTimeout is the timeout for database queries.
	DBQueryTimeout = 30 * time.Second
	// DateFormat is the expected format for date strings.
	DateFormat = "2006-01-02"

	// DefaultS3Endpoint is the Massive flat files S3-compatible endpoint.
	DefaultS3Endpoint = "https://files.massive.com"
	// DefaultS3Bucket is the S3 bucket containing flat files.
	DefaultS3Bucket = "flatfiles"
	// DefaultS3Prefix is the S3 key prefix for US stock SIP data.
	DefaultS3Prefix = "us_stocks_sip"
	// DefaultS3IndicesPrefix is the S3 key prefix for US index data.
	DefaultS3IndicesPrefix = "us_indices"
)

// SymbolInfo holds a ticker symbol, its database ID, and optional listing date.
// When ListingDate is set, it overrides the global query_start for backfilling
// if the listing date is more recent than the configured start.
type SymbolInfo struct {
	// Symbol is the ticker symbol (e.g., "AAPL").
	Symbol string
	// ID is the database primary key for this symbol. Used as $1 in sync queries.
	// Zero when using static symbols (no database).
	ID int64
	// ListingDate is the optional date when the symbol started trading.
	// If set and more recent than the global query_start, backfill starts from this date.
	// If nil, the global query_start is used.
	ListingDate *time.Time
}

// SyncQuerySet defines the SQL queries for reading and writing sync timestamps
// for a specific data type. The queries use PostgreSQL $N bind parameters:
//   - Read: $1 = asset_id. Must return two TIMESTAMPTZ columns: (oldest, newest).
//     Returns zero rows if no sync record exists for this asset.
//   - WriteOldest: $1 = asset_id, $2 = timestamp. Updates the oldest sync boundary.
//   - WriteNewest: $1 = asset_id, $2 = timestamp. Updates the newest sync boundary.
type SyncQuerySet struct {
	Read        string `json:"read"`
	WriteOldest string `json:"write_oldest"`
	WriteNewest string `json:"write_newest"`
}

// FetcherConfig defines the configuration for the Massive data fetcher plugin.
type FetcherConfig struct {
	// APIKey is the Massive API key for authenticating with WebSocket and REST APIs.
	APIKey string `json:"api_key"`
	// BaseURL is the REST API base URL (defaults to "https://api.massive.com").
	BaseURL string `json:"base_url"`
	// WSServer is the WebSocket server URL (defaults to "wss://socket.massive.com").
	WSServer string `json:"ws_server"`
	// WSDataTypes is a list of data types to stream via WebSocket.
	// Supported values: "1Min", "1Sec", "trades", "quotes".
	// Defaults to ["1Min"] if not specified.
	// Example: ["1Min", "quotes"] streams 1-minute bars and NBBO quotes.
	WSDataTypes []string `json:"ws_data_types"`
	// Symbols is a list of stock ticker symbols to subscribe to.
	// Use ["*"] to subscribe to all tickers.
	Symbols []string `json:"symbols"`
	// QueryStart is a mapping of data type/frequency to start date (YYYY-MM-DD).
	// For bars, keys should be timeframe strings (e.g., "1Min", "5Min", "1H", "1D").
	// For trades and quotes, use "trades" and "quotes" as keys.
	// The keys determine which data types/frequencies are backfilled.
	// On subsequent restarts, backfill resumes from the sync window stored in the database.
	// If empty, no automatic backfill is performed.
	// Example: {"1Min": "2024-01-01", "1D": "2020-01-01", "trades": "2024-06-01"}
	QueryStart map[string]string `json:"query_start"`
	// BackfillBatchSize is the pagination limit for REST API backfill requests.
	// Defaults to 50000 if not set.
	BackfillBatchSize int `json:"backfill_batch_size"`
	// BackfillAdjusted controls whether backfilled bars are split-adjusted.
	// Defaults to true.
	BackfillAdjusted *bool `json:"backfill_adjusted"`
	// SymbolsDSN is the PostgreSQL connection string for fetching symbols and
	// storing sync timestamps. When set, symbols are queried from the database
	// and the static Symbols field is ignored.
	// Example: "postgres://user:pass@localhost:5432/mydb?sslmode=disable"
	SymbolsDSN string `json:"symbols_dsn"`
	// SymbolsQuery is the SQL query to execute when SymbolsDSN is set.
	// The query must return exactly 3 columns: (id, ticker, listing_date).
	//   - id: integer primary key, used as $1 in sync queries
	//   - ticker: string symbol name
	//   - listing_date: nullable date (DATE, TIMESTAMP, TIMESTAMPTZ, or YYYY-MM-DD string)
	// Example: "SELECT id, ticker, listed FROM asset WHERE is_active = TRUE"
	SymbolsQuery string `json:"symbols_query"`
	// BackfillParallelism is the number of symbols to backfill concurrently.
	// Defaults to runtime.NumCPU() if not set or zero.
	BackfillParallelism int `json:"backfill_parallelism"`
	// SyncQueries maps data type keys (matching query_start keys) to SQL query sets
	// for reading and writing sync timestamps. Required when symbols_dsn is set
	// and query_start is non-empty: every key in query_start must have a corresponding
	// entry in sync_queries.
	SyncQueries map[string]SyncQuerySet `json:"sync_queries"`
	// S3AccessKey is the access key ID for the Massive flat files S3 endpoint.
	// Falls back to MASSIVE_S3_ACCESS_KEY environment variable.
	S3AccessKey string `json:"s3_access_key"`
	// S3SecretKey is the secret access key for the Massive flat files S3 endpoint.
	// Falls back to MASSIVE_S3_SECRET_KEY environment variable.
	S3SecretKey string `json:"s3_secret_key"`
	// DynamicTicks enables dynamic, runtime-driven trades/quotes subscriptions.
	// When true, tick streams (trades/quotes) start with an EMPTY subscription
	// and are subscribed/unsubscribed per-symbol at runtime via the RPC control
	// API or an in-process trigger; the static Symbols set is ignored for ticks
	// (it is still used for aggregate 1Sec/1Min streams and backfill).
	// Defaults to false (today's static behavior, fully back-compatible).
	DynamicTicks bool `json:"dynamic_ticks"`
	// MaxDynamicSymbols is the per-DataType cap on concurrently-subscribed tick
	// symbols when DynamicTicks is enabled (e.g. 500 permits up to 500 trade
	// symbols AND up to 500 quote symbols). It is a symbols cap, not a streams
	// cap. Defaults to DefaultMaxDynamicSymbols when <= 0.
	MaxDynamicSymbols int `json:"max_dynamic_symbols"`
	// SymbolInfos is populated at runtime from either Symbols (converted to SymbolInfo with nil dates)
	// or from the database query results. This field is not parsed from config.
	SymbolInfos []SymbolInfo `json:"-"`
}

// DefaultMaxDynamicSymbols is the per-DataType subscription cap applied when
// MaxDynamicSymbols is not set (or <= 0) and DynamicTicks is enabled.
const DefaultMaxDynamicSymbols = 500

// ValidWSDataTypes is the set of valid values for WSDataTypes.
var ValidWSDataTypes = map[string]bool{
	"1Min":   true,
	"1Sec":   true,
	"trades": true,
	"quotes": true,
}

// ValidateConfig checks that the configuration is internally consistent.
// It returns an error if required fields are missing or mismatched.
func ValidateConfig(config *FetcherConfig) error {
	if config.SymbolsDSN != "" {
		if config.SymbolsQuery == "" {
			return fmt.Errorf("symbols_query is required when symbols_dsn is set")
		}

		// Every query_start key must have a corresponding sync_queries entry.
		for key := range config.QueryStart {
			sq, ok := config.SyncQueries[key]
			if !ok {
				return fmt.Errorf("sync_queries entry required for query_start key %q", key)
			}
			if sq.Read == "" {
				return fmt.Errorf("sync_queries[%q].read is required", key)
			}
			if sq.WriteOldest == "" {
				return fmt.Errorf("sync_queries[%q].write_oldest is required", key)
			}
			if sq.WriteNewest == "" {
				return fmt.Errorf("sync_queries[%q].write_newest is required", key)
			}
		}
	}

	if config.DynamicTicks {
		hasTick := false
		for _, dt := range config.WSDataTypes {
			if dt == "trades" || dt == "quotes" {
				hasTick = true
				break
			}
		}
		if !hasTick {
			log.Warn("[massive] dynamic_ticks is true but neither trades nor quotes is in ws_data_types; dynamic mode is a no-op")
		}
		if len(config.Symbols) > 0 {
			log.Warn("[massive] dynamic_ticks is true: the symbols set is IGNORED for trades/quotes (tick streams start empty and are driven at runtime); symbols is still used for aggregate streams and backfill")
		}
		if config.MaxDynamicSymbols <= 0 {
			config.MaxDynamicSymbols = DefaultMaxDynamicSymbols
		}
	}
	return nil
}

// FetchSymbolsFromDB queries PostgreSQL for the list of symbols to track.
// The query must return exactly 3 columns: (id, ticker, listing_date).
//   - id: integer primary key (used as $1 in sync queries)
//   - ticker: string symbol name
//   - listing_date: nullable date (DATE, TIMESTAMP, TIMESTAMPTZ, or YYYY-MM-DD string)
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

	// Validate column count.
	fieldDescs := rows.FieldDescriptions()
	colCount := len(fieldDescs)
	if colCount != 3 {
		return nil, fmt.Errorf("symbols_query must return 3 columns (id, ticker, listed), got %d", colCount)
	}

	var symbols []SymbolInfo
	for rows.Next() {
		var (
			id          int64
			symbol      string
			listingDate interface{}
		)
		if err := rows.Scan(&id, &symbol, &listingDate); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		info := SymbolInfo{
			Symbol: symbol,
			ID:     id,
		}

		if listingDate != nil {
			parsedDate, err := parseListingDate(listingDate)
			if err != nil {
				log.Warn("[massive] failed to parse listing date for %s: %v, using nil", symbol, err)
			} else {
				info.ListingDate = &parsedDate
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

// backfillKeyRank orders query_start keys from finest to coarsest granularity.
// Unknown keys sort last (rank 99) but remain deterministic via the name
// tie-break in OrderedBackfillKeys.
var backfillKeyRank = map[string]int{
	"quotes":   0,
	"trades":   1,
	"1Sec":     2,
	"1Min":     3,
	"5Min":     4,
	"15Min":    5,
	"1H":       6,
	"1D":       7,
	"1D-index": 8,
}

// OrderedBackfillKeys returns the keys of a query_start map in a deterministic,
// aggregation-safe order: finest granularity first, coarsest last.
//
// This ordering is load-bearing, not cosmetic. The ondiskagg trigger derives
// coarser bars from finer ones on every write (e.g. 1Min -> 1D), and
// executor.WriteCSM overwrites by epoch. If the authoritative vendor 1D bar is
// written before the 1Min bars for the same date, the trigger re-derives that
// day's 1D bar from 1Min and silently replaces the vendor bar. Writing 1Min
// first and 1D last means the vendor bar lands last and wins.
//
// Go randomizes map iteration order, so iterating query_start directly made
// this a coin flip per run. Always range over this instead.
func OrderedBackfillKeys(queryStart map[string]string) []string {
	keys := make([]string, 0, len(queryStart))
	for k := range queryStart {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		ri, ok := backfillKeyRank[keys[i]]
		if !ok {
			ri = 99
		}
		rj, ok := backfillKeyRank[keys[j]]
		if !ok {
			rj = 99
		}
		if ri != rj {
			return ri < rj
		}
		return keys[i] < keys[j]
	})
	return keys
}

// EffectiveBackfillStart returns the effective start date for backfilling a symbol.
// It returns the later (more recent) of configStart and listingDate (if set).
//
// Rationale: listingDate is the earliest date data can exist for a symbol, so
// backfilling before it is pointless. But if configStart is more recent than the
// listing date, the user's configured start takes precedence.
func EffectiveBackfillStart(configStart time.Time, listingDate *time.Time) time.Time {
	if listingDate == nil {
		return configStart
	}
	// Compare calendar dates, not timestamps. Both dates should represent
	// "the start of day X" but may be in different timezones.
	// Extract year/month/day and compare those directly.
	listingY, listingM, listingD := listingDate.Date()
	configY, configM, configD := configStart.Date()

	// Create comparable dates (both at midnight UTC for fair comparison).
	listingDay := time.Date(listingY, listingM, listingD, 0, 0, 0, 0, time.UTC)
	configDay := time.Date(configY, configM, configD, 0, 0, 0, 0, time.UTC)

	// Use the later of the two: listing date caps how far back we can go,
	// but configStart may be even more recent and should take precedence.
	if listingDay.After(configDay) {
		return *listingDate
	}
	return configStart
}
