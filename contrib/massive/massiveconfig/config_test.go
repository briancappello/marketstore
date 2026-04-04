package massiveconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEffectiveBackfillStart(t *testing.T) {
	t.Parallel()

	configStart := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	listingBefore := time.Date(2019, 6, 15, 0, 0, 0, 0, time.UTC)
	listingAfter := time.Date(2022, 3, 10, 0, 0, 0, 0, time.UTC)
	listingSame := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		configStart time.Time
		listingDate *time.Time
		expected    time.Time
	}{
		{
			name:        "nil listing date uses config start",
			configStart: configStart,
			listingDate: nil,
			expected:    configStart,
		},
		{
			name:        "listing date before config start uses config start",
			configStart: configStart,
			listingDate: &listingBefore,
			expected:    configStart,
		},
		{
			name:        "listing date after config start uses listing date",
			configStart: configStart,
			listingDate: &listingAfter,
			expected:    listingAfter,
		},
		{
			name:        "listing date same as config start uses config start",
			configStart: configStart,
			listingDate: &listingSame,
			expected:    configStart,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := EffectiveBackfillStart(tt.configStart, tt.listingDate)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseListingDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       interface{}
		expected    time.Time
		expectError bool
	}{
		{
			name:     "time.Time extracts date",
			input:    time.Date(2024, 6, 15, 14, 30, 45, 0, time.FixedZone("EDT", -4*3600)),
			expected: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "string YYYY-MM-DD format",
			input:    "2023-11-20",
			expected: time.Date(2023, 11, 20, 0, 0, 0, 0, time.UTC),
		},
		{
			name:        "invalid string format",
			input:       "11/20/2023",
			expectError: true,
		},
		{
			name:        "unsupported type int",
			input:       20231120,
			expectError: true,
		},
		{
			name:        "unsupported type float",
			input:       2023.1120,
			expectError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := parseListingDate(tt.input)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestSymbolInfo(t *testing.T) {
	t.Parallel()

	// Test that SymbolInfo can hold symbols with and without dates and IDs.
	listingDate := time.Date(2021, 5, 1, 0, 0, 0, 0, time.UTC)

	symbolWithAll := SymbolInfo{
		Symbol:      "COIN",
		ID:          42,
		ListingDate: &listingDate,
	}

	symbolMinimal := SymbolInfo{
		Symbol: "AAPL",
	}

	assert.Equal(t, "COIN", symbolWithAll.Symbol)
	assert.Equal(t, int64(42), symbolWithAll.ID)
	assert.NotNil(t, symbolWithAll.ListingDate)
	assert.Equal(t, listingDate, *symbolWithAll.ListingDate)

	assert.Equal(t, "AAPL", symbolMinimal.Symbol)
	assert.Equal(t, int64(0), symbolMinimal.ID)
	assert.Nil(t, symbolMinimal.ListingDate)
}

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	fullSyncQuery := SyncQuerySet{
		Read:        "SELECT oldest, newest FROM sync WHERE id = $1",
		WriteOldest: "UPDATE sync SET oldest = $2 WHERE id = $1",
		WriteNewest: "UPDATE sync SET newest = $2 WHERE id = $1",
	}

	tests := []struct {
		name        string
		config      FetcherConfig
		expectError string
	}{
		{
			name: "valid config with sync_queries",
			config: FetcherConfig{
				SymbolsDSN:   "postgres://localhost/test",
				SymbolsQuery: "SELECT id, ticker, listed FROM asset",
				QueryStart:   map[string]string{"1Min": "2024-01-01", "1D": "2020-01-01"},
				SyncQueries: map[string]SyncQuerySet{
					"1Min": fullSyncQuery,
					"1D":   fullSyncQuery,
				},
			},
		},
		{
			name: "missing symbols_query when dsn is set",
			config: FetcherConfig{
				SymbolsDSN: "postgres://localhost/test",
			},
			expectError: "symbols_query is required when symbols_dsn is set",
		},
		{
			name: "missing sync_queries entry for query_start key",
			config: FetcherConfig{
				SymbolsDSN:   "postgres://localhost/test",
				SymbolsQuery: "SELECT id, ticker, listed FROM asset",
				QueryStart:   map[string]string{"1Min": "2024-01-01", "1D": "2020-01-01"},
				SyncQueries: map[string]SyncQuerySet{
					"1Min": fullSyncQuery,
					// Missing "1D"
				},
			},
			expectError: "sync_queries entry required for query_start key \"1D\"",
		},
		{
			name: "empty read query in sync_queries",
			config: FetcherConfig{
				SymbolsDSN:   "postgres://localhost/test",
				SymbolsQuery: "SELECT id, ticker, listed FROM asset",
				QueryStart:   map[string]string{"1Min": "2024-01-01"},
				SyncQueries: map[string]SyncQuerySet{
					"1Min": {Read: "", WriteOldest: "UPDATE ...", WriteNewest: "UPDATE ..."},
				},
			},
			expectError: "sync_queries[\"1Min\"].read is required",
		},
		{
			name: "empty write_oldest in sync_queries",
			config: FetcherConfig{
				SymbolsDSN:   "postgres://localhost/test",
				SymbolsQuery: "SELECT id, ticker, listed FROM asset",
				QueryStart:   map[string]string{"1Min": "2024-01-01"},
				SyncQueries: map[string]SyncQuerySet{
					"1Min": {Read: "SELECT ...", WriteOldest: "", WriteNewest: "UPDATE ..."},
				},
			},
			expectError: "sync_queries[\"1Min\"].write_oldest is required",
		},
		{
			name: "empty write_newest in sync_queries",
			config: FetcherConfig{
				SymbolsDSN:   "postgres://localhost/test",
				SymbolsQuery: "SELECT id, ticker, listed FROM asset",
				QueryStart:   map[string]string{"1Min": "2024-01-01"},
				SyncQueries: map[string]SyncQuerySet{
					"1Min": {Read: "SELECT ...", WriteOldest: "UPDATE ...", WriteNewest: ""},
				},
			},
			expectError: "sync_queries[\"1Min\"].write_newest is required",
		},
		{
			name: "no dsn means no validation needed",
			config: FetcherConfig{
				QueryStart: map[string]string{"1Min": "2024-01-01"},
				// No SymbolsDSN, so SyncQueries is not required.
			},
		},
		{
			name: "dsn with no query_start means no sync_queries needed",
			config: FetcherConfig{
				SymbolsDSN:   "postgres://localhost/test",
				SymbolsQuery: "SELECT id, ticker, listed FROM asset",
				// No QueryStart, so SyncQueries is not required.
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateConfig(&tt.config)
			if tt.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
