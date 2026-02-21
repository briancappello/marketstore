package massiveconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	// Test that SymbolInfo can hold both symbols with and without dates.
	listingDate := time.Date(2021, 5, 1, 0, 0, 0, 0, time.UTC)

	symbolWithDate := SymbolInfo{
		Symbol:      "COIN",
		ListingDate: &listingDate,
	}

	symbolWithoutDate := SymbolInfo{
		Symbol:      "AAPL",
		ListingDate: nil,
	}

	assert.Equal(t, "COIN", symbolWithDate.Symbol)
	assert.NotNil(t, symbolWithDate.ListingDate)
	assert.Equal(t, listingDate, *symbolWithDate.ListingDate)

	assert.Equal(t, "AAPL", symbolWithoutDate.Symbol)
	assert.Nil(t, symbolWithoutDate.ListingDate)
}
