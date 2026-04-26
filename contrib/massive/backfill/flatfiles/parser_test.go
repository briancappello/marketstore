package flatfiles

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
)

const dayAggsCSV = `ticker,volume,open,close,high,low,window_start,transactions
A,953587,135.21,133.43,135.73,132.87,1735794000000000000,20240
AA,2703886,38.165,37.99,39.04,37.9,1735794000000000000,33119
AAPL,50000000,150.00,155.50,156.00,149.50,1735794000000000000,500000
`

const minuteAggsCSV = `ticker,volume,open,close,high,low,window_start,transactions
AAPL,219,134.99,134.99,134.99,134.99,1735823220000000000,5
AAPL,100,136.9499,136.9499,136.9499,136.9499,1735825440000000000,1
MSFT,1975,276.75,275.52,276.75,275.25,1735823220000000000,83
MSFT,2349,275.2,274.46,275.2,274.46,1735825440000000000,99
`

func TestParseAndWrite_DayAggs(t *testing.T) {
	t.Parallel()

	reader := strings.NewReader(dayAggsCSV)
	date := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	csm, stats, err := ParseAndWrite(reader, nil, "1D", date)

	require.NoError(t, err)
	assert.Equal(t, 3, stats.RowsRead)
	assert.Equal(t, 3, stats.RowsMatched)
	assert.Equal(t, 3, stats.SymbolCount)

	// Should have 3 time bucket keys (one per symbol: A, AA, AAPL).
	assert.Len(t, csm, 3)

	for _, sym := range []string{"A", "AA", "AAPL"} {
		tbk := utilsio.NewTimeBucketKeyFromString(sym + "/1D/OHLCV")
		cs := csm[*tbk]
		require.NotNil(t, cs, "missing CSM entry for %s", sym)
		epochs := cs.GetColumn("Epoch").([]int64)
		assert.Len(t, epochs, 1, "expected 1 bar for %s", sym)
	}
}

func TestParseAndWrite_MinuteAggs(t *testing.T) {
	t.Parallel()

	reader := strings.NewReader(minuteAggsCSV)
	date := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	csm, stats, err := ParseAndWrite(reader, nil, "1Min", date)

	require.NoError(t, err)
	assert.Equal(t, 4, stats.RowsRead)
	assert.Equal(t, 4, stats.RowsMatched)
	assert.Equal(t, 2, stats.SymbolCount) // AAPL and MSFT
	assert.Len(t, csm, 2)

	// AAPL should have 2 minute bars.
	aaplTbk := utilsio.NewTimeBucketKeyFromString("AAPL/1Min/OHLCV")
	aaplCs := csm[*aaplTbk]
	require.NotNil(t, aaplCs)
	assert.Len(t, aaplCs.GetColumn("Epoch").([]int64), 2)

	// MSFT should have 2 minute bars.
	msftTbk := utilsio.NewTimeBucketKeyFromString("MSFT/1Min/OHLCV")
	msftCs := csm[*msftTbk]
	require.NotNil(t, msftCs)
	assert.Len(t, msftCs.GetColumn("Epoch").([]int64), 2)
}

func TestParseAndWrite_SymbolFilter(t *testing.T) {
	t.Parallel()

	symbolSet := map[string]bool{"AAPL": true}
	reader := strings.NewReader(dayAggsCSV)
	date := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	csm, stats, err := ParseAndWrite(reader, symbolSet, "1D", date)

	require.NoError(t, err)
	assert.Equal(t, 3, stats.RowsRead)
	assert.Equal(t, 1, stats.RowsMatched) // Only AAPL
	assert.Equal(t, 1, stats.SymbolCount)
	assert.Len(t, csm, 1)

	tbk := utilsio.NewTimeBucketKeyFromString("AAPL/1D/OHLCV")
	cs := csm[*tbk]
	require.NotNil(t, cs)
}

func TestParseAndWrite_EmptyFile(t *testing.T) {
	t.Parallel()

	reader := strings.NewReader("ticker,volume,open,close,high,low,window_start,transactions\n")
	date := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	csm, stats, err := ParseAndWrite(reader, nil, "1D", date)

	require.NoError(t, err)
	assert.Equal(t, 0, stats.RowsRead)
	assert.Equal(t, 0, stats.RowsMatched)
	assert.Equal(t, 0, stats.SymbolCount)
	assert.Len(t, csm, 0)
}

func TestParseAndWrite_MissingRequiredColumn(t *testing.T) {
	t.Parallel()

	// Missing "open" column (a required column).
	csv := "ticker,close,high,low,window_start\nAAPL,155,156,149,1735794000000000000\n"
	reader := strings.NewReader(csv)
	date := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	_, _, err := ParseAndWrite(reader, nil, "1D", date)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "open")
}

func TestEpochConversion(t *testing.T) {
	t.Parallel()

	// 1735794000000000000 nanoseconds = 1735794000 seconds = 2025-01-02 05:00:00 UTC
	csv := `ticker,volume,open,close,high,low,window_start,transactions
AAPL,100,150.00,155.50,156.00,149.50,1735794000000000000,1000
`
	reader := strings.NewReader(csv)
	date := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	csm, stats, err := ParseAndWrite(reader, nil, "1D", date)

	require.NoError(t, err)
	assert.Equal(t, 1, stats.RowsMatched)

	// Verify the epoch was correctly converted from nanoseconds to seconds.
	tbk := utilsio.NewTimeBucketKeyFromString("AAPL/1D/OHLCV")
	cs := csm[*tbk]
	require.NotNil(t, cs)

	epochCol := cs.GetColumn("Epoch")
	require.NotNil(t, epochCol)
	epochs, ok := epochCol.([]int64)
	require.True(t, ok)
	require.Len(t, epochs, 1)
	assert.Equal(t, int64(1735794000), epochs[0])
}

func TestBuildColumnIndex(t *testing.T) {
	t.Parallel()

	t.Run("valid header", func(t *testing.T) {
		header := []string{"ticker", "volume", "open", "close", "high", "low", "window_start", "transactions"}
		idx, err := buildColumnIndex(header)
		require.NoError(t, err)
		assert.Equal(t, 0, idx.ticker)
		assert.Equal(t, 1, idx.volume)
		assert.Equal(t, 2, idx.open)
		assert.Equal(t, 3, idx.close)
		assert.Equal(t, 4, idx.high)
		assert.Equal(t, 5, idx.low)
		assert.Equal(t, 6, idx.windowStart)
	})

	t.Run("reordered columns", func(t *testing.T) {
		header := []string{"window_start", "ticker", "high", "low", "open", "close", "volume", "transactions"}
		idx, err := buildColumnIndex(header)
		require.NoError(t, err)
		assert.Equal(t, 1, idx.ticker)
		assert.Equal(t, 6, idx.volume)
		assert.Equal(t, 4, idx.open)
		assert.Equal(t, 5, idx.close)
		assert.Equal(t, 2, idx.high)
		assert.Equal(t, 3, idx.low)
		assert.Equal(t, 0, idx.windowStart)
	})

	t.Run("missing volume is allowed", func(t *testing.T) {
		// Index flat files have no volume column; this should succeed.
		header := []string{"ticker", "open", "close", "high", "low", "window_start"}
		idx, err := buildColumnIndex(header)
		require.NoError(t, err)
		assert.Equal(t, -1, idx.volume)
		assert.False(t, idx.HasVolume())
	})

	t.Run("missing required column", func(t *testing.T) {
		header := []string{"ticker", "open", "high", "low", "window_start"}
		_, err := buildColumnIndex(header)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "close")
	})
}

func TestParseRow(t *testing.T) {
	t.Parallel()

	idx := columnIndex{
		ticker:      0,
		volume:      1,
		open:        2,
		close:       3,
		high:        4,
		low:         5,
		windowStart: 6,
	}

	t.Run("valid row", func(t *testing.T) {
		record := []string{"AAPL", "953587", "135.21", "133.43", "135.73", "132.87", "1735794000000000000"}
		open, high, low, clos, volume, epoch, err := parseRow(record, idx)
		require.NoError(t, err)
		assert.InDelta(t, 135.21, open, 0.001)
		assert.InDelta(t, 135.73, high, 0.001)
		assert.InDelta(t, 132.87, low, 0.001)
		assert.InDelta(t, 133.43, clos, 0.001)
		assert.Equal(t, uint64(953587), volume)
		assert.Equal(t, int64(1735794000), epoch)
	})

	t.Run("fractional volume", func(t *testing.T) {
		// Some aggregates may have fractional volume (e.g., crypto).
		record := []string{"X:BTCUSD", "1234.567", "50000.0", "51000.0", "52000.0", "49000.0", "1735794000000000000"}
		_, _, _, _, volume, _, err := parseRow(record, idx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1234), volume) // truncated to uint64
	})

	t.Run("invalid open", func(t *testing.T) {
		record := []string{"AAPL", "100", "abc", "155", "156", "149", "1735794000000000000"}
		_, _, _, _, _, _, err := parseRow(record, idx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "open")
	})

	t.Run("invalid window_start", func(t *testing.T) {
		record := []string{"AAPL", "100", "150", "155", "156", "149", "not_a_number"}
		_, _, _, _, _, _, err := parseRow(record, idx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "window_start")
	})
}

// Index flat file CSV: no volume or transactions columns.
const indexDayAggsCSV = `ticker,open,close,high,low,window_start
I:AAVE100,96.767,99.2175,100.4065,95.5217,1701756000000000000
I:AAVE10RP,96.7652,99.2205,100.3647,95.5207,1701756000000000000
I:XNDXTRND,2755.6953,2759.323,2763.9738,2748.6122,1701756000000000000
`

func TestParseAndWrite_IndexDayAggs(t *testing.T) {
	t.Parallel()

	reader := strings.NewReader(indexDayAggsCSV)
	date := time.Date(2023, 12, 5, 0, 0, 0, 0, time.UTC)

	csm, stats, err := ParseAndWrite(reader, nil, "1D-index", date)

	require.NoError(t, err)
	assert.Equal(t, 3, stats.RowsRead)
	assert.Equal(t, 3, stats.RowsMatched)
	assert.Equal(t, 3, stats.SymbolCount)
	assert.Len(t, csm, 3)

	// Verify index tickers are normalized: "I:AAVE100" -> "^AAVE100".
	// TBKs should use OHLC attribute group (no Volume).
	for _, sym := range []string{"^AAVE100", "^AAVE10RP", "^XNDXTRND"} {
		tbk := utilsio.NewTimeBucketKeyFromString(sym + "/1D-index/OHLC")
		cs := csm[*tbk]
		require.NotNil(t, cs, "missing CSM entry for %s", sym)

		// Should have OHLC columns but NOT Volume.
		epochs := cs.GetColumn("Epoch").([]int64)
		assert.Len(t, epochs, 1, "expected 1 bar for %s", sym)
		assert.NotNil(t, cs.GetColumn("Open"), "expected Open column for %s", sym)
		assert.NotNil(t, cs.GetColumn("High"), "expected High column for %s", sym)
		assert.NotNil(t, cs.GetColumn("Low"), "expected Low column for %s", sym)
		assert.NotNil(t, cs.GetColumn("Close"), "expected Close column for %s", sym)
		assert.Nil(t, cs.GetColumn("Volume"), "Volume should be absent for index %s", sym)
	}
}

func TestParseAndWrite_IndexSymbolFilter(t *testing.T) {
	t.Parallel()

	// Filter using normalized names (with ^ prefix).
	symbolSet := map[string]bool{"^AAVE100": true, "^XNDXTRND": true}
	reader := strings.NewReader(indexDayAggsCSV)
	date := time.Date(2023, 12, 5, 0, 0, 0, 0, time.UTC)

	csm, stats, err := ParseAndWrite(reader, symbolSet, "1D-index", date)

	require.NoError(t, err)
	assert.Equal(t, 3, stats.RowsRead)
	assert.Equal(t, 2, stats.RowsMatched) // ^AAVE100 and ^XNDXTRND
	assert.Equal(t, 2, stats.SymbolCount)
	assert.Len(t, csm, 2)
}

func TestParseAndWrite_IndexEmptyFile(t *testing.T) {
	t.Parallel()

	reader := strings.NewReader("ticker,open,close,high,low,window_start\n")
	date := time.Date(2023, 12, 5, 0, 0, 0, 0, time.UTC)

	csm, stats, err := ParseAndWrite(reader, nil, "1D-index", date)

	require.NoError(t, err)
	assert.Equal(t, 0, stats.RowsRead)
	assert.Equal(t, 0, stats.SymbolCount)
	assert.Len(t, csm, 0)
}

func TestParseAndWrite_IndexEpochConversion(t *testing.T) {
	t.Parallel()

	// 1701756000000000000 nanoseconds = 1701756000 seconds = 2023-12-05 06:00:00 UTC
	csv := `ticker,open,close,high,low,window_start
I:SPX,4500.00,4550.50,4560.00,4490.00,1701756000000000000
`
	reader := strings.NewReader(csv)
	date := time.Date(2023, 12, 5, 0, 0, 0, 0, time.UTC)

	csm, _, err := ParseAndWrite(reader, nil, "1D-index", date)

	require.NoError(t, err)

	tbk := utilsio.NewTimeBucketKeyFromString("^SPX/1D-index/OHLC")
	cs := csm[*tbk]
	require.NotNil(t, cs)

	epochs := cs.GetColumn("Epoch").([]int64)
	require.Len(t, epochs, 1)
	assert.Equal(t, int64(1701756000), epochs[0])
}

func TestNormalizeTicker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"I:AAVE100", "^AAVE100"},
		{"I:SPX", "^SPX"},
		{"I:XNDXTRND", "^XNDXTRND"},
		{"AAPL", "AAPL"},         // Non-index ticker unchanged
		{"X:BTCUSD", "X:BTCUSD"}, // Crypto prefix unchanged
		{"", ""},                 // Empty string
		{"I:", "^"},              // Edge case: prefix only
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, NormalizeTicker(tt.input), "NormalizeTicker(%q)", tt.input)
	}
}

func TestParseRow_NoVolume(t *testing.T) {
	t.Parallel()

	// Index column layout: no volume column.
	idx := columnIndex{
		ticker:      0,
		volume:      -1, // absent
		open:        1,
		close:       2,
		high:        3,
		low:         4,
		windowStart: 5,
	}

	record := []string{"I:SPX", "4500.00", "4550.50", "4560.00", "4490.00", "1701756000000000000"}
	open, high, low, clos, volume, epoch, err := parseRow(record, idx)
	require.NoError(t, err)
	assert.InDelta(t, 4500.00, open, 0.001)
	assert.InDelta(t, 4560.00, high, 0.001)
	assert.InDelta(t, 4490.00, low, 0.001)
	assert.InDelta(t, 4550.50, clos, 0.001)
	assert.Equal(t, uint64(0), volume) // No volume -> 0
	assert.Equal(t, int64(1701756000), epoch)
}
