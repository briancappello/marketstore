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

func TestParseAndWrite_MissingColumn(t *testing.T) {
	t.Parallel()

	// Missing "volume" column.
	csv := "ticker,open,close,high,low,window_start,transactions\nAAPL,150,155,156,149,1735794000000000000,1000\n"
	reader := strings.NewReader(csv)
	date := time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

	_, _, err := ParseAndWrite(reader, nil, "1D", date)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "volume")
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

	t.Run("missing column", func(t *testing.T) {
		header := []string{"ticker", "open", "close", "high", "low", "window_start"}
		_, err := buildColumnIndex(header)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "volume")
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
