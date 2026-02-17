package backfill

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEstimateBarsPerDay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		timeframe string
		expected  int
	}{
		{"1Min", 960},   // 16 hours * 60 minutes
		{"5Min", 192},   // 960 / 5
		{"15Min", 64},   // 960 / 15
		{"1H", 16},      // 16 hours
		{"4H", 4},       // 16 / 4
		{"1D", 1},       // 1 bar per day
		{"1W", 1},       // 1 bar per week (treated as 1)
		{"1Sec", 57600}, // 16 * 60 * 60
	}

	for _, tt := range tests {
		t.Run(tt.timeframe, func(t *testing.T) {
			t.Parallel()
			result := estimateBarsPerDay(tt.timeframe)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTimeframeChunkDays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		timeframe   string
		minExpected int
		maxExpected int
		description string
	}{
		{"1Min", 50, 80, "~50k limit / 960 bars/day = ~52 trading days = ~73 calendar days"},
		{"5Min", 250, 365, "~50k / 192 = ~260 trading days, capped at 365"},
		{"1H", 365, 365, "~50k / 16 = ~3125 trading days, capped at 365"},
		{"1D", 365, 365, "~50k / 1 = tons of days, capped at 365"},
	}

	for _, tt := range tests {
		t.Run(tt.timeframe, func(t *testing.T) {
			t.Parallel()
			result := timeframeChunkDays(tt.timeframe)
			assert.GreaterOrEqual(t, result, tt.minExpected, tt.description)
			assert.LessOrEqual(t, result, tt.maxExpected, tt.description)
		})
	}
}

func TestSplitDateRange(t *testing.T) {
	t.Parallel()

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC) // ~5 months

	t.Run("1Min splits into multiple chunks", func(t *testing.T) {
		t.Parallel()
		chunks := splitDateRange(from, to, "1Min")
		// 5 months = ~150 days, chunk size ~70 days = ~2-3 chunks
		assert.GreaterOrEqual(t, len(chunks), 2)
		assert.LessOrEqual(t, len(chunks), 4)

		// First chunk starts at from.
		assert.Equal(t, from, chunks[0].from)
		// Last chunk ends at to.
		assert.Equal(t, to, chunks[len(chunks)-1].to)

		// Chunks are contiguous.
		for i := 1; i < len(chunks); i++ {
			assert.Equal(t, chunks[i-1].to, chunks[i].from)
		}
	})

	t.Run("1D results in single chunk", func(t *testing.T) {
		t.Parallel()
		chunks := splitDateRange(from, to, "1D")
		// 1D has huge chunk size (365 days), so 5 months fits in one chunk.
		assert.Equal(t, 1, len(chunks))
		assert.Equal(t, from, chunks[0].from)
		assert.Equal(t, to, chunks[0].to)
	})

	t.Run("short range stays single chunk", func(t *testing.T) {
		t.Parallel()
		shortTo := from.Add(7 * 24 * time.Hour) // 1 week
		chunks := splitDateRange(from, shortTo, "1Min")
		assert.Equal(t, 1, len(chunks))
	})
}
