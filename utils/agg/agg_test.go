package agg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

func TestAggregate_Monthly(t *testing.T) {
	t.Parallel()

	// Create daily bars spanning Jan-Feb 2024 (Jan has 31 days, Feb has 29 days - leap year)
	// We'll use 3 days from Jan and 2 days from Feb to keep test simple
	epochs := []int64{
		time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).Unix(), // Jan 15
		time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC).Unix(), // Jan 20
		time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC).Unix(), // Jan 31
		time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC).Unix(),  // Feb 1
		time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC).Unix(), // Feb 15
	}

	opens := []float64{100.0, 102.0, 101.0, 105.0, 107.0}
	highs := []float64{105.0, 108.0, 106.0, 110.0, 112.0}
	lows := []float64{99.0, 101.0, 100.0, 104.0, 106.0}
	closes := []float64{104.0, 103.0, 105.0, 109.0, 111.0}
	volumes := []int64{1000, 1500, 1200, 2000, 1800}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	cs.AddColumn("High", highs)
	cs.AddColumn("Low", lows)
	cs.AddColumn("Close", closes)
	cs.AddColumn("Volume", volumes)

	// Aggregate to monthly
	result, err := Aggregate(cs, "1M")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have 2 monthly bars (Jan and Feb)
	assert.Equal(t, 2, result.Len())

	resultEpochs := result.GetEpoch()
	resultOpens := result.GetColumn("Open").([]float64)
	resultHighs := result.GetColumn("High").([]float64)
	resultLows := result.GetColumn("Low").([]float64)
	resultCloses := result.GetColumn("Close").([]float64)
	resultVolumes := result.GetColumn("Volume").([]int64)

	// January bar (truncated to Jan 1)
	expectedJanEpoch := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	assert.Equal(t, expectedJanEpoch, resultEpochs[0])
	assert.Equal(t, 100.0, resultOpens[0])         // First open: Jan 15 = 100.0
	assert.Equal(t, 108.0, resultHighs[0])         // Max high: Jan 20 = 108.0
	assert.Equal(t, 99.0, resultLows[0])           // Min low: Jan 15 = 99.0
	assert.Equal(t, 105.0, resultCloses[0])        // Last close: Jan 31 = 105.0
	assert.Equal(t, int64(3700), resultVolumes[0]) // Sum: 1000+1500+1200 = 3700

	// February bar (truncated to Feb 1)
	expectedFebEpoch := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC).Unix()
	assert.Equal(t, expectedFebEpoch, resultEpochs[1])
	assert.Equal(t, 105.0, resultOpens[1])         // First open: Feb 1 = 105.0
	assert.Equal(t, 112.0, resultHighs[1])         // Max high: Feb 15 = 112.0
	assert.Equal(t, 104.0, resultLows[1])          // Min low: Feb 1 = 104.0
	assert.Equal(t, 111.0, resultCloses[1])        // Last close: Feb 15 = 111.0
	assert.Equal(t, int64(3800), resultVolumes[1]) // Sum: 2000+1800 = 3800
}

func TestAggregate_MultiMonth(t *testing.T) {
	t.Parallel()

	// Test 3M (quarterly) aggregation
	epochs := []int64{
		time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).Unix(), // Q1
		time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC).Unix(), // Q1
		time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC).Unix(), // Q1
		time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC).Unix(), // Q2
		time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC).Unix(), // Q2
	}

	opens := []float64{100.0, 102.0, 104.0, 106.0, 108.0}
	highs := []float64{105.0, 107.0, 109.0, 111.0, 113.0}
	lows := []float64{99.0, 101.0, 103.0, 105.0, 107.0}
	closes := []float64{101.0, 103.0, 105.0, 107.0, 109.0}
	volumes := []int64{1000, 1000, 1000, 1000, 1000}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	cs.AddColumn("High", highs)
	cs.AddColumn("Low", lows)
	cs.AddColumn("Close", closes)
	cs.AddColumn("Volume", volumes)

	// Aggregate to quarterly (3M)
	result, err := Aggregate(cs, "3M")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have 2 quarterly bars (Q1 and Q2)
	assert.Equal(t, 2, result.Len())

	resultEpochs := result.GetEpoch()
	resultOpens := result.GetColumn("Open").([]float64)
	resultHighs := result.GetColumn("High").([]float64)
	resultLows := result.GetColumn("Low").([]float64)
	resultCloses := result.GetColumn("Close").([]float64)
	resultVolumes := result.GetColumn("Volume").([]int64)

	// Q1 bar (Jan-Mar, truncated to Jan 1)
	expectedQ1Epoch := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	assert.Equal(t, expectedQ1Epoch, resultEpochs[0])
	assert.Equal(t, 100.0, resultOpens[0])         // First open: Jan = 100.0
	assert.Equal(t, 109.0, resultHighs[0])         // Max high: Mar = 109.0
	assert.Equal(t, 99.0, resultLows[0])           // Min low: Jan = 99.0
	assert.Equal(t, 105.0, resultCloses[0])        // Last close: Mar = 105.0
	assert.Equal(t, int64(3000), resultVolumes[0]) // Sum: 1000*3 = 3000

	// Q2 bar (Apr-Jun, truncated to Apr 1)
	expectedQ2Epoch := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC).Unix()
	assert.Equal(t, expectedQ2Epoch, resultEpochs[1])
	assert.Equal(t, 106.0, resultOpens[1])         // First open: Apr = 106.0
	assert.Equal(t, 113.0, resultHighs[1])         // Max high: May = 113.0
	assert.Equal(t, 105.0, resultLows[1])          // Min low: Apr = 105.0
	assert.Equal(t, 109.0, resultCloses[1])        // Last close: May = 109.0
	assert.Equal(t, int64(2000), resultVolumes[1]) // Sum: 1000*2 = 2000
}

func TestAggregate_MonthlyYearBoundary(t *testing.T) {
	t.Parallel()

	// Test aggregation across year boundary (Dec 2023 -> Jan 2024)
	epochs := []int64{
		time.Date(2023, 12, 15, 0, 0, 0, 0, time.UTC).Unix(), // Dec 2023
		time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC).Unix(), // Dec 2023
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),   // Jan 2024
		time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).Unix(),  // Jan 2024
	}

	opens := []float64{100.0, 102.0, 104.0, 106.0}
	highs := []float64{105.0, 107.0, 109.0, 111.0}
	lows := []float64{99.0, 101.0, 103.0, 105.0}
	closes := []float64{101.0, 103.0, 105.0, 107.0}
	volumes := []int64{1000, 1000, 1000, 1000}

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	cs.AddColumn("High", highs)
	cs.AddColumn("Low", lows)
	cs.AddColumn("Close", closes)
	cs.AddColumn("Volume", volumes)

	result, err := Aggregate(cs, "1M")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have 2 monthly bars (Dec 2023 and Jan 2024)
	assert.Equal(t, 2, result.Len())

	resultEpochs := result.GetEpoch()

	// December 2023 bar
	expectedDecEpoch := time.Date(2023, 12, 1, 0, 0, 0, 0, time.UTC).Unix()
	assert.Equal(t, expectedDecEpoch, resultEpochs[0])

	// January 2024 bar
	expectedJanEpoch := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	assert.Equal(t, expectedJanEpoch, resultEpochs[1])
}

func TestAggregate_EmptyInput(t *testing.T) {
	t.Parallel()

	cs := io.NewColumnSeries()
	result, err := Aggregate(cs, "1M")
	require.NoError(t, err)
	assert.Equal(t, 0, result.Len())
}

func TestAggregate_NilInput(t *testing.T) {
	t.Parallel()

	result, err := Aggregate(nil, "1M")
	require.NoError(t, err)
	assert.Equal(t, 0, result.Len())
}

func TestAggregate_InvalidTimeframe(t *testing.T) {
	t.Parallel()

	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{time.Now().Unix()})
	cs.AddColumn("Open", []float64{100.0})
	cs.AddColumn("High", []float64{105.0})
	cs.AddColumn("Low", []float64{99.0})
	cs.AddColumn("Close", []float64{104.0})

	_, err := Aggregate(cs, "invalid")
	assert.Error(t, err)
}
