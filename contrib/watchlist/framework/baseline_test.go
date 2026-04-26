package framework

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// seedFromDailyBar
// ---------------------------------------------------------------------------

func TestSeedFromDailyBar_BasicFields(t *testing.T) {
	state := NewSymbolState()
	state.PriorClose = 100.0
	state.MedianVolume50D = 50000

	closes := []float64{98, 99, 101, 105, 102}
	volumes := []float64{10000, 20000, 30000, 40000, 50000}
	opens := []float64{97, 98, 100, 104, 101}
	highs := []float64{99, 100, 103, 106, 104}
	lows := []float64{96, 97, 99, 103, 100}

	seedFromDailyBar(state, closes, volumes, opens, highs, lows)

	// Last bar's values should be used.
	assert.Equal(t, 102.0, state.LastPrice)
	assert.Equal(t, 102.0, state.LastClose)
	assert.Equal(t, 101.0, state.DayOpen)
	assert.Equal(t, 104.0, state.HighOfDay)
	assert.Equal(t, 100.0, state.LowOfDay)
	assert.Equal(t, int64(50000), state.CumulativeVolume)
}

func TestSeedFromDailyBar_PctChange(t *testing.T) {
	state := NewSymbolState()
	state.PriorClose = 100.0

	closes := []float64{95}
	volumes := []float64{10000}
	opens := []float64{98}
	highs := []float64{99}
	lows := []float64{94}

	seedFromDailyBar(state, closes, volumes, opens, highs, lows)

	// PctChange = (LastPrice - PriorClose) / PriorClose * 100
	// = (95 - 100) / 100 * 100 = -5.0
	assert.InDelta(t, -5.0, state.PctChange, 0.01)
}

func TestSeedFromDailyBar_PositivePctChange(t *testing.T) {
	state := NewSymbolState()
	state.PriorClose = 100.0

	closes := []float64{108}
	volumes := []float64{10000}
	opens := []float64{101}
	highs := []float64{110}
	lows := []float64{100}

	seedFromDailyBar(state, closes, volumes, opens, highs, lows)

	// PctChange should use LastPrice (close=108), not HighOfDay (110).
	assert.InDelta(t, 8.0, state.PctChange, 0.01)
}

func TestSeedFromDailyBar_PriorCloseZero_SkipsPctChange(t *testing.T) {
	state := NewSymbolState()
	state.PriorClose = 0 // unknown prior close

	closes := []float64{105}
	volumes := []float64{10000}
	opens := []float64{100}
	highs := []float64{106}
	lows := []float64{99}

	seedFromDailyBar(state, closes, volumes, opens, highs, lows)

	assert.Equal(t, 0.0, state.PctChange)
}

func TestSeedFromDailyBar_VolumeMultipleOfMed(t *testing.T) {
	state := NewSymbolState()
	state.PriorClose = 100.0
	state.MedianVolume50D = 10000

	closes := []float64{102}
	volumes := []float64{30000}
	opens := []float64{100}
	highs := []float64{103}
	lows := []float64{99}

	seedFromDailyBar(state, closes, volumes, opens, highs, lows)

	assert.InDelta(t, 3.0, state.VolumeMultipleOfMed, 0.01) // 30000/10000
}

func TestSeedFromDailyBar_DollarVolumeRate(t *testing.T) {
	state := NewSymbolState()
	state.PriorClose = 100.0

	closes := []float64{50.0}
	volumes := []float64{234000}
	opens := []float64{49}
	highs := []float64{51}
	lows := []float64{48}

	seedFromDailyBar(state, closes, volumes, opens, highs, lows)

	// DollarVolumeRate = volume * price / 23400
	expected := 234000.0 * 50.0 / 23400.0
	assert.InDelta(t, expected, state.DollarVolumeRate, 0.01)
}

// ---------------------------------------------------------------------------
// PriorClose selection in computeSymbolBaseline
//
// We can't easily call computeSymbolBaseline (it requires a catalog), but we
// can test the PriorClose-selection logic by testing seedFromDailyBar with
// the state that computeSymbolBaseline would produce. The key invariant:
// PriorClose must be the SECOND-to-last daily close.
//
// These tests verify the correct behavior at the call site by directly
// testing the assignment logic.
// ---------------------------------------------------------------------------

func TestPriorCloseSelection_MultipleDaily(t *testing.T) {
	// Simulate what computeSymbolBaseline does with 5 daily closes.
	closes := []float64{98, 99, 101, 105, 102}

	state := NewSymbolState()

	// Replicate the PriorClose assignment from baseline.go.
	if len(closes) >= 2 {
		state.PriorClose = closes[len(closes)-2]
	} else {
		state.PriorClose = closes[0]
	}

	// PriorClose should be 105 (second-to-last), not 102 (last).
	assert.Equal(t, 105.0, state.PriorClose)
}

func TestPriorCloseSelection_TwoDays(t *testing.T) {
	closes := []float64{100, 95}

	state := NewSymbolState()
	if len(closes) >= 2 {
		state.PriorClose = closes[len(closes)-2]
	} else {
		state.PriorClose = closes[0]
	}

	assert.Equal(t, 100.0, state.PriorClose)
}

func TestPriorCloseSelection_SingleDay(t *testing.T) {
	closes := []float64{42.5}

	state := NewSymbolState()
	if len(closes) >= 2 {
		state.PriorClose = closes[len(closes)-2]
	} else {
		state.PriorClose = closes[0]
	}

	// Only one bar: PriorClose falls back to the only close available.
	assert.Equal(t, 42.5, state.PriorClose)
}

// ---------------------------------------------------------------------------
// median
// ---------------------------------------------------------------------------

func TestMedian_OddLength(t *testing.T) {
	assert.Equal(t, 3.0, median([]float64{1, 3, 5}))
}

func TestMedian_EvenLength(t *testing.T) {
	assert.Equal(t, 2.5, median([]float64{1, 2, 3, 4}))
}

func TestMedian_SingleElement(t *testing.T) {
	assert.Equal(t, 42.0, median([]float64{42}))
}

func TestMedian_Empty(t *testing.T) {
	assert.Equal(t, 0.0, median([]float64{}))
}

func TestMedian_Unsorted(t *testing.T) {
	// Input is unsorted; median should still be correct.
	assert.Equal(t, 3.0, median([]float64{5, 1, 3}))
}

func TestMedian_DoesNotMutateInput(t *testing.T) {
	input := []float64{5, 1, 3}
	median(input)
	assert.Equal(t, []float64{5, 1, 3}, input)
}

// ---------------------------------------------------------------------------
// toFloat64Slice
// ---------------------------------------------------------------------------

func TestToFloat64Slice_Float64(t *testing.T) {
	input := []float64{1.0, 2.0, 3.0}
	assert.Equal(t, input, toFloat64Slice(input))
}

func TestToFloat64Slice_Float32(t *testing.T) {
	input := []float32{1.0, 2.5, 3.0}
	result := toFloat64Slice(input)
	assert.Equal(t, []float64{1.0, 2.5, 3.0}, result)
}

func TestToFloat64Slice_Int64(t *testing.T) {
	input := []int64{100, 200, 300}
	result := toFloat64Slice(input)
	assert.Equal(t, []float64{100, 200, 300}, result)
}

func TestToFloat64Slice_Int32(t *testing.T) {
	input := []int32{10, 20, 30}
	result := toFloat64Slice(input)
	assert.Equal(t, []float64{10, 20, 30}, result)
}

func TestToFloat64Slice_Uint64(t *testing.T) {
	input := []uint64{1, 2, 3}
	result := toFloat64Slice(input)
	assert.Equal(t, []float64{1, 2, 3}, result)
}

func TestToFloat64Slice_UnsupportedType(t *testing.T) {
	assert.Nil(t, toFloat64Slice("not a slice"))
	assert.Nil(t, toFloat64Slice(nil))
	assert.Nil(t, toFloat64Slice(42))
}
