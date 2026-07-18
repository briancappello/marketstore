package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils"
)

func TestFinalizeVolume(t *testing.T) {
	t.Parallel()
	assert.Equal(t, enum.Size(0), finalizeVolume(0))
	assert.Equal(t, enum.Size(0), finalizeVolume(-1))
	// Positive sub-1 fractional volume clamps up to 1.
	assert.Equal(t, enum.Size(1), finalizeVolume(0.014))
	assert.Equal(t, enum.Size(1), finalizeVolume(0.9))
	// >= 1 truncates toward zero.
	assert.Equal(t, enum.Size(1), finalizeVolume(1.0))
	assert.Equal(t, enum.Size(1), finalizeVolume(1.9))
	assert.Equal(t, enum.Size(10), finalizeVolume(10.5))
}

// TestFromTradesWholeShareUnchanged verifies whole-share trades sum to the same
// integer volume as before the float refactor.
func TestFromTradesWholeShareUnchanged(t *testing.T) {
	t.Parallel()
	symbol := "TEST_WHOLE_SHARE"
	trades := NewTrade(symbol, 4)
	base := time.Date(2020, 11, 20, 10, 3, 0, 0, utils.InstanceConfig.Timezone)
	trades.Add(base.Unix(), 0, 100.0, 10, enum.NYSE, enum.TapeA, 0, enum.RegularSale)
	trades.Add(base.Unix(), 1, 101.0, 5, enum.NYSE, enum.TapeA, 0, enum.RegularSale)

	bars, err := FromTrades(trades, symbol, "1Min")
	assert.Nil(t, err)
	assert.Len(t, bars.Epoch, 1)
	assert.Equal(t, enum.Size(15), bars.Volume[0])
}

// TestFromTradesFractionalEmitsBar verifies a bucket of only fractional-share
// trades now emits a bar with volume 1 (previously suppressed when sizes
// truncated to 0 at ingest).
func TestFromTradesFractionalEmitsBar(t *testing.T) {
	t.Parallel()
	symbol := "TEST_FRACTIONAL"
	trades := NewTrade(symbol, 4)
	base := time.Date(2020, 11, 20, 10, 3, 0, 0, utils.InstanceConfig.Timezone)
	trades.Add(base.Unix(), 0, 100.0, 0.014, enum.NYSE, enum.TapeA, 0, enum.RegularSale)
	trades.Add(base.Unix(), 1, 100.0, 0.9, enum.NYSE, enum.TapeA, 0, enum.RegularSale)

	bars, err := FromTrades(trades, symbol, "1Min")
	assert.Nil(t, err)
	assert.Len(t, bars.Epoch, 1)
	// 0.014 + 0.9 = 0.914 -> finalizeVolume -> 1.
	assert.Equal(t, enum.Size(1), bars.Volume[0])
}

func TestTradeBuildCsmColumns(t *testing.T) {
	t.Parallel()
	tr := NewTrade("TEST_TRADE_CSM", 2)
	tr.Add(1000, 5, 12.5, 3.5, enum.NYSE, enum.TapeA, 1, enum.RegularSale, enum.OddLotTrade)
	cs := tr.GetCs()

	size, ok := cs.GetColumn("Size").([]float64)
	assert.True(t, ok, "Size column should be []float64")
	assert.Equal(t, []float64{3.5}, size)

	corr, ok := cs.GetColumn("Correction").([]byte)
	assert.True(t, ok, "Correction column should be []byte")
	assert.Equal(t, []byte{1}, corr)
}

func TestQuoteAppendAndBuildCsm(t *testing.T) {
	t.Parallel()
	q := NewQuote("TEST_QUOTE_CSM", 2)
	q.Add(1000, 5, 10.0, 10.5, 100, 200, enum.NYSE, enum.Nasdaq,
		enum.QuoteCondition(1), 2, 3)
	q.Add(1001, 6, 11.0, 11.5, 300, 400, enum.NYSEArca, enum.CboeBZX,
		enum.QuoteCondition(4), 5, 6)

	assert.Equal(t, 2, q.Len())

	csm := q.BuildCsm()
	cs, ok := (*csm)[*q.Tbk]
	assert.True(t, ok)

	cond2, ok := cs.GetColumn("Cond2").([]byte)
	assert.True(t, ok, "Cond2 column should be []byte")
	assert.Equal(t, []byte{2, 5}, cond2)

	indicators, ok := cs.GetColumn("Indicators").([]byte)
	assert.True(t, ok, "Indicators column should be []byte")
	assert.Equal(t, []byte{3, 6}, indicators)

	bidSize, ok := cs.GetColumn("BidSize").([]uint64)
	assert.True(t, ok)
	assert.Equal(t, []uint64{100, 300}, bidSize)
}
