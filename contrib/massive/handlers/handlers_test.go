package handlers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregateUnmarshal(t *testing.T) {
	t.Parallel()

	raw := `[{
		"ev": "AM",
		"sym": "GTE",
		"v": 4110,
		"av": 9470157,
		"op": 0.4372,
		"vw": 0.4488,
		"o": 0.4488,
		"c": 0.4486,
		"h": 0.4489,
		"l": 0.4486,
		"a": 0.4352,
		"z": 685,
		"s": 1610144640000,
		"e": 1610144700000
	}]`

	var bars []Aggregate
	err := json.Unmarshal([]byte(raw), &bars)
	assert.Nil(t, err)
	assert.Len(t, bars, 1)
	assert.Equal(t, "GTE", bars[0].Symbol)
	assert.Equal(t, 0.4488, bars[0].Open)
	assert.Equal(t, 0.4486, bars[0].Close)
	assert.Equal(t, 0.4489, bars[0].High)
	assert.Equal(t, 0.4486, bars[0].Low)
	assert.Equal(t, 4110, bars[0].Volume)
	assert.Equal(t, int64(1610144640000), bars[0].StartTimestamp)
	assert.Equal(t, int64(1610144700000), bars[0].EndTimestamp)
}

func TestTradeUnmarshal(t *testing.T) {
	t.Parallel()

	raw := `[{
		"ev": "T",
		"sym": "MSFT",
		"x": 4,
		"i": "12345",
		"z": 3,
		"p": 114.125,
		"s": 100,
		"c": [0, 12],
		"t": 1536036818784,
		"q": 3681328
	}]`

	var trades []Trade
	err := json.Unmarshal([]byte(raw), &trades)
	assert.Nil(t, err)
	assert.Len(t, trades, 1)
	assert.Equal(t, "MSFT", trades[0].Symbol)
	assert.Equal(t, 114.125, trades[0].Price)
	assert.Equal(t, 100, trades[0].Size)
	assert.Equal(t, 4, trades[0].Exchange)
	assert.Equal(t, 3, trades[0].Tape)
	assert.Equal(t, []int{0, 12}, trades[0].Conditions)
	assert.Equal(t, int64(1536036818784), trades[0].Timestamp)
}

func TestQuoteUnmarshal(t *testing.T) {
	t.Parallel()

	raw := `[{
		"ev": "Q",
		"sym": "MSFT",
		"bx": 4,
		"bp": 114.125,
		"bs": 100,
		"ax": 7,
		"ap": 114.128,
		"as": 160,
		"c": 0,
		"t": 1536036818784,
		"q": 50385480,
		"z": 3
	}]`

	var quotes []Quote
	err := json.Unmarshal([]byte(raw), &quotes)
	assert.Nil(t, err)
	assert.Len(t, quotes, 1)
	assert.Equal(t, "MSFT", quotes[0].Symbol)
	assert.Equal(t, 114.125, quotes[0].BidPrice)
	assert.Equal(t, 114.128, quotes[0].AskPrice)
	assert.Equal(t, 100, quotes[0].BidSize)
	assert.Equal(t, 160, quotes[0].AskSize)
	assert.Equal(t, 4, quotes[0].BidExchange)
	assert.Equal(t, 7, quotes[0].AskExchange)
	assert.Equal(t, int64(1536036818784), quotes[0].Timestamp)
}
