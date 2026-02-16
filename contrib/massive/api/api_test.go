package api

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHistoricAggregatesUnmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"adjusted": true,
		"queryCount": 2,
		"resultsCount": 2,
		"status": "OK",
		"ticker": "AAPL",
		"results": [
			{"c": 75.0875, "h": 75.15, "l": 73.7975, "n": 1, "o": 74.06, "t": 1577941200000, "v": 135647456, "vw": 74.6099},
			{"c": 74.3575, "h": 75.145, "l": 74.125, "n": 1, "o": 74.2875, "t": 1578027600000, "v": 146535512, "vw": 74.7026}
		]
	}`

	var agg HistoricAggregates
	err := json.Unmarshal([]byte(raw), &agg)
	assert.Nil(t, err)
	assert.Equal(t, "AAPL", agg.Ticker)
	assert.Equal(t, "OK", agg.Status)
	assert.True(t, agg.Adjusted)
	assert.Len(t, agg.Results, 2)

	assert.Equal(t, 74.06, agg.Results[0].Open)
	assert.Equal(t, 75.0875, agg.Results[0].Close)
	assert.Equal(t, 75.15, agg.Results[0].High)
	assert.Equal(t, 73.7975, agg.Results[0].Low)
	assert.Equal(t, float64(135647456), agg.Results[0].Volume)
	assert.Equal(t, int64(1577941200000), agg.Results[0].EpochMilliseconds)
}

func TestHistoricTradesUnmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"status": "OK",
		"request_id": "abc123",
		"results": [
			{
				"conditions": [12, 41],
				"exchange": 11,
				"id": "1",
				"participant_timestamp": 1517562000015577000,
				"price": 171.55,
				"sequence_number": 1063,
				"sip_timestamp": 1517562000016036600,
				"size": 100,
				"tape": 3
			}
		]
	}`

	var trades HistoricTrades
	err := json.Unmarshal([]byte(raw), &trades)
	assert.Nil(t, err)
	assert.Equal(t, "OK", trades.Status)
	assert.Len(t, trades.Results, 1)

	tick := trades.Results[0]
	assert.Equal(t, 171.55, tick.Price)
	assert.Equal(t, float64(100), tick.Size)
	assert.Equal(t, 11, tick.Exchange)
	assert.Equal(t, 3, tick.Tape)
	assert.Equal(t, []int{12, 41}, tick.Conditions)
	assert.Equal(t, int64(1517562000016036600), tick.SIPTimestamp)
	assert.Equal(t, "1", tick.ID)
}

func TestHistoricQuotesUnmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"status": "OK",
		"request_id": "abc123",
		"results": [
			{
				"ask_exchange": 0,
				"ask_price": 0,
				"ask_size": 0,
				"bid_exchange": 11,
				"bid_price": 102.7,
				"bid_size": 60,
				"conditions": [1],
				"participant_timestamp": 1517562000065321200,
				"sequence_number": 2060,
				"sip_timestamp": 1517562000065700400,
				"tape": 3
			}
		]
	}`

	var quotes HistoricQuotes
	err := json.Unmarshal([]byte(raw), &quotes)
	assert.Nil(t, err)
	assert.Equal(t, "OK", quotes.Status)
	assert.Len(t, quotes.Results, 1)

	tick := quotes.Results[0]
	assert.Equal(t, 102.7, tick.BidPrice)
	assert.Equal(t, float64(60), tick.BidSize)
	assert.Equal(t, 11, tick.BidExchange)
	assert.Equal(t, float64(0), tick.AskPrice)
	assert.Equal(t, float64(0), tick.AskSize)
	assert.Equal(t, 0, tick.AskExchange)
	assert.Equal(t, []int{1}, tick.Conditions)
	assert.Equal(t, int64(1517562000065700400), tick.SIPTimestamp)
}

func TestListTickersUnmarshal(t *testing.T) {
	t.Parallel()

	raw := `{
		"count": 1,
		"status": "OK",
		"request_id": "e70013",
		"results": [
			{
				"active": true,
				"cik": "0001090872",
				"composite_figi": "BBG000BWQYZ5",
				"currency_name": "usd",
				"locale": "us",
				"market": "stocks",
				"name": "Agilent Technologies Inc.",
				"primary_exchange": "XNYS",
				"ticker": "A",
				"type": "CS"
			}
		]
	}`

	var resp ListTickersResponse
	err := json.Unmarshal([]byte(raw), &resp)
	assert.Nil(t, err)
	assert.Equal(t, "OK", resp.Status)
	assert.Equal(t, 1, resp.Count)
	assert.Len(t, resp.Results, 1)
	assert.Equal(t, "A", resp.Results[0].Ticker)
	assert.Equal(t, "Agilent Technologies Inc.", resp.Results[0].Name)
	assert.Equal(t, "stocks", resp.Results[0].Market)
	assert.True(t, resp.Results[0].Active)
}

func TestAddAPIKey(t *testing.T) {
	t.Parallel()

	apiKey = "test_key"

	// Empty next_url returns empty.
	assert.Equal(t, "", addAPIKey(""))

	// Adds API key to a URL.
	result := addAPIKey("https://api.massive.com/v3/trades/AAPL?cursor=abc123")
	assert.Contains(t, result, "apiKey=test_key")
	assert.Contains(t, result, "cursor=abc123")
}

func TestHistoricAggregatesWithNextURL(t *testing.T) {
	t.Parallel()

	// Test that the response correctly parses next_url for pagination.
	raw := `{
		"adjusted": true,
		"queryCount": 50000,
		"resultsCount": 50000,
		"status": "OK",
		"ticker": "AAPL",
		"next_url": "https://api.massive.com/v2/aggs/ticker/AAPL/range/1/minute/2024-01-01/2024-06-01?cursor=abc123&limit=50000",
		"results": [
			{"c": 75.0875, "h": 75.15, "l": 73.7975, "n": 1, "o": 74.06, "t": 1577941200000, "v": 135647456, "vw": 74.6099}
		]
	}`

	var agg HistoricAggregates
	err := json.Unmarshal([]byte(raw), &agg)
	assert.Nil(t, err)
	assert.Equal(t, "AAPL", agg.Ticker)
	assert.Equal(t, 50000, agg.ResultCount)
	assert.NotEmpty(t, agg.NextURL)
	assert.Contains(t, agg.NextURL, "cursor=abc123")
}
