package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/mapping"
	"github.com/alpacahq/marketstore/v4/contrib/massive/metrics"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models"
	modelsenum "github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	millisToNanos = 1000 * 1000
	millisToSecs  = 1000
)

// MakeBarsHandler creates a handler for bar aggregate messages with the specified timeframe.
// The timeframe should be "1Min" or "1Sec" to match the WebSocket subscription.
// Each call receives a single JSON-encoded Aggregate object.
func MakeBarsHandler(timeframe string) func([]byte) {
	return func(msg []byte) {
		if msg == nil {
			return
		}

		var bar Aggregate
		if err := json.Unmarshal(msg, &bar); err != nil {
			log.Warn("[massive] error unmarshalling bar message: %v", err)
			return
		}

		if bar.Symbol == "" {
			return
		}

		epoch := bar.StartTimestamp / millisToSecs

		tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/%s/OHLCV", bar.Symbol, timeframe))
		csm := io.NewColumnSeriesMap()

		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", []int64{epoch})
		cs.AddColumn("Open", []float64{bar.Open})
		cs.AddColumn("High", []float64{bar.High})
		cs.AddColumn("Low", []float64{bar.Low})
		cs.AddColumn("Close", []float64{bar.Close})
		cs.AddColumn("Volume", []uint64{uint64(bar.Volume)})
		csm.AddColumnSeries(*tbk, cs)

		if err := executor.WriteCSM(csm, false); err != nil {
			log.Error("[massive] bar write failure for %v: %v", tbk.String(), err)
		}

		metrics.MassiveStreamLastUpdate.WithLabelValues("bar").SetToCurrentTime()
	}
}

// BarsHandler processes incoming minute-bar aggregate messages from the
// Massive WebSocket API and writes them to MarketStore.
// Deprecated: Use MakeBarsHandler("1Min") instead.
func BarsHandler(msg []byte) {
	MakeBarsHandler("1Min")(msg)
}

// MakeTradeHandler creates a handler for trade messages. It writes the FULL
// trade schema (Exchange, TapeID, Cond1..Cond4, Correction) using models.Trade
// and the shared mapping package, so the live feed encodes ticks identically to
// the REST/flat-file backfill paths and merges cleanly into the existing
// 1Sec/TRADE bucket. exMap maps Massive exchange ids to SIP chars.
func MakeTradeHandler(exMap *mapping.ExchangeMap) func([]byte) {
	return func(msg []byte) {
		if msg == nil {
			return
		}

		var t Trade
		if err := json.Unmarshal(msg, &t); err != nil {
			log.Warn("[massive] error unmarshalling trade message: %v", err)
			return
		}

		if t.Size <= 0 || t.Price <= 0 {
			return
		}

		model := buildTradeModel(&t, exMap)
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write trades csm: %v", err)
			return
		}
		metrics.MassiveStreamLastUpdate.WithLabelValues("trade").SetToCurrentTime()
	}
}

// buildTradeModel converts a decoded WS trade into a single-row models.Trade,
// encoding exchange/tape/conditions via the shared mapping so the row matches
// the backfill schema exactly.
func buildTradeModel(t *Trade, exMap *mapping.ExchangeMap) *models.Trade {
	timestamp := time.Unix(0, int64(millisToNanos*float64(t.Timestamp)))

	// Map Massive integer condition codes to SIP chars, dropping codes with no
	// SIP mapping (functionally inert for consolidation), matching the backfill.
	conditions := make([]modelsenum.TradeCondition, 0, len(t.Conditions))
	for _, c := range t.Conditions {
		if sc, ok := mapping.TradeConditionToSIP(c); ok {
			conditions = append(conditions, sc)
		}
	}

	model := models.NewTrade(strings.Replace(t.Symbol, "/", ".", 1), 1)
	model.Add(
		timestamp.Unix(), timestamp.Nanosecond(),
		modelsenum.Price(t.Price),
		float64(t.Size),
		exMap.Get(t.Exchange),
		mapping.TapeToChar(t.Tape),
		0, // correction: live trade messages carry no correction code
		conditions...,
	)
	return model
}

// MakeQuoteHandler creates a handler for NBBO quote messages. It writes the
// FULL quote schema (BidExchange, AskExchange, Cond, Cond2, Indicators) using
// models.Quote so the live feed merges cleanly into the existing 1Sec/QUOTE
// bucket. The live wire format carries a single condition and no indicators, so
// Cond2/Indicators are written as 0. exMap maps Massive exchange ids to SIP
// chars.
func MakeQuoteHandler(exMap *mapping.ExchangeMap) func([]byte) {
	return func(msg []byte) {
		if msg == nil {
			return
		}

		var q Quote
		if err := json.Unmarshal(msg, &q); err != nil {
			log.Warn("[massive] error unmarshalling quote message: %v", err)
			return
		}

		model := buildQuoteModel(&q, exMap)
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write quotes csm: %v", err)
			return
		}
		metrics.MassiveStreamLastUpdate.WithLabelValues("quote").SetToCurrentTime()
	}
}

// buildQuoteModel converts a decoded WS quote into a single-row models.Quote.
// The live wire format carries a single condition and no indicators, so
// Cond2/Indicators are written as 0. Sizes are in shares (post SEC MDI).
func buildQuoteModel(q *Quote, exMap *mapping.ExchangeMap) *models.Quote {
	timestamp := time.Unix(0, int64(millisToNanos*float64(q.Timestamp)))

	model := models.NewQuote(strings.Replace(q.Symbol, "/", ".", 1), 1)
	model.Add(
		timestamp.Unix(), timestamp.Nanosecond(),
		q.BidPrice, q.AskPrice,
		q.BidSize, q.AskSize,
		exMap.Get(q.BidExchange),
		exMap.Get(q.AskExchange),
		modelsenum.QuoteCondition(q.Condition),
		0, // cond2: not present on the live quote wire format
		0, // indicators: not present on the live quote wire format
	)
	return model
}
