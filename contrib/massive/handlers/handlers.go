package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/metrics"
	"github.com/alpacahq/marketstore/v4/executor"
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

// TradeHandler processes a single incoming trade message from the Massive
// WebSocket API and writes it to MarketStore.
func TradeHandler(msg []byte) {
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

	timestamp := time.Unix(0, int64(millisToNanos*float64(t.Timestamp)))
	key := fmt.Sprintf("%s/1Sec/TRADE", strings.Replace(t.Symbol, "/", ".", 1))
	tbk := *io.NewTimeBucketKey(key)

	writeMap := map[io.TimeBucketKey][]tradeRecord{
		tbk: {{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			px:    t.Price,
			sz:    uint64(t.Size),
		}},
	}

	writeTrades(writeMap)
	metrics.MassiveStreamLastUpdate.WithLabelValues("trade").SetToCurrentTime()
}

// QuoteHandler processes a single incoming NBBO quote message from the Massive
// WebSocket API and writes it to MarketStore.
func QuoteHandler(msg []byte) {
	if msg == nil {
		return
	}

	var q Quote
	if err := json.Unmarshal(msg, &q); err != nil {
		log.Warn("[massive] error unmarshalling quote message: %v", err)
		return
	}

	timestamp := time.Unix(0, int64(millisToNanos*float64(q.Timestamp)))
	key := fmt.Sprintf("%s/1Min/QUOTE", strings.Replace(q.Symbol, "/", ".", 1))
	tbk := *io.NewTimeBucketKey(key)

	writeMap := map[io.TimeBucketKey][]quoteRecord{
		tbk: {{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			bidPx: q.BidPrice,
			askPx: q.AskPrice,
			bidSz: uint64(q.BidSize),
			askSz: uint64(q.AskSize),
		}},
	}

	writeQuotes(writeMap)
	metrics.MassiveStreamLastUpdate.WithLabelValues("quote").SetToCurrentTime()
}
