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

// BarsHandler processes incoming minute-bar aggregate messages from the
// Massive WebSocket API and writes them to MarketStore.
func BarsHandler(msg []byte) {
	if msg == nil {
		return
	}

	bars := make([]Aggregate, 0)
	if err := json.Unmarshal(msg, &bars); err != nil {
		log.Warn("[massive] error unmarshalling bars message: %v", err)
		return
	}

	for _, bar := range bars {
		if bar.Symbol == "" {
			continue
		}

		epoch := bar.StartTimestamp / millisToSecs

		tbk := io.NewTimeBucketKeyFromString(fmt.Sprintf("%s/1Min/OHLCV", bar.Symbol))
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
	}

	metrics.MassiveStreamLastUpdate.WithLabelValues("bar").SetToCurrentTime()
}

// TradeHandler processes incoming trade messages from the Massive WebSocket
// API and writes them to MarketStore.
func TradeHandler(msg []byte) {
	if msg == nil {
		return
	}

	trades := make([]Trade, 0)
	if err := json.Unmarshal(msg, &trades); err != nil {
		log.Warn("[massive] error unmarshalling trades message: %v", err)
		return
	}

	writeMap := make(map[io.TimeBucketKey][]tradeRecord)

	for _, t := range trades {
		if t.Size <= 0 || t.Price <= 0 {
			continue
		}

		timestamp := time.Unix(0, int64(millisToNanos*float64(t.Timestamp)))
		key := fmt.Sprintf("%s/1Sec/TRADE", strings.Replace(t.Symbol, "/", ".", 1))
		tbk := *io.NewTimeBucketKey(key)

		writeMap[tbk] = append(writeMap[tbk], tradeRecord{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			px:    t.Price,
			sz:    uint64(t.Size),
		})
	}

	writeTrades(writeMap)
	metrics.MassiveStreamLastUpdate.WithLabelValues("trade").SetToCurrentTime()
}

// QuoteHandler processes incoming NBBO quote messages from the Massive
// WebSocket API and writes them to MarketStore.
func QuoteHandler(msg []byte) {
	if msg == nil {
		return
	}

	quotes := make([]Quote, 0)
	if err := json.Unmarshal(msg, &quotes); err != nil {
		log.Warn("[massive] error unmarshalling quotes message: %v", err)
		return
	}

	writeMap := make(map[io.TimeBucketKey][]quoteRecord)

	for _, q := range quotes {
		timestamp := time.Unix(0, int64(millisToNanos*float64(q.Timestamp)))
		key := fmt.Sprintf("%s/1Min/QUOTE", strings.Replace(q.Symbol, "/", ".", 1))
		tbk := *io.NewTimeBucketKey(key)

		writeMap[tbk] = append(writeMap[tbk], quoteRecord{
			epoch: timestamp.Unix(),
			nanos: int32(timestamp.Nanosecond()),
			bidPx: q.BidPrice,
			askPx: q.AskPrice,
			bidSz: uint64(q.BidSize),
			askSz: uint64(q.AskSize),
		})
	}

	writeQuotes(writeMap)
	metrics.MassiveStreamLastUpdate.WithLabelValues("quote").SetToCurrentTime()
}
