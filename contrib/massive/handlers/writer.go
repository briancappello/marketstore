package handlers

import (
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

type tradeRecord struct {
	epoch int64
	nanos int32
	px    float64
	sz    uint64
}

type quoteRecord struct {
	epoch int64
	nanos int32
	bidPx float64
	askPx float64
	bidSz uint64
	askSz uint64
}

func writeTrades(writeMap map[io.TimeBucketKey][]tradeRecord) {
	csm := io.NewColumnSeriesMap()

	for tbk, trades := range writeMap {
		epoch := make([]int64, 0, len(trades))
		nanos := make([]int32, 0, len(trades))
		px := make([]float64, 0, len(trades))
		sz := make([]uint64, 0, len(trades))

		for _, tr := range trades {
			epoch = append(epoch, tr.epoch)
			nanos = append(nanos, tr.nanos)
			px = append(px, tr.px)
			sz = append(sz, tr.sz)
		}

		csm.AddColumn(tbk, "Epoch", epoch)
		csm.AddColumn(tbk, "Nanoseconds", nanos)
		csm.AddColumn(tbk, "Price", px)
		csm.AddColumn(tbk, "Size", sz)
	}

	if err := executor.WriteCSM(csm, true); err != nil {
		log.Error("[massive] failed to write trades csm: %v", err)
	}
}

func writeQuotes(writeMap map[io.TimeBucketKey][]quoteRecord) {
	csm := io.NewColumnSeriesMap()

	for tbk, quotes := range writeMap {
		epoch := make([]int64, 0, len(quotes))
		nanos := make([]int32, 0, len(quotes))
		bidPx := make([]float64, 0, len(quotes))
		askPx := make([]float64, 0, len(quotes))
		bidSz := make([]uint64, 0, len(quotes))
		askSz := make([]uint64, 0, len(quotes))

		for _, q := range quotes {
			epoch = append(epoch, q.epoch)
			nanos = append(nanos, q.nanos)
			bidPx = append(bidPx, q.bidPx)
			askPx = append(askPx, q.askPx)
			bidSz = append(bidSz, q.bidSz)
			askSz = append(askSz, q.askSz)
		}

		csm.AddColumn(tbk, "Epoch", epoch)
		csm.AddColumn(tbk, "Nanoseconds", nanos)
		csm.AddColumn(tbk, "BidPrice", bidPx)
		csm.AddColumn(tbk, "AskPrice", askPx)
		csm.AddColumn(tbk, "BidSize", bidSz)
		csm.AddColumn(tbk, "AskSize", askSz)
	}

	if err := executor.WriteCSM(csm, true); err != nil {
		log.Error("[massive] failed to write quotes csm: %v", err)
	}
}
