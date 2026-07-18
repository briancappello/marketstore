package models

import (
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	quoteSuffix    string = "QUOTE"
	quoteTimeframe string = "1Sec"
)

// Quote defines schema and helper functions for storing Ask-Bid quote data.
//
// The internal buffers are append-style (make([]T, 0, capacity)); use Add to
// append rows and Len to get the row count. This differs from the historical
// index-based API (which required the exact row count up front) and supports
// streaming ingestion where the per-symbol row count is unknown until a ticker
// boundary is reached.
type Quote struct {
	Tbk         *io.TimeBucketKey
	Epoch       []int64
	Nanos       []int32
	BidPrice    []float64
	AskPrice    []float64
	BidSize     []uint64
	AskSize     []uint64
	BidExchange []byte
	AskExchange []byte
	// Cond is the first/primary quote condition (raw Massive int cast to byte).
	Cond []byte
	// Cond2 is the second quote condition (raw Massive int cast to byte).
	Cond2 []byte
	// Indicators is the first quote indicator (raw Massive int cast to byte).
	Indicators []byte

	WriteTime time.Duration
}

// BarBucketKey returns a string bucket key for a given symbol and timeframe.
func QuoteBucketKey(symbol string) string {
	return symbol + "/" + quoteTimeframe + "/" + quoteSuffix
}

// NewQuote creates a new Quote object and initializes its internal column
// buffers to the given capacity (a hint; buffers grow via append as needed).
func NewQuote(symbol string, capacity int) *Quote {
	model := &Quote{
		Tbk: io.NewTimeBucketKey(QuoteBucketKey(symbol)),
	}
	model.make(capacity)
	return model
}

// Key returns the key of the model's time bucket.
func (model *Quote) Key() string {
	return model.Tbk.GetItemKey()
}

// Len returns the length of the internal column buffers.
func (model *Quote) Len() int {
	return len(model.Epoch)
}

// Symbol returns the Symbol part if the TimeBucketKey of this model.
func (model *Quote) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

// make allocates buffers for this model.
func (model *Quote) make(capacity int) {
	model.Epoch = make([]int64, 0, capacity)
	model.Nanos = make([]int32, 0, capacity)
	model.BidPrice = make([]float64, 0, capacity)
	model.AskPrice = make([]float64, 0, capacity)
	model.BidSize = make([]uint64, 0, capacity)
	model.AskSize = make([]uint64, 0, capacity)
	model.BidExchange = make([]byte, 0, capacity)
	model.AskExchange = make([]byte, 0, capacity)
	model.Cond = make([]byte, 0, capacity)
	model.Cond2 = make([]byte, 0, capacity)
	model.Indicators = make([]byte, 0, capacity)
}

// Add appends a new data point to the internal buffers.
// cond and cond2 are the (raw Massive) quote conditions; indicator is the first
// quote indicator. bidSize/askSize are share counts.
func (model *Quote) Add(epoch int64, nanos int, bidPrice, askPrice float64,
	bidSize, askSize int, bidExchange, askExchange enum.Exchange,
	cond enum.QuoteCondition, cond2, indicator byte,
) {
	model.Epoch = append(model.Epoch, epoch)
	model.Nanos = append(model.Nanos, int32(nanos))
	model.BidPrice = append(model.BidPrice, bidPrice)
	model.AskPrice = append(model.AskPrice, askPrice)
	model.BidSize = append(model.BidSize, uint64(bidSize))
	model.AskSize = append(model.AskSize, uint64(askSize))
	model.BidExchange = append(model.BidExchange, byte(bidExchange))
	model.AskExchange = append(model.AskExchange, byte(askExchange))
	model.Cond = append(model.Cond, byte(cond))
	model.Cond2 = append(model.Cond2, cond2)
	model.Indicators = append(model.Indicators, indicator)
}

// BuildCsm prepares an io.ColumnSeriesMap object and populates it's columns with the contents of the internal buffers
// it is included in the .Write() method
// so use only when you need to work with the ColumnSeriesMap before writing it to disk.
func (model *Quote) BuildCsm() *io.ColumnSeriesMap {
	csm := io.NewColumnSeriesMap()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", model.Epoch)
	cs.AddColumn("Nanoseconds", model.Nanos)
	cs.AddColumn("AskPrice", model.AskPrice)
	cs.AddColumn("BidPrice", model.BidPrice)
	cs.AddColumn("AskSize", model.AskSize)
	cs.AddColumn("BidSize", model.BidSize)
	cs.AddColumn("BidExchange", model.BidExchange)
	cs.AddColumn("AskExchange", model.AskExchange)
	cs.AddColumn("Cond", model.Cond)
	cs.AddColumn("Cond2", model.Cond2)
	cs.AddColumn("Indicators", model.Indicators)
	csm.AddColumnSeries(*model.Tbk, cs)
	return &csm
}

// Write persist the internal buffers to disk.
func (model *Quote) Write() error {
	start := time.Now()
	csm := model.BuildCsm()
	err := executor.WriteCSM(*csm, true)
	model.WriteTime = time.Since(start)
	if err != nil {
		log.Error("Failed to write quotes for %s (%+v)", model.Key(), err)
	}
	return err
}
