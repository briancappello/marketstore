package models

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	barSuffix      = "OHLCV"
	barSuffixNoVol = "OHLC"
	oneDay         = 24 * time.Hour

	// IndexPrefix is the prefix for index symbols. Symbols starting with
	// this prefix are treated as indices (no volume data) and use the OHLC
	// attribute group instead of OHLCV.
	IndexPrefix = "^"
)

// IsIndex returns true if the symbol represents an index (prefixed with "^").
// Index symbols use the OHLC attribute group (no Volume column).
func IsIndex(symbol string) bool {
	return strings.HasPrefix(symbol, IndexPrefix)
}

// Bar is a data model to persist arrays of OHLCV (or OHLC for indices) bar data.
type Bar struct {
	Tbk                    *io.TimeBucketKey
	Csm                    io.ColumnSeriesMap
	Epoch                  []int64
	Open, High, Low, Close []enum.Price
	Volume                 []enum.Size
	WriteTime              time.Duration
	hasVolume              bool
}

// BarBucketKey returns a string bucket key for a given symbol and timeframe.
// Index symbols (prefixed with "^") use the OHLC attribute group; all others
// use OHLCV.
func BarBucketKey(symbol, timeframe string) string {
	suffix := barSuffix
	if IsIndex(symbol) {
		suffix = barSuffixNoVol
	}
	return symbol + "/" + timeframe + "/" + suffix
}

// NewBar creates a new Bar object and initializes its internal column buffers
// to the given capacity. For index symbols (prefixed with "^"), the bar uses
// the OHLC attribute group and does not allocate or store Volume data.
func NewBar(symbol, timeframe string, capacity int) *Bar {
	model := &Bar{
		Tbk:       io.NewTimeBucketKey(BarBucketKey(symbol, timeframe)),
		Csm:       io.NewColumnSeriesMap(),
		hasVolume: !IsIndex(symbol),
	}
	model.make(capacity)
	return model
}

// HasVolume reports whether this bar includes volume data.
func (model *Bar) HasVolume() bool {
	return model.hasVolume
}

// Key returns the key of the model's time bucket.
func (model *Bar) Key() string {
	return model.Tbk.GetItemKey()
}

// Len returns the length of the internal column buffers.
func (model *Bar) Len() int {
	return len(model.Epoch)
}

// Symbol returns the Symbol part if the TimeBucketKey of this model.
func (model *Bar) Symbol() string {
	return model.Tbk.GetItemInCategory("Symbol")
}

// make allocates buffers for this model.
func (model *Bar) make(capacity int) {
	model.Epoch = make([]int64, 0, capacity)
	model.Open = make([]enum.Price, 0, capacity)
	model.High = make([]enum.Price, 0, capacity)
	model.Low = make([]enum.Price, 0, capacity)
	model.Close = make([]enum.Price, 0, capacity)
	if model.hasVolume {
		model.Volume = make([]enum.Size, 0, capacity)
	}
}

// Add adds a new data point to the internal buffers, and increments the internal index by one.
// For bars without volume (index symbols), the volume argument is ignored.
func (model *Bar) Add(epoch int64, open, high, low, clos enum.Price, volume enum.Size) {
	model.Epoch = append(model.Epoch, epoch)
	model.Open = append(model.Open, open)
	model.High = append(model.High, high)
	model.Low = append(model.Low, low)
	model.Close = append(model.Close, clos)
	if model.hasVolume {
		model.Volume = append(model.Volume, volume)
	}
}

func (model *Bar) GetCs() *io.ColumnSeries {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", model.Epoch)
	cs.AddColumn("Open", model.Open)
	cs.AddColumn("High", model.High)
	cs.AddColumn("Low", model.Low)
	cs.AddColumn("Close", model.Close)
	if model.hasVolume {
		cs.AddColumn("Volume", model.Volume)
	}
	return cs
}

// BuildCsm prepares an io.ColumnSeriesMap object and populates it's columns with the contents of the internal buffers
// it is included in the .Write() method
// so use only when you need to work with the ColumnSeriesMap before writing it to disk.
func (model *Bar) BuildCsm() *io.ColumnSeriesMap {
	csm := io.NewColumnSeriesMap()
	cs := model.GetCs()
	csm.AddColumnSeries(*model.Tbk, cs)
	return &csm
}

// Write persist the internal buffers to disk.
func (model *Bar) Write() error {
	start := time.Now()
	csm := model.BuildCsm()
	err := executor.WriteCSM(*csm, false)
	model.WriteTime = time.Since(start)
	if err != nil {
		log.Error("Failed to write bars for %s (%+v)", model.Key(), err)
	}
	return err
}

type ConsolidatedUpdateInfo struct {
	UpdateHighLow bool
	UpdateLast    bool
	UpdateVolume  bool
}

// https://polygon.io/glossary/us/stocks/conditions-indicators

var ConditionToUpdateInfo = map[enum.TradeCondition]ConsolidatedUpdateInfo{
	enum.NoTradeCondition:   {true, true, true},
	enum.RegularSale:        {true, true, true},   // Regular Sale
	enum.Acquisition:        {true, true, true},   // Acquisition
	enum.AveragePriceTrade:  {false, false, true}, // Average Price Trade
	enum.AutomaticExecution: {true, true, true},   // Automatic Execution
	enum.BunchedTrade:       {true, true, true},   // Bunched Trade
	enum.BunchedSoldTrade:   {true, false, true},  // Bunched Sold Trade
	// 6: {?, ?, ? },  //  CAP Election
	enum.CashSale:           {false, false, true}, // Cash Sale
	enum.ClosingPrints:      {true, true, true},   // Closing Prints
	enum.CrossTrade:         {true, true, true},   // Cross Trade
	enum.DerivativelyPriced: {true, false, true},  // Derivatively Priced
	enum.Distribution:       {true, true, true},   // Distribution
	//	12: {false, false, true},  // XXX: Form T is disabled with the purpose to include extended hours data in mkts
	enum.ExtendedHoursTrade:        {false, false, true},  // Extended Trading Hours (Sold Out of Sequence)
	enum.IntermarketSweep:          {true, true, true},    // Intermarket Sweep
	enum.MarketCenterOfficialClose: {false, false, false}, // Market Center Official Close
	enum.MarketCenterOfficialOpen:  {false, false, false}, // Market Center Official Open
	// 17: {?, ?, ?}, // Market Center Opening Trade
	// 18: {?, ?, ?}, // Market Center Reopening Trade
	// 19: {?, ?, ?}, // Market Center Closing Trade
	enum.NextDay:             {false, false, true}, // Next Day
	enum.PriceVariationTrade: {false, false, true}, // Price Variation Trade
	enum.PriorReferencePrice: {true, false, true},  // Prior Reference Price
	enum.Rule155Trade:        {true, true, true},   // Rule 155 Trade (AMEX)
	// 24: {?, ?, ?}, // Rule 127 NYSE
	enum.OpeningPrints: {true, true, true}, // Opening Prints
	// 26: {?, ?, ?}, // Opened
	enum.StoppedStock:    {true, true, true},  // Stopped Stock (Regular Trade)
	enum.ReopeningPrints: {true, true, true},  // Re-Opening Prints
	enum.Seller:          {true, false, true}, // Seller
	enum.SoldLast:        {true, true, true},  // Sold Last
	// 32: {?, ?, ?}, // Sold Out
	enum.SoldOutOfSequence: {true, false, true}, // Sold (out of Sequence)
	enum.SplitTrade:        {true, true, true},  // Split Trade
	// 35: {?, ?, ?},  // Stock option
	enum.YellowFlagRegularTrade:     {true, true, true},   // Yellow Flag Regular Trade
	enum.OddLotTrade:                {false, false, true}, // Odd Lot Trade
	enum.CorrectedConsolidatedClose: {true, true, false},  // Corrected Consolidated Close (per listing market)
	// 39: {?, ?, ?}, // Unknown
	// 40: {?, ?, ?}, // Held
	// 41: {?, ?, ?}, // Trade Thru Exempt
	// 42: {?, ?, ?}, // NonEligible
	// 43: {?, ?, ?}, // NonEligible Extended
	// 44: {?, ?, ?}, // Canceled
	// 45: {?, ?, ?}, // Recovery
	// 46: {?, ?, ?}, // Correction
	// 47: {?, ?, ?}, // As of
	// 48: {?, ?, ?}, // As of Correction
	// 49: {?, ?, ?}, // As of Cancel
	// 50: {?, ?, ?}, // OOB
	// 51: {?, ?, ?}, // Summary
	enum.ContingentTrade:          {false, false, true}, // Contingent Trade
	enum.QualifiedContingentTrade: {false, false, true}, // Qualified Contingent Trade ("QCT")
	// 54: {?, ?, ?}, // Errored
	// 55: {?, ?, ?}, // OPENING_REOPENING_TRADE_DETAIL
	// 56: {TBD, TBD, TBD}, // Placeholder
	// 59: {TBD, TBD, TBD}, // Placeholder for 611 exempt
}

func conditionToUpdateInfo(conditions []enum.TradeCondition) ConsolidatedUpdateInfo {
	r := ConsolidatedUpdateInfo{true, true, true}

	for _, condition := range conditions {
		if val, ok := ConditionToUpdateInfo[condition]; ok {
			r.UpdateHighLow = r.UpdateHighLow && val.UpdateHighLow
			r.UpdateLast = r.UpdateLast && val.UpdateLast
			r.UpdateVolume = r.UpdateVolume && val.UpdateVolume
		}
	}

	return r
}

var bucketDurations = map[string]time.Duration{
	"1Sec": time.Second,
	"1Min": time.Minute,
	"1H":   time.Hour,
	"1D":   oneDay,
}

// finalizeVolume converts an accumulated float64 trade volume into the
// enum.Size (uint64) stored on a bar. A positive sub-1 volume (fractional-share
// activity) is clamped up to 1 so that "something traded" is distinguishable
// from "nothing traded" (which remains 0); larger values truncate toward zero.
func finalizeVolume(v float64) enum.Size {
	if v <= 0 {
		return 0
	}
	if iv := enum.Size(v); iv == 0 {
		return 1
	}
	return enum.Size(v)
}

func FromTrades(trades *Trade, symbol, timeframe string) (*Bar, error) {
	bar := NewBar(symbol, timeframe, len(trades.Epoch))

	bucketDuration, found := bucketDurations[timeframe]
	if !found {
		return nil, fmt.Errorf("unsupported timeframe: %v", timeframe)
	}

	var epoch int64
	var open, high, low, clos enum.Price
	var volume float64
	lastBucketTimestamp := time.Time{}

	marketCenterOfficialCloseProcessed := false
	for i, price := range trades.Price {
		timestamp := time.Unix(trades.Epoch[i], int64(trades.Nanos[i]))
		bucketTimestamp := timestamp.Truncate(bucketDuration)

		if bucketTimestamp.Before(lastBucketTimestamp) {
			log.Warn("[Bar.FromTrades] got an out-of-order tick: %v %v %v %v (last: %v), skipping",
				timestamp, bar.Symbol(), trades.Price[i], trades.Size[i], lastBucketTimestamp)
			continue
		}

		if !lastBucketTimestamp.Equal(bucketTimestamp) {
			if open != 0 && volume != 0 {
				bar.Add(epoch, open, high, low, clos, finalizeVolume(volume))
			}

			lastBucketTimestamp = bucketTimestamp
			epoch = bucketTimestamp.Unix()
			open = 0
			high = 0
			low = math.MaxFloat64
			clos = 0
			volume = 0
			marketCenterOfficialCloseProcessed = false
		}

		var conditions []enum.TradeCondition
		if len(trades.Cond1) > i {
			conditions = append(conditions, trades.Cond1[i])
		}
		if len(trades.Cond2) > i {
			conditions = append(conditions, trades.Cond2[i])
		}
		if len(trades.Cond3) > i {
			conditions = append(conditions, trades.Cond3[i])
		}
		if len(trades.Cond4) > i {
			conditions = append(conditions, trades.Cond4[i])
		}

		if timeframe == "1D" {
			for _, condition := range conditions {
				if condition == enum.MarketCenterOfficialOpen {
					open = price
				}
				if condition == enum.MarketCenterOfficialClose {
					clos = price
					volume = trades.Size[i]
					marketCenterOfficialCloseProcessed = true
				}
			}
		}

		updateInfo := conditionToUpdateInfo(conditions)

		if !updateInfo.UpdateLast && !updateInfo.UpdateHighLow && !updateInfo.UpdateVolume {
			continue
		}

		if updateInfo.UpdateHighLow {
			if high < price {
				high = price
			}
			if low > price {
				low = price
			}
		}

		if updateInfo.UpdateLast {
			if open == 0 {
				open = price
			}

			if timeframe != "1D" {
				clos = price
			} else if timeframe == "1D" && !marketCenterOfficialCloseProcessed {
				clos = price
			}
		}

		if timeframe != "1D" || !marketCenterOfficialCloseProcessed {
			if updateInfo.UpdateVolume {
				volume += trades.Size[i]
			}
		}
	}

	if open != 0 && volume != 0 {
		bar.Add(epoch, open, high, low, clos, finalizeVolume(volume))
	}

	return bar, nil
}
