package flatfiles

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/mapping"
	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/models/enum"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Quote CSV column names from the Massive flat files.
const (
	colAskExchange = "ask_exchange"
	colAskPrice    = "ask_price"
	colAskSize     = "ask_size"
	colBidExchange = "bid_exchange"
	colBidPrice    = "bid_price"
	colBidSize     = "bid_size"
	colIndicators  = "indicators"
)

// quoteRoundLotCutoff is the date on/after which Massive reports quote bid/ask
// sizes in shares; before it, sizes are round lots (SEC MDI transition).
var quoteRoundLotCutoff = time.Date(2025, 11, 3, 0, 0, 0, 0, time.UTC)

// quoteColumnIndex holds CSV column positions for quote fields.
type quoteColumnIndex struct {
	ticker      int
	askExchange int
	askPrice    int
	askSize     int
	bidExchange int
	bidPrice    int
	bidSize     int
	conditions  int
	indicators  int
	sipTime     int
}

func buildQuoteColumnIndex(header []string) (quoteColumnIndex, error) {
	idx := quoteColumnIndex{
		ticker: -1, askExchange: -1, askPrice: -1, askSize: -1,
		bidExchange: -1, bidPrice: -1, bidSize: -1,
		conditions: -1, indicators: -1, sipTime: -1,
	}
	for i, col := range header {
		switch col {
		case colTicker:
			idx.ticker = i
		case colAskExchange:
			idx.askExchange = i
		case colAskPrice:
			idx.askPrice = i
		case colAskSize:
			idx.askSize = i
		case colBidExchange:
			idx.bidExchange = i
		case colBidPrice:
			idx.bidPrice = i
		case colBidSize:
			idx.bidSize = i
		case colConditions:
			idx.conditions = i
		case colIndicators:
			idx.indicators = i
		case colSIPTime:
			idx.sipTime = i
		}
	}
	if idx.ticker < 0 || idx.askPrice < 0 || idx.askSize < 0 || idx.bidPrice < 0 ||
		idx.bidSize < 0 || idx.askExchange < 0 || idx.bidExchange < 0 || idx.sipTime < 0 {
		return idx, fmt.Errorf("missing required quote CSV column(s)")
	}
	return idx, nil
}

// normalizeQuoteSizeShares converts a raw Massive quote size to shares given the
// quote timestamp and the symbol's round lot.
func normalizeQuoteSizeShares(rawSize float64, ts time.Time, roundLot int) uint64 {
	if ts.Before(quoteRoundLotCutoff) {
		if roundLot <= 0 {
			roundLot = 100
		}
		return uint64(rawSize * float64(roundLot))
	}
	return uint64(rawSize)
}

// ParseQuotesStream reads a decompressed quotes CSV stream, filters by
// symbolSet, normalizes sizes to shares using roundLot, and invokes emit once
// per ticker boundary with that symbol's CSM.
func ParseQuotesStream(
	r io.Reader,
	symbolSet map[string]bool,
	exMap *mapping.ExchangeMap,
	roundLot func(sym string) int,
	date time.Time,
	emit func(utilsio.ColumnSeriesMap),
) (ParseStats, error) {
	br := bufio.NewReaderSize(r, csvBufferSize)
	csvReader := csv.NewReader(br)
	csvReader.ReuseRecord = true

	header, err := csvReader.Read()
	if err != nil {
		return ParseStats{}, fmt.Errorf("read quotes CSV header: %w", err)
	}
	colIdx, err := buildQuoteColumnIndex(header)
	if err != nil {
		return ParseStats{}, err
	}

	var stats ParseStats
	var currentTicker string
	var currentQuote *models.Quote
	var currentRoundLot int
	dateStr := date.Format(dateFormat)

	flush := func() {
		if currentQuote == nil || currentQuote.Len() == 0 {
			return
		}
		stats.SymbolCount++
		emit(*currentQuote.BuildCsm())
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			var parseErr *csv.ParseError
			if errors.As(err, &parseErr) {
				log.Warn("[flatfiles] skipping malformed quote row on %s: %v", dateStr, err)
				continue
			}
			return stats, fmt.Errorf("read quotes CSV on %s: %w", dateStr, err)
		}
		stats.RowsRead++

		ticker := NormalizeTicker(record[colIdx.ticker])
		if symbolSet != nil && !symbolSet[ticker] {
			continue
		}

		if ticker != currentTicker {
			flush()
			currentTicker = ticker
			currentQuote = models.NewQuote(ticker, tickCapacity)
			currentRoundLot = 100
			if roundLot != nil {
				currentRoundLot = roundLot(ticker)
			}
		}

		epoch, nanos, bidPrice, askPrice, bidSize, askSize,
			bidExchange, askExchange, cond, cond2, indicator, perr :=
			parseQuoteRow(record, colIdx, exMap, currentRoundLot)
		if perr != nil {
			log.Warn("[flatfiles] skipping bad quote row for %s on %s: %v", ticker, dateStr, perr)
			continue
		}
		stats.RowsMatched++

		currentQuote.Add(epoch, nanos, bidPrice, askPrice, int(bidSize), int(askSize),
			bidExchange, askExchange, cond, cond2, indicator)
	}

	flush()
	return stats, nil
}

func parseQuoteRow(record []string, idx quoteColumnIndex, exMap *mapping.ExchangeMap, roundLot int) (
	epoch int64, nanos int, bidPrice, askPrice float64, bidSize, askSize uint64,
	bidExchange, askExchange enum.Exchange, cond enum.QuoteCondition,
	cond2, indicator byte, err error,
) {
	sipNanos, err := strconv.ParseInt(record[idx.sipTime], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("parse sip_timestamp: %w", err)
	}
	ts := time.Unix(0, sipNanos)
	epoch = ts.Unix()
	nanos = ts.Nanosecond()

	bidPrice, err = strconv.ParseFloat(record[idx.bidPrice], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("parse bid_price: %w", err)
	}
	askPrice, err = strconv.ParseFloat(record[idx.askPrice], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("parse ask_price: %w", err)
	}

	bidSizeRaw, err := strconv.ParseFloat(record[idx.bidSize], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("parse bid_size: %w", err)
	}
	askSizeRaw, err := strconv.ParseFloat(record[idx.askSize], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("parse ask_size: %w", err)
	}
	bidSize = normalizeQuoteSizeShares(bidSizeRaw, ts, roundLot)
	askSize = normalizeQuoteSizeShares(askSizeRaw, ts, roundLot)

	bidExInt, err := strconv.Atoi(strings.TrimSpace(record[idx.bidExchange]))
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("parse bid_exchange: %w", err)
	}
	askExInt, err := strconv.Atoi(strings.TrimSpace(record[idx.askExchange]))
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("parse ask_exchange: %w", err)
	}
	bidExchange = exMap.Get(bidExInt)
	askExchange = exMap.Get(askExInt)

	// Quote conditions/indicators have no SIP mapping; store raw ints as bytes.
	if idx.conditions >= 0 {
		conds := parseConditionInts(record[idx.conditions])
		if len(conds) > 0 {
			cond = enum.QuoteCondition(conds[0])
		}
		if len(conds) > 1 {
			cond2 = byte(conds[1])
		}
	}
	if idx.indicators >= 0 {
		inds := parseConditionInts(record[idx.indicators])
		if len(inds) > 0 {
			indicator = byte(inds[0])
		}
	}

	return epoch, nanos, bidPrice, askPrice, bidSize, askSize,
		bidExchange, askExchange, cond, cond2, indicator, nil
}
