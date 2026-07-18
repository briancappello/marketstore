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

// Trade CSV column names from the Massive flat files.
const (
	colConditions = "conditions"
	colCorrection = "correction"
	colExchange   = "exchange"
	colPrice      = "price"
	colSIPTime    = "sip_timestamp"
	colSize       = "size"
	colTape       = "tape"
)

// tickCapacity is the initial per-symbol slice capacity for tick models. A few
// thousand handles a low-volume symbol-day without reallocation; busy symbols
// grow via append.
const tickCapacity = 4096

// csvBufferSize is the scanner buffer used for the streaming csv reader. Quote
// rows in particular can be long, so it is set generously.
const csvBufferSize = 1 << 20 // 1 MiB

// tradeColumnIndex holds CSV column positions for trade fields.
type tradeColumnIndex struct {
	ticker     int
	conditions int
	correction int
	exchange   int
	price      int
	sipTime    int
	size       int
	tape       int
}

func buildTradeColumnIndex(header []string) (tradeColumnIndex, error) {
	idx := tradeColumnIndex{
		ticker: -1, conditions: -1, correction: -1, exchange: -1,
		price: -1, sipTime: -1, size: -1, tape: -1,
	}
	for i, col := range header {
		switch col {
		case colTicker:
			idx.ticker = i
		case colConditions:
			idx.conditions = i
		case colCorrection:
			idx.correction = i
		case colExchange:
			idx.exchange = i
		case colPrice:
			idx.price = i
		case colSIPTime:
			idx.sipTime = i
		case colSize:
			idx.size = i
		case colTape:
			idx.tape = i
		}
	}
	// Required columns (conditions/correction are optional).
	if idx.ticker < 0 || idx.price < 0 || idx.sipTime < 0 || idx.size < 0 ||
		idx.exchange < 0 || idx.tape < 0 {
		return idx, fmt.Errorf("missing required trade CSV column(s)")
	}
	return idx, nil
}

// parseConditionInts parses a comma-separated integer condition list (e.g.
// "14,12,37,41"). Empty string yields no values.
func parseConditionInts(s string) []int {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			continue
		}
		out = append(out, v)
	}
	return out
}

// ParseTradesStream reads a decompressed trades CSV stream, filters by
// symbolSet, and invokes emit once per ticker boundary with that symbol's CSM.
// The CSV is expected to be grouped by ticker (sorted) so per-symbol emission
// requires no whole-day accumulation.
func ParseTradesStream(
	r io.Reader,
	symbolSet map[string]bool,
	exMap *mapping.ExchangeMap,
	date time.Time,
	emit func(utilsio.ColumnSeriesMap),
) (ParseStats, error) {
	br := bufio.NewReaderSize(r, csvBufferSize)
	csvReader := csv.NewReader(br)
	csvReader.ReuseRecord = true

	header, err := csvReader.Read()
	if err != nil {
		return ParseStats{}, fmt.Errorf("read trades CSV header: %w", err)
	}
	colIdx, err := buildTradeColumnIndex(header)
	if err != nil {
		return ParseStats{}, err
	}

	var stats ParseStats
	var currentTicker string
	var currentTrade *models.Trade
	dateStr := date.Format(dateFormat)

	flush := func() {
		if currentTrade == nil || currentTrade.Len() == 0 {
			return
		}
		stats.SymbolCount++
		csm := utilsio.NewColumnSeriesMap()
		csm.AddColumnSeries(*currentTrade.Tbk, currentTrade.GetCs())
		emit(csm)
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			var parseErr *csv.ParseError
			if errors.As(err, &parseErr) {
				log.Warn("[flatfiles] skipping malformed trade row on %s: %v", dateStr, err)
				continue
			}
			return stats, fmt.Errorf("read trades CSV on %s: %w", dateStr, err)
		}
		stats.RowsRead++

		ticker := NormalizeTicker(record[colIdx.ticker])
		if symbolSet != nil && !symbolSet[ticker] {
			continue
		}

		epoch, nanos, price, size, exchange, tape, correction, conditions, perr :=
			parseTradeRow(record, colIdx, exMap)
		if perr != nil {
			log.Warn("[flatfiles] skipping bad trade row for %s on %s: %v", ticker, dateStr, perr)
			continue
		}
		stats.RowsMatched++

		if ticker != currentTicker {
			flush()
			currentTicker = ticker
			currentTrade = models.NewTrade(ticker, tickCapacity)
		}

		currentTrade.Add(epoch, nanos, price, size, exchange, tape, correction, conditions...)
	}

	flush()
	return stats, nil
}

func parseTradeRow(record []string, idx tradeColumnIndex, exMap *mapping.ExchangeMap) (
	epoch int64, nanos int, price enum.Price, size float64,
	exchange enum.Exchange, tape enum.Tape, correction byte,
	conditions []enum.TradeCondition, err error,
) {
	sipNanos, err := strconv.ParseInt(record[idx.sipTime], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, nil, fmt.Errorf("parse sip_timestamp: %w", err)
	}
	ts := time.Unix(0, sipNanos)
	epoch = ts.Unix()
	nanos = ts.Nanosecond()

	priceF, err := strconv.ParseFloat(record[idx.price], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, nil, fmt.Errorf("parse price: %w", err)
	}
	price = enum.Price(priceF)

	size, err = strconv.ParseFloat(record[idx.size], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, nil, fmt.Errorf("parse size: %w", err)
	}

	exInt, err := strconv.Atoi(strings.TrimSpace(record[idx.exchange]))
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, nil, fmt.Errorf("parse exchange: %w", err)
	}
	exchange = exMap.Get(exInt)

	tapeInt, err := strconv.Atoi(strings.TrimSpace(record[idx.tape]))
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, nil, fmt.Errorf("parse tape: %w", err)
	}
	tape = mapping.TapeToChar(tapeInt)

	if idx.correction >= 0 {
		if c, cerr := strconv.Atoi(strings.TrimSpace(record[idx.correction])); cerr == nil {
			correction = byte(c)
		}
	}

	if idx.conditions >= 0 {
		for _, code := range parseConditionInts(record[idx.conditions]) {
			if sc, ok := mapping.TradeConditionToSIP(code); ok {
				conditions = append(conditions, sc)
				if len(conditions) == 4 { // model stores at most 4
					break
				}
			}
		}
	}

	return epoch, nanos, price, size, exchange, tape, correction, conditions, nil
}
