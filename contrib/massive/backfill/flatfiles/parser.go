package flatfiles

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/models/enum"
	utilsio "github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// CSV column names from the Massive flat files.
const (
	colTicker       = "ticker"
	colVolume       = "volume"
	colOpen         = "open"
	colClose        = "close"
	colHigh         = "high"
	colLow          = "low"
	colWindowStart  = "window_start"
	colTransactions = "transactions"
)

// nanosPerSecond is the divisor to convert Unix nanosecond timestamps to seconds.
const nanosPerSecond = 1_000_000_000

// BarCapacity returns the initial slice capacity for a Bar based on timeframe.
// 1D bars have 1 row per symbol per file. 1Min bars cover extended hours
// trading (4:00 AM - 8:00 PM ET = 16 hours = 960 minutes).
func BarCapacity(timeframe string) int {
	switch timeframe {
	case "1D":
		return 1
	case "1Min":
		return 960
	default:
		return 512
	}
}

// ParseStats tracks statistics from a single file parse operation.
type ParseStats struct {
	RowsRead    int
	RowsMatched int
	SymbolCount int
}

// ParseAndWrite reads a decompressed CSV stream from a Massive flat file,
// filters rows by the symbol set, and returns a single ColumnSeriesMap
// containing all matched symbols' bar data for bulk writing.
//
// The CSV is expected to be sorted by ticker, which allows streaming: we
// accumulate bars for the current symbol and flush into the CSM when the
// ticker changes.
//
// symbolSet maps ticker strings to true for symbols we want. If nil, all
// symbols are accepted. timeframe should be "1D" or "1Min".
func ParseAndWrite(
	reader io.Reader,
	symbolSet map[string]bool,
	timeframe string,
	date time.Time,
) (utilsio.ColumnSeriesMap, ParseStats, error) {
	csvReader := csv.NewReader(reader)
	csvReader.ReuseRecord = true

	// Read and parse header row.
	header, err := csvReader.Read()
	if err != nil {
		return nil, ParseStats{}, fmt.Errorf("read CSV header: %w", err)
	}

	colIdx, err := buildColumnIndex(header)
	if err != nil {
		return nil, ParseStats{}, err
	}

	csm := utilsio.NewColumnSeriesMap()
	var stats ParseStats
	var currentTicker string
	var currentBar *models.Bar
	capacity := BarCapacity(timeframe)

	flushBar := func() {
		if currentBar == nil || currentBar.Len() == 0 {
			return
		}
		stats.SymbolCount++
		csm.AddColumnSeries(*currentBar.Tbk, currentBar.GetCs())
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Distinguish CSV parse errors (malformed row, wrong field count)
			// from I/O errors (connection reset, gzip corruption). Parse
			// errors are row-level and can be skipped; I/O errors are fatal
			// because the underlying stream is broken.
			var parseErr *csv.ParseError
			if errors.As(err, &parseErr) {
				log.Warn("[flatfiles] skipping malformed CSV row on %s: %v", date.Format("2006-01-02"), err)
				continue
			}
			// I/O or decompression error: abort the parse.
			return csm, stats, fmt.Errorf("read CSV on %s: %w", date.Format("2006-01-02"), err)
		}
		stats.RowsRead++

		ticker := record[colIdx.ticker]

		// Filter by symbol set.
		if symbolSet != nil && !symbolSet[ticker] {
			continue
		}

		// Parse numeric fields.
		open, high, low, clos, volume, epoch, err := parseRow(record, colIdx)
		if err != nil {
			log.Warn("[flatfiles] skipping bad row for %s on %s: %v", ticker, date.Format("2006-01-02"), err)
			continue
		}

		stats.RowsMatched++

		// Flush on ticker boundary change.
		if ticker != currentTicker {
			flushBar()
			currentTicker = ticker
			currentBar = models.NewBar(ticker, timeframe, capacity)
		}

		currentBar.Add(epoch, enum.Price(open), enum.Price(high), enum.Price(low), enum.Price(clos), enum.Size(volume))
	}

	// Flush the last symbol.
	flushBar()

	return csm, stats, nil
}

// columnIndex holds the column positions for required CSV fields.
type columnIndex struct {
	ticker      int
	volume      int
	open        int
	close       int
	high        int
	low         int
	windowStart int
}

// buildColumnIndex maps column names to their positions in the CSV header.
func buildColumnIndex(header []string) (columnIndex, error) {
	idx := columnIndex{
		ticker:      -1,
		volume:      -1,
		open:        -1,
		close:       -1,
		high:        -1,
		low:         -1,
		windowStart: -1,
	}

	for i, col := range header {
		switch col {
		case colTicker:
			idx.ticker = i
		case colVolume:
			idx.volume = i
		case colOpen:
			idx.open = i
		case colClose:
			idx.close = i
		case colHigh:
			idx.high = i
		case colLow:
			idx.low = i
		case colWindowStart:
			idx.windowStart = i
		}
	}

	// Validate all required columns are present.
	if idx.ticker < 0 {
		return idx, fmt.Errorf("missing required CSV column: %s", colTicker)
	}
	if idx.open < 0 {
		return idx, fmt.Errorf("missing required CSV column: %s", colOpen)
	}
	if idx.high < 0 {
		return idx, fmt.Errorf("missing required CSV column: %s", colHigh)
	}
	if idx.low < 0 {
		return idx, fmt.Errorf("missing required CSV column: %s", colLow)
	}
	if idx.close < 0 {
		return idx, fmt.Errorf("missing required CSV column: %s", colClose)
	}
	if idx.volume < 0 {
		return idx, fmt.Errorf("missing required CSV column: %s", colVolume)
	}
	if idx.windowStart < 0 {
		return idx, fmt.Errorf("missing required CSV column: %s", colWindowStart)
	}

	return idx, nil
}

// parseRow extracts OHLCV data from a CSV record. Returns open, high, low, close,
// volume (as uint64), and epoch (window_start converted from nanoseconds to seconds).
func parseRow(record []string, idx columnIndex) (open, high, low, clos float64, volume uint64, epoch int64, err error) {
	open, err = strconv.ParseFloat(record[idx.open], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("parse open: %w", err)
	}
	high, err = strconv.ParseFloat(record[idx.high], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("parse high: %w", err)
	}
	low, err = strconv.ParseFloat(record[idx.low], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("parse low: %w", err)
	}
	clos, err = strconv.ParseFloat(record[idx.close], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("parse close: %w", err)
	}

	// Volume is a float in the CSV (e.g., "953587") but MarketStore stores it as uint64.
	volFloat, err := strconv.ParseFloat(record[idx.volume], 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("parse volume: %w", err)
	}
	volume = uint64(volFloat)

	// window_start is Unix nanoseconds; convert to seconds for MarketStore Epoch.
	windowStartNanos, err := strconv.ParseInt(record[idx.windowStart], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, fmt.Errorf("parse window_start: %w", err)
	}
	epoch = windowStartNanos / nanosPerSecond

	return open, high, low, clos, volume, epoch, nil
}
