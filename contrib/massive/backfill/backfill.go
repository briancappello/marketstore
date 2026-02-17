package backfill

import (
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/models"
	modelsenum "github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// dayFetchParallelism controls how many days are fetched concurrently
// for trades and quotes. This is separate from symbol parallelism.
var dayFetchParallelism = runtime.NumCPU()

const (
	millisToSec = 1000

	// Market hours: 16 hours including extended hours (4am-8pm ET).
	marketHoursPerDay = 16
	// Trading days per week (Mon-Fri), ignoring holidays.
	tradingDaysPerWeek = 5
	// API limit per request.
	apiRecordLimit = 50000
)

// NY is the New York timezone used for market-day calculations.
var NY, _ = time.LoadLocation("America/New_York")

// timeframeChunkDays estimates how many calendar days can fit in one API request
// for a given timeframe, staying under the 50k record limit.
// Returns calendar days (not trading days) to simplify date arithmetic.
func timeframeChunkDays(timeframe string) int {
	barsPerDay := estimateBarsPerDay(timeframe)
	if barsPerDay == 0 {
		// Unknown timeframe, use conservative default.
		return 7
	}

	// Calculate trading days that fit in the limit.
	tradingDays := apiRecordLimit / barsPerDay
	if tradingDays < 1 {
		tradingDays = 1
	}

	// Convert trading days to calendar days (approximate: 7 calendar days = 5 trading days).
	calendarDays := (tradingDays * 7) / tradingDaysPerWeek

	// Cap at reasonable maximum to avoid huge requests.
	if calendarDays > 365 {
		calendarDays = 365
	}

	return calendarDays
}

// estimateBarsPerDay returns the estimated max bars per trading day for a timeframe.
func estimateBarsPerDay(timeframe string) int {
	// Parse multiplier and unit from timeframe (e.g., "1Min" -> 1, "Min").
	suffixes := []struct {
		suffix     string
		barsPerDay int // based on 16 market hours
	}{
		{"Sec", marketHoursPerDay * 60 * 60}, // 57600 bars/day
		{"Min", marketHoursPerDay * 60},      // 960 bars/day
		{"H", marketHoursPerDay},             // 16 bars/day
		{"D", 1},                             // 1 bar/day
		{"W", 1},                             // ~0.2 bars/day, treat as 1
		{"M", 1},                             // ~0.05 bars/day, treat as 1
		{"Y", 1},                             // ~0.004 bars/day, treat as 1
	}

	for _, s := range suffixes {
		if len(timeframe) > len(s.suffix) && timeframe[len(timeframe)-len(s.suffix):] == s.suffix {
			numStr := timeframe[:len(timeframe)-len(s.suffix)]
			var multiplier int
			if _, err := fmt.Sscanf(numStr, "%d", &multiplier); err != nil || multiplier == 0 {
				return 0
			}
			return s.barsPerDay / multiplier
		}
	}
	return 0
}

// dateRange represents a time range for parallel fetching.
type dateRange struct {
	from time.Time
	to   time.Time
}

// splitDateRange splits a date range into chunks based on the timeframe.
func splitDateRange(from, to time.Time, timeframe string) []dateRange {
	chunkDays := timeframeChunkDays(timeframe)
	chunkDuration := time.Duration(chunkDays) * 24 * time.Hour

	var ranges []dateRange
	for chunkStart := from; chunkStart.Before(to); chunkStart = chunkStart.Add(chunkDuration) {
		chunkEnd := chunkStart.Add(chunkDuration)
		if chunkEnd.After(to) {
			chunkEnd = to
		}
		ranges = append(ranges, dateRange{from: chunkStart, to: chunkEnd})
	}
	return ranges
}

// Bars fetches historical bar aggregates from the Massive REST API
// and writes them to MarketStore. The timeframe parameter specifies the
// bar frequency (e.g., "1Min", "5Min", "1H", "1D").
// Date ranges are split into chunks and fetched in parallel for improved throughput.
func Bars(
	client *http.Client,
	symbol string,
	timeframe string,
	from, to time.Time,
	limit int,
	adjusted bool,
	writerWP *worker.Pool,
) error {
	if from.IsZero() {
		from = time.Date(2014, 1, 1, 0, 0, 0, 0, NY)
	}
	if to.IsZero() {
		to = time.Now()
	}

	// Convert MarketStore timeframe to API timespan and multiplier.
	apiTimespan, multiplier, err := timeframeToAPI(timeframe)
	if err != nil {
		return fmt.Errorf("invalid timeframe %q: %w", timeframe, err)
	}

	// Split the date range into chunks for parallel fetching.
	chunks := splitDateRange(from, to, timeframe)

	if len(chunks) == 1 {
		// Single chunk: fetch directly without parallelization overhead.
		return fetchAndWriteBars(client, symbol, timeframe, apiTimespan, multiplier,
			from, to, limit, adjusted, writerWP)
	}

	// Fetch chunks in parallel.
	type chunkResult struct {
		idx     int
		results []api.AggResult
		err     error
	}

	results := make(chan chunkResult, len(chunks))
	sem := make(chan struct{}, dayFetchParallelism)
	var wg sync.WaitGroup

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, dr dateRange) {
			defer wg.Done()
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release

			resp, err := api.GetHistoricAggregates(client, symbol, apiTimespan, multiplier,
				dr.from, dr.to, limit, adjusted)
			if err != nil {
				results <- chunkResult{idx: idx, err: err}
				return
			}
			results <- chunkResult{idx: idx, results: resp.Results}
		}(i, chunk)
	}

	// Close results channel when all fetches complete.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results by chunk index for ordered assembly.
	chunkResults := make([][]api.AggResult, len(chunks))
	for res := range results {
		if res.err != nil {
			return res.err
		}
		chunkResults[res.idx] = res.results
	}

	// Flatten results in chunk order.
	var allResults []api.AggResult
	for _, cr := range chunkResults {
		allResults = append(allResults, cr...)
	}

	if len(allResults) == 0 {
		return nil
	}

	model := models.NewBar(symbol, timeframe, len(allResults))
	for _, bar := range allResults {
		epoch := bar.EpochMilliseconds / millisToSec
		ts := time.Unix(epoch, 0)
		if ts.After(to) || ts.Before(from) {
			continue
		}
		model.Add(epoch,
			modelsenum.Price(bar.Open),
			modelsenum.Price(bar.High),
			modelsenum.Price(bar.Low),
			modelsenum.Price(bar.Close),
			modelsenum.Size(bar.Volume),
		)
	}

	writerWP.Do(func() {
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write %s bars for %s: %v", timeframe, symbol, err)
		}
	})

	return nil
}

// fetchAndWriteBars is a helper for single-chunk bar fetches (no parallelization).
func fetchAndWriteBars(
	client *http.Client,
	symbol, timeframe, apiTimespan string,
	multiplier int,
	from, to time.Time,
	limit int,
	adjusted bool,
	writerWP *worker.Pool,
) error {
	resp, err := api.GetHistoricAggregates(client, symbol, apiTimespan, multiplier, from, to, limit, adjusted)
	if err != nil {
		return err
	}

	if len(resp.Results) == 0 {
		return nil
	}

	model := models.NewBar(symbol, timeframe, len(resp.Results))
	for _, bar := range resp.Results {
		epoch := bar.EpochMilliseconds / millisToSec
		ts := time.Unix(epoch, 0)
		if ts.After(to) || ts.Before(from) {
			continue
		}
		model.Add(epoch,
			modelsenum.Price(bar.Open),
			modelsenum.Price(bar.High),
			modelsenum.Price(bar.Low),
			modelsenum.Price(bar.Close),
			modelsenum.Size(bar.Volume),
		)
	}

	writerWP.Do(func() {
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write %s bars for %s: %v", timeframe, symbol, err)
		}
	})

	return nil
}

// timeframeToAPI converts a MarketStore timeframe string (e.g., "1Min", "5Min", "1H", "1D")
// to Massive API timespan and multiplier values.
func timeframeToAPI(timeframe string) (apiTimespan string, multiplier int, err error) {
	// Validate the timeframe using CandleDurationFromString
	_, err = utils.CandleDurationFromString(timeframe)
	if err != nil {
		return "", 0, err
	}

	// Parse the timeframe string to extract multiplier and suffix
	// Timeframe format is: <number><suffix> (e.g., "1Min", "5Min", "1H", "1D")
	suffixes := []struct {
		suffix      string
		apiTimespan string
	}{
		{"Sec", "second"},
		{"Min", "minute"},
		{"H", "hour"},
		{"D", "day"},
		{"W", "week"},
		{"M", "month"},
		{"Y", "year"},
	}

	for _, s := range suffixes {
		if len(timeframe) > len(s.suffix) && timeframe[len(timeframe)-len(s.suffix):] == s.suffix {
			numStr := timeframe[:len(timeframe)-len(s.suffix)]
			var n int
			if _, err := fmt.Sscanf(numStr, "%d", &n); err != nil {
				return "", 0, fmt.Errorf("invalid multiplier in timeframe %q: %w", timeframe, err)
			}
			return s.apiTimespan, n, nil
		}
	}

	return "", 0, fmt.Errorf("unsupported timeframe: %s", timeframe)
}

// Trades fetches historical tick-level trades from the Massive REST API
// for each market day in the from/to range and writes them to MarketStore.
// Days are fetched in parallel for improved throughput on I/O-bound workloads.
func Trades(
	client *http.Client,
	symbol string,
	from, to time.Time,
	limit int,
	writerWP *worker.Pool,
) error {
	const hoursInDay = 24

	// Collect market days to fetch.
	var dates []time.Time
	for date := from; to.After(date); date = date.Add(hoursInDay * time.Hour) {
		if calendar.Nasdaq.IsMarketDay(date) {
			dates = append(dates, date)
		}
	}

	if len(dates) == 0 {
		return nil
	}

	// Fetch days in parallel.
	type dayResult struct {
		date   time.Time
		trades []api.TradeTick
		err    error
	}

	results := make(chan dayResult, len(dates))
	sem := make(chan struct{}, dayFetchParallelism)
	var wg sync.WaitGroup

	for _, date := range dates {
		wg.Add(1)
		go func(d time.Time) {
			defer wg.Done()
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release

			resp, err := api.GetHistoricTrades(client, symbol, d, limit)
			if err != nil {
				results <- dayResult{date: d, err: err}
				return
			}
			results <- dayResult{date: d, trades: resp.Results}
		}(date)
	}

	// Close results channel when all fetches complete.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results, preserving order by date for deterministic writes.
	dayTrades := make(map[time.Time][]api.TradeTick)
	for res := range results {
		if res.err != nil {
			return res.err
		}
		dayTrades[res.date] = res.trades
	}

	// Flatten results in date order.
	var allTrades []api.TradeTick
	for _, date := range dates {
		allTrades = append(allTrades, dayTrades[date]...)
	}

	if len(allTrades) == 0 {
		return nil
	}

	model := models.NewTrade(symbol, len(allTrades))
	for i := range allTrades {
		tick := &allTrades[i]
		timestamp := time.Unix(0, tick.SIPTimestamp)

		// Convert Massive condition codes to MarketStore enum values.
		conditions := make([]modelsenum.TradeCondition, len(tick.Conditions))
		for j, c := range tick.Conditions {
			conditions[j] = modelsenum.TradeCondition(c)
		}

		model.Add(
			timestamp.Unix(), timestamp.Nanosecond(),
			modelsenum.Price(tick.Price),
			modelsenum.Size(tick.Size),
			modelsenum.Exchange(tick.Exchange),
			modelsenum.Tape(tick.Tape),
			conditions...,
		)
	}

	writerWP.Do(func() {
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write trades for %s: %v", symbol, err)
		}
	})

	return nil
}

// Quotes fetches historical NBBO quotes from the Massive REST API
// for each market day in the from/to range and writes them to MarketStore.
// Days are fetched in parallel for improved throughput on I/O-bound workloads.
func Quotes(
	client *http.Client,
	symbol string,
	from, to time.Time,
	limit int,
	writerWP *worker.Pool,
) error {
	const hoursInDay = 24

	// Collect market days to fetch.
	var dates []time.Time
	for date := from; to.After(date); date = date.Add(hoursInDay * time.Hour) {
		if calendar.Nasdaq.IsMarketDay(date) {
			dates = append(dates, date)
		}
	}

	if len(dates) == 0 {
		return nil
	}

	// Fetch days in parallel.
	type dayResult struct {
		date   time.Time
		quotes []api.QuoteTick
		err    error
	}

	results := make(chan dayResult, len(dates))
	sem := make(chan struct{}, dayFetchParallelism)
	var wg sync.WaitGroup

	for _, date := range dates {
		wg.Add(1)
		go func(d time.Time) {
			defer wg.Done()
			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release

			resp, err := api.GetHistoricQuotes(client, symbol, d, limit)
			if err != nil {
				results <- dayResult{date: d, err: err}
				return
			}
			results <- dayResult{date: d, quotes: resp.Results}
		}(date)
	}

	// Close results channel when all fetches complete.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results, preserving order by date for deterministic writes.
	dayQuotes := make(map[time.Time][]api.QuoteTick)
	for res := range results {
		if res.err != nil {
			return res.err
		}
		dayQuotes[res.date] = res.quotes
	}

	// Flatten results in date order.
	var allQuotes []api.QuoteTick
	for _, date := range dates {
		allQuotes = append(allQuotes, dayQuotes[date]...)
	}

	if len(allQuotes) == 0 {
		return nil
	}

	model := models.NewQuote(symbol, len(allQuotes))
	for _, tick := range allQuotes {
		timestamp := time.Unix(0, tick.SIPTimestamp)

		var cond modelsenum.QuoteCondition
		if len(tick.Conditions) > 0 {
			cond = modelsenum.QuoteCondition(tick.Conditions[0])
		}

		model.Add(
			timestamp.Unix(), timestamp.Nanosecond(),
			tick.BidPrice, tick.AskPrice,
			int(tick.BidSize), int(tick.AskSize),
			modelsenum.Exchange(tick.BidExchange),
			modelsenum.Exchange(tick.AskExchange),
			cond,
		)
	}

	writerWP.Do(func() {
		if err := model.Write(); err != nil {
			log.Error("[massive] failed to write quotes for %s: %v", symbol, err)
		}
	})

	return nil
}
