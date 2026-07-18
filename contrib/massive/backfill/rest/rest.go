package rest

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/mapping"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models"
	modelsenum "github.com/alpacahq/marketstore/v4/models/enum"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
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

// restExchangeMap resolves exchange ids to SIP chars for the REST backfiller.
// It is initialized lazily (and falls back to the static table) so the REST
// path encodes exchanges identically to the flat-file path.
var (
	restExchangeMap     *mapping.ExchangeMap
	restExchangeMapOnce sync.Once
)

func exchangeMap(client *http.Client) *mapping.ExchangeMap {
	restExchangeMapOnce.Do(func() {
		restExchangeMap = mapping.LoadExchangeMap(client)
	})
	return restExchangeMap
}

// roundLotCutoff is the date on/after which Massive reports quote bid/ask sizes
// in shares; before it, sizes are in round lots and must be multiplied by the
// symbol's round_lot to normalize to shares (SEC MDI transition).
var roundLotCutoff = time.Date(2025, 11, 3, 0, 0, 0, 0, time.UTC)

// defaultRoundLot is used when the symbol's round_lot is unknown.
const defaultRoundLot = 100

// normalizeQuoteSize converts a raw Massive quote size to shares given the
// quote's timestamp and the symbol's round lot.
func normalizeQuoteSize(rawSize float64, ts time.Time, roundLot int) uint64 {
	if ts.Before(roundLotCutoff) {
		if roundLot <= 0 {
			roundLot = defaultRoundLot
		}
		return uint64(rawSize * float64(roundLot))
	}
	return uint64(rawSize)
}

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
// Returns context.Canceled if the context is cancelled during the operation.
// If writer is nil, data is written directly to disk via executor.WriteCSM.
func Bars(
	ctx context.Context,
	client *http.Client,
	symbol string,
	timeframe string,
	from, to time.Time,
	limit int,
	adjusted bool,
	writerWP *worker.Pool,
	writer backfill.Writer,
) error {
	// Check for cancellation at start.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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
		return fetchAndWriteBars(ctx, client, symbol, timeframe, apiTimespan, multiplier,
			from, to, limit, adjusted, writerWP, writer)
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

	// Create a cancellable context for the parallel fetches.
	fetchCtx, fetchCancel := context.WithCancel(ctx)
	defer fetchCancel()

	for i, chunk := range chunks {
		wg.Add(1)
		go func(idx int, dr dateRange) {
			defer wg.Done()

			// Check for cancellation before acquiring semaphore.
			select {
			case <-fetchCtx.Done():
				results <- chunkResult{idx: idx, err: fetchCtx.Err()}
				return
			case sem <- struct{}{}: // acquire
			}
			defer func() { <-sem }() // release

			// Check again after acquiring semaphore.
			select {
			case <-fetchCtx.Done():
				results <- chunkResult{idx: idx, err: fetchCtx.Err()}
				return
			default:
			}

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
			fetchCancel() // Cancel remaining fetches.
			return res.err
		}
		chunkResults[res.idx] = res.results
	}

	// Check for cancellation before writing.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
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
		if err := writeModel(model.BuildCsm(), writer, timeframe+" bars", symbol, false); err != nil {
			log.Error("[massive] failed to write %s bars for %s: %v", timeframe, symbol, err)
		}
	})

	return nil
}

// writeModel writes a model's CSM using the provided writer, or falls back to
// direct disk write. isVariableLength selects the on-disk record layout: bars
// are fixed (false); tick data (trades/quotes) MUST be variable (true) so that
// multiple records can share a 1Sec interval and nanosecond IntervalTicks are
// preserved.
func writeModel(csm *io.ColumnSeriesMap, writer backfill.Writer, dataType, symbol string, isVariableLength bool) error {
	if writer != nil {
		return writer.WriteCSM(*csm, isVariableLength)
	}
	return executor.WriteCSM(*csm, isVariableLength)
}

// fetchAndWriteBars is a helper for single-chunk bar fetches (no parallelization).
func fetchAndWriteBars(
	ctx context.Context,
	client *http.Client,
	symbol, timeframe, apiTimespan string,
	multiplier int,
	from, to time.Time,
	limit int,
	adjusted bool,
	writerWP *worker.Pool,
	writer backfill.Writer,
) error {
	// Check for cancellation.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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
		if err := writeModel(model.BuildCsm(), writer, timeframe+" bars", symbol, false); err != nil {
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
// Returns context.Canceled if the context is cancelled during the operation.
// If writer is nil, data is written directly to disk via executor.WriteCSM.
func Trades(
	ctx context.Context,
	client *http.Client,
	symbol string,
	from, to time.Time,
	limit int,
	writerWP *worker.Pool,
	writer backfill.Writer,
) error {
	// Check for cancellation at start.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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

	// Create a cancellable context for the parallel fetches.
	fetchCtx, fetchCancel := context.WithCancel(ctx)
	defer fetchCancel()

	for _, date := range dates {
		wg.Add(1)
		go func(d time.Time) {
			defer wg.Done()

			// Check for cancellation before acquiring semaphore.
			select {
			case <-fetchCtx.Done():
				results <- dayResult{date: d, err: fetchCtx.Err()}
				return
			case sem <- struct{}{}: // acquire
			}
			defer func() { <-sem }() // release

			// Check again after acquiring semaphore.
			select {
			case <-fetchCtx.Done():
				results <- dayResult{date: d, err: fetchCtx.Err()}
				return
			default:
			}

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
			fetchCancel() // Cancel remaining fetches.
			return res.err
		}
		dayTrades[res.date] = res.trades
	}

	// Check for cancellation before writing.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Flatten results in date order.
	var allTrades []api.TradeTick
	for _, date := range dates {
		allTrades = append(allTrades, dayTrades[date]...)
	}

	if len(allTrades) == 0 {
		return nil
	}

	exMap := exchangeMap(client)
	model := models.NewTrade(symbol, len(allTrades))
	for i := range allTrades {
		tick := &allTrades[i]
		timestamp := time.Unix(0, tick.SIPTimestamp)

		// Map Massive integer condition codes to SIP chars, dropping codes
		// that have no SIP mapping (functionally inert for consolidation).
		conditions := make([]modelsenum.TradeCondition, 0, len(tick.Conditions))
		for _, c := range tick.Conditions {
			if sc, ok := mapping.TradeConditionToSIP(c); ok {
				conditions = append(conditions, sc)
			}
		}

		model.Add(
			timestamp.Unix(), timestamp.Nanosecond(),
			modelsenum.Price(tick.Price),
			tick.Size,
			exMap.Get(tick.Exchange),
			mapping.TapeToChar(tick.Tape),
			byte(tick.Correction),
			conditions...,
		)
	}

	writerWP.Do(func() {
		if err := writeModel(model.BuildCsm(), writer, "trades", symbol, true); err != nil {
			log.Error("[massive] failed to write trades for %s: %v", symbol, err)
		}
	})

	return nil
}

// Quotes fetches historical NBBO quotes from the Massive REST API
// for each market day in the from/to range and writes them to MarketStore.
// Days are fetched in parallel for improved throughput on I/O-bound workloads.
// Returns context.Canceled if the context is cancelled during the operation.
// If writer is nil, data is written directly to disk via executor.WriteCSM.
func Quotes(
	ctx context.Context,
	client *http.Client,
	symbol string,
	from, to time.Time,
	limit int,
	writerWP *worker.Pool,
	writer backfill.Writer,
) error {
	// Check for cancellation at start.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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

	// Create a cancellable context for the parallel fetches.
	fetchCtx, fetchCancel := context.WithCancel(ctx)
	defer fetchCancel()

	for _, date := range dates {
		wg.Add(1)
		go func(d time.Time) {
			defer wg.Done()

			// Check for cancellation before acquiring semaphore.
			select {
			case <-fetchCtx.Done():
				results <- dayResult{date: d, err: fetchCtx.Err()}
				return
			case sem <- struct{}{}: // acquire
			}
			defer func() { <-sem }() // release

			// Check again after acquiring semaphore.
			select {
			case <-fetchCtx.Done():
				results <- dayResult{date: d, err: fetchCtx.Err()}
				return
			default:
			}

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
			fetchCancel() // Cancel remaining fetches.
			return res.err
		}
		dayQuotes[res.date] = res.quotes
	}

	// Check for cancellation before writing.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Flatten results in date order.
	var allQuotes []api.QuoteTick
	for _, date := range dates {
		allQuotes = append(allQuotes, dayQuotes[date]...)
	}

	if len(allQuotes) == 0 {
		return nil
	}

	exMap := exchangeMap(client)
	// The REST path does not fetch per-symbol round_lot; use the default.
	roundLot := defaultRoundLot
	model := models.NewQuote(symbol, len(allQuotes))
	for _, tick := range allQuotes {
		timestamp := time.Unix(0, tick.SIPTimestamp)

		// Quote conditions/indicators have no SIP mapping; store raw ints.
		var cond modelsenum.QuoteCondition
		if len(tick.Conditions) > 0 {
			cond = modelsenum.QuoteCondition(tick.Conditions[0])
		}
		var cond2 byte
		if len(tick.Conditions) > 1 {
			cond2 = byte(tick.Conditions[1])
		}
		var indicator byte
		if len(tick.Indicators) > 0 {
			indicator = byte(tick.Indicators[0])
		}

		bidSize := normalizeQuoteSize(tick.BidSize, timestamp, roundLot)
		askSize := normalizeQuoteSize(tick.AskSize, timestamp, roundLot)

		model.Add(
			timestamp.Unix(), timestamp.Nanosecond(),
			tick.BidPrice, tick.AskPrice,
			int(bidSize), int(askSize),
			exMap.Get(tick.BidExchange),
			exMap.Get(tick.AskExchange),
			cond, cond2, indicator,
		)
	}

	writerWP.Do(func() {
		if err := writeModel(model.BuildCsm(), writer, "quotes", symbol, true); err != nil {
			log.Error("[massive] failed to write quotes for %s: %v", symbol, err)
		}
	})

	return nil
}
