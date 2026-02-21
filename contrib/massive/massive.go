package main

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	jsoniter "github.com/json-iterator/go"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/handlers"
	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	defaultWSServer  = "wss://socket.massive.com"
	defaultWSPath    = "/stocks"
	maxRecvMsgSize   = 2048000
	pingInterval     = 10 * time.Second
	reconnectBackoff = time.Second
	dateFormat       = "2006-01-02"

	defaultBackfillBatchSize = 50000
	backfillHTTPTimeout      = 30 * time.Second
	maxConnsPerHost          = 100
)

// Prefix represents a Massive WebSocket event type prefix used for subscriptions.
type Prefix string

const (
	// PrefixAggMinute subscribes to per-minute aggregate bars (AM.*).
	PrefixAggMinute Prefix = "AM."
	// PrefixAggSecond subscribes to per-second aggregate bars (A.*).
	PrefixAggSecond Prefix = "A."
	// PrefixTrade subscribes to tick-level trades.
	PrefixTrade Prefix = "T."
	// PrefixQuote subscribes to NBBO quotes.
	PrefixQuote Prefix = "Q."
)

// wsFrequencyToPrefix maps MarketStore timeframe strings to WebSocket prefixes.
// Only 1Min and 1Sec are supported for WebSocket streaming.
var wsFrequencyToPrefix = map[string]Prefix{
	"1Min": PrefixAggMinute,
	"1Sec": PrefixAggSecond,
}

// Use jsoniter because it supports marshal/unmarshal of map[interface{}]interface{} type.
// When the config file contains nested structures like query_start: {1Min: "2024-01-01"},
// the standard "encoding/json" library cannot marshal the structure because the config
// is parsed from a YAML file to map[string]interface{}, and nested maps become
// map[interface{}]interface{} which encoding/json doesn't support.
var json = jsoniter.ConfigCompatibleWithStandardLibrary

// MassiveFetcher is a MarketStore background worker that streams
// real-time market data from the Massive WebSocket API, with optional
// backfill from the REST API on startup.
type MassiveFetcher struct {
	config massiveconfig.FetcherConfig
	types  map[string]struct{} // bars, quotes, trades
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewBgWorker returns a new instance of MassiveFetcher.
// nolint:deadcode // plugin interface
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(conf)
	config := massiveconfig.FetcherConfig{}
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parse massive config: %w", err)
	}

	// Fetch symbols from PostgreSQL if configured, otherwise use static Symbols list.
	if config.SymbolsDSN != "" {
		if config.SymbolsQuery == "" {
			return nil, fmt.Errorf("symbols_query is required when symbols_dsn is set")
		}
		symbolInfos, err := massiveconfig.FetchSymbolsFromDB(config.SymbolsDSN, config.SymbolsQuery)
		if err != nil {
			return nil, fmt.Errorf("fetch symbols from database: %w", err)
		}
		if len(symbolInfos) == 0 {
			return nil, fmt.Errorf("no symbols returned from database query")
		}
		config.SymbolInfos = symbolInfos
		// Also populate Symbols for WebSocket streaming (which ignores dates).
		config.Symbols = make([]string, len(symbolInfos))
		for i, info := range symbolInfos {
			config.Symbols[i] = info.Symbol
		}
		// Log count with listing dates if any have them.
		withDates := 0
		for _, info := range symbolInfos {
			if info.ListingDate != nil {
				withDates++
			}
		}
		if withDates > 0 {
			log.Info("[massive] loaded %d symbols from database (%d with listing dates)", len(symbolInfos), withDates)
		} else {
			log.Info("[massive] loaded %d symbols from database", len(symbolInfos))
		}
	} else {
		// Convert static Symbols to SymbolInfos (no listing dates).
		config.SymbolInfos = make([]massiveconfig.SymbolInfo, len(config.Symbols))
		for i, sym := range config.Symbols {
			config.SymbolInfos[i] = massiveconfig.SymbolInfo{Symbol: sym}
		}
	}

	t := map[string]struct{}{}
	for _, dt := range config.DataTypes {
		if dt == "bars" || dt == "quotes" || dt == "trades" {
			t[dt] = struct{}{}
		}
	}
	if len(t) == 0 {
		return nil, fmt.Errorf("at least one valid data_type is required (bars, quotes, trades)")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &MassiveFetcher{
		config: config,
		types:  t,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Run starts the Massive data fetcher. If query_start is configured, it first
// backfills historical data from the REST API, then starts WebSocket streaming.
func (mf *MassiveFetcher) Run() {
	api.SetAPIKey(mf.config.APIKey)

	if mf.config.BaseURL != "" {
		api.SetBaseURL(mf.config.BaseURL)
	}

	// Run backfill if query_start is set.
	if len(mf.config.QueryStart) > 0 {
		if err := mf.runBackfill(); err != nil {
			log.Info("[massive] backfill stopped: %v", err)
			return
		}
	}

	// Check if context was cancelled during backfill.
	select {
	case <-mf.ctx.Done():
		log.Info("[massive] shutdown requested, skipping WebSocket streaming")
		return
	default:
	}

	// Start WebSocket streaming.
	wsServer := mf.config.WSServer
	if wsServer == "" {
		wsServer = defaultWSServer
	}

	// Only pass ws_query_start to the subscribe message when the server is local.
	wsQueryStart := ""
	if mf.config.WSQueryStart != "" && isLocalHost(wsServer) {
		wsQueryStart = mf.config.WSQueryStart
	}

	for dataType := range mf.types {
		switch dataType {
		case "bars":
			// Start a stream for each configured ws_frequency.
			wsFrequencies := mf.config.WSFrequencies
			if len(wsFrequencies) == 0 {
				wsFrequencies = []string{"1Min"} // default to 1Min
			}
			for _, freq := range wsFrequencies {
				prefix, ok := wsFrequencyToPrefix[freq]
				if !ok {
					log.Error("[massive] invalid ws_frequency %q: only 1Min and 1Sec are supported for WebSocket streaming", freq)
					continue
				}
				handler := handlers.MakeBarsHandler(freq)
				mf.wg.Add(1)
				go func(p Prefix, h func([]byte)) {
					defer mf.wg.Done()
					mf.streamForever(wsServer, mf.config.APIKey, p, mf.config.Symbols, wsQueryStart, h)
				}(prefix, handler)
			}
		case "quotes":
			mf.wg.Add(1)
			go func() {
				defer mf.wg.Done()
				mf.streamForever(wsServer, mf.config.APIKey, PrefixQuote, mf.config.Symbols, wsQueryStart, handlers.QuoteHandler)
			}()
		case "trades":
			mf.wg.Add(1)
			go func() {
				defer mf.wg.Done()
				mf.streamForever(wsServer, mf.config.APIKey, PrefixTrade, mf.config.Symbols, wsQueryStart, handlers.TradeHandler)
			}()
		}
	}

	// Wait for context cancellation.
	<-mf.ctx.Done()
	log.Info("[massive] shutdown requested, waiting for goroutines to finish...")
	mf.wg.Wait()
	log.Info("[massive] shutdown complete")
}

// Shutdown cancels the context and stops all background operations.
func (mf *MassiveFetcher) Shutdown() {
	mf.cancel()
}

// runBackfill performs a historical data backfill from the Massive REST API
// for all configured data types and symbols. On subsequent restarts, backfill
// resumes from the last written timestamp for each symbol/datatype combination.
// Backfill runs up to the latest market trading time (not wall-clock time).
// Returns context.Canceled if shutdown was requested during backfill.
func (mf *MassiveFetcher) runBackfill() error {
	// Use the latest market trading time as the end boundary, not wall-clock time.
	// This ensures we only backfill data that could actually exist.
	// For intraday data (trades, quotes, sub-daily bars), use extended hours.
	// For daily+ bars, use regular market hours since extended hours data is not relevant.
	now := time.Now()
	endExtended := calendar.Nasdaq.LatestMarketTime(now)
	endRegular := calendar.Nasdaq.LatestMarketTimeRegular(now)

	batchSize := mf.config.BackfillBatchSize
	if batchSize <= 0 {
		batchSize = defaultBackfillBatchSize
	}

	adjusted := true
	if mf.config.BackfillAdjusted != nil {
		adjusted = *mf.config.BackfillAdjusted
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxConnsPerHost,
			MaxConnsPerHost:     maxConnsPerHost,
		},
		Timeout: backfillHTTPTimeout,
	}

	writerWP := worker.NewWorkerPool(mf.ctx, 1)

	log.Info("[massive] starting backfill (extended hours end: %s, regular hours end: %s)",
		endExtended.Format(time.RFC3339), endRegular.Format(time.RFC3339))

	for _, symInfo := range mf.config.SymbolInfos {
		symbol := symInfo.Symbol

		// Check for cancellation between symbols.
		select {
		case <-mf.ctx.Done():
			writerWP.CloseAndWait()
			return mf.ctx.Err()
		default:
		}

		if symbol == "*" {
			log.Warn("[massive] backfill with wildcard symbol is not supported, use the backfiller CLI instead")
			continue
		}

		// Iterate over query_start keys to determine what to backfill.
		// Keys are either timeframes (e.g., "1Min", "1D") for bars,
		// or "trades"/"quotes" for tick data.
		for key, startDateStr := range mf.config.QueryStart {
			// Check for cancellation between data types.
			select {
			case <-mf.ctx.Done():
				writerWP.CloseAndWait()
				return mf.ctx.Err()
			default:
			}

			configStart, err := time.ParseInLocation(dateFormat, startDateStr, calendar.Nasdaq.Tz())
			if err != nil {
				log.Error("[massive] invalid query_start date %q for %s: %v", startDateStr, key, err)
				continue
			}

			// Apply per-symbol listing date override if available and more recent.
			effectiveStart := massiveconfig.EffectiveBackfillStart(configStart, symInfo.ListingDate)

			// Check if effectiveStart is in the future (listing date not yet reached).
			if effectiveStart.After(endExtended) {
				log.Info("[massive] %s listing date %s is in the future, skipping backfill",
					symbol, effectiveStart.Format(dateFormat))
				continue
			}

			switch key {
			case "trades":
				// Only process if trades is in data_types.
				if _, ok := mf.types["trades"]; !ok {
					continue
				}
				tbk := io.NewTimeBucketKey(models.TradeBucketKey(symbol))
				decision := mf.determineBackfillStart(tbk, effectiveStart, endExtended, false)
				if decision.skip {
					continue
				}
				log.Info("[massive] %s backfilling trades from %s to %s",
					symbol, decision.start.Format(time.RFC3339), endExtended.Format(time.RFC3339))
				if err := backfill.Trades(mf.ctx, client, symbol, decision.start, endExtended, batchSize, writerWP); err != nil {
					if err == context.Canceled {
						writerWP.CloseAndWait()
						return err
					}
					log.Warn("[massive] failed to backfill trades for %s: %v", symbol, err)
				}
			case "quotes":
				// Only process if quotes is in data_types.
				if _, ok := mf.types["quotes"]; !ok {
					continue
				}
				tbk := io.NewTimeBucketKey(models.QuoteBucketKey(symbol))
				decision := mf.determineBackfillStart(tbk, effectiveStart, endExtended, false)
				if decision.skip {
					continue
				}
				log.Info("[massive] %s backfilling quotes from %s to %s",
					symbol, decision.start.Format(time.RFC3339), endExtended.Format(time.RFC3339))
				if err := backfill.Quotes(mf.ctx, client, symbol, decision.start, endExtended, batchSize, writerWP); err != nil {
					if err == context.Canceled {
						writerWP.CloseAndWait()
						return err
					}
					log.Warn("[massive] failed to backfill quotes for %s: %v", symbol, err)
				}
			default:
				// Assume it's a bar timeframe (e.g., "1Min", "5Min", "1H", "1D").
				// Only process if bars is in data_types.
				if _, ok := mf.types["bars"]; !ok {
					continue
				}
				tf := key
				// Use regular market hours for daily+ timeframes, extended hours for intraday.
				end := endExtended
				daily := isDailyOrLonger(tf)
				if daily {
					end = endRegular
				}
				tbk := io.NewTimeBucketKey(models.BarBucketKey(symbol, tf))
				decision := mf.determineBackfillStart(tbk, effectiveStart, end, daily)
				if decision.skip {
					continue
				}
				log.Info("[massive] %s backfilling %s from %s to %s",
					symbol, tf, decision.start.Format(time.RFC3339), end.Format(time.RFC3339))
				if err := backfill.Bars(mf.ctx, client, symbol, tf, decision.start, end, batchSize, adjusted, writerWP); err != nil {
					if err == context.Canceled {
						writerWP.CloseAndWait()
						return err
					}
					log.Warn("[massive] failed to backfill %s bars for %s: %v", tf, symbol, err)
				}
			}
		}
	}

	writerWP.CloseAndWait()
	log.Info("[massive] backfill complete")
	return nil
}

// findLastTimestamp queries the database for the most recent timestamp in the
// given TimeBucketKey. Returns a zero time if no data exists or on error.
func findLastTimestamp(tbk *io.TimeBucketKey) time.Time {
	cDir := executor.ThisInstance.CatalogDir
	query := planner.NewQuery(cDir)
	query.AddTargetKey(tbk)

	start := time.Unix(0, 0).In(utils.InstanceConfig.Timezone)
	end := time.Unix(math.MaxInt64, 0).In(utils.InstanceConfig.Timezone)
	query.SetRange(start, end)
	query.SetRowLimit(io.LAST, 1)

	parsed, err := query.Parse()
	if err != nil {
		// This is expected if no data exists yet for this symbol.
		return time.Time{}
	}

	reader, err := executor.NewReader(parsed)
	if err != nil {
		log.Warn("[massive] failed to create reader for %s: %v", tbk, err)
		return time.Time{}
	}

	csm, err := reader.Read()
	if err != nil {
		log.Warn("[massive] failed to read data for %s: %v", tbk, err)
		return time.Time{}
	}

	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}

	ts, err := cs.GetTime()
	if err != nil {
		log.Warn("[massive] failed to get time from %s: %v", tbk, err)
		return time.Time{}
	}

	return ts[0]
}

// findFirstTimestamp queries the database for the earliest timestamp in the
// given TimeBucketKey. Returns a zero time if no data exists or on error.
func findFirstTimestamp(tbk *io.TimeBucketKey) time.Time {
	cDir := executor.ThisInstance.CatalogDir
	query := planner.NewQuery(cDir)
	query.AddTargetKey(tbk)

	start := time.Unix(0, 0).In(utils.InstanceConfig.Timezone)
	end := time.Unix(math.MaxInt64, 0).In(utils.InstanceConfig.Timezone)
	query.SetRange(start, end)
	query.SetRowLimit(io.FIRST, 1)

	parsed, err := query.Parse()
	if err != nil {
		// This is expected if no data exists yet for this symbol.
		return time.Time{}
	}

	reader, err := executor.NewReader(parsed)
	if err != nil {
		log.Warn("[massive] failed to create reader for %s: %v", tbk, err)
		return time.Time{}
	}

	csm, err := reader.Read()
	if err != nil {
		log.Warn("[massive] failed to read data for %s: %v", tbk, err)
		return time.Time{}
	}

	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return time.Time{}
	}

	ts, err := cs.GetTime()
	if err != nil {
		log.Warn("[massive] failed to get time from %s: %v", tbk, err)
		return time.Time{}
	}

	return ts[0]
}

// firstMarketDayOnOrAfter returns the first market day on or after the given time.
// For a time like 2025-01-01 (Wednesday, but a holiday), it would return 2025-01-02.
// Returns the input truncated to midnight in the market timezone.
func firstMarketDayOnOrAfter(t time.Time) time.Time {
	day := truncateToLocalMidnight(t, calendar.Nasdaq.Tz())
	const maxDaysForward = 10 // Should never need more than this
	for i := 0; i < maxDaysForward; i++ {
		if calendar.Nasdaq.IsMarketDay(day) {
			return day
		}
		day = day.AddDate(0, 0, 1)
	}
	return t // Fallback
}

// truncateToLocalMidnight returns a time representing midnight on the same
// calendar date in the given location. Unlike time.Truncate which truncates
// to UTC midnight, this properly handles timezone offsets.
func truncateToLocalMidnight(t time.Time, loc *time.Location) time.Time {
	local := t.In(loc)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
}

// configStartNeedsBackfill checks if configStart requires backfilling data before
// the first existing timestamp. This accounts for the fact that configStart is
// typically midnight, while firstTS is the actual first data point (e.g., market open).
// For daily bars, we compare dates. For intraday, we compare the first market day.
func configStartNeedsBackfill(configStart, firstTS time.Time, dailyOrLonger bool) bool {
	if firstTS.IsZero() {
		return false // No existing data, will backfill from configStart anyway
	}

	tz := calendar.Nasdaq.Tz()

	if dailyOrLonger {
		// For daily bars, compare calendar dates.
		configDate := truncateToLocalMidnight(configStart, tz)
		firstDate := truncateToLocalMidnight(firstTS, tz)
		return configDate.Before(firstDate)
	}

	// For intraday data, check if the first market day on or after configStart
	// is before the first data's market day. We compare dates only.
	configFirstMarketDay := firstMarketDayOnOrAfter(configStart)
	firstTSDate := truncateToLocalMidnight(firstTS, tz)
	return configFirstMarketDay.Before(firstTSDate)
}

// backfillDecision contains the result of determineBackfillStart.
type backfillDecision struct {
	start time.Time // When to start backfilling from
	skip  bool      // If true, skip backfilling entirely
}

// determineBackfillStart checks for existing data and returns the appropriate start time.
// It returns a backfillDecision indicating what action to take.
// For daily+ timeframes, set dailyOrLonger=true to use date comparison instead of timestamp
// comparison (since daily bars have timestamps at midnight, not market close).
func (mf *MassiveFetcher) determineBackfillStart(
	tbk *io.TimeBucketKey,
	configStart, end time.Time,
	dailyOrLonger bool,
) backfillDecision {
	start := configStart
	firstTS := findFirstTimestamp(tbk)
	lastTS := findLastTimestamp(tbk)

	// Check if configStart requires backfilling earlier data.
	needsEarlierData := configStartNeedsBackfill(configStart, firstTS, dailyOrLonger)
	if needsEarlierData {
		// start remains as configStart
	} else if !lastTS.IsZero() {
		// Otherwise, start from after the last written timestamp (not inclusive).
		start = lastTS.Add(time.Nanosecond)
	}

	// Skip if data is up to date.
	if !lastTS.IsZero() {
		isUpToDate := false
		if dailyOrLonger {
			// For daily+ bars, compare calendar dates since bar timestamps are at midnight,
			// not market close. A bar dated 2026-02-20 represents data through market close
			// on that day.
			lastDate := truncateToLocalMidnight(lastTS, end.Location())
			endDate := truncateToLocalMidnight(end, end.Location())
			isUpToDate = !lastDate.Before(endDate)
		} else {
			// For intraday data, compare timestamps directly.
			isUpToDate = !lastTS.Before(end)
		}
		// Only skip if up to date AND we don't need to backfill earlier data.
		if isUpToDate && !needsEarlierData {
			return backfillDecision{
				start: start,
				skip:  true,
			}
		}
	}

	// Skip if start is after end.
	if start.After(end) {
		return backfillDecision{
			start: start,
			skip:  true,
		}
	}

	return backfillDecision{
		start: start,
		skip:  false,
	}
}

// isDailyOrLonger returns true if the timeframe represents daily or longer periods.
// Daily+ timeframes use regular market hours for backfill since extended hours data
// is aggregated into regular session bars by data providers.
func isDailyOrLonger(tf string) bool {
	// Daily+ timeframes end with "D", "W", "M", or "Y" (e.g., "1D", "1W", "1M", "1Y").
	// We check the suffix, not ContainsAny, to avoid matching "1Min" (which contains "M").
	return strings.HasSuffix(tf, "D") ||
		strings.HasSuffix(tf, "W") ||
		strings.HasSuffix(tf, "M") ||
		strings.HasSuffix(tf, "Y")
}

// buildSubScope builds the comma-separated subscription string.
// e.g. "AM.AAPL,AM.MSFT" or "AM.*"
func buildSubScope(prefix Prefix, symbols []string) string {
	if len(symbols) == 0 {
		symbols = []string{"*"}
	}
	var buf bytes.Buffer
	for i, sym := range symbols {
		buf.WriteString(string(prefix) + sym)
		if i < len(symbols)-1 {
			buf.WriteString(",")
		}
	}
	return buf.String()
}

// formatScopeForLog returns a human-readable description of the subscription scope.
// If there are more than 10 symbols, it returns a count instead of listing them all.
func formatScopeForLog(prefix Prefix, symbols []string) string {
	if len(symbols) == 0 || (len(symbols) == 1 && symbols[0] == "*") {
		return string(prefix) + "*"
	}
	if len(symbols) > 10 {
		return fmt.Sprintf("%s<%d symbols>", prefix, len(symbols))
	}
	return buildSubScope(prefix, symbols)
}

// streamForever connects to the Massive WebSocket API and processes messages,
// reconnecting automatically on any failure. It exits when the context is cancelled.
func (mf *MassiveFetcher) streamForever(server, apiKey string, prefix Prefix, symbols []string, wsQueryStart string, handler func([]byte)) {
	scope := buildSubScope(prefix, symbols)
	scopeLog := formatScopeForLog(prefix, symbols)
	for {
		select {
		case <-mf.ctx.Done():
			log.Info("[massive] stopping stream for %s", scopeLog)
			return
		default:
		}

		err := mf.stream(server, apiKey, scope, scopeLog, wsQueryStart, handler)
		if err != nil {
			// Don't log if we're shutting down.
			select {
			case <-mf.ctx.Done():
				log.Info("[massive] stopping stream for %s", scopeLog)
				return
			default:
				log.Warn("[massive] stream disconnected, reconnecting... {scope:%s, error:%v}", scopeLog, err)
			}
		}

		// Wait before reconnecting, but check for cancellation.
		select {
		case <-mf.ctx.Done():
			log.Info("[massive] stopping stream for %s", scopeLog)
			return
		case <-time.After(reconnectBackoff):
		}
	}
}

// stream runs a single WebSocket session: connect, authenticate, subscribe, read.
// It returns when the context is cancelled or an error occurs.
func (mf *MassiveFetcher) stream(server, apiKey, scope, scopeLog, wsQueryStart string, handler func([]byte)) error {
	conn, err := connect(server, apiKey)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	if err := authenticate(conn, apiKey); err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	if err := subscribe(conn, scope, scopeLog, wsQueryStart); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	log.Info("[massive] streaming {scope:%s}", scopeLog)

	conn.SetReadLimit(maxRecvMsgSize)

	for {
		// Check for context cancellation.
		select {
		case <-mf.ctx.Done():
			return mf.ctx.Err()
		default:
		}

		if err := conn.SetReadDeadline(time.Now().Add(6 * pingInterval / 5)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		handler(msg)
	}
}

func connect(server, apiKey string) (*websocket.Conn, error) {
	u, err := url.Parse(server)
	if err != nil {
		return nil, fmt.Errorf("parse server URL: %w", err)
	}

	// Only append default path if server URL has no path or just "/"
	if u.Path == "" || u.Path == "/" {
		u.Path = defaultWSPath
	}

	q := u.Query()
	q.Set("apiKey", apiKey)
	u.RawQuery = q.Encode()

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second

	conn, resp, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}

	const statusSwitchingProtocols = http.StatusSwitchingProtocols
	if resp.StatusCode != statusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	// Read the initial "connected" status message.
	_, msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read connected message: %w", err)
	}
	if !strings.Contains(string(msg), "connected") {
		conn.Close()
		return nil, fmt.Errorf("unexpected connect response: %s", string(msg))
	}

	return conn, nil
}

func authenticate(conn *websocket.Conn, apiKey string) error {
	authMsg := fmt.Sprintf(`{"action":"auth","params":"%s"}`, apiKey)
	if err := conn.WriteMessage(websocket.TextMessage, []byte(authMsg)); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}

	if !strings.Contains(string(msg), "auth_success") && !strings.Contains(string(msg), "authenticated") {
		return fmt.Errorf("auth failed: %s", string(msg))
	}

	log.Info("[massive] authenticated successfully")
	return nil
}

func subscribe(conn *websocket.Conn, scope, scopeLog, wsQueryStart string) error {
	var subMsg string
	if wsQueryStart != "" {
		subMsg = fmt.Sprintf(`{"action":"subscribe","params":"%s","date":"%s"}`, scope, wsQueryStart)
	} else {
		subMsg = fmt.Sprintf(`{"action":"subscribe","params":"%s"}`, scope)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte(subMsg)); err != nil {
		return fmt.Errorf("send subscribe: %w", err)
	}

	_, msg, err := conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("read subscribe response: %w", err)
	}

	if !strings.Contains(string(msg), "success") {
		return fmt.Errorf("subscription failed: %s", string(msg))
	}

	log.Info("[massive] subscribed to %s", scopeLog)
	return nil
}

// isLocalHost returns true if the given WebSocket server URL points to
// localhost or 127.0.0.1.
func isLocalHost(server string) bool {
	u, err := url.Parse(server)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1"
}

func main() {}
