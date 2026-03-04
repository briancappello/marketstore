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
	config      massiveconfig.FetcherConfig
	wsDataTypes map[string]struct{} // 1Min, 1Sec, trades, quotes
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
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

	// Parse and validate ws_data_types.
	wsDataTypes := map[string]struct{}{}
	if len(config.WSDataTypes) == 0 {
		// Default to 1Min if not specified.
		wsDataTypes["1Min"] = struct{}{}
	} else {
		for _, dt := range config.WSDataTypes {
			if !massiveconfig.ValidWSDataTypes[dt] {
				return nil, fmt.Errorf("invalid ws_data_type %q: must be one of 1Min, 1Sec, trades, quotes", dt)
			}
			wsDataTypes[dt] = struct{}{}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &MassiveFetcher{
		config:      config,
		wsDataTypes: wsDataTypes,
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Run starts the Massive data fetcher. If query_start is configured, it first
// starts WebSocket streaming immediately, then backfills historical data concurrently.
// This ensures no real-time data is missed even during long backfills.
func (mf *MassiveFetcher) Run() {
	api.SetAPIKey(mf.config.APIKey)

	if mf.config.BaseURL != "" {
		api.SetBaseURL(mf.config.BaseURL)
	}

	// Start WebSocket streaming immediately to avoid missing real-time data.
	mf.startStreaming()

	// Run backfill concurrently if query_start is set and backfill is not disabled.
	// Any overlap with streaming data is harmless - duplicate writes are idempotent.
	if len(mf.config.QueryStart) > 0 {
		if utils.InstanceConfig.NoBackfill {
			log.Info("[massive] backfill disabled via --no-backfill flag, skipping")
		} else if err := mf.runBackfill(); err != nil {
			log.Info("[massive] backfill stopped: %v", err)
		}
	}

	// Wait for context cancellation.
	<-mf.ctx.Done()
	log.Info("[massive] shutdown requested, waiting for goroutines to finish...")
	mf.wg.Wait()
	log.Info("[massive] shutdown complete")
}

// startStreaming launches WebSocket streaming goroutines for all configured data types.
func (mf *MassiveFetcher) startStreaming() {
	wsServer := mf.config.WSServer
	if wsServer == "" {
		wsServer = defaultWSServer
	}

	// Only pass ws_query_start to the subscribe message when the server is local.
	wsQueryStart := ""
	if mf.config.WSQueryStart != "" && isLocalHost(wsServer) {
		wsQueryStart = mf.config.WSQueryStart
	}

	for dataType := range mf.wsDataTypes {
		switch dataType {
		case "1Min":
			handler := handlers.MakeBarsHandler("1Min")
			mf.wg.Add(1)
			go func() {
				defer mf.wg.Done()
				mf.streamForever(wsServer, mf.config.APIKey, PrefixAggMinute, mf.config.Symbols, wsQueryStart, handler)
			}()
		case "1Sec":
			handler := handlers.MakeBarsHandler("1Sec")
			mf.wg.Add(1)
			go func() {
				defer mf.wg.Done()
				mf.streamForever(wsServer, mf.config.APIKey, PrefixAggSecond, mf.config.Symbols, wsQueryStart, handler)
			}()
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
				tbk := io.NewTimeBucketKey(models.TradeBucketKey(symbol))
				decision := mf.determineBackfillStart(tbk, effectiveStart, endExtended)
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
				tbk := io.NewTimeBucketKey(models.QuoteBucketKey(symbol))
				decision := mf.determineBackfillStart(tbk, effectiveStart, endExtended)
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
				tf := key
				// Use regular market hours for daily+ timeframes, extended hours for intraday.
				end := endExtended
				if isDailyOrLonger(tf) {
					end = endRegular
				}
				tbk := io.NewTimeBucketKey(models.BarBucketKey(symbol, tf))
				decision := mf.determineBackfillStart(tbk, effectiveStart, end)
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

// backfillDecision contains the result of determineBackfillStart.
type backfillDecision struct {
	start time.Time // When to start backfilling from
	skip  bool      // If true, skip backfilling entirely
}

// determineBackfillStart checks for existing data and returns the appropriate start time.
//
// Logic:
//   - No data exists: backfill from effectiveStart (the earlier of query_start and
//     listing date) up to end.
//   - Data exists: assume the earliest data is already correct; only backfill from
//     the latest written timestamp up to end.
func (mf *MassiveFetcher) determineBackfillStart(
	tbk *io.TimeBucketKey,
	effectiveStart, end time.Time,
) backfillDecision {
	lastTS := findLastTimestamp(tbk)

	var start time.Time
	if lastTS.IsZero() {
		// No data on disk: backfill from the effective start.
		start = effectiveStart
	} else {
		// Data exists: resume from just after the last written timestamp.
		start = lastTS.Add(time.Nanosecond)
	}

	// Skip if already up to date or start is beyond end.
	if !start.Before(end) {
		return backfillDecision{start: start, skip: true}
	}

	return backfillDecision{start: start, skip: false}
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

	// Set up ping/pong handling to keep the connection alive.
	// The pong handler resets the read deadline when we receive a pong from the server.
	// The gorilla/websocket library automatically sends pong responses to server pings,
	// but we need to send our own pings to detect dead connections.
	conn.SetPongHandler(func(appData string) error {
		return conn.SetReadDeadline(time.Now().Add(2 * pingInterval))
	})

	// Start a goroutine to send periodic pings to the server.
	pingDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					return
				}
			case <-mf.ctx.Done():
				return
			case <-pingDone:
				return
			}
		}
	}()
	defer close(pingDone)

	// Set initial read deadline.
	if err := conn.SetReadDeadline(time.Now().Add(2 * pingInterval)); err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}

	for {
		// Check for context cancellation.
		select {
		case <-mf.ctx.Done():
			return mf.ctx.Err()
		default:
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		// Reset read deadline after receiving any message.
		if err := conn.SetReadDeadline(time.Now().Add(2 * pingInterval)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
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
