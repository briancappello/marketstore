package main

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
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
}

// NewBgWorker returns a new instance of MassiveFetcher.
// nolint:deadcode // plugin interface
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(conf)
	config := massiveconfig.FetcherConfig{}
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parse massive config: %w", err)
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

	return &MassiveFetcher{
		config: config,
		types:  t,
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
		mf.runBackfill()
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
				go streamForever(wsServer, mf.config.APIKey, prefix, mf.config.Symbols, wsQueryStart, handler)
			}
		case "quotes":
			go streamForever(wsServer, mf.config.APIKey, PrefixQuote, mf.config.Symbols, wsQueryStart, handlers.QuoteHandler)
		case "trades":
			go streamForever(wsServer, mf.config.APIKey, PrefixTrade, mf.config.Symbols, wsQueryStart, handlers.TradeHandler)
		}
	}

	select {} // block forever
}

// runBackfill performs a historical data backfill from the Massive REST API
// for all configured data types and symbols. On subsequent restarts, backfill
// resumes from the last written timestamp for each symbol/datatype combination.
// Backfill runs up to the latest market trading time (not wall-clock time).
func (mf *MassiveFetcher) runBackfill() {
	// Use the latest market trading time as the end boundary, not wall-clock time.
	// This ensures we only backfill data that could actually exist.
	end := calendar.Nasdaq.LatestMarketTime(time.Now())

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

	writerWP := worker.NewWorkerPool(1)

	log.Info("[massive] starting backfill to %s", end.Format(time.RFC3339))

	for _, symbol := range mf.config.Symbols {
		if symbol == "*" {
			log.Warn("[massive] backfill with wildcard symbol is not supported, use the backfiller CLI instead")
			continue
		}

		// Iterate over query_start keys to determine what to backfill.
		// Keys are either timeframes (e.g., "1Min", "1D") for bars,
		// or "trades"/"quotes" for tick data.
		for key, startDateStr := range mf.config.QueryStart {
			configStart, err := time.Parse(dateFormat, startDateStr)
			if err != nil {
				log.Error("[massive] invalid query_start date %q for %s: %v", startDateStr, key, err)
				continue
			}

			switch key {
			case "trades":
				// Only process if trades is in data_types.
				if _, ok := mf.types["trades"]; !ok {
					continue
				}
				tbk := io.NewTimeBucketKey(models.TradeBucketKey(symbol))
				start, skip := mf.determineBackfillStart(tbk, configStart, end, "trades", symbol)
				if skip {
					continue
				}
				log.Info("[massive] backfilling trades for %s from %s to %s",
					symbol, start.Format(time.RFC3339), end.Format(time.RFC3339))
				if err := backfill.Trades(client, symbol, start, end, batchSize, writerWP); err != nil {
					log.Warn("[massive] failed to backfill trades for %s: %v", symbol, err)
				}
			case "quotes":
				// Only process if quotes is in data_types.
				if _, ok := mf.types["quotes"]; !ok {
					continue
				}
				tbk := io.NewTimeBucketKey(models.QuoteBucketKey(symbol))
				start, skip := mf.determineBackfillStart(tbk, configStart, end, "quotes", symbol)
				if skip {
					continue
				}
				log.Info("[massive] backfilling quotes for %s from %s to %s",
					symbol, start.Format(time.RFC3339), end.Format(time.RFC3339))
				if err := backfill.Quotes(client, symbol, start, end, batchSize, writerWP); err != nil {
					log.Warn("[massive] failed to backfill quotes for %s: %v", symbol, err)
				}
			default:
				// Assume it's a bar timeframe (e.g., "1Min", "5Min", "1H", "1D").
				// Only process if bars is in data_types.
				if _, ok := mf.types["bars"]; !ok {
					continue
				}
				tf := key
				tbk := io.NewTimeBucketKey(models.BarBucketKey(symbol, tf))
				start, skip := mf.determineBackfillStart(tbk, configStart, end, tf+" bars", symbol)
				if skip {
					continue
				}
				log.Info("[massive] backfilling %s bars for %s from %s to %s",
					tf, symbol, start.Format(time.RFC3339), end.Format(time.RFC3339))
				if err := backfill.Bars(client, symbol, tf, start, end, batchSize, adjusted, writerWP); err != nil {
					log.Warn("[massive] failed to backfill %s bars for %s: %v", tf, symbol, err)
				}
			}
		}
	}

	writerWP.CloseAndWait()
	log.Info("[massive] backfill complete")
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

// determineBackfillStart checks for existing data and returns the appropriate start time.
// It returns (start time, skip) where skip is true if the backfill should be skipped.
func (mf *MassiveFetcher) determineBackfillStart(
	tbk *io.TimeBucketKey,
	configStart, end time.Time,
	dataType, symbol string,
) (time.Time, bool) {
	start := configStart
	lastTS := findLastTimestamp(tbk)
	if !lastTS.IsZero() {
		// Start from after the last written timestamp (not inclusive).
		start = lastTS.Add(time.Nanosecond)
		log.Info("[massive] resuming %s backfill for %s from %s (last written: %s)",
			dataType, symbol, start.Format(time.RFC3339), lastTS.Format(time.RFC3339))
	}

	// Skip if the last timestamp matches or exceeds the latest trading time.
	if !lastTS.IsZero() && !lastTS.Before(end) {
		log.Info("[massive] %s data for %s is up to date (last: %s, market: %s)",
			dataType, symbol, lastTS.Format(time.RFC3339), end.Format(time.RFC3339))
		return start, true
	}

	// Skip if start is after end.
	if start.After(end) {
		log.Info("[massive] %s data for %s is up to date, skipping backfill", dataType, symbol)
		return start, true
	}

	return start, false
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

// streamForever connects to the Massive WebSocket API and processes messages,
// reconnecting automatically on any failure.
func streamForever(server, apiKey string, prefix Prefix, symbols []string, wsQueryStart string, handler func([]byte)) {
	scope := buildSubScope(prefix, symbols)
	for {
		err := stream(server, apiKey, scope, wsQueryStart, handler)
		if err != nil {
			log.Warn("[massive] stream disconnected, reconnecting... {scope:%s, error:%v}", scope, err)
		}
		time.Sleep(reconnectBackoff)
	}
}

// stream runs a single WebSocket session: connect, authenticate, subscribe, read.
func stream(server, apiKey, scope, wsQueryStart string, handler func([]byte)) error {
	conn, err := connect(server, apiKey)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()

	if err := authenticate(conn, apiKey); err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	if err := subscribe(conn, scope, wsQueryStart); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	log.Info("[massive] streaming {scope:%s}", scope)

	conn.SetReadLimit(maxRecvMsgSize)

	for {
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

func subscribe(conn *websocket.Conn, scope, wsQueryStart string) error {
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

	log.Info("[massive] subscribed to %s", scope)
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
