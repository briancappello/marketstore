package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/flatfiles"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/rest"
	"github.com/alpacahq/marketstore/v4/contrib/massive/handlers"
	"github.com/alpacahq/marketstore/v4/contrib/massive/mapping"
	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
	"github.com/alpacahq/marketstore/v4/contrib/massive/subscription"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/contrib/massive/ws"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/models"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	dateFormat = "2006-01-02"

	defaultBackfillBatchSize = 50000
	backfillHTTPTimeout      = 30 * time.Second
	maxConnsPerHost          = 100

	// flatFileAvailableHourET is the hour (ET) after which flat files for the
	// previous market day are expected to be available. The data provider
	// publishes flat files for date D at approximately 11 AM ET on D+1; we
	// use noon as a buffer.
	flatFileAvailableHourET = 12
)

// wsDataTypeToTopic maps our config data type names to WebSocket topics.
var wsDataTypeToTopic = map[string]ws.Topic{
	"1Min":   ws.StocksMinAggs,
	"1Sec":   ws.StocksSecAggs,
	"trades": ws.StocksTrades,
	"quotes": ws.StocksQuotes,
}

// dataTypeToTopic is the single mapping from subscription.DataType to ws.Topic.
// It mirrors wsDataTypeToTopic for the tick types and lives here (not in the
// subscription package) because subscription must not import ws — that would
// pull the ws client into the trigger's dependency graph.
var dataTypeToTopic = map[subscription.DataType]ws.Topic{
	subscription.Trades: ws.StocksTrades,
	subscription.Quotes: ws.StocksQuotes,
}

// topicFor returns the ws.Topic for a subscription.DataType.
func topicFor(dt subscription.DataType) ws.Topic { return dataTypeToTopic[dt] }

// reconcileInterval is the slow safety-net cadence at which the control loop
// re-applies the manager's Active() snapshot onto live clients. It is a
// correctness backstop for dropped edge events / server-side resets, not the
// primary latency path.
const reconcileInterval = 30 * time.Second

// dynClientRegistry is the seam between the connection lifecycle (which
// creates/tears down the shared ws.Client per reconnect) and the single control
// goroutine (which reads from it). Mutex-guarded because both sides touch it.
//
// Since the account permits only one connection, all configured dynamic tick
// DataTypes (Trades, Quotes) map to the SAME *ws.Client instance. The map is
// keyed by DataType so the control loop can ask "is the connection for this
// data type live?" independently; the topic (T vs Q) still distinguishes the
// subscription frames on the shared wire.
type dynClientRegistry struct {
	mu      sync.Mutex
	clients map[subscription.DataType]*ws.Client
}

func newDynClientRegistry() *dynClientRegistry {
	return &dynClientRegistry{clients: map[subscription.DataType]*ws.Client{}}
}

func (r *dynClientRegistry) set(dt subscription.DataType, c *ws.Client) {
	r.mu.Lock()
	r.clients[dt] = c
	r.mu.Unlock()
}

func (r *dynClientRegistry) clear(dt subscription.DataType) {
	r.mu.Lock()
	delete(r.clients, dt)
	r.mu.Unlock()
}

func (r *dynClientRegistry) get(dt subscription.DataType) *ws.Client {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.clients[dt]
}

// normalizeMapKeys recursively converts map[interface{}]interface{} (produced by
// gopkg.in/yaml.v2 for nested config maps) into map[string]interface{} so the
// stdlib encoding/json can marshal it. We deliberately do NOT use jsoniter here:
// jsoniter v1.1.12 + modern-go/reflect2 v1.0.2 are unmaintained and their unsafe
// reflect internals corrupt/crash under Go 1.26 when decoders are built under
// concurrency (the massive plugin's streaming dispatch faulted this way).
func normalizeMapKeys(v interface{}) interface{} {
	switch m := v.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(m))
		for k, val := range m {
			out[fmt.Sprint(k)] = normalizeMapKeys(val)
		}
		return out
	case map[string]interface{}:
		for k, val := range m {
			m[k] = normalizeMapKeys(val)
		}
		return m
	case []interface{}:
		for i, val := range m {
			m[i] = normalizeMapKeys(val)
		}
		return m
	default:
		return v
	}
}

// MassiveFetcher is a MarketStore background worker that streams
// real-time market data from the Massive WebSocket API, with optional
// backfill from the REST API on startup.
type MassiveFetcher struct {
	config      massiveconfig.FetcherConfig
	wsDataTypes map[string]struct{} // 1Min, 1Sec, trades, quotes
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	// httpClient is shared across all backfill cycles (initial + reconnect
	// gap-fills). Reusing a single client preserves the underlying
	// http.Transport's connection pool, so reconnect-triggered backfills
	// don't re-pay TCP dial + TLS handshake costs and don't leak
	// orphaned idle connections from prior cycles.
	httpClient *http.Client

	// subMgr is the ref-counted source of truth for dynamic tick
	// subscriptions. Non-nil only when dynamic_ticks is enabled.
	subMgr *subscription.Manager
	// dynClients is the registry of currently-live dynamic tick clients,
	// keyed by DataType. Non-nil only when dynamic_ticks is enabled.
	dynClients *dynClientRegistry
	// enabledTickTypes is the set of tick DataTypes in ws_data_types
	// (subset of {Trades, Quotes}). Used by the control loop's reconcile sweep.
	enabledTickTypes []subscription.DataType
}

// NewBgWorker returns a new instance of MassiveFetcher.
// nolint:deadcode // plugin interface
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	data, _ := json.Marshal(normalizeMapKeys(conf))
	config := massiveconfig.FetcherConfig{}
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parse massive config: %w", err)
	}

	// Validate config consistency (sync_queries required for each query_start key, etc.).
	if err := massiveconfig.ValidateConfig(&config); err != nil {
		return nil, err
	}

	// Fetch symbols from PostgreSQL if configured, otherwise use static Symbols list.
	if config.SymbolsDSN != "" {
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
		// Convert static Symbols to SymbolInfos (no listing dates, no IDs).
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

	// Dynamic tick subscriptions (optional). When enabled, trades/quotes
	// streams start empty and are driven at runtime.
	var (
		subMgr           *subscription.Manager
		dynClients       *dynClientRegistry
		enabledTickTypes []subscription.DataType
	)
	if config.DynamicTicks {
		for _, dt := range subscription.AllDataTypes {
			if _, ok := wsDataTypes[dt.String()]; ok {
				enabledTickTypes = append(enabledTickTypes, dt)
			}
		}
		subMgr = subscription.New(config.MaxDynamicSymbols)
		dynClients = newDynClientRegistry()
		// Publish the singleton so an in-process trigger (Approach A) reaches
		// the same Manager the bgworker owns.
		subscription.Default = subMgr
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Long-lived HTTP client, shared across every runBackfill() invocation.
	// The transport's connection pool persists across reconnect gap-fills,
	// avoiding repeated DNS + TCP dial + TLS handshake on each cycle.
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxConnsPerHost,
			MaxConnsPerHost:     maxConnsPerHost,
		},
		Timeout: backfillHTTPTimeout,
	}

	return &MassiveFetcher{
		config:           config,
		wsDataTypes:      wsDataTypes,
		ctx:              ctx,
		cancel:           cancel,
		httpClient:       httpClient,
		subMgr:           subMgr,
		dynClients:       dynClients,
		enabledTickTypes: enabledTickTypes,
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

	// Start the single dynamic-subscription control goroutine (if enabled)
	// before streaming so it is ready to route Change events as soon as the
	// shared connection registers its tick topics.
	if mf.subMgr != nil {
		mf.wg.Add(1)
		go mf.runControlLoop()
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

// streamTopic bundles a configured WebSocket topic with everything needed to
// (re)subscribe it on the single shared connection and route its messages.
type streamTopic struct {
	dataType string                 // config name: "1Min", "1Sec", "trades", "quotes"
	topic    ws.Topic               // wire topic (A/AM/T/Q)
	handler  func([]byte)           // writes the decoded message to MarketStore
	tickers  []string               // static tickers to subscribe at handshake (nil for dynamic)
	dynamic  *subscription.DataType // non-nil if this topic is driven at runtime
}

// startStreaming opens a SINGLE WebSocket connection that multiplexes every
// configured data type (1Min/1Sec/trades/quotes) onto one socket.
//
// The Massive account permits exactly ONE concurrent WebSocket connection, but
// that connection may subscribe to all channels (A, AM, T, Q) at once. Opening
// a separate connection per data type trips the server's max_connections limit
// (all but the first are rejected), so everything shares one client.
//
// Static topics are subscribed during the handshake; dynamic tick topics
// (trades/quotes under dynamic_ticks) connect empty and are driven at runtime
// by the control loop.
func (mf *MassiveFetcher) startStreaming() {
	feed := ws.Feed(ws.RealTime)
	if mf.config.WSServer != "" {
		feed = ws.Feed(mf.config.WSServer)
	}

	topics := mf.buildStreamTopics()
	if len(topics) == 0 {
		log.Error("[massive] no valid ws_data_types to stream")
		return
	}

	// Connect once, subscribing all static topics during the handshake.
	client, err := mf.wsConnect(feed, topics)
	if err != nil {
		log.Error("[massive] initial connection failed: %v", err)
		// Retry loop will sleep until the next pre-market open and try again.
		mf.wg.Add(1)
		go mf.streamWithRestart(feed, topics, streamState{})
		return
	}

	mf.wg.Add(1)
	go mf.streamWithRestart(feed, topics, streamState{client: client})
}

// buildStreamTopics resolves the configured ws_data_types into the ordered set
// of topics to multiplex onto the shared connection.
func (mf *MassiveFetcher) buildStreamTopics() []streamTopic {
	// Tick handlers encode the FULL trade/quote schema using the shared mapping
	// (exchange ids → SIP chars) so the live feed merges into the same on-disk
	// buckets the REST/flat-file backfills write. Load the exchange map once.
	exMap := mapping.LoadExchangeMap(mf.httpClient)

	dataTypeHandlers := map[string]func([]byte){
		"1Min":   handlers.MakeBarsHandler("1Min"),
		"1Sec":   handlers.MakeBarsHandler("1Sec"),
		"trades": handlers.MakeTradeHandler(exMap),
		"quotes": handlers.MakeQuoteHandler(exMap),
	}

	tickers := mf.config.Symbols
	if len(tickers) == 0 {
		tickers = []string{"*"}
	}

	var topics []streamTopic
	for dataType := range mf.wsDataTypes {
		topic, ok := wsDataTypeToTopic[dataType]
		if !ok {
			log.Error("[massive] unknown data type %q, skipping", dataType)
			continue
		}

		st := streamTopic{
			dataType: dataType,
			topic:    topic,
			handler:  dataTypeHandlers[dataType],
			tickers:  tickers,
		}

		// Dynamic tick topics connect with no subscription; symbols are added
		// and removed at runtime by the control loop.
		if dynDT := mf.dynamicDataType(dataType); dynDT != nil {
			st.tickers = nil
			st.dynamic = dynDT
		}

		topics = append(topics, st)
	}
	return topics
}

// dynamicDataType returns a non-nil DataType when the given config data type is
// a tick stream running in dynamic mode; otherwise nil (static).
func (mf *MassiveFetcher) dynamicDataType(dataType string) *subscription.DataType {
	if mf.subMgr == nil {
		return nil
	}
	dt, ok := subscription.ParseDataType(dataType)
	if !ok {
		return nil
	}
	for _, e := range mf.enabledTickTypes {
		if e == dt {
			return &dt
		}
	}
	return nil
}

// streamState carries the mutable state of the single connection's retry loop:
// an optional pre-connected client (used for the first iteration so
// startStreaming's successful dial is not wasted) and whether the current
// attempt is a reconnect (which triggers a gap-fill backfill).
type streamState struct {
	client      *ws.Client
	isReconnect bool
}

// streamWithRestart is the outer retry loop for a single data type's WebSocket
// stream. On any fatal error (including connection-limit rejections), it sleeps
// until the next pre-market open (3:58 AM ET) and retries. On a successful
// reconnect it also triggers a backfill to cover any data gap.
//
// state.client, if set, is the pre-connected client used for the first
// iteration (skipping the connect step, since startStreaming already did it).
func (mf *MassiveFetcher) streamWithRestart(feed ws.Feed, topics []streamTopic, state streamState) {
	defer mf.wg.Done()

	for {
		var err error
		if state.client != nil {
			// Use the pre-connected client (first iteration from startStreaming).
			err = mf.stream(topics, state.client, state.isReconnect)
			state.client = nil // consumed; next iteration connects fresh
		} else {
			// Connect and stream.
			err = mf.connectAndStream(feed, topics, state.isReconnect)
		}

		if err == nil {
			// Clean shutdown via context cancellation.
			return
		}

		// Log differently for connection-limit vs other fatal errors.
		if errors.Is(err, ws.ErrConnectionLimit) {
			log.Error("[massive] connection limit reached, scheduling retry at next pre-market open")
		} else {
			log.Error("[massive] stream failed (%v), scheduling retry at next pre-market open", err)
		}

		// Sleep until 3:58 AM ET on the next trading day.
		wakeTime := nextPreMarketOpen(time.Now())
		log.Info("[massive] next reconnect attempt at %s", wakeTime.Format(time.RFC3339))

		select {
		case <-mf.ctx.Done():
			return
		case <-time.After(time.Until(wakeTime)):
			log.Info("[massive] waking up for scheduled reconnect")
		}

		state.isReconnect = true
	}
}

// wsConnect creates the single WebSocket client, registers every static topic's
// subscription, and performs the full synchronous handshake (dial → auth →
// subscribe → confirmation). Dynamic tick topics are NOT subscribed here; they
// connect empty and are driven at runtime by the control loop.
func (mf *MassiveFetcher) wsConnect(feed ws.Feed, topics []streamTopic) (*ws.Client, error) {
	client := ws.New(mf.config.APIKey, feed)
	for _, st := range topics {
		if st.dynamic != nil {
			continue // dynamic ticks subscribe at runtime
		}
		client.Subscribe(st.topic, st.tickers...)
	}

	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return client, nil
}

// connectAndStream creates the WebSocket client, connects, and streams messages
// until a fatal error or context cancellation. Used by streamWithRestart for
// reconnection attempts (the initial connection is done by startStreaming).
func (mf *MassiveFetcher) connectAndStream(feed ws.Feed, topics []streamTopic, isReconnect bool) error {
	client, err := mf.wsConnect(feed, topics)
	if err != nil {
		return err
	}
	return mf.stream(topics, client, isReconnect)
}

// stream runs the message loop for the already-connected shared client until
// the connection drops or the context is cancelled. The single socket carries
// messages for every configured topic; each message is routed to the matching
// handler by its "ev" (event type) field.
//
// It returns:
//   - nil on clean shutdown (context cancelled)
//   - ws.ErrConnectionLimit if the server rejected us for exceeding the connection limit
//   - ws.ErrAuthFailed if the API key is invalid
//   - a wrapped error for any other fatal failure
//
// When isReconnect is true, a background backfill is triggered to fill any data
// gap from the time the stream was down.
func (mf *MassiveFetcher) stream(topics []streamTopic, client *ws.Client, isReconnect bool) error {
	defer client.Close()

	router := newMessageRouter(topics)

	// Log what we're streaming.
	names := make([]string, len(topics))
	for i, st := range topics {
		names[i] = st.dataType
	}
	log.Info("[massive] streaming %v on a single connection", names)

	// For each dynamic tick topic: register this fresh client and immediately
	// replay the current desired set onto it (the connection was established
	// with an empty tick subscription). Deregister when the connection ends so
	// the control loop skips edge events until the next reconnect replays.
	for _, st := range topics {
		if st.dynamic == nil {
			continue
		}
		dt := *st.dynamic
		mf.dynClients.set(dt, client)
		mf.replaceSubscriptions(client, dt, mf.subMgr.Active(dt))
		defer mf.dynClients.clear(dt)
	}

	// On reconnect, backfill any data gap in the background.
	if isReconnect && len(mf.config.QueryStart) > 0 && !utils.InstanceConfig.NoBackfill {
		go func() {
			log.Info("[massive] running gap-fill backfill after reconnect")
			if err := mf.runBackfill(); err != nil && err != context.Canceled {
				log.Warn("[massive] gap-fill backfill failed: %v", err)
			}
		}()
	}

	for {
		select {
		case <-mf.ctx.Done():
			// Check if there's a pending error that explains why the context
			// was cancelled (e.g., backfill detected HTTP 401 first).
			select {
			case err := <-client.Err():
				log.Error("[massive] error during shutdown: %v", err)
			default:
			}
			log.Info("[massive] stopping stream")
			return nil
		case <-client.Done():
			// Connection lost. Retrieve the error.
			select {
			case err := <-client.Err():
				return fmt.Errorf("fatal stream error: %w", err)
			default:
				return fmt.Errorf("connection closed unexpectedly")
			}
		case msg := <-client.Output():
			router.dispatch(msg)
		}
	}
}

// messageRouter dispatches each incoming WebSocket message to the handler for
// its event type. The single multiplexed socket carries A/AM/T/Q events, so the
// "ev" field identifies which configured handler should process it.
type messageRouter struct {
	byEvent map[string]func([]byte)
}

// evHeader is the minimal envelope used to read just the event-type tag.
type evHeader struct {
	Ev string `json:"ev"`
}

// newMessageRouter builds an ev→handler map from the configured topics. The
// wire event type matches the topic prefix (A, AM, T, Q).
func newMessageRouter(topics []streamTopic) *messageRouter {
	byEvent := make(map[string]func([]byte), len(topics))
	for _, st := range topics {
		byEvent[st.topic.Prefix()] = st.handler
	}
	return &messageRouter{byEvent: byEvent}
}

// dispatch routes a single message to its handler based on the "ev" field.
func (r *messageRouter) dispatch(msg []byte) {
	var h evHeader
	if err := json.Unmarshal(msg, &h); err != nil {
		log.Warn("[massive] could not read event type from message: %v", err)
		return
	}
	handler, ok := r.byEvent[h.Ev]
	if !ok {
		// Unconfigured event type (e.g., a status frame that slipped through,
		// or a channel we don't handle). Ignore quietly.
		return
	}
	handler(msg)
}

// runControlLoop is the single goroutine that drives dynamic tick subscriptions
// on the shared client. It consumes the manager's single Change channel and
// applies each event to the connection using the event's DataType→topic mapping
// (edge-triggered), with a slow reconcile sweep as a level-fallback safety net.
// There is exactly ONE such goroutine for the whole worker — a Go channel
// delivers each value to one receiver, so a second consumer would steal events.
func (mf *MassiveFetcher) runControlLoop() {
	defer mf.wg.Done()

	changes := mf.subMgr.Changes()
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mf.ctx.Done():
			return
		case chg := <-changes: // edge (primary path)
			client := mf.dynClients.get(chg.DataType)
			if client == nil {
				// Connection is down/mid-reconnect; the post-reconnect replay
				// will apply the current Active() set.
				continue
			}
			topic := topicFor(chg.DataType)
			var err error
			if chg.Active {
				err = client.AddTickers(topic, chg.Symbol)
			} else {
				err = client.RemoveTickers(topic, chg.Symbol)
			}
			if err != nil {
				// Intentionally logged-and-continued: the reconcile sweep is the
				// backstop. A wedged/closed client converges on the next replay.
				log.Warn("[massive] control: %s %s/%s failed: %v",
					actionLabel(chg.Active), chg.Symbol, chg.DataType, err)
			}
		case <-ticker.C: // level fallback (safety net)
			for _, dt := range mf.enabledTickTypes {
				if client := mf.dynClients.get(dt); client != nil {
					mf.replaceSubscriptions(client, dt, mf.subMgr.Active(dt))
				}
			}
		}
	}
}

func actionLabel(active bool) string {
	if active {
		return "subscribe"
	}
	return "unsubscribe"
}

// replaceSubscriptions makes the client's live subscription for dt match want
// exactly. It diffs against the client's retained set and issues the minimal
// add/remove. Used for the post-reconnect replay and the periodic reconcile.
func (mf *MassiveFetcher) replaceSubscriptions(client *ws.Client, dt subscription.DataType, want []string) {
	topic := topicFor(dt)
	have := client.RetainedTickers(topic)

	wantSet := make(map[string]struct{}, len(want))
	for _, s := range want {
		wantSet[s] = struct{}{}
	}
	haveSet := make(map[string]struct{}, len(have))
	for _, s := range have {
		haveSet[s] = struct{}{}
	}

	var toAdd, toRemove []string
	for s := range wantSet {
		if _, ok := haveSet[s]; !ok {
			toAdd = append(toAdd, s)
		}
	}
	for s := range haveSet {
		if _, ok := wantSet[s]; !ok {
			toRemove = append(toRemove, s)
		}
	}

	if len(toAdd) > 0 {
		if err := client.AddTickers(topic, toAdd...); err != nil {
			log.Warn("[massive] control: reconcile add %v on %s failed: %v", toAdd, dt, err)
		}
	}
	if len(toRemove) > 0 {
		if err := client.RemoveTickers(topic, toRemove...); err != nil {
			log.Warn("[massive] control: reconcile remove %v on %s failed: %v", toRemove, dt, err)
		}
	}
}

// --- SubscriptionController implementation (Approach B) ---

// Subscribe acquires a runtime subscription for symbol on each named data type.
func (mf *MassiveFetcher) Subscribe(symbol string, dataTypes []string) error {
	if mf.subMgr == nil {
		return fmt.Errorf("dynamic subscriptions not enabled (set dynamic_ticks: true)")
	}
	dts, err := mf.parseDataTypes(dataTypes)
	if err != nil {
		return err
	}
	for _, dt := range dts {
		if _, err := mf.subMgr.Acquire(symbol, dt); err != nil {
			return err
		}
	}
	return nil
}

// Unsubscribe releases a runtime subscription for symbol on each named data type.
func (mf *MassiveFetcher) Unsubscribe(symbol string, dataTypes []string) error {
	if mf.subMgr == nil {
		return fmt.Errorf("dynamic subscriptions not enabled (set dynamic_ticks: true)")
	}
	dts, err := mf.parseDataTypes(dataTypes)
	if err != nil {
		return err
	}
	for _, dt := range dts {
		mf.subMgr.Release(symbol, dt)
	}
	return nil
}

// ActiveSubscriptions returns the current intended subscription set as
// symbol -> data types.
func (mf *MassiveFetcher) ActiveSubscriptions() map[string][]string {
	out := map[string][]string{}
	if mf.subMgr == nil {
		return out
	}
	for _, dt := range subscription.AllDataTypes {
		for _, sym := range mf.subMgr.Active(dt) {
			out[sym] = append(out[sym], dt.String())
		}
	}
	return out
}

// parseDataTypes converts the wire data-type names to DataTypes, erroring on
// any unrecognized name (no silent drop).
func (mf *MassiveFetcher) parseDataTypes(names []string) ([]subscription.DataType, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("no data types provided")
	}
	out := make([]subscription.DataType, 0, len(names))
	for _, n := range names {
		dt, ok := subscription.ParseDataType(n)
		if !ok {
			return nil, fmt.Errorf("unknown data type %q: must be one of trades, quotes", n)
		}
		out = append(out, dt)
	}
	return out, nil
}

// nextPreMarketOpen returns 3:58 AM ET on the next applicable trading day.
// If it is currently before 3:58 AM on a trading day, returns today's 3:58 AM.
// Otherwise, returns 3:58 AM on the next trading day per the NASDAQ calendar.
func nextPreMarketOpen(now time.Time) time.Time {
	tz := calendar.Nasdaq.Tz()
	local := now.In(tz)

	// Pre-market opens at 4:00 AM ET; we connect 2 minutes early.
	targetHour := 3
	targetMin := 58

	today358 := time.Date(local.Year(), local.Month(), local.Day(),
		targetHour, targetMin, 0, 0, tz)

	// If we haven't passed 3:58 AM today and today is a trading day, use today.
	if local.Before(today358) && calendar.Nasdaq.IsMarketDay(local) {
		return today358
	}

	// Otherwise, find the next trading day.
	nextDay := calendar.Nasdaq.NextMarketDay(local)
	return time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(),
		targetHour, targetMin, 0, 0, tz)
}

// Shutdown cancels the context and stops all background operations.
func (mf *MassiveFetcher) Shutdown() {
	mf.cancel()
	// Release pooled HTTP connections so they don't linger past shutdown.
	if t, ok := mf.httpClient.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
}

// runBackfill performs a historical data backfill from the Massive REST API
// for all configured data types and symbols.
//
// The backfill uses sync timestamps stored in PostgreSQL (via sync_queries) to
// track confirmed coverage for each symbol and data type. On each run, it checks
// for two types of gaps:
//   - Forward gap: new market data exists past the newest sync timestamp.
//   - Backward gap: query_start was moved earlier than the oldest sync timestamp.
//
// After each successful backfill for a symbol+datatype, the sync timestamp is
// immediately updated in PostgreSQL so that a restart does not re-fetch the same data.
//
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

	// Reuse the long-lived httpClient (constructed in NewBgWorker) across
	// every backfill cycle so the underlying http.Transport's connection
	// pool persists across reconnect-triggered gap-fills.
	httpClient := mf.httpClient

	// Open a connection pool for sync reads/writes throughout the backfill.
	// Using pgxpool.Pool instead of pgx.Conn so that multiple symbol-level
	// goroutines can safely perform concurrent database operations.
	var pgPool *pgxpool.Pool
	hasSyncQueries := len(mf.config.SyncQueries) > 0 && mf.config.SymbolsDSN != ""
	if hasSyncQueries {
		ctx, cancel := context.WithTimeout(mf.ctx, massiveconfig.DBQueryTimeout)
		var err error
		pgPool, err = pgxpool.New(ctx, mf.config.SymbolsDSN)
		cancel()
		if err != nil {
			return fmt.Errorf("connect to postgres for sync: %w", err)
		}
		defer pgPool.Close()
	}

	parallelism := mf.config.BackfillParallelism
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	apiWP := worker.NewWorkerPool(mf.ctx, parallelism)
	writerWP := worker.NewWorkerPool(mf.ctx, 1)

	log.Info("[massive] starting backfill (%d symbol workers, extended hours end: %s, regular hours end: %s)",
		parallelism, endExtended.Format(time.RFC3339), endRegular.Format(time.RFC3339))

	// --- Phase 1: Flat file backfill (historical) + REST current-day backfill ---
	// These run concurrently since their date ranges never overlap:
	//   - Flat files cover dates where files are expected to be published
	//     (up to flatFileAvailableThrough).
	//   - REST covers the gap from the flat file cutoff through the latest
	//     market time (typically today and possibly yesterday).
	var flatFileDone map[string]bool
	var phase1 sync.WaitGroup

	phase1.Add(1)
	go func() {
		defer phase1.Done()
		flatFileDone = mf.runFlatFileBackfill(pgPool, parallelism, endRegular, endExtended, now)
	}()

	phase1.Add(1)
	go func() {
		defer phase1.Done()
		mf.restBackfillCurrentDay(httpClient, endRegular, endExtended, now, batchSize, adjusted, parallelism)
	}()

	phase1.Wait()

	for _, symInfo := range mf.config.SymbolInfos {
		// Check for cancellation between symbols.
		select {
		case <-mf.ctx.Done():
			apiWP.CloseAndWait()
			writerWP.CloseAndWait()
			return mf.ctx.Err()
		default:
		}

		if symInfo.Symbol == "*" {
			log.Warn("[massive] backfill with wildcard symbol is not supported, use the backfiller CLI instead")
			continue
		}

		currentSymInfo := symInfo
		apiWP.Do(func() {
			// Iterate over query_start keys to determine what to backfill.
			// Keys are either timeframes (e.g., "1Min", "1D") for bars,
			// or "trades"/"quotes" for tick data.
			//
			// Finest granularity first, 1D last (see
			// massiveconfig.OrderedBackfillKeys).
			for _, key := range massiveconfig.OrderedBackfillKeys(mf.config.QueryStart) {
				startDateStr := mf.config.QueryStart[key]
				// Skip keys already handled by flat file backfill.
				if flatFileDone[key] {
					continue
				}

				// Check for cancellation between data types.
				select {
				case <-mf.ctx.Done():
					return
				default:
				}

				configStart, err := time.ParseInLocation(dateFormat, startDateStr, calendar.Nasdaq.Tz())
				if err != nil {
					log.Error("[massive] invalid query_start date %q for %s: %v", startDateStr, key, err)
					continue
				}

				// Apply per-symbol listing date override if available and more recent.
				effectiveStart := massiveconfig.EffectiveBackfillStart(configStart, currentSymInfo.ListingDate)

				// Use regular market hours for daily+ timeframes, extended hours for intraday.
				end := endExtended
				if isDailyOrLonger(key) {
					end = endRegular
				}

				// Check if effectiveStart is in the future (listing date not yet reached).
				if effectiveStart.After(end) {
					continue
				}

				// Read the sync window from the database.
				var sw massiveconfig.SyncWindow
				syncQueries, haveSQ := mf.config.SyncQueries[key]
				if haveSQ && pgPool != nil && currentSymInfo.ID != 0 {
					sw = massiveconfig.ReadSyncWindow(mf.ctx, pgPool, syncQueries.Read, currentSymInfo.ID)
				}

				// --- Forward gap: new market data since last sync ---
				mf.backfillForward(httpClient, pgPool, writerWP, currentSymInfo, key, effectiveStart, end, sw, syncQueries, batchSize, adjusted)

				// --- Backward gap: query_start moved earlier than oldest sync ---
				mf.backfillBackward(httpClient, pgPool, writerWP, currentSymInfo, key, effectiveStart, sw, syncQueries, batchSize, adjusted)
			}
		})
	}

	apiWP.CloseAndWait()
	writerWP.CloseAndWait()
	log.Info("[massive] backfill complete")
	return nil
}

// runFlatFileBackfill runs the flat file backfill phase for all query_start keys
// that have a corresponding flat file data type (1D, 1Min). It uses S3 flat files
// instead of the REST API for bulk bar data.
//
// Returns a set of query_start keys that were successfully handled, so the
// subsequent REST backfill loop can skip them.
//
// Coverage is tracked per symbol via sync_queries (the same store the REST
// backfill uses) whenever a database is configured. Without a database there is
// no per-symbol store, so the global checkpoint file is used instead.
func (mf *MassiveFetcher) runFlatFileBackfill(
	pgPool *pgxpool.Pool, parallelism int, endRegular, endExtended, now time.Time,
) map[string]bool {
	done := make(map[string]bool)

	// Check for S3 credentials.
	s3AccessKey := mf.config.S3AccessKey
	s3SecretKey := mf.config.S3SecretKey
	if s3AccessKey == "" {
		s3AccessKey = resolveEnvVar("MASSIVE_S3_ACCESS_KEY")
	}
	if s3SecretKey == "" {
		s3SecretKey = resolveEnvVar("MASSIVE_S3_SECRET_KEY")
	}
	if s3AccessKey == "" || s3SecretKey == "" {
		// No S3 credentials: skip flat file backfill silently.
		return done
	}

	s3Client, err := flatfiles.NewS3Client(s3AccessKey, s3SecretKey)
	if err != nil {
		log.Warn("[massive] failed to create S3 client, skipping flat file backfill: %v", err)
		return done
	}

	// Read checkpoint from the data root directory.
	dataDir := executor.ThisInstance.CatalogDir.GetPath()
	checkpoint, err := flatfiles.ReadCheckpoint(dataDir)
	if err != nil {
		log.Warn("[massive] failed to read flat file checkpoint, starting fresh: %v", err)
		checkpoint = make(flatfiles.Checkpoint)
	}

	// Build symbol set from the current universe.
	symbolSet := make(map[string]bool, len(mf.config.SymbolInfos))
	for _, si := range mf.config.SymbolInfos {
		symbolSet[si.Symbol] = true
	}

	w := &backfill.DirectWriter{}

	// Finest granularity first, 1D last: the ondiskagg 1Min->1D trigger would
	// otherwise overwrite the authoritative vendor daily bar. See
	// massiveconfig.OrderedBackfillKeys.
	for _, key := range massiveconfig.OrderedBackfillKeys(mf.config.QueryStart) {
		startDateStr := mf.config.QueryStart[key]
		select {
		case <-mf.ctx.Done():
			return done
		default:
		}

		ffType, ok := flatfiles.DataTypes[key]
		if !ok {
			continue // not a flat file type (e.g., trades, quotes)
		}

		configStart, err := time.Parse(dateFormat, startDateStr)
		if err != nil {
			log.Warn("[massive] invalid query_start date %q for %s, skipping flat file backfill", startDateStr, key)
			continue
		}

		// Determine end date for this data type.
		end := endExtended
		if isDailyOrLonger(key) {
			end = endRegular
		}
		// Flat files are date-oriented; use just the date portion.
		endDate := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)

		// Cap end date to the latest date whose flat file is expected to be
		// published. Flat files for date D become available ~noon ET on D+1.
		// Dates beyond this cutoff are handled by the REST current-day backfill.
		maxFlatFile := flatFileAvailableThrough(now)
		if endDate.After(maxFlatFile) {
			endDate = maxFlatFile
		}

		// Preferred path: per-symbol coverage, shared with the REST backfill.
		// The global checkpoint records dates the downloader processed, not
		// coverage each symbol actually has, so a symbol added after a range
		// was downloaded would otherwise never be backfilled for that range.
		if syncQueries, haveSQ := mf.config.SyncQueries[key]; haveSQ && pgPool != nil {
			flatfiles.RunSyncedBackfill(mf.ctx, s3Client, w, pgPool, mf.config.SymbolInfos,
				syncQueries, key, ffType, configStart, endDate,
				flatfiles.BackfillConfig{Parallelism: parallelism})
			done[key] = true
			continue
		}

		sw := checkpoint[key]
		changed := false

		// --- Forward gap: from checkpoint newest (or configStart) to endDate ---
		var forwardStart time.Time
		if sw.Newest != "" {
			parsed, parseErr := time.Parse(dateFormat, sw.Newest)
			if parseErr == nil {
				forwardStart = parsed.AddDate(0, 0, 1) // day after last backfilled
			}
		}
		if forwardStart.IsZero() {
			forwardStart = configStart
		}

		if !forwardStart.After(endDate) {
			forwardDates := flatfiles.MarketDays(forwardStart, endDate)
			if len(forwardDates) > 0 {
				log.Info("[massive] flat file forward backfill %s: %s to %s (%d market days)",
					key, forwardStart.Format(dateFormat), endDate.Format(dateFormat), len(forwardDates))

				if sw.Oldest == "" {
					sw.Oldest = configStart.Format(dateFormat)
				}

				// Progress callback: advance sw.Newest as the contiguous
				// high-water mark moves forward through the date list.
				onForwardProgress := func(date time.Time) {
					sw.Newest = date.Format(dateFormat)
					checkpoint[key] = sw
					if writeErr := flatfiles.WriteCheckpoint(dataDir, checkpoint); writeErr != nil {
						log.Warn("[massive] failed to write flat file checkpoint: %v", writeErr)
					}
				}

				_, _, backfillErr := flatfiles.BackfillDates(mf.ctx, s3Client, w, symbolSet, key, ffType.S3Prefix, ffType.S3DataType, forwardDates, flatfiles.BackfillConfig{
					Parallelism: parallelism,
					OnProgress:  onForwardProgress,
				})
				if backfillErr != nil {
					log.Warn("[massive] flat file forward backfill %s: %v", key, backfillErr)
				}
				changed = true
			}
		}

		// --- Backward gap: configStart moved earlier than checkpoint oldest ---
		if sw.Oldest != "" {
			oldest, parseErr := time.Parse(dateFormat, sw.Oldest)
			if parseErr == nil && configStart.Before(oldest) {
				backwardEnd := oldest.AddDate(0, 0, -1) // day before oldest backfilled
				backwardDates := flatfiles.MarketDays(configStart, backwardEnd)
				if len(backwardDates) > 0 {
					log.Info("[massive] flat file backward backfill %s: %s to %s (%d market days)",
						key, configStart.Format(dateFormat), backwardEnd.Format(dateFormat), len(backwardDates))

					// Progress callback: the high-water mark tells us the
					// contiguous range [dates[0]..date] is complete, so
					// sw.Oldest can move to dates[0] (= configStart) once
					// any progress is made.
					onBackwardProgress := func(date time.Time) {
						sw.Oldest = backwardDates[0].Format(dateFormat)
						checkpoint[key] = sw
						if writeErr := flatfiles.WriteCheckpoint(dataDir, checkpoint); writeErr != nil {
							log.Warn("[massive] failed to write flat file checkpoint: %v", writeErr)
						}
					}

					_, _, backfillErr := flatfiles.BackfillDates(mf.ctx, s3Client, w, symbolSet, key, ffType.S3Prefix, ffType.S3DataType, backwardDates, flatfiles.BackfillConfig{
						Parallelism: parallelism,
						OnProgress:  onBackwardProgress,
					})
					if backfillErr != nil {
						log.Warn("[massive] flat file backward backfill %s: %v", key, backfillErr)
					}
					changed = true
				}
			}
		}

		if changed {
			// Final checkpoint write to ensure the last state is persisted.
			checkpoint[key] = sw
			if writeErr := flatfiles.WriteCheckpoint(dataDir, checkpoint); writeErr != nil {
				log.Warn("[massive] failed to write flat file checkpoint: %v", writeErr)
			}
		}

		done[key] = true
	}

	return done
}

// restBackfillCurrentDay backfills recent market dates that are not yet covered
// by flat files (which have a ~1 day publication delay). It uses the Massive
// REST API to fetch 1D and 1Min bars for all symbols in parallel.
//
// The date range starts the day after flatFileAvailableThrough(now) and extends
// through the latest market time. This typically covers 1-2 market days (today
// and possibly yesterday if flat files haven't been published yet). On weekends
// after the publication cutoff, this range is empty and no REST calls are made.
func (mf *MassiveFetcher) restBackfillCurrentDay(
	httpClient *http.Client,
	endRegular, endExtended, now time.Time,
	batchSize int,
	adjusted bool,
	parallelism int,
) {
	restStart := flatFileAvailableThrough(now).AddDate(0, 0, 1)

	// Finest granularity first, 1D last (see massiveconfig.OrderedBackfillKeys).
	for _, key := range massiveconfig.OrderedBackfillKeys(mf.config.QueryStart) {
		select {
		case <-mf.ctx.Done():
			return
		default:
		}

		// Only handle flat-file data types (1D, 1Min). Other data types
		// (trades, quotes) are handled by the per-symbol REST loop.
		if _, ok := flatfiles.DataTypes[key]; !ok {
			continue
		}

		end := endExtended
		if isDailyOrLonger(key) {
			end = endRegular
		}

		// Convert end to a date for market day enumeration.
		endDate := time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)
		if restStart.After(endDate) {
			// No gap: flat files cover everything through endDate.
			continue
		}

		restDates := flatfiles.MarketDays(restStart, endDate)
		if len(restDates) == 0 {
			continue
		}

		startTime := time.Now()
		log.Info("[massive] REST current-day backfill %s: %s to %s (%d market days, %d symbols, parallelism=%d)",
			key, restStart.Format(dateFormat), endDate.Format(dateFormat),
			len(restDates), len(mf.config.SymbolInfos), parallelism)

		// Use the date range boundaries as the API time window.
		// restStart is midnight UTC; end is the actual market close time.
		apiFrom := time.Date(restStart.Year(), restStart.Month(), restStart.Day(),
			0, 0, 0, 0, calendar.Nasdaq.Tz())
		apiTo := end

		apiWP := worker.NewWorkerPool(mf.ctx, parallelism)
		writerWP := worker.NewWorkerPool(mf.ctx, 1)

		for _, symInfo := range mf.config.SymbolInfos {
			select {
			case <-mf.ctx.Done():
				apiWP.CloseAndWait()
				writerWP.CloseAndWait()
				return
			default:
			}

			if symInfo.Symbol == "*" {
				continue
			}

			currentSym := symInfo.Symbol
			currentKey := key
			apiWP.Do(func() {
				if err := rest.Bars(mf.ctx, httpClient, currentSym, currentKey,
					apiFrom, apiTo, batchSize, adjusted, writerWP, nil); err != nil {
					if err == context.Canceled {
						return
					}
					if errors.Is(err, api.ErrAuthFailed) {
						log.Error("[massive] API authentication failed during REST current-day backfill: %v", err)
						mf.cancel()
						return
					}
					log.Warn("[massive] REST current-day backfill %s %s: %v", currentSym, currentKey, err)
				}
			})
		}

		apiWP.CloseAndWait()
		writerWP.CloseAndWait()

		log.Info("[massive] REST current-day backfill %s complete: %d symbols in %s",
			key, len(mf.config.SymbolInfos), time.Since(startTime).Round(time.Millisecond))
	}
}

// resolveEnvVar returns the value of an environment variable, or empty string.
func resolveEnvVar(name string) string {
	return os.Getenv(name)
}

// backfillForward handles the forward gap: fetching data from the newest sync
// timestamp (or local data) up to the market close time.
func (mf *MassiveFetcher) backfillForward(
	httpClient *http.Client,
	db massiveconfig.PGDB,
	writerWP *worker.Pool,
	symInfo massiveconfig.SymbolInfo,
	dataType string,
	effectiveStart, end time.Time,
	sw massiveconfig.SyncWindow,
	syncQueries massiveconfig.SyncQuerySet,
	batchSize int,
	adjusted bool,
) {
	symbol := symInfo.Symbol

	// If sync says we're caught up, skip.
	if sw.Newest != nil && !sw.Newest.Before(end) {
		return
	}

	// Determine where to start the forward backfill.
	// Priority: sync newest > local lastTS > effectiveStart.
	var forwardStart time.Time
	if sw.Newest != nil {
		forwardStart = sw.Newest.Add(time.Nanosecond)
	} else {
		// No sync record. Check local data as a fallback to avoid re-fetching
		// data that's already on disk (e.g., first run with sync enabled on
		// an existing database).
		tbk := tbkForDataType(symbol, dataType)
		lastTS := findLastTimestamp(tbk)
		if !lastTS.IsZero() {
			forwardStart = lastTS.Add(time.Nanosecond)
		} else {
			forwardStart = effectiveStart
		}
	}

	if !forwardStart.Before(end) {
		return
	}

	log.Info("[massive] %s backfilling %s from %s to %s",
		symbol, dataType, forwardStart.Format(time.RFC3339), end.Format(time.RFC3339))

	err := mf.executeBackfill(httpClient, writerWP, symbol, dataType, forwardStart, end, batchSize, adjusted)
	if err != nil {
		if err == context.Canceled {
			return
		}
		if errors.Is(err, api.ErrAuthFailed) {
			log.Error("[massive] API authentication failed, stopping backfill: %v", err)
			mf.cancel()
			return
		}
		log.Warn("[massive] failed to backfill %s %s: %v", symbol, dataType, err)
		return
	}

	// Write the newest sync timestamp after successful backfill.
	if db != nil && syncQueries.WriteNewest != "" && symInfo.ID != 0 {
		if writeErr := massiveconfig.WriteSyncTimestamp(mf.ctx, db, syncQueries.WriteNewest, symInfo.ID, end); writeErr != nil {
			log.Warn("[massive] failed to write newest sync for %s %s: %v", symbol, dataType, writeErr)
		}
	}

	// Also write oldest if this is the first sync (no prior record).
	if sw.Oldest == nil && db != nil && syncQueries.WriteOldest != "" && symInfo.ID != 0 {
		writeStart := effectiveStart
		if sw.Newest != nil {
			// We only filled forward, oldest was already set or doesn't need to move.
			writeStart = *sw.Newest
		}
		// On first sync, oldest = effectiveStart (the beginning of what we requested).
		if sw.Newest == nil {
			if writeErr := massiveconfig.WriteSyncTimestamp(mf.ctx, db, syncQueries.WriteOldest, symInfo.ID, writeStart); writeErr != nil {
				log.Warn("[massive] failed to write oldest sync for %s %s: %v", symbol, dataType, writeErr)
			}
		}
	}
}

// backfillBackward handles the backward gap: fetching data from effectiveStart
// up to the oldest sync timestamp when query_start has been moved earlier.
func (mf *MassiveFetcher) backfillBackward(
	httpClient *http.Client,
	db massiveconfig.PGDB,
	writerWP *worker.Pool,
	symInfo massiveconfig.SymbolInfo,
	dataType string,
	effectiveStart time.Time,
	sw massiveconfig.SyncWindow,
	syncQueries massiveconfig.SyncQuerySet,
	batchSize int,
	adjusted bool,
) {
	symbol := symInfo.Symbol

	// Backward backfill only makes sense if we have an existing sync record
	// and effectiveStart is earlier than our oldest coverage.
	if sw.Oldest == nil || !effectiveStart.Before(*sw.Oldest) {
		return
	}

	backwardEnd := *sw.Oldest

	log.Info("[massive] %s backfilling %s backward from %s to %s",
		symbol, dataType, effectiveStart.Format(time.RFC3339), backwardEnd.Format(time.RFC3339))

	err := mf.executeBackfill(httpClient, writerWP, symbol, dataType, effectiveStart, backwardEnd, batchSize, adjusted)
	if err != nil {
		if err == context.Canceled {
			return
		}
		if errors.Is(err, api.ErrAuthFailed) {
			log.Error("[massive] API authentication failed, stopping backfill: %v", err)
			mf.cancel()
			return
		}
		log.Warn("[massive] failed to backfill %s %s backward: %v", symbol, dataType, err)
		return
	}

	// Update the oldest sync timestamp.
	if db != nil && syncQueries.WriteOldest != "" && symInfo.ID != 0 {
		if writeErr := massiveconfig.WriteSyncTimestamp(mf.ctx, db, syncQueries.WriteOldest, symInfo.ID, effectiveStart); writeErr != nil {
			log.Warn("[massive] failed to write oldest sync for %s %s: %v", symbol, dataType, writeErr)
		}
	}
}

// executeBackfill dispatches a backfill request to the appropriate function
// based on data type (trades, quotes, or bar timeframe).
func (mf *MassiveFetcher) executeBackfill(
	httpClient *http.Client,
	writerWP *worker.Pool,
	symbol, dataType string,
	start, end time.Time,
	batchSize int,
	adjusted bool,
) error {
	switch dataType {
	case "trades":
		return rest.Trades(mf.ctx, httpClient, symbol, start, end, batchSize, writerWP, nil)
	case "quotes":
		return rest.Quotes(mf.ctx, httpClient, symbol, start, end, batchSize, writerWP, nil)
	default:
		return rest.Bars(mf.ctx, httpClient, symbol, dataType, start, end, batchSize, adjusted, writerWP, nil)
	}
}

// tbkForDataType returns the TimeBucketKey for a symbol and data type.
func tbkForDataType(symbol, dataType string) *io.TimeBucketKey {
	switch dataType {
	case "trades":
		return io.NewTimeBucketKey(models.TradeBucketKey(symbol))
	case "quotes":
		return io.NewTimeBucketKey(models.QuoteBucketKey(symbol))
	default:
		return io.NewTimeBucketKey(models.BarBucketKey(symbol, dataType))
	}
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

// tickTolerance is the tolerance used for trades and quotes when deciding
// whether backfill data is up to date. Tick data has no fixed period, so
// we use a small fixed window: if the last tick is within this duration of
// the session close, we consider the data complete.
const tickTolerance = 1 * time.Minute

// backfillDecision contains the result of determineBackfillStart.
type backfillDecision struct {
	start time.Time // When to start backfilling from
	skip  bool      // If true, skip backfilling entirely
}

// determineBackfillStart checks for existing data and returns the appropriate
// start time for backfill. This is the fallback path used when sync queries
// are not configured (static symbols without a database).
//
// The dataType parameter is used to determine whether the data is "up to date":
//   - For bar timeframes (e.g., "1Min", "1D"), the last bar in a session starts
//     one period before close, so a small gap between lastTS and end is expected.
//   - For tick data ("trades", "quotes"), there is no fixed period; a 1-minute
//     tolerance is used instead.
//
// Logic:
//   - No data exists: backfill from effectiveStart.
//   - Data exists and up to date: skip.
//   - Data exists but behind: backfill from lastTS + 1ns.
func (mf *MassiveFetcher) determineBackfillStart(
	tbk *io.TimeBucketKey,
	effectiveStart, end time.Time,
	dataType string,
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

	if isUpToDate(lastTS, effectiveStart, end, dataType, calendar.Nasdaq.Tz()) {
		return backfillDecision{start: start, skip: true}
	}

	return backfillDecision{start: start, skip: false}
}

// isUpToDate determines whether existing data is sufficiently current that
// backfill can be skipped. This is a pure function with no database
// dependency, making it easy to test.
//
// The logic varies by data type:
//   - Intraday bars (< 1D): the last bar of a session starts one timeframe
//     period before close. If lastTS >= end - period, data is up to date.
//   - Daily+ bars (1D, 1W, 1M, 1Y): the bar epoch is at a canonical time
//     (e.g., midnight) that may be far from the close time. Instead of a
//     duration tolerance, we compare calendar periods in the market timezone:
//     same date for 1D, same ISO week for 1W, same month for 1M, same year
//     for 1Y.
//   - Tick data (trades, quotes): no fixed period; uses a small constant
//     tolerance (tickTolerance).
//
// If lastTS is zero (no data), the function returns false (not up to date)
// unless effectiveStart is at or past end (listing date in the future).
func isUpToDate(lastTS, effectiveStart, end time.Time, dataType string, marketTZ *time.Location) bool {
	// Compute the start time the same way determineBackfillStart does.
	var start time.Time
	if lastTS.IsZero() {
		start = effectiveStart
	} else {
		start = lastTS.Add(time.Nanosecond)
	}

	// If start is already at or past end, nothing to fetch regardless of type.
	if !start.Before(end) {
		return true
	}

	// No data on disk — need to backfill (the start < end check above already
	// handles the future-listing-date case).
	if lastTS.IsZero() {
		return false
	}

	if isDailyOrLonger(dataType) {
		return isDailyUpToDate(lastTS, end, dataType, marketTZ)
	}

	// For intraday bars, use the timeframe duration as tolerance.
	// For tick data, use the fixed tick tolerance.
	tolerance := tickTolerance
	if dataType != "trades" && dataType != "quotes" {
		if tf := utils.TimeframeFromString(dataType); tf != nil {
			tolerance = tf.Duration
		}
	}

	return !lastTS.Before(end.Add(-tolerance))
}

// isDailyUpToDate checks whether the last written bar falls in the same
// calendar period as end, using the market timezone for date comparisons.
//
// For 1D bars, "same period" means same calendar date.
// For 1W bars, same ISO week. For 1M, same month. For 1Y, same year.
func isDailyUpToDate(lastTS, end time.Time, dataType string, marketTZ *time.Location) bool {
	cd, err := utils.CandleDurationFromString(dataType)
	if err != nil {
		// Unknown timeframe — fall back to not skipping so we don't miss data.
		return false
	}

	// Both times must be in the market timezone so that date extraction
	// (Year/Month/Day, ISOWeek) is correct. A daily bar stored as
	// 2026-04-02T04:00:00Z is midnight ET — same date as an April 2 close.
	// But 2026-04-02T03:00:00Z is 11 PM ET on April 1 — a different date.
	lastLocal := lastTS.In(marketTZ)
	endLocal := end.In(marketTZ)

	return cd.Truncate(lastLocal).Equal(cd.Truncate(endLocal))
}

// flatFileAvailableThrough returns the latest market date whose flat file is
// expected to be published on S3. The data provider publishes flat files for
// date D at approximately 11 AM ET on D+1; we use flatFileAvailableHourET
// (noon) as a safety buffer.
//
// Before noon ET: files available through the day before yesterday.
// After noon ET: files available through yesterday.
func flatFileAvailableThrough(now time.Time) time.Time {
	et := now.In(calendar.Nasdaq.Tz())
	// After the cutoff hour, yesterday's file should be available.
	// Before the cutoff, only day-before-yesterday is safe.
	daysBack := 2
	if et.Hour() >= flatFileAvailableHourET {
		daysBack = 1
	}
	d := et.AddDate(0, 0, -daysBack)
	return time.Date(d.Year(), d.Month(), d.Day(), 0, 0, 0, 0, time.UTC)
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

func main() {}
