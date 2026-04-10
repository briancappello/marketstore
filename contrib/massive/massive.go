package main

import (
	"context"
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
	jsonit "github.com/json-iterator/go"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/flatfiles"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/rest"
	"github.com/alpacahq/marketstore/v4/contrib/massive/handlers"
	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
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
)

// wsDataTypeToTopic maps our config data type names to WebSocket topics.
var wsDataTypeToTopic = map[string]ws.Topic{
	"1Min":   ws.StocksMinAggs,
	"1Sec":   ws.StocksSecAggs,
	"trades": ws.StocksTrades,
	"quotes": ws.StocksQuotes,
}

// Use jsoniter because it supports marshal/unmarshal of map[interface{}]interface{} type.
// When the config file contains nested structures like query_start: {1Min: "2024-01-01"},
// the standard "encoding/json" library cannot marshal the structure because the config
// is parsed from a YAML file to map[string]interface{}, and nested maps become
// map[interface{}]interface{} which encoding/json doesn't support.
var jsonAPI = jsonit.ConfigCompatibleWithStandardLibrary

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
	data, _ := jsonAPI.Marshal(conf)
	config := massiveconfig.FetcherConfig{}
	if err := jsonAPI.Unmarshal(data, &config); err != nil {
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

// startStreaming connects each configured data type to the Massive WebSocket API
// sequentially (so no two connections are dialing at the same time), then launches
// a goroutine per data type to run the message loop.
//
// Sequential connection ensures the server has fully registered each connection's
// subscription before the next one dials. This prevents transient "bare" connections
// that count against the account's connection limit.
func (mf *MassiveFetcher) startStreaming() {
	feed := ws.Feed(ws.RealTime)
	if mf.config.WSServer != "" {
		feed = ws.Feed(mf.config.WSServer)
	}

	// Map data types to their handlers.
	dataTypeHandlers := map[string]func([]byte){
		"1Min":   handlers.MakeBarsHandler("1Min"),
		"1Sec":   handlers.MakeBarsHandler("1Sec"),
		"trades": handlers.TradeHandler,
		"quotes": handlers.QuoteHandler,
	}

	tickers := mf.config.Symbols
	if len(tickers) == 0 {
		tickers = []string{"*"}
	}

	for dataType := range mf.wsDataTypes {
		topic, ok := wsDataTypeToTopic[dataType]
		if !ok {
			log.Error("[massive] unknown data type %q, skipping", dataType)
			continue
		}

		handler := dataTypeHandlers[dataType]

		// Connect sequentially: dial, auth, subscribe, and wait for confirmation
		// before moving on to the next data type.
		client, err := mf.wsConnect(dataType, topic, feed, tickers)
		if err != nil {
			log.Error("[massive] %s: initial connection failed: %v", dataType, err)
			// Launch the retry goroutine which will sleep until the next
			// pre-market open and try again.
			mf.wg.Add(1)
			go mf.streamWithRestart(dataType, topic, handler, feed, tickers)
			continue
		}

		log.Info("[massive] streaming %s (%d symbols)", dataType, len(tickers))

		mf.wg.Add(1)
		go mf.streamWithRestart(dataType, topic, handler, feed, tickers, withClient(client))
	}
}

// streamOption allows startStreaming to pass an already-connected client to
// streamWithRestart for the initial run, skipping the first connect.
type streamOption func(*streamState)
type streamState struct {
	client      *ws.Client
	isReconnect bool
}

func withClient(c *ws.Client) streamOption {
	return func(s *streamState) {
		s.client = c
	}
}

// streamWithRestart is the outer retry loop for a single data type's WebSocket
// stream. On any fatal error (including connection-limit rejections), it sleeps
// until the next pre-market open (3:58 AM ET) and retries. On a successful
// reconnect it also triggers a backfill to cover any data gap.
//
// If called with withClient(), uses the pre-connected client for the first
// iteration (skipping the connect step, since startStreaming already did it).
func (mf *MassiveFetcher) streamWithRestart(
	dataType string,
	topic ws.Topic,
	handler func([]byte),
	feed ws.Feed,
	tickers []string,
	opts ...streamOption,
) {
	defer mf.wg.Done()

	state := streamState{}
	for _, opt := range opts {
		opt(&state)
	}

	for {
		var err error
		if state.client != nil {
			// Use the pre-connected client (first iteration from startStreaming).
			err = mf.stream(dataType, handler, state.client, state.isReconnect)
			state.client = nil // consumed; next iteration connects fresh
		} else {
			// Connect and stream.
			err = mf.connectAndStream(dataType, topic, handler, feed, tickers, state.isReconnect)
		}

		if err == nil {
			// Clean shutdown via context cancellation.
			return
		}

		// Log differently for connection-limit vs other fatal errors.
		if errors.Is(err, ws.ErrConnectionLimit) {
			log.Error("[massive] %s: connection limit reached, scheduling retry at next pre-market open", dataType)
		} else {
			log.Error("[massive] %s: stream failed (%v), scheduling retry at next pre-market open", dataType, err)
		}

		// Sleep until 3:58 AM ET on the next trading day.
		wakeTime := nextPreMarketOpen(time.Now())
		log.Info("[massive] %s: next reconnect attempt at %s", dataType, wakeTime.Format(time.RFC3339))

		select {
		case <-mf.ctx.Done():
			return
		case <-time.After(time.Until(wakeTime)):
			log.Info("[massive] %s: waking up for scheduled reconnect", dataType)
		}

		state.isReconnect = true
	}
}

// wsConnect creates a new WebSocket client, registers the subscription, and
// performs the full synchronous handshake (dial → auth → subscribe → confirmation).
// Returns the connected client ready for streaming.
func (mf *MassiveFetcher) wsConnect(
	dataType string,
	topic ws.Topic,
	feed ws.Feed,
	tickers []string,
) (*ws.Client, error) {
	client := ws.New(mf.config.APIKey, feed)
	client.Subscribe(topic, tickers...)

	if err := client.Connect(); err != nil {
		return nil, fmt.Errorf("connect %s: %w", dataType, err)
	}
	return client, nil
}

// connectAndStream creates a WebSocket client, connects, and streams messages
// until a fatal error or context cancellation. Used by streamWithRestart for
// reconnection attempts (the initial connection is done by startStreaming).
func (mf *MassiveFetcher) connectAndStream(
	dataType string,
	topic ws.Topic,
	handler func([]byte),
	feed ws.Feed,
	tickers []string,
	isReconnect bool,
) error {
	client, err := mf.wsConnect(dataType, topic, feed, tickers)
	if err != nil {
		return err
	}
	log.Info("[massive] streaming %s (%d symbols)", dataType, len(tickers))
	return mf.stream(dataType, handler, client, isReconnect)
}

// stream runs the message loop for an already-connected client until the
// connection drops or the context is cancelled.
//
// It returns:
//   - nil on clean shutdown (context cancelled)
//   - ws.ErrConnectionLimit if the server rejected us for exceeding the connection limit
//   - ws.ErrAuthFailed if the API key is invalid
//   - a wrapped error for any other fatal failure
//
// When isReconnect is true, a background backfill is triggered to fill any data
// gap from the time the stream was down.
func (mf *MassiveFetcher) stream(
	dataType string,
	handler func([]byte),
	client *ws.Client,
	isReconnect bool,
) error {
	defer client.Close()

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
				log.Error("[massive] %s error during shutdown: %v", dataType, err)
			default:
			}
			log.Info("[massive] stopping stream for %s", dataType)
			return nil
		case <-client.Done():
			// Connection lost. Retrieve the error.
			select {
			case err := <-client.Err():
				return fmt.Errorf("fatal error on %s: %w", dataType, err)
			default:
				return fmt.Errorf("connection closed unexpectedly for %s", dataType)
			}
		case msg := <-client.Output():
			handler(msg)
		}
	}
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

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxConnsPerHost,
			MaxConnsPerHost:     maxConnsPerHost,
		},
		Timeout: backfillHTTPTimeout,
	}

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

	// --- Phase 1: Flat file backfill for 1D and 1Min bars ---
	flatFileDone := mf.runFlatFileBackfill(parallelism, endRegular, endExtended)

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
			for key, startDateStr := range mf.config.QueryStart {
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
func (mf *MassiveFetcher) runFlatFileBackfill(parallelism int, endRegular, endExtended time.Time) map[string]bool {
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

	for key, startDateStr := range mf.config.QueryStart {
		select {
		case <-mf.ctx.Done():
			return done
		default:
		}

		s3DataType, ok := flatfiles.DataTypes[key]
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

				_, _, backfillErr := flatfiles.BackfillDates(mf.ctx, s3Client, w, symbolSet, key, s3DataType, forwardDates, flatfiles.BackfillConfig{
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

					_, _, backfillErr := flatfiles.BackfillDates(mf.ctx, s3Client, w, symbolSet, key, s3DataType, backwardDates, flatfiles.BackfillConfig{
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
