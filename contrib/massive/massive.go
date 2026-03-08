package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	jsonit "github.com/json-iterator/go"
	massivews "github.com/massive-com/client-go/v3/websocket"

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
	dateFormat = "2006-01-02"

	defaultBackfillBatchSize = 50000
	backfillHTTPTimeout      = 30 * time.Second
	maxConnsPerHost          = 100
)

// wsDataTypeToTopic maps our config data type names to upstream WebSocket topics.
var wsDataTypeToTopic = map[string]massivews.Topic{
	"1Min":   massivews.StocksMinAggs,
	"1Sec":   massivews.StocksSecAggs,
	"trades": massivews.StocksTrades,
	"quotes": massivews.StocksQuotes,
}

// wsLogger adapts the MarketStore log package to the upstream client's Logger interface.
type wsLogger struct{}

func (l *wsLogger) Debugf(template string, args ...any) { log.Debug("[massive/ws] "+template, args...) }
func (l *wsLogger) Infof(template string, args ...any)  { log.Info("[massive/ws] "+template, args...) }
func (l *wsLogger) Errorf(template string, args ...any) { log.Error("[massive/ws] "+template, args...) }

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
// Each data type gets its own upstream massivews.Client instance with automatic
// reconnection, exponential backoff, and auth failure detection.
func (mf *MassiveFetcher) startStreaming() {
	feed := massivews.Feed(massivews.RealTime)
	if mf.config.WSServer != "" {
		feed = massivews.Feed(mf.config.WSServer)
	}

	// Map data types to their handlers.
	dataTypeHandlers := map[string]func([]byte){
		"1Min":   handlers.MakeBarsHandler("1Min"),
		"1Sec":   handlers.MakeBarsHandler("1Sec"),
		"trades": handlers.TradeHandler,
		"quotes": handlers.QuoteHandler,
	}

	for dataType := range mf.wsDataTypes {
		topic, ok := wsDataTypeToTopic[dataType]
		if !ok {
			log.Error("[massive] unknown data type %q, skipping", dataType)
			continue
		}

		handler := dataTypeHandlers[dataType]

		client, err := massivews.New(massivews.Config{
			APIKey:  mf.config.APIKey,
			Feed:    feed,
			Market:  massivews.Stocks,
			RawData: true,
			Log:     &wsLogger{},
			ReconnectCallback: func(err error) {
				if err != nil {
					log.Warn("[massive] reconnecting %s: %v", dataType, err)
				} else {
					log.Info("[massive] reconnected %s", dataType)
				}
			},
		})
		if err != nil {
			log.Error("[massive] failed to create WebSocket client for %s: %v", dataType, err)
			continue
		}

		// Subscribe to the topic with configured symbols.
		tickers := mf.config.Symbols
		if len(tickers) == 0 {
			tickers = []string{"*"}
		}
		if err := client.Subscribe(topic, tickers...); err != nil {
			log.Error("[massive] failed to subscribe %s: %v", dataType, err)
			client.Close()
			continue
		}

		if err := client.Connect(); err != nil {
			log.Error("[massive] failed to connect %s: %v", dataType, err)
			client.Close()
			continue
		}

		log.Info("[massive] streaming %s (%d symbols)", dataType, len(tickers))

		mf.wg.Add(1)
		go func(dt string, c *massivews.Client, h func([]byte)) {
			defer mf.wg.Done()
			defer c.Close()

			for {
				select {
				case <-mf.ctx.Done():
					log.Info("[massive] stopping stream for %s", dt)
					return
				case err := <-c.Error():
					log.Error("[massive] fatal error on %s: %v", dt, err)
					return
				case msg, ok := <-c.Output():
					if !ok {
						log.Info("[massive] output channel closed for %s", dt)
						return
					}
					// In RawData mode, output is json.RawMessage (individual messages).
					if raw, ok := msg.(json.RawMessage); ok {
						h([]byte(raw))
					}
				}
			}
		}(dataType, client, handler)
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
				if err := backfill.Trades(mf.ctx, client, symbol, decision.start, endExtended, batchSize, writerWP, nil); err != nil {
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
				if err := backfill.Quotes(mf.ctx, client, symbol, decision.start, endExtended, batchSize, writerWP, nil); err != nil {
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
				if err := backfill.Bars(mf.ctx, client, symbol, tf, decision.start, end, batchSize, adjusted, writerWP, nil); err != nil {
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

func main() {}
