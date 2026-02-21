package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gobwas/glob"
	jsoniter "github.com/json-iterator/go"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// json iter supports marshal/unmarshal of map[interface{}]interface{} type
// which is produced by gopkg.in/yaml.v2 for nested maps.
var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	dateFormat                 = "2006-01-02"
	defaultBatchSize           = 50000
	defaultMaxConnsPerHost     = 100
	defaultMaxIdleConnsPerHost = 100
)

// fromFlags is a custom flag type that collects multiple -from freq=date pairs.
type fromFlags map[string]string

func (f *fromFlags) String() string {
	if f == nil {
		return ""
	}
	var parts []string
	for k, v := range *f {
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, ",")
}

func (f *fromFlags) Set(value string) error {
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid format %q, expected freq=date (e.g., 1Min=2024-01-01)", value)
	}
	key := strings.TrimSpace(parts[0])
	dateStr := strings.TrimSpace(parts[1])

	// Validate the date format
	if _, err := time.Parse(dateFormat, dateStr); err != nil {
		return fmt.Errorf("invalid date %q for %s: %w", dateStr, key, err)
	}

	(*f)[key] = dateStr
	return nil
}

var (
	dir            string
	fromDates      = make(fromFlags)
	to             string
	symbols        string
	parallelism    int
	apiKey         string
	baseURL        string
	batchSize      int
	adjusted       bool
	configFilePath string

	// symbolInfos holds resolved symbols with optional listing dates.
	// Populated either from database (symbols_dsn) or API (glob pattern).
	symbolInfos []massiveconfig.SymbolInfo
)

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	flag.StringVar(&dir, "dir", "", "mktsdb directory (overrides mkts.yml)")
	flag.Var(&fromDates, "from",
		"start date per frequency as key=value (e.g., -from 1Min=2024-01-01 -from 1D=2020-01-01). "+
			"Use 'trades' and 'quotes' as keys for tick data. "+
			"If not specified, uses query_start from config file.")
	flag.StringVar(&to, "to", time.Now().Format(dateFormat),
		"backfill to date (YYYY-MM-DD) [not included]")
	flag.StringVar(&symbols, "symbols", "",
		"glob pattern of symbols to backfill (* = all). If not specified, uses symbols from config file.")
	flag.IntVar(&parallelism, "parallelism", runtime.NumCPU(),
		"number of parallel API workers (default NumCPU)")
	flag.IntVar(&batchSize, "batchSize", 0,
		"pagination size for API requests (default from config or 50000)")
	flag.StringVar(&apiKey, "apiKey", "", "Massive API key (from flag, POLYGON_API_KEY, MASSIVE_API_KEY env, or config)")
	flag.StringVar(&baseURL, "baseURL", "",
		"override Massive API base URL (default from config or https://api.massive.com)")
	flag.BoolVar(&adjusted, "adjusted", true,
		"request split-adjusted price data")
	flag.StringVar(&configFilePath, "config", "/etc/mkts.yml",
		"path to the mkts.yml config file")

	flag.Parse()
}

func main() {
	// Load and parse config file first to get defaults.
	instanceMeta, massiveConfig := initWriter()

	// Apply config defaults for flags not explicitly set.
	applyConfigDefaults(massiveConfig)

	// If apiKey still not set, check environment variables.
	if apiKey == "" {
		apiKey = os.Getenv("POLYGON_API_KEY")
	}
	if apiKey == "" {
		apiKey = os.Getenv("MASSIVE_API_KEY")
	}
	if apiKey == "" {
		log.Error("[massive] apiKey is required (via -apiKey flag, POLYGON_API_KEY, MASSIVE_API_KEY env, or config)")
		os.Exit(1)
	}

	if len(fromDates) == 0 {
		log.Error("[massive] no backfill dates specified (use -from flags or query_start in config)")
		os.Exit(1)
	}

	api.SetAPIKey(apiKey)
	if baseURL != "" {
		api.SetBaseURL(baseURL)
	}

	end, err := time.Parse(dateFormat, to)
	if err != nil {
		log.Error("[massive] failed to parse -to: %v", err)
		os.Exit(1)
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
			MaxConnsPerHost:     defaultMaxConnsPerHost,
		},
		Timeout: 30 * time.Second,
	}

	// Resolve symbols: database (with listing dates) or API (glob pattern).
	symbolInfos = resolveSymbolInfos(client, massiveConfig)
	if len(symbolInfos) == 0 {
		log.Error("[massive] no symbols resolved")
		os.Exit(1)
	}
	withDates := 0
	for _, info := range symbolInfos {
		if info.ListingDate != nil {
			withDates++
		}
	}
	if withDates > 0 {
		log.Info("[massive] selected %d symbols (%d with listing dates)", len(symbolInfos), withDates)
	} else {
		log.Info("[massive] selected %d symbols", len(symbolInfos))
	}

	// Set up context with signal handling for graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C and SIGTERM for graceful shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info("[massive] received signal %v, initiating shutdown...", sig)
		cancel()
	}()

	startTime := time.Now()

	// Process each key in fromDates. Keys are either timeframes (e.g., "1Min", "1D")
	// for bars, or "trades"/"quotes" for tick data.
	for key, startDateStr := range fromDates {
		// Check for cancellation between data types.
		select {
		case <-ctx.Done():
			log.Info("[massive] backfill cancelled")
			instanceMeta.WALFile.Shutdown()
			os.Exit(0)
		default:
		}

		configStart, err := time.Parse(dateFormat, startDateStr)
		if err != nil {
			log.Warn("[massive] invalid date %q for %s, skipping", startDateStr, key)
			continue
		}

		switch key {
		case "trades":
			runBackfill(ctx, "trades", symbolInfos, configStart, end, func(symInfo massiveconfig.SymbolInfo, effectiveStart time.Time, writerWP *worker.Pool) {
				if err := backfill.Trades(ctx, client, symInfo.Symbol, effectiveStart, end, batchSize, writerWP); err != nil {
					if err == context.Canceled {
						return
					}
					log.Warn("[massive] failed to backfill trades for %s: %v", symInfo.Symbol, err)
				}
			})
		case "quotes":
			runBackfill(ctx, "quotes", symbolInfos, configStart, end, func(symInfo massiveconfig.SymbolInfo, effectiveStart time.Time, writerWP *worker.Pool) {
				if err := backfill.Quotes(ctx, client, symInfo.Symbol, effectiveStart, end, batchSize, writerWP); err != nil {
					if err == context.Canceled {
						return
					}
					log.Warn("[massive] failed to backfill quotes for %s: %v", symInfo.Symbol, err)
				}
			})
		default:
			// Assume it's a bar timeframe (e.g., "1Min", "5Min", "1H", "1D").
			timeframe := key
			runBackfill(ctx, timeframe+" bars", symbolInfos, configStart, end, func(symInfo massiveconfig.SymbolInfo, effectiveStart time.Time, writerWP *worker.Pool) {
				if err := backfill.Bars(ctx, client, symInfo.Symbol, timeframe, effectiveStart, end, batchSize, adjusted, writerWP); err != nil {
					if err == context.Canceled {
						return
					}
					log.Warn("[massive] failed to backfill %s bars for %s: %v", timeframe, symInfo.Symbol, err)
				}
			})
		}
	}

	instanceMeta.WALFile.Shutdown()
	log.Info("[massive] backfill complete in %s", time.Since(startTime))
}

// applyConfigDefaults applies defaults from the massive bgworker config
// for any flags that weren't explicitly set.
func applyConfigDefaults(cfg *massiveconfig.FetcherConfig) {
	if cfg == nil {
		return
	}

	// API key from config (lowest priority, will be overridden by env vars).
	if apiKey == "" && cfg.APIKey != "" {
		apiKey = cfg.APIKey
	}

	// Base URL from config.
	if baseURL == "" && cfg.BaseURL != "" {
		baseURL = cfg.BaseURL
	}

	// Batch size from config.
	if batchSize == 0 && cfg.BackfillBatchSize > 0 {
		batchSize = cfg.BackfillBatchSize
	}
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}

	// Symbols from config (join as glob pattern).
	// Only apply if symbols_dsn is not set (database takes precedence).
	if symbols == "" && cfg.SymbolsDSN == "" && len(cfg.Symbols) > 0 {
		if len(cfg.Symbols) == 1 {
			symbols = cfg.Symbols[0]
		} else {
			// Multiple symbols: create a brace glob pattern like "{AAPL,MSFT,SPY}".
			symbols = "{" + strings.Join(cfg.Symbols, ",") + "}"
		}
	}
	// Don't default to "*" here - let resolveSymbolInfos handle the logic.

	// query_start from config provides the backfill dates.
	if len(fromDates) == 0 && len(cfg.QueryStart) > 0 {
		for key, dateStr := range cfg.QueryStart {
			// Validate date format before adding.
			if _, err := time.Parse(dateFormat, dateStr); err != nil {
				log.Warn("[massive] invalid query_start date %q for %s in config, skipping", dateStr, key)
				continue
			}
			fromDates[key] = dateStr
		}
		log.Info("[massive] using query_start from config: %v", fromDates)
	}
}

// normalizeSymbolPattern converts a comma-separated list of symbols (e.g., "AAPL,MSFT,SPY")
// into a brace glob pattern (e.g., "{AAPL,MSFT,SPY}") that the glob library understands.
// If the input is already a glob pattern (contains *, ?, {, or [), it's returned as-is.
func normalizeSymbolPattern(pattern string) string {
	// If it looks like a glob pattern already, return as-is.
	if strings.ContainsAny(pattern, "*?{[") {
		return pattern
	}

	// Check if it's a comma-separated list.
	if strings.Contains(pattern, ",") {
		parts := strings.Split(pattern, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return "{" + strings.Join(parts, ",") + "}"
	}

	// Single symbol, return as-is.
	return pattern
}

func runBackfill(ctx context.Context, name string, symbols []massiveconfig.SymbolInfo, configStart, end time.Time, fn func(massiveconfig.SymbolInfo, time.Time, *worker.Pool)) {
	log.Info("[massive] backfilling %s for %d symbols from %s to %s",
		name, len(symbols), configStart.Format(dateFormat), end.Format(dateFormat))
	apiWP := worker.NewWorkerPool(ctx, parallelism)
	writerWP := worker.NewWorkerPool(ctx, 1)

	for _, symInfo := range symbols {
		// Check for cancellation between symbols.
		select {
		case <-ctx.Done():
			apiWP.CloseAndWait()
			writerWP.CloseAndWait()
			return
		default:
		}

		// Compute effective start date for this symbol.
		effectiveStart := massiveconfig.EffectiveBackfillStart(configStart, symInfo.ListingDate)

		// Skip if effective start is after end (future listing date).
		if effectiveStart.After(end) {
			log.Info("[massive] %s listing date %s is in the future, skipping backfill",
				symInfo.Symbol, effectiveStart.Format(dateFormat))
			continue
		}

		currentSymInfo := symInfo
		currentStart := effectiveStart
		apiWP.Do(func() {
			fn(currentSymInfo, currentStart, writerWP)
		})
	}

	apiWP.CloseAndWait()
	writerWP.CloseAndWait()
}

// resolveSymbolInfos resolves symbols from database (if configured) or API (glob pattern).
// Database resolution returns symbols with optional listing dates.
// API resolution returns symbols without listing dates.
func resolveSymbolInfos(client *http.Client, cfg *massiveconfig.FetcherConfig) []massiveconfig.SymbolInfo {
	// Priority 1: Database query (with potential listing dates).
	if cfg != nil && cfg.SymbolsDSN != "" {
		if cfg.SymbolsQuery == "" {
			log.Error("[massive] symbols_query is required when symbols_dsn is set")
			os.Exit(1)
		}
		infos, err := massiveconfig.FetchSymbolsFromDB(cfg.SymbolsDSN, cfg.SymbolsQuery)
		if err != nil {
			log.Error("[massive] failed to fetch symbols from database: %v", err)
			os.Exit(1)
		}
		log.Info("[massive] loaded %d symbols from database", len(infos))
		return infos
	}

	// Priority 2: CLI -symbols flag or config symbols (via glob pattern against API).
	pattern := symbols
	if pattern == "" {
		pattern = "*"
	}
	symbolList := resolveSymbolsFromAPI(client, pattern)

	// Convert to SymbolInfo without listing dates.
	result := make([]massiveconfig.SymbolInfo, len(symbolList))
	for i, sym := range symbolList {
		result[i] = massiveconfig.SymbolInfo{Symbol: sym}
	}
	return result
}

func resolveSymbolsFromAPI(client *http.Client, pattern string) []string {
	log.Info("[massive] listing tickers for pattern: %s", pattern)
	pattern = normalizeSymbolPattern(pattern)
	g := glob.MustCompile(pattern)

	tickers, err := api.ListTickers(client)
	if err != nil {
		log.Error("[massive] failed to list tickers: %v", err)
		os.Exit(1)
	}

	seen := make(map[string]struct{})
	var result []string
	for _, t := range tickers {
		if t.Ticker == "" {
			continue
		}
		if _, dup := seen[t.Ticker]; dup {
			continue
		}
		if g.Match(t.Ticker) {
			result = append(result, t.Ticker)
			seen[t.Ticker] = struct{}{}
		}
	}

	sort.Strings(result)
	return result
}

func initWriter() (*executor.InstanceMetadata, *massiveconfig.FetcherConfig) {
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Error("[massive] failed to read config: %v", err)
		os.Exit(1)
	}

	config, err := utils.ParseConfig(data)
	if err != nil {
		log.Error("[massive] failed to parse config: %v", err)
		os.Exit(1)
	}
	utils.InstanceConfig = *config

	rootDir := config.RootDirectory
	if dir != "" {
		rootDir = dir
	}

	cfg := utils.NewDefaultConfig(rootDir)
	cfg.WALRotateInterval = config.WALRotateInterval
	cfg.WALBypass = true
	c := di.NewContainer(cfg)

	// Load ondiskagg triggers if configured.
	var tm []*trigger.Matcher
	for _, ts := range config.Triggers {
		if ts.Module == "ondiskagg.so" {
			tm = append(tm, trigger.NewTriggerMatcher(ts))
			break
		}
	}
	c.InjectTriggerMatchers(tm)

	// Extract massive bgworker config if present.
	massiveConfig := findMassiveBgWorkerConfig(config)

	return executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile()), massiveConfig
}

// findMassiveBgWorkerConfig searches for a massive.so bgworker in the config
// and parses its config section into a FetcherConfig.
func findMassiveBgWorkerConfig(config *utils.MktsConfig) *massiveconfig.FetcherConfig {
	for _, bg := range config.BgWorkers {
		if bg.Module == "massive.so" {
			// Marshal the config map to JSON, then unmarshal to FetcherConfig.
			// Using jsoniter to handle map[interface{}]interface{} from YAML.
			data, err := json.Marshal(bg.Config)
			if err != nil {
				log.Warn("[massive] failed to marshal bgworker config: %v", err)
				return nil
			}

			var fetcherConfig massiveconfig.FetcherConfig
			if err := json.Unmarshal(data, &fetcherConfig); err != nil {
				log.Warn("[massive] failed to parse bgworker config: %v", err)
				return nil
			}

			return &fetcherConfig
		}
	}
	return nil
}
