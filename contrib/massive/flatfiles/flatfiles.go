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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gobwas/glob"
	jsoniter "github.com/json-iterator/go"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
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
	defaultMaxConnsPerHost     = 100
	defaultMaxIdleConnsPerHost = 100
)

// flatFileDataTypes maps the query_start keys for bars to the S3 flat file data type names.
var flatFileDataTypes = map[string]string{
	"1D":   "day_aggs_v1",
	"1Min": "minute_aggs_v1",
}

// fromFlags is a custom flag type that collects multiple -from freq=date pairs.
// This matches the REST backfiller's interface exactly.
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
	// Support both "YYYY-MM-DD" (applies to all types) and "key=YYYY-MM-DD".
	parts := strings.SplitN(value, "=", 2)
	if len(parts) == 1 {
		// Plain date: applies to both 1D and 1Min.
		dateStr := strings.TrimSpace(parts[0])
		if _, err := time.Parse(dateFormat, dateStr); err != nil {
			return fmt.Errorf("invalid date %q: %w", dateStr, err)
		}
		(*f)["1D"] = dateStr
		(*f)["1Min"] = dateStr
		return nil
	}

	key := strings.TrimSpace(parts[0])
	dateStr := strings.TrimSpace(parts[1])

	if _, err := time.Parse(dateFormat, dateStr); err != nil {
		return fmt.Errorf("invalid date %q for %s: %w", dateStr, key, err)
	}

	if _, ok := flatFileDataTypes[key]; !ok {
		return fmt.Errorf("unsupported data type %q: flat files only support 1D and 1Min", key)
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
	configFilePath string
	grpcAddress    string
	noRPC          bool
)

func parseFlags() {
	flag.StringVar(&dir, "dir", "", "mktsdb directory (overrides mkts.yml)")
	flag.Var(&fromDates, "from",
		"start date for backfill. Either a plain date (applies to both 1D and 1Min) "+
			"or key=date pairs (e.g., -from 1D=2020-01-01 -from 1Min=2024-01-01). "+
			"If not specified, uses query_start from config file.")
	flag.StringVar(&to, "to", time.Now().AddDate(0, 0, -1).Format(dateFormat),
		"backfill to date (YYYY-MM-DD, inclusive, default: yesterday)")
	flag.StringVar(&symbols, "symbols", "",
		"glob pattern of symbols to backfill (* = all). If not specified, uses symbols from config file.")
	flag.IntVar(&parallelism, "parallelism", runtime.NumCPU(),
		"number of parallel S3 downloads (default: NumCPU)")
	flag.StringVar(&configFilePath, "config", "mkts.yml",
		"path to the mkts.yml config file")
	flag.StringVar(&grpcAddress, "grpc", "localhost:5995",
		"gRPC server address for writing data (default: localhost:5995)")
	flag.BoolVar(&noRPC, "no-rpc", false,
		"write directly to the filesystem instead of via gRPC to a running server")

	flag.Parse()
}

func main() {
	parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info("[flatfiles] received signal %v, initiating shutdown...", sig)
		cancel()
		sig = <-sigChan
		log.Info("[flatfiles] received second signal %v, forcing exit", sig)
		os.Exit(1)
	}()

	// Initialize writer based on mode.
	var w backfill.Writer
	var instanceMeta *executor.InstanceMetadata
	var massiveConfig *massiveconfig.FetcherConfig

	if noRPC {
		log.Info("[flatfiles] using direct disk mode (--no-rpc)")
		instanceMeta, massiveConfig = initWriter()
		w = &backfill.DirectWriter{}
	} else {
		log.Info("[flatfiles] using gRPC mode, connecting to %s", grpcAddress)
		rpcWriter, err := backfill.NewRPCWriter(ctx, grpcAddress)
		if err != nil {
			log.Error("[flatfiles] failed to connect to gRPC server: %v", err)
			os.Exit(1)
		}
		defer rpcWriter.Close()
		w = rpcWriter
		massiveConfig = loadConfigOnly()
	}

	// Apply config defaults for flags not explicitly set.
	applyConfigDefaults(massiveConfig)

	// Resolve S3 credentials.
	s3AccessKey := resolveEnv(massiveConfig.S3AccessKey, "MASSIVE_S3_ACCESS_KEY")
	s3SecretKey := resolveEnv(massiveConfig.S3SecretKey, "MASSIVE_S3_SECRET_KEY")
	if s3AccessKey == "" || s3SecretKey == "" {
		log.Error("[flatfiles] S3 credentials required (via config s3_access_key/s3_secret_key or MASSIVE_S3_ACCESS_KEY/MASSIVE_S3_SECRET_KEY env)")
		os.Exit(1)
	}

	s3Client, err := NewS3Client(s3AccessKey, s3SecretKey)
	if err != nil {
		log.Error("[flatfiles] failed to create S3 client: %v", err)
		os.Exit(1)
	}

	if len(fromDates) == 0 {
		log.Error("[flatfiles] no backfill dates specified (use -from flag or query_start in config)")
		os.Exit(1)
	}

	// Resolve symbols.
	apiKey := resolveEnv(massiveConfig.APIKey, "MASSIVE_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("POLYGON_API_KEY")
	}
	symbolInfos := resolveSymbolInfos(apiKey, massiveConfig)
	if len(symbolInfos) == 0 {
		log.Error("[flatfiles] no symbols resolved")
		os.Exit(1)
	}

	// Build a symbol set for filtering CSV rows.
	symbolSet := make(map[string]bool, len(symbolInfos))
	for _, si := range symbolInfos {
		symbolSet[si.Symbol] = true
	}

	log.Info("[flatfiles] selected %d symbols", len(symbolSet))

	endDate, err := time.Parse(dateFormat, to)
	if err != nil {
		log.Error("[flatfiles] failed to parse -to: %v", err)
		os.Exit(1)
	}

	startTime := time.Now()

	// Process each data type (1D, 1Min).
	for key, startDateStr := range fromDates {
		select {
		case <-ctx.Done():
			log.Info("[flatfiles] backfill cancelled")
			if instanceMeta != nil {
				instanceMeta.WALFile.Shutdown()
			}
			os.Exit(0)
		default:
		}

		s3DataType, ok := flatFileDataTypes[key]
		if !ok {
			log.Warn("[flatfiles] unsupported data type %q, skipping", key)
			continue
		}

		configStart, err := time.Parse(dateFormat, startDateStr)
		if err != nil {
			log.Warn("[flatfiles] invalid date %q for %s, skipping", startDateStr, key)
			continue
		}

		runFlatFileBackfill(ctx, s3Client, w, symbolSet, key, s3DataType, configStart, endDate, parallelism)
	}

	if instanceMeta != nil {
		instanceMeta.WALFile.Shutdown()
	}
	log.Info("[flatfiles] backfill complete in %s", time.Since(startTime))
}

// runFlatFileBackfill processes all dates in [startDate, endDate] for a single
// data type, downloading flat files from S3 and writing bars to MarketStore.
func runFlatFileBackfill(
	ctx context.Context,
	s3Client *S3Client,
	w backfill.Writer,
	symbolSet map[string]bool,
	timeframe string, // "1D" or "1Min"
	s3DataType string, // "day_aggs_v1" or "minute_aggs_v1"
	startDate, endDate time.Time,
	parallelism int,
) {
	// Build the list of market dates to process.
	var dates []time.Time
	for d := startDate; !d.After(endDate); d = d.AddDate(0, 0, 1) {
		if calendar.Nasdaq.IsMarketDay(d) {
			dates = append(dates, d)
		}
	}

	if len(dates) == 0 {
		log.Info("[flatfiles] no market days in range for %s", timeframe)
		return
	}

	log.Info("[flatfiles] backfilling %s: %d market days from %s to %s for %d symbols",
		timeframe, len(dates), startDate.Format(dateFormat), endDate.Format(dateFormat), len(symbolSet))

	downloadWP := worker.NewWorkerPool(ctx, parallelism)
	writerWP := worker.NewWorkerPool(ctx, 1)

	var totalRows int64
	var totalSymbols int64
	var processedDates int64

	for _, date := range dates {
		select {
		case <-ctx.Done():
			downloadWP.CloseAndWait()
			writerWP.CloseAndWait()
			return
		default:
		}

		currentDate := date
		downloadWP.Do(func() {
			reader, err := s3Client.Download(ctx, s3DataType, currentDate)
			if err != nil {
				// File may not exist for this date (e.g., data not yet available).
				if ctx.Err() == nil {
					log.Warn("[flatfiles] %s: failed to download %s: %v", currentDate.Format(dateFormat), timeframe, err)
				}
				return
			}
			defer reader.Close()

			csm, stats, err := ParseAndWrite(reader, symbolSet, timeframe, currentDate)
			if err != nil {
				log.Warn("[flatfiles] %s: failed to parse %s: %v", currentDate.Format(dateFormat), timeframe, err)
				return
			}

			if len(csm) > 0 {
				writerWP.Do(func() {
					if err := w.WriteCSM(csm, false); err != nil {
						log.Warn("[flatfiles] %s: failed to write %s: %v", currentDate.Format(dateFormat), timeframe, err)
					}
				})
			}

			n := atomic.AddInt64(&processedDates, 1)
			atomic.AddInt64(&totalRows, int64(stats.RowsMatched))
			atomic.AddInt64(&totalSymbols, int64(stats.SymbolCount))

			if n%50 == 0 || n == int64(len(dates)) {
				log.Info("[flatfiles] %s: processed %d/%d dates (%d rows, %d symbol-writes)",
					timeframe, n, len(dates),
					atomic.LoadInt64(&totalRows), atomic.LoadInt64(&totalSymbols))
			}
		})
	}

	downloadWP.CloseAndWait()
	writerWP.CloseAndWait()

	log.Info("[flatfiles] %s complete: %d dates, %d rows, %d symbol-writes",
		timeframe, atomic.LoadInt64(&processedDates), atomic.LoadInt64(&totalRows), atomic.LoadInt64(&totalSymbols))
}

// applyConfigDefaults applies defaults from the massive bgworker config
// for any flags that weren't explicitly set.
func applyConfigDefaults(cfg *massiveconfig.FetcherConfig) {
	if cfg == nil {
		return
	}

	// Symbols from config (join as glob pattern).
	if symbols == "" && cfg.SymbolsDSN == "" && len(cfg.Symbols) > 0 {
		if len(cfg.Symbols) == 1 {
			symbols = cfg.Symbols[0]
		} else {
			symbols = "{" + strings.Join(cfg.Symbols, ",") + "}"
		}
	}

	// query_start from config provides the backfill dates.
	// Only use 1D and 1Min keys (flat files only support these).
	if len(fromDates) == 0 && len(cfg.QueryStart) > 0 {
		for key, dateStr := range cfg.QueryStart {
			if _, ok := flatFileDataTypes[key]; !ok {
				continue // skip non-bar types like "trades", "quotes"
			}
			if _, err := time.Parse(dateFormat, dateStr); err != nil {
				log.Warn("[flatfiles] invalid query_start date %q for %s in config, skipping", dateStr, key)
				continue
			}
			fromDates[key] = dateStr
		}
		if len(fromDates) > 0 {
			log.Info("[flatfiles] using query_start from config: %v", fromDates)
		}
	}
}

// resolveEnv returns configVal if non-empty, otherwise the value of the named
// environment variable.
func resolveEnv(configVal, envVar string) string {
	if configVal != "" {
		return configVal
	}
	return os.Getenv(envVar)
}

// normalizeSymbolPattern converts a comma-separated list of symbols into a
// brace glob pattern. If the input already looks like a glob, it's returned as-is.
func normalizeSymbolPattern(pattern string) string {
	if strings.ContainsAny(pattern, "*?{[") {
		return pattern
	}
	if strings.Contains(pattern, ",") {
		parts := strings.Split(pattern, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	return pattern
}

// resolveSymbolInfos resolves symbols from CLI flag, database, or config.
// For plain symbol names (no wildcards), symbols are used directly without
// API validation -- the flat file contains all tickers, so unknown symbols
// simply produce zero matched rows.
func resolveSymbolInfos(apiKey string, cfg *massiveconfig.FetcherConfig) []massiveconfig.SymbolInfo {
	// Priority 1: CLI -symbols flag.
	if symbols != "" {
		return resolveSymbolsFromInput(apiKey, symbols)
	}

	// Priority 2: Database query.
	if cfg != nil && cfg.SymbolsDSN != "" {
		if cfg.SymbolsQuery == "" {
			log.Error("[flatfiles] symbols_query is required when symbols_dsn is set")
			os.Exit(1)
		}
		infos, err := massiveconfig.FetchSymbolsFromDB(cfg.SymbolsDSN, cfg.SymbolsQuery)
		if err != nil {
			log.Error("[flatfiles] failed to fetch symbols from database: %v", err)
			os.Exit(1)
		}
		log.Info("[flatfiles] loaded %d symbols from database", len(infos))
		return infos
	}

	// Priority 3: Config symbols list.
	if cfg != nil && len(cfg.Symbols) > 0 {
		return resolveSymbolsFromInput(apiKey, strings.Join(cfg.Symbols, ","))
	}

	log.Error("[flatfiles] no symbols configured")
	os.Exit(1)
	return nil
}

// resolveSymbolsFromInput resolves a symbol string which may be:
//   - A plain comma-separated list: "AAPL,MSFT,SPY" -> used directly
//   - A single symbol: "AAPL" -> used directly
//   - A glob pattern: "*" or "AA*" -> resolved via the Massive ticker API
func resolveSymbolsFromInput(apiKey, input string) []massiveconfig.SymbolInfo {
	// If the input contains no wildcard characters, use symbols directly
	// without calling the API. This avoids needing an API key for simple lists.
	if !strings.ContainsAny(input, "*?{[") {
		parts := strings.Split(input, ",")
		result := make([]massiveconfig.SymbolInfo, 0, len(parts))
		for _, s := range parts {
			s = strings.TrimSpace(s)
			if s != "" {
				result = append(result, massiveconfig.SymbolInfo{Symbol: s})
			}
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Symbol < result[j].Symbol
		})
		return result
	}

	// Wildcard pattern: resolve via API.
	return resolveSymbolsFromPattern(apiKey, input)
}

// resolveSymbolsFromPattern resolves a glob pattern against the Massive ticker API.
func resolveSymbolsFromPattern(apiKey, pattern string) []massiveconfig.SymbolInfo {
	if apiKey == "" {
		log.Error("[flatfiles] API key required to resolve symbol patterns (via config api_key, MASSIVE_API_KEY, or POLYGON_API_KEY env)")
		os.Exit(1)
	}

	api.SetAPIKey(apiKey)

	log.Info("[flatfiles] listing tickers for pattern: %s", pattern)
	pattern = normalizeSymbolPattern(pattern)
	g := glob.MustCompile(pattern)

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
			MaxConnsPerHost:     defaultMaxConnsPerHost,
		},
		Timeout: 30 * time.Second,
	}

	tickers, err := api.ListTickers(client)
	if err != nil {
		log.Error("[flatfiles] failed to list tickers: %v", err)
		os.Exit(1)
	}

	seen := make(map[string]struct{})
	var result []massiveconfig.SymbolInfo
	for _, t := range tickers {
		if t.Ticker == "" {
			continue
		}
		if _, dup := seen[t.Ticker]; dup {
			continue
		}
		if g.Match(t.Ticker) {
			result = append(result, massiveconfig.SymbolInfo{Symbol: t.Ticker})
			seen[t.Ticker] = struct{}{}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Symbol < result[j].Symbol
	})
	return result
}

// loadConfigOnly loads the config file to extract massive bgworker settings
// without initializing the disk writer. Used in RPC mode.
func loadConfigOnly() *massiveconfig.FetcherConfig {
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Error("[flatfiles] failed to read config: %v", err)
		os.Exit(1)
	}

	config, err := utils.ParseConfig(data)
	if err != nil {
		log.Error("[flatfiles] failed to parse config: %v", err)
		os.Exit(1)
	}

	return findMassiveBgWorkerConfig(config)
}

func initWriter() (*executor.InstanceMetadata, *massiveconfig.FetcherConfig) {
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		log.Error("[flatfiles] failed to read config: %v", err)
		os.Exit(1)
	}

	config, err := utils.ParseConfig(data)
	if err != nil {
		log.Error("[flatfiles] failed to parse config: %v", err)
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
	cfg.BackgroundSync = false
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

	massiveConfig := findMassiveBgWorkerConfig(config)

	return executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile()), massiveConfig
}

// findMassiveBgWorkerConfig searches for a massive.so bgworker in the config
// and parses its config section into a FetcherConfig.
func findMassiveBgWorkerConfig(config *utils.MktsConfig) *massiveconfig.FetcherConfig {
	for _, bg := range config.BgWorkers {
		if bg.Module == "massive.so" {
			data, err := json.Marshal(bg.Config)
			if err != nil {
				log.Warn("[flatfiles] failed to marshal bgworker config: %v", err)
				return nil
			}

			var fetcherConfig massiveconfig.FetcherConfig
			if err := json.Unmarshal(data, &fetcherConfig); err != nil {
				log.Warn("[flatfiles] failed to parse bgworker config: %v", err)
				return nil
			}

			return &fetcherConfig
		}
	}
	return nil
}
