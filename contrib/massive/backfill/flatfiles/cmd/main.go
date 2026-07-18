package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers on default mux.
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gobwas/glob"
	jsoniter "github.com/json-iterator/go"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/flatfiles"
	"github.com/alpacahq/marketstore/v4/contrib/massive/mapping"
	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
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

// flatFileDataTypes maps the query_start keys for bars to their S3 flat file types.
// This is a reference to the canonical map in the flatfiles package.
var flatFileDataTypes = flatfiles.DataTypes

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
	// Support both "YYYY-MM-DD" (applies to equity types) and "key=YYYY-MM-DD".
	parts := strings.SplitN(value, "=", 2)
	if len(parts) == 1 {
		// Plain date: applies to equity types (1D and 1Min) only.
		// Index types must be specified explicitly (e.g., -from 1D-index=2020-01-01).
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
		return fmt.Errorf("unsupported data type %q: flat files support 1D, 1Min, 1D-index, trades, and quotes", key)
	}

	(*f)[key] = dateStr
	return nil
}

var (
	dir              string
	fromDates        = make(fromFlags)
	to               string
	symbols          string
	parallelism      int
	writeConcurrency int
	writeBuffer      int
	configFilePath   string
	grpcAddress      string
	noRPC            bool
	pprofAddr        string
	progressMode     string
	assumeYes        bool

	// writeConcurrencySet reports whether -write-concurrency was passed
	// explicitly on the command line.
	writeConcurrencySet bool

	// symbolsIsWildcard reports whether the resolved -symbols input was a glob
	// pattern (e.g. "*"), used to gate full-universe tick backfills.
	symbolsIsWildcard bool
)

func parseFlags() {
	flag.StringVar(&dir, "dir", "", "mktsdb directory (overrides mkts.yml)")
	flag.Var(&fromDates, "from",
		"start date for backfill. Either a plain date (applies to equity types 1D and 1Min) "+
			"or key=date pairs (e.g., -from 1D=2020-01-01 -from 1Min=2024-01-01 -from 1D-index=2020-01-01). "+
			"Index types must use key=date form. If not specified, uses query_start from config file.")
	flag.StringVar(&to, "to", time.Now().AddDate(0, 0, -1).Format(dateFormat),
		"backfill to date (YYYY-MM-DD, inclusive, default: yesterday)")
	flag.StringVar(&symbols, "symbols", "",
		"glob pattern of symbols to backfill (* = all). If not specified, uses symbols from config file.")
	flag.IntVar(&parallelism, "parallelism", runtime.NumCPU(),
		"number of parallel S3 downloads (default: NumCPU)")
	flag.IntVar(&writeConcurrency, "write-concurrency", 2,
		"number of concurrent writer goroutines (default: 2, or 4 with --no-rpc)")
	flag.IntVar(&writeBuffer, "write-buffer", 10,
		"buffered channel capacity between downloaders and writers (default: 10)")
	flag.StringVar(&configFilePath, "config", "mkts.yml",
		"path to the mkts.yml config file")
	flag.StringVar(&grpcAddress, "grpc", "localhost:5995",
		"gRPC server address for writing data (default: localhost:5995)")
	flag.BoolVar(&noRPC, "no-rpc", false,
		"write directly to the filesystem instead of via gRPC to a running server")
	flag.StringVar(&pprofAddr, "pprof", "",
		"address for pprof HTTP server (e.g., localhost:6060). Disabled if empty.")
	flag.StringVar(&progressMode, "progress", "auto",
		"interactive progress bar with ETA: auto (only when stderr is a terminal), always, or never")
	flag.BoolVar(&assumeYes, "yes", false,
		"assume yes to confirmation prompts (required for full-universe '*' tick backfills in non-interactive runs)")
	flag.BoolVar(&assumeYes, "y", false, "alias for --yes")
	flag.BoolVar(&assumeYes, "force", false, "alias for --yes")

	flag.Parse()

	// Record whether the user explicitly set -write-concurrency so we can
	// apply a mode-specific default (see main) only when they did not.
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "write-concurrency" {
			writeConcurrencySet = true
		}
	})
}

func main() {
	parseFlags()

	// Start pprof server if requested.
	if pprofAddr != "" {
		go func() {
			log.Info("[flatfiles] pprof server listening on %s", pprofAddr)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				log.Error("[flatfiles] pprof server failed: %v", err)
			}
		}()
	}

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
		// Direct disk writes parallelize well, so default to a higher writer
		// concurrency than the gRPC path. Kept moderate (4) because each
		// concurrent writer holds a full date's CSM in memory; for 1Min over
		// the full symbol universe these are large, so too many writers cause
		// memory/disk contention. Only override when the user did not set
		// -write-concurrency explicitly.
		if !writeConcurrencySet {
			writeConcurrency = 4
		}
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

	s3Client, err := flatfiles.NewS3Client(s3AccessKey, s3SecretKey)
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

	// If any tick data type is requested, build the shared exchange map (one
	// API call) and a per-symbol round_lot resolver, and gate full-universe
	// '*' backfills.
	var tickRes *tickResources
	for key := range fromDates {
		if flatfiles.IsTickKey(key) {
			if symbolsIsWildcard && !confirmFullUniverse() {
				log.Error("[flatfiles] full-universe tick backfill declined; aborting")
				if instanceMeta != nil {
					instanceMeta.WALFile.Shutdown()
				}
				os.Exit(1)
			}
			tickRes = newTickResources(apiKey)
			break
		}
	}

	startTime := time.Now()

	// Process each data type (1D, 1Min, 1D-index, trades, quotes).
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

		ffType, ok := flatFileDataTypes[key]
		if !ok {
			log.Warn("[flatfiles] unsupported data type %q, skipping", key)
			continue
		}

		configStart, err := time.Parse(dateFormat, startDateStr)
		if err != nil {
			log.Warn("[flatfiles] invalid date %q for %s, skipping", startDateStr, key)
			continue
		}

		if ffType.Tick {
			runTickBackfill(ctx, s3Client, w, symbolSet, key, ffType, configStart, endDate, parallelism, tickRes)
		} else {
			runFlatFileBackfill(ctx, s3Client, w, symbolSet, key, ffType, configStart, endDate, parallelism)
		}
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
	s3Client *flatfiles.S3Client,
	w backfill.Writer,
	symbolSet map[string]bool,
	timeframe string, // "1D", "1Min", or "1D-index"
	ffType flatfiles.FlatFileType,
	startDate, endDate time.Time,
	parallelism int,
) {
	dates := flatfiles.MarketDays(startDate, endDate)
	if len(dates) == 0 {
		log.Info("[flatfiles] no market days in range for %s", timeframe)
		return
	}

	log.Info("[flatfiles] backfilling %s: %d market days from %s to %s for %d symbols",
		timeframe, len(dates), startDate.Format(dateFormat), endDate.Format(dateFormat), len(symbolSet))

	_, _, err := flatfiles.BackfillDates(ctx, s3Client, w, symbolSet, timeframe, ffType.S3Prefix, ffType.S3DataType, dates, flatfiles.BackfillConfig{
		Parallelism:      parallelism,
		WriteConcurrency: writeConcurrency,
		WriteBufferSize:  writeBuffer,
		ProgressBar:      progressMode,
	})
	if err != nil && ctx.Err() == nil {
		log.Warn("[flatfiles] %s backfill encountered errors: %v", timeframe, err)
	}
}

// tickResources holds the shared exchange map and round_lot resolver used by
// tick (trades/quotes) backfills.
type tickResources struct {
	exMap    *mapping.ExchangeMap
	roundLot func(string) int
}

// newTickResources builds the exchange map (one API call, static fallback on
// error) and a cached per-symbol round_lot resolver. If no API key is
// available, the static exchange table is used and round_lot defaults to 100.
func newTickResources(apiKey string) *tickResources {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
			MaxConnsPerHost:     defaultMaxConnsPerHost,
		},
		Timeout: 30 * time.Second,
	}

	var exMap *mapping.ExchangeMap
	if apiKey != "" {
		api.SetAPIKey(apiKey)
		exMap = mapping.LoadExchangeMap(client)
	} else {
		log.Warn("[flatfiles] no API key; using static exchange table and default round_lot=100")
		exMap = mapping.StaticExchangeMap()
	}

	var mu sync.Mutex
	cache := make(map[string]int)
	roundLot := func(sym string) int {
		mu.Lock()
		if v, ok := cache[sym]; ok {
			mu.Unlock()
			return v
		}
		mu.Unlock()

		rl := 100
		if apiKey != "" {
			if v, err := api.GetTickerRoundLot(client, sym); err == nil && v > 0 {
				rl = v
			}
		}

		mu.Lock()
		cache[sym] = rl
		mu.Unlock()
		return rl
	}

	return &tickResources{exMap: exMap, roundLot: roundLot}
}

// confirmFullUniverse gates a full-universe ('*') tick backfill. It returns
// true if the run may proceed. With --yes it always proceeds. Otherwise, if
// stdin is not a TTY it refuses (returns false) without blocking; if stdin is a
// TTY it prompts interactively.
func confirmFullUniverse() bool {
	if assumeYes {
		return true
	}

	fi, err := os.Stdin.Stat()
	if err != nil || (fi.Mode()&os.ModeCharDevice) == 0 {
		log.Error("[flatfiles] full-universe ('*') tick backfill requested without --yes in a " +
			"non-interactive session. Tick volumes are enormous (~155M trades / ~497M quotes per " +
			"day). Re-run with --yes to proceed.")
		return false
	}

	fmt.Fprintln(os.Stderr,
		"WARNING: full-universe ('*') tick backfill. Tick volumes are enormous "+
			"(~155M trades / ~497M quotes per day) and this is not optimized for bulk history.")
	fmt.Fprint(os.Stderr, "Proceed? [y/N]: ")
	var resp string
	if _, err := fmt.Fscanln(os.Stdin, &resp); err != nil {
		return false
	}
	resp = strings.ToLower(strings.TrimSpace(resp))
	return resp == "y" || resp == "yes"
}

// runTickBackfill processes all dates in [startDate, endDate] for a tick data
// type (trades/quotes), streaming per-symbol writes to MarketStore.
func runTickBackfill(
	ctx context.Context,
	s3Client *flatfiles.S3Client,
	w backfill.Writer,
	symbolSet map[string]bool,
	dataType string, // "trades" | "quotes"
	ffType flatfiles.FlatFileType,
	startDate, endDate time.Time,
	parallelism int,
	res *tickResources,
) {
	dates := flatfiles.MarketDays(startDate, endDate)
	if len(dates) == 0 {
		log.Info("[flatfiles] no market days in range for %s", dataType)
		return
	}

	log.Info("[flatfiles] backfilling %s: %d market days from %s to %s for %d symbols",
		dataType, len(dates), startDate.Format(dateFormat), endDate.Format(dateFormat), len(symbolSet))

	var roundLot func(string) int
	exMap := mapping.StaticExchangeMap()
	if res != nil {
		exMap = res.exMap
		roundLot = res.roundLot
	}

	// Tick downloads are large; use a lower default parallelism unless the
	// user explicitly raised it.
	tickParallelism := parallelism
	if tickParallelism > 4 {
		tickParallelism = 4
	}

	_, _, err := flatfiles.BackfillTicks(ctx, s3Client, w, symbolSet, dataType,
		ffType.S3Prefix, ffType.S3DataType, exMap, roundLot, dates, flatfiles.BackfillConfig{
			Parallelism:      tickParallelism,
			WriteConcurrency: writeConcurrency,
			WriteBufferSize:  writeBuffer,
			ProgressBar:      progressMode,
		})
	if err != nil && ctx.Err() == nil {
		log.Warn("[flatfiles] %s backfill encountered errors: %v", dataType, err)
	}
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
	// Only use keys that map to flat file data types (1D, 1Min, 1D-index).
	if len(fromDates) == 0 && len(cfg.QueryStart) > 0 {
		for key, dateStr := range cfg.QueryStart {
			if _, ok := flatFileDataTypes[key]; !ok {
				continue // skip unsupported data types
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
	if strings.ContainsAny(input, "*?[") {
		// A glob pattern (but not a brace list) is treated as a wildcard for
		// the purpose of the full-universe tick gate.
		symbolsIsWildcard = true
	}
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
	// BackgroundSync MUST be enabled. executor.WriteCSM queues every write
	// command into txnPipe.writeChannel (buffer = WriteChannelCommandDepth,
	// 1,000,000) inside its per-symbol loop and only calls RequestFlush AFTER
	// the loop finishes. Without BackgroundSync, the SyncWAL drain goroutine
	// never starts, so a single WriteCSM that generates more than 1M commands
	// fills the channel and blocks forever mid-loop -- the flush that would
	// drain it is unreachable. Full-universe 1Min data hits this (~1.6M bars
	// per day = ~1.6M commands); daily (~10k commands) stays under the buffer
	// and so worked by accident. With WALBypass=true, SyncWAL flushes straight
	// to primary storage and skips WAL-file writes, so there is no WAL
	// overhead -- it just provides the concurrent drainer WriteCSM requires.
	cfg.BackgroundSync = true
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
