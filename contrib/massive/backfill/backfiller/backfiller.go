package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/gobwas/glob"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

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
	bars           bool
	quotes         bool
	trades         bool
	symbols        string
	parallelism    int
	apiKey         string
	baseURL        string
	batchSize      int
	adjusted       bool
	configFilePath string
	barFrequencies string
)

// nolint:gochecknoinits // cobra's standard way to initialize flags
func init() {
	flag.StringVar(&dir, "dir", "", "mktsdb directory (overrides mkts.yml)")
	flag.Var(&fromDates, "from",
		"start date per frequency as key=value (e.g., -from 1Min=2024-01-01 -from 1D=2020-01-01). "+
			"Use 'trades' and 'quotes' as keys for tick data.")
	flag.StringVar(&to, "to", time.Now().Format(dateFormat),
		"backfill to date (YYYY-MM-DD) [not included]")
	flag.BoolVar(&bars, "bars", false, "backfill bars")
	flag.BoolVar(&quotes, "quotes", false, "backfill quotes")
	flag.BoolVar(&trades, "trades", false, "backfill trades")
	flag.StringVar(&symbols, "symbols", "*",
		"glob pattern of symbols to backfill (* = all)")
	flag.IntVar(&parallelism, "parallelism", runtime.NumCPU(),
		"number of parallel API workers (default NumCPU)")
	flag.IntVar(&batchSize, "batchSize", defaultBatchSize,
		"pagination size for API requests")
	flag.StringVar(&apiKey, "apiKey", "", "Massive API key (required)")
	flag.StringVar(&baseURL, "baseURL", "",
		"override Massive API base URL (default https://api.massive.com)")
	flag.BoolVar(&adjusted, "adjusted", true,
		"request split-adjusted price data")
	flag.StringVar(&configFilePath, "config", "/etc/mkts.yml",
		"path to the mkts.yml config file")
	flag.StringVar(&barFrequencies, "barFrequencies", "1Min",
		"comma-separated list of bar timeframes to backfill (e.g., 1Min,5Min,1H,1D)")

	flag.Parse()
}

func main() {
	if apiKey == "" {
		log.Error("[massive] apiKey is required")
		os.Exit(1)
	}

	if len(fromDates) == 0 {
		log.Error("[massive] at least one -from flag is required (e.g., -from 1Min=2024-01-01)")
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

	instanceMeta := initWriter()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: defaultMaxIdleConnsPerHost,
			MaxConnsPerHost:     defaultMaxConnsPerHost,
		},
		Timeout: 30 * time.Second,
	}

	// Resolve symbols.
	symbolList := resolveSymbols(client, symbols)
	if len(symbolList) == 0 {
		log.Error("[massive] no symbols matched pattern: %s", symbols)
		os.Exit(1)
	}
	log.Info("[massive] selected %d symbols", len(symbolList))

	startTime := time.Now()

	if bars {
		frequencies := strings.Split(barFrequencies, ",")
		for _, tf := range frequencies {
			tf = strings.TrimSpace(tf)
			if tf == "" {
				continue
			}

			// Look up the start date for this frequency
			startDateStr, ok := fromDates[tf]
			if !ok {
				log.Warn("[massive] no -from date specified for %s, skipping", tf)
				continue
			}
			start, _ := time.Parse(dateFormat, startDateStr) // already validated

			timeframe := tf // capture for closure
			startCopy := start
			runBackfill(timeframe+" bars", symbolList, startCopy, end, func(sym string, writerWP *worker.Pool) {
				if err := backfill.Bars(client, sym, timeframe, startCopy, end, batchSize, adjusted, writerWP); err != nil {
					log.Warn("[massive] failed to backfill %s bars for %s: %v", timeframe, sym, err)
				}
			})
		}
	}

	if trades {
		startDateStr, ok := fromDates["trades"]
		if !ok {
			log.Warn("[massive] no -from date specified for trades, skipping")
		} else {
			start, _ := time.Parse(dateFormat, startDateStr) // already validated
			runBackfill("trades", symbolList, start, end, func(sym string, writerWP *worker.Pool) {
				if err := backfill.Trades(client, sym, start, end, batchSize, writerWP); err != nil {
					log.Warn("[massive] failed to backfill trades for %s: %v", sym, err)
				}
			})
		}
	}

	if quotes {
		startDateStr, ok := fromDates["quotes"]
		if !ok {
			log.Warn("[massive] no -from date specified for quotes, skipping")
		} else {
			start, _ := time.Parse(dateFormat, startDateStr) // already validated
			runBackfill("quotes", symbolList, start, end, func(sym string, writerWP *worker.Pool) {
				if err := backfill.Quotes(client, sym, start, end, batchSize, writerWP); err != nil {
					log.Warn("[massive] failed to backfill quotes for %s: %v", sym, err)
				}
			})
		}
	}

	instanceMeta.WALFile.Shutdown()
	log.Info("[massive] backfill complete in %s", time.Since(startTime))
}

func runBackfill(name string, symbolList []string, start, end time.Time, fn func(string, *worker.Pool)) {
	log.Info("[massive] backfilling %s for %d symbols from %s to %s",
		name, len(symbolList), start.Format(dateFormat), end.Format(dateFormat))
	apiWP := worker.NewWorkerPool(parallelism)
	writerWP := worker.NewWorkerPool(1)

	for _, sym := range symbolList {
		currentSymbol := sym
		apiWP.Do(func() {
			fn(currentSymbol, writerWP)
		})
	}

	apiWP.CloseAndWait()
	writerWP.CloseAndWait()
}

func resolveSymbols(client *http.Client, pattern string) []string {
	log.Info("[massive] listing tickers for pattern: %s", pattern)
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

func initWriter() *executor.InstanceMetadata {
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

	return executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
}
