// Command repair replaces split-adjusted bars with unadjusted bars for the
// buckets a split could have affected. Run it against a live server; it
// reads buckets and writes bars over gRPC.
//
//	repair -grpc localhost:5995 -config mkts.yml -dry-run
//	repair -grpc localhost:5995 -config mkts.yml -timeframes 1Min,1D
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/repair"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/rest"
	"github.com/alpacahq/marketstore/v4/utils"
)

const (
	defaultBatchSize = 50000
	maxRecvMsgSize   = 1 << 30
)

func main() {
	os.Exit(run())
}

// nolint:forbidigo // CLI output
func run() int {
	var (
		grpcAddress = flag.String("grpc", "localhost:5995", "gRPC address of the MarketStore server")
		configPath  = flag.String("config", "mkts.yml", "mkts.yml to read the massive api_key/base_url from")
		apiKey      = flag.String("apiKey", "", "Massive API key (default: MASSIVE_API_KEY, POLYGON_API_KEY or config)")
		baseURL     = flag.String("baseURL", "", "Massive API base URL override")
		symbols     = flag.String("symbols", "", "comma-separated symbols to repair (default: all)")
		timeframes  = flag.String("timeframes", "", "comma-separated bar timeframes to repair (default: all stored)")
		parallelism = flag.Int("parallelism", runtime.NumCPU(), "symbols repaired at once")
		batchSize   = flag.Int("batchSize", defaultBatchSize, "API pagination limit")
		dryRun      = flag.Bool("dry-run", false, "print the affected buckets and ranges, write nothing")
	)
	flag.Parse()

	key, cfgBaseURL := configCredentials(*configPath)
	if *apiKey != "" {
		key = *apiKey
	}
	for _, env := range []string{"MASSIVE_API_KEY", "POLYGON_API_KEY"} {
		if key == "" {
			key = os.Getenv(env)
		}
	}
	if key == "" {
		fmt.Fprintln(os.Stderr, "repair: a Massive API key is required (-apiKey, MASSIVE_API_KEY or config)")
		return 2
	}
	api.SetAPIKey(key)
	if *baseURL != "" {
		api.SetBaseURL(*baseURL)
	} else if cfgBaseURL != "" {
		api.SetBaseURL(cfgBaseURL)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	conn, err := grpc.NewClient(*grpcAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxRecvMsgSize), grpc.MaxCallSendMsgSize(maxRecvMsgSize)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "repair: connect %s: %v\n", *grpcAddress, err)
		return 1
	}
	defer conn.Close()

	httpClient := &http.Client{Timeout: 2 * time.Minute}
	splits, err := api.ListSplits(httpClient, api.SplitsQuery{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "repair: list splits: %v\n", err)
		return 1
	}

	opts := repair.Options{
		Symbols:     set(*symbols),
		Timeframes:  set(*timeframes),
		Parallelism: *parallelism,
		DryRun:      *dryRun,
		Now:         time.Now().In(calendar.Nasdaq.Tz()),
	}
	ranges, err := repair.Plan(ctx, repair.NewGRPCStore(conn), splits, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "repair: plan: %v\n", err)
		return 1
	}

	perTF := map[string]int{}
	symbolsHit := map[string]struct{}{}
	for _, r := range ranges {
		perTF[r.Timeframe]++
		symbolsHit[r.Symbol] = struct{}{}
		if *dryRun {
			fmt.Printf("%-24s %s .. %s  (split %s)\n", r.TBK,
				r.From.In(calendar.Nasdaq.Tz()).Format("2006-01-02 15:04"),
				r.To.In(calendar.Nasdaq.Tz()).Format("2006-01-02 15:04"),
				r.Split.Format("2006-01-02"))
		}
	}
	fmt.Printf("plan: %d splits from Massive, %d buckets in %d symbols to repair %v\n",
		len(splits), len(ranges), len(symbolsHit), perTF)
	if *dryRun {
		return 0
	}

	writer, err := backfill.NewRPCWriter(ctx, *grpcAddress)
	if err != nil {
		fmt.Fprintf(os.Stderr, "repair: %v\n", err)
		return 1
	}
	defer writer.Close()

	started := time.Now()
	fetch := func(ctx context.Context, symbol, timeframe string, from, to time.Time) (int, error) {
		return rest.BarsWindow(ctx, httpClient, symbol, timeframe, from, to, *batchSize, writer)
	}
	rep := repair.Run(ctx, ranges, fetch, *parallelism)
	for _, f := range rep.Failures {
		fmt.Fprintf(os.Stderr, "FAILED %s: %v\n", f.Range.TBK, f.Err)
	}
	fmt.Printf("repaired %d/%d buckets, %d bars written, %d failed, in %s\n",
		rep.Repaired, len(ranges), rep.Bars, len(rep.Failures), time.Since(started).Round(time.Second))
	if len(rep.Failures) > 0 || ctx.Err() != nil {
		return 1
	}
	return 0
}

func set(csv string) map[string]bool {
	if csv == "" {
		return nil
	}
	out := map[string]bool{}
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out[s] = true
		}
	}
	return out
}

// configCredentials reads api_key and base_url from the massive bgworker in
// mkts.yml. A missing or unreadable file yields empty values.
func configCredentials(path string) (key, baseURL string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", ""
	}
	cfg, err := utils.ParseConfig(data)
	if err != nil {
		return "", ""
	}
	for _, bg := range cfg.BgWorkers {
		if !strings.Contains(bg.Module, "massive") {
			continue
		}
		raw, err := json.Marshal(stringKeys(bg.Config))
		if err != nil {
			return "", ""
		}
		var c struct {
			APIKey  string `json:"api_key"`
			BaseURL string `json:"base_url"`
		}
		_ = json.Unmarshal(raw, &c)
		return c.APIKey, c.BaseURL
	}
	return "", ""
}

func stringKeys(v interface{}) interface{} {
	switch m := v.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(m))
		for k, val := range m {
			out[fmt.Sprint(k)] = stringKeys(val)
		}
		return out
	case map[string]interface{}:
		for k, val := range m {
			m[k] = stringKeys(val)
		}
		return m
	case []interface{}:
		for i, val := range m {
			m[i] = stringKeys(val)
		}
		return m
	default:
		return v
	}
}
