package backfill

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/alpacahq/marketstore/v4/cmd/start"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/polygon_config"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/spf13/cobra"
)

const (
	usage   = "backfill"
	short   = "Backfill 1Min bars"
	long    = short
	example = "marketstore backfill --start 2020-01-01 --symbols AMD,NVDA"
	format  = "2006-01-02"
)

var (
	Cmd = &cobra.Command{
		Use:     usage,
		Short:   short,
		Long:    long,
		Example: example,
		RunE:    executeStart,
	}
	ConfigFilePath     string
	Config             polygon_config.FetcherConfig
	StartDate          string
	EndDate            string
	Parallelism        int
	Symbols            string

	startTime, endTime time.Time
	symbolList         []string
)

func init() {
	Cmd.Flags().StringVar(&ConfigFilePath, "config", "./mkts.yml",
		"File path to the marketstore configuration file.")
	Cmd.Flags().StringVarP(&StartDate, "start", "s", "",
		"The date to start from (YYYY-MM-DD)")
	Cmd.Flags().StringVarP(&EndDate, "end", "e", "",
		"The date to end at (YYYY-MM-DD, default tomorrow)")
	Cmd.Flags().IntVar(&Parallelism, "parallelism", runtime.NumCPU(),
		"Parallelism (default NumCPU)")
	Cmd.Flags().StringVar(&Symbols, "symbols", "*",
		"Comma-separated list of symbols to backfill (default all in DB)")
}

func executeStart(cmd *cobra.Command, args []string) error {
	initConfig()
	initWriter()

	if Symbols == "*" {
		for symbol := range executor.ThisInstance.CatalogDir.GatherCategoriesAndItems()["Symbol"] {
			symbolList = append(symbolList, symbol)
		}
	} else if Symbols != "" {
		symbolList = strings.Split(Symbols, ",")
	} else {
		symbolList = Config.Symbols
	}
	// FIXME verify symbols exist on polygon?

	// free memory in the background every 1 minute for long running backfills with very high parallelism
	go func() {
		for {
			<-time.After(time.Minute)
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			memStart := m.Alloc
			log.Info("freeing memory...")
			debug.FreeOSMemory()
			runtime.ReadMemStats(&m)
			memEnd := m.Alloc
			log.Info(
				"mem stats: [start: %v end: %v freed: %v]",
				bytefmt.ByteSize(memStart),
				bytefmt.ByteSize(memEnd),
				bytefmt.ByteSize(memStart-memEnd),
			)
		}
	}()

	sem := make(chan struct{}, Parallelism)

	log.Info("[polygon] backfilling bars from %v to %v", startTime.Format(format), endTime.Format(format))

	for _, sym := range symbolList {
		s := startTime
		e := endTime

		log.Info("[polygon] backfilling bars for %v", sym)

		for e.After(s) {
			if calendar.Nasdaq.IsMarketDay(s) {
				log.Info("[polygon] backfilling bars for %v on %v", sym, s.Format(format))

				sem <- struct{}{}
				go func(t time.Time) {
					defer func() { <-sem }()

					if err := backfill.Bars(sym, t, t.Add(24*time.Hour)); err != nil {
						log.Warn("[polygon] failed to backfill bars for %v on %v (%v)", sym, t.Format(format), err)
					}
				}(s)
			}
			s = s.Add(24 * time.Hour)
		}
	}

	// make sure all goroutines finish
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	log.Info("[polygon] backfilling complete")

	if len(executor.ThisInstance.TriggerMatchers) > 0 {
		log.Info("[polygon] waiting for 10 more seconds for ondiskagg triggers to complete")
		time.Sleep(10 * time.Second)
	}

	return nil
}

func initConfig() {
	// Attempt to read mkts.yml config file
	data, err := ioutil.ReadFile(ConfigFilePath)
	if err != nil {
		log.Fatal("failed to read configuration file error: %s", err.Error())
		os.Exit(1)
	}

	// Attempt to set configuration
	err = utils.InstanceConfig.Parse(data)
	if err != nil {
		log.Fatal("failed to parse configuration file error: %v", err.Error())
		os.Exit(1)
	}

	// Attempt to set the polygon plugin config settings
	foundPolygonConfig := false
	for _, bgConfig := range utils.InstanceConfig.BgWorkers {
		if bgConfig.Module == "polygon.so" {
			data, _ := json.Marshal(bgConfig.Config)
			if err = json.Unmarshal(data, &Config); err != nil {
				log.Fatal("failed to parse configuration file error: %v", err.Error())
				os.Exit(1)
			}
			foundPolygonConfig = true
			break
		}
	}
	if !foundPolygonConfig {
		log.Fatal("polygon background worker is not configured in %s", ConfigFilePath)
		os.Exit(1)
	}

	if Config.APIKey == "" {
		log.Fatal("[polygon] api key is required")
		os.Exit(1)
	}
	api.SetAPIKey(Config.APIKey)

	if StartDate != "" {
		startTime, err = time.Parse(format, StartDate)
	} else {
		startTime, err = time.Parse(format, Config.QueryStart)
	}

	if err != nil {
		log.Fatal("[backfill] failed to parse start date from timestamp (%v)", err)
		os.Exit(1)
	}

	if EndDate != "" {
		endTime, err = time.Parse(format, EndDate)

		if err != nil {
			log.Fatal("[backfill] failed to parse end date from timestamp (%v)", err)
			os.Exit(1)
		}
	} else {
		t := time.Now()
		endTime = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Round(time.Minute)
	}
}

func initWriter() {
	executor.NewInstanceSetup(utils.InstanceConfig.RootDirectory, true, true, true, true)

	// if configured, also load the 1Min ondiskagg trigger
	for _, triggerSetting := range utils.InstanceConfig.Triggers {
		if triggerSetting.Module == "ondiskagg.so" && triggerSetting.On == "*/1Min/OHLCV" {
			triggerMatcher := start.NewTriggerMatcher(triggerSetting)
			executor.ThisInstance.TriggerMatchers = append(executor.ThisInstance.TriggerMatchers, triggerMatcher)
			break
		}
	}
}
