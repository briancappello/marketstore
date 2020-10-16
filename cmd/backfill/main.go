package backfill

import (
	"fmt"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/alpacahq/marketstore/v4/cmd/start"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/api"
	"github.com/alpacahq/marketstore/v4/contrib/polygon/backfill"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/spf13/cobra"
)

const (
	usage            = "backfill"
	short            = "Backfill 1Min bars"
	long             = short
	example          = "marketstore backfill --timeframe 1D --symbols AMD,NVDA --start 2010-01-01"
	format           = "2006-01-02"
	defaultStartDate = "2004-01-01"
)

var (
	Cmd = &cobra.Command{
		Use:     usage,
		Short:   short,
		Long:    long,
		Example: example,
		RunE:    executeStart,
	}
	ConfigFilePath string
	StartDate      string
	EndDate        string
	Parallelism    int
	Symbols        string
	Timeframe      string

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
	Cmd.Flags().StringVar(&Timeframe, "timeframe", "1Min",
		"The timeframe to backfill (eg 1Min or 1D)")
}

func executeStart(cmd *cobra.Command, args []string) error {
	if err := initConfig(); err != nil {
		return err
	}
	initWriter()

	if Symbols != "" && Symbols != "*" {
		symbolList = strings.Split(Symbols, ",")
	} else {
		for symbol := range executor.ThisInstance.CatalogDir.GatherCategoriesAndItems()["Symbol"] {
			symbolList = append(symbolList, symbol)
		}
	}
	// FIXME verify symbols exist on polygon

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

	log.Info("[backfill] backfilling bars from %v to %v", startTime.Format(format), endTime.Format(format))

	backfillMinuteBars := func(tbk *io.TimeBucketKey, s, e time.Time) {
		symbol := tbk.GetItemInCategory("Symbol")
		cd, err := tbk.GetCandleDuration()
		if err != nil {
			log.Error("[backfill] error creating TBK Candle Duration")
			return
		}

		for e.After(s) {
			if calendar.Nasdaq.IsMarketDay(s) {
				log.Info("[backfill] backfilling bars for %v on %v", symbol, s.Format(format))

				sem <- struct{}{}
				go func(t time.Time) {
					defer func() { <-sem }()

					if err := backfill.Bars(symbol, t, t.Add(24*time.Hour), cd.String); err != nil {
						log.Warn("[backfill] failed to backfill minutely bars for %v on %v (%v)", symbol, t.Format(format), err)
					}
				}(s)
			}
			s = s.Add(24 * time.Hour)
		}
	}

	for _, sym := range symbolList {
		tbk := io.NewTimeBucketKeyFromString(sym + "/" + Timeframe + "/OHLCV")
		cd, err := tbk.GetCandleDuration()
		if err != nil {
			log.Error("[backfill] error creating TBK Candle Duration: %v", err)
		}

		log.Info("[backfill] backfilling %v bars for %v", cd.String, sym)
		if cd.Suffix() == "Min" || cd.Suffix() == "T" {
			backfillMinuteBars(tbk, startTime, endTime)
		} else if cd.Suffix() == "D" {
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()

				if err := backfill.Bars(sym, startTime, endTime, cd.String); err != nil {
					log.Warn("[backfill] failed to backfill daily bars for %v (%v)", sym, err)
				}
			}()
		}
	}

	// make sure all goroutines finish
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}

	log.Info("[backfill] backfilling complete")

	if len(executor.ThisInstance.TriggerMatchers) > 0 {
		log.Info("[backfill] waiting for 10 more seconds for ondiskagg triggers to complete")
		time.Sleep(10 * time.Second)
	}

	return nil
}

func initConfig() error {
	// Attempt to set configuration
	var err error

	if err = utils.InstanceConfig.Load(ConfigFilePath); err != nil {
		return err
	}

	api.SetAPIKey(utils.InstanceConfig.Alpaca.APIKey)

	if StartDate != "" {
		startTime, err = time.Parse(format, StartDate)
	} else {
		startTime, err = time.Parse(format, defaultStartDate)
	}

	if err != nil {
		return fmt.Errorf("[backfill] failed to parse start date from timestamp (%v)", err)
	}

	if EndDate != "" {
		endTime, err = time.Parse(format, EndDate)

		if err != nil {
			return fmt.Errorf("[backfill] failed to parse end date from timestamp (%v)", err)
		}
	} else {
		t := time.Now().AddDate(0, 0, 1)
		endTime = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Round(time.Minute)
	}

	return nil
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
