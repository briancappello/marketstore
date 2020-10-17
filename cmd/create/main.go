// Package create - because packages cannot be named 'init' in go.
//go:generate go-bindata -pkg create default.yml
package create

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/alpaca/api"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/spf13/cobra"
)

const (
	usage   = "init"
	short   = "Creates a new mkts.yml file"
	long    = "This command creates a new mkts.yml file in the current directory"
	example = "marketstore init"

	configFilePath = "mkts.yml"
)

var (
	// Cmd is the init command.
	Cmd = &cobra.Command{
		Use:        usage,
		Short:      short,
		Long:       long,
		SuggestFor: []string{"create", "new"},
		Example:    example,
		RunE:       executeInit,
	}
	Symbols      string
	symbolsList  []string
	validSymbols map[string]int
)

func init() {
	Cmd.Flags().StringVar(&Symbols, "symbols", "",
		"Optional comma-separated list of symbols to initialize")
}

// executeInit implements the init command.
func executeInit(*cobra.Command, []string) error {
	// serialize default file.
	data, err := Asset("default.yml")
	if err != nil {
		return err
	}

	// write to current directory (if no mkts.yml file exists yet)
	_, err = os.Stat(configFilePath)
	if os.IsNotExist(err) {
		err = ioutil.WriteFile(configFilePath, data, 0644)
		if err != nil {
			return err
		}
	}

	if err := utils.InstanceConfig.Load(configFilePath); err != nil {
		return err
	}
	executor.NewInstanceSetup(utils.InstanceConfig.RootDirectory, true, true, true, true)

	// fetch valid symbols from Alpaca API
	api.SetCredentials(
		utils.InstanceConfig.Alpaca.APIKey,
		utils.InstanceConfig.Alpaca.APISecret,
	)
	assets, err := api.ListAssets()
	if err != nil {
		return err
	}
	validSymbols = make(map[string]int)
	for _, asset := range assets {
		if strings.Contains(asset.Symbol, "-") || len(asset.Symbol) > 4 {
			continue
		}
		validSymbols[asset.Symbol] = 0
	}

	if Symbols != "" {
		for _, symbol := range strings.Split(Symbols, ",") {
			if _, valid := validSymbols[symbol]; valid {
				symbolsList = append(symbolsList, symbol)
			} else {
				log.Warn("invalid symbol passed: %v", symbol)
			}
		}
	} else {
		for symbol := range validSymbols {
			symbolsList = append(symbolsList, symbol)
		}
	}

	ohlcvDataShapes := []io.DataShape{
		{Name: "Epoch", Type: io.EPOCH},
		{Name: "Open", Type: io.FLOAT32},
		{Name: "High", Type: io.FLOAT32},
		{Name: "Low", Type: io.FLOAT32},
		{Name: "Close", Type: io.FLOAT32},
		{Name: "Volume", Type: io.INT64},
	}

	for _, symbol := range symbolsList {
		if _, err := create(io.NewTimeBucketKey(symbol+"/1Min/OHLCV"), ohlcvDataShapes); err != nil {
			log.Warn("%v", err)
		}

		if _, err := create(io.NewTimeBucketKey(symbol+"/1D/OHLCV"), ohlcvDataShapes); err != nil {
			log.Warn("%v", err)
		}

		//latestExisting, err := findLatestExisting(symbol, "1Min")
		//if err != nil {
		//	log.Warn("%v: %v", err, symbol)
		//} else if !latestExisting.IsZero() {
		//	log.Info("%v latest 1Min: %v", symbol, latestExisting)
		//}
	}
	return nil
}

func create(tbk *io.TimeBucketKey, dataShapes []io.DataShape) (bool, error) {
	/*
		Create a new time bucket with the given datashape
		returns (created, error)
	*/
	tf, err := tbk.GetTimeFrame()
	if err != nil {
		return false, err
	}
	dir := tbk.GetPathToYearFiles(executor.ThisInstance.RootDir)
	year := int16(time.Now().Year())
	rt := io.EnumRecordTypeByName("fixed")

	tbinfo := io.NewTimeBucketInfo(*tf, dir, "Default", year, dataShapes, rt)

	err = executor.ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)
	if err != nil {
		if strings.Contains(err.Error(), "Can not overwrite file") {
			return false, nil
		} else {
			return false, fmt.Errorf("creation of new catalog entry failed: %s", err.Error())
		}
	}

	return true, nil
}

func findLatestExisting(symbol, timeframe string) (time.Time, error) {
	latestExisting := time.Time{}
	tbk := io.NewTimeBucketKey(fmt.Sprintf("%s/%s/OHLCV", symbol, timeframe))

	instance := executor.ThisInstance
	cDir := instance.CatalogDir
	q := planner.NewQuery(cDir)
	q.AddTargetKey(tbk)
	q.SetRowLimit(io.LAST, 1)

	parsed, err := q.Parse()
	if err != nil {
		return latestExisting, fmt.Errorf("[polygon] query parse failure (%v)", err)
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		return latestExisting, fmt.Errorf("[polygon] new scanner failure (%v)", err)
	}

	csm, err := scanner.Read()
	if err != nil {
		return latestExisting, fmt.Errorf("[polygon] scanner read failure (%v)", err)
	}

	// if len(epoch) == 0, no data exists yet; return the default value defined above
	epoch := csm[*tbk].GetEpoch()
	if len(epoch) > 0 {
		latestExisting = time.Unix(epoch[len(epoch)-1], 0)
	}

	return latestExisting, nil
}
