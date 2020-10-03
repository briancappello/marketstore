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
)

// executeInit implements the init command.
func executeInit(*cobra.Command, []string) error {
	// serialize default file.
	data, err := Asset("default.yml")
	if err != nil {
		return err
	}

	// write to current directory.
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

	api.SetCredentials(
		utils.InstanceConfig.Alpaca.APIKey,
		utils.InstanceConfig.Alpaca.APISecret,
	)

	assets, err := api.ListAssets()
	if err != nil {
		return err
	}

	ohlcvDataShapes := []io.DataShape{
		{Name: "Epoch", Type: io.EPOCH},
		{Name: "Open", Type: io.FLOAT32},
		{Name: "High", Type: io.FLOAT32},
		{Name: "Low", Type: io.FLOAT32},
		{Name: "Close", Type: io.FLOAT32},
		{Name: "Volume", Type: io.INT32},
	}

	for _, asset := range assets {
		if strings.Contains(asset.Symbol, "-") || len(asset.Symbol) > 4 {
			continue
		}

		log.Info("Creating timebuckets for %v", asset.Symbol)
		if err := create(io.NewTimeBucketKey(asset.Symbol + "/1Min/OHLCV"), ohlcvDataShapes); err != nil {
			log.Warn("%v", err)
		}
		if err := create(io.NewTimeBucketKey(asset.Symbol + "/1D/OHLCV"), ohlcvDataShapes); err != nil {
			log.Warn("%v", err)
		}
	}
	return nil
}

func create(tbk *io.TimeBucketKey, dataShapes []io.DataShape) error {
	tf, err := tbk.GetTimeFrame()
	if err != nil {
		return err
	}
	dir := tbk.GetPathToYearFiles(executor.ThisInstance.RootDir)
	year := int16(time.Now().Year())
	rt := io.EnumRecordTypeByName("fixed")

	tbinfo := io.NewTimeBucketInfo(*tf, dir, "Default", year, dataShapes, rt)

	err = executor.ThisInstance.CatalogDir.AddTimeBucket(tbk, tbinfo)
	if err != nil && !strings.Contains(err.Error(), "Can not overwrite file") {
		return fmt.Errorf("creation of new catalog entry failed: %s", err.Error())
	}

	return nil
}
