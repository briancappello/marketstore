package frontend_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

func setupListSymbols(t *testing.T) *frontend.DataService {
	t.Helper()

	rootDir := t.TempDir()
	// MakeDummyCurrencyDir creates data for years 2000, 2001, 2002
	// with symbols EURUSD, USDJPY, NZDUSD
	// and timeframes 1Min, 5Min, 15Min, 1H, 4H, 1D
	test.MakeDummyCurrencyDir(rootDir, true, false)
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	_ = executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
	atomic.StoreUint32(&frontend.Queryable, uint32(1))

	qs := frontend.NewQueryService(c.GetCatalogDir())
	writer, _ := executor.NewWriter(c.GetCatalogDir(), c.GetInitWALFile())
	return frontend.NewDataService(rootDir, c.GetCatalogDir(), sqlparser.NewAggRunner(nil), writer, qs)
}

func TestListSymbols_NoFilters(t *testing.T) {
	service := setupListSymbols(t)

	req := &frontend.ListSymbolsRequest{}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 3)
	// Should contain all 3 symbols
	assert.Contains(t, response.Results, "EURUSD")
	assert.Contains(t, response.Results, "USDJPY")
	assert.Contains(t, response.Results, "NZDUSD")
}

func TestListSymbols_FilterByTimeframe(t *testing.T) {
	service := setupListSymbols(t)

	req := &frontend.ListSymbolsRequest{
		Timeframe: "1Min",
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 3)
	// All symbols have 1Min data
	assert.Contains(t, response.Results, "EURUSD")
	assert.Contains(t, response.Results, "USDJPY")
	assert.Contains(t, response.Results, "NZDUSD")
}

func TestListSymbols_FilterByNonexistentTimeframe(t *testing.T) {
	service := setupListSymbols(t)

	req := &frontend.ListSymbolsRequest{
		Timeframe: "1Sec", // This timeframe doesn't exist in the test data
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 0)
}

func TestListSymbols_FilterByDateISO(t *testing.T) {
	service := setupListSymbols(t)

	// Test with a date in year 2001 (which has data) using ISO format
	req := &frontend.ListSymbolsRequest{
		Date: "2001-06-15",
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 3)
	assert.Contains(t, response.Results, "EURUSD")
	assert.Contains(t, response.Results, "USDJPY")
	assert.Contains(t, response.Results, "NZDUSD")
}

func TestListSymbols_FilterByDateEpoch(t *testing.T) {
	service := setupListSymbols(t)

	// Test with a date in year 2001 (which has data) using epoch seconds
	req := &frontend.ListSymbolsRequest{
		Date: "992563200", // 2001-06-15 00:00:00 UTC
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 3)
	assert.Contains(t, response.Results, "EURUSD")
	assert.Contains(t, response.Results, "USDJPY")
	assert.Contains(t, response.Results, "NZDUSD")
}

func TestListSymbols_FilterByDateNoData(t *testing.T) {
	service := setupListSymbols(t)

	// Test with a date in year 2020 (which has no data - test data only has 2000-2002)
	req := &frontend.ListSymbolsRequest{
		Date: "2020-06-15",
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 0)
}

func TestListSymbols_FilterByDateInvalidFormat(t *testing.T) {
	service := setupListSymbols(t)

	req := &frontend.ListSymbolsRequest{
		Date: "not-a-date",
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "invalid date format")
}

func TestListSymbols_FilterByTimeframeAndDate(t *testing.T) {
	service := setupListSymbols(t)

	// Test with both timeframe and date filters
	req := &frontend.ListSymbolsRequest{
		Timeframe: "1D",
		Date:      "2001-06-15",
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 3)
}

func TestListSymbols_TBKFormat(t *testing.T) {
	service := setupListSymbols(t)

	req := &frontend.ListSymbolsRequest{
		Format: "tbk",
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	// 3 symbols * 6 timeframes = 18 TBKs
	assert.Len(t, response.Results, 18)
}

func TestListSymbols_FilterByDateWithNonUTCTimezone(t *testing.T) {
	// The server uses InstanceConfig.Timezone for on-disk index calculations.
	// When configured with a non-UTC timezone (e.g. America/New_York), the
	// date filter in ListSymbols must construct day boundaries in that same
	// timezone so the file offset scan hits the correct records.
	origTZ := utils.InstanceConfig.Timezone
	loc, err := time.LoadLocation("America/New_York")
	assert.Nil(t, err)
	utils.InstanceConfig.Timezone = loc
	defer func() { utils.InstanceConfig.Timezone = origTZ }()

	service := setupListSymbols(t)

	req := &frontend.ListSymbolsRequest{
		Timeframe: "1D",
		Date:      "2001-06-15",
	}
	var response frontend.ListSymbolsResponse
	err = service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 3)
	assert.Contains(t, response.Results, "EURUSD")
	assert.Contains(t, response.Results, "USDJPY")
	assert.Contains(t, response.Results, "NZDUSD")
}

func TestListSymbols_ResultsAreSorted(t *testing.T) {
	service := setupListSymbols(t)

	req := &frontend.ListSymbolsRequest{
		Timeframe: "1Min",
	}
	var response frontend.ListSymbolsResponse
	err := service.ListSymbols(nil, req, &response)

	assert.Nil(t, err)
	assert.Len(t, response.Results, 3)
	// Results should be sorted alphabetically
	assert.Equal(t, "EURUSD", response.Results[0])
	assert.Equal(t, "NZDUSD", response.Results[1])
	assert.Equal(t, "USDJPY", response.Results[2])
}
