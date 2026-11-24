package splitsync_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/splitsync"
	"github.com/alpacahq/marketstore/v4/corpaction"
)

func set(syms ...string) func(string) bool {
	m := map[string]bool{}
	for _, s := range syms {
		m[s] = true
	}
	return func(s string) bool { return m[s] }
}

func TestToEventMapsFields(t *testing.T) {
	e, err := splitsync.ToEvent(api.Split{
		Ticker: "NVDA", ExecutionDate: "2024-06-10", SplitFrom: 1, SplitTo: 10, AdjustmentType: "forward_split",
	})
	require.NoError(t, err)
	ny, _ := time.LoadLocation("America/New_York")
	assert.Equal(t, time.Date(2024, 6, 10, 0, 0, 0, 0, ny).Unix(), e.Effective.Unix())
	assert.Equal(t, 10.0, e.PriceFactor)
	assert.Equal(t, corpaction.ForwardSplit, e.Subtype)

	for typ, want := range map[string]corpaction.Subtype{
		"reverse_split": corpaction.ReverseSplit, "stock_dividend": corpaction.StockDividend,
	} {
		e, err := splitsync.ToEvent(api.Split{Ticker: "X", ExecutionDate: "2024-01-02", SplitFrom: 2, SplitTo: 1, AdjustmentType: typ})
		require.NoError(t, err)
		assert.Equal(t, want, e.Subtype)
	}
}

func TestBuildSkipsMalformedAndUntracked(t *testing.T) {
	results := []api.Split{
		{Ticker: "AAPL", ExecutionDate: "2020-08-31", SplitFrom: 1, SplitTo: 4, AdjustmentType: "forward_split"},
		{Ticker: "AAPL", ExecutionDate: "2014-06-09", SplitFrom: 0, SplitTo: 7, AdjustmentType: "forward_split"},
		{Ticker: "AAPL", ExecutionDate: "2012-01-03", SplitFrom: 1, SplitTo: 2, AdjustmentType: "spinoff"},
		{Ticker: "AAPL", ExecutionDate: "not-a-date", SplitFrom: 1, SplitTo: 2, AdjustmentType: "forward_split"},
		{Ticker: "ZZZZ", ExecutionDate: "2021-01-04", SplitFrom: 1, SplitTo: 2, AdjustmentType: "forward_split"},
	}
	plan := splitsync.Build(results, set("AAPL"), false, nil)
	assert.Equal(t, 1, plan.Written)
	assert.Len(t, plan.Skipped, 3)
	assert.Equal(t, []string{"AAPL"}, plan.Symbols)
	require.Len(t, plan.Events, 1)
	assert.Equal(t, 4.0, plan.Events[0].PriceFactor)
}

func TestBuildWildcardTracksEverything(t *testing.T) {
	results := []api.Split{
		{Ticker: "ZZZZ", ExecutionDate: "2021-01-04", SplitFrom: 1, SplitTo: 2, AdjustmentType: "forward_split"},
	}
	plan := splitsync.Build(results, set(), true, nil)
	assert.Equal(t, []string{"ZZZZ"}, plan.Symbols)
}

func TestBuildNeutralizesWithdrawn(t *testing.T) {
	keep, err := corpaction.NewSplit("AAPL", corpaction.EffectiveDate(2020, 8, 31), 1, 4, corpaction.ForwardSplit)
	require.NoError(t, err)
	gone, err := corpaction.NewSplit("AAPL", corpaction.EffectiveDate(2021, 1, 4), 1, 2, corpaction.ForwardSplit)
	require.NoError(t, err)
	onlyStored, err := corpaction.NewSplit("MSFT", corpaction.EffectiveDate(2003, 2, 18), 1, 2, corpaction.ForwardSplit)
	require.NoError(t, err)

	results := []api.Split{
		{Ticker: "AAPL", ExecutionDate: "2020-08-31", SplitFrom: 1, SplitTo: 4, AdjustmentType: "forward_split"},
	}
	stored := map[string][]corpaction.Event{"AAPL": {keep, gone}, "MSFT": {onlyStored}}
	plan := splitsync.Build(results, set("AAPL", "MSFT"), false, stored)
	assert.Equal(t, 1, plan.Written)
	assert.Equal(t, 2, plan.Neutralized)
	assert.Equal(t, []string{"AAPL", "MSFT"}, plan.Symbols)
	for _, e := range plan.Events {
		if e.Effective.Equal(gone.Effective) || e.Symbol == "MSFT" {
			assert.True(t, e.IsNeutral(), "%s %s", e.Symbol, e.Effective)
		}
	}

	// A second sync over the now-neutralized store changes nothing more.
	stored = map[string][]corpaction.Event{"AAPL": {keep}}
	plan = splitsync.Build(results, set("AAPL"), false, stored)
	assert.Equal(t, 0, plan.Neutralized)
}

func TestBuildSkipsUnchanged(t *testing.T) {
	keep, err := corpaction.NewSplit("AAPL", corpaction.EffectiveDate(2020, 8, 31), 1, 4, corpaction.ForwardSplit)
	require.NoError(t, err)
	results := []api.Split{
		{Ticker: "AAPL", ExecutionDate: "2020-08-31", SplitFrom: 1, SplitTo: 4, AdjustmentType: "forward_split"},
	}
	plan := splitsync.Build(results, set("AAPL"), false, map[string][]corpaction.Event{"AAPL": {keep}})
	assert.Empty(t, plan.Events)
	assert.Equal(t, 1, plan.Unchanged)

	// A corrected ratio is written again.
	results[0].SplitTo = 10
	plan = splitsync.Build(results, set("AAPL"), false, map[string][]corpaction.Event{"AAPL": {keep}})
	require.Len(t, plan.Events, 1)
	assert.Equal(t, 10.0, plan.Events[0].PriceFactor)
}

func TestBuildMergesSameDayBeforeComparing(t *testing.T) {
	results := []api.Split{
		{Ticker: "BTX", ExecutionDate: "2016-06-17", SplitFrom: 100, SplitTo: 1, AdjustmentType: "reverse_split"},
		{Ticker: "BTX", ExecutionDate: "2016-06-17", SplitFrom: 1, SplitTo: 2, AdjustmentType: "forward_split"},
	}
	plan := splitsync.Build(results, set("BTX"), false, nil)
	require.Len(t, plan.Events, 1, "one merged row per day")
	assert.InDelta(t, 0.02, plan.Events[0].PriceFactor, 1e-12)

	// Once stored, the merged row is recognized as unchanged.
	plan = splitsync.Build(results, set("BTX"), false, map[string][]corpaction.Event{"BTX": plan.Events})
	assert.Empty(t, plan.Events)
	assert.Equal(t, 1, plan.Unchanged)
}
