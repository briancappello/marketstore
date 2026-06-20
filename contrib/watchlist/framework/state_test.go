package framework_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework"
)

func TestListWatchlistNames_Empty(t *testing.T) {
	mgr := framework.NewSymbolStateManager()
	names := mgr.ListWatchlistNames()
	assert.Empty(t, names)
}

func TestListWatchlistNames_WithRankings(t *testing.T) {
	mgr := framework.NewSymbolStateManager()
	mgr.SetWatchlistRanking("PCT_CHANGE_UP", []framework.RankedSymbol{
		{Symbol: "AAPL", Rank: 1},
	})
	mgr.SetWatchlistRanking("VOLUME_UP", []framework.RankedSymbol{
		{Symbol: "TSLA", Rank: 1},
	})

	names := mgr.ListWatchlistNames()
	sort.Strings(names)
	assert.Equal(t, []string{"PCT_CHANGE_UP", "VOLUME_UP"}, names)
}

func TestAllWatchlistRankings_Empty(t *testing.T) {
	mgr := framework.NewSymbolStateManager()
	all := mgr.AllWatchlistRankings()
	assert.Empty(t, all)
}

func TestAllWatchlistRankings_ReturnsSnapshot(t *testing.T) {
	mgr := framework.NewSymbolStateManager()
	ranking := []framework.RankedSymbol{
		{Symbol: "AAPL", Rank: 1, Fields: []framework.Field{{Key: "pct_change", Value: 5.2}}},
		{Symbol: "NVDA", Rank: 2, Fields: []framework.Field{{Key: "pct_change", Value: 4.1}}},
	}
	mgr.SetWatchlistRanking("PCT_CHANGE_UP", ranking)

	all := mgr.AllWatchlistRankings()
	assert.Len(t, all, 1)
	assert.Len(t, all["PCT_CHANGE_UP"], 2)
	assert.Equal(t, "AAPL", all["PCT_CHANGE_UP"][0].Symbol)
	assert.Equal(t, "NVDA", all["PCT_CHANGE_UP"][1].Symbol)

	// Verify it's a copy -- modifying the result shouldn't affect the manager.
	all["PCT_CHANGE_UP"][0].Symbol = "MODIFIED"
	original := mgr.GetWatchlistRanking("PCT_CHANGE_UP")
	assert.Equal(t, "AAPL", original[0].Symbol)
}
