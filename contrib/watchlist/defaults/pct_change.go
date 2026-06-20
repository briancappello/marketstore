package defaults

import (
	"sort"

	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework"
)

// pctChangeEntry is the shared scratch record for both PctChangeUp and
// PctChangeDown. Promoted from a function-local type so each strategy can
// reuse its backing slice across Rank() calls.
type pctChangeEntry struct {
	symbol    string
	pctChange float64
	volume    int64
}

// --- PCT_CHANGE_UP: Top N by positive percent change ---

// PctChangeUp ranks curated symbols by percent change (descending),
// filtered to positive changes only.
//
// Rank() is invoked serially by the framework's ranking loop, so the
// reusable entries slice does not need synchronization.
type PctChangeUp struct {
	limit   int
	entries []pctChangeEntry
}

// NewPctChangeUp creates a new PctChangeUp strategy.
func NewPctChangeUp(config map[string]interface{}) (framework.WatchlistStrategy, error) {
	limit := 100
	if v, ok := config["limit"]; ok {
		if f, ok := v.(float64); ok {
			limit = int(f)
		}
	}
	return &PctChangeUp{limit: limit}, nil
}

func (s *PctChangeUp) Name() string { return "PCT_CHANGE_UP" }

func (s *PctChangeUp) Configure(config map[string]interface{}) error { return nil }

func (s *PctChangeUp) Rank(curated map[string]*framework.SymbolState) []framework.RankedSymbol {
	entries := s.entries[:0]
	for sym, state := range curated {
		if state.PctChange > 0 {
			entries = append(entries, pctChangeEntry{sym, state.PctChange, state.CumulativeVolume})
		}
	}
	s.entries = entries

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].pctChange > entries[j].pctChange
	})

	limit := s.limit
	if limit > len(entries) {
		limit = len(entries)
	}

	result := make([]framework.RankedSymbol, limit)
	for i := 0; i < limit; i++ {
		result[i] = framework.RankedSymbol{
			Symbol: entries[i].symbol,
			Rank:   i + 1,
			Fields: []framework.Field{
				{Key: "pct_change", Value: entries[i].pctChange},
				{Key: "volume", Value: float64(entries[i].volume)},
			},
		}
	}
	return result
}

// --- PCT_CHANGE_DOWN: Top N by negative percent change (biggest losers) ---

// PctChangeDown ranks curated symbols by percent change (ascending),
// filtered to negative changes only.
//
// Rank() is invoked serially, so entries does not need synchronization.
type PctChangeDown struct {
	limit   int
	entries []pctChangeEntry
}

// NewPctChangeDown creates a new PctChangeDown strategy.
func NewPctChangeDown(config map[string]interface{}) (framework.WatchlistStrategy, error) {
	limit := 100
	if v, ok := config["limit"]; ok {
		if f, ok := v.(float64); ok {
			limit = int(f)
		}
	}
	return &PctChangeDown{limit: limit}, nil
}

func (s *PctChangeDown) Name() string { return "PCT_CHANGE_DOWN" }

func (s *PctChangeDown) Configure(config map[string]interface{}) error { return nil }

func (s *PctChangeDown) Rank(curated map[string]*framework.SymbolState) []framework.RankedSymbol {
	entries := s.entries[:0]
	for sym, state := range curated {
		if state.PctChange < 0 {
			entries = append(entries, pctChangeEntry{sym, state.PctChange, state.CumulativeVolume})
		}
	}
	s.entries = entries

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].pctChange < entries[j].pctChange
	})

	limit := s.limit
	if limit > len(entries) {
		limit = len(entries)
	}

	result := make([]framework.RankedSymbol, limit)
	for i := 0; i < limit; i++ {
		result[i] = framework.RankedSymbol{
			Symbol: entries[i].symbol,
			Rank:   i + 1,
			Fields: []framework.Field{
				{Key: "pct_change", Value: entries[i].pctChange},
				{Key: "volume", Value: float64(entries[i].volume)},
			},
		}
	}
	return result
}
