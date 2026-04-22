package defaults

import (
	"sort"

	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework"
)

// --- VOLUME_UP: Top N by raw volume among gainers ---

// VolumeUp ranks curated symbols by cumulative volume (descending),
// filtered to positive percent change only.
type VolumeUp struct {
	limit int
}

// NewVolumeUp creates a new VolumeUp strategy.
func NewVolumeUp(config map[string]interface{}) (framework.WatchlistStrategy, error) {
	limit := 100
	if v, ok := config["limit"]; ok {
		if f, ok := v.(float64); ok {
			limit = int(f)
		}
	}
	return &VolumeUp{limit: limit}, nil
}

func (s *VolumeUp) Name() string { return "VOLUME_UP" }

func (s *VolumeUp) Configure(config map[string]interface{}) error { return nil }

func (s *VolumeUp) Rank(curated map[string]*framework.SymbolState) []framework.RankedSymbol {
	type entry struct {
		symbol    string
		volume    int64
		pctChange float64
	}

	var entries []entry
	for sym, state := range curated {
		if state.PctChange > 0 {
			entries = append(entries, entry{sym, state.CumulativeVolume, state.PctChange})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].volume > entries[j].volume
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
			Fields: map[string]interface{}{
				"volume":     entries[i].volume,
				"pct_change": entries[i].pctChange,
			},
		}
	}
	return result
}

// --- VOLUME_DOWN: Top N by raw volume among losers ---

// VolumeDown ranks curated symbols by cumulative volume (descending),
// filtered to negative percent change only.
type VolumeDown struct {
	limit int
}

// NewVolumeDown creates a new VolumeDown strategy.
func NewVolumeDown(config map[string]interface{}) (framework.WatchlistStrategy, error) {
	limit := 100
	if v, ok := config["limit"]; ok {
		if f, ok := v.(float64); ok {
			limit = int(f)
		}
	}
	return &VolumeDown{limit: limit}, nil
}

func (s *VolumeDown) Name() string { return "VOLUME_DOWN" }

func (s *VolumeDown) Configure(config map[string]interface{}) error { return nil }

func (s *VolumeDown) Rank(curated map[string]*framework.SymbolState) []framework.RankedSymbol {
	type entry struct {
		symbol    string
		volume    int64
		pctChange float64
	}

	var entries []entry
	for sym, state := range curated {
		if state.PctChange < 0 {
			entries = append(entries, entry{sym, state.CumulativeVolume, state.PctChange})
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].volume > entries[j].volume
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
			Fields: map[string]interface{}{
				"volume":     entries[i].volume,
				"pct_change": entries[i].pctChange,
			},
		}
	}
	return result
}
