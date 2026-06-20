package framework

// RunRankings executes all registered watchlist strategies against the
// current curated symbol states, updates the state manager with the results,
// and returns the rankings keyed by watchlist name.
//
// Caller contract: must be invoked serially (WatchlistWorker.TriggerRanking
// holds rankingMu around this call). The reusable curated snapshot relies
// on that serialization. Strategies must not retain a reference to the
// curated map beyond the call.
func RunRankings(mgr *SymbolStateManager) map[string][]RankedSymbol {
	curated := mgr.reusableCuratedSnapshot()

	results := make(map[string][]RankedSymbol, len(mgr.strategies))
	for _, strategy := range mgr.strategies {
		ranking := strategy.Rank(curated)
		// Ensure rank numbers are set.
		for i := range ranking {
			ranking[i].Rank = i + 1
		}
		mgr.SetWatchlistRanking(strategy.Name(), ranking)
		results[strategy.Name()] = ranking
	}
	return results
}

// DetectCurationChanges compares each symbol's IsCurated vs WasCurated
// and returns the added and removed lists. It also flips WasCurated to
// match IsCurated for the next cycle.
func DetectCurationChanges(mgr *SymbolStateManager) (added, removed []CurationChangeEntry) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	for symbol, state := range mgr.states {
		if state.IsCurated && !state.WasCurated {
			added = append(added, CurationChangeEntry{Symbol: symbol, Reason: "meets_criteria"})
			state.WasCurated = true
		} else if !state.IsCurated && state.WasCurated {
			removed = append(removed, CurationChangeEntry{Symbol: symbol, Reason: "below_criteria"})
			state.WasCurated = false
		}
	}
	return added, removed
}
