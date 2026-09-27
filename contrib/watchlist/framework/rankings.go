package framework

import (
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework/session"
)

// RankedList is one published watchlist: a strategy's ranking under one
// basis for one session window.
type RankedList struct {
	// Name is the published name: the strategy name, plus
	// TraditionalSuffix for the traditional basis.
	Name    string
	Basis   Basis
	Window  session.Window
	Symbols []RankedSymbol
}

// rankAll runs every strategy for every basis available in window w's
// session over curated, and returns the published lists. It is the single
// ranking step for both live rankings and rewinds, so the two cannot drift
// apart.
//
// curated maps symbols to states positioned on w's trading date; each
// strategy sees per-basis views of them, never the states themselves.
// Strategies must be used by one caller at a time (they keep scratch state).
func rankAll(strategies []WatchlistStrategy, curated map[string]*SymbolState, w session.Window) []RankedList {
	day := w.TradingDate.Unix()
	type viewKey struct {
		basis      Basis
		needMedian bool
	}
	views := map[viewKey]map[string]*SymbolState{}

	var lists []RankedList
	for _, strategy := range strategies {
		needMedian := usesVolumeMedian(strategy)
		for _, b := range basesOf(strategy) {
			if !b.Available(w.Session) {
				continue
			}
			k := viewKey{b, needMedian}
			view, ok := views[k]
			if !ok {
				view = buildView(curated, day, w.Session, b, needMedian)
				views[k] = view
			}

			var ranking []RankedSymbol
			if cr, ok := strategy.(ContextualRanker); ok {
				ranking = cr.RankAt(RankContext{
					TradingDate: w.TradingDate, Session: w.Session, Basis: b, WindowEnd: w.End,
				}, view)
			} else {
				ranking = strategy.Rank(view)
			}

			for i := range ranking {
				ranking[i].Rank = i + 1
				// Attach the price, prior close and open as fields so both the
				// WebSocket push (flattened) and the REST response (nested
				// under "fields") carry them. They come from the list's view,
				// so prior_close is the list's baseline and open the
				// session's open. Aggregate strategies emit non-symbol rows
				// (e.g. "Industrials") absent from the view; those get none.
				if st, ok := view[ranking[i].Symbol]; ok && st != nil {
					ranking[i].Fields = append(ranking[i].Fields,
						Field{Key: "price", Value: st.LastPrice},
						Field{Key: "prior_close", Value: st.PriorClose},
						Field{Key: "open", Value: st.DayOpen},
						Field{Key: "premarket_volume", Value: float64(st.PremarketVolume)},
					)
				}
			}
			lists = append(lists, RankedList{
				Name: ListName(strategy.Name(), b), Basis: b, Window: w, Symbols: ranking,
			})
		}
	}
	return lists
}

// RunRankings ranks the live curated states for window w, stores the lists
// as the current live rankings (replacing lists not available in w's
// session), and returns them.
//
// Caller contract: must be invoked serially (WatchlistWorker.TriggerRanking
// holds rankingMu around this call). The reusable curated snapshot relies
// on that serialization.
func RunRankings(mgr *SymbolStateManager, w session.Window) []RankedList {
	lists := rankAll(mgr.strategies, mgr.reusableCuratedSnapshot(), w)
	mgr.setLists(lists)
	return lists
}

// DetectCurationChanges compares each symbol's IsCurated vs WasCurated
// and returns the added and removed lists. It also flips WasCurated to
// match IsCurated for the next cycle.
func DetectCurationChanges(mgr *SymbolStateManager) (added, removed []CurationChangeEntry) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	for symbol, state := range mgr.states {
		switch state.syncCuration() {
		case curationAdded:
			added = append(added, CurationChangeEntry{Symbol: symbol, Reason: "meets_criteria"})
		case curationRemoved:
			removed = append(removed, CurationChangeEntry{Symbol: symbol, Reason: "below_criteria"})
		}
	}
	return added, removed
}
