package framework

import "sync"

// Manager is the package-level singleton shared between the BgWorker and
// Trigger. It is initialized by the BgWorker at startup.
var Manager *SymbolStateManager

// SymbolStateManager is the thread-safe container for all per-symbol state.
// It is shared between the BgWorker (which precomputes baselines and runs
// the ranking goroutine) and the Trigger (which updates running state on
// every tick).
type SymbolStateManager struct {
	mu     sync.RWMutex
	states map[string]*SymbolState

	// curated tracks the set of currently curated symbols for efficient
	// membership checks and change detection.
	curatedMu sync.RWMutex
	curated   map[string]struct{}

	// watchlists stores the latest ranking results per watchlist name.
	watchlistsMu sync.RWMutex
	watchlists   map[string][]RankedSymbol

	// curator is the active Curator implementation.
	curator Curator

	// strategies is the list of active WatchlistStrategy implementations.
	strategies []WatchlistStrategy

	// curatedSnapshot is a reusable map populated by CuratedStatesInto.
	// It is safe to reuse because RunRankings holds the worker's
	// rankingMu while the snapshot is in use, ensuring only one
	// outstanding caller. Lazily allocated on first call.
	curatedSnapshot map[string]*SymbolState
}

// NewSymbolStateManager creates a new empty state manager.
func NewSymbolStateManager() *SymbolStateManager {
	return &SymbolStateManager{
		states:     make(map[string]*SymbolState),
		curated:    make(map[string]struct{}),
		watchlists: make(map[string][]RankedSymbol),
	}
}

// SetCurator sets the active Curator implementation.
func (m *SymbolStateManager) SetCurator(c Curator) {
	m.curator = c
}

// AddStrategy adds a WatchlistStrategy to the active set.
func (m *SymbolStateManager) AddStrategy(s WatchlistStrategy) {
	m.strategies = append(m.strategies, s)
}

// GetOrCreate returns the SymbolState for the given symbol, creating it
// if it doesn't exist. Thread-safe.
func (m *SymbolStateManager) GetOrCreate(symbol string) *SymbolState {
	m.mu.RLock()
	s, ok := m.states[symbol]
	m.mu.RUnlock()
	if ok {
		return s
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Double-check after acquiring write lock.
	if s, ok = m.states[symbol]; ok {
		return s
	}
	s = NewSymbolState()
	m.states[symbol] = s
	return s
}

// Get returns the SymbolState for the given symbol, or nil if not found.
func (m *SymbolStateManager) Get(symbol string) *SymbolState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.states[symbol]
}

// AllStates returns a shallow copy of all symbol states. The SymbolState
// pointers are shared (not cloned), so callers must not modify them.
func (m *SymbolStateManager) AllStates() map[string]*SymbolState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make(map[string]*SymbolState, len(m.states))
	for k, v := range m.states {
		cp[k] = v
	}
	return cp
}

// CuratedStates returns a snapshot of only the curated symbol states.
//
// The returned map is freshly allocated. Most callers in performance-
// sensitive paths should prefer reusableCuratedSnapshot, which reuses a
// per-Manager scratch map across cycles.
func (m *SymbolStateManager) CuratedStates() map[string]*SymbolState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.curatedMu.RLock()
	defer m.curatedMu.RUnlock()

	cp := make(map[string]*SymbolState, len(m.curated))
	for sym := range m.curated {
		if s, ok := m.states[sym]; ok {
			cp[sym] = s
		}
	}
	return cp
}

// reusableCuratedSnapshot returns the manager's reusable snapshot map,
// repopulated with the current curated states. The returned map MUST be
// treated as read-only and the reference MUST NOT be retained beyond the
// current ranking cycle: the next call clears and refills it.
//
// This is safe only because RunRankings is invoked serially by
// WatchlistWorker.TriggerRanking under rankingMu. If callers ever
// parallelize, switch back to CuratedStates which allocates a fresh map.
//
// Eliminating the per-cycle map allocation matters because
// N_curated × pointer-size × load-factor approaches several hundred KB
// every cycle, and the previous map's bucket array becomes garbage
// immediately on the next cycle.
func (m *SymbolStateManager) reusableCuratedSnapshot() map[string]*SymbolState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.curatedMu.RLock()
	defer m.curatedMu.RUnlock()

	if m.curatedSnapshot == nil {
		m.curatedSnapshot = make(map[string]*SymbolState, len(m.curated))
	} else {
		for k := range m.curatedSnapshot {
			delete(m.curatedSnapshot, k)
		}
	}
	for sym := range m.curated {
		if s, ok := m.states[sym]; ok {
			m.curatedSnapshot[sym] = s
		}
	}
	return m.curatedSnapshot
}

// IsCurated returns whether a symbol is currently in the curated set.
func (m *SymbolStateManager) IsCurated(symbol string) bool {
	m.curatedMu.RLock()
	defer m.curatedMu.RUnlock()
	_, ok := m.curated[symbol]
	return ok
}

// UpdateCuration sets or clears a symbol's curation status and returns
// whether the status changed.
func (m *SymbolStateManager) UpdateCuration(symbol string, curated bool) (changed bool) {
	m.curatedMu.Lock()
	defer m.curatedMu.Unlock()

	_, wasCurated := m.curated[symbol]
	if curated && !wasCurated {
		m.curated[symbol] = struct{}{}
		return true
	}
	if !curated && wasCurated {
		delete(m.curated, symbol)
		return true
	}
	return false
}

// CuratedCount returns the number of currently curated symbols.
func (m *SymbolStateManager) CuratedCount() int {
	m.curatedMu.RLock()
	defer m.curatedMu.RUnlock()
	return len(m.curated)
}

// SetWatchlistRanking stores the latest ranking for a named watchlist.
func (m *SymbolStateManager) SetWatchlistRanking(name string, ranking []RankedSymbol) {
	m.watchlistsMu.Lock()
	defer m.watchlistsMu.Unlock()
	m.watchlists[name] = ranking
}

// GetWatchlistRanking returns the latest ranking for a named watchlist.
func (m *SymbolStateManager) GetWatchlistRanking(name string) []RankedSymbol {
	m.watchlistsMu.RLock()
	defer m.watchlistsMu.RUnlock()
	return m.watchlists[name]
}

// ListWatchlistNames returns the names of all watchlists that have rankings.
func (m *SymbolStateManager) ListWatchlistNames() []string {
	m.watchlistsMu.RLock()
	defer m.watchlistsMu.RUnlock()
	names := make([]string, 0, len(m.watchlists))
	for name := range m.watchlists {
		names = append(names, name)
	}
	return names
}

// AllWatchlistRankings returns a snapshot of all current watchlist rankings.
func (m *SymbolStateManager) AllWatchlistRankings() map[string][]RankedSymbol {
	m.watchlistsMu.RLock()
	defer m.watchlistsMu.RUnlock()
	cp := make(map[string][]RankedSymbol, len(m.watchlists))
	for name, ranking := range m.watchlists {
		dst := make([]RankedSymbol, len(ranking))
		copy(dst, ranking)
		cp[name] = dst
	}
	return cp
}

// SymbolCount returns the total number of tracked symbols.
func (m *SymbolStateManager) SymbolCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.states)
}
