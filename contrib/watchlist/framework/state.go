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

// SymbolCount returns the total number of tracked symbols.
func (m *SymbolStateManager) SymbolCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.states)
}
