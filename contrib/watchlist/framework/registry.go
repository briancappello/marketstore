package framework

import "sync"

// CuratorFactory creates a Curator from a config map (the "curation" block
// in mkts.yml).
type CuratorFactory func(config map[string]interface{}) (Curator, error)

// WatchlistFactory creates a WatchlistStrategy from a config map (one entry
// in the "watchlists" array in mkts.yml).
type WatchlistFactory func(config map[string]interface{}) (WatchlistStrategy, error)

var (
	registryMu         sync.RWMutex
	curatorFactory     CuratorFactory
	watchlistFactories = map[string]WatchlistFactory{}
)

// RegisterCurator sets the factory used to create the Curator instance.
// Calling this more than once replaces the previous factory.
// Typically called from an init() function in the plugin's package main.
func RegisterCurator(factory CuratorFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	curatorFactory = factory
}

// RegisterWatchlist registers a named WatchlistStrategy factory.
// The name should match the watchlist's Name() return value and is used
// for lookup when mkts.yml references a watchlist by name.
// Typically called from an init() function in the plugin's package main.
func RegisterWatchlist(name string, factory WatchlistFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	watchlistFactories[name] = factory
}

// GetCuratorFactory returns the registered CuratorFactory, or nil if none.
func GetCuratorFactory() CuratorFactory {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return curatorFactory
}

// GetWatchlistFactory returns the registered WatchlistFactory for the given name.
func GetWatchlistFactory(name string) (WatchlistFactory, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := watchlistFactories[name]
	return f, ok
}

// GetAllWatchlistFactories returns a copy of all registered watchlist factories.
func GetAllWatchlistFactories() map[string]WatchlistFactory {
	registryMu.RLock()
	defer registryMu.RUnlock()
	cp := make(map[string]WatchlistFactory, len(watchlistFactories))
	for k, v := range watchlistFactories {
		cp[k] = v
	}
	return cp
}

// ResetRegistry clears all registered factories. Intended for testing only.
func ResetRegistry() {
	registryMu.Lock()
	defer registryMu.Unlock()
	curatorFactory = nil
	watchlistFactories = map[string]WatchlistFactory{}
}
