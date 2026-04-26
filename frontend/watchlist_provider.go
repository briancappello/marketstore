package frontend

import "sync"

// WatchlistProvider is an optional interface that plugins can implement
// to expose watchlist data via the RPC layer. The watchlist BgWorker
// registers an adapter during startup so that ListWatchlists RPC calls
// can read ranking data without a compile-time dependency on the plugin.
type WatchlistProvider interface {
	// ListNames returns the names of all configured watchlists.
	ListNames() []string
	// GetRanking returns the current ranking for a named watchlist.
	// Returns nil if the watchlist does not exist.
	GetRanking(name string) []WatchlistRankingEntry
	// AllRankings returns the current rankings for all watchlists.
	AllRankings() map[string][]WatchlistRankingEntry
}

// WatchlistRankingEntry is a single ranked symbol in a watchlist.
type WatchlistRankingEntry struct {
	Symbol string
	Rank   int
	Fields map[string]interface{}
}

var (
	watchlistProviderMu sync.RWMutex
	watchlistProvider   WatchlistProvider
)

// RegisterWatchlistProvider registers a WatchlistProvider for the RPC layer.
// Typically called by the watchlist BgWorker during startup.
func RegisterWatchlistProvider(p WatchlistProvider) {
	watchlistProviderMu.Lock()
	defer watchlistProviderMu.Unlock()
	watchlistProvider = p
}

// GetWatchlistProvider returns the registered WatchlistProvider, or nil
// if no provider has been registered.
func GetWatchlistProvider() WatchlistProvider {
	watchlistProviderMu.RLock()
	defer watchlistProviderMu.RUnlock()
	return watchlistProvider
}
