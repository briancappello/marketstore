package frontend

import (
	"errors"
	"sync"
	"time"
)

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
	// Rankings answers a live or rewind query. Errors wrap
	// ErrWatchlistInvalid or ErrWatchlistNotFound.
	Rankings(q WatchlistQuery) (WatchlistResult, error)
}

// WatchlistQuery asks for rankings. Empty Session and AsOf mean the live
// session; empty Names means every list available in the resolved session.
type WatchlistQuery struct {
	Names   []string
	Session string
	AsOf    string
}

// WatchlistList is one list with the ranking window it covers.
type WatchlistList struct {
	Name        string
	Basis       string // "session" or "traditional"
	Session     string // "premarket", "regular" or "afterhours"
	TradingDate string // YYYY-MM-DD, America/New_York
	WindowStart time.Time
	WindowEnd   time.Time
	Complete    bool
	Entries     []WatchlistRankingEntry
}

// WatchlistResult answers a WatchlistQuery.
type WatchlistResult struct {
	Lists []WatchlistList
}

var (
	// ErrWatchlistInvalid is a client error: a malformed session or as_of, a
	// non-trading date, a session not started, or a list not available in
	// the session. REST maps it to 400, gRPC to InvalidArgument.
	ErrWatchlistInvalid = errors.New("invalid watchlist request")
	// ErrWatchlistNotFound: no such watchlist. REST 404, gRPC NotFound.
	ErrWatchlistNotFound = errors.New("watchlist not found")
)

// WatchlistRankingField is a single named numeric metric on a watchlist
// ranking entry. Mirrors framework.Field at the frontend layer.
type WatchlistRankingField struct {
	Key   string
	Value float64
}

// WatchlistRankingEntry is a single ranked symbol in a watchlist.
type WatchlistRankingEntry struct {
	Symbol string
	Rank   int
	Fields []WatchlistRankingField
	// Sector is an optional non-numeric label used by aggregate strategies.
	Sector string
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
