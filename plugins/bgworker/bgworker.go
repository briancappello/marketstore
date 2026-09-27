// Package bgworker provides interface for bgworker plugins.  A bgworker plugin
// has to implement the following function.
// NewBgWorker(config map[string]interface{}) (BgWorker, error)
//
// Background workers run under the marketstore server by implementing the
// interface, started at the very beginning of the server lifecycle before the
// query interface is started, but internal state shuold be fledged. The server
// does not handle panics that happen within the plugin.  A plugin can recover
// from panics, but be careful not to screw the server state if touching
// internal API.  It is often better to just let it go.
//
// Configuration is as follows.
//
//	bgworkers:
//	  - module: xxxWorker.so
//	    name: datafeed
//	    config: <according to the plulgin>
package bgworker

import (
	"errors"
	"fmt"
	"time"
)

// BgWorker is the interface that background worker plugins must implement.
// Run is called in a separate goroutine and should block until the worker
// is done (typically by waiting on a context or channel).
// Shutdown is called during server shutdown to signal the worker to stop.
// Implementations that have no cleanup to perform should provide an explicit
// no-op Shutdown with a comment explaining why.
type BgWorker interface {
	Run()
	Shutdown()
}

// WatchlistRankingField is a single named numeric metric on a watchlist
// entry. It mirrors framework.Field at the plugin/host boundary so the
// framework's typed representation can be passed across the plugin
// boundary without re-introducing map[string]interface{} allocations.
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

// WatchlistDataSource is an optional interface that a BgWorker can implement
// to expose watchlist ranking data to the server's RPC layer. The host checks
// for this interface after loading the plugin and wires it into the frontend.
type WatchlistDataSource interface {
	// ListWatchlistNames returns the names of all available watchlists.
	ListWatchlistNames() []string
	// GetWatchlistRanking returns the current ranking for a named watchlist.
	GetWatchlistRanking(name string) []WatchlistRankingEntry
	// AllWatchlistRankings returns all current watchlist rankings.
	AllWatchlistRankings() map[string][]WatchlistRankingEntry
	// Rankings answers a query for live or rewound rankings. Errors wrap
	// ErrWatchlistInvalid (a bad or unavailable request) or
	// ErrWatchlistNotFound (an unknown list name).
	Rankings(q WatchlistQuery) (WatchlistResult, error)
}

// WatchlistQuery asks for rankings. Empty Session and AsOf mean the live
// session. Empty Names means every list available in the resolved session.
type WatchlistQuery struct {
	Names   []string
	Session string // "premarket", "regular" or "afterhours"
	AsOf    string // ISO-8601 date or date-time
}

// WatchlistList is one list with the ranking window it covers.
type WatchlistList struct {
	Name        string
	Basis       string // "session" or "traditional"
	Session     string
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

// Errors a WatchlistDataSource wraps, so the host can classify failures that
// happen inside a plugin without importing the plugin's packages.
var (
	// ErrWatchlistInvalid: the request is malformed, names a non-trading
	// date or a session that has not started, or asks for a list that is
	// not available in the session.
	ErrWatchlistInvalid = errors.New("invalid watchlist request")
	// ErrWatchlistNotFound: no such watchlist.
	ErrWatchlistNotFound = errors.New("watchlist not found")
)

// SubscriptionController is an optional interface a BgWorker can implement to
// allow the RPC layer to drive live tick subscriptions at runtime. The host
// checks for this interface after loading the plugin and wires it into the
// frontend (mirroring WatchlistDataSource).
type SubscriptionController interface {
	// Subscribe acquires a runtime subscription for the given symbol on each of
	// the named data types ("trades", "quotes"). An unrecognized data type
	// returns an error.
	Subscribe(symbol string, dataTypes []string) error
	// Unsubscribe releases a runtime subscription for the given symbol on each
	// of the named data types.
	Unsubscribe(symbol string, dataTypes []string) error
	// ActiveSubscriptions returns the current intended subscription set as a
	// map of symbol -> data types.
	ActiveSubscriptions() map[string][]string
}

// SessionFactsRebuilder is an optional interface a BgWorker can implement to
// let the RPC layer queue a rebuild of derived session facts. The host checks
// for it after loading the plugin and wires it into the frontend.
type SessionFactsRebuilder interface {
	// QueueSessionFactsRebuild queues every trading day from..to for the
	// given symbols (all symbols with 1Min data when empty) and returns how
	// many (symbol, day) entries were queued. It fails on a replica.
	QueueSessionFactsRebuild(symbols []string, from, to time.Time) (queued int, err error)
}

// SymbolLoader is an interface to retrieve symbol object from plugin.
type SymbolLoader interface {
	LoadSymbol(symbolName string) (interface{}, error)
}

// Load loads new BgWorker instance using loader, and initializes it with config.
func Load(loader SymbolLoader, config map[string]interface{}) (BgWorker, error) {
	symbolName := "NewBgWorker"
	sym, err := loader.LoadSymbol(symbolName)
	if err != nil {
		return nil, fmt.Errorf("unable to load %s", symbolName)
	}

	newFunc, ok := sym.(func(map[string]interface{}) (BgWorker, error))
	if !ok {
		return nil, fmt.Errorf("%s does not comply function spec", symbolName)
	}
	return newFunc(config)
}
