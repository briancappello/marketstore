// Package framework provides the extensible watchlist/curation plugin framework
// for MarketStore. It defines interfaces for custom Curator and WatchlistStrategy
// implementations, manages per-symbol state, and handles the trigger/bgworker
// lifecycle.
//
// The framework ships with default (no-op) implementations. Custom implementations
// can be registered via RegisterCurator and RegisterWatchlist, typically from a
// separate repository that compiles its own watchlist.so plugin.
package framework

// Curator decides whether a symbol belongs in the curated universe.
// The curated universe represents the set of symbols considered "safe to
// trade algorithmically" — i.e., having sufficient liquidity and data quality.
//
// Implementations must be safe for concurrent calls across different symbols.
type Curator interface {
	// Init is called once after baselines are computed for all symbols.
	// It receives read-only access to the full state map.
	Init(states map[string]*SymbolState)

	// Evaluate returns true if the symbol should be part of the curated
	// universe given its current state. Called on every tick for every symbol.
	Evaluate(symbol string, state *SymbolState) bool
}

// WatchlistStrategy defines the filtering and ranking logic for a single
// watchlist. Each strategy produces an ordered list of symbols from the
// curated universe.
type WatchlistStrategy interface {
	// Name returns the watchlist identifier (e.g., "TOP_GAINERS").
	// This is used as the third segment of the stream key:
	// WATCHLISTS/{TimeFrame}/{Name}.
	Name() string

	// Configure is called once at startup with the watchlist's config
	// block from mkts.yml.
	Configure(config map[string]interface{}) error

	// Rank takes a snapshot of all curated symbol states and returns
	// an ordered list of ranked symbols for this watchlist.
	// Called periodically by the ranking goroutine.
	Rank(curated map[string]*SymbolState) []RankedSymbol
}

// RankedSymbol is a single entry in a watchlist ranking result.
type RankedSymbol struct {
	Symbol string
	Rank   int
	// Fields holds computed metrics for this symbol in this watchlist.
	// Keys and values depend on the WatchlistStrategy implementation
	// (e.g., "pct_change", "volume", "volume_multiple_of_median").
	Fields map[string]interface{}
}
