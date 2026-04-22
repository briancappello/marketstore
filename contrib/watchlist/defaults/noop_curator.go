package defaults

import (
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework"
)

// NoopCurator is a Curator that accepts all symbols into the curated universe.
// It is the default curator shipped with the watchlist plugin.
type NoopCurator struct{}

// NewNoopCurator creates a new NoopCurator.
func NewNoopCurator(config map[string]interface{}) (framework.Curator, error) {
	return &NoopCurator{}, nil
}

// Init is a no-op.
func (c *NoopCurator) Init(states map[string]*framework.SymbolState) {}

// Evaluate always returns true — all symbols pass curation.
func (c *NoopCurator) Evaluate(symbol string, state *framework.SymbolState) bool {
	return true
}
