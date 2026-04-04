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

import "fmt"

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
