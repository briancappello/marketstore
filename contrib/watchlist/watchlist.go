// Package main is the Go plugin entry point for the watchlist trigger and
// background worker. It registers the default (no-op) curator and four
// generic watchlist strategies, then delegates to the framework.
//
// To use custom curation/watchlist logic, create a separate repository
// that imports the framework package, registers custom implementations in
// init(), and compiles its own watchlist.so to replace this default one.
package main

import (
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/defaults"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
)

func init() {
	// Register the default no-op curator (all symbols pass curation).
	framework.RegisterCurator(defaults.NewNoopCurator)

	// Register the four default watchlist strategies.
	framework.RegisterWatchlist("PCT_CHANGE_UP", defaults.NewPctChangeUp)
	framework.RegisterWatchlist("PCT_CHANGE_DOWN", defaults.NewPctChangeDown)
	framework.RegisterWatchlist("VOLUME_UP", defaults.NewVolumeUp)
	framework.RegisterWatchlist("VOLUME_DOWN", defaults.NewVolumeDown)
}

// NewTrigger is the exported symbol loaded by MarketStore's plugin system.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	return framework.NewTrigger(conf)
}

// NewBgWorker is the exported symbol loaded by MarketStore's plugin system.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	return framework.NewBgWorker(conf)
}

func main() {}
