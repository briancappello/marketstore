package frontend

import "sync"

// SubscriptionController is an optional interface a plugin can implement to
// drive live tick subscriptions at runtime. The Massive BgWorker registers an
// adapter during startup so that Subscribe RPC calls can mutate subscriptions
// without a compile-time dependency on the plugin (mirrors WatchlistProvider).
type SubscriptionController interface {
	// Subscribe acquires a runtime subscription for the given symbol on each of
	// the named data types ("trades", "quotes").
	Subscribe(symbol string, dataTypes []string) error
	// Unsubscribe releases a runtime subscription for the given symbol on each
	// of the named data types.
	Unsubscribe(symbol string, dataTypes []string) error
	// ActiveSubscriptions returns the current intended subscription set as a
	// map of symbol -> data types.
	ActiveSubscriptions() map[string][]string
}

var (
	subscriptionControllerMu sync.RWMutex
	subscriptionController   SubscriptionController
)

// RegisterSubscriptionController registers a SubscriptionController for the RPC
// layer. Typically called by a BgWorker during startup.
func RegisterSubscriptionController(c SubscriptionController) {
	subscriptionControllerMu.Lock()
	defer subscriptionControllerMu.Unlock()
	subscriptionController = c
}

// GetSubscriptionController returns the registered SubscriptionController, or
// nil if none has been registered.
func GetSubscriptionController() SubscriptionController {
	subscriptionControllerMu.RLock()
	defer subscriptionControllerMu.RUnlock()
	return subscriptionController
}
