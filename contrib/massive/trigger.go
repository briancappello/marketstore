package main

import (
	"strings"

	"github.com/alpacahq/marketstore/v4/contrib/massive/subscription"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// SubscriptionTrigger is an example trigger (Approach A) that flips dynamic
// tick subscriptions on/off in response to bar writes. It fires on a bar key
// (e.g. "*/1Sec/OHLCV"), derives the symbol, and asks an injected Signal
// whether to enter or exit. On entry it Acquires trades+quotes for the symbol;
// on exit it Releases them. It drives the SAME subscription.Manager the
// bgworker owns via the package singleton subscription.Default — both live in
// this plugin's .so, so the in-process reference is safe.
//
// The bundled Signal is a no-op example; real strategies replace it. Detecting
// signals from bar records is the strategy's job, out of scope for this plugin.
type SubscriptionTrigger struct {
	signal Signal
}

// Signal decides, from a symbol's freshly-written bar records, whether to begin
// or end high-fidelity tick streaming for that symbol.
type Signal interface {
	EntryDetected(symbol string, records []trigger.Record) bool
	ExitDetected(symbol string, records []trigger.Record) bool
}

// noopSignal never signals entry or exit. It is the default so the bundled
// trigger is inert until a real Signal is wired in.
type noopSignal struct{}

func (noopSignal) EntryDetected(string, []trigger.Record) bool { return false }
func (noopSignal) ExitDetected(string, []trigger.Record) bool  { return false }

// NewTrigger is the trigger plugin factory. It lives in the same main package
// as NewBgWorker so both are exported from massive.so.
// nolint:deadcode // plugin interface
func NewTrigger(_ map[string]interface{}) (trigger.Trigger, error) {
	return &SubscriptionTrigger{signal: noopSignal{}}, nil
}

// Fire is invoked after bars are written for the matched key. It only signals
// intent through the manager; it never touches the ws.Client (the bgworker's
// control loop owns the connection).
func (t *SubscriptionTrigger) Fire(keyPath string, records []trigger.Record) {
	mgr := subscription.Default
	if mgr == nil {
		// dynamic_ticks is not enabled (no manager); nothing to drive.
		return
	}

	sym := symbolFromKeyPath(keyPath)
	if sym == "" {
		return
	}

	if t.signal.EntryDetected(sym, records) {
		if _, err := mgr.Acquire(sym, subscription.Trades); err != nil {
			log.Warn("[massive/trigger] acquire trades for %s failed: %v", sym, err)
		}
		if _, err := mgr.Acquire(sym, subscription.Quotes); err != nil {
			log.Warn("[massive/trigger] acquire quotes for %s failed: %v", sym, err)
		}
	}
	if t.signal.ExitDetected(sym, records) {
		mgr.Release(sym, subscription.Trades)
		mgr.Release(sym, subscription.Quotes)
	}
}

// symbolFromKeyPath extracts the symbol from a key path like "AAPL/1Sec/OHLCV".
func symbolFromKeyPath(keyPath string) string {
	idx := strings.IndexByte(keyPath, '/')
	if idx <= 0 {
		return ""
	}
	return keyPath[:idx]
}
