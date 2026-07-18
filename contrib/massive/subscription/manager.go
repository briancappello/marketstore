// Package subscription provides the shared source of truth for dynamic,
// ref-counted per-symbol tick (trades/quotes) subscriptions in the Massive
// plugin. It is its own package (importing neither the ws client nor the plugin
// root) so the bgworker and an in-process trigger can both depend on it without
// import cycles.
package subscription

import (
	"fmt"
	"sync"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// DataType identifies a tick data stream that can be dynamically subscribed.
type DataType uint8

const (
	Trades DataType = iota
	Quotes
)

// String returns the canonical config/wire data-type name ("trades"/"quotes").
// This is the single definition of the DataType<->name mapping; the ws.Topic
// mapping lives in the plugin layer (the subscription package must not import
// ws so the trigger can import it without cycles).
func (d DataType) String() string {
	switch d {
	case Trades:
		return "trades"
	case Quotes:
		return "quotes"
	default:
		return "unknown"
	}
}

// ParseDataType maps a config/wire name to a DataType. ok=false for anything
// other than "trades"/"quotes".
func ParseDataType(s string) (DataType, bool) {
	switch s {
	case "trades":
		return Trades, true
	case "quotes":
		return Quotes, true
	default:
		return 0, false
	}
}

// AllDataTypes is the set of supported tick data types, useful for iteration.
var AllDataTypes = []DataType{Trades, Quotes}

// Change is emitted when a symbol's effective subscription state flips.
type Change struct {
	Symbol   string
	DataType DataType
	Active   bool // true = now subscribed, false = now unsubscribed
}

// Default is the package-level singleton set by the bgworker in NewBgWorker so
// an in-process trigger (Approach A) can reach the same Manager the bgworker
// owns. Mirrors contrib/watchlist/framework.Manager.
var Default *Manager

// Manager is the ref-counted source of truth for dynamic tick subscriptions.
// It records desired (intent) state only: Acquire/Release update refcounts and
// enqueue Change events; the actual upstream subscribe/unsubscribe is performed
// asynchronously by the bgworker's control goroutine.
type Manager struct {
	mu sync.Mutex
	// refs[dataType][symbol] = number of holders. >0 means subscribed.
	refs map[DataType]map[string]int
	// ch carries Change events for ALL data types to the SINGLE control
	// goroutine. It must have exactly one consumer — a Go channel delivers each
	// value to only one receiver, so multiple consumers would steal each
	// other's events.
	ch chan Change
	// chTaken guards against multiple consumers of ch (misuse).
	chTaken bool

	maxSymbols int
}

const changeChanBuffer = 1024

// New creates a Manager with a per-DataType symbol cap. A maxSymbols <= 0 means
// no cap.
func New(maxSymbols int) *Manager {
	return &Manager{
		refs: map[DataType]map[string]int{
			Trades: {},
			Quotes: {},
		},
		ch:         make(chan Change, changeChanBuffer),
		maxSymbols: maxSymbols,
	}
}

// Changes returns the single shared channel of Change events. It MUST NOT be
// consumed by more than one goroutine: a Go channel delivers each value to
// exactly one receiver, so a second consumer would steal events. Calling
// Changes more than once panics to guard against this misuse.
func (m *Manager) Changes() <-chan Change {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.chTaken {
		panic("subscription: Changes() called more than once; the change channel must have a single consumer")
	}
	m.chTaken = true
	return m.ch
}

// Acquire increments the refcount for sym/dt; it emits an Active=true Change
// only on the 0->1 transition. Returns the new refcount. A successful return
// means the desired state has been recorded (and a Change enqueued) — it does
// NOT mean the upstream subscribe frame has been sent or acked.
//
// The per-DataType cap (maxSymbols) is checked against the number of distinct
// symbols with refcount > 0 within dt; a bump on an already-present symbol never
// trips the cap.
func (m *Manager) Acquire(sym string, dt DataType) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	set := m.refs[dt]
	if set == nil {
		set = map[string]int{}
		m.refs[dt] = set
	}

	cur := set[sym]
	if cur == 0 {
		if m.maxSymbols > 0 && len(set) >= m.maxSymbols {
			return 0, fmt.Errorf("subscription cap reached for %s: %d symbols (max %d)",
				dt, len(set), m.maxSymbols)
		}
	}

	set[sym] = cur + 1
	if cur == 0 {
		m.emit(Change{Symbol: sym, DataType: dt, Active: true})
	}
	return set[sym], nil
}

// Release decrements the refcount for sym/dt; it emits an Active=false Change
// only on the 1->0 transition. Returns the new refcount (0 if it was already
// absent). Same intent-only semantics as Acquire.
func (m *Manager) Release(sym string, dt DataType) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	set := m.refs[dt]
	if set == nil {
		return 0
	}
	cur := set[sym]
	if cur == 0 {
		return 0
	}
	if cur == 1 {
		delete(set, sym)
		m.emit(Change{Symbol: sym, DataType: dt, Active: false})
		return 0
	}
	set[sym] = cur - 1
	return set[sym]
}

// Active returns a snapshot of currently-subscribed symbols for a data type
// (used to replay after reconnect and to answer status queries).
func (m *Manager) Active(dt DataType) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.refs[dt]
	out := make([]string, 0, len(set))
	for sym := range set {
		out = append(out, sym)
	}
	return out
}

// emit sends a Change without blocking. If the buffer is full (control loop
// stalled), it logs and drops the edge event — the control loop's periodic
// reconcile against Active() recovers the state, so nothing is permanently lost.
// Caller must hold m.mu.
func (m *Manager) emit(c Change) {
	select {
	case m.ch <- c:
	default:
		log.Warn("[massive/subscription] change channel full, dropping edge event for %s/%s (reconcile will recover)",
			c.Symbol, c.DataType)
	}
}
