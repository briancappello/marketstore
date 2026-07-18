# Design: Dynamic Per-Symbol Trade/Quote Subscriptions

Status: Proposed
Scope: Add the ability to **subscribe/unsubscribe individual symbols to the live
trades/quotes WebSocket feed at runtime**, driven by (A) an in-process trigger
and (B) an external RPC/HTTP control API. Approach **B is the primary** use case;
A is also supported. Built on a new **true live subscribe/unsubscribe** path in
the `ws` client.

---

## 1. Goal & Use Case

Algorithmic strategies run against `1Sec`/`1Min` OHLCV. When a strategy detects
entry interest in a symbol, it wants to **turn on** high-fidelity `trades` +
`quotes` streaming for that symbol; when the position is exited, it wants to
**turn off** that symbol's tick subscription. Symbols come and go continuously
without restarting the server.

This is fundamentally different from today's model, where the WebSocket symbol
set is fixed at startup (`massive.go:218`), baked into the handshake
(`ws/client.go:219-229`), and only changeable by a full reconnect at the next
pre-market open or a process restart. There is no `unsubscribe` and no runtime
control surface.

**Requirements:**
1. Subscribe/unsubscribe a symbol's `trades` and/or `quotes` stream while the
   process runs, with no reconnect.
2. Primary driver: **external** caller (Python/other service) via RPC/HTTP
   (Approach B).
3. Secondary driver: **in-process trigger** firing on bar writes (Approach A).
4. Ref-counted: multiple strategies may want the same symbol; the stream stays
   up until the last interested party releases it.
5. Survive reconnects: the **current active set is the source of truth**, not
   config; after a dropped connection the worker re-subscribes the active set.

**Explicitly out of scope (this doc):** the strategy/signal logic itself
(Approach A's trigger only provides the wiring + a trivial example); historical
backfill of the pre-subscription gap (already available via the flat-file/REST
backfillers and can be invoked separately).

---

## 2. Current State (verified)

- **`ws.Client` is static & single-goroutine.** `Subscribe()` only appends to
  `c.subs` and "must be called before Connect()" (`ws/client.go:164-171`). The
  handshake sends one `subscribe` per registered sub (`client.go:219-229`).
  `writeLoop` only sends pings (`client.go:431-446`). There is **no
  `unsubscribe` action** (`client.go:112-115`) and **no mutex** on `c.subs`
  (`client.go:137`); the struct doc says "not safe for concurrent use"
  (`client.go:129-131`).
- **One connection per data type.** `startStreaming` dials a separate connection
  per `wsDataTypes` entry, each bound to a single handler
  (`massive.go:211-248`).
- **Symbol set is immutable after `NewBgWorker`.** `mf.config.Symbols` is read
  once and reused even across reconnects (`massive.go:218`, `272-360`).
- **Existing runtime-control precedents (templates we will reuse):**
  - `WatchlistDataSource` optional interface (`plugins/bgworker/bgworker.go:51-61`),
    type-asserted and registered at host startup
    (`cmd/start/plugins.go:28-31`) into a frontend provider
    (`frontend/watchlist_provider.go`). This is a **read** path
    (`DataService.ListWatchlists` `frontend/list_watchlists.go:42`,
    `GRPCService.ListWatchlists` `frontend/grpc.go:462`). We extend the same
    shape into a **write/command** path.
  - Trigger ↔ bgworker shared singleton with locking
    (`contrib/watchlist/framework/state.go:5-37`). This is the template for
    Approach A.
- **Handlers already correct.** `TradeHandler`/`QuoteHandler` write
  `{sym}/1Sec/TRADE` and `{sym}/1Sec/QUOTE` and the schema/backfill work is done,
  so once a symbol's stream turns on, data lands correctly with no extra work.

---

## 3. Architecture Overview

```
 (B) External client ──RPC/HTTP──┐
                                  ▼
                       frontend SubscriptionController provider (registered at startup)
                                  │  (in host process)
                                  ▼
 (A) massive trigger ──►  SubscriptionManager  ◄── massive bgworker owns it
     (Fire on bar write)   (ref-counted set,        (initialized in NewBgWorker)
                            change channel,
                            mutex-guarded)
                                  │ change events
                                  ▼
                       per-data-type subscription control goroutine
                                  │
                                  ▼
                       ws.Client.Subscribe/Unsubscribe(live)  ◄── NEW wire path
                                  │
                                  ▼
                       Massive WS: {"action":"subscribe"|"unsubscribe","params":"T.AAPL,Q.AAPL"}
```

The **`SubscriptionManager`** is the single source of truth, owned by the
bgworker. Both A and B feed it. The bgworker's per-connection control goroutine
consumes change events and drives the live `ws.Client` subscribe/unsubscribe.

---

## 4. Component 1 — Live subscribe/unsubscribe in `ws.Client`

This is the foundational change; both approaches depend on it.

### 4.1 Wire protocol
Add the unsubscribe action (`ws/client.go:112-115`):
```go
const (
    actionAuth        action = "auth"
    actionSubscribe   action = "subscribe"
    actionUnsubscribe action = "unsubscribe"
)
```
Massive/Polygon accept `{"action":"unsubscribe","params":"T.AAPL,Q.AAPL"}` on a
live connection (same `params` grammar as subscribe, `buildSubParams`
`client.go:338`).

Also add a sentinel error (alongside `ErrAuthFailed`/`ErrConnectionLimit`,
`client.go:53-56`) returned to control callers when the connection is gone:
```go
var errConnClosed = errors.New("connection closed")
```

### 4.2 Make the client concurrency-safe for control writes
The problem: `writeLoop` owns writes to `c.conn` (pings); a live
subscribe/unsubscribe is also a write. Two writers to one gorilla `*Conn` is
unsafe. **Solution: route all control writes through `writeLoop` via a channel**,
so the write side stays single-goroutine.

Add to `Client` (`client.go:131-144`):
```go
type controlReq struct {
    act    action
    params string
    // result receives exactly one value: the result of writing the control
    // frame (nil on success, non-nil on write error). It MUST be created
    // buffered with capacity 1 by the sender (see step 3 below) so that
    // writeLoop's single send never blocks — even if the caller has already
    // abandoned the request (e.g. timed out or saw c.done close). Confirmation
    // of the server's "success" status is handled asynchronously (see §4.4).
    result chan error
}

ctrlCh   chan controlReq   // live control writes, drained by writeLoop
subMu    sync.Mutex        // guards subs (now mutated at runtime)
```
`ctrlCh` is created in `New()` (buffered, e.g. 64). Each `controlReq.result` is
created **per call** in `UpdateSubscription`, always with `make(chan error, 1)`
(buffered, size 1).

### 4.3 New exported methods (post-Connect)
```go
// UpdateSubscription adds or removes tickers for a topic on the LIVE
// connection. Safe to call after Connect() and concurrently. It updates the
// retained subscription set (for reconnect replay) and writes the control
// frame. Returns an error if the connection is closed or the write fails.
func (c *Client) UpdateSubscription(act action, topic Topic, tickers ...string) error
```
Two thin wrappers for clarity:
```go
func (c *Client) AddTickers(topic Topic, tickers ...string) error    // act=subscribe
func (c *Client) RemoveTickers(topic Topic, tickers ...string) error // act=unsubscribe
```
Behavior:
1. Under `subMu`, update the retained set `c.subs` (merge for subscribe, prune
   for unsubscribe) so a later reconnect replays the **current** set.
2. Build `params` via `buildSubParams(topic, tickers)`.
3. Create `result := make(chan error, 1)` (**buffered, size 1** — this is the
   contract that lets `writeLoop` send without blocking, §4.4). Send a
   `controlReq{act, params, result}` on `ctrlCh`, then wait for the outcome,
   selecting on three cases:
   - `err := <-result` → return `err` (nil = frame written successfully).
   - `<-c.done` → connection closed; return a "connection closed" error.
   - `<-time.After(writeWait)` → give up waiting; return a timeout error.

   The send on `ctrlCh` itself also selects on `c.done` (and may use the same
   `writeWait` bound) so a stalled/closed connection cannot block the caller
   indefinitely. **Crucially, because `result` is buffered size 1, it is safe for
   the caller to stop waiting (on `c.done` or timeout) before `writeLoop`
   sends:** `writeLoop`'s `result <- err` still succeeds into the buffer and
   never blocks, even though no one will ever read it. The abandoned channel is
   then garbage-collected. (An unbuffered `result` would deadlock `writeLoop`
   here — hence the size-1 requirement.)

The pre-Connect `Subscribe()` stays as-is for the initial handshake; internally
it appends under `subMu`.

### 4.4 `writeLoop` drains control requests (and fails pending ones on exit)
`writeLoop` (`client.go:431-446`) gains a third `select` case **and a
drain-and-fail on exit**:
```go
func (c *Client) writeLoop() {
    ticker := time.NewTicker(pingPeriod)
    defer ticker.Stop()
    // On exit, fail any control requests still buffered in ctrlCh so their
    // callers never park forever. See §4.5 for why this is required.
    defer c.failPendingControl()

    for {
        select {
        case <-c.done:
            return
        case <-ticker.C:
            c.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                return // ping failure tears down the connection
            }
        case req := <-c.ctrlCh:
            c.conn.SetWriteDeadline(time.Now().Add(writeWait))
            err := c.sendControl(req.act, req.params)
            req.result <- err // never blocks: result is buffered size 1 (§4.3),
                              // so this succeeds even if the caller already gave up
            if err != nil {
                return // write failure tears down like a ping failure
            }
        }
    }
}

// failPendingControl drains ctrlCh non-blockingly and replies to each pending
// request with a "connection closed" error. Because each result is buffered
// size 1, every reply succeeds without blocking. After writeLoop has exited,
// no new requests can be serviced; any that race in afterward are caught by
// UpdateSubscription's own select-on-done/timeout (§4.5).
func (c *Client) failPendingControl() {
    for {
        select {
        case req := <-c.ctrlCh:
            req.result <- errConnClosed
        default:
            return
        }
    }
}
```
**Confirmation handling:** the server replies with a `status:"success"` (or
`status:"error"`) message *asynchronously* on the read side. We do **not** block
the caller on it (the handshake's synchronous `expectStatus` is only for
connect-time). Instead, `handleStreamingStatus` (`client.go:415-428`) already
logs `success`/`error` during streaming; we extend it to log the affected
`params` for observability. A failed subscribe surfaces as: no data arrives +
an `error` status logged. (A future enhancement could correlate request→ack;
deferred — see §9.)

### 4.5 Concurrency & lifecycle notes
- `c.subs` now mutated at runtime → guarded by `subMu` everywhere
  (`Connect` handshake loop, `UpdateSubscription`).
- `ctrlCh` writes happen only from caller goroutines; the single reader is
  `writeLoop`. The size-1 `result` buffer guarantees `writeLoop` never blocks on
  its `result <- err` send regardless of whether the caller is still waiting
  (§4.3).

- **The ctrlCh-send → done-close TOCTOU window (why `writeLoop` must drain on
  exit).** `done` is closed only by `readLoop` exiting (`client.go:367-370`),
  **not** by `writeLoop`. So there is a window where `writeLoop` has already
  returned — e.g. on its own ping-write failure (`client.go:441-442`) or after a
  control-write failure — but `readLoop` has not yet observed the dead connection
  and closed `done`. A caller that *already passed* the `ctrlCh` send (because the
  buffer had space) and is now parked on `<-result` would, in that window, see
  neither a drain of `ctrlCh` (writeLoop is gone) nor a closed `done` — a
  transient hang until `readLoop` finally closes `done`.

  This is closed by **two cooperating layers**:
  1. **`writeLoop` drains and fails pending `ctrlCh` requests on exit**
     (`failPendingControl`, §4.4). Any request already buffered when `writeLoop`
     returns is immediately answered with `errConnClosed`. This is the primary
     fix — it does not depend on `done` being closed yet.
  2. **`UpdateSubscription` selects on `c.done` AND a `writeWait` timeout** while
     both sending to `ctrlCh` and awaiting `result` (§4.3). This bounds any
     residual race (e.g. a request that arrives in `ctrlCh` *after*
     `failPendingControl` has already returned but before a fresh client exists):
     the caller returns a timeout/closed error rather than hanging.

  Layer 1 removes the hang for the common case (request buffered before exit);
  layer 2 is the backstop for the narrow post-drain race. Neither alone is
  sufficient; together they guarantee `UpdateSubscription` always returns.

- Update the struct doc: the client is now safe for **concurrent
  `UpdateSubscription` calls** (serialized via `ctrlCh` + `subMu`); `Output`/
  `Err`/`Done`/`Close` remain as before.

### 4.6 Tests (`ws/client_test.go` — new)
Use a `httptest` WebSocket server (gorilla upgrader) that records frames:
- Subscribe-while-connected sends `{"action":"subscribe","params":"T.AAPL"}`.
- Unsubscribe sends `{"action":"unsubscribe","params":"T.AAPL"}`.
- Concurrent `UpdateSubscription` calls serialize (no interleaved/corrupt
  frames).
- `UpdateSubscription` after `Close()` returns an error, does not panic.
- **TOCTOU drain:** a `controlReq` buffered in `ctrlCh` when `writeLoop` exits is
  answered with `errConnClosed` by `failPendingControl` (not left to hang). Drive
  this by making the test server stop reading / drop the socket so the next
  `writeLoop` write fails, enqueue a control request that lands in the buffer, and
  assert `UpdateSubscription` returns `errConnClosed` promptly (well under the
  `writeWait` timeout, proving it's the drain and not the timeout backstop).
- `UpdateSubscription` returns within `writeWait` even if the connection is wedged
  with no drain (timeout backstop, layer 2).
- Retained set reflects adds/removes (assert via an exported test hook or by
  re-reading frames after a simulated reconnect).

**Running with `-race` (CI integration).** The concurrency design here (the
`ctrlCh`/`subMu`/`Changes()` interplay) is exactly the kind of code the race
detector should exercise, but the repo disables `-race` globally
(`AGENTS.md`: "Race detector: Disabled globally" — `contrib/stream/shelf` fails
under it), so a blanket `go test -race ./...` is not viable and CI won't catch
these races automatically. To make `-race` *actually exercised* for the new,
race-safe packages, add a **dedicated, scoped make target** rather than relying
on a comment:
```make
# contrib/massive/Makefile
race-test:        ## Run the race detector on the dynamic-subscription packages only.
	go test -race ./ws/... ./subscription/...
```
Document in the PR/CI config that `race-test` should be run for changes touching
`ws/` or `subscription/`. (Scoped to these two packages so it never pulls in the
`-race`-incompatible `contrib/stream/shelf` code.)

---

## 5. Component 2 — `SubscriptionManager` (shared source of truth)

New file `contrib/massive/subscription/manager.go` (own package so both the
plugin root and trigger can import without cycles).

```go
package subscription

type DataType uint8
const ( Trades DataType = iota; Quotes )

// String returns the canonical config/wire data-type name ("trades"/"quotes").
// This is the SINGLE definition of the DataType<->name mapping; the ws.Topic
// mapping lives in the plugin layer (see §5.2, intentionally — the subscription
// package must not import ws so the trigger can import it without cycles).
func (d DataType) String() string {
    switch d {
    case Trades: return "trades"
    case Quotes: return "quotes"
    default:     return "unknown"
    }
}

// ParseDataType maps a config/wire name to a DataType. ok=false for anything
// other than "trades"/"quotes".
func ParseDataType(s string) (DataType, bool) {
    switch s {
    case "trades": return Trades, true
    case "quotes": return Quotes, true
    default:       return 0, false
    }
}

// Change is emitted when a symbol's effective subscription state flips.
type Change struct {
    Symbol   string
    DataType DataType
    Active   bool // true = now subscribed, false = now unsubscribed
}

type Manager struct {
    mu    sync.Mutex
    // refcount[dataType][symbol] = number of holders. >0 means subscribed.
    refs  map[DataType]map[string]int
    // ch carries Change events for ALL data types to the SINGLE control
    // goroutine (see §6.3). It must have exactly one consumer — a Go channel
    // delivers each value to only one receiver, so multiple consumers would
    // steal each other's events. The control goroutine routes each Change to
    // the correct client by Change.DataType.
    ch    chan Change   // buffered; single consumer (the control goroutine)
    // limits
    maxSymbols int
}

func New(maxSymbols int) *Manager
func (m *Manager) Changes() <-chan Change

// Acquire increments the refcount; emits an Active=true Change only on the
// 0->1 transition. Returns the new refcount. A successful return means the
// DESIRED state has been recorded (and a Change enqueued) — it does NOT mean
// the upstream subscribe frame has been sent or acked. See "intent vs.
// confirmed state" below.
func (m *Manager) Acquire(sym string, dt DataType) (int, error)

// Release decrements; emits Active=false only on the 1->0 transition. Same
// intent-only semantics as Acquire.
func (m *Manager) Release(sym string, dt DataType) int

// Active returns a snapshot of currently-subscribed symbols for a data type
// (used to replay after reconnect and to answer status queries).
func (m *Manager) Active(dt DataType) []string
```

The v1 API is **ref-count only** (`Acquire`/`Release`). There is intentionally
**no absolute-replace `SetActive`** in v1 — see "Deferred: SetActive" below.

Design points:
- **Ref-counting** satisfies requirement 4: `Acquire`/`Release` only flip the
  wire state on 0↔1 transitions. A holder identity is not tracked (simple
  counter); callers that need per-strategy idempotency can adopt the deferred
  per-owner extension (§15.1). v1 keeps a single shared refcount.
- **`maxSymbols` cap is per `DataType` (a "symbols" cap, not a "streams" cap).**
  The denominator is the number of distinct symbols with refcount > 0 *within a
  single `DataType`* — i.e. `len(refs[dt])`. `Acquire(sym, dt)` rejects (returns
  an error) when adding `sym` would push `len(refs[dt])` past `maxSymbols`; an
  `Acquire` on an already-present symbol (refcount ≥ 1) never trips the cap
  (it's just a refcount bump). So `max_dynamic_symbols: 500` permits up to 500
  trade symbols **and** up to 500 quote symbols (worst case 1000 upstream
  streams). Rationale: the operationally meaningful unit is "how many symbols am
  I watching"; trades and quotes for the same symbol are one logical interest.
  (If a strict total-stream budget is ever needed, sum `len(refs[dt])` across
  data types in the check — called out as an alternative, not v1.)
- The change channel is buffered and has a **single consumer** (the one control
  goroutine, §6.3). If it fills (control loop stalled), `Acquire`/`Release` log
  and apply the state anyway — the control loop reconciles against `Active()` on
  its next tick (it always trusts the manager snapshot, not just the event), so
  no change is permanently lost.
- **`Changes()` returns the single shared channel and MUST NOT be consumed by
  more than one goroutine.** Because a Go channel delivers each value to exactly
  one receiver, a second consumer would steal events (e.g. a `Quotes` change
  consumed by a trades-only loop and dropped). This is why the design uses one
  control goroutine that routes by `DataType` rather than one goroutine per
  connection (§6.3). The `Manager` may guard against misuse by panicking if
  `Changes()` is called more than once.
- **Intent vs. confirmed wire state (important for response semantics).** The
  `Manager` is the source of truth for *desired* state only. `Acquire`/`Release`
  update the refcount and enqueue a `Change`; they do **not** block on, and have
  no knowledge of, the actual upstream subscribe/unsubscribe round-trip. The wire
  action happens later and asynchronously: the control goroutine drains the
  `Change` (or hits the reconcile sweep) and calls
  `ws.Client.AddTickers`/`RemoveTickers`, whose own `result` only confirms the
  *frame was written* — not that the server acked it (server `success`/`error`
  is logged asynchronously, §4.4). Therefore **`Active()` and anything derived
  from it reflect intent, not confirmed live streaming.**   Under normal load the
  gap is sub-second, but a full channel, a mid-reconnect connection, or a
  rejected symbol can widen it. Callers that need confirmed-live semantics must
  observe actual data arriving in the `{sym}/1Sec/TRADE|QUOTE` buckets (precise
  per-request ack correlation is deferred, §15.2).

**Deferred: `SetActive` (absolute replace) — NOT in v1.** An earlier draft
included a `SetActive(dt, syms)` that forces the set to exactly `syms`. It is
**dropped from v1** for two reasons:
1. **No v1 caller.** The only external surface (the §7.3 `Subscribe` RPC) is
   single-symbol subscribe/unsubscribe, which maps cleanly onto `Acquire`/
   `Release`. Nothing in v1 needs bulk replace.
2. **Undefined interaction with ref-counting.** `SetActive` and `Acquire`/
   `Release` would share one `refs` map with conflicting models: if strategy X
   holds a refcount on `C` via `Acquire`, what does another caller's
   `SetActive([A,B])` do to `C`? Blowing `C` away violates X's outstanding
   reference; preserving `C` means `SetActive` is not actually "absolute" — there
   is no correct single answer without a richer ownership model.

   If a bulk API is needed later, it must be defined against the **per-owner**
   model (§15.1), e.g. `SetOwnerActive(owner, dt, syms)` that replaces only that
   owner's contribution and recomputes the union refcount — never touching other
   owners' references. Until then, callers that want a set applied issue
   individual `Acquire`/`Release` calls (idempotent at the wire level via 0↔1
   transitions).

### 5.1 Reconciliation model (robust to drops)
The control goroutine is **edge-triggered with level fallback**:
- It reacts to `Change` events for low latency (sub-second under normal load).
- It also reconciles the full `Active(dt)` snapshot against what each
  `ws.Client` currently has (a) on a slow safety-net timer (e.g. every 30s) and
  (b) immediately after any reconnect. The timer is a *correctness* backstop for
  dropped events / server-side resets, **not** the primary path — steady-state
  latency is driven by the edge events, so the 30s timer never gates a normal
  subscribe.

### 5.2 `DataType` ↔ `ws.Topic` mapping (single source of truth)

There are three representations of "trades/quotes" already or newly in play:
the config/wire **string** ("trades"/"quotes"), the new
`subscription.DataType`, and the existing `ws.Topic` (`StocksTrades`,
`StocksQuotes`, `ws/client.go:69-74`). To avoid the implementer reinventing
ad-hoc conversions (the "`chg.DataType` maps to this topic" hand-wave), the
mappings are defined **exactly once each**:

- **`DataType` ↔ string:** `DataType.String()` / `subscription.ParseDataType`
  in the `subscription` package (§5). This is the only place names are encoded.
- **`DataType` → `ws.Topic`:** a single table in the **plugin layer**
  (`massive.go`), placed next to and mirroring the existing
  `wsDataTypeToTopic` (`massive.go:50-56`). It lives here — not in the
  `subscription` package — because `subscription` must not import `ws` (so the
  trigger can import `subscription` without pulling the ws client into the
  trigger's dependency graph).

```go
// massive.go — mirrors wsDataTypeToTopic (massive.go:50-56).
// dataTypeToTopic is the SINGLE mapping from subscription.DataType to ws.Topic.
var dataTypeToTopic = map[subscription.DataType]ws.Topic{
    subscription.Trades: ws.StocksTrades,
    subscription.Quotes: ws.StocksQuotes,
}

// topicFor is the helper referenced by the control loop (§6.3).
func topicFor(dt subscription.DataType) ws.Topic { return dataTypeToTopic[dt] }
```

Consistency note: `wsDataTypeToTopic["trades"] == dataTypeToTopic[Trades]` and
likewise for quotes — both ultimately resolve to the same `ws.Topic`. A small
unit test asserts the two tables agree for the tick types, so they cannot drift.

---

## 6. Component 3 — bgworker integration

### 6.1 Owning the manager
In `NewBgWorker` (`massive.go:84`), construct
`mf.subMgr = subscription.New(cfg.MaxDynamicSymbols)` and store on
`MassiveFetcher`. Initialize the package singleton too (for Approach A's
trigger, mirroring `framework.Manager`): `subscription.Default = mf.subMgr`.

Also add to `MassiveFetcher`:
- `dynClients` — a mutex-guarded `map[DataType]*ws.Client` registry (the seam
  between connection lifecycle and the single control goroutine, §6.3.1).
- `enabledTickTypes []DataType` — derived from `ws_data_types` ∩ {trades,quotes},
  used by the control loop's reconcile sweep.

The **single** control goroutine (§6.3) is launched once from `Run()` when
`dynamic_ticks` is enabled (not per connection), and lives for the whole process
lifetime.

### 6.2 Dynamic mode vs. static mode
A connection for `trades`/`quotes` runs in one of two modes:
- **Static** (today's behavior): subscribes the configured symbol set at
  connect. Used when `dynamic_ticks: false` (default for back-compat) or when a
  static `symbols` set is desired.
- **Dynamic**: connects with an **empty** tick subscription and only
  subscribes/unsubscribes per `SubscriptionManager`. Enabled by
  `dynamic_ticks: true` (config, §9). The firehose `["*"]` default is **never**
  used in dynamic mode, and the static `symbols` set is **ignored for ticks** (it
  does not pre-seed tick subscriptions — see §9 for the full semantics and the
  startup warning).

Aggregate streams (`1Sec`/`1Min`) are unaffected and remain static — they still
subscribe the configured `symbols` (or `*`) because strategies need
full-universe bars to detect signals. `symbols` is also still used for backfill.

### 6.3 Control goroutine — exactly ONE, owns all dynamic tick clients

There is **a single control goroutine** for the whole worker. It owns the
registry of currently-live dynamic tick clients (`trades` and/or `quotes`,
whichever are enabled), consumes the **single** `subMgr.Changes()` channel, and
routes each `Change` to the correct client by `DataType`.

> **Why one goroutine, not one-per-connection.** A Go channel delivers each
> value to exactly one receiver. If we launched a per-connection goroutine and
> each did `chg := <-subMgr.Changes()`, a `Quotes` change could be received by
> the trades goroutine and discarded, and the quotes goroutine would never see
> it — the event is lost (only the slow 30s reconcile would eventually fix it,
> defeating the low-latency goal). A single consumer that routes by `DataType`
> eliminates this entirely. (Alternatives considered: per-`DataType` channels,
> or a fan-out broadcaster giving each loop its own channel. Both work but add
> machinery; a single routing goroutine is simpler and is the chosen design.)

The control goroutine holds a small map `clients map[DataType]*ws.Client`,
updated by the connection lifecycle (see §6.3.1) under a mutex, since the client
for a given `DataType` is replaced on every reconnect:

```
// single control goroutine, started once in Run() when dynamic_ticks is on
reconcileTicker := time.NewTicker(reconcileInterval) // e.g. 30s safety net
for {
  select {
  case <-mf.ctx.Done():
      return
  case chg := <-mf.subMgr.Changes():            // edge (primary path)
      client := mf.dynClients.get(chg.DataType)  // nil if that conn is down
      if client == nil {
          // Connection is mid-reconnect; skip — the post-reconnect replay
          // (§6.3.1) will apply the current Active() set.
          continue
      }
      if chg.Active { client.AddTickers(topicFor(chg.DataType), chg.Symbol) }
      else          { client.RemoveTickers(topicFor(chg.DataType), chg.Symbol) }
  case <-reconcileTicker.C:                      // level fallback (safety net)
      for _, dt := range mf.enabledTickTypes {
          if client := mf.dynClients.get(dt); client != nil {
              reconcile(client, topicFor(dt), mf.subMgr.Active(dt))
          }
      }
  }
}
```

`mf.dynClients` is a tiny mutex-guarded `map[DataType]*ws.Client` (a "client
registry"). It is the seam between the connection lifecycle (which creates/tears
down clients) and the single control goroutine (which reads from it).
`topicFor(dt)` is the `DataType`→`ws.Topic` helper defined once in §5.2 (mirrors
`wsDataTypeToTopic`); the control loop never builds topic prefixes inline.

#### 6.3.1 Reconnect handling (decoupled from the control goroutine)
The per-data-type connection lifecycle (`streamWithRestart`, `massive.go:272`)
creates a **fresh `ws.Client` on every (re)connect**. To keep the single control
goroutine correct without re-binding it:

- When a dynamic tick connection finishes its handshake, the connection
  goroutine **registers** its new client: `mf.dynClients.set(dt, client)` and
  immediately **replays the active set** onto it:
  `reconcile(client, topicFor(dt), mf.subMgr.Active(dt))`. (Dynamic connections
  connect with an empty tick subscription, then this replay applies the current
  desired set — so a reconnect restores exactly what was subscribed.)
- When that connection drops, the connection goroutine **deregisters**:
  `mf.dynClients.clear(dt)`. While cleared, edge events for that `DataType` are
  skipped by the control loop (see the `client == nil` branch above) and applied
  by the next replay on reconnect.

This keeps the control goroutine stable for the whole process lifetime; only the
entries in `dynClients` churn. The 30s reconcile is a pure backstop and is not
on the latency path.

### 6.4 Status read-back
Extend the manager with a snapshot accessor and expose it through the RPC
provider (§7) so external callers can query "what's currently subscribed".

---

## 7. Component 4 — Approach B: external RPC/HTTP control (PRIMARY)

Mirror the `WatchlistProvider` pattern exactly, but as a **command** interface.

### 7.1 Optional bgworker interface (`plugins/bgworker/bgworker.go`)
```go
// SubscriptionController is an optional interface a BgWorker can implement to
// allow the RPC layer to drive live tick subscriptions at runtime.
type SubscriptionController interface {
    Subscribe(symbol string, dataTypes []string) error   // dataTypes: "trades","quotes"
    Unsubscribe(symbol string, dataTypes []string) error
    ActiveSubscriptions() map[string][]string             // symbol -> data types
}
```
`MassiveFetcher` implements it by translating each `dataTypes` string via
`subscription.ParseDataType` (§5, the single name→`DataType` mapping) and calling
`subMgr.Acquire`/`Release`/`Active`. An unrecognized data-type string returns an
error (no silent drop). `ActiveSubscriptions` builds its `symbol -> []string`
result using `DataType.String()`.

### 7.2 Host registration (`cmd/start/plugins.go`)
Alongside the existing `WatchlistDataSource` assert (`plugins.go:28`):
```go
if sc, ok := bgWorker.(bgworker.SubscriptionController); ok {
    frontend.RegisterSubscriptionController(&subscriptionAdapter{src: sc})
    log.Info("Registered subscription controller from BgWorker %s", name)
}
```
New `frontend/subscription_provider.go` mirrors `watchlist_provider.go`:
mutex-guarded global + `RegisterSubscriptionController` / `GetSubscriptionController`.
An adapter (`subscriptionAdapter`) bridges the plugin interface to a frontend
interface (same indirection rationale as `convertBgRanking`,
`plugins.go:73-93`).

### 7.3 JSON-RPC method (`frontend/subscribe.go` — new)
Add a `DataService` method (registered automatically by
`s.RegisterService(service, "")` `frontend/server.go:79`):
```go
type SubscribeRequest struct {
    Symbol    string   `msgpack:"symbol"`
    DataTypes []string `msgpack:"data_types"` // ["trades","quotes"]
    Action    string   `msgpack:"action"`     // "subscribe" | "unsubscribe"
}
type SubscribeResponse struct {
    // Active is the manager's current DESIRED subscription set (intent), NOT a
    // confirmation that the upstream stream is live. See response semantics
    // below and §5 "intent vs. confirmed wire state".
    Active map[string][]string `msgpack:"active"`
}
func (s *DataService) Subscribe(r *http.Request, req *SubscribeRequest, resp *SubscribeResponse) error
```
Guards: `Queryable` check (cf. `list_watchlists.go:47`); nil controller →
clear error ("dynamic subscriptions not available"); validate `action`. Data-type
validation is delegated to the plugin's `SubscriptionController`, which uses
`subscription.ParseDataType` (§5) — the frontend does not hard-code the
`{trades,quotes}` set, keeping that knowledge single-sourced in the
`subscription` package.

**Response semantics (must be documented for clients).** A successful
`Subscribe` response means the request was *accepted and the desired state
recorded* — the symbol's refcount was incremented and a `Change` enqueued. It
does **not** mean tick data is already flowing. The actual subscribe frame is
sent asynchronously by the control goroutine (§6.3); confirmation of live data
is only observable by seeing rows land in `{sym}/1Sec/TRADE|QUOTE`. The returned
`Active` map is the manager's intent snapshot. Concretely:
- A 200/`nil`-error response = "accepted", analogous to an idempotent PUT of
  desired state — not "streaming confirmed".
- Errors are returned synchronously only for *request-level* failures: nil
  controller, invalid `action`, unknown data type, or `max_dynamic_symbols`
  exceeded (`Acquire` returns an error). Upstream/wire failures (bad symbol,
  server reject) are **not** reflected in the response; they surface as logged
  `error` status and absent data.
- Clients needing confirmed-live behavior should poll the target bucket (or use
  the existing streaming/query paths) until data appears, with their own
  timeout. Per-request wire-ack correlation is a deferred enhancement (§15.2).

### 7.4 gRPC method (`proto/marketstore.proto` + `frontend/grpc.go`)
Add to `service Marketstore` (`marketstore.proto:196`):
```proto
rpc Subscribe (SubscribeRequest) returns (SubscribeResponse);

message SubscribeRequest {
  string symbol             = 1;
  repeated string data_types = 2;  // "trades","quotes"
  string action             = 3;   // "subscribe" | "unsubscribe"
}

// proto3 maps cannot have a `repeated` value type, so the per-symbol list of
// data types is wrapped in a message.
message DataTypeList {
  repeated string data_types = 1;
}

message SubscribeResponse {
  // active mirrors the JSON-RPC SubscribeResponse.Active (map[string][]string):
  // symbol -> its currently-intended data types. Same field name ("active") on
  // both transports; the gRPC side just needs the DataTypeList wrapper for the
  // repeated value. Reflects INTENT, not confirmed wire state (§5, §7.3).
  map<string, DataTypeList> active = 1;
}
```
Regenerate via `proto/Makefile`. Implement `GRPCService.Subscribe` (cf.
`grpc.go:462` `ListWatchlists`) delegating to the same provider.

**Transport asymmetry (intentional, confirmed).** JSON-RPC uses
`map[string][]string` directly (`§7.3`, msgpack supports a list value); gRPC uses
`map<string, DataTypeList>` because proto3 forbids `repeated` map values. Both
expose the **same logical field named `active`** with identical meaning; only the
value encoding differs. The provider returns the plugin-neutral
`map[string][]string`, and each transport adapts it (gRPC wraps each slice in a
`DataTypeList`).

### 7.5 Why a provider indirection (not a direct plugin call)
The frontend cannot import the plugin and vice-versa (the `convertBgRanking`
comment, `plugins.go:73-77`). The registered-provider pattern is the established,
working solution and keeps the gRPC/JSON-RPC handlers free of plugin deps.

### 7.6 Tests
- `frontend/subscribe_test.go`: mock `SubscriptionController` (like
  `list_watchlists_test.go`'s `mockProvider`), assert subscribe/unsubscribe
  delegate, unknown data type errors, nil-provider path, `Queryable` gate.
- gRPC handler test alongside existing grpc tests.

---

## 8. Component 5 — Approach A: trigger-driven (SECONDARY)

A new trigger in `contrib/massive/` (or a thin example under
`contrib/massive/subscription/trigger/`) firing on `*/1Sec/OHLCV` (or `1Min`):

```go
func (t *SubscriptionTrigger) Fire(keyPath string, records []trigger.Record) {
    sym := symbolFromKeyPath(keyPath)
    // Example signal hook — real strategy logic is out of scope.
    if t.signal.EntryDetected(sym, records) {
        subscription.Default.Acquire(sym, subscription.Trades)
        subscription.Default.Acquire(sym, subscription.Quotes)
    }
    if t.signal.ExitDetected(sym, records) {
        subscription.Default.Release(sym, subscription.Trades)
        subscription.Default.Release(sym, subscription.Quotes)
    }
}
```
- Uses the **shared singleton** `subscription.Default` (set by the bgworker in
  `NewBgWorker`), exactly like `framework.Manager`
  (`contrib/watchlist/framework/state.go:5-7`). Trigger and bgworker are in the
  same `.so`, so this is a safe in-process reference.
- The trigger only **signals**; it never touches the `ws.Client` (it has no
  access, and shouldn't). The bgworker's control loop owns the connection.
- The bundled trigger ships with a no-op/example signal; teams plug in real
  strategy logic. (Detecting signals from bar records is the strategy's job, not
  this design's.)

Config wiring: add a `triggers:` entry pointing at `massive.so` (the trigger
factory `NewTrigger` lives in the same module) on `*/1Sec/OHLCV`.

---

## 9. Config

New `massiveconfig.FetcherConfig` fields (`massiveconfig/config.go`):
```yaml
bgworkers:
  - module: massive.so
    config:
      ws_data_types: [ '1Sec', 'trades', 'quotes' ]  # ticks present but...
      dynamic_ticks: true            # ...start with EMPTY tick subs; driven at runtime
      max_dynamic_symbols: 500       # per-DataType cap (≤500 trade syms AND ≤500 quote syms)
      # symbols: used for aggregate (1Sec/1Min) streams + backfill; IGNORED for
      #          trades/quotes when dynamic_ticks is true (see below)
```
- `dynamic_ticks` (default **false** → today's static behavior, fully
  back-compatible).
- `max_dynamic_symbols` (default e.g. 500) → `SubscriptionManager` cap,
  **per `DataType`** (§5): up to 500 trade symbols and up to 500 quote symbols
  (worst case 1000 upstream streams). It is a symbols cap, not a streams cap.

- **What `symbols` means in dynamic mode (must be explicit).** When
  `dynamic_ticks: true`:
  - **Aggregate streams (`1Sec`/`1Min`)** still subscribe the static `symbols`
    set (or `*`) — strategies need full-universe bars to detect signals.
  - **Tick streams (`trades`/`quotes`)** start with an **empty** subscription
    and are driven only by the `SubscriptionManager`. The static `symbols` set is
    **ignored** for ticks — it does **not** pre-seed tick subscriptions.
  - **Backfill** continues to use `symbols` (and `query_start`) unchanged.

  Rationale: pre-seeding ticks from a large static `symbols`/`*` set would defeat
  the purpose (and could blow the cost budget). If an operator *wants* a fixed
  set of always-on tick symbols in addition to dynamic ones, that is a future
  `dynamic_ticks` companion option (e.g. `tick_seed_symbols`) — not v1; for now
  use `dynamic_ticks: false` for fully-static tick streaming.

Validation (`ValidateConfig`, `massiveconfig/config.go`):
- `dynamic_ticks: true` but neither `trades` nor `quotes` in `ws_data_types`
  → **warn** (nothing to control; dynamic mode is a no-op).
- `dynamic_ticks: true` and a non-empty/`*` `symbols` set → **warn** that
  `symbols` is ignored for trades/quotes in dynamic mode (still used for
  aggs/backfill), so the operator isn't surprised that those tick symbols don't
  auto-subscribe.
- `max_dynamic_symbols <= 0` → apply the default (treat as unset).

---

## 10. File-Level Change Summary

| File | Change |
|---|---|
| `contrib/massive/ws/client.go` | `actionUnsubscribe`; `ctrlCh`+`subMu`; `UpdateSubscription`/`AddTickers`/`RemoveTickers`; `writeLoop` control case; doc update |
| `contrib/massive/ws/client_test.go` | NEW: live sub/unsub, concurrency, post-close, reconnect replay |
| `contrib/massive/subscription/manager.go` | NEW: `DataType` + `DataType.String()`/`ParseDataType` (single name↔DataType mapping); ref-counted `Manager` (`Acquire`/`Release`/`Active`/`Changes`), `Change` events, `Default` singleton (no `SetActive` in v1) |
| `contrib/massive/subscription/manager_test.go` | NEW: refcount transitions, cap, snapshot, `ParseDataType`/`String` round-trip |
| `contrib/massive/massive.go` | Own `subMgr` + `dynClients` registry + `enabledTickTypes`; `dataTypeToTopic` map + `topicFor()` (single `DataType`→`ws.Topic` mapping, mirrors `wsDataTypeToTopic`); dynamic vs static tick mode; **single** control goroutine (started in `Run()`) that routes `Change`s by `DataType`; register/deregister + replay `Active()` in the connection lifecycle on (re)connect/drop; implement `SubscriptionController` (via `ParseDataType`) |
| `contrib/massive/massive_test.go` | Consistency test: `dataTypeToTopic` agrees with `wsDataTypeToTopic` for trades/quotes |
| `contrib/massive/Makefile` | NEW `race-test` target: `go test -race ./ws/... ./subscription/...` (scoped, avoids the `-race`-incompatible `contrib/stream/shelf`) |
| `contrib/massive/massiveconfig/config.go` | `DynamicTicks`, `MaxDynamicSymbols` + validation |
| `plugins/bgworker/bgworker.go` | NEW optional `SubscriptionController` interface |
| `cmd/start/plugins.go` | Type-assert + register subscription controller (mirror watchlist) |
| `frontend/subscription_provider.go` | NEW: provider iface + register/get (mirror `watchlist_provider.go`) |
| `frontend/subscribe.go` | NEW: `DataService.Subscribe` JSON-RPC handler |
| `frontend/subscribe_test.go` | NEW |
| `proto/marketstore.proto` | `rpc Subscribe` + messages; regenerate |
| `frontend/grpc.go` | `GRPCService.Subscribe` |
| `contrib/massive/subscription/trigger/` (or massive root) | NEW: example `SubscriptionTrigger` (Approach A) |

---

## 11. Failure Modes & Edge Cases

- **Reconnect resets server subs.** Mitigated by register + replay `Active()` in
  the connection lifecycle on reconnect (§6.3.1), plus the 30s reconcile backstop
  (§5.1). The manager — not config — is the source of truth.
- **Single-consumer channel constraint.** `Changes()` has exactly one consumer
  (the single control goroutine, §6.3); routing by `DataType` happens inside that
  goroutine. Multiple consumers would steal events — guarded against by
  `Changes()` panicking on a second call (§5).
- **Event lost while a connection is mid-reconnect.** If a `Change` arrives while
  that `DataType`'s client is deregistered, the control loop skips it
  (`client == nil`); the post-reconnect replay of `Active()` (§6.3.1) applies the
  current desired set, so the effect converges without relying on the 30s timer.
- **Control channel backpressure.** Buffered `ctrlCh` (write side) + buffered
  manager `ch` (single consumer); if either fills, the 30s level-reconcile
  guarantees eventual convergence because the control loop always trusts the
  `Active()` snapshot, not just the edge event (§5).
- **Subscribe ack failure** (bad symbol, server error). Surfaces as logged
  `error` status (§4.4); data simply never arrives. The manager keeps the symbol
  "active" and the next reconcile re-attempts. Per-request ack correlation is a
  possible future enhancement (deferred).
- **Connection limit.** Dynamic ticks reuse the **existing** per-data-type
  connection (one trades conn, one quotes conn) — we add/remove tickers on it,
  **not** new connections — so the account connection limit is unaffected.
- **Cap exceeded.** `Acquire` returns an error → RPC returns a clear failure; the
  trigger logs and skips.
- **Double subscribe / double release.** Ref-counting makes these safe and
  idempotent at the wire level (only 0↔1 transitions emit frames).
- **Shutdown.** `ctx.Done()` stops the control goroutine and the connection
  loops; `Close()` tears down clients; `UpdateSubscription` after close returns an
  error (no panic).

---

## 12. Security / Operational Notes

- The `Subscribe` RPC mutates server behavior; it inherits the same exposure as
  other RPC methods. If MarketStore RPC is reachable by untrusted clients,
  consider gating behind the existing auth/network controls. `max_dynamic_symbols`
  bounds blast radius.
- Tick volume per subscribed symbol is large but **bounded by the active set**;
  the cap and ref-counting keep it controlled. This is the intended,
  cost-appropriate alternative to full-universe tick streaming.

---

## 13. Phasing

1. **Phase 1 (foundation):** `ws.Client` live sub/unsub + tests;
   `SubscriptionManager` + tests. No behavior change yet (nothing calls them).
2. **Phase 2 (Approach B, primary):** bgworker dynamic mode + `dynClients`
   registry + **single** routing control goroutine + `SubscriptionController`;
   frontend provider + JSON-RPC `Subscribe`; config. End-to-end: external client
   toggles a symbol, data appears/stops.
3. **Phase 3:** gRPC `Subscribe` (proto regen) + parity tests.
4. **Phase 4 (Approach A):** example `SubscriptionTrigger` wired to the shared
   manager singleton; docs.

Each phase builds, vets, and tests independently; Phase 1 is mergeable alone.

---

## 14. Test / Verification Plan

- Unit: `ws` live sub/unsub framing & concurrency; `SubscriptionManager`
  refcount transitions, cap, snapshot, change emission; frontend `Subscribe`
  handler (mock controller); gRPC handler.
- Response semantics: `Subscribe` returns success (and `Active` reflecting the
  new intent) even when the control loop has not yet drained the `Change` (e.g.
  with a full channel / no consumer) — proving the response is intent, not
  confirmed wire state; request-level failures (unknown data type, cap exceeded,
  nil controller, bad action) return synchronous errors.
- Routing/control-loop: a single control goroutine consuming `Changes()` routes
  `Trades` and `Quotes` events to the correct client (no cross-`DataType` loss);
  `Changes()` called twice panics; an event arriving while a `DataType` is
  deregistered is applied by the post-reconnect `Active()` replay.
- Integration (manual or scripted against a test WS server / mock feed):
  subscribe a symbol via RPC → trade/quote rows appear in `{sym}/1Sec/TRADE|QUOTE`;
  unsubscribe → rows stop; reconnect mid-session → active set restored.
- Back-compat: `dynamic_ticks` unset → identical to today (static subs); with
  `dynamic_ticks: true` and a non-empty `symbols`, ticks start empty (symbols
  ignored for ticks) while aggs still subscribe `symbols` (§9), and a warning is
  emitted.
- Cap: `Acquire` past `max_dynamic_symbols` for a `DataType` errors; the same
  symbol acquired for both trades and quotes counts once per data type (per-
  `DataType` denominator, §5).
- `make fmt`, `go vet ./...`, `make build`, `make plugins`; plus
  `make -C contrib/massive race-test` for `ws/` + `subscription/` changes.

---

## 15. Open Questions

1. **Per-owner subscriptions vs simple refcount?** Simple refcount is proposed
   for v1. If strategies need guaranteed idempotency without coordinating counts,
   add a per-owner set keyed by an owner id (RPC param). This is also the
   prerequisite for a well-defined bulk/replace API: a deferred
   `SetOwnerActive(owner, dt, syms)` that replaces only that owner's contribution
   and recomputes the union (see "Deferred: `SetActive`" in §5). Deferred unless
   needed.
2. **Request→ack correlation in `ws.Client`?** Deferred. Consequence: the
   `Subscribe` RPC response and `Active()` reflect **intent**, not confirmed live
   streaming (documented in §5 "intent vs. confirmed wire state" and §7.3
   "Response semantics"). Level-reconcile covers eventual correctness; logs cover
   observability. Add request→server-ack correlation (and an optional
   confirmed-live RPC field) only if precise per-symbol confirmation becomes
   required.
3. **Auto-expiry / TTL on subscriptions?** Optional safety: a subscription with
   no activity or no refresh for N minutes auto-releases, to prevent leaks from
   a crashed external strategy. Proposed as a Phase-2+ option (TTL in the
   manager); not required for v1.
