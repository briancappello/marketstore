# Root Cause Analysis: WAL 5ms Ticker Polling Loop

## Summary

The `SyncWAL` function has a hardcoded ticker that fires every 5ms (200 times/second) to check if the write channel is >= 80% full. This is wasteful when idle and was added as an arbitrary heuristic with no justification for the specific interval.

## 1. The Three Tickers in `SyncWAL`

`SyncWAL` (`executor/wal.go:718`) runs three concurrent tickers in a `select` loop:

| Ticker | Interval | Purpose |
|--------|----------|---------|
| `tickerWAL` | 500ms (`walRefresh`) | Periodic WAL flush — drains `writeChannel` and commits to WAL + primary storage |
| `tickerPrimary` | 5min (`primaryRefresh`) | Checkpoint (fsync) + periodic WAL rotation |
| `tickerCheck` | 5ms (`walRefresh / 100`) | Emergency backpressure — flushes if `writeChannel` >= 80% full |

These are hardcoded constants in `internal/di/wal.go:58-60`:
```go
defaultWalSyncInterval            = 500 * time.Millisecond
defaultPrimaryDiskRefreshInterval = 5 * time.Minute
```

The 5ms interval is derived at `wal.go:729`:
```go
tickerCheck := time.NewTicker(walRefresh / numTickerCheckPerWALRefresh)
```
where `numTickerCheckPerWALRefresh = 100` (line 723). So: `500ms / 100 = 5ms`.

**None of these intervals are user-configurable.** Only `walRotateInterval` (default 5) is exposed via config, and it controls how many `primaryRefresh` ticks occur before WAL truncation (i.e., `5 * 5min = 25 minutes`).

## 2. Why 5ms Was Chosen

Commit `87b42374` ("Add frequent check of the channel overflow", 2018-01-28, Hitoshi Harada) introduced the `tickerCheck` with exactly this logic:

```diff
+tickerCheck := time.NewTicker(WALRefresh / 100)
...
+case <-tickerCheck.C:
+    queued := len(ThisInstance.TXNPipe.writeChannel)
+    if float64(queued)/float64(chanCap) >= 0.8 {
```

There is **no comment or commit message explaining why 100x was chosen**. The commit message is simply "Add frequent check of the channel overflow." The divisor of 100 appears to be an arbitrary "check often enough" heuristic — a simple way to catch a rapidly filling channel between the coarser 500ms WAL flushes.

## 3. The Write Channel

Defined in `executor/cache.go:15-31`:

```go
const WriteChannelCommandDepth = 1000000  // capacity: 1 million

type TransactionPipe struct {
    tgID         int64
    writeChannel chan *wal.WriteCommand  // capacity 1,000,000
    flushChannel chan chan struct{}       // capacity 1,000,000 (!)
}
```

- **Capacity**: 1,000,000 write commands
- **80% threshold**: 800,000 commands
- Commands are enqueued at `wal.go:219`: `wf.txnPipe.writeChannel <- wc`
- Commands are drained in `FlushToWAL` (`wal.go:235-259`) by reading `len(writeChannel)` then consuming that many items

Note: the `flushChannel` also has capacity 1,000,000 which is excessive — it's used for synchronous flush requests (`RequestFlush` at `wal.go:791`) and typically has at most 1 pending item. `RequestFlush` (`wal.go:799`) explicitly dedupes via `len(flushChannel) > 0`, so the oversized buffer is harmless; do not shrink it without auditing that path.

## 4. Cost of Each 5ms Tick

On each tick (`wal.go:745-751`):

```go
case <-tickerCheck.C:
    queued := len(wf.txnPipe.writeChannel)                        // (1) channel length check
    if float64(queued)/float64(chanCap) >= writeChannelCapThreshold { // (2) two int->float64 conversions, division, comparison
        if err := wf.FlushToWAL(); err != nil {                   // (3) only if threshold exceeded
```

Per tick when idle:
1. **Timer channel receive**: goroutine wakeup from scheduler
2. **`len(chan)`**: O(1), reads an atomic counter on the channel's internal `qcount` field
3. **Two `float64()` conversions, one float division, one comparison**: trivially cheap
4. **No allocation, no syscall, no I/O**

**Individual cost**: ~100-200ns of CPU per tick. At 200 ticks/second, that's ~20-40us/second of CPU — **negligible in absolute terms**.

**However**, the real cost is:
- **Goroutine wakeups**: 200 wakeups/second of the `SyncWAL` goroutine when the system is idle. On a multi-core runtime with other work running this is nearly free (the goroutine is cheap to schedule on an already-running P), but it still represents pointless work in the runtime's timer wheel.
- **Latency interference**: in the `select`, `tickerCheck` fires 100x more often than `tickerWAL`, meaning idle wakeups are dominated by no-op check ticks rather than the meaningful 500ms flush ticks.
- **Power/thermal (speculative)**: on embedded or power-sensitive deployments, unnecessary wakeups *may* prevent deeper CPU sleep states. This has not been measured for MarketStore and should not be the primary motivation.

## 5. How the 500ms WAL Flush Interacts With the 5ms Check

The three cases in the `select` at `wal.go:735-767` are:

1. **`tickerWAL` (500ms)**: Always flushes whatever is in `writeChannel`, even if it's empty (in which case `FlushToWAL` just increments TGID and returns — `wal.go:238-239`)
2. **`tickerCheck` (5ms)**: Only flushes if channel is >= 80% full (800,000 commands)
3. **`flushChannel`**: Synchronous flush request from `RequestFlush()`

**The 5ms check is purely a safety valve**. Under normal operation, the 500ms ticker handles all flushing. The 5ms check only matters if a burst of > 800,000 write commands arrives within a single 500ms window — an extreme scenario.

In practice, Go's `select` with multiple ready channels picks one pseudo-randomly. When both `tickerWAL` and `tickerCheck` are ready, either may fire. But since `tickerCheck` fires 100x more frequently, it dominates idle CPU.

## 6. WAL Rotation / Checkpoint Interval

`tickerPrimary` fires every 5 minutes (`defaultPrimaryDiskRefreshInterval`). On each fire:
1. `CreateCheckpoint()` is called (`wal.go:464`) — syncs filesystem via `io.Syncfs()`
2. `primaryFlushCounter` increments
3. Every `walRotateInterval` checkpoints (default 5), the WAL file is truncated (`wal.go:757-765`)

So WAL rotation = `5 checkpoints * 5 minutes = 25 minutes` by default.

**Configurable?**

| Parameter | Value | Configurable? | Location |
|-----------|-------|---------------|----------|
| WAL flush interval | 500ms | No | `internal/di/wal.go:59` |
| Backpressure check interval | 5ms (500ms/100) | No | `executor/wal.go:723,729` |
| Checkpoint interval | 5min | No | `internal/di/wal.go:60` |
| WAL rotate interval | 5 checkpoints (25min) | Yes (`wal_rotate_interval`) | `utils/config.go:94,143` |
| Write channel capacity | 1,000,000 | No | `executor/cache.go:15` |
| Backpressure threshold | 80% (800,000) | No | `executor/wal.go:724` |

## 7. Could This Be Replaced With a Non-Polling Approach?

Yes. Several approaches would eliminate the 5ms polling:

### Option A: Writer-side notification (Recommended)

Add a dedicated, capacity-1, non-blocking signal channel and let `QueueWriteCommand` poke it when the write channel crosses the threshold. **Do not reuse `flushChannel`** — that channel is `chan chan struct{}`, where each element is a reply channel the receiver signals on completion (`wal.go:740-744`). Sending a synthetic reply channel from the writer would either leak the goroutine reading the reply or starve other queued `RequestFlush` callers.

Changes required:

1. **`executor/cache.go`** — add a new field on `TransactionPipe`:
   ```go
   type TransactionPipe struct {
       tgID           int64
       writeChannel   chan *wal.WriteCommand
       flushChannel   chan chan struct{}
       backpressureCh chan struct{} // capacity 1, coalesced backpressure signal
   }
   ```
   Initialize `backpressureCh: make(chan struct{}, 1)` in `NewTransactionPipe`.

2. **`executor/wal.go` `QueueWriteCommand`** — non-blocking signal after enqueue:
   ```go
   func (wf *WALFileType) QueueWriteCommand(wc *wal.WriteCommand) {
       wf.txnPipe.writeChannel <- wc
       const writeChannelCapThreshold = 0.8
       if len(wf.txnPipe.writeChannel) >= int(float64(cap(wf.txnPipe.writeChannel))*writeChannelCapThreshold) {
           select {
           case wf.txnPipe.backpressureCh <- struct{}{}:
           default: // signal already pending; coalesce
           }
       }
   }
   ```

3. **`executor/wal.go` `SyncWAL`** — replace `tickerCheck` with a new `select` case:
   ```go
   case <-wf.txnPipe.backpressureCh:
       if err := wf.FlushToWAL(); err != nil {
           log.Error("[backpressure] failed to FlushToWAL: " + err.Error())
       }
   ```
   Remove `tickerCheck` and the `numTickerCheckPerWALRefresh` constant.

4. **Shutdown safety** — the existing shutdown path (`wal.go:768-782`) handles the final flush. The new `backpressureCh` case lives inside the `if !*wf.shutdownPending` branch alongside the other tickers, so no new shutdown logic is needed. The signal channel does not need closing; once `SyncWAL` returns, any pending signal is harmlessly garbage-collected with the `TransactionPipe`.

This eliminates *all* idle wakeups while providing immediate (sub-microsecond, single channel send) response to actual backpressure. The `len()` check happens only when a command is actually written, not 200 times/second.

### Option B: Remove `tickerCheck` entirely

`QueueWriteCommand` (`wal.go:218-219`) is an unconditional send: `wf.txnPipe.writeChannel <- wc`. There is no `select`/`default`, so a full channel **blocks the caller**, propagating backpressure all the way up through the gRPC/JSON-RPC write handlers to the client. That is itself a correct backpressure mechanism.

With a 1,000,000-capacity channel and 500ms flush interval, you'd need a sustained write rate of >2M commands/second for 500ms to overflow. If the system can sustain that rate, the 5ms check only catches it ~5ms before blocking anyway — the protection it adds over plain blocking is marginal.

Option B is the smallest possible change (delete `tickerCheck`) and arguably correct, but Option A retains the original intent (early flush before the channel saturates) without the polling cost.

### Option C: Adaptive ticker

Start with a long interval (e.g., 500ms), shorten it when writes are detected, lengthen when idle. More complex than A and offers no advantage over event-driven signalling.

**Recommendation: Option A.** Smallest behavioral change versus today (still flushes early under load), zero idle wakeups, no new dependencies.

## 8. Test Plan

1. **Existing unit tests** in `executor/wal_test.go` and the rest of `executor/...` must continue to pass:
   ```
   go test ./executor/...
   ```
2. **Race detector** on the executor package (the repo-wide `-race` flag is disabled due to `contrib/stream/shelf/shelf_test.go`, but it works for individual packages):
   ```
   go test -race ./executor/...
   ```
3. **New test** in `executor/wal_test.go` that:
   - Constructs a `WALFileType` with `SyncWAL` running.
   - Calls `QueueWriteCommand` enough times to cross the 80% threshold.
   - Asserts that `FlushToWAL` is invoked **before** the next 500ms `tickerWAL` boundary (use a fake clock or a much smaller `walRefresh` for the test, e.g. 5s, and assert the flush completes within ~100ms).
   - Asserts that calling `QueueWriteCommand` once when the channel is < 80% full produces *no* extra flush.
4. **Coalescing test**: hammer `QueueWriteCommand` from many goroutines past the threshold and assert no goroutines leak (signal sends remain non-blocking) and the flush count stays bounded.
5. **Idle behavior**: verify with `runtime.NumGoroutine()` and a CPU profile over a 5s idle window that the `SyncWAL` goroutine wakes only on the 500ms `tickerWAL` and the 5min `tickerPrimary`, not 200x/sec.
6. **Integration tests**: `make integration-test-jsonrpc` and `make integration-test-grpc` to confirm write paths are unaffected end-to-end.
