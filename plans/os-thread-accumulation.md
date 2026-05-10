# Root Cause Analysis: 3,612 OS Threads with 18 Goroutines

## Summary

The 3,612 OS threads are **not caused by any single smoking gun** but by a combination of factors, all rooted in the same fundamental Go runtime behavior: **OS threads are created on demand and never released**. The primary contributor is the **massive plugin's backfill process**, which creates many short-lived goroutines that make **CGo-based DNS lookups** (via the net package's cgo resolver) and **HTTP connections** that trigger syscalls. Over 12 days of operation, including multiple backfill cycles and WebSocket reconnections, threads accumulate monotonically.

## Factor 1: CGo DNS Resolution (Primary Thread Factory)

**This is the most significant thread source.**

Since `CGO_ENABLED=1` (required for the plugin system), Go's `net` package uses the **CGo-based DNS resolver** by default on Linux. Every DNS lookup calls `C.getaddrinfo()` via CGo.

Key code path in Go's standard library:
- `net/cgo_unix.go:228` — `cgoLookupIP()` calls `doBlockingWithCtx()`
- `net/cgo_unix.go` — `doBlockingWithCtx()` spawns a **new goroutine** for each cancellable DNS lookup:
  ```go
  go func() {
      defer releaseThread()
      r.res, r.err = blocking()  // calls C.getaddrinfo via CGo
      res <- r
  }()
  ```
- `net/cgo_unix_cgo.go:84` — the actual C call: `C.getaddrinfo()`

Each CGo call causes `entersyscall()` (`runtime/cgocall.go:167`), which **detaches the goroutine's P** (processor). If all P's are detached, the Go scheduler creates a **new OS thread** via `newm()` (`runtime/proc.go:1799`) to service the remaining runnable goroutines.

The concurrency limit is set in `net/rlimit_unix.go:21-30`: up to **500 concurrent DNS threads** (capped by RLIMIT_NOFILE). This means a burst of DNS lookups can create hundreds of OS threads instantly.

### Where the DNS lookups happen in the process

1. **Massive REST API backfill** — `api/api.go:404`: `client.Do(req)` to `api.massive.com`. Each HTTP request triggers a DNS lookup. With `backfill_parallelism` defaulting to `runtime.NumCPU()` (`massive.go:487-488`) and potentially hundreds/thousands of symbols, the backfill creates bursts of concurrent HTTP requests.

2. **Massive S3 flat file downloads** — `backfill/flatfiles/backfill.go:121`: worker pool downloads from S3 endpoint (`files.massive.com`), each requiring DNS resolution.

3. **Massive WebSocket connections** — `ws/client.go:182`: `websocket.DefaultDialer.Dial()` resolves `socket.massive.com`.

4. **PostgreSQL connections** — `massive.go:477`: `pgxpool.New()` resolves the PostgreSQL host from `symbols_dsn`. The `pgx` library (pure Go, no CGo itself) still uses the net package's CGo resolver for DNS.

5. **Watchlist plugin PostgreSQL** — `watchlists/industry_aggregate.go:129` and `watchlists/sector_aggregate.go:124`: `sql.Open("postgres", dsn)` via `lib/pq` (also pure Go, but DNS still goes through CGo resolver).

## Factor 2: Go Plugin System Uses CGo (Startup Thread Cost)

Confirmed in Go's source at `plugin/plugin_dlopen.go:6-34`:
```go
//go:build (linux && cgo) || (darwin && cgo) || (freebsd && cgo)
// #cgo linux LDFLAGS: -ldl
import "C"
```

Each `plugin.Open()` call invokes `C.dlopen()` and `C.dlsym()` via CGo. Plugins are loaded at:

- `plugins/load.go:46` — absolute path plugins (watchlist.so)
- `plugins/load.go:61` — GOPATH-relative plugins (ondiskagg.so, streamreplay.so, massive.so)

But the **same .so is only opened once** (`plugin.Open` caches by path). The config loads:

| Plugin | `plugin.Open()` calls |
|--------|----------------------|
| `watchlist.so` | 1 (trigger) + 0 (bgworker, cached) = 1 CGo call |
| `ondiskagg.so` | 1 (first trigger) + 0 (2nd, cached) + 0 (3rd, cached) = 1 CGo call |
| `streamreplay.so` | 1 call |
| `massive.so` | 1 call |

**Total: 4 `plugin.Open()` CGo calls at startup.** This creates a small, fixed number of threads — not the primary contributor.

## Factor 3: Backfill Goroutine Churn (Thread Accumulation Engine)

The massive plugin creates **many goroutines that enter syscalls concurrently**:

### REST Backfill (`massive.go:442-591`)
- Creates a worker pool with `parallelism` goroutines (`massive.go:490`): `worker.NewWorkerPool(mf.ctx, parallelism)` — default `runtime.NumCPU()`
- Creates a writer pool: `worker.NewWorkerPool(mf.ctx, 1)` (`massive.go:491`)
- For each symbol (potentially thousands from `symbols_dsn`), submits work to the pool
- Each work item calls `rest.Bars()`, `rest.Trades()`, or `rest.Quotes()`

### Per-symbol REST calls (`backfill/rest/rest.go`)
- **Bars** (`rest.go:180-210`): Splits date ranges into chunks, then spawns goroutines per chunk with a semaphore of `dayFetchParallelism = runtime.NumCPU()` (`rest.go:25`). Each goroutine spawns another goroutine to close the results channel (`rest.go:213`).
- **Trades** (`rest.go:419-448`): Same pattern — one goroutine per market day with semaphore limiting.
- **Quotes** (`rest.go:563-592`): Same pattern.

### Flat file backfill (`backfill/flatfiles/backfill.go:90-247`)
- Creates download worker pool: `worker.NewWorkerPool(ctx, cfg.Parallelism)` (default 8, `backfill.go:107`)
- Creates `cfg.WriteConcurrency` writer goroutines (default 2, `backfill.go:147-160`)

### WebSocket streaming (`massive.go:186-230`)
- Per data type: `streamWithRestart()` goroutine (`massive.go:222,229`)
- Each WebSocket client spawns 2 goroutines: `readLoop()` and `writeLoop()` (`ws/client.go:243-244`)

### Thread creation cascade during backfill

With N symbols and M data types:
1. `parallelism` worker goroutines make concurrent HTTP requests
2. Each HTTP request -> DNS lookup -> CGo `getaddrinfo()` -> `entersyscall()`
3. Scheduler detaches P, needs new thread for remaining goroutines
4. Next goroutine picks up, also does HTTP -> DNS -> CGo -> new thread
5. Over 12 days with multiple backfill cycles (startup + reconnect gap-fills at `massive.go:364-371`), hundreds/thousands of threads accumulate

## Factor 4: Reconnect-Triggered Backfills

When a WebSocket stream drops and reconnects, `stream()` at `massive.go:364-371` triggers a **full backfill in a fire-and-forget goroutine**:
```go
if isReconnect && len(mf.config.QueryStart) > 0 {
    go func() {
        if err := mf.runBackfill(); err != nil { ... }
    }()
}
```

Over 12 days, each reconnect creates a new backfill cycle with its own worker pools, HTTP clients, and goroutine cascades. Each cycle creates new threads that **never get released**.

## Factor 5: HTTP Transport Connection Pool

The HTTP client at `massive.go:461-467` is configured with:
```go
Transport: &http.Transport{
    MaxIdleConnsPerHost: 100,
    MaxConnsPerHost:     100,
}
```

This allows up to 100 concurrent connections per host. Each connection involves TCP dial (DNS + connect syscalls). However, the `http.Client` is created fresh per `runBackfill()` call, and old transports may leak idle connections if not explicitly closed.

## What is NOT Contributing

| Suspected cause | Status | Why |
|---|---|---|
| `lib/pq` CGo | **Not CGo** | `lib/pq` is pure Go. No `import "C"` found in its source. |
| `pgx` CGo | **Not CGo** | `jackc/pgx` is pure Go. No `import "C"` found. |
| `gorilla/websocket` CGo | **Not CGo** | Pure Go. No `import "C"` found. |
| `runtime.LockOSThread()` in app code | **None found** | Zero matches in both codebases. |
| Explicit CGo in app code | **None found** | Zero `import "C"` in either codebase. |
| Goroutine leak | **Unlikely** | Only 18 goroutines at the time of measurement. Goroutines spawned during backfill cycles are spawned, do work (including CGo syscalls that pin an M), and exit cleanly. The OS threads created to service them while pinned remain parked in the runtime — Go never releases them. The pattern is "goroutines came and went, threads stayed", not a goroutine leak. |

## The Math

Rough calculation for thread accumulation over 12 days:

- **Startup backfill**: N symbols x M data types x chunks x DNS calls. If N=500 symbols, M=3 data types, each symbol makes ~5-10 HTTP requests -> ~7,500-15,000 DNS lookups, creating up to ~500 threads (capped by `concurrentThreadsLimit` in `net/rlimit_unix.go`).
- **Daily WebSocket reconnects**: Each reconnect triggers gap-fill backfill -> new worker pools -> new HTTP clients -> new DNS lookups -> new threads.
- **Flat file S3 downloads**: Hundreds of market days x S3 downloads -> more DNS lookups -> more threads.
- **12 days of operation**: Multiple backfill cycles compound. Even if each cycle only adds 50-100 threads beyond the previous peak, 12 days of market-hours reconnects accumulate to thousands.

### Why the count exceeds 500 (the DNS cap)

The CGo DNS path is capped at ~500 concurrent threads via `threadLimit` in `net/net.go:811`. Once that cap is reached, additional `getaddrinfo` calls **block** rather than spawning new threads. Yet we observe 3,612 threads — over 7x the DNS cap. The DNS resolver is therefore necessary but not sufficient to explain the full count. Other contributors that cause `entersyscall()` and detach P's, leading the scheduler to spawn fresh M's:

1. **Blocking HTTP body reads** on slow/large REST responses. While the goroutine is in `read()` syscall on a TCP socket, its M is pinned. With 100 max concurrent connections per host (`MaxConnsPerHost`) across multiple hosts (`api.massive.com`, `files.massive.com`, `socket.massive.com`, PostgreSQL, S3), this alone can pin hundreds of M's during peak backfill.
2. **TLS handshake syscalls** during connection establishment (handshake involves multiple read/write syscalls on the raw socket before the TLS state machine progresses).
3. **PostgreSQL connection pool I/O** (pgxpool / lib/pq): each pool connection that's reading or writing is in a syscall.
4. **File I/O during writer pool** (executor writes, WAL flushes triggered by writer goroutines).
5. **S3 download body reads** in the flat file backfill worker pool.

Each of these can cause the scheduler to call `newm()` independently of the DNS path. The DNS resolver is the most prolific *single* source, but the >500 surplus is the sum of all blocking-syscall sources accumulated across 12 days of backfill cycles. This is why fixing DNS alone may reduce — but not eliminate — the long-term growth.

## Recommendations

### 0. Confirm the diagnosis empirically (do this first)

Before applying any fix, capture `/debug/pprof/threadcreate` from a running instance that exhibits the high thread count:

```
go tool pprof http://<host>:<pprof-port>/debug/pprof/threadcreate
```

The profile attributes every thread creation to the goroutine stack that triggered `newm()`. If `cgoLookupIP` / `doBlockingWithCtx` dominates, the DNS hypothesis is confirmed and Recommendation #1 will be highest impact. If something else dominates (e.g., HTTP body reads, file I/O, pgxpool), prioritize accordingly. Without this, the analysis below is plausible but not proven for this specific deployment.

Also collect `runtime.NumThread` (via `/proc/self/status`'s `Threads:` field) and `runtime.NumGoroutine()` over time to track growth.

### 1. Force pure-Go DNS resolver (highest impact, lowest risk)

Set `GODEBUG=netdns=go` in the process environment, or build with `-tags netgo`. This eliminates CGo DNS calls entirely, preventing the largest single thread-creation mechanism. The pure-Go resolver runs lookups on regular goroutines that don't pin an M during the lookup.

Caveats:
- The pure-Go resolver reads `/etc/resolv.conf` and `/etc/hosts` directly. Behavior in unusual NSS setups (LDAP, mDNS, custom NSS modules) will differ — verify hostname resolution still works in the deployment environment.
- `GODEBUG=netdns=go+1` enables debug logging to confirm the resolver in use.

### 2. Reduce backfill parallelism on high-core machines

`BackfillParallelism` defaults to `runtime.NumCPU()` (`contrib/massive/massive.go:485-488`). On a 32-core host this means 32 concurrent symbol workers, each potentially in a syscall (TCP read, TLS handshake, DNS) and each potentially pinning an M. Set `backfill_parallelism` explicitly in config to a smaller fixed value (e.g., 4-8) regardless of NumCPU. This is a config-only change with no code modification.

### 3. Reuse HTTP clients across backfill cycles

`runBackfill()` creates a new `http.Client` and `http.Transport` each call (`contrib/massive/massive.go:461`). Promoting these to long-lived fields on `MassiveFetcher` allows the connection pool to persist across reconnect-triggered backfills, which reduces TCP dials, TLS handshakes, and DNS lookups.

Caveat: `http.Transport` already caches connections internally while idle, but evicts them after `IdleConnTimeout`. The win here is avoiding *transport reconstruction* on every reconnect cycle, not eliminating per-request DNS (which only Recommendation #1 fully addresses).

### 4. Add a DNS cache (optional, only if #1 is not viable)

If pure-Go DNS cannot be used (e.g., deployment requires CGo NSS for some reason), inject a `DialContext` that caches resolved IPs for a few minutes. Libraries like `github.com/rs/dnscache` are common choices. This bounds DNS-driven thread creation regardless of resolver choice.

### 5. Set `GOMAXPROCS` conservatively (situational)

Reducing P count modestly reduces simultaneous-syscall pressure, which can lower peak thread creation. This is a tuning lever, not a fix — useful primarily if the host has many more cores than the workload genuinely needs.

### 6. `runtime/debug.SetMaxThreads` — use with extreme caution

**Warning**: per `runtime/debug/garbage.go:121-135`, exceeding `SetMaxThreads` **crashes the program** (it is not a recoverable panic). The default is 10,000. With current observed thread count of 3,612 and growing, setting this anywhere near the current count would risk taking down production.

Acceptable use: set to a value comfortably above the historical peak (e.g., 8,000) purely as a safety ceiling to prevent runaway thread creation from exhausting the OS rather than as a diagnostic tool. Do **not** use it as a "get a stack trace when threads grow" mechanism — the program will be dead before you can read the trace.

If diagnostic visibility into thread creation is the actual goal, use the `threadcreate` pprof profile (Recommendation #0) instead.
