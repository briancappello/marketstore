# MarketStore

A database server optimized for financial time-series data — built for algorithmic trading, real-time streaming, and quantitative research.

MarketStore stores OHLCV bars, trades, and quotes in a compact, memory-mapped columnar format. It provides real-time WebSocket streaming with curation-aware routing, on-disk aggregation pipelines, and an extensible plugin framework for custom data feeds and analytics.

## Quickstart

### Prerequisites

- Go 1.24+
- PostgreSQL (for symbol universe sync via the Massive plugin)

### Build

```bash
git clone https://github.com/briancappello/marketstore.git
cd marketstore

# Build the server binary
make build

# Build all plugins (massive, ondiskagg, watchlist, streamreplay)
make plugins
```

### Configure

Edit `mkts.yml` to set your data directory, ports, and plugin configuration. See the [Configuration](#configuration) section below for details.

### Run

```bash
# Initialize data directory (first time only)
./marketstore init

# Start the server
./marketstore start
```

The server listens on:
- **:5993** — HTTP (JSON-RPC + WebSocket at `/ws`)
- **:5995** — gRPC

### Custom Watchlists

To deploy custom curation/watchlist logic, build a custom `watchlist.so` from a separate repo (see [marketstore-watchlists](../marketstore-watchlists)):

```bash
cd ../marketstore-watchlists
make install    # builds and copies watchlist.so into marketstore/build/bin/
```

---

## Architecture

### Data Flow Overview

```
                        ┌─────────────────────────────────────┐
                        │         External Data Source         │
                        │  (Polygon, Alpaca, flat files, etc.) │
                        └──────────────────┬──────────────────┘
                                           │
                          REST backfill + WebSocket streaming
                                           │
                                           ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                            MARKETSTORE SERVER                                 │
│                                                                              │
│  ┌─────────────────┐     ┌────────────────────────────────────────────────┐ │
│  │  Massive Plugin  │     │              Trigger Pipeline                  │ │
│  │  (BgWorker)      │     │                                                │ │
│  │                  │     │  TRADE write ──► ondiskagg ──► 1Sec OHLCV      │ │
│  │ • WS streaming   │────►│  1Sec write  ──► ondiskagg ──► 1Min OHLCV      │ │
│  │ • REST backfill  │     │  1Min write  ──► ondiskagg ──► 1D   OHLCV      │ │
│  │ • Flat file load │     │                                                │ │
│  │ • Symbol sync    │     │  */1Sec/* write ──► watchlist trigger           │ │
│  │   (PostgreSQL)   │     │  */1Min/* write ──► watchlist trigger           │ │
│  └─────────────────┘     └────────────────────────┬───────────────────────┘ │
│                                                    │                         │
│                                                    ▼                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                     Watchlist Trigger (watchlist.so)                    │ │
│  │                                                                        │ │
│  │  For each tick:                                                        │ │
│  │    1. Read latest bar from disk                                        │ │
│  │    2. Update per-symbol running state (high, low, volume, etc.)        │ │
│  │    3. Evaluate Curator: is this symbol curated?                        │ │
│  │       • YES → stream.Push()      (broadcast to wildcard subscribers)   │ │
│  │       • NO  → stream.PushDirect() (direct subscribers only)            │ │
│  │                                                                        │ │
│  │  Periodically (every 1s):                                              │ │
│  │    4. Run WatchlistStrategy.Rank() for each registered strategy        │ │
│  │    5. Push WATCHLISTS/TimeFrame/NAME updates                           │ │
│  │    6. Push CURATION/TimeFrame/CHANGES if curated set changed           │ │
│  └────────────────────────────────────┬───────────────────────────────────┘ │
│                                       │                                      │
│                                       ▼                                      │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                    WebSocket Stream (frontend/stream)                   │ │
│  │                                                                        │ │
│  │  Subscriber routing:                                                   │ │
│  │    • Push()       → delivered to ALL matching subscribers              │ │
│  │    • PushDirect() → delivered only to subscribers with a concrete      │ │
│  │                     symbol in their pattern (not wildcards)             │ │
│  └──────────┬─────────────────────┬─────────────────────┬────────────────┘ │
│             │                     │                     │                    │
└─────────────┼─────────────────────┼─────────────────────┼────────────────────┘
              │                     │                     │
              ▼                     ▼                     ▼
     ┌──────────────┐    ┌───────────────────┐    ┌─────────────────┐
     │ Web Frontend  │    │ Mobile App        │    │ Trading System  │
     │ (fa-techan)   │    │ (ta-droid)        │    │ (nautilus)      │
     │               │    │                   │    │                 │
     │ */1Min/OHLCV  │    │ */1Min/OHLCV      │    │ AAPL/1Min/OHLCV │
     │ WATCHLISTS/*  │    │ WATCHLISTS/*      │    │ TSLA/1Min/OHLCV │
     │ CURATION/*    │    │ CURATION/*        │    │ (specific syms) │
     └──────────────┘    └───────────────────┘    └─────────────────┘
```

### Three-Tier Data Model

```
┌─────────────────────────────────────────────────────────────────┐
│  TIER 1: Full Universe (internal)                               │
│  All 10K+ symbols written by the Massive data feeder.           │
│  Not exposed externally. State tracked for all symbols.         │
├─────────────────────────────────────────────────────────────────┤
│  TIER 2: Curated Universe (the public data interface)           │
│  Symbols meeting the Curator's criteria:                        │
│    • Min price threshold (e.g., $0.10)                          │
│    • Min dollar volume rate (liquidity proxy)                   │
│    • Dynamic: sleepy symbols promoted on volume surge           │
│  Subscribers to */TimeFrame/* only see curated symbols.         │
├─────────────────────────────────────────────────────────────────┤
│  TIER 3: Watchlists (ranked subsets of the curated universe)    │
│  Named, ordered lists: TOP_GAINERS, MOMENTUM, SMA_CROSS_UP...  │
│  Pushed periodically under WATCHLISTS/TimeFrame/NAME.           │
└─────────────────────────────────────────────────────────────────┘
```

### Curation-Aware WebSocket Routing

The watchlist trigger acts as the sole outbound WS gateway. It uses two push modes:

| Mode | Function | Who receives | When |
|------|----------|--------------|------|
| Broadcast | `stream.Push()` | All matching subscribers (including `*/1Min/OHLCV` wildcards) | Symbol is curated |
| Direct | `stream.PushDirect()` | Only subscribers with a concrete symbol pattern (e.g., `AAPL/1Min/OHLCV`) | Symbol is NOT curated |

This means:
- A client subscribing to `*/1Min/OHLCV` receives ticks only for curated symbols
- A trading system subscribing to `AAPL/1Min/OHLCV` receives AAPL ticks regardless of curation status (needed for position exit decisions)
- Wildcards in non-symbol positions work as expected: `AAPL/1Min/*` gets all attribute groups for AAPL

---

## Plugins

### Massive (Data Feeder)

`contrib/massive/massive.so` — Background worker that ingests market data.

**Capabilities:**
- WebSocket streaming of real-time trades and quotes
- REST API backfill for historical 1Min and 1D bars
- Flat file bulk loading
- PostgreSQL-based symbol universe synchronization
- Configurable worker pool for parallel backfill

**Data path:**
```
Massive WS receives trade → writes to SYMBOL/1Sec/TRADE
  → ondiskagg fires: aggregates into SYMBOL/1Sec/OHLCV
    → ondiskagg fires: aggregates into SYMBOL/1Min/OHLCV
      → ondiskagg fires: aggregates into SYMBOL/1D/OHLCV
      → watchlist trigger fires: curation + push to WS subscribers
```

### OnDiskAgg (Aggregation)

`contrib/ondiskagg/ondiskagg.so` — Trigger that aggregates bars into higher timeframes on write.

Configured as a chain:
```yaml
triggers:
  - module: ondiskagg.so
    on: "*/1Sec/TRADE"
    config:
      destinations: [1Sec]    # TRADE → 1Sec OHLCV
  - module: ondiskagg.so
    on: "*/1Sec/OHLCV"
    config:
      destinations: [1Min]    # 1Sec → 1Min
  - module: ondiskagg.so
    on: "*/1Min/OHLCV"
    config:
      destinations: [1D]      # 1Min → 1D
```

### Watchlist (Curation + Ranking Framework)

`contrib/watchlist/watchlist.so` — Trigger + BgWorker that replaces `stream.so` as the sole outbound WebSocket gateway.

**Framework architecture:**
```
contrib/watchlist/
  framework/       ← importable Go library (the public API)
    interfaces.go    Curator, WatchlistStrategy interfaces
    registry.go      RegisterCurator(), RegisterWatchlist()
    trigger.go       Fire(): state update → curation → Push/PushDirect
    worker.go        Run(): baselines → ranking loop
    state.go         SymbolStateManager (thread-safe singleton)
    symbol_state.go  SymbolState with Extra map[string]interface{}
    baseline.go      Historical data queries for precomputation
    push.go          Message envelope construction
    rankings.go      Cross-symbol ranking engine
  defaults/        ← shipped implementations
    noop_curator.go    All symbols pass (no filtering)
    pct_change.go      PCT_CHANGE_UP, PCT_CHANGE_DOWN
    volume.go          VOLUME_UP, VOLUME_DOWN
```

**Plugin extensibility:** The default `watchlist.so` registers a no-op curator and four basic strategies. Custom logic lives in a separate repo that imports `contrib/watchlist/framework`, registers its own `Curator` and `WatchlistStrategy` implementations, and compiles a replacement `watchlist.so`. See [marketstore-watchlists](../marketstore-watchlists) for a full example.

**Message format:** All WS messages use a consistent envelope:
```json
{"msg_type": "bar|trade|quote|curation_change|watchlist_update", "payload": {...}}
```

### StreamReplay

`contrib/streamreplay/streamreplay.so` — Background worker that replays historical data through the stream on startup or on schedule. Useful for testing downstream clients without waiting for market hours.

---

## Configuration

The server is configured via `mkts.yml`. Key sections:

### Server

```yaml
root_directory: data
listen_port: 5993
grpc_listen_port: 5995
timezone: "America/New_York"
```

### Attribute Group Types (Data Schemas)

```yaml
attrgroup_types:
  OHLCV:
    record_type: fixed
    columns:
      - [Open, FLOAT32]
      - [High, FLOAT32]
      - [Low, FLOAT32]
      - [Close, FLOAT32]
      - [Volume, INT64]
  TRADE:
    record_type: variable
    columns:
      - [Price, FLOAT64]
      - [Size, UINT32]
      - [Exchange, BYTE]
      # ...
  QUOTE:
    record_type: variable
    columns:
      - [BidPrice, FLOAT64]
      - [AskPrice, FLOAT64]
      # ...
```

### Triggers

```yaml
triggers:
  # Watchlist trigger: sole outbound WS gateway
  - module: watchlist.so
    on: "*/1Min/*"
    config:
      curation:
        min_price: 0.10
        min_dollar_vol_rate: 1000.0
        lookback_secs: 300
      watchlists:
        - name: PCT_CHANGE_UP
          limit: 100
        - name: MOMENTUM
          limit: 50

  # Aggregation chain
  - module: ondiskagg.so
    on: "*/1Sec/OHLCV"
    config:
      destinations: [1Min]
```

### Background Workers

```yaml
bgworkers:
  - module: watchlist.so
    name: WatchlistBaselines
    config:
      baseline_lookback_days: 60
      median_window: 50
      ranking_interval_ms: 1000

  - module: massive.so
    name: Massive
    config:
      api_key: ${MASSIVE_API_KEY}
      ws_url: "wss://..."
      # ...
```

---

## WebSocket Protocol

### Connecting

```
ws://localhost:5993/ws
```

### Subscribing

Send a msgpack-encoded message:
```json
{"action": "subscribe", "tbks": ["*/1Min/OHLCV", "WATCHLISTS/1Min/*"]}
```

Response:
```json
{"action": "subscribed", "tbks": ["*/1Min/OHLCV", "WATCHLISTS/1Min/*"]}
```

### Subscription Patterns

Patterns follow glob syntax with `/` as separator (`*` matches any single segment):

| Pattern | Receives |
|---------|----------|
| `*/1Min/OHLCV` | All curated symbols, 1Min bars |
| `AAPL/1Min/OHLCV` | AAPL specifically (regardless of curation) |
| `AAPL/1Min/*` | All attribute groups for AAPL 1Min |
| `*/1Min/*` | All curated symbols, all attribute groups |
| `WATCHLISTS/1Min/*` | All watchlist ranking updates |
| `WATCHLISTS/1Min/MOMENTUM` | Only MOMENTUM watchlist updates |
| `CURATION/1Min/CHANGES` | Curation set membership changes |

### Message Types

All messages are msgpack-encoded with the structure:
```
{key: "SYMBOL/TimeFrame/AttrGroup", data: {msg_type: "...", payload: {...}}}
```

**Bar tick** (`*/TimeFrame/OHLCV`):
```json
{
  "msg_type": "bar",
  "payload": {
    "symbol": "AAPL",
    "epoch": 1681234567,
    "open": 150.0,
    "high": 151.2,
    "low": 149.8,
    "close": 151.0,
    "volume": 1234567
  }
}
```

**Watchlist update** (`WATCHLISTS/TimeFrame/NAME`):
```json
{
  "msg_type": "watchlist_update",
  "payload": {
    "name": "MOMENTUM",
    "timeframe": "1Min",
    "symbols": [
      {"symbol": "AAPL", "rank": 1, "momentum_score": 8.3, "pct_change": 5.2},
      {"symbol": "NVDA", "rank": 2, "momentum_score": 7.1, "pct_change": 4.8}
    ]
  }
}
```

**Curation change** (`CURATION/TimeFrame/CHANGES`):
```json
{
  "msg_type": "curation_change",
  "payload": {
    "added": [{"symbol": "SMCI", "reason": "meets_criteria"}],
    "removed": [{"symbol": "XYZ", "reason": "below_criteria"}],
    "curated_count": 3847
  }
}
```

---

## Development & Testing

### Repository Structure

```
├── cmd/start/              Server entry point, plugin loading
├── executor/               Storage engine (WAL, writer, reader)
├── planner/                Query planning
├── catalog/                Catalog directory tree management
├── frontend/
│   ├── stream/             WebSocket pub/sub (Push, PushDirect, Handler)
│   └── ...                 HTTP/gRPC endpoints
├── plugins/
│   ├── trigger/            Trigger plugin interface + Record type
│   └── bgworker/           Background worker plugin interface
├── contrib/
│   ├── massive/            Data feeder plugin (backfill + streaming)
│   ├── ondiskagg/          On-disk aggregation trigger
│   ├── watchlist/          Curation + watchlist framework
│   │   ├── framework/      The importable library
│   │   └── defaults/       Default curator + strategies
│   ├── stream/             Legacy stream trigger (superseded by watchlist)
│   └── streamreplay/       Stream replay worker
├── utils/                  Shared utilities (config, timeframe, IO)
├── internal/di/            Dependency injection container
├── tests/integ/            Docker-based integration tests
└── mkts.yml                Server configuration
```

### Running Tests

```bash
# Unit tests (all packages)
make unit-test

# Stream + watchlist tests specifically
go test -v ./frontend/stream/ ./contrib/watchlist/...

# Import/CSV integration tests (requires build)
make build
make import-csv-test

# Docker-based integration tests
make integration-test-jsonrpc
make integration-test-grpc
```

### Test Architecture

**Unit tests** use Go's standard `testing` package with `t.TempDir()` for isolation:
```go
rootDir := t.TempDir()
cfg := utils.NewDefaultConfig(rootDir)
cfg.BackgroundSync = false
c := di.NewContainer(cfg)
executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
```

**Watchlist integration tests** (`contrib/watchlist/framework/integration_test.go`) boot the full pipeline in-process:
- Temp directory per test (auto-cleaned)
- In-process MarketStore storage engine
- `httptest.NewServer` for the WS endpoint
- Mock Curator/WatchlistStrategy implementations injected per test
- Data written via `executor.WriteCSM()`, trigger fired directly
- `gorilla/websocket` test clients subscribe and assert received messages
- No Docker, no external services, no wall-clock dependencies
- Ranking triggered synchronously via `worker.TriggerRanking()` for determinism

**Integration tests** (`tests/integ/`) use Docker:
- MarketStore built into a Docker image
- Python `pymarketstore` client in a separate container
- Pytest exercises write/query/subscribe round-trips

### Building Plugins

All plugins are built as Go `.so` shared objects:

```bash
# Build all plugins
make plugins

# Build a specific plugin
make -C contrib/watchlist

# Build with debug symbols
make -C contrib/watchlist debug
```

**Important**: Plugins must be compiled with the exact same Go version as the main binary. Mismatched versions cause `plugin.Open()` failures at runtime.

### Adding a New Trigger Plugin

1. Create `contrib/myplugin/myplugin.go` (package main)
2. Export `NewTrigger(conf map[string]interface{}) (trigger.Trigger, error)`
3. Implement `trigger.Trigger` interface (single method: `Fire(keyPath string, records []trigger.Record)`)
4. Add a Makefile following the `contrib/ondiskagg/Makefile` pattern
5. Add to root `Makefile` `plugins:` target
6. Configure in `mkts.yml` under `triggers:` with an `on:` glob pattern

### Adding a New Background Worker Plugin

1. Same structure, but export `NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error)`
2. Implement `bgworker.BgWorker` interface (single method: `Run()`)
3. Configure in `mkts.yml` under `bgworkers:`

### Custom Watchlist Plugin Development

The watchlist plugin is designed for external extension. See [marketstore-watchlists](../marketstore-watchlists) for a complete example including:
- Custom `Curator` (liquidity-based filtering)
- Custom `WatchlistStrategy` implementations (relative volume, momentum, SMA crossover)
- Build and deployment instructions

The workflow:
1. Create a new repo with `go.mod` requiring `github.com/alpacahq/marketstore/v4`
2. Import `contrib/watchlist/framework` and `contrib/watchlist/defaults`
3. Implement `framework.Curator` and/or `framework.WatchlistStrategy`
4. Register in `init()`, delegate `NewTrigger`/`NewBgWorker` to framework
5. Build with `go build -buildmode=plugin -o watchlist.so .`
6. Deploy by replacing the default `watchlist.so`

### Useful Make Targets

| Target | Description |
|--------|-------------|
| `make build` | Build server binary + all plugins |
| `make plugins` | Build plugin .so files only |
| `make install` | Install binary to $GOPATH/bin |
| `make unit-test` | Run all Go unit tests |
| `make integration-test-jsonrpc` | Docker-based integration tests (JSON-RPC) |
| `make integration-test-grpc` | Docker-based integration tests (gRPC) |
| `make import-csv-test` | CSV import/export validation |
| `make fmt` | Format all Go source |
| `make debug` | Build all with debug symbols |
