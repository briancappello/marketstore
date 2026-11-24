# Massive Data Fetcher

This module builds a MarketStore background worker which streams real-time
market data from the [Massive WebSocket API](https://massive.com/docs/websocket/quickstart)
and optionally backfills historical data from the
[Massive REST API](https://massive.com/docs/rest/stocks/aggregates/custom-bars).
It runs as a goroutine behind the MarketStore process and writes incoming bars,
trades, and quotes directly to disk.

## Configuration

`massive.so` is built as a plugin, so you configure it in the MarketStore
configuration file under `bgworkers`.

### Options

| Name                  | Type              | Default                    | Description                                                                               |
|-----------------------|-------------------|----------------------------|-------------------------------------------------------------------------------------------|
| `api_key`             | string            | (required)                 | Your Massive API key                                                                      |
| `base_url`            | string            | `https://api.massive.com`  | REST API base URL                                                                         |
| `ws_server`           | string            | `wss://socket.massive.com` | WebSocket server URL                                                                      |
| `ws_data_types`       | []string          | `["1Min"]`                 | Data types to stream via WebSocket: `1Min`, `1Sec`, `quotes`, `trades`                    |
| `symbols`             | []string          | `["*"]`                    | Symbols to subscribe to (use `*` for all)                                                 |
| `symbols_dsn`         | string            | (none)                     | PostgreSQL connection string for dynamic symbol lookup (overrides `symbols` if set)       |
| `symbols_query`       | string            | (none)                     | SQL query returning a single column of symbols (required when `symbols_dsn` is set)       |
| `query_start`         | map[string]string | (none)                     | Start dates per frequency for backfill. Keys are timeframes (e.g., `1Min`, `1D`) or `trades`/`quotes`. |
| `backfill_batch_size` | int               | `50000`                    | Pagination limit for REST API backfill                                                    |
| `splits_sync`         | bool              | `true`                     | Sync split events from Massive into `{symbol}/1D/SPLITS` (see [Adjusted prices](#adjusted-prices)) |
| `splits_sync_time`    | string            | `03:30`                    | Daily split sync time, `HH:MM` America/New_York                                           |

### Example

```yaml
bgworkers:
  - module: massive.so
    name: Massive
    config:
      api_key: your_api_key
      ws_server: wss://socket.massive.com
      # Stream 1-second bars, 1-minute bars, and trades in real-time
      ws_data_types: ["1Sec", "1Min", "trades"]
      symbols:
        - AAPL
        - SPY
      # Backfill is controlled separately via query_start
      query_start:
        1Min: "2024-01-01"
        1D: "2020-01-01"
        trades: "2024-06-01"
```

Note: `ws_data_types` controls real-time WebSocket streaming, while `query_start`
controls which data types are backfilled. They are independent - you can stream
`1Min` bars without backfilling them, or backfill `1D` bars without streaming them.

### Dynamic Symbols from PostgreSQL

Instead of defining a static symbol list, you can query symbols from a PostgreSQL
database at startup. This is useful when managing symbols through an external system.

```yaml
bgworkers:
  - module: massive.so
    name: Massive
    config:
      api_key: your_api_key
      ws_data_types: ["1Min"]
      # Query symbols from PostgreSQL instead of using static list
      symbols_dsn: "postgres://user:password@localhost:5432/mydb?sslmode=disable"
      symbols_query: "SELECT symbol FROM tracked_symbols WHERE active = true"
      query_start:
        1Min: "2024-01-01"
```

When `symbols_dsn` is set:
- The `symbols` field is ignored
- The query must return a single column containing symbol strings
- If the database connection fails or returns no symbols, the plugin fails to start
- Symbols are fetched once at startup (restart MarketStore to reload symbols)

#### Per-Symbol Listing Dates

The database query can optionally return a second column with the symbol's listing
date. When a listing date is provided and is more recent than the global `query_start`,
backfilling for that symbol starts from the listing date instead.

This is useful for newer symbols (e.g., IPOs) where requesting data before their
listing date would be wasteful.

```yaml
bgworkers:
  - module: massive.so
    name: Massive
    config:
      api_key: your_api_key
      ws_data_types: ["1Min"]
      symbols_dsn: "postgres://user:password@localhost:5432/mydb?sslmode=disable"
      # Query returns (symbol, listing_date) pairs
      symbols_query: "SELECT symbol, listing_date FROM tracked_symbols WHERE active = true"
      query_start:
        1Min: "2020-01-01"  # Global start, but newer symbols use their listing date
```

**Listing date behavior:**
- If `listing_date` is `NULL`, the global `query_start` is used
- If `listing_date` is earlier than `query_start`, the global `query_start` is used
- If `listing_date` is more recent than `query_start`, backfilling starts from `listing_date`
- If `listing_date` is in the future, backfilling is skipped (but WebSocket streaming still subscribes)

**Supported date formats:**
- PostgreSQL `DATE` type
- PostgreSQL `TIMESTAMP` or `TIMESTAMPTZ` (time portion is ignored)
- Text in `YYYY-MM-DD` format

### Testing with Mock Server

A mock WebSocket server compatible with this plugin is available at
`../fin/fin/ws/data_server.py`. To test locally:

```bash
# Start the mock server
python ../fin/fin/ws/data_server.py

# Configure the plugin with the mock server
# ws_server: ws://localhost:8765
```

## Backfill

### Automatic (in-process)

Set `query_start` in the plugin config. On startup, the plugin starts
WebSocket streaming immediately, then backfills historical data concurrently
for all configured symbols from `query_start` to now. This ensures no
real-time data is missed even during long backfills. Any overlap between
backfill and streaming data is harmless since duplicate writes are idempotent.

Note: wildcard (`*`) symbols are not supported for in-process backfill - use
the standalone backfiller CLI instead.

### Standalone CLI

For large-scale backfills across many symbols, use the standalone backfiller
binary which supports parallel downloads and glob-based symbol filtering:

```bash
# Build the backfiller
make -C contrib/massive

# Run it (backfill 1Min and 1D bars with different start dates)
massive_backfiller \
  -apiKey YOUR_KEY \
  -bars \
  -barFrequencies "1Min,1D" \
  -from 1Min=2024-01-01 \
  -from 1D=2020-01-01 \
  -to 2025-01-01 \
  -symbols "A*" \
  -parallelism 8 \
  -config /etc/mkts.yml

# Backfill trades
massive_backfiller \
  -apiKey YOUR_KEY \
  -trades \
  -from trades=2024-06-01 \
  -to 2025-01-01 \
  -symbols "AAPL,MSFT" \
  -config /etc/mkts.yml
```

#### CLI Flags

| Flag              | Default                   | Description                                     |
|-------------------|---------------------------|-------------------------------------------------|
| `-apiKey`         | (required)                | Massive API key                                 |
| `-baseURL`        | `https://api.massive.com` | REST API base URL override                      |
| `-bars`           | false                     | Backfill bars                                   |
| `-barFrequencies` | `1Min`                    | Comma-separated list of bar timeframes          |
| `-trades`         | false                     | Backfill tick trades                            |
| `-quotes`         | false                     | Backfill NBBO quotes                            |
| `-from`           | (required)                | Start date per type as `freq=date` (repeatable) |
| `-to`             | today                     | End date (YYYY-MM-DD)                           |
| `-symbols`        | `*`                       | Glob pattern for symbols                        |
| `-parallelism`    | NumCPU                    | Number of parallel API workers                  |
| `-batchSize`      | 50000                     | API pagination limit                            |
| `-config`         | `/etc/mkts.yml`           | Path to MarketStore config                      |
| `-dir`            | (from config)             | Override mktsdb data directory                  |

## Adjusted prices

Every bar this plugin writes is **unadjusted** (raw), from every source: the
WebSocket stream, flat files, REST backfill and reconnect gap-fill. REST
requests always send `adjusted=false`, and a response that reports
`adjusted: true` is rejected. The `backfill_adjusted` option is removed:
`true` stops the plugin at startup, `false` only logs a deprecation warning.

The plugin also syncs split events (forward splits, reverse splits and stock
dividends) from `GET /stocks/v1/splits` into the corporate-event store, in the
bucket `{symbol}/1D/SPLITS`. It syncs at startup and daily at
`splits_sync_time`. Every tracked symbol is covered: the configured symbols
plus every symbol that already has data in this instance.

To get split-adjusted data, apply the `adjust` function when you query:

```python
import pymarketstore as pymkts
p = pymkts.Params('NVDA', '1D', 'OHLCV', functions=["adjust('split')"])
```

Prices (`Open`, `High`, `Low`, `Close`, `VWAP`, `Price`, `BidPrice`,
`AskPrice`) are divided by the split factor, and sizes (`Volume`, `Size`,
`BidSize`, `AskSize`) are multiplied by it, for every row before the split's
execution date. Splits that have been announced but have not started yet are
ignored.

### Repairing bars stored adjusted

Before this change, REST backfill stored split-adjusted bars by default.
Those bars are wrong for every symbol that split after they were fetched.
The repair command fixes them. For each `{symbol}/{timeframe}/OHLCV` bucket, it
finds the latest split after the bucket's first bar. It then fetches raw bars
from that first bar up to the split and compares them with the stored bars. It
rewrites, through the running server's gRPC API, only the stored bars whose
prices differ from raw by more than `-tolerance` (default 1%). Bars that are
already raw are kept as stored: flat-file bars have more price decimals than
REST bars, which are rounded to 4. Trade and quote buckets are never touched,
because Massive never adjusts them. Running it again rewrites nothing.

```bash
go build -o repair ./contrib/massive/backfill/repair/cmd

# 1. Review the plan: every affected bucket and range, nothing is written.
./repair -grpc localhost:5995 -config mkts.yml -dry-run

# 2. Repair. -timeframes and -symbols narrow the run.
./repair -grpc localhost:5995 -config mkts.yml -timeframes 1Min,1D
```

The last line reports the number of buckets checked, the number of adjusted
bars rewritten and the failures. The exit code is non-zero if any bucket failed;
re-run it to retry. The API key comes from `-apiKey`, `MASSIVE_API_KEY`,
`POLYGON_API_KEY`, or the massive bgworker's `api_key` in `-config`.

## Data Types

### Bars (`AM`)
Minute-bar OHLCV aggregates written to `{symbol}/1Min/OHLCV`.

### Trades (`T`)
Tick-level trades written to `{symbol}/1Sec/TRADE`.

### Quotes (`Q`)
NBBO quotes written to `{symbol}/1Min/QUOTE`.
