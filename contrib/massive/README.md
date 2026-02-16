# Massive Data Fetcher

This module builds a MarketStore background worker which streams real-time
market data from the [Massive WebSocket API](https://massive.com/docs/websocket/quickstart)
and optionally backfills historical data from the
[Massive REST API](https://massive.com/docs/rest/stocks/aggregates/custom-bars).
It runs as a goroutine behind the MarketStore process and writes incoming bars,
trades, and quotes directly to disk.

This plugin replaces the legacy Polygon fetcher (`contrib/polygon`).

## Configuration

`massive.so` is built as a plugin, so you configure it in the MarketStore
configuration file under `bgworkers`.

### Options

| Name                  | Type              | Default                    | Description                                                                               |
|-----------------------|-------------------|----------------------------|-------------------------------------------------------------------------------------------|
| `api_key`             | string            | (required)                 | Your Massive API key                                                                      |
| `base_url`            | string            | `https://api.massive.com`  | REST API base URL                                                                         |
| `ws_server`           | string            | `wss://socket.massive.com` | WebSocket server URL                                                                      |
| `data_types`          | []string          | (required)                 | Data types to subscribe to: `bars`, `quotes`, `trades`                                    |
| `symbols`             | []string          | `["*"]`                    | Symbols to subscribe to (use `*` for all)                                                 |
| `query_start`         | map[string]string | (none)                     | Start dates per frequency. Keys are timeframes (e.g., `1Min`, `1D`) or `trades`/`quotes`. |
| `bar_frequencies`     | []string          | `["1Min"]`                 | Bar timeframes to backfill (e.g., `["1Min", "5Min", "1H", "1D"]`)                         |
| `backfill_batch_size` | int               | `50000`                    | Pagination limit for REST API backfill                                                    |
| `backfill_adjusted`   | bool              | `true`                     | Whether backfilled bars are split-adjusted                                                |

### Example

```yaml
bgworkers:
  - module: massive.so
    name: Massive
    config:
      api_key: your_api_key
      ws_server: wss://socket.massive.com
      data_types: ["bars", "trades"]
      symbols:
        - AAPL
        - SPY
      bar_frequencies: ["1Min", "1D"]
      query_start:
        1Min: "2024-01-01"
        1D: "2020-01-01"
        trades: "2024-06-01"
```

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

Set `query_start` in the plugin config. On startup, the plugin backfills
historical data for all configured symbols from `query_start` to now, then
starts streaming. Note: wildcard (`*`) symbols are not supported for
in-process backfill - use the standalone backfiller CLI instead.

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
| `-adjusted`       | true                      | Request split-adjusted bars                     |
| `-config`         | `/etc/mkts.yml`           | Path to MarketStore config                      |
| `-dir`            | (from config)             | Override mktsdb data directory                  |

## Data Types

### Bars (`AM`)
Minute-bar OHLCV aggregates written to `{symbol}/1Min/OHLCV`.

### Trades (`T`)
Tick-level trades written to `{symbol}/1Sec/TRADE`.

### Quotes (`Q`)
NBBO quotes written to `{symbol}/1Min/QUOTE`.
