# massive/split-ingestion Specification

## Purpose
Keeps the corporate-event store's `SPLITS` buckets in sync with Massive's split history, so that raw Massive data can be adjusted on read.

## Requirements

### Requirement: Splits come from the Massive splits endpoint
The massive plugin SHALL read split events from `GET /stocks/v1/splits`. It SHALL read every page of the result. It SHALL map each result to the event store like this:
- `execution_date` → `Epoch`, at 00:00 America/New_York of that date
- `split_from` → `SplitFrom`, and `split_to` → `SplitTo`
- `adjustment_type` → `Subtype`: `forward_split`, `reverse_split` or `stock_dividend`

A result with an unknown `adjustment_type`, or with a `split_from` or `split_to` that is not positive, SHALL be skipped and logged. The rest of the sync SHALL continue.

#### Scenario: Forward split mapped
- **WHEN** Massive returns `{ticker: "NVDA", execution_date: "2024-06-10", split_from: 1, split_to: 10, adjustment_type: "forward_split"}`
- **THEN** `NVDA/1D/SPLITS` holds a row at 2024-06-10 00:00 ET with factor 10.0 and subtype forward split

#### Scenario: Malformed result
- **WHEN** one result has `split_from` = 0
- **THEN** that result is skipped with a log line, and every other result is written

### Requirement: Sync scope
A sync SHALL cover the tracked symbols: the configured symbol set (static `symbols` or the `symbols_query` result) plus every symbol that already has data in this MarketStore instance. When the configured set is `["*"]`, every symbol that Massive returns SHALL be tracked. For each tracked symbol, a sync SHALL write that symbol's full split history and SHALL neutralize stored splits that Massive no longer returns.

#### Scenario: Symbol added only through dynamic ticks
- **WHEN** TSLA is not in the configured symbols, but has a `TSLA/TRADE` bucket from a dynamic tick subscription
- **THEN** the next sync writes TSLA's splits

#### Scenario: Untracked symbol
- **WHEN** Massive returns a split for a symbol that is not configured and has no data in this instance, and the configured set is not `["*"]`
- **THEN** no bucket is created for that symbol

### Requirement: Sync schedule
The plugin SHALL run a sync shortly after startup, and again once a day at a configurable time (`splits_sync_time`, America/New_York, default `03:30`). A sync SHALL NOT wait for, or block, bar backfill or streaming. A failed sync SHALL be retried on the next schedule and SHALL NOT stop the plugin. Setting `splits_sync: false` SHALL turn syncing off. The default SHALL be on. The `--no-backfill` flag SHALL NOT turn syncing off.

#### Scenario: Startup sync
- **WHEN** the plugin starts with default config
- **THEN** a split sync runs, and splits are written while streaming is already running

#### Scenario: API failure
- **WHEN** the splits endpoint returns HTTP 500 for every retry
- **THEN** the plugin logs the failure, keeps streaming, and tries again at the next scheduled time

#### Scenario: Auth failure
- **WHEN** the splits endpoint returns HTTP 403
- **THEN** the sync stops with a logged auth error and does not retry until the next scheduled time

### Requirement: Sync outcome is observable
Each sync SHALL log the number of results read, events written, events neutralized and results skipped. The plugin SHALL expose the time of the last successful sync as a metric.

#### Scenario: Successful sync
- **WHEN** a sync completes
- **THEN** one log line reports the counts, and the last-successful-sync metric equals the completion time
