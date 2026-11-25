## Purpose

Makes sure that every bar the massive plugin and its standalone tools write is unadjusted. All sources then share one share basis, and stored data never goes out of date.

## ADDED Requirements

### Requirement: REST bar requests are always unadjusted
Every request the massive plugin or its standalone tools send to `/v2/aggs/...` SHALL set `adjusted=false`. This covers startup backfill, reconnect gap-fill, the REST backfill command and the split repair command. A response whose `adjusted` field is `true` SHALL be rejected with an error and SHALL NOT be written.

#### Scenario: Backfill request
- **WHEN** the plugin backfills `AAPL/1Min/OHLCV`
- **THEN** every aggregates request has the query parameter `adjusted=false`

#### Scenario: Server ignores the flag
- **WHEN** an aggregates response has `"adjusted": true`
- **THEN** the bars are not written, and the request fails with an error that names the symbol

### Requirement: The backfill_adjusted key is removed
The plugin SHALL fail at startup, with an error that names the key and the fix, if `backfill_adjusted` is `true`. If `backfill_adjusted` is `false`, the plugin SHALL start and log a deprecation warning. If the key is missing, the plugin SHALL start with no warning.

#### Scenario: Old config with adjusted true
- **WHEN** `mkts.yml` sets `backfill_adjusted: true`
- **THEN** the plugin does not start, and the error says that `backfill_adjusted` is removed, that bars are always stored raw, and that `adjust('split')` gives adjusted data

#### Scenario: Old config with adjusted false
- **WHEN** `mkts.yml` sets `backfill_adjusted: false`
- **THEN** the plugin starts and logs one deprecation warning

### Requirement: The REST backfill command has no adjusted option
The standalone REST backfill command SHALL NOT accept an `-adjusted` flag. Passing it SHALL fail with an unknown-flag error.

#### Scenario: Old invocation
- **WHEN** the REST backfill command is run with `-adjusted=true`
- **THEN** it exits with an unknown-flag error before it sends any request
