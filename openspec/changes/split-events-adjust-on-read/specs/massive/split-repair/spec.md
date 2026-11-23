## Purpose

Gives operators one explicit command that replaces split-adjusted bars, written before this change, with raw bars. It touches only the symbols and date ranges that a split could have affected.

## ADDED Requirements

### Requirement: Affected ranges are derived from splits and stored data
For each symbol and each stored OHLCV bar bucket (`<SYMBOL>/<TIMEFRAME>/OHLCV`), the repair command SHALL find the latest split for that symbol whose effective date is after the bucket's first stored bar and not later than today. If such a split exists, the affected range SHALL be from the bucket's first stored bar to the end of the trading day before that split's effective date. If no such split exists, the bucket SHALL NOT be touched. The command SHALL read split history from Massive, so that it works even when the server has never synced splits. Trade and quote buckets SHALL NOT be touched, because Massive never adjusts them.

#### Scenario: Split inside stored range
- **WHEN** `NVDA/1Min/OHLCV` starts at 2024-01-02 and NVDA split on 2024-06-10
- **THEN** the affected range for that bucket is 2024-01-02 to 2024-06-07 inclusive

#### Scenario: Split before stored range
- **WHEN** `AAPL/1Min/OHLCV` starts at 2024-01-02 and AAPL's latest split was 2020-08-31
- **THEN** `AAPL/1Min/OHLCV` is not touched

### Requirement: Dry run lists the plan without writing
With `--dry-run`, the command SHALL print each affected symbol, bucket and range, plus totals, and SHALL write nothing.

#### Scenario: Dry run
- **WHEN** the repair command runs with `--dry-run`
- **THEN** it prints the affected buckets and ranges, and no bucket on the server changes

### Requirement: Repair rewrites affected ranges with raw bars
Without `--dry-run`, the command SHALL fetch unadjusted bars for each affected range from Massive and write them to the same bucket, through the server's write API. For one symbol, it SHALL write timeframes from finest to coarsest. Running the command again SHALL give the same stored data. The command SHALL accept a symbol filter and SHALL take the same connection and parallelism options as the REST backfill command.

#### Scenario: Adjusted history replaced
- **WHEN** `NVDA/1D/OHLCV` holds the adjusted close 12.10 for 2024-06-07, and the repair runs
- **THEN** `NVDA/1D/OHLCV` holds the raw close (about 121.0) for 2024-06-07, and `adjust('split')` returns about 12.10 again

#### Scenario: Re-run
- **WHEN** the repair command runs twice in a row with no new splits in between
- **THEN** the second run writes the same values, and the stored data does not change

### Requirement: Repair reports its outcome
The command SHALL report the number of buckets repaired, bars written and failures. It SHALL exit non-zero if any bucket failed. A failed bucket SHALL NOT stop the other buckets.

#### Scenario: One symbol fails
- **WHEN** fetching bars for one affected symbol fails after retries
- **THEN** the other buckets are repaired, the failure is reported by symbol, and the command exits non-zero
