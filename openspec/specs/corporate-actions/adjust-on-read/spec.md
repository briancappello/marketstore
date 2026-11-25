# corporate-actions/adjust-on-read Specification

## Purpose
Puts unadjusted stored data on the current share basis at query time, using the events in the corporate-event store. Raw data stays raw on disk and is never out of date.

## Requirements

### Requirement: Events come from the corporate-event store
The `adjust` query function SHALL read events only from the corporate-event store. It SHALL NOT read the ICE `<SYMBOL>/1D/ACTIONS` bucket. If a symbol has no events, `adjust` SHALL return its input unchanged.

#### Scenario: No events
- **WHEN** `adjust('split')` runs on `MSFT/1Min/OHLCV` and MSFT has no stored splits
- **THEN** the result equals the unadjusted query result

#### Scenario: ICE events are ignored
- **WHEN** `AAPL/1D/ACTIONS` holds an ICE split row and `AAPL/1D/SPLITS` is empty
- **THEN** `adjust('split')` on AAPL data returns it unchanged

### Requirement: Arguments select event kinds
`adjust('split')` SHALL apply `SPLITS` events only. `adjust('dividend')` SHALL apply dividend events only (no dividend kind exists yet). `adjust()` with no argument, or with both arguments, SHALL apply every kind it supports. Argument matching SHALL ignore case.

#### Scenario: Dividend-only adjust
- **WHEN** `adjust('dividend')` runs on a symbol that only has split events
- **THEN** the result equals the unadjusted query result

### Requirement: Factors apply to rows before the effective time
For each row, `adjust` SHALL compute a cumulative price factor and a cumulative volume factor. Each is the product of the matching factor of every applied event whose `Epoch` is later than the row's timestamp. Rows at or after an event's `Epoch` SHALL NOT be affected by that event.

#### Scenario: Boundary at the execution date
- **WHEN** a 1:4 split takes effect on 2024-06-10, and the query returns a 1Min bar at 2024-06-07 19:59 ET and a 1Min bar at 2024-06-10 04:00 ET
- **THEN** the 2024-06-07 bar's prices are divided by 4, and the 2024-06-10 bar is unchanged

#### Scenario: Two splits compound
- **WHEN** a symbol has a 1:2 split effective 2020-01-02 and a 1:3 split effective 2022-01-03
- **THEN** a bar from 2019 has prices divided by 6, and a bar from 2021 has prices divided by 3

### Requirement: Only events already in effect apply
`adjust` SHALL ignore every event whose `Epoch` is later than the time the query runs. A split announced for a future date SHALL NOT change any data until that date begins.

#### Scenario: Announced split
- **WHEN** a split for 2026-12-01 is stored and `adjust('split')` runs on 2026-11-23
- **THEN** no row is adjusted for that split

### Requirement: Columns are adjusted by role, for every numeric type
`adjust` SHALL divide price columns by the cumulative price factor and multiply size columns by the cumulative volume factor. Column roles SHALL be decided by name:
- price: `Open`, `High`, `Low`, `Close`, `VWAP`, `Price`, `BidPrice`, `AskPrice`
- size: `Volume`, `Size`, `BidSize`, `AskSize`

This SHALL work for `float32`, `float64`, `int64` and `uint64` columns. The output column SHALL keep its input type. Integer columns SHALL be rounded to the nearest integer. Float price columns SHALL be rounded to 4 decimal places. Every other column, including `Epoch` and `Nanoseconds`, SHALL be returned unchanged.

#### Scenario: float32 OHLCV bar
- **WHEN** a 1D bar with `float32` Close 400.0 and `int64` Volume 1000 is from before a 1:4 split
- **THEN** the adjusted bar has `float32` Close 100.0 and `int64` Volume 4000

#### Scenario: Trade size goes up, not down
- **WHEN** a trade with `float64` Price 400.0 and `float64` Size 10 is from before a 1:4 split
- **THEN** the adjusted trade has Price 100.0 and Size 40

#### Scenario: Reverse split rounds sizes
- **WHEN** a bar with `int64` Volume 1005 is from before a 10:1 reverse split
- **THEN** the adjusted Volume is 101, and the prices are multiplied by 10

#### Scenario: Unknown columns pass through
- **WHEN** a trade row has an `Exchange` byte column and a `Cond1` byte column
- **THEN** both are returned unchanged

### Requirement: New events become visible promptly
Once an event is written, `adjust` SHALL apply it to queries that start no more than 60 seconds later. This SHALL hold whether the event was written in-process or through the write API. Concurrent queries SHALL be safe to run while events are being written.

#### Scenario: Split synced while the server is running
- **WHEN** a split is written to `AAPL/1D/SPLITS` and an `adjust('split')` query on AAPL starts 60 seconds later
- **THEN** the result reflects the new split
