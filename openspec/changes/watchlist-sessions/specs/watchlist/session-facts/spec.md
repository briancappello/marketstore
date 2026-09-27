## Purpose

Stores per-symbol, per-trading-date session facts (each session's volume and closing price) as derived data, so that session baselines and per-session volume medians do not need to rescan 1Min history. Also sets the rules that this derived data must follow.

## ADDED Requirements

### Requirement: Session facts bucket
The system SHALL keep one row per symbol per completed trading date in the bucket `<SYMBOL>/1D/SESSIONS`, with these columns:
- `PreVolume`, `RegVolume`, `PostVolume`: each session's total volume from 1Min bars
- `PreClose`, `RegClose`, `PostClose`: the last 1Min close of each session
- `Version`: the definition version the row was computed with

A session with no bars SHALL be recorded so that it can be told apart from a session that has not been computed yet. The bucket SHALL be readable through the normal query APIs.

#### Scenario: Facts for a completed day
- **WHEN** a trading date's afterhours session has ended for a symbol that traded in all three sessions
- **THEN** `<SYMBOL>/1D/SESSIONS` holds a row for that date with all three volumes, both closes and the current `Version`

#### Scenario: Session with no trades
- **WHEN** a symbol had no afterhours bars on a trading date
- **THEN** that date's row records that afterhours had no bars, and afterhours-based baselines for the next premarket treat the symbol as having no data

### Requirement: Store facts, not rolling statistics
The bucket SHALL store only per-date facts. Rolling statistics, such as volume medians and moving averages, SHALL be computed from the facts when they are read. They SHALL NOT be stored.

#### Scenario: Changing the median window
- **WHEN** the configured median window changes from 50 to 20 trading dates
- **THEN** relative volume uses a 20-date median at once, with no rebuild of stored data

### Requirement: Rebuildable from source bars
1Min bars SHALL be the source of truth. Every row SHALL be fully recomputable from the 1Min bars of its trading date. Rows written live and rows written by a rebuild SHALL be identical for the same bars.

#### Scenario: Rebuild equals live
- **WHEN** the rows for a range of dates are deleted and rebuilt from unchanged 1Min bars
- **THEN** the rebuilt rows equal the rows that were written live

### Requirement: Versioned definition
Each row SHALL record the definition version it was computed with. A reader SHALL treat a row whose version is not the current version as missing.

#### Scenario: Old-version row
- **WHEN** a row has `Version` 1 and the current definition is version 2
- **THEN** the row is not used, and the value is obtained as if the row were missing

### Requirement: Late data invalidates facts
When 1Min bars of an already-computed trading date are written or rewritten (for example by a backfill or an outage fill), the system SHALL recompute that symbol's row for that date.

#### Scenario: Gap fill after the day ended
- **WHEN** an outage fill writes 14:00–14:30 regular-session bars for a completed trading date
- **THEN** that symbol's `RegVolume` for that date is recomputed to include the filled bars

### Requirement: Rebuild command
The system SHALL provide one command that rebuilds or backfills session facts. It SHALL accept a date range and, optionally, a list of symbols. It SHALL be safe to run while the server is serving live traffic, and running it twice SHALL give the same result.

#### Scenario: Initial backfill
- **WHEN** the rebuild command runs for the last 60 trading dates on a database that has 1Min history and no session facts
- **THEN** every symbol with 1Min bars on those dates has a row per date

#### Scenario: Repeated run
- **WHEN** the rebuild command runs twice over the same range with unchanged bars
- **THEN** the second run leaves the rows unchanged

### Requirement: Defined fallback for missing facts
When a needed row is missing or has an old version, the system SHALL compute the value from that date's 1Min bars. When the 1Min bars are missing too, the symbol SHALL be treated as having no data for that value.

#### Scenario: Missing row with bars present
- **WHEN** a row for date D is missing and the 1Min bars for D exist
- **THEN** baselines and medians that need D give the same result as when the row exists

#### Scenario: Missing row and bars
- **WHEN** both the row and the 1Min bars for date D are missing
- **THEN** the symbol has no data for D and is handled by the ranking-basis drop-out rule

### Requirement: Only the leader writes
Only a leader (non-replica) instance SHALL write session facts or run their invalidation. A replica SHALL NOT attempt those writes. It SHALL read the facts that it receives through replication.

#### Scenario: Replica at end of day
- **WHEN** afterhours ends on a replica instance
- **THEN** the replica writes no session facts, and it attempts none

#### Scenario: Replica reads replicated facts
- **WHEN** the leader has written session facts for date D
- **THEN** a replica that has replicated them uses them for baselines and medians
