## Purpose

Lets a replica that missed more than the replication log retains find exactly which bucket ranges changed, so it can heal fixed buckets without a full resync and knows which tick buckets it cannot heal.

## ADDED Requirements

### Requirement: The master records changed ranges per bucket
For every logged transaction group, the master SHALL record, for each bucket it wrote, the sequence number and the epoch range written. The ledger SHALL coalesce entries so that its size depends on the number of buckets and a bounded number of ranges per bucket, not on the number of writes. It SHALL keep at least `replication.ledger_max_age` of history (default 30 days), and SHALL survive master restarts.

#### Scenario: Coalesced history
- **WHEN** a bucket receives 100,000 writes over a day, all within the same two calendar days of epochs
- **THEN** the ledger holds a bounded number of ranges for that bucket, which together cover both days

### Requirement: A replica can ask what changed since a sequence
The master SHALL answer a request "changes since sequence S" with, for each bucket written after S, the epoch ranges written and the bucket's record type (fixed or variable). If S is older than the ledger's history, the answer SHALL say so.

#### Scenario: Changes since a cursor
- **WHEN** a replica asks for changes since sequence 100, and since then the master rewrote `NVDA/1Min/OHLCV` for 2026-11-20 and appended ticks to `AAPL/1Sec/TRADE` on 2026-11-24
- **THEN** the answer lists the NVDA 1Min range for 2026-11-20 as fixed and the AAPL trade range for 2026-11-24 as variable

### Requirement: Fixed buckets are healed from the ledger
On a gap notice, the replica SHALL re-pull from the master every listed range of a fixed bucket, whatever its age, through the existing bucket backfill, and SHALL write the result idempotently. Only then SHALL it continue the stream.

#### Scenario: Old correction healed
- **WHEN** the master corrected `MSFT/1Min/OHLCV` for 2026-11-20 while the replica was beyond retention
- **THEN** after the replica resumes, its MSFT 1Min bars for 2026-11-20 equal the master's

### Requirement: Unhealable tick gaps are reported
For a variable bucket listed in a gap, the replica SHALL NOT re-pull the range. Re-pulling appends, so it would duplicate rows. It SHALL log the bucket and range, and increment `replication_unrecoverable_tick_gaps_total`. Only ranges first written before the gap's end are considered; later ones arrive through the stream. To support this, the ledger's ranges carry their first and last sequence.

#### Scenario: Tick gap past retention
- **WHEN** a gap lists `AAPL/1Sec/TRADE` for 2026-11-24
- **THEN** no AAPL trades are re-pulled, the log line names the bucket, epochs and sequences, and `replication_unrecoverable_tick_gaps_total` increases. The metric has no per-bucket labels, because a series per bucket and range would be unbounded

### Requirement: Ledger past its history falls back to a deep resync
If the change ledger cannot cover the replica's cursor, the replica SHALL log that a full resync is required, raise a metric and keep streaming. It SHALL NOT delete local data automatically.

#### Scenario: Replica offline for two months
- **WHEN** a replica's cursor is older than the ledger's history
- **THEN** it logs that a full resync is required, and continues with the live stream
