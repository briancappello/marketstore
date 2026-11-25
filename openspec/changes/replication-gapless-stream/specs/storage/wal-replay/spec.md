## Purpose

Makes WAL replay after a crash leave every bucket exactly as a crash-free run would, including variable-length (tick) buckets, whose writes append instead of overwriting.

## ADDED Requirements

### Requirement: Replay does not re-append variable writes that already reached primary storage
When WAL replay re-applies a transaction group, it SHALL skip each variable-length write whose primary write had already completed before the crash, and SHALL apply each one that had not. Fixed-length writes SHALL be replayed as before. This SHALL hold on the master and on replicas, and SHALL NOT depend on replication being configured.

#### Scenario: Crash after primary writes
- **WHEN** a transaction group appending 100 AAPL trades is fully written to primary storage, and the process is killed before the next checkpoint
- **THEN** after restart and WAL replay the bucket holds those 100 trades once

#### Scenario: Crash before primary writes
- **WHEN** the process is killed after the WAL fsync of that transaction group but before its primary writes
- **THEN** after restart and WAL replay the bucket holds the 100 trades once

#### Scenario: Crash in the middle of primary writes
- **WHEN** a transaction group writes trades to AAPL and MSFT, and the process is killed after the AAPL write but before the MSFT write
- **THEN** after restart and WAL replay each bucket holds its trades once

### Requirement: Repeated writes to one block in a transaction group
When one transaction group carries several variable-length writes to the same bucket file and interval, they SHALL be committed as a single write whose result equals applying them in order.

#### Scenario: Two flushes' worth of ticks in one group
- **WHEN** two write requests append trades for the same second of `AAPL/1Sec/TRADE` before one flush
- **THEN** a read returns the same rows, in the same order, as when each request was flushed on its own

### Requirement: The WAL format stays readable by older binaries
The information replay uses SHALL be carried so that a binary built before this change parses new transaction groups, both from the WAL and from the replication stream, exactly as before. A WAL written by an older binary SHALL replay as before.

#### Scenario: Old replica, new master
- **WHEN** an old replica receives a transaction group written by a new master
- **THEN** it parses and applies the same write commands as before this change
