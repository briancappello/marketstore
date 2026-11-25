## Purpose

Makes the replica apply received transaction groups fast enough to drain master backfill bursts, while keeping order and the meaning of its resume cursor.

## ADDED Requirements

### Requirement: Received transaction groups are applied in batches
The replica SHALL apply consecutive received transaction groups in batches, with one WAL flush per batch. A batch SHALL close when it reaches `replication.replay_batch_max` entries (default 1000), when it reaches `replication.replay_batch_bytes` (default 64 MiB), when it reaches `replication.replay_batch_commands` write commands (default 250,000), or when no new entry has arrived for `replication.replay_batch_wait` (default 50ms). Writes in a batch SHALL be applied in sequence order. A later write to the same bucket and epoch SHALL win.

#### Scenario: Burst drained in batches
- **WHEN** 5,000 transaction groups arrive back to back
- **THEN** the replica applies them in at most 5 batches of up to 1,000 entries each, and ends with the same data as applying them one at a time

#### Scenario: Live trickle stays fresh
- **WHEN** one transaction group arrives and no other follows
- **THEN** it is applied within 50ms plus one flush

### Requirement: A batch and its cursor become durable together
The replica SHALL commit a batch's writes and the batch's last sequence number (the new cursor) in a single WAL transaction group. After a crash, WAL replay SHALL restore both or neither. The replica SHALL then resume from the restored cursor + 1. Because WAL replay is idempotent for variable records (see `storage/wal-replay`), a batch whose primary writes completed before a crash SHALL NOT add rows when it is replayed. No entry SHALL be applied twice, so tick buckets never get duplicate rows.

#### Scenario: Crash after a batch flush
- **WHEN** the replica process crashes right after the WAL flush and primary writes of batch 2001-3000, before the next checkpoint
- **THEN** after restart its cursor is 3000, it requests 3001, and its tick buckets hold each tick of 2001-3000 exactly once

#### Scenario: Crash before a batch flush
- **WHEN** the replica crashes while batch 2001-3000 is being built, before its flush
- **THEN** after restart its cursor is 2000, it requests 2001, and the batch is applied once

### Requirement: Replay throughput is measured
The replica SHALL expose metrics for entries applied per second, batch sizes and the age of the newest applied entry.

#### Scenario: Throughput visible
- **WHEN** the replica drains a burst
- **THEN** the applied-per-second metric shows the drain rate, and the lag metrics return to near zero when it finishes
