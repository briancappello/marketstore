## Purpose

Keeps every committed transaction group on the master in a retained, sequenced log, so replicas can catch up on anything they missed instead of losing it.

## ADDED Requirements

### Requirement: Every committed transaction group is logged with a sequence number
When replication is enabled, the master SHALL append every transaction group it commits to the WAL to the replication log, before primary files are written, and with no drops. Each entry SHALL get a sequence number that is exactly one greater than the previous entry's. Sequence numbers SHALL continue across master restarts. A flush that commits no transaction group SHALL NOT consume a sequence number.

#### Scenario: Consecutive sequence numbers
- **WHEN** the master commits three transaction groups, and between them flushes once with nothing to write
- **THEN** the log holds three entries with consecutive sequence numbers

#### Scenario: Restart continues the sequence
- **WHEN** the last entry before a master restart has sequence 1000 and the master commits a new transaction group after restarting
- **THEN** the new entry has sequence 1001

### Requirement: The log is retained by size and age
The master SHALL keep log entries until the log is larger than `replication.log_max_bytes` (default 20 GiB) or an entry is older than `replication.log_max_age` (default 48h). Then it SHALL delete the oldest entries first. Retention SHALL NOT depend on replica progress. The master SHALL expose the log's first and last sequence number, its size and the age of its oldest entry as metrics.

#### Scenario: Size limit
- **WHEN** appending an entry would make the log exceed `log_max_bytes`
- **THEN** the oldest entries are deleted until it fits, and the first-sequence metric rises accordingly

#### Scenario: Slow replica does not pin the log
- **WHEN** a replica stays disconnected for longer than `log_max_age`
- **THEN** the master still deletes entries older than `log_max_age`

### Requirement: Logging never blocks the write path on a replica
Appending to the log SHALL depend only on the master's own disk. No replica's state (slow, disconnected or crashed) SHALL delay a WAL flush. If an append fails (for example, the disk is full), the master SHALL keep accepting writes, mark the log broken from that sequence on, log an error and raise a metric. Replicas SHALL then be told that the stream has a gap at that point.

#### Scenario: Replica stalled
- **WHEN** a replica stops reading its stream during a 4M-row backfill on the master
- **THEN** the master's WAL flush latency is unchanged, and every transaction group is in the log

#### Scenario: Log disk full
- **WHEN** an append fails because the disk is full
- **THEN** master writes still succeed, the log-broken metric is 1, and a resuming replica is told about the gap

### Requirement: Recent entries are served from a byte-bounded memory tail
The master SHALL keep the most recent log entries in memory, up to `replication.hot_tail_bytes` (default 256 MiB). A stream whose cursor is inside the tail SHALL be served from memory. A stream whose cursor is older SHALL be served from disk. The tail SHALL be bounded by bytes, not by message count.

#### Scenario: Caught-up replica
- **WHEN** a replica's cursor is within the last 256 MiB of entries
- **THEN** its stream is served without reading the log from disk

#### Scenario: Lagging replica
- **WHEN** a replica's cursor is older than the memory tail but still within retention
- **THEN** its stream is served from disk, and it receives every entry in order
