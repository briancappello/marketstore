## Purpose

Delivers the master's replication log to each replica in order, without gaps, at the replica's own pace. A replica can resume exactly where it stopped after a disconnect or a restart.

## ADDED Requirements

### Requirement: A replica requests the stream from its cursor
A replica SHALL persist the sequence number of the last transaction group it applied durably (its cursor). When it connects, it SHALL request the stream starting at cursor + 1. A replica with no cursor SHALL start at the master's current last sequence + 1, and SHALL rely on bootstrap backfill for earlier data.

#### Scenario: Resume after restart
- **WHEN** a replica with cursor 5000 restarts while the master has logged up to 5200
- **THEN** it receives entries 5001 to 5200 in order, followed by live entries

#### Scenario: Resume after disconnect
- **WHEN** a replica's connection drops after it applied entry 7000 and reconnects 10 minutes later
- **THEN** it receives entry 7001 next

### Requirement: Entries are delivered in order and without gaps
The master SHALL send each entry to a stream exactly once, in sequence order, with its sequence number. The replica SHALL check that each received sequence is exactly cursor + 1. If it is not, the replica SHALL reconnect from cursor + 1, and SHALL NOT apply the out-of-order entry.

#### Scenario: Gap detected
- **WHEN** the replica has cursor 100 and receives entry 102
- **THEN** it does not apply 102, it reconnects requesting 101, and it then applies 101 and 102 in order

#### Scenario: Ticks are not lost
- **WHEN** the master writes 10,000 trade ticks for AAPL during a backfill burst that the replica drains slowly
- **THEN** the replica's `AAPL/1Sec/TRADE` bucket ends with the same 10,000 ticks as the master, with no duplicates

### Requirement: Each stream reads at its own pace
Each replica stream SHALL read the log from its own cursor. A slow or stalled stream SHALL NOT delay the master, the log, or any other replica's stream. Lag SHALL be exposed per replica as metrics (sequence lag and byte lag).

#### Scenario: One slow replica
- **WHEN** replica R1 stops reading while replica R2 keeps up
- **THEN** R2's lag stays near zero and R1's lag grows, with no effect on the master

### Requirement: Resume points outside retention are reported
If a replica requests a sequence that is older than the log's first sequence, or that falls in a range the log marked broken, the master SHALL reply with a gap notice that gives the first sequence it can serve. The replica SHALL then fill the gap through the change ledger (see `replication/change-ledger`), raise a metric, and continue the stream from the sequence the master gave.

#### Scenario: Cursor past retention
- **WHEN** a replica with cursor 100 reconnects and the log's first sequence is 50,000
- **THEN** it is told about the gap 101-49,999, heals fixed buckets through the change ledger, and streams from 50,000

### Requirement: Mixed versions degrade, never fail
A master SHALL serve a replica that sends no start sequence as a best-effort live stream from the current tail, as today. A replica that receives entries without sequence numbers SHALL apply them as today, and SHALL log once that resume is unavailable.

#### Scenario: Old replica, new master
- **WHEN** a replica built before this change connects to a new master
- **THEN** it receives live entries from the current tail, and nothing on the master fails
