## Why

Replication from taichi to p1 loses data. The master drops a transaction group whenever one of its two 500-message buffers is full: 2.85M were dropped in the 29h after the 2026-11-24 reboot, which is 4.7% of writes. The drops come in bursts during backfills, which is also when old bars get corrected. The replica's catch-up passes cannot see a correction that is below its watermark and older than the 24h deep-pass lookback, so p1 kept wrong 11/20-11/24 bars with no way to heal. Tick buckets (TRADE/QUOTE) have no catch-up path at all, so every dropped tick is lost for good. `docs/design/logical_replication_design.md` §9 deferred "old corrections", "tick backfill" and "sequence-gap detection". In practice those gaps are hit every time taichi runs a catch-up backfill.

## What Changes

- **E: gapless, resumable stream.**
  - The master appends every transaction group it commits to a retained, disk-backed **replication log**, with a per-message sequence number.
  - Each replica stream reads the log from its own cursor, at its own pace. A slow replica falls behind; it no longer loses messages.
  - The replica persists the last sequence number it applied, and resumes from it after a reconnect or restart. Any gap is filled from the log, ticks included.
  - The master never blocks on a replica (§3 isolation is kept).
- **C: byte-bounded hot tail.** Recent log entries are kept in a memory buffer capped by bytes instead of a fixed message count, so a replica that keeps up is served from memory. The fixed 500-message channels are gone.
- **D: change ledger for replicas past retention.**
  - The master keeps a compact per-bucket record of which epoch ranges were written, and in which sequence range.
  - A replica whose cursor is older than the log's retention asks for the ranges changed since its cursor, and re-pulls exactly those for fixed buckets.
  - For tick buckets past retention, the gap cannot be refilled without duplicating rows. It is reported (metric + log), not guessed at.
- **B: faster replay on the replica.** The replica applies received messages in batches, with one write and one WAL flush per batch instead of one per message. A replica then drains backfill bursts faster than the master produces them. A measurement task confirms the current bottleneck first.
- **Observability.** Metrics for replication lag (sequence numbers and bytes), log size and retention, ledger heals and unrecoverable tick gaps. A drop now only happens when a replica falls past retention.
- **BREAKING (replication protocol):** `GetWALStream` gains a start-sequence request field, and the responses carry sequence numbers. Master and replicas must be upgraded together. A new-master/old-replica pair keeps working only as a best-effort stream.
- One-time rollout step: copy p1's missing tick history from taichi while p1 is stopped. History from before the replica's first cursor is out of scope for the protocol.

## Capabilities

### New Capabilities
- `replication/replication-log`: the master's retained, sequenced replication log, its retention, and the byte-bounded hot tail.
- `replication/resumable-stream`: cursor-based streaming, replica resume, gap detection and gap fill, and master isolation.
- `replication/change-ledger`: per-bucket change ranges for replicas past retention, plus fixed-bucket heal and tick-gap reporting.
- `replication/batched-replay`: how the replica applies received transaction groups in batches, with durability and ordering.

### Modified Capabilities
<!-- None: replication has no main specs yet in openspec/specs/. -->

## Impact

- **Code:**
  - `replication/` (sender, grpc_server, grpc_client, receiver, replay, and backfill driver integration)
  - `executor/wal.go` (handing committed transaction groups to the log)
  - `proto/replication.proto` plus regenerated code
  - `internal/di/replication.go`
  - `utils/config.go` (new `replication` options)
- **Disk:** a new log directory on the master (default bounded by bytes and age), and a small cursor file on each replica.
- **Ops:** taichi and p1 must be deployed together. `deep_heal_interval`/`backfill_lookback` stay as a safety net. `docs/design/logical_replication_design.md` is updated.
- **Follow-on:** the ta-droid `feat/tauri-port` merge waits until this change is verified end to end.
