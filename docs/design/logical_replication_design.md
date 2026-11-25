# Logical Replication Design

Status: implemented
Date: 2026-08-18, revised 2026-11-25 (gapless resumable stream: openspec change
`replication-gapless-stream`)
Author: brian

## 1. Problem

MarketStore ships a WAL-based master→replica replication feature
(`replication/`, `internal/di/replication.go`). Its live-write feed works and is
covered by CI (`tests/replication`), but it is unusable as a real replica
because it has **no bootstrap and no catch-up**: a replica only receives writes
that occur *after* it connects, pre-existing data is never transferred, and any
write missed during a disconnect is lost forever. It also has an isolation
defect: a slow or broken replica can stall the master's write path (see §3).

We want a replica for a **dev / analytics mirror** of a production instance on
the same LAN:

- **Freshness target:** seconds-stale is fine; minutes is not.
- **Hard constraint:** a broken, slow, or disconnected replica must **never**
  degrade the master's availability or write throughput. Replica failure is
  acceptable; master failure is not.
- **Scope:** deletes are **out of scope** (accepted divergence). Point-in-time
  consistency is not required; per-bucket eventual consistency is sufficient.

## 2. Approach

Replication is two independent problems: **bootstrap/catch-up** (copy existing
and missed data) and **live tail** (apply new writes). We use the right tool for
each and deliberately keep them decoupled:

- **Live tail = a sequenced, retained replication log, streamed over gRPC.**
  - *Master.* It appends every committed transaction group to a replication log (`replication/replog`) with a consecutive sequence number.
  - *Each replica stream* reads that log from its own cursor, at its own pace (`replication/grpc_server.go`).
  - *The replica* applies what it receives in batches, and commits each batch together with its cursor in one WAL transaction group (`replication/batch.go`). The decode and apply of tick records (`replication/replay.go`) is unchanged.
  - *Transport.* We do **not** build a WebSocket endpoint. Replication is server-to-server on the LAN, so gRPC's separate port costs nothing.

- **Bootstrap + catch-up = pull backfill (new).** The replica pulls historical
  ranges from the master's **existing** query API. No new server read endpoints
  are required:
  - enumerate every bucket with `ListSymbols(format=TBK)` →
    `catalog.ListTimeBucketKeyNames`;
  - for each bucket, `Query` the range `[watermark, now]`;
  - write locally via the normal write path (idempotent: `WriteCSM` overwrites
    by epoch);
  - advance a per-bucket watermark.

### 2.1 Correctness model

Completeness comes from the **stream**. The backfill reconciler is a safety net.

1. **Every committed transaction group is logged.** The master appends each
   transaction group to the replication log right after its WAL fsync, with a
   sequence number exactly one greater than the previous one. Empty flushes
   consume no sequence. The log is retained by size and age
   (`log_max_bytes`, `log_max_age`), never by replica progress.
2. **A replica resumes exactly.** Its cursor is the sequence of the last
   transaction group it applied, and it is committed in the same WAL
   transaction group as that data. After a crash, WAL replay restores both or
   neither. On (re)connect the replica asks for cursor + 1.
3. **Order is checked.** The replica applies an entry only if its sequence is
   cursor + 1. On a jump it reconnects at cursor + 1. Ticks are therefore
   neither lost nor duplicated while the log still holds them.
4. **A gap past retention is healed from the change ledger.**
   - *What it is.* A replica whose cursor the log no longer holds gets a gap notice. The master's change ledger records, per bucket, which epoch ranges were written and in which sequences.
   - *Healing.* The replica re-pulls every fixed range written in the gap, however old the epochs are, then streams from where the log resumes.
   - *Ticks* cannot be re-pulled without duplicating rows, so those ranges are reported (`replication_unrecoverable_tick_gaps_total`).
   - *Too far behind.* A cursor older than the ledger's history (`ledger_max_age`) is reported as "full resync required".
5. **The reconciler still runs.**
   - The periodic `[watermark + 1, now]` pass is unchanged. In steady state it finds nothing to write.
   - Deep passes run at startup and every `deep_heal_interval`, but no longer after each reconnect. A reconnect to a sequenced master loses nothing, so a deep pass is requested only after streaming from an unsequenced (old) master.
   - The reconciler's remaining job is to cover what the stream cannot know: bootstrap, and anything written before the replica's first cursor.

Fixed writes are idempotent by epoch, so the stream, the heal and the
reconciler may overlap freely. Tick writes are not idempotent, which is why they
come only from the stream, exactly once.

**Crash replay.** WAL replay is idempotent for tick records too, on the master
and on replicas (`executor/walreplay.go`; design D7 of the openspec change).
- *Before.* Replay used to re-append every tick written since the last checkpoint.
- *How.* The master records each variable file's size before a transaction group's primary writes, in a trailer that older parsers ignore. Replay skips a write whose index slot already points at or past that size.

## 3. Isolation: the master must never stall (required)

No replica state (slow, stalled, disconnected or crashed) may delay the
master's write path.

**History.**
- The first version pushed each transaction group through a 500-slot sender channel, then through 500-slot per-replica channels. A stalled replica blocked the WAL flush.
- The fix (F1–F3) made both hand-offs non-blocking: they dropped on overflow, with a guarded subscriber map.
- That removed the stall, but it turned overload into silent loss: 2.85M transaction groups dropped in 29h on taichi, in backfill bursts.

**Now the stream is pulled, not pushed.**
- **The write path only appends.** `FlushCommandsToWAL` hands each transaction group to the replication log under the WAL file lock (`fileMu`), after the WAL fsync.
  - The append is one `WriteAt` plus an in-memory update. The log is fsynced every 100 ms by its own goroutine, and a checkpoint's `syncfs` also covers it.
  - The append depends only on the master's own disk.
- **Each stream reads at its own pace.** A `GetWALStream` goroutine keeps its own cursor. It reads the byte-capped memory tail (`hot_tail_bytes`), or the segment files when it is behind, and it waits on a broadcast channel that every append closes.
  - Readers never hold the log's lock while they do disk I/O.
  - A slow replica only blocks its own `stream.Send` in gRPC flow control. It falls behind; it loses nothing until retention.
- **A failing log does not fail writes.** If an append fails (for example, the disk is full), the master keeps accepting writes and marks the log broken from that sequence.
  - The break is recorded in a pre-allocated `STATE` file, written in place, so a restart never reuses the lost sequences.
  - Readers wait at the break. Once an append succeeds again, it leaves a durable hole, and readers in it get a `log_broken` gap notice. The change ledger still records the lost groups.
- **Old replicas** (no `resumable` flag) are streamed from the tail as before. A gap is skipped for them and counted in `replication_dropped_messages_total`, because they cannot parse a gap notice.

## 4. Components

### Master

- `replication/replog` — the replication log.
  - *Layout.* Append-only segment files of CRC-checked entries (`seq | tgID | wallclock | len | crc32c | tgSerialized`).
  - *Sealing and startup.* The active segment is `<first>.seg`. A full one is sealed as `<first>-<last>.seg`, so startup scans only unsealed segments, and sealed ones are indexed on first read.
  - *Retention* deletes whole segments, oldest first.
  - *Memory tail.* The byte-capped tail and its append broadcast are in `tail.go`.
- `replication/masterlog.go` — `MasterLog`, the executor's `ReplicationLog`. It appends each committed transaction group to the log and records it in the ledger. WAL replay at startup appends groups whose `tgID` is newer than the log's last entry.
- `replication/ledger` — the change ledger.
  - *Ranges.* Per bucket, coalesced epoch ranges with their sequence span. A bucket keeps at most 64 ranges, beyond which the closest merge.
  - *Persistence.* Snapshotted every 10 minutes and at shutdown. At startup it is rebuilt from the snapshot plus the log after it.
- `replication/grpc_server.go` — `GetWALStream` (per-stream cursor reader), `ChangesSince` (ledger ranges, chunked), and per-stream lag metrics.

### Replica

A replica is a normal MarketStore instance started with `master_host` set.

- `replication/receiver.go` — requests cursor + 1, checks sequence continuity, and reconnects on a jump.
  - On a gap notice it heals through a `GapHealer`, then moves the cursor.
  - It still applies the unsequenced stream of an old master.
- `replication/batch.go` — `BatchApplier`.
  - *Batching.* Received entries are batched (`replay_batch_*`), and each batch is committed with its cursor in one WAL transaction group: `executor.BatchWriter` plus `WALFileType.CommitCommands`.
  - *Why it matters.* Batching gives about 138× today's apply rate on p1 (`notes.md` of the openspec change).
- `replication/cursor.go` — the cursor bucket, `__replication__/1D/CURSOR`.
  - *Hidden.* Names starting with `__` are internal (`catalog.IsInternal`): no listing, wildcard query or trigger sees them.
  - *January 2.* The row sits on 2000-01-02, because a `1D` row dated January 1 is unreadable (`io.TimeToIndex` is 0-based for `1D`).
- `replication/heal.go` — `LedgerHealer`. It re-pulls the fixed ranges of a gap with `backfill.HealRange`, which writes only rows that differ and leaves the watermark alone. Tick ranges are reported.
- `replication/backfill` — bootstrap and reconcile, unchanged except as noted in §2.1:
  - the backfill client, to the master's **main** gRPC service;
  - the watermark store;
  - the worker pool;
  - the driver.
- `marketstore tool replication set-cursor | status` — set a stopped replica's cursor (`--seq N` or `--master-tail`), or show a running one's cursor, its master's last sequence and the lag.

## 5. Data flow

Per write on the master:
1. The WAL commit.
2. The log append (sequence S).
3. The ledger update.
4. The primary writes.

Each stream picks up S from the memory tail, or from disk when it is behind. On the replica:
1. The receiver checks that S is cursor + 1.
2. The batcher adds S to the open batch.
3. The batch, plus cursor = S, is committed as one WAL transaction group.

Replica startup:

1. WAL replay restores the last committed batch and its cursor, or neither.
2. The stream resumes at cursor + 1; with no cursor yet, at the master's tail.
3. Bootstrap and periodic reconciles run alongside, as before.

## 6. Configuration

Existing (`utils/config.go` `ReplicationSetting`): `Enabled`, `MasterHost`
(replication stream, default port 5996), TLS, retry.

New fields required on the replica for backfill:

- `master_query_host` — the master's **main** gRPC address (e.g. `host:5995`)
  used by the backfill client. Distinct from `master_host` (the stream port).
- `reconcile_interval` — periodic reconcile cadence (default e.g. 5m).
- `backfill_lookback` — how far a deep pass reaches before the last complete
  deep pass (default 24h). Heals master-side corrections to epochs that were
  less than this old when made and were missed while disconnected or dropped.
  Only rows that differ are written; the cost is query volume.
- `deep_heal_interval` — how often the deep pass runs (default 24h).
- Optional: `backfill_parallelism`, backfill start bound.

Replication log, ledger and batched replay (`utils/config.go`). Byte sizes
accept `KiB`/`MiB`/`GiB`/`TiB`; an omitted key keeps its default.

| Key | Default | On | Meaning |
|---|---|---|---|
| `log_max_bytes` | 20GiB | master | log retention by size |
| `log_max_age` | 48h | master | log retention by age |
| `hot_tail_bytes` | 256MiB | master | newest log entries kept in memory |
| `ledger_max_age` | 720h | master | change ledger history; at least `log_max_age` |
| `replay_batch_max` | 1000 | replica | entries per batch |
| `replay_batch_bytes` | 64MiB | replica | bytes per batch |
| `replay_batch_commands` | 250000 | replica | write commands per batch |
| `replay_batch_wait` | 50ms | replica | longest a batch stays open after its first entry |

Note that `replication.enabled` means "act as a master": serve the stream and
keep the replication log. A replica is an instance with `master_host` set.

TLS is optional and off by default (LAN, trusted).

## 7. Testing

The gapless stream is covered by unit and integration tests in
`replication/...`, `executor/` and `cmd/tool/replication`:
- consecutive sequences across restarts;
- a stalled stream that delays neither the master nor other streams;
- no drop across a 100k-entry burst;
- gap notices;
- resume and jump handling;
- batched replay equal to per-entry replay;
- replica crash recovery with exact ticks;
- idempotent WAL replay of ticks;
- ledger coalescing, restart and heal.

The F1–F3 tests below described the removed sender and fan-out.

- **Server isolation (F1–F3):**
  - `Send` never blocks when its buffer is full (drop path taken).
  - A stalled replica channel does not block `SendReplicationMessage` or other
    replicas.
  - Race test (`-race`) for concurrent connect/disconnect vs fan-out on
    `StreamChannels`.
- **Replica backfill:** enumerate → query → write → advance watermark; idempotent
  re-apply of the same epoch; reconnect triggers gap backfill; watermark persists
  across restart.
- **Integration (extends `tests/replication`):**
  - **Bootstrap:** write to the master *before* the replica connects; start the
    replica; assert the pre-existing data appears. (This is exactly what the
    built-in feature cannot do.)
  - **Master isolation:** freeze the replica; assert the master's writes/queries
    continue unaffected and drop metrics increment.
  - **Catch-up:** disconnect the replica during writes, reconnect, assert
    convergence.

## 8. Phasing

- **Phase 0 — server isolation (F1–F3).** Independently shippable; makes the
  existing feature safe to run at all. Small.
- **Phase 1 — replica bootstrap.** Backfill client, watermark store, backfill
  worker, driver wiring; enable live+bootstrap on startup. The core new
  capability.
- **Phase 2 — catch-up.** Reconnect-gap backfill and periodic reconcile.
- **Phase 3 — integration tests and hardening** (bootstrap, isolation, catch-up).
- **Phase 4 — gapless stream (2026-11).**
  - The sequenced replication log, cursor-based resumable streams, batched replay with an atomic cursor, and the change ledger.
  - Idempotent WAL replay of ticks.
  - It replaces F1–F3's drop-on-overflow with a pull model, in openspec change `replication-gapless-stream`.

## 9. Non-goals / deferred

- **Deletes** — not propagated; accepted divergence for the dev-mirror use case.
- **Variable-length (tick) backfill** — the pull backfill still skips TRADE/QUOTE and other variable-length buckets.
  - *Why.* Their writes append rather than overwrite by epoch, so re-pulling an overlapping range would duplicate rows.
  - *Now.* Ticks come from the sequenced stream, exactly once, while the log holds them. A tick range missed beyond the log's retention is reported, not healed (`replication_unrecoverable_tick_gaps_total`).
  - *Still out of scope.* Tick history from before a replica's first cursor. Copy it once, offline, with both instances stopped.
- **Old corrections** — resolved.
  - *While the log holds them,* they arrive through the stream like any other write.
  - *Beyond retention,* the change ledger names the ranges to re-pull, however old.
  - *Only a cursor older than `ledger_max_age`* needs the deep resync: delete `replication_watermarks.json` on the replica, which forces a full idempotent re-bootstrap of fixed buckets.
- **Point-in-time consistency** — only per-bucket eventual consistency.
- **New WebSocket replication endpoint** — unnecessary for LAN server-to-server;
  the gRPC feed is reused.
- **Transaction-group sequence-gap detection** — resolved.
  - `tgID` is not consecutive: empty flushes consume one.
  - The replication log therefore assigns its own consecutive sequence, and the replica checks it.
- **Multiple masters, and replicas of replicas** — out of scope.
