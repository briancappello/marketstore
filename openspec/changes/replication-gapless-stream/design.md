## Context

See proposal.md (Why). Current mechanics this design replaces or builds on:

- **Master.** `FlushCommandsToWAL` (`executor/wal.go`) calls `ReplicationSender.Send(tgSerialized)` after the WAL fsync, while holding `fileMu` (added in 04e035b5).
  - `Sender.Send` drops when its 500-slot channel is full.
  - A single goroutine fans out to per-replica 500-slot channels (`replication/grpc_server.go`), and those also drop when full.
  - `GetWALStream` takes no request fields.
- **Transaction-group IDs** start at `time.Now().UnixNano()` and also increment on empty flushes (`executor/cache.go`, `FlushToWAL`). They are monotonic but not consecutive, so they cannot detect gaps.
- **The master WAL** is truncated every `wal_rotate_interval` × 5 min (about 25 min on taichi), so it cannot serve resends.
- **Replica receive path.** `Receiver.Run` → `Replayer.Replay` → `ParseTGData` → `WTSetToCSM` → `GetDefaultWriter().WriteCSM`. That is one WriteCSM, and so one `RequestFlush`/WAL fsync, per received transaction group.
- **Replica catch-up.** The backfill driver (`replication/backfill/driver.go`) runs shallow passes ([watermark+1, now]) and daily deep passes (24h before the last complete deep pass). It skips variable (tick) buckets.
- **Measured on taichi** (2026-11-24/25): 2.85M dropped of 61M writes. Drops were zero during a minute of live trading, and they come in backfill bursts.

## Goals / Non-Goals

**Goals:**
- No transaction group is lost while it is within log retention, whatever the replica's speed or uptime.
- The master's write path never waits on a replica (keeps §3 of `logical_replication_design.md`).
- A replica past retention heals fixed buckets exactly, and reports what it cannot heal.

**Non-Goals:**
- Copying tick history from before a replica's first cursor. This is a one-time operational copy (tasks, rollout).
- Multi-master, deletes, and point-in-time consistency across buckets (unchanged non-goals).
- Removing the backfill driver. Shallow and deep passes stay as a safety net. They become no-ops in steady state.

## Decisions

### D1. The replication log is append-only segment files with its own sequence
- **Location and layout.** The log lives in `<root>/replication_log/` as segments of about 256 MiB, named by their first sequence. An entry is `seq u64 | tgID i64 | wallclock i64 | len u32 | crc32 | tgSerialized`. When a segment is opened, an in-memory index (seq → offset) is built by scanning it.
- **Retention.** Retention deletes whole segments, oldest first (spec: size and age).
- **Sequence numbers.** They come from the log, not from `tgID`, so empty flushes consume none. At startup, the next sequence is read from the last entry of the last segment.
- **Appending.** The append happens in `FlushCommandsToWAL`, right after the WAL fsync, while `fileMu` is still held. Log appends are therefore ordered exactly like WAL commits, without a second lock. The log is fsynced on a 100 ms group-commit timer, not per entry.
- **Crash consistency.** If the master crashes after the WAL commit but before the log is durable, WAL replay at startup re-applies the transaction group to primary. Replay also appends every replayed transaction group whose `tgID` is greater than the log's last `tgID`. The log therefore never ends up missing a transaction group that primary has.
- *Alternatives:*
  - Reusing the WAL means fighting its 25-minute truncation and checkpoint semantics. A longer rotation would also slow startup replay.
  - An embedded KV store (bbolt or badger) is a new dependency for an append-only workload.
  - Keeping only memory loses everything on a master restart.

### D2. Streams pull from cursors; the hot tail is a byte-capped ring (C)
- **Removed.** `Sender` and its channel, plus the per-replica channels and the fan-out goroutine.
- **Notification.** The log owns a byte-capped in-memory ring of the newest entries (`hot_tail_bytes`, default 256 MiB), and a broadcast channel that is closed and replaced on every append.
- **Reading.** Each `GetWALStream` goroutine keeps its own cursor:
  - If the cursor is in the ring, it reads from the ring.
  - Otherwise it reads segments from disk.
  - When it reaches the tail, it waits on the broadcast channel.
- **Flow control.** gRPC flow control can block `stream.Send`, but only for that stream's goroutine. The master never blocks, and no message is ever dropped. A replica that is too slow just falls behind, up to retention.
- *Alternative:* keep the channels and make them bigger (plain C). That shrinks the drops but never removes them, and it uses memory per replica.

### D3. Protocol: start sequence, sequenced entries, gap notice
In `proto/replication.proto`:
- `GetWALStreamRequest` gains `start_seq uint64`. The value 0 means legacy: stream from the current tail.
- `GetWALStreamResponse` gains `seq uint64` and `oneof { transaction_group | gap }`. `gap` carries `first_available uint64` and a reason (`retention` or `log_broken`).
- A new RPC, `ChangesSince(ChangesSinceRequest{since_seq})`, returns a stream of `BucketChanges{tbk, record_type, ranges[]{epoch_from, epoch_to}}` (D5).

Old replicas send no `start_seq` and ignore unknown fields, so they keep today's behavior (spec: mixed versions). A new replica on an old master never sees `seq`. It logs once and falls back to today's apply path.

### D4. Replica batches, with the cursor committed in the same transaction group (B)
- **Reading and batching.** `Receiver` reads entries into a channel. A batcher closes a batch at `replay_batch_max` (1000), `replay_batch_bytes` (64 MiB) or `replay_batch_wait` (50 ms).
- **Applying.** For each batch, the replica parses every transaction group in sequence order. It queues all their write commands, plus one write of the new cursor, through one `Writer`, and then calls `RequestFlush` once.
- **Atomicity.** The batch and its cursor must land in a single WAL transaction group. A new `WALFileType.WithFlushHeld(fn)` holds `fileMu` while `fn` queues the commands, so a ticker flush cannot split the batch. The flush that follows then takes all of them.
- **Cursor storage.** The cursor is a fixed bucket, `__replication__/1D/CURSOR`: one row at a constant epoch, with column `Seq uint64`. WAL replay therefore restores the cursor together with the data, and a crash leaves both or neither (spec).
  - Watchlist, listing and symbol-enumeration code skip symbols that start with `__`. This is a small shared helper used by `ListSymbols` and the watchlist universe.
  - Replicas never forward to other replicas, and the bucket is never sent to the master.
- *Alternatives considered:*
  - A cursor file written after the flush leaves a window where ticks are applied twice after a crash.
  - A new WAL record type is the cleanest, but it changes the WAL format and replay for every instance.
  - Merging CSMs per bucket before writing loses the order of variable appends.
- **Throughput.** Task 1.1 measures it first: transaction groups per second, per-TG vs batched, on p1's disk. That confirms the bottleneck before the rest is built.

### D5. Change ledger: coalesced ranges per bucket, snapshotted (D)
- **What is recorded.** For each logged transaction group, the master derives, per bucket key, the epoch range from the write commands (`Index` + timeframe + year from `WALKeyPath`). It records `(firstSeq, lastSeq, epochMin, epochMax, lastWallclock)` per bucket.
- **Coalescing.** A new write extends the bucket's newest range when its epochs overlap, or fall within one timeframe interval of it. Otherwise it opens a new range. At 64 ranges per bucket, the two closest ranges merge, which over-approximates and is safe because re-pulls are idempotent.
- **Persistence.** Snapshots go to `<root>/replication_ledger.snap` every 10 minutes and at shutdown. At startup, the ledger is rebuilt from the snapshot plus the log entries after it. If the log was truncated past the snapshot, the ledger history starts at the snapshot.
- **Expiry.** Ranges with `lastWallclock` older than `ledger_max_age` (30 days) are dropped.
- **Size.** About 50k buckets × a few ranges × 40 B, so a few MB in practice.
- **Healing.** `ChangesSince` returns the ranges with `lastSeq > since`. The replica heals fixed ranges with the existing `BackfillBucket` over [epochMin, epochMax], and reports variable ranges (spec).

### D6. Configuration
New `replication` keys (master unless noted):

| Key | Default | Applies to |
|---|---|---|
| `log_max_bytes` | 20 GiB | master |
| `log_max_age` | 48h | master |
| `hot_tail_bytes` | 256 MiB | master |
| `ledger_max_age` | 720h | master |
| `replay_batch_max` | 1000 | replica |
| `replay_batch_bytes` | 64 MiB | replica |
| `replay_batch_wait` | 50ms | replica |

The defaults are sized for taichi (132 GB free today). Task 1.2 measures the real daily log volume before the rollout, and the rollout sets the values explicitly.

## Risks / Trade-offs

- **[Log disk use on taichi, which is 91% full]** → The log is bounded by `log_max_bytes`. Task 1.2 measures the volume. A full disk marks the log broken instead of stopping writes (spec), and replicas fall back to the ledger.
- **[Extra master work per flush: log append + ledger update while holding `fileMu`]** → The append is a buffered write (fsync every 100 ms), and the ledger update is in memory. A task benchmarks flush latency before and after. The acceptance bar is no measurable regression in `write_csm_duration` p99 on a replayed taichi burst.
- **[`__replication__` bucket visible in the catalog]** → The `__` prefix is filtered by a shared helper. A test lists symbols on a replica and checks it is absent.
- **[Over-approximated ledger ranges re-pull more than needed]** → Re-pulls are idempotent and bounded by the 64-range cap.
- **[Tick gaps past retention remain unhealable]** → Accepted and reported. With a 48h log, only an outage longer than retention produces them.
- **[Protocol change]** → Deploy master and replica together. Mixed versions degrade to today's behavior and do not fail (spec).

## Migration Plan

1. Deploy the new master (taichi) and restart it. The log starts empty at sequence 1, and the ledger starts empty.
2. After 20:00 ET, when no tick writes are flowing:
   - stop p1;
   - rsync taichi's `*/1Sec/TRADE` and `*/1Sec/QUOTE` files to p1 (`--checksum`, keeping p1's old copies in a backup dir);
   - set p1's cursor to taichi's current last sequence (new `marketstore tool replication set-cursor`);
   - deploy the new replica build and start it.
3. Run the end-to-end checks (tasks section 8), then merge ta-droid `feat/tauri-port` into `main`.

Rollback: redeploy the previous builds on both machines. The old master ignores `replication_log/`, and the old replica ignores the `__replication__` bucket. Delete both afterwards.
