## Context

See proposal.md (Why). Current mechanics this design replaces or builds on:

- **Master.** `FlushCommandsToWAL` (`executor/wal.go`) calls `ReplicationSender.Send(tgSerialized)` after the WAL fsync, while holding `fileMu` (added in 04e035b5).
  - `Sender.Send` drops when its 500-slot channel is full.
  - A single goroutine fans out to per-replica 500-slot channels (`replication/grpc_server.go`), and those also drop when full.
  - `GetWALStream` takes no request fields.
- **Transaction-group IDs** start at `time.Now().UnixNano()` and also increment on empty flushes (`executor/cache.go`, `FlushToWAL`). They are monotonic but not consecutive, so they cannot detect gaps.
- **The master WAL** is truncated every `wal_rotate_interval` × 5 min (about 25 min on taichi), so it cannot serve resends.
- **Replica receive path.** `Receiver.Run` → `Replayer.Replay` → `ParseTGData` → `WTSetToCSM` → `GetDefaultWriter().WriteCSM`, once per **write set**. That is one WriteCSM, and so one `RequestFlush`/WAL fsync, per write command of the received transaction group (about 16 per TG on taichi's live traffic), not one per transaction group. Measured on p1 (task 1.1): 24 TG/s.
- **WAL replay is not idempotent for ticks.** `Replay` (`executor/walreplay.go`) re-applies every transaction group after the last completed checkpoint, whether or not its primary writes already ran. Fixed writes are idempotent. Variable writes are a read-append-rewrite, so after a process crash every tick written since the last checkpoint (up to 5 min) is duplicated. This applies to the master and the replica alike, today. D7 fixes it.
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
  - This relies on two facts. First, `tgID` is seeded from wall-clock nanoseconds at process start, so a restarted master's IDs are greater than the previous run's (unless the clock steps back). Second, the log lives on the same filesystem as the data, so a checkpoint's `syncfs` also makes the log durable: a transaction group older than the last checkpoint is never missing from the log.
  - The log must therefore be opened before WAL replay runs in `GetInitWALFile`.
- **Appends are plain `WriteAt`s, one per entry, with no user-space buffer.** A checkpoint's `syncfs` therefore covers every appended entry. Sealed segments are renamed `<first>-<last>.seg`, so startup scans only unsealed segments, and sealed ones are indexed on their first read.
- **Broken log.**
  - A failed append consumes its sequence and marks the log broken from it. The first lost sequence is written in place to a 16-byte, pre-allocated `STATE` file. On ext4 (taichi), an in-place overwrite needs no free space.
  - While the log is broken, readers at or past the break wait. They get no gap notice, because the hole's end is unknown, so no replica's cursor can move past a sequence that isn't on disk.
  - The next successful append starts a new segment. That leaves a durable hole, and readers in it get a `log_broken` gap notice. `STATE` is cleared only after the new segment is fsynced.
  - A master restarted while broken starts at the recorded break + 1 at least, so no sequence is ever reused.
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

A new replica with no cursor also sends `start_seq = 0`. A new master stamps `seq` on tail entries too, so the replica adopts the first `seq` it receives as its cursor. No extra "what is your last sequence" call is needed.

`GetWALStreamRequest` also carries `resumable bool`. New replicas always set it.
- **Why it's needed.** `start_seq = 0` alone cannot tell an old replica from a new one with no cursor. An old replica must never receive a gap message: its receiver would hand the empty `transaction_group` to `ParseTGData`, which panics on empty input.
- **Old replicas (flag unset).** The master skips a gap and counts the skipped entries in `replication_dropped_messages_total`, as before.
- **Resumable streams.** A gap notice ends the stream. The replica heals and reconnects at `first_available`, so it never applies entries past a gap it has not healed.

**Replica reconnects.**
- *Deep heals.* A sequenced stream resumes exactly, so a reconnect no longer requests a deep heal pass (about 25 GB of writes on p1). The receiver asks for one (`ErrNeedsDeepHeal`) only after streaming from an unsequenced (old) master.
- *Backoff.* `Retryer`'s backoff used to grow with every reconnect for the life of the process (`interval × coeff^n`). An attempt that made progress now resets it (`ErrMadeProgress`). Gap heals and sequence jumps reconnect routinely, so without the reset the delay would reach hours.
- *Live-only replicas.* With no `master_query_host`, there is nothing to heal from: a gap is logged, counted in `replication_replica_unhealed_gaps_total`, and skipped.

### D4. Replica batches, with the cursor committed in the same transaction group (B)
- **Reading and batching.** `Receiver` reads entries into a channel. A batcher closes a batch at `replay_batch_max` (1000), `replay_batch_bytes` (64 MiB), `replay_batch_commands`, or `replay_batch_wait` (50 ms) after the batch's first entry.
- **Wait measured from the first entry.** An idle timer would never fire at taichi's live rate (80+ TG/s), so batches would only close when full, about 12 s late.
- **The cursor can be 0.** It means nothing has been applied yet, and resuming starts at sequence 1. Cursor-only writes (`SetCursor`) are therefore flagged explicitly, instead of being inferred from a sequence of 0, which on a data entry means an unsequenced master.
- **Applying.** For each batch, the replica parses every transaction group in sequence order. It builds all their write commands, plus one write of the new cursor, through one collecting `Writer`, and then commits them with one WAL flush.
- **Atomicity.** The batch and its cursor must land in a single WAL transaction group.
  - A collecting `Writer` (`executor.NewBatchWriter`) builds the write commands into its own slice instead of the shared write channel.
  - `WALFileType.CommitCommands(cmds)` then takes `fileMu`, drains whatever other writers have queued, appends `cmds`, and writes it all as one transaction group with one fsync.
  - A ticker flush cannot split the batch, because the batch is never in the channel.
  - *Rejected: `WithFlushHeld(fn)`, holding `fileMu` while `fn` queues into the channel.* The channel holds 1M commands, and the flusher that drains it needs `fileMu`. A batch, or a concurrent backfill write, that fills the channel while the lock is held deadlocks the replica. A large bootstrap bucket alone is one command per bar. It would also hold `fileMu` through parsing and catalog work, and a `RequestFlush` inside `fn` would self-deadlock on the non-reentrant mutex.
- **Replay idempotency.** The cursor alone does not stop duplicate ticks after a crash, because WAL replay re-applies transaction groups whose primary writes already ran (see Context). D7 makes WAL replay idempotent for variable records on every instance. The replica therefore needs no replay rule of its own.
- **Cursor storage.** The cursor is a fixed bucket, `__replication__/1D/CURSOR`: one row at a constant epoch, with column `Seq uint64`. WAL replay therefore restores the cursor together with the data, and a crash leaves both or neither (spec).
  - Watchlist, listing and symbol-enumeration code skip symbols that start with `__`. This is a small shared helper used by `ListSymbols` and the watchlist universe.
  - Replicas never forward to other replicas, and the bucket is never sent to the master.
- *Alternatives considered:*
  - A cursor file written after the flush leaves a window where ticks are applied twice after a crash.
  - A new WAL record type is the cleanest, but it changes the WAL format and replay for every instance.
  - Merging CSMs per bucket before writing loses the order of variable appends.
- **Batch size bound.** A batch also closes at `replay_batch_commands` write commands (default 250k, a quarter of the write channel's depth). The count comes from each transaction group's header (bytes 8–16), so no parse is needed. This keeps one commit's memory and WAL record bounded.
- **Throughput.** Task 1.1 measured it on p1's disk with a 213 MiB taichi WAL: 24 TG/s today, 357 TG/s with one flush per TG, 2,374 TG/s per 100-TG batch and 3,345 TG/s per 1000-TG batch (138×). See `notes.md`.

### D5. Change ledger: coalesced ranges per bucket, snapshotted (D)
- **What is recorded.** For each logged transaction group, the master derives, per bucket key, the epoch range from the write commands (`Index` + timeframe + year from `WALKeyPath`). It records `(firstSeq, lastSeq, epochMin, epochMax, lastWallclock)` per bucket.
- **Coalescing.** A new write extends the bucket's newest range when its epochs overlap, or fall within one timeframe interval of it. Otherwise it opens a new range. At 64 ranges per bucket, the two closest ranges merge, which over-approximates and is safe because re-pulls are idempotent.
- **Persistence.** Snapshots go to `<root>/replication_ledger.snap` every 10 minutes and at shutdown. At startup, the ledger is rebuilt from the snapshot plus the log entries after it. If the log was truncated past the snapshot, the ledger history starts at the snapshot.
- **Expiry.** Ranges with `lastWallclock` older than `ledger_max_age` (30 days) are dropped.
- **Size.** taichi has 50,231 buckets (2026-11-25).
  - *Ranges.* Intraday buckets get one range per trading day. `1D` buckets get about one per week, because weekends break adjacency. That is about 640k ranges over 30 days.
  - *Cost.* About 26 MB in memory, and a gob snapshot of about 15 MB.
  - *Locking.* A snapshot copies the ranges under the ledger lock and encodes them outside it, so the write path waits milliseconds at most.
  - *Broken log.* A failed append still returns its consumed sequence, so the ledger records groups that the disk log lost.
- **Healing.** `ChangesSince` returns the ranges with `lastSeq > since`. The replica heals fixed ranges over [epochMin, epochMax], and reports variable ranges (spec). `BackfillBucket` is watermark-anchored (`[watermark+1−lookback, now]`), so the heal uses a range variant, `HealRange`. It shares the query, the closed-bar filter and the row-diff write, and leaves the watermark alone.

### D7. WAL replay is idempotent for variable records (master and replica)
A crash today duplicates every tick written since the last checkpoint, on the master and on the replica. The master's own tick buckets would then differ from a replica that got each transaction group once from the log. So this change fixes replay itself, for all instances.

The fix rests on two invariants of the variable write path (`WriteBufferToFileIndirect`):
- A variable data file only grows after it is created. A rewritten block is always written at end-of-file, and the old block is never overwritten.
- The 24-byte index slot `{Index, Offset, Len}` is updated after its block is written.

So if a slot's `Offset` is at or past the file's size from before a transaction group's primary writes began, the slot already holds that group's write.

**Recording the pre-write size.**
- `FlushCommandsToWAL` records each variable file's size (`preEOF`) before it writes the WAL.
  - It `stat`s each distinct variable file once per flush. That is one syscall per file, next to the open that the primary write already does.
  - Nothing else writes primary files between the WAL write and the primary write, because `fileMu` is held for the whole call.
- The sizes travel in a **trailer** appended to `tgSerialized`, after the last write command: `preEOF int64` for each variable command in command order, then `count int64`, then an 8-byte magic.
  - The WAL checksum covers the trailer.
  - `ParseTGData` (old and new) reads exactly `WTCount` commands and ignores trailing bytes. Old binaries and old replicas therefore parse new transaction groups unchanged; they just don't use the trailer.
- Within one transaction group, variable commands for the same file and index are **merged** into one command before serialization, by concatenating their payloads in order.
  - This gives the same block as applying them one by one. The block is old + P1 + P2 stably sorted by interval ticks, and stable sorting is associative over concatenation.
  - Each slot then gets at most one write per transaction group, so the per-slot check is exact.

**Replay.**
- For a variable command, replay reads the slot. It skips the command when `Index != 0 && Offset >= preEOF`; otherwise it appends as today.
- Fixed commands and transaction groups without a trailer (written by an old binary) replay as today.

**Why this is exact.**
- Flushes are serialized, and a transaction group reaches the WAL only after the previous group's primary writes have finished. So only the last group in a WAL file can be partly applied, and the per-slot check settles each of its slots on its own.
- Re-applying a slot writes at the current end-of-file. That is beyond the `preEOF` of every earlier group, and no later group exists in the WAL.
- A failed primary write (logged and skipped today) leaves the slot below `preEOF`, so replay heals it.

**Power loss.**
- If the block persisted but the slot did not, the slot is below `preEOF`, so the command is re-applied from the intact old block. That is correct.
- If the file lost appended pages, later slots also fall below their `preEOF` and are re-applied. That is correct too.
- Only "slot persisted, block lost" stays corrupt. That is a pre-existing torn-write case that this change neither causes nor fixes.

**Replication.** The replica ignores the master's trailer, because master file offsets mean nothing locally. It writes through its own `FlushCommandsToWAL`, which records its own trailer. Merged commands are still ordinary write sets, so `WTSetToCSM` handles them unchanged.

**Rollback.** An old binary replaying a new WAL ignores the trailer and duplicates, exactly as it does today. Rolling back after a clean shutdown leaves nothing to replay.

*Alternatives:*
- *A "primary applied" marker per transaction group in the WAL.* It is not fsynced with the primary pages. After a power loss it can claim writes that never reached disk, which turns today's duplicates into lost ticks.
- *Content-based de-duplication at replay.* Identical genuine ticks (same nanosecond, price and size) are indistinguishable from replayed ones.
- *Full merged blocks in the WAL.* Replace semantics would be idempotent, but they change what replicas receive. Old replicas would append whole blocks, and continuation writes would inflate the WAL.
- *A replica-only cursor filter.* It fixes only the replica, and leaves a window inside one flush's primary writes.

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
| `replay_batch_commands` | 250000 | replica |

The defaults are sized for taichi (132 GB free today). Task 1.2 measures the real daily log volume before the rollout, and the rollout sets the values explicitly.

## Risks / Trade-offs

- **[Log disk use on taichi, which is 91% full]** → The log is bounded by `log_max_bytes`. Task 1.2 measures the volume. A full disk marks the log broken instead of stopping writes (spec), and replicas fall back to the ledger.
- **[Extra master work per flush: log append + ledger update while holding `fileMu`]** → The append is a buffered write (fsync every 100 ms), and the ledger update is in memory. A task benchmarks flush latency before and after. The acceptance bar is no measurable regression in `write_csm_duration` p99 on a replayed taichi burst.
- **[`__replication__` bucket visible in the catalog]** → The `__` prefix is filtered by a shared helper. A test lists symbols on a replica and checks it is absent.
- **[Over-approximated ledger ranges re-pull more than needed]** → Re-pulls are idempotent and bounded by the 64-range cap.
- **[Tick gaps past retention remain unhealable]** → Accepted and reported. With a 48h log, only an outage longer than retention produces them.
- **[Idempotent replay (D7) touches the write path of every instance]** → The trailer is ignored by old parsers, and the same-slot merge is semantically neutral. Tests cover: crash after the WAL fsync but before primary, crash mid primary loop, crash after primary, an old-format WAL, and identical decoded blocks with the merge versus without it.
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
