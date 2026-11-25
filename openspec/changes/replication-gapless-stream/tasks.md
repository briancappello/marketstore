## 0. Baseline

- [x] 0.1 Record the test baseline: `go test ./replication/... ./executor/... ./internal/di/... ./frontend/... ./cmd/...` (pass/fail per package), plus `go test -race ./replication/...`, in `notes.md`.

## 1. Measure, and make crash replay safe, before building

- [x] 1.1 Benchmark replica apply throughput on p1's disk: transaction groups per second when applied one per WriteCSM (today) vs in batches of 100/1000 with one flush. Use a burst captured from a taichi WAL file. Verify by recording the numbers in `notes.md`. If batching is not at least 10× faster, stop and revisit D4 with the user.
- [ ] 1.2 Measure taichi's replication byte volume per day and per backfill burst (sum of `len(tgSerialized)` over a day, from a temporary counter or a WAL sample). Verify by recording it in `notes.md` and setting the rollout values of `log_max_bytes`/`log_max_age` from it.

- [ ] 1.3 Idempotent variable replay (D7): same-slot merge of variable commands, the `preEOF` trailer in `tgSerialized`, and the slot check in WAL replay. Verify with tests for each scenario in `specs/storage/wal-replay`: crash before primary, mid primary loop and after primary give each tick once; merged and unmerged writes decode to the same rows; an old-format transaction group (no trailer) still parses and replays as before; old `ParseTGData` semantics (`WTCount` commands) are unchanged on a trailer-carrying group.

## 2. Replication log (master)

- [ ] 2.1 Add `replication/log` with segments, the entry format, a CRC, the startup scan, next-sequence recovery and size/age retention. Verify with unit tests: consecutive sequences, restart continuation, a corrupt tail entry truncated on open, deletion by size and by age.
- [ ] 2.2 Add the byte-capped hot-tail ring and the append broadcast. Verify with tests: readers inside the tail never touch disk (counting reader), readers behind it get every entry in order.
- [ ] 2.3 Wire the append into `FlushCommandsToWAL` (after the WAL fsync, while holding `fileMu`), with the 100 ms group-commit fsync, and handle append failure as "log broken". Verify with tests: empty flushes consume no sequence; a disk-full fake marks the log broken while WAL writes still succeed.
- [ ] 2.4 WAL replay appends replayed transaction groups with `tgID` greater than the log's last `tgID`. Verify with a crash test: commit to WAL, kill before the log fsync, restart, and the log holds the transaction group once.
- [ ] 2.5 Metrics: first/last sequence, bytes, oldest-entry age, log-broken flag. Verify that they appear at `/metrics` in a test server.

## 3. Resumable stream (protocol + server + client)

- [ ] 3.1 Extend `proto/replication.proto` (`start_seq`, `seq`, the `gap` oneof, `ChangesSince`) and regenerate. Verify with `make generate` and a clean build.
- [ ] 3.2 Replace `Sender`/fan-out with per-stream cursor readers over the log (D2). Keep the legacy path for `start_seq = 0`. Verify with tests: one stalled stream does not delay the master flush or another stream; legacy clients get the tail; no message is dropped across a 100k-entry burst.
- [ ] 3.3 Gap notice for cursors older than the first sequence, or inside a broken range. Verify with a test.
- [ ] 3.4 Client: request `cursor+1`, check sequence continuity, reconnect on a jump, handle a gap notice by running the ledger heal (5.3), and fall back on a legacy master. Verify with tests for each scenario in `specs/replication/resumable-stream`.
- [ ] 3.5 Per-replica lag metrics (sequence and bytes) on the master. Verify with a test.

## 4. Batched replay with an atomic cursor (replica)

- [ ] 4.1 Add `executor.NewBatchWriter` (collects commands), `WALFileType.CommitCommands` (one transaction group under `fileMu`) and the `__replication__/1D/CURSOR` bucket read/write. Verify with tests: concurrent ticker flushes and other writers never split a committed batch from its cursor, and a batch larger than the free write-channel capacity commits without blocking.
- [ ] 4.2 The batcher (max entries/bytes/commands/wait) applies each batch plus the cursor in one transaction group. Verify that it gives the same result as per-entry apply on a recorded burst, and that the latency of a single trickled entry stays within 50 ms plus a flush.
- [ ] 4.3 Crash tests on the replica: kill after a batch flush or before it, restart, and check that the cursor is exact and tick buckets have no duplicate and no missing rows (relies on 1.3).
- [ ] 4.4 Hide `__`-prefixed symbols from `ListSymbols`, the watchlist universe and the backfill driver's enumeration. Verify with tests.
- [ ] 4.5 Replay metrics (applied/s, batch sizes, newest applied age). Verify with a test.

## 5. Change ledger

- [ ] 5.1 Derive per-bucket epoch ranges from the write commands, and coalesce them with the 64-range cap (D5). Verify with unit tests: fixed and variable keys, overlapping and adjacent ranges, cap merging.
- [ ] 5.2 Snapshot plus rebuild from the log on startup, and expiry by `ledger_max_age`. Verify with restart tests.
- [ ] 5.3 `ChangesSince` RPC and the replica heal: re-pull fixed ranges via a range variant of `BackfillBucket` (`HealRange`, watermark untouched), report variable ranges (log + `replication_unrecoverable_tick_gaps_total`), and log "full resync required" past the ledger's history. Verify with tests for each scenario in `specs/replication/change-ledger`.

## 6. Config, tools, docs

- [ ] 6.1 Add the D6 keys to `utils/config.go` with validation and defaults, and to `mkts.yml.example` and `examples/replication/*.yml`. Verify with config tests.
- [ ] 6.2 `marketstore tool replication set-cursor` and `marketstore tool replication status` (cursor, master sequence, lag). Verify with tests.
- [ ] 6.3 Update `docs/design/logical_replication_design.md`: the correctness model, §3, and §9 (sequence-gap detection, old corrections, ticks). Verify by reading it back.

## 7. Integration

- [ ] 7.1 Two-process integration test (`tests/replication` or a Go test with two instances): a 1M-row backfill burst with ticks on the master while the replica is throttled, then a replica restart mid-burst, then a disconnect longer than a tiny retention. Afterwards fixed buckets equal the master's, ticks are exact while within retention, and the tick gap past retention is reported. Verify that the test passes and `make replication-test` stays green.
- [ ] 7.2 Benchmark: the master's `write_csm_duration` p99 during the burst, before vs after, with no measurable regression. Record it in `notes.md`.
- [ ] 7.3 Re-run the 0.1 baseline and report new failures and pre-existing failures as separate counts.

## 8. Rollout and verification

- [ ] 8.1 Deploy taichi per the Migration Plan, then the p1 tick-history copy, `set-cursor` and the p1 deploy. Verify that `tool replication status` shows lag near zero.
- [ ] 8.2 Over one trading day, including the overnight backfill, check:
  - `replication_dropped_messages_total` stays at 0;
  - after the overnight backfill, a bit-exact comparison of 1Min/1D/SESSIONS for the last 5 trading days between taichi and p1 finds no differences;
  - AAPL tick files match byte for byte after 20:00 ET.

  Record the results in `notes.md`.
- [ ] 8.3 Close watchlist-sessions 13.2: on the same day, check p1's `1D/SESSIONS` parity and the live session-boundary rankings (09:30 and 16:00 ET). Then merge ta-droid `feat/tauri-port` into `main` and push.
