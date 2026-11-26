# Notes: replication-gapless-stream

## 0.1 Test baseline (2026-11-25, before any code change, at 1efab6fe)

`go test -count=1 ./replication/... ./executor/... ./internal/di/... ./frontend/... ./cmd/...`: 13 packages with tests, all pass, 0 failures.

| Package | Result |
|---|---|
| replication | ok |
| replication/backfill | ok |
| executor | ok |
| executor/buffile | ok |
| executor/wal | ok |
| internal/di | ok |
| frontend | ok |
| frontend/stream | ok |
| cmd/connect | ok |
| cmd/connect/loader | ok |
| cmd/connect/session | ok |
| cmd/start | ok |
| cmd/tool/sessionfacts | ok |

`go test -count=1 -race ./replication/...`: replication ok, replication/backfill ok. No data races.

Pre-existing failures: none.

## 1.1 Replica apply throughput (2026-11-25)

Harness: `replication/replay_bench_test.go` (`TestReplayApplyThroughput`). It runs only when `MKTS_REPLAY_BENCH_WAL` is set.

```bash
MKTS_REPLAY_BENCH_WAL=$HOME/bench-gapless/taichi.walfile MKTS_REPLAY_BENCH_TGS=5000 \
  go test -count=1 -timeout 30m -run TestReplayApplyThroughput -v ./replication/
```

- **Input.** taichi's active WAL, copied at 13:37 MST, during regular trading hours: 213 MiB, 121,222 transaction groups, 1,949,052 write sets (about 16 per TG). The runs used the first 5,000 TGs (78,339 write sets, 8.6 MiB).
- **Disk.** p1 `/home` (nvme1n1p4), the same filesystem as p1's data dir. The live p1 replica was running on it during the runs, which is realistic contention. `/tmp` is tmpfs on p1, so it is not used.
- **Setup.** BackgroundSync on, `wal_sync_interval` 2s, as on p1. A warm-up pass creates every bucket first, so the timed runs measure steady-state apply.

| Mode | Time | TG/s | write sets/s | vs today |
|---|---|---|---|---|
| today: `Replayer.Replay`, one WriteCSM + flush per write set | 205.70s | 24.3 | 381 | 1× |
| one flush per TG (`QueueCSM` per set) | 14.02s | 356.7 | 5,589 | 14.7× |
| one flush per 100 TGs | 2.11s | 2,373.8 | 37,192 | 97.7× |
| one flush per 1000 TGs | 1.49s | 3,344.6 | 52,403 | 137.6× |

A 300-TG run agreed: 31.6 / 359.6 / 2,756.9 / 3,643.8 TG/s, so 115.5× at 1000.

**Gate: passed.** Batching is far above the 10× bar, so D4 stands.

**Findings:**
- Today's replica pays one WAL fsync per write *set*, not per transaction group as the design's Context said. That is about 16 fsyncs per TG.
- A flush per TG alone would give 14.7×. That is not enough margin, though: taichi's live rate is about 80–130 TG/s (see 1.2), and backfill bursts are larger.
- taichi produced about 121k TGs in one WAL rotation window (≤25 min), which is roughly 80+ TG/s during trading. Today's 24 TG/s replica path is slower than that even outside backfills, which fits the measured drops.

## 1.3 Idempotent variable replay (2026-11-25)

- **Tests.** `executor/walreplay_variable_test.go` simulates a crash by snapshotting the WAL and the data files, at three points: before, in the middle of, and after a transaction group's primary writes. The group continues existing tick blocks, starts new ones, and carries two writes to the same slot.
  - With the fix, all three leave each tick once.
  - With the slot check disabled (replay passes `noPreEOF`), "mid primary" and "after primary" fail: rows are duplicated. "before primary" passes either way. That is the pre-existing bug, reproduced.
- **Format.** `executor/wal_trailer_test.go` covers four cases:
  - a trailer-carrying group parses into the same write sets as the old format;
  - an old-format group carries no pre-write sizes;
  - fixed-only groups stay byte-identical to the old format;
  - a mismatched trailer is ignored, and same-slot merging leaves fixed writes and the caller's commands alone.
- **Suites.** The full 0.1 suite passes: 13/13 packages. `go test -race ./replication/... ./executor/` passes.

## Findings outside this change

- **`1D` bars dated January 1 are written into the file header and never read back.** Tier 4: reported, not fixed. Found in 4.1.
  - *Cause.* For `1D`, `io.TimeToIndex` returns `YearDay() - 1`, so January 1 gets index 0. Every other timeframe is 1-based. `IndexToOffset(0)` is `Headersize - recordSize`, the last bytes of the 37,024-byte header. The reader treats index 0 as an empty slot.
  - *Impact.* US equities have no January 1 sessions, so normally nothing is lost. A crypto feed or an aggregate that produces a January 1 daily bar loses it, and overwrites header padding. taichi has 38,471 `1D` buckets.
  - *Proposed fix.* Make 1D indexes 1-based, as every other timeframe is. This changes the on-disk layout of every `1D` bucket, so it needs a migration (shift each year file by one record) and its own change.
  - *Here.* The replication cursor row uses January 2, which avoids the bug.

## 7.1 Integration (2026-11-25)

`TestGaplessReplicationEndToEnd` (`replication/integration_test.go`, skipped with `-short`) runs a real master and replica in one process. The master runs its log, ledger and gRPC replication service. The replica runs the receiver, the BatchApplier and the LedgerHealer, with production BackgroundSync on. It takes about 16 s.

1. **Burst and crash.** The master writes a 1M-row backfill burst: 1,000 transaction groups × 20 symbols × 50 `1Min` bars, plus 10 `TICKA` trades per group. The replica is throttled to 1 ms per entry. Halfway through, while the replica is behind, it is killed: its data directory is copied uncheckpointed, and it restarts on the copy. Afterwards, every fixed bucket and `TICKA` equal the master's: 1,000,000 bar rows and exactly 10,000 ticks.
2. **Outage past retention.** The replica stops while the master writes:
   - a correction of `SYM00`'s oldest bars;
   - 25 `TICKB` ticks;
   - 200 groups of filler.

   Retention (8 MiB) then deletes all of it from the log. On reconnect the replica gets a gap notice, and every fixed bucket ends equal to the master's, the correction included. `TICKA` is still exact. `TICKB` is not re-pulled, and `replication_unrecoverable_tick_gaps_total` rises by 1.

Gotcha: a heal takes closed bars only, as the backfill does, so test bars must be dated in the past.

`make replication-test` (Docker), 2026-11-25:

| Run | Result |
|---|---|
| baseline `1efab6fe` (separate worktree) | 5/5 pass |
| this change, before the test fix | 4/5 pass: `test_bootstrap_of_preexisting_data` failed with `category: Timeframe not in catalog` |
| this change, after the test fix | 5/5 pass |

**Why it failed.** The test queried the replica right after writing to the master, and the query sat outside its retry loop. A replica now applies in batches that close up to `replay_batch_wait` (50 ms) after their first entry, so the first query can run before the replica holds the bucket. That is a new failure caused by this change, in the test's retry rather than in replication. **The fix** moves the query inside the retry. With the fix, the data arrives within the test's 20 s deadline.

## 7.2 Master write latency during a burst (2026-11-25, p1 `/home`)

Harness: `TestMasterWriteLatencyDuringBurst` (`replication/master_bench_test.go`, opt-in with `MKTS_MASTER_BENCH_DIR`).
- **Workload.** 8 concurrent writers, each committing 500-bar `WriteCSM`s over 25 buckets apiece. BackgroundSync is on, with the 500 ms WAL interval taichi uses.
- **What is timed.** Each `WriteCSM`, which is what `write_csm_duration` observes.
- **"Replication off"** stands for the old push design, whose only write-path cost was a non-blocking channel send.
- **"Replication on"** adds the log append, the ledger update and the 100 ms group-commit fsync loop, with one healthy and one stalled replica stream reading the log.

| Run (4,000 writes) | p50 | p90 | p99 | max |
|---|---|---|---|---|
| round 1, off | 0.74 ms | 2.47 ms | 3.68 ms | 1199 ms |
| round 1, on | 0.71 ms | 1.21 ms | 3.45 ms | 1377 ms |
| round 2, off | 0.76 ms | 2.74 ms | 6.69 ms | 947 ms |
| round 2, on | 0.73 ms | 1.24 ms | 3.66 ms | 1375 ms |

**Result: no measurable p99 regression.** The "on" p99 is within the run-to-run noise of "off". The maxima are about 1 s in both modes: btrfs fsync stalls on p1, not replication.

**Caveat.** This is p1's disk (btrfs), not taichi's (ext4). Running the harness on taichi needs about 1 GB of scratch writes on its 91%-full production disk, so it was not run there without approval.

## 7.3 Final test run vs the 0.1 baseline (2026-11-25)

The same commands as 0.1.

| | Baseline (`1efab6fe`) | Final |
|---|---|---|
| `go test ./replication/... ./executor/... ./internal/di/... ./frontend/... ./cmd/...` | 13/13 packages pass | 16/16 pass: the 13, plus the new `replication/replog`, `replication/ledger` and `cmd/tool/replication` |
| `go test -race ./replication/...` | 2/2 pass | 4/4 pass, no data races |
| `go test -race ./executor/` | not in 0.1 | passes |
| `make replication-test` (Docker) | 5/5 pass | 5/5 pass |
| `go vet ./...` | not recorded | clean |

**New failures caused by this change: 0 remaining.** Two were found, both fixed and both test-side:
- *`test_bootstrap` in `make replication-test`.* It queried outside its retry loop (see 7.1).
- *`TestGaplessReplicationEndToEnd` under `-race`.* The 1M-row test runs about 11× slower with the race detector (182 s vs 16 s) and outran `waitCursor`'s fixed 30 s deadline. `waitCursor` now fails only when the cursor stops moving for 30 s.

**Pre-existing failures: 0.**

**Pre-existing defects found and not fixed (Tier 4, reported):**
- `1D` bars dated January 1 are unreadable (see "Findings outside this change").
- `executor/cache.go` is not gofmt-clean. It was left untouched, so the diff stays clean.
- `executor.NewInstanceSetup` returns a shared global, so tests elsewhere that build several instances may compare one instance with itself. It is worked around in `replication` tests with `ownInstance`.

## 8.1 Rollout (2026-11-25, approved by the user, after 23:48 ET with taichi idle)

1. **taichi.** `4725cbde` deployed with `./deploy.sh --no-restart` (the config was identical to production), then `systemctl --user restart marketstore` at 21:50 MST.
   - The log opened at sequence 0. It took a startup burst of about 670 MB / 29,140 entries in the first minute (about 3.9 MB/s), then went idle.
   - No drops; the log is not broken.
2. **p1 was already down,** since 12:01 MST, before this session started.
   - Its log stops with no shutdown message and a crash WAL was left behind. No OOM or kill entry was found; the cause is unknown.
   - The crash WAL was replayed with `set-cursor`, the new build, before the tick copy, so its tick writes could not land in the copied files.
3. **Ticks.** p1's old `AAPL/1Sec/{TRADE,QUOTE}` (79 MB) were moved to `~/.local/share/marketstore/ticks-old-20261125`. taichi's copies (6.7 GB, last written 09-21) were rsynced over and verified with SHA-256: identical.
4. **Cursor set to 0,** so p1 streams taichi's whole log from sequence 1, not `--master-tail`.
5. **Bug found at the first p1 start.** p1 requested the tail instead of sequence 1: its cursor was unreadable.
   - *Cause.* The cursor row at 2000-01-02 00:00 UTC is January 1 in `America/New_York`, which is p1's time zone. `1D` rows on January 1 are unreadable, and the server's own cursor writes landed in the bucket header. Every test had run in UTC.
   - *Response.* p1 was stopped cleanly after about 70 s, and the fix was committed (`f248b920`, with a 25-pair time-zone test). The `__replication__` bucket was deleted and recreated with `set-cursor --seq 0`, and p1 was restarted.
6. **Result.** p1 requested sequence 1 and drained all 29,140 entries in about 75 s (41 batches, about 390 TG/s).
   - `tool replication status`: cursor 29140 = master 29140, lag 0.
   - Master `replica_lag_seq`/`replica_lag_bytes`: 0. `replication_dropped_messages_total`: 0. Gaps 0, unrecoverable tick gaps 0.
   - The startup deep pass compared 14,157,258 rows: all identical, 0 missing, 0 revised. 551 local-only rows are old, and taichi no longer has them.
7. **Spot check (SHA-256 of year files).**
   - `AAPL/1D/OHLCV`, `AAPL/1D/SESSIONS` and `MSFT/1D/OHLCV` are identical.
   - `AAPL/1Min`, `NVDA/1Min` and `SPY/1Sec` differ, but only in records from 2026-09-08/09/18/23. That is history from before this change and outside the deep pass's window. October and November are identical.

## 1.2 Replication volume on taichi (partial, 2026-11-26)

Sampled from taichi's WAL size every 5 s, from 11-25 15:37 ET to 11-26 09:10 ET (17.6 h, 1,145 MiB).

| Window (ET) | Bytes |
|---|---|
| 11-25 15:37–16:00 (last 23 min of the regular session) | 396 MiB, about 290 KB/s |
| 11-25 16:00–20:00 (after-hours) | 17–42 MiB per hour |
| 11-25 23:50 (taichi restart burst, see 8.1) | 639 MiB in about a minute; 5 s peak 36 MiB/s |
| 11-26 00:00–09:10 (Thanksgiving) | about 0: one transaction group, no overnight backfill |

**Estimate for one trading day:** about 7 GB. That is 6.5 h × about 1 GB/h in the regular session, plus about 0.3 GB of extended hours. It does not yet include an overnight backfill, which a holiday night did not produce, or restart bursts.

**Rollout values: the defaults stay,** `log_max_bytes: 20GiB` and `log_max_age: 48h`.
- *What they cover.* About 2–2.5 trading days, so size and age bind at about the same point.
- *Why not larger.* taichi ingests no live ticks: its only tick buckets were last written 09-21. Fixed buckets past retention heal from the change ledger, so a larger log would mostly cost disk on a 91%-full volume.

**Still open:** the per-burst volume of a real overnight backfill. The first chance is the night after Friday 11-27 or Monday 11-30. The WAL sampler was stopped on 11-26: the replication log's own `replication_log_bytes` and entry wallclocks now measure the volume exactly.
