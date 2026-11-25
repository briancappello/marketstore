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
