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
