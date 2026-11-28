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
   - **Correction (2026-11-28):** this spot check generalised from a few files and was wrong for `1D/SESSIONS`. p1 was missing 175,054 session-fact rows from 2025 through July 2026. See "Pre-rollout history holes".

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

### 1.2 update: Friday 2026-11-27, a half day (measured 2026-11-28)

Source: the monitor's `health.csv`, every 15 min. Bytes are the growth of taichi's `replication_log_bytes`. That is exact, because no retention deletion happened on 11-27 (`first_seq` stayed 1).

**When the overnight backfill runs.** taichi's "overnight backfill" is the massive feed's reconnect at 03:58 ET on the *next trading day*. It fills the previous trading day. So no backfill ran on Friday night. The one for Friday's session is due on Mon 11-30 at 03:58 ET, before the 06:00 parity.

| Window (ET, 11-27) | Entries | Bytes |
|---|---|---|
| 03:58 backfill of Wed 11-25 (14 s, inside 03:45–04:00) | 53 | 421 MiB (about 8 MB per entry) |
| premarket 04:00–09:30 | 311,026 | 106 MiB |
| regular 09:30–13:00 (3.5 h) | 1,107,186 | 1,828 MiB (about 520 MiB/h) |
| after-hours 13:00–17:00 | 243,216 | 75 MiB |
| 17:00–24:00 | 84 | 10 MiB |
| **day total** | **1,661,566** | **2,439 MiB** |

- **Busiest 15 minutes.** By bytes, 03:45–04:00: the backfill, 421 MiB in 53 entries. By entries, 10:00–10:15: 98,373 entries, 140 MiB.
- **Retention at work.** At 11-28 00:00 ET, `first_seq` moved from 1 to 24,085 and the log shrank from 3.23 GB to 2.73 GB. The oldest retained entry is then about 54 h old, over the 48 h `log_max_age`. That is by design: a 256 MiB segment is deleted only once the *next* segment starts past the cutoff. Here the segment from seq 24,085 spans 11-25 23:5x to 11-27 morning.
- **The monitor's report was wrong here.** It estimated bytes as entries × the retained log's average entry size, and reported 3,025 MiB and a 179 MiB busiest window. It is fixed in `234933d8` (see "Monitor fixes, 2026-11-28").

**Do the rollout values still fit? Yes.** No config change was made.
- *A full trading day.* Scaling the regular session to 6.5 h gives about 3.4 GiB, plus about 0.2 GiB of extended hours and about 0.4 GiB of backfill, so about 4 GiB. The 11-26 WAL estimate was 7 GB, and 11-25 was busier at the close. Take 4–7 GiB per trading day.
- *What fills 48 h.* At most 2 trading days, so 8–14 GiB. That is under `log_max_bytes: 20GiB`, so `log_max_age: 48h` binds first, and the size cap stays a safety margin.
- *Disk.* taichi's `/home` is now 67% used, with 455 GiB free, not the 91% recorded above. 20 GiB is about 4% of the free space.
- *Still to confirm.* The first full trading day is Mon 11-30, with its backfill on Tue 12-01 at 03:58 ET. Until then 1.2 stays open.

## 8.2/8.3 monitoring (from 2026-11-26)

`scripts/replication-monitor.sh` runs on p1, started by hand (`start`/`stop`/`status`/`once`), as p1 has no systemd units. It needs p1 → taichi access only: taichi cannot reach p1. State and reports live in `~/.local/share/marketstore/monitor/`.

- **Every 15 min:** health, plus taichi's log size for 1.2.
- **Trading days:**
  - 06:00 and 20:30: `tool replication compare --days 5` (1Min/OHLCV, 1D/OHLCV, 1D/SESSIONS, row by row).
  - 20:30: SHA-256 of every tick file on both hosts.
  - Open ± 5 min and close ± 5 min (the calendar's close, 13:00 on half days): watchlist snapshots from both servers.
- **21:00:** a daily report.
- **`READY-8.2-<day>`** is written once a trading day had no alerts, its evening parity and tick hashes passed, and the next trading day's 06:00 parity passed.
- **Alerts** go to `notify-send` and `alerts.log`.

**First run, 2026-11-26 (Thanksgiving, quiet):**
- Parity: 35,380 buckets, 9,810,481 rows identical, 0 missing / differing / replica-only (11/19–11/26). 127 buckets exist only on p1: its own extra symbols. The 11/24 `1D/SESSIONS` rows that 13.2 recorded as lagging are now equal to taichi's.
- Tick files: identical.

**Finding for 8.3, not replication: p1's watchlists differ from taichi's.**
- *Size of the difference.* None of the 8 lists is identical. `INDUSTRY_AGGREGATE` has 130 entries on p1 against 138 on taichi, and the sector names differ. Per-symbol `prior_close` differs: SOXL is 30.61 on p1 against 32.02 on taichi.
- *Taichi is right.* 32.02 is SOXL's 11/25 `1D` close.
- *The data is not the cause.* p1's `1D/OHLCV` and `1D/SESSIONS` files are byte-identical to taichi's, and both run watchlist plugin `b91e04d` with the same watchlist config.
- *Hypothesis, unverified.* p1 seeded its watchlists (11,860 symbols, against taichi's 11,794) and then applied taichi's 29,140-entry startup burst through its triggers, as if it were live.
- *Effect on 8.3.* Its "live session-boundary rankings" check fails until this is understood.

## 8.2 monitoring: Friday 2026-11-27, a half day (checked 2026-11-28 06:20 ET)

Friday closed at 13:00 ET, so it is a **half-day data point, not the 8.2 trading day**. 8.2 needs a full trading day and the next trading day's 06:00 parity. The first full-day verdict is for Mon 11-30, written on Tue 12-01 after 06:05 ET.

| Check | Result | Evidence |
|---|---|---|
| Monitor running | pass | pid 484926 since 11-26; restarted 11-28 06:23 ET (under 1 s) to load the fixes below |
| `health.csv` every 15 min | pass | 179 rows from 11-26 09:45 to 11-28 06:15, no missing slot (only 1 s jitter) |
| Alerts | pass | `alerts.log` does not exist: no alert was ever raised |
| Parity 06:00 / 20:30 | pass | 7,890,281 / 8,956,216 rows identical; 0 missing, 0 differing, 0 replica-only; 127 buckets only on p1 (known) |
| Tick SHA-256 at 20:30 | pass | `AAPL/1Sec/{TRADE,QUOTE}/2026.bin` identical. These are the only tick files on taichi. |
| Watchlist snapshots | ran, differ | 09:25, 09:35, 12:55, 13:05 all ran at the half-day times (see "8.3: why p1's watchlists differ") |
| `tool replication status` | pass | cursor 1,690,708 = master 1,690,708, lag 0 |
| taichi metrics | pass | dropped 0, log_broken 0, first seq 24,085, last 1,690,708, 2.73 GB, oldest entry 54.5 h (per-segment retention, see 1.2) |
| p1 metrics | pass | replica_gaps 0, unrecoverable_tick_gaps 0, full_resync_required 0 |
| Max lag in a 15-min sample | pass | 75 entries during the regular session |
| Parity, run by hand 11-28 06:20 ET | pass | 8,956,216 rows identical, 0 missing / differing / replica-only, 127 p1-only buckets; tick files identical |

**Server logs since 11-27.**
- *taichi.* One error: the massive websocket was force-disconnected at 20:21 ET (close 1012, outside market hours). It reconnects on Mon 11-30 at 03:58 ET. No panic.
- *p1.* At 21:06 ET, taichi closed the replication stream. p1 reconnected 10 s later, "requesting sequence 1690708", and resumed with no gap. That is the resumable stream working. The nightly deep pass (22:00 MST) warns about 10 `*/1Sec/OHLCV` buckets holding old local-only rows from 2026-08-26 (known history). No panic.

**Pre-rollout split rows that were missing on p1.**
- *What was healed.* The 11-27 deep pass wrote 3 missing rows: `GMEX`, `SPRC` and `YAAS` `1D/SPLITS`, all effective 2026-11-27.
- *Why they were missing.* taichi's split sync wrote them on 11-24 at 07:15 MST, before the rollout, through the old lossy stream. The deep pass only reads periods up to now, so a future-dated row is healed only once its date arrives.
- *What is still missing.* A SHA-256 of every `*/1D/SPLITS/2026.bin`: 767 files are identical, and p1 lacks one bucket, `RC/1D/SPLITS`. That is a 1:10 reverse split effective **2026-12-08**, written on taichi on 11-24.
- *Effect.* By the 11-27 pattern, p1 heals it at the 12-08 deep pass (22:00 MST), so p1 serves unadjusted RC during the 12-08 session.
- *Fixed in the design, not by copying* (see "Fixes deployed 2026-11-28"). The 8.2 parity does not cover `1D/SPLITS`.

## Monitor fixes, 2026-11-28

1. **`tool replication market-day` panicked on every ordinary trading day** (`58b32128`).
   - *Cause.* `calendar.MarketClose` returned nil for any day not listed in the calendar, and only holidays and early closes are listed.
   - *Effect.* The monitor reads a non-zero exit as "not a trading day". On Mon 11-30 it would have skipped every parity, tick and snapshot job. It only worked so far because 11-26 was a holiday and 11-27 an early close.
   - *Fix.* `MarketClose` returns the normal close on ordinary trading days, as its doc says.
   - *Check.* `TestMarketClose` (calendar) and `TestMarketDayReportsTheRegularClose` (the CLI) both failed before the fix.
   - *Scope.* `streamtrigger`'s 1D `nasdaq` shelf deadline had the same nil. Neither host configures it (`stream.so` runs only on `*/1Sec/QUOTE|TRADE`, with no filter).
   - *Deployed.* `./marketstore` and `watchlist.so` were rebuilt on p1 (they must match), and the monitor was restarted. The running p1 server is unchanged. Backups are in `/tmp/opencode/*.bak-20261126`.
2. **The report's byte volume was estimated, not measured** (`234933d8`). It now uses `log_bytes` growth per interval, and names the intervals where retention deleted a segment.
3. **A half day would have been written as `READY-8.2-<day>`** (`234933d8`). Friday would have got one on Monday at 06:05 ET. An early close now gets `HALFDAY-8.2-<day>`.

Checks for 2 and 3: `scripts/replication-monitor_test.sh`, with fixtures and a fake binary. It passes 7/7. Against the old report and verdict logic it fails 6 of 7.

## 8.3: why p1's watchlists differ from taichi's (investigated 2026-11-28)

The 11-26 hypothesis does not hold. Replicated writes do reach p1's triggers, and that is correct: live volumes and prices match exactly. The difference has two independent causes.

### Cause 1: baselines are a one-time snapshot taken while their inputs were changing (both hosts)

**How baselines are loaded.**
- The worker loads a trading day's baselines once: at startup, or one hour after afterhours ends (`refreshBaselines` → `loadBaselines`).
- The baselines are:
  - the traditional `PriorClose`, the previous date's `1D/OHLCV` close;
  - `PrevAfterhoursClose`;
  - the per-session volume medians, from `1D/SESSIONS`.
- `refreshBaselines` returns early while the same day is loaded, so a later rewrite of those inputs is never picked up.

**On 11-25 both hosts loaded the 11-27 baselines while those inputs were being rewritten.**
- *taichi* loaded at 21:50:47 MST, between its REST current-day backfill of 1Min (21:50:25–48) and of 1D (21:50:48–51:06). Its traditional `prior_close` is therefore the ondiskagg 1D close, which is the 15:59 1Min close. That holds for 357 of the 365 traditional symbols at 09:35; the other 8 have no 15:59 bar. Only 71 of 365 equal the final, official 1D close. SOXL's 32.02 matched only because its 15:59 close equals its official close. BNAI shows 8.13 against an official 8.15.
- *p1* loaded at 21:59:46 MST, while it was replaying taichi's 29,140-entry log from seq 1 (about 21:59:26 to 22:00:41). Its `1D` and `1D/SESSIONS` rows were still in intermediate states, so it captured transient values: SOXL 30.6101 and BNAI 8.635. Neither is an 11-25 close. Only 21 of 371 equal the final 1D close.

**What follows.**
- *By session and basis.* On 11-27, every session-basis `prior_close` matched (premarket, regular, afterhours: 0 differences). Every `*_TRADITIONAL` list differed in the regular session. `volume_multiple` and `momentum_score` differ because the medians come from the same snapshot (CLIP: median 1,820 on taichi, 1,859 on p1, same volume).
- *In steady state, with no restart.* The 18:00/21:00 ET load for the next day comes before the 03:58 ET overnight backfill, which rewrites the previous day's 1D with the official close. Both hosts then hold the ondiskagg close, which is the same on both but not the official close the spec asks for. On p1 the load also races replication lag.

### Cause 2: sectors and industries come from each host's own postgres

**The source.** `SECTOR_AGGREGATE` and `INDUSTRY_AGGREGATE` read `fa_techan` at `127.0.0.1:5432` on each host. The query is the same, but the databases are not:

| | taichi | p1 |
|---|---|---|
| sectors | 12, Nasdaq-style ("Finance", "Health Care", "Consumer Discretionary", "Telecommunications", "Miscellaneous") | 11, Yahoo-style ("Financial Services", "Healthcare", "Consumer Cyclical", "Communication Services") |
| equities with a sector | 6,372 (5,949 mappings loaded at start) | 4,184 |

**Consequences.**
- *The aggregates can never match.* This is why `SECTOR_AGGREGATE` (12 vs 11) and `INDUSTRY_AGGREGATE` (138 vs 130) differ.
- *No shared source today.* taichi's postgres listens on localhost only, and p1 gets "no route to host" on 5432.

### Hypothesis (c), p1's extra symbols: not a cause

- *Seeding.* Both hosts seeded the same 10,911 symbols from 11-25 bars. p1's 66 extra symbols have no current bars, and `view` drops a symbol whose `LiveDay` is not the ranked day.
- *Lists.* At 11-28 06:30 ET (quiet), `PCT_CHANGE_UP/DOWN` and `VOLUME_UP/DOWN` are byte-identical on both hosts. Only the two aggregates and the median-based `MOMENTUM` / `RELATIVE_VOLUME` differ.

### Friday's snapshots: the difference persisted and did not converge

- *Persisted.* 09:25 and 13:05 have 4 of 8 lists identical, and 09:35 and 12:55 have 0 of 20. The traditional baselines were set at the 11-27 day roll from the 11-25 snapshot, and nothing reloaded them.
- *Monday.* The 11-30 baselines were loaded on Friday at 16:00:03 (taichi) and 16:00:16 MST (p1), with p1 at lag 0. Monday may therefore match on traditional `prior_close`. Both hosts will still be wrong against the official close once the 03:58 ET backfill rewrites 11-27.

## Fixes deployed 2026-11-28 (approved; markets closed)

### 1. The overnight backfill runs at 03:00 ET (`eec0056d`)

- *Before.* The backfill for an overnight outage started with the 03:58 reconnect, so its historical writes (421 MiB on 11-27) raced the first live bars at 04:00.
- *After.* The sleeping retry loop queues it at 03:00 ET and still reconnects at 03:58. A successful full backfill records when it started, so the 03:58 reconnect fills only the 59 minutes since.
- *On failure.* If the 03:00 backfill fails, the reconnect runs the full backfill as before.
- *Why not just move the reconnect to 03:00.* Outside 04:00–20:00, a disconnect sleeps until the *next trading day's* wake time. A vendor drop between 03:00 and 04:00 would then leave the stream down for the whole day.
- *Tests.* `TestOvernightBackfillRunsAnHourBeforePreMarket` and `TestReconnectAfterOvernightBackfillOnlyFillsSinceIt`.
- *First live run.* Mon 11-30 at 03:00 ET, filling Friday.

### 2. Future-dated corporate actions now replicate (`652f8394`)

**Why `RC/1D/SPLITS` never reached p1.** RC is a 1:10 reverse split effective 12-08.
1. *The write was lost.* taichi's split sync wrote it on 11-24 at 07:15:57 MST, 4 s after taichi restarted. p1's legacy stream was disconnected from 07:14:49 to 07:16:09, and that protocol resumes at the tail.
2. *The backfill could not repair it.* `BackfillBucket` reads `[watermark, now]` and keeps closed bars only; `HealRange` (the change-ledger heal) keeps closed bars only too. An event dated in the future is outside both. The deep pass reaches it only once its 1D "bar" has closed, which is the *end* of the effective day. GMEX, SPRC and YAAS (effective 11-27) were healed at 00:00 ET on 11-28 that way.

**This was a design gap, not only a pre-rollout leftover.** Under the gapless design, a split write lost past log retention would be dropped by the ledger heal, and never retried.

**The fix.**
- Buckets whose attribute group is a `corpaction` kind are events, not bars.
- Every backfill pass compares their whole range, future dates included, with the local rows and writes the rows that differ. The watermark stays at or below now.
- `HealRange` keeps future-dated events. Bars keep the closed-bar rule.

**Tests and proof.**
- *Tests.* `TestBackfillCopiesFutureDatedSplits` (shallow and deep pass) and `TestHealRangeKeepsFutureDatedSplits`. Both failed before the fix.
- *Live.* p1's first pass after the restart (04:51:48 MST) wrote `RC/1D/SPLITS`. Its SHA-256 equals taichi's (`c4af6ecd…`).

### 3. Watchlist baselines reload when their 1D inputs change (`727670ca`)

**The mechanism.**
- *Triggers.* The watchlist trigger is now also bound to `*/1D/OHLCV` and `*/1D/SESSIONS`. These entries are in `mkts.yml` on both hosts, with backups at `mkts.yml.bak-20261128`.
- *Which writes count.* A write to a date whose afterhours has ended marks the symbol. A date's own 1D bar, rewritten every minute while it forms, is ignored until then.
- *The reload.* On the next ranking tick, the worker reloads the marked symbols' baselines for their live day and for the loaded next day. Full loads and reloads are serialized, so an older full load cannot overwrite a newer reload.
- *The rewind cache.* The same signal drops the rewind cache from the changed date. On a replica that cache was never invalidated, because `OnChange` fires only for the leader's own facts writes.

**Tests.** The `baseline_inputs_test.go` tests replay the 11-25 sequence: the baselines load with the ondiskagg close 8.13, and the official close 8.15 is written afterwards.
- *With the fix.* The traditional `prior_close` is 8.15.
- *With the trigger path disabled.* It stays at 8.13, and all three tests fail.

**Deploy.**
- *taichi.* `727670ca` via `deploy.sh`, restarted at 06:49 ET. The startup sequence reproduced the incident and corrected itself:
  - 04:49:39 MST: baselines loaded;
  - 04:49:58: the REST 1D backfill complete;
  - 04:50:40: "reloaded baselines of 11326 symbols whose 1D inputs changed in 1.807s".
- *p1.* Built at `727670ca` (binary and `watchlist.so`), shut down cleanly ("exiting..."), and started with `start.sh` at 06:51 ET. It resumed at sequence 1712526 with lag 0 and gaps 0, and seeded 10,544 symbols, the same as taichi.

**Result at a quiet time (11-28 06:53 ET).**
- *Live lists.* 6 of 8 are byte-identical, `MOMENTUM` and `RELATIVE_VOLUME` included (it was 4 of 8 before).
- *Rewound 11-27 regular session, as of 12:55.* 16 of 20 are identical, every `*_TRADITIONAL` list included. BNAI's traditional `prior_close` is 8.15, the official close, on both hosts.
- *What still differs.* Only `SECTOR_AGGREGATE*` and `INDUSTRY_AGGREGATE*`: cause 2, the host-local postgres. No decision on that yet.

**Observed, not fixed (Tier 4).**
- *What.* taichi reset p1's idle replication stream twice, 9.6 h apart (11-27 19:06 and 11-28 04:41 MST). Both times p1 resumed at the exact next sequence.
- *Likely cause.* Neither side sets a gRPC keepalive, so an idle-connection reset is plausible.
- *Effect.* Harmless with the resumable stream.

## Replication keepalive (`9c13cd19`, deployed 2026-11-28 07:57 ET)

**What the two resets were.**
- At 04:41:26, taichi logged "closed replication connection" and p1 logged "connection reset by peer" within 3 ms of each other.
- At 19:05:34, taichi closed the stream first, and p1 noticed 26 s later.
- Neither log gave a reason. Neither host logged a network event. p1 reaches taichi over Wi-Fi (`wlp9s0f0`, -60 dBm, 0% loss in a 100-ping test), and taichi is on Ethernet.
- **The cause is still unknown.**

**The change.**
- *Pings.* Replica and master ping every 30 s, with a 20 s ack timeout. This covers the replication stream and the backfill's query connection.
- *Server policy.* Both gRPC servers accept pings as often as every 10 s. With gRPC's default policy (5 min), a pinging client is closed with `too_many_pings`.
- *Logging.* The master now logs the context cause when a stream ends, so the next reset will say why.

**Proof.**
- `TestIdleConnectionSurvivesKeepalivePings`: under the default server policy the idle connection is closed with `ENHANCE_YOUR_CALM "too_many_pings"` within 40 s. Under `serverKeepalive` it survives 80 s.
- *Live.* After both hosts restarted, there were no GOAWAY messages, and the stream was up with lag 0.

## Sector and industry data: where it comes from (investigated 2026-11-28)

`SECTOR_AGGREGATE` and `INDUSTRY_AGGREGATE` read `fa_techan` on each host. That is the dashboard's database. The `fin` database, the home `fin-stack` D3 intends for symbol data, has the schema on both hosts but no rows.

**Two writers, two taxonomies.** Both are in `~/dev/fin`:
- *`fin init`.* It reads the Nasdaq screener API (`api.nasdaq.com/api/screener/stocks`) for AMEX, NYSE and NASDAQ GS/GM/CM, and gets the **Nasdaq taxonomy**: 12 sectors, including "Finance", "Health Care", "Telecommunications" and "Miscellaneous".
- *`fin sync-symbols`.* It reads Massive, Alpaca and Nasdaq submarkets, and classifies only equities with `sector_id IS NULL`, through yfinance: the **Yahoo taxonomy**, 11 sectors.
- *What decides.* The first command to classify an equity wins permanently, because nothing reclassifies it.

**Which host got which.**
- *taichi* was seeded by `fin init`. Its asset ids run in init's exchange order: AMEX 1–274, NYSE, GS, GM, CM. It has 0 Yahoo sector names. It has no shell history for these commands, so why later `sync-symbols` runs added no Yahoo names is unverified.
- *p1* was built by `reset_db.sh` and then `uv run fin sync-symbols`, several times, per its shell history. It was never initialised with `fin init`. Its ids are interleaved: 3,788 runs of exchanges.

**Also found (fin, Tier 4).**
- *Mixed parents.* `fin init` links each industry to the first sector it meets. On taichi, 845 equities have an industry whose parent sector is not their own sector.
- *Mixed taxonomies to come.* Any future yfinance enrichment would put Yahoo names into taichi's Nasdaq table.

## Pre-rollout history holes (found and healed 2026-11-28)

**How it was found.** A file-by-file comparison of every `*/1D/*` year file (93,584 common files) found the following on p1:
- `1D/SESSIONS`: the 2025 file was missing for 872 symbols, and 691 files for 2026 differed. The 2026 rows that were only on taichi date from January to mid-July 2026, plus 11 August/September values that differ.
- `1D/OHLCV`: identical, except 62 old rows that exist only on p1.

`compare --days 400` (from 2025-10-24) confirmed 101,696 rows missing on p1.

**Why the backfill never filled it.**
- The rows were lost through the old, lossy stream before the rollout.
- All 916 affected buckets had a p1 watermark of 2026-11-27. `BackfillBucket` reads `[watermark, now]` plus a 24 h deep lookback, so a row dated before the watermark is never fetched again; its own comment says so.
- The 5-day parity window cannot see it.
- Since the rollout, writes go through the retained log and the ledger heal reaches any date. So this is a one-time pre-rollout hole, not an ongoing leak, but nothing re-checks old history.

**The heal (approved).**
1. Stopped p1 cleanly at 08:24 ET.
2. Backed up `replication_watermarks.json` to `replication_watermarks.json.bak-20261128`.
3. Removed the watermarks of the 916 `1D/SESSIONS` buckets.
4. Restarted p1.

The startup deep pass then read their full history: "missing=175054 revised=11 written=175065", in 29 s.

**Verified.**
- *Files.* All 12,665 `*/1D/SESSIONS/*.bin` year files are byte-identical on both hosts, with none missing.
- *Rows.* `compare --since 2025-01-01 --buckets 1D/SESSIONS` (new flag, `9508e330`): 1,317,153 rows identical, 0 missing, 0 differing.
