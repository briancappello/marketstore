# Notes

## Baseline (task 0.1, before any change, HEAD 0d0c2a9d)

`go test ./uda/... ./sqlparser/... ./frontend/... ./executor/... ./contrib/massive/...`:
17 packages ok, 0 failed.

## Deviation: ICE loader kept in uda/adjust

`contrib/ice/cmd/reorg/show.go` imports `adjust.NewCorporateActions`, `Load` and
`RateChangeEvents`. Removing them would break the build of `contrib/ice`, which
the user asked to leave untouched. The ICE types stay in `uda/adjust/caloader.go`
for that command only; `Adjust.Accum` no longer uses them. Only the racy
`RateChangeCacheMap` / `GetRateChanges` cache is removed.

## Pre-existing flaky executor test (not caused by this change)

Under parallel load, `go test ./executor/` sometimes panics in
`wal.ReadStatus` ("slice bounds out of range [2:0]") or with "Failed attempt to
write to WAL". This change does not touch `executor/`. On the baseline commit
0d0c2a9d, 6 parallel copies of the executor test binary x 5 rounds failed 12/30
with the same panic. Serial runs pass 10/10 on both baseline and HEAD.
Fixed afterwards (separate commit): flushes run inline from writer
goroutines whenever no SyncWAL loop drains flushChannel (before it starts,
during shutdown, or without BackgroundSync), and concurrent flushes raced on
the shared WAL file offset. A mutex now serializes flush, checkpoint and
rotation. `TestConcurrentInlineFlushes` fails 5/5 without the fix; the same
6x5 parallel stress now fails 0/30.

## Verification (task 6.2)

- 20 packages ok across uda, sqlparser, frontend, executor, contrib/massive,
  contrib/ice, corpaction. No new failures. The only failure seen was the
  pre-existing executor flake above.
- `go test -race` clean: uda/adjust, corpaction, contrib/massive,
  contrib/massive/splitsync, contrib/massive/api.
- `go vet` clean on corpaction, uda/adjust, contrib/massive/...
- `openspec validate split-events-adjust-on-read --strict`: valid.
- massive.so builds with -buildmode=plugin.

## Taichi rollout (task 6.1, verified live instead of a local stub), 2026-11-23

- Config: removed `backfill_adjusted: true` from taichi `~/dev/marketstore/mkts.yml`
  (backup `mkts.yml.bak-20261123`). deploy.sh copied it to prod.
- Deployed e574563a. The startup split sync read 28,123 Massive results and wrote
  5,639 events. The sync now settles at 0 written / 3,088 symbols unchanged.
- Live `adjust('split')` on NVDA/1D: 2024-06-07 close 1208.88 -> 120.888,
  volume x10; 2024-06-10 onward unchanged.
- Before the repair: tar of the 2,860 affected 1D/1Min buckets at
  `taichi:~/marketstore-backups/pre-split-repair-20261123/ohlcv-1D-1Min.tar`.
- Real split contamination found: 53 bars, all VSEE (1:80 reverse split on
  2026-09-24). 52 1Min bars and the 1D bar of 2026-09-23 were stored adjusted:
  on 9/24 the 9/23 flat file was not published yet, so the plugin backfilled
  9/23 over REST with adjusted=true. Also 2 VSEE 1Sec bars from the same
  REST gap-fill.
- The first repair version rewrote whole ranges. REST rounds prices to 4
  decimals and flat files keep 5-6, so it replaced 1.7M 1Min bars with
  rounded ones, and replaced flat-file 1D volumes. Restored from the backup
  through gRPC, then reran the fixed repair (it matches split factors):
  53 bars (1Min/1D) + 2 (1Sec) rewritten, and a re-run rewrites 0.
- Correction: that first restore compared floats with a 1e-6 relative
  tolerance. Most 4-decimal rounding falls below it (6.771796 -> 6.7718), so
  44,389 rounded rows in 756 buckets stayed rounded, and the diff checks had
  the same blind spot. Found while comparing p1 with taichi. Restored again
  with bit-exact comparison. A bit-exact diff now shows only VSEE's 53 bars
  differing from the backup in history.

## p1 replica sync, 2026-11-23

p1 (replica, started via ./start.sh) shut down cleanly at 18:41, before the
taichi repair, and had no SPLITS buckets. Replication would not have healed
the old bars, because the deep pass reaches only 24h back and the replica
rejects outside writes. With p1 stopped, `rsync --checksum` pulled from taichi
every content-different file in the 2,860 repaired buckets, VSEE/1Sec and all
3,088 */1D/SPLITS buckets: 2,766 bar files, with p1's old copies in
`~/marketstore-backups/p1-pre-split-sync-20261123`. p1 was rebuilt at
5f012889 and restarted. After its WAL replay, a second checksum pass shows 0
content differences, and live queries (VSEE raw/adjusted, NVDA adjusted,
SPLITS) match taichi.
- Left in place: 13,738 historical bars that the first version added where
  the store had a gap (mostly OTC periods flat files omit). They are raw
  Massive bars, and MarketStore cannot delete single rows.
