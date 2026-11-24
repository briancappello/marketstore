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
Tier 4: needs its own investigation (WAL status read racing a truncated file).

## Verification (task 6.2)

- 20 packages ok across uda, sqlparser, frontend, executor, contrib/massive,
  contrib/ice, corpaction. No new failures. The only failure seen was the
  pre-existing executor flake above.
- `go test -race` clean: uda/adjust, corpaction, contrib/massive,
  contrib/massive/splitsync, contrib/massive/api.
- `go vet` clean on corpaction, uda/adjust, contrib/massive/...
- `openspec validate split-events-adjust-on-read --strict`: valid.
- massive.so builds with -buildmode=plugin.
