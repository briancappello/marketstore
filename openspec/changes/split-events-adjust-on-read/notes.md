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
