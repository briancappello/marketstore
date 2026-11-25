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
