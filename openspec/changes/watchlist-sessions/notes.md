# Implementation notes

## Baseline (task 0.1)

Recorded on master at 61083f46, before any code change (the working tree had the user's
uncommitted `cmd/start/main.go` edit, which is not part of this change):

`go test ./contrib/watchlist/... ./contrib/calendar/... ./frontend/... ./cmd/...`

- 7 packages with tests: 7 pass, 0 fail
  (watchlist/framework, calendar, frontend, frontend/stream, cmd/connect/loader,
  cmd/connect/session, cmd/start)

## Decisions made during implementation

- **as_of today, before the session start, after the session has started** (e.g. now 11:00,
  `session=regular&as_of=<today>T08:00`). The spec's "before the session → whole session" rule
  covers past dates. For today, the window is session start to now (the whole session so far),
  capped at now like every other window. Not an error, because the session has started.
- **Artifact update: `RegClose` column in `1D/SESSIONS`.** The official-close fallback ("last
  regular-session 1Min close") is needed for past dates too, because vendor 1D bars land late
  (on 2026-09-27 there was still no 1D bar for Fri 2026-09-25). Without a stored regular close,
  every such lookup would scan 1Min history. Added to the session-facts spec and design D7/D8.
- **Fallback cache never stores a day still in progress** (`sessionfacts.Service.Rows`). Found
  while wiring baselines: a partial row for today would have been served for the rest of the
  day. Test: `TestIncompleteDayIsNotCached`.
- **`VolumeMedianUser` optional strategy interface.** The drop-out rule for relative volume can't
  be enforced by the view alone: MOMENTUM would rank a symbol with no median at score 0 rather
  than dropping it. Strategies that depend on relative volume declare it (MOMENTUM, in the
  watchlists repo, task 11). RELATIVE_VOLUME already drops such symbols through its minimum.
- **Startup seeding replaced.** `ComputeBaselines`/`seedFromDailyBar` (prior close from
  `closes[len-2]`, running state seeded from the last *daily* bar) are gone, with their tests.
  The worker now seeds the live session's trading date from its 1Min bars and loads that date's
  baselines (`seedDay`). A symbol with no bars in the live session is absent from its rankings,
  as the drop-out rule requires. `baseline_lookback_days` is still parsed but no longer used.
- **Existing races fixed** (required by task 7.1): the trigger wrote `IsCurated` and the curator
  read live fields without the state lock, while the 1Sec and 1Min triggers run on different
  workers. `TestRankingConcurrentWithTriggers` reports the race on the old code.
- **API behavior changes beyond the metadata:** JSON-RPC `ListWatchlists` with an unknown name
  now returns an error (it returned an empty list); REST `GET /v1/watchlists/{name}` for a known
  list with no entries now returns 200 with an empty list (it returned 404).
- **Proto regeneration:** only `marketstore.proto` was regenerated. `marketstore_grpc.pb.go`
  changed only in its protoc version comment, so it was left as it was.

## Test counts after tasks 1-10

`go test ./contrib/watchlist/... ./contrib/calendar/... ./frontend/... ./cmd/...`:
11 packages with tests, 11 pass, 0 fail (baseline: 7/7; the 4 new packages are new code).
`go test -race ./contrib/watchlist/...`: clean.

## Group 11 (marketstore-watchlists)

- **Framework fix found by group 11 (Tier 1):** `checkNames` validated names against the
  registered factories only, so after GAP became traditional-only a request for the bare
  `GAP_UP` passed validation and returned an empty list instead of 404. Names are now checked
  against what each strategy publishes (`framework.PublishedNames`). Test:
  `TestTraditionalOnlyBareNameIsUnknown`.
- **SMA_CROSS latent bug fixed with 11.2:** it read daily closes up to *now* but treated the last
  one as the previous day's. Once the day's 1D bar exists (or in any rewind) that is the ranking
  day's own close. Closes are now read strictly before the ranking date, and cached per date
  (up to 4 dates) so live and rewind rankings don't evict each other.
- MOMENTUM and RELATIVE_VOLUME declare `UsesVolumeMedian`.
- **11.3 was verified without touching p1's artifacts:** `make build` in place would overwrite
  `./marketstore` and `../marketstore-watchlists/watchlist.so`, which p1's `./start.sh` loads.
  Both were built into a temp directory with the Makefile flags and loaded into a throwaway
  leader on ports 15993/15995 with a copy of AAPL's bars. The plugin loaded, all strategies
  registered, the facts job ran, the REST rewind returned values matching the raw 1Min/1D data,
  and the log had no error-level lines.

## End-to-end verification (real server, real clients)

Environment: the built host and plugins (`marketstore`, `watchlist.so`, `ondiskagg.so`, `stream.so`)
from a scratch directory (never the artifacts p1's `./start.sh` loads). The data is a
reflinked copy of 82 liquid symbols' real 1Min/1D history. It ran a **leader** with
production's trigger chain (watchlist on 1Sec/1Min OHLCV, ondiskagg 1Sec→1Min and 1Min→1D)
and a **replica** replicating from it.

Clients: a Go program (gRPC with the new proto, JSON-RPC msgpack, REST, WebSocket stream),
`pymarketstore` (old API, JSON-RPC and gRPC), and the `marketstore` CLI (`connect` in remote
and local mode, `tool session-facts rebuild` offline and `--url`). Every value is checked
against an independent oracle that recomputes from raw bars fetched through the `Query` RPC,
with hardcoded session boundaries.

| Suite | Result |
|---|---|
| Leader: API, rewinds (3 dates × 3 sessions + partial windows), facts, relative volume, live writes + WS pushes + late-data recompute + forced rewind | 233/233 |
| Replica: same suites plus live lists compared with the leader's | 211/211 |
| pymarketstore (old API over JSON-RPC and gRPC, and SESSIONS readable via query) | 11/11 |
| Restart: live lists before == after (3 ticks) | pass |
| R7 on the replica: no journal dir, no facts job, rebuild refused online and offline, SESSIONS files byte-identical to the leader's | pass |
| R2: offline rebuild over live-written rows leaves all 82 SESSIONS files byte-identical; online rebuild (`--url`) drains and leaves them identical | pass |
| CLI local mode (`-d --config`) == remote mode, same timestamps and values | pass |

About 2,500 list entries were checked field by field (price, open, both kinds of prior close,
pct_change, volume, premarket_volume, gap_pct), plus 5,248 SESSIONS rows and relative-volume
multiples against the oracle's own 50-day medians.

### Defects found by the end-to-end run (all fixed and covered by unit tests)

- **The 1D stage must be regular-session only (config, needs rollout).** The leader's
  1Min→1D `ondiskagg` stage (`mkts.yml.example`, and so taichi) has no `filter: "nasdaq"`.
  For the live day it builds a 1D bar from *all* 1Min bars, so its close is the last
  afterhours print (the vendor bar overwrites it later). The baseline loader reads that bar as
  the official close, so afterhours rewinds compute every move as 0%, and the next day's
  traditional baseline would be the afterhours print (bug #3 again). With the filter added,
  the 1D bar is the regular session and every check passes. **Rollout task 13.0 added.**
- **Nondeterministic order of tied entries.** Strategies sorted map-derived entries with
  `sort.Slice` and no tie-break, so a restart or rewind could order ties differently from the
  live ranking. All 10 sort sites (defaults and marketstore-watchlists) now break ties by
  symbol (group name for aggregates).
- **The catalog scanned the `.watchlist` state directory** and warned at every startup. The
  catalog now skips hidden directories.
- `pymarketstore`'s gRPC wrapper reports every error status as "Could not connect" (existing
  bug, other repo, not fixed): the server's NotFound was verified through the generated stub.
