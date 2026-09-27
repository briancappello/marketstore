## 0. Baseline

- [ ] 0.1 Record the pre-change test baseline: `go test ./contrib/watchlist/... ./contrib/calendar/... ./frontend/... ./cmd/...`. Save the pass/fail counts in the change notes, so that later failures that already existed are not blamed on this change.

## 1. CLI local-mode timezone (separate commit, Tier 3)

- [ ] 1.1 Add `--config` (default: `./mkts.yml` when present) and `--timezone` to `marketstore connect`. Local mode sets `utils.InstanceConfig.Timezone` before opening the directory and warns when it falls back to UTC. Verify with a test that writes a bar under America/New_York and reads it back in local mode with the same timestamp. The test fails on the current code.
- [ ] 1.2 Check by hand: `connect -d` on a copy of `data/AAPL` shows the 758k-share opening bar at 13:30 UTC for 2026-09-23 (it shows 08:30 on the current code).

## 2. Calendar sessions

- [ ] 2.1 Add `SessionBounds(date)` to `contrib/calendar`, returning half-open premarket/regular/afterhours windows, with an error for weekends and holidays. Verify with table tests: a normal day, an early close, a holiday, a weekend, an EST date and an EDT date.
- [ ] 2.2 Change `IsRegularMarketOpen` to use `SessionBounds` (no hardcoded 09:30). Verify that the existing `TestEpochIsRegularMarketOpen` and `TestRegularIsStrictSubsetOfExtended` still pass.

## 3. Window resolution

- [ ] 3.1 Add the `framework/session` package with `Resolve(session, asOf, now) → Window`, and the exported error sentinels. Verify with table tests covering every scenario in `specs/watchlist/trading-sessions`: inside, after, before-on-a-past-date, date-only, weekend, holiday, not started, future, and all four omitted-parameter defaults.

## 4. Per-session running state

- [ ] 4.1 Extend `SymbolState`/`applyBars` with per-session accumulators (open, high, low, last, volume ledger) that bars are routed into by their session. Verify with tests: premarket volume is kept out of regular; the regular open is the 09:30 bar; a bar at exactly 09:30 goes to regular; a minute rewritten three times counts once; 1Sec and 1Min bars together don't double-count; EST bars from 19:00–20:00 stay on the same trading date.
- [ ] 4.2 Report `premarket_volume` (during premarket, the volume so far; later, carried forward). Verify with a test.
- [ ] 4.3 Remove the `PriorClose = LastClose` day-roll behavior. Verify with a test that after a day roll with afterhours prints, no field takes its value from an afterhours print unless the spec defines it that way.

## 5. Session facts (`1D/SESSIONS`)

- [ ] 5.1 Create the `contrib/watchlist/sessionfacts` package: row schema (including `PreBars/RegBars/PostBars` and `Version`), a pure `Compute(date, bars)`, and `Write`/`Read`. Verify with unit tests for `Compute` on known bars, and a write-then-read round trip.
- [ ] 5.2 Daily job: after afterhours ends plus the grace period, compute each symbol's row from the 1Min bars on disk and write it. Verify with an integration test that a completed day produces rows equal to `Compute` over the same bars (R2).
- [ ] 5.3 Version check: readers treat rows with an old version as missing. Verify with a test (R3).
- [ ] 5.4 Late-data invalidation: when the 1Min trigger sees a bar for an earlier trading date, append `(symbol, date)` to the dirty journal. The bgworker recomputes those rows and then removes the entries. Add a daily recompute of the previous 3 trading dates as a safety net. Verify with a test that a gap fill for a completed day updates `RegVolume`, and that the journal survives a restart (R4).
- [ ] 5.5 Fallback: a missing or old-version row is computed from 1Min bars when needed, and cached. If the 1Min bars are missing too, report no data. Verify with both scenarios from the spec (R6).
- [ ] 5.6 Leader only: the writer, the journal and the job are off when `Replication.IsReplica()` is true. Verify with a test that a replica config makes no write attempts (R7).
- [ ] 5.7 `marketstore tool session-facts rebuild --from --to [--symbols]`, offline mode (server stopped, writes through the executor). Verify that rebuild equals live rows, and that a second run changes nothing (R2, R5).
- [ ] 5.8 Online rebuild: a leader-only admin RPC that queues dates on the bgworker. The CLI calls it when a server is running. Verify with a test that it is rejected on a replica and that queued dates get recomputed.

## 6. Baselines and medians

- [ ] 6.1 Official close RC(date): the 1D close, and for today before the 1D bar exists, the last regular 1Min close. Verify with tests for both cases.
- [ ] 6.2 Traditional `prior_close` = RC(the previous trading date by calendar), replacing `closes[len-2]`. Verify with a test where today's 1D bar is missing (the old code picks the wrong bar).
- [ ] 6.3 Session `prior_close` per session (previous day's `PostClose`, today's premarket last, RC(today)). Verify with one test per session, matching the ranking-basis spec scenarios.
- [ ] 6.4 Per-session medians computed on read from the last N facts rows before the date. They are loaded at startup and refreshed when the day rolls. Verify with tests: a normal regular session gives 1.0; premarket gives 3.0; changing `median_window` takes effect without a rebuild (R1); only dates before D are used.
- [ ] 6.5 Drop-out rule: symbols missing a needed baseline are left out of rankings that use it. Verify with the "no afterhours prints" and "no premarket prints" scenarios.

## 7. Ranking views and bases

- [ ] 7.1 Build a per-basis value snapshot for each curated symbol and pass the snapshots to strategies instead of live pointers. Verify with tests that the fields match the ranking-basis spec for each session and basis, and that `go test -race` on the ranking loop plus triggers shows no race.
- [ ] 7.2 Add the optional `BasisSupport` interface and the output naming (`NAME` / `NAME_TRADITIONAL`, traditional only in the regular session). Verify with tests of which lists exist in each session.
- [ ] 7.3 Add the optional `ContextualRanker` interface with `RankContext{TradingDate, Session, Basis, WindowEnd}`. Verify with a test strategy that receives the right context, live and rewound.

## 8. Live loop

- [ ] 8.1 Resolve the live window on every tick, and schedule an extra tick at session boundaries. Verify with tests using a fake clock: rollover at 09:30 and at the close; overnight and weekend show the last completed session with `complete` true.
- [ ] 8.2 Push metadata (`basis`, `session`, `trading_date`, `as_of`) in `watchlist_update`, and push traditional lists only in the regular session. Verify with the WebSocket integration test (`integration_test.go` harness).

## 9. Rewind engine

- [ ] 9.1 `RankingsAt(query)`: resolve, read the window's bars in batches, load baselines, fold into fresh states, curate, build views, run fresh strategy instances. Verify with a test that for the same bars, the rewound result equals the live result at the window end.
- [ ] 9.2 Single-flight plus a one-at-a-time semaphore, and an LRU cache for complete windows that R4 invalidation clears. Verify with tests: concurrent identical requests compute once; a gap fill clears the cached date; live pushes are not delayed while a rewind runs.

## 10. API surface

- [ ] 10.1 Add `Rankings(RankingQuery)` to `bgworker.WatchlistDataSource` and bridge it in `cmd/start/plugins.go` and `frontend.WatchlistProvider`. The old methods call it with an empty query. Verify that the existing `frontend/list_watchlists_test.go` and `rest_test.go` pass.
- [ ] 10.2 Proto: `session`/`as_of` on the request and metadata on `WatchlistRanking`. Regenerate the Go code. Verify that `make generate` produces a clean diff and the build passes.
- [ ] 10.3 JSON-RPC, gRPC and REST: parameters, metadata and error mapping (400/404, `InvalidArgument`/`NotFound`). Verify with a handler test for each scenario in `specs/watchlist/rankings-api`, including `GET /v1/watchlists/GAP_UP` returning 404.
- [ ] 10.4 Listing all watchlists returns only the lists available in the resolved session. Verify with a test.

## 11. marketstore-watchlists (edited in `../marketstore-watchlists`)

- [ ] 11.1 GAP and SMA_CROSS implement `BasisSupport` (traditional only). GAP uses the regular open from the traditional view. Verify with a plugin build and a test that only `GAP_*_TRADITIONAL` / `SMA_CROSS_*_TRADITIONAL` are published.
- [ ] 11.1a Register `VOLUME_UP`/`VOLUME_DOWN` (from `contrib/watchlist/defaults`) in `watchlist.go`. Verify with a test that both lists and their `_TRADITIONAL` variants are published in the regular session, and that both bare lists are published in premarket.
- [ ] 11.2 SMA_CROSS implements `ContextualRanker` and keys its daily-close cache on `ctx.TradingDate`. Verify with a test that a rewind to date D uses only closes from before D.
- [ ] 11.3 Rebuild `watchlist.so` against the new host and check that it loads (`make build` in both repos, then start without plugin errors).

## 12. ta-droid (edited in `../ta-droid`)

- [ ] 12.1 Add labels for the `*_TRADITIONAL` lists and for `VOLUME_UP/DOWN`, and remove `GAP_UP/GAP_DOWN`/`SMA_CROSS_*`. Default the "Top Gainers/Losers" view to the traditional lists during the regular session. Verify with `__tests__/watchlists.test.ts`.
- [ ] 12.2 Show `session`/`basis`, and interpret `prior_close`/`open` according to `basis`. Verify with `watchlistsScreen.test.tsx`.

## 13. Rollout

- [ ] 13.1 Run the offline rebuild on taichi for the last 60 trading dates. Verify by checking the row counts per date in `*/1D/SESSIONS`, and by spot-checking AAPL/MSFT/TSLA against 1Min sums.
- [ ] 13.2 Deploy taichi and then p1. Verify that p1 has the replicated `1D/SESSIONS` rows and made no write attempts (logs). Check live rankings through each session boundary for one trading day.
- [ ] 13.3 Re-run the baseline from 0.1 and report failures that already existed and new ones as separate counts.
