## Context

See `proposal.md` for the motivation and the specs for the required behavior. The current state that shapes this design:

- **Running state** is one `SymbolState` per symbol for the current New York calendar day (`contrib/watchlist/framework/day_state.go`, commit `1cd47c19`). Volume is a per-minute ledger. A 1Min bar replaces its minute, and 1Sec bars count only for minutes that have no 1Min bar yet. `DayOpen` is the earliest bar of the day, which is the 04:00 bar. On a day roll, `PriorClose` becomes the last price seen, which is an afterhours print.
- **Baselines** (`baseline.go`) read ~60 1D bars per symbol at startup. They give `MedianVolume50D` (consolidated regular-session volume) and `PriorClose = closes[len-2]`.
- **Ranking loop** (`worker.go`): every `ranking_interval_ms` (60s in the p1 `mkts.yml`), it runs every strategy over the curated states and pushes `watchlist_update` on `WATCHLISTS/1Min/<NAME>`. Strategies get **shared `*SymbolState` pointers** that triggers keep changing while strategies read them.
- **Strategies** live in `../marketstore-watchlists`:
  - PCT_CHANGE_UP/DOWN, RELATIVE_VOLUME, MOMENTUM, GAP_UP/DOWN, SECTOR/INDUSTRY_AGGREGATE, SMA_CROSS_UP/DOWN.
  - VOLUME_UP/DOWN exist in `contrib/watchlist/defaults` and are configured in `mkts.yml`, but the plugin never registers them, so they are never produced today. This change registers them.
  - Each keeps scratch slices between calls, so it is not safe for concurrent use.
  - SMA_CROSS caches daily closes keyed on `time.Now().YearDay()`.
- **Host/plugin boundary:** `bgworker.WatchlistDataSource` (3 methods), bridged by `cmd/start/plugins.go` to `frontend.WatchlistProvider`. The frontend serves it over JSON-RPC (`list_watchlists.go`), gRPC (`grpc.go`) and REST (`rest_handlers.go`).
- **Calendar** (`contrib/calendar`) knows trading days, early closes and the 04:00 extended open. `IsRegularMarketOpen` hardcodes 09:30.
- **Data:**
  - 1D bars are regular-session bars from the vendor, with the official close.
  - 1Min bars cover 04:00–20:00 ET (checked on AAPL, MSFT and TSLA).
  - There are ~11.8k symbols with 1Min data. The 2026 1Min files take 82 GB.
- **Deployment:** taichi is the leader and p1 is a replica. On a replica, writes go to `ErrorWriter`, and triggers still fire. Bgworkers have no replica filter (`mkts.yml` notes, `internal/di/trigger.go`). `utils.InstanceConfig.Replication.IsReplica()` tells a plugin which role it has.
- **Local-mode CLI:** `cmd/connect/main.go` never loads the server config, so `InstanceConfig.Timezone` stays at its default (UTC).

## Goals / Non-Goals

**Goals:**
- Live and rewound rankings come from **one** computation path, so they cannot drift apart.
- Strategies stay unaware of sessions and bases where possible. The framework prepares what each strategy sees.
- A rewind never blocks or changes the live ranking loop.
- `1D/SESSIONS` follows derived-data requirements R1–R7 (see `specs/watchlist/session-facts`), and its package is kept separate so it can later become a general derived-series framework without rework.

**Non-Goals:**
- A general derived-series framework. There is one hand-written derivation.
- Storing ranking snapshots, and connecting `/ws/replay` to watchlists. `RankingsAt` makes that a small follow-up.
- Changing how 1D bars are produced (the ondiskagg filter settings).
- Per-session curation thresholds. The existing curator runs on session-scoped values without changes.

## Decisions

### D1. Session boundaries live in the calendar
Add `SessionBounds(date) → {Pre, Reg, Post}` (half-open, in the calendar timezone) to `contrib/calendar`. It returns an error for non-trading dates. `IsRegularMarketOpen` is changed to use it, which removes the hardcoded 09:30. The resolution rules from the trading-sessions spec go in a small `framework/session` package: `Resolve(session, asOf, now) → Window{TradingDate, Session, Start, End, Complete}`.
- *Alternative:* compute boundaries inside the watchlist framework. Rejected: early closes and holidays already live in the calendar, and other plugins (ondiskagg) need the same boundaries.

### D2. One fold function, applied per session
Extend the existing `applyBars` ledger so that `SymbolState` holds one accumulator per session (`open`, `openEpoch`, `high`, `low`, `last`, `lastEpoch`, volume ledger) for the current trading date. Each bar goes to the session that contains its epoch. Baseline fields (the official closes and the session closes needed as `prior_close`, plus the medians) sit next to the accumulators.

Rewind uses the **same** fold on bars read from disk into a fresh state. So "live equals rewind" (rankings-api spec) holds by construction, and a test compares the two directly.
- *Alternative:* a separate `SymbolState` per session. Rejected: it triples the map overhead and the baseline plumbing for no behavioral gain.
- *Known limit:* live high and low only ever widen. A rewrite that lowers a bar's high is not undone live. Rewind reads final bars, so it is exact. This matches today's behavior.

### D3. Strategies see a per-basis view, not live state
For each ranking cycle and each basis available in the session, the framework builds a **value snapshot** per curated symbol. It fills the fields strategies already read (`PriorClose`, `PctChange`, `DayOpen`, `CumulativeVolume`, `VolumeMultipleOfMed`, `LastPrice`, …) with that session's and basis's values. The `Extra` map is shared, as today.
- Strategies keep their current code, and `pct_change`/`prior_close` get their meaning from the basis.
- It also removes today's data race, where strategies read `*SymbolState` while triggers write to it.
- It's cheap: one struct copy per curated symbol per basis per cycle, at a 60s interval.
- *Alternative:* add basis parameters to every strategy. Rejected: it changes every strategy and repeats the basis logic in each one.

### D4. Basis support is declared by the strategy; the framework names the output
New optional interface: `BasisSupport { Bases() []Basis }`. Without it, a strategy supports both bases. The framework publishes each basis's result under `Name()` for the session basis and `Name()+"_TRADITIONAL"` for the traditional basis. The traditional result is produced only in the regular session.
- GAP and SMA_CROSS declare traditional only, so they appear only as `GAP_UP_TRADITIONAL` etc., with no change to how they are registered.
- *Alternative:* register traditional variants by hand in the plugin. Rejected: it doubles registrations and makes it easy to miss one.

### D5. As-of context for strategies that read disk
New optional interface: `ContextualRanker { RankAt(ctx RankContext, curated) }`, where `RankContext = {TradingDate, Session, Basis, WindowEnd}`. The framework calls `RankAt` when it exists and `Rank` otherwise. SMA_CROSS implements it and keys its daily-close cache on `ctx.TradingDate` instead of `time.Now()`, so a rewind reads the closes from before that date.

### D6. Rewind engine
`RankingsAt(query) → Result`:
1. Resolve the window (D1).
2. Read the window's 1Min bars for the symbols that have 1Min data, in batches.
3. Load the baselines for that date (D7, D8).
4. Fold into fresh states (D2).
5. Run the curator on them.
6. Build the basis views (D3).
7. Run **fresh strategy instances** made from the registered factories. Live instances keep scratch state and are not safe for concurrent use.

Protection for the live path:
- At most one rewind computation runs at a time (single-flight per window, then a semaphore).
- Results for complete windows go into an LRU cache keyed by `(date, session, facts version)`. R4 invalidation clears the affected dates.
- *Alternative:* reuse the live strategy instances under `rankingMu`. Rejected: a slow rewind would stall the live loop.

### D7. Session facts (`1D/SESSIONS`)
A separate package, `contrib/watchlist/sessionfacts`. Its interface is `Version`, `Compute(date, bars) Row`, `Write(rows)`, `Read(symbol, from, to)`, so it can later be moved into a framework.
- **Row.** Epoch = the trading date, the same as 1D bars. Columns: `PreVolume/RegVolume/PostVolume int64`, `PreClose/PostClose float64`, `PreBars/RegBars/PostBars int32` (0 means the session had no bars), `Version int32`. A fixed-width bucket, so it is replicated like any other.
- **R2 (rebuildable).** Live rows are computed **from the 1Min bars on disk**, not from the in-memory ledger. The job runs after afterhours ends plus a grace period, and it is the same `Compute` the rebuild command uses. It reads about 960 bars per symbol per day, once a day.
- **R3 (versioned).** A reader drops rows with a version other than the current one, and they count as missing.
- **R4 (late data).** The 1Min watchlist trigger already sees every write, and `applyBars` already notices earlier-day bars. Those now add `(symbol, date)` to a dirty set. The set is written to an append-only journal under the root directory, so a restart doesn't lose it. The bgworker works through it every few minutes. Journal entries are removed after the recompute is written.
- **R5 (rebuild command).** `marketstore tool session-facts rebuild --from --to [--symbols]`, with two modes:
  - **offline** (server stopped): writes through the executor, for the initial backfill
  - **online**: calls a leader-only admin RPC, which queues the dates on the bgworker, for repairs while the server runs
- **R6 (fallback).** A missing or old-version row is computed from 1Min bars when needed, and cached in memory.
- **R7 (leader only).** The writer, the journal and the job are turned off when `utils.InstanceConfig.Replication.IsReplica()` is true. Replicas only read.
- *Alternative:* keep facts in memory and rebuild at startup. Rejected: that is a ~20 GB scan per startup. *Alternative:* a separate database. Rejected: it wouldn't be replicated, and it would be a second source of truth (see the derived-data discussion in the proposal).

### D8. Baselines and medians
- **Official close RC(date):** the 1D close. For today before the 1D bar exists, the last regular close in the live ledger.
- **Session `prior_close`:** premarket uses `PostClose` of the previous trading date's facts; regular uses today's live premarket `last` (a rewind uses `PreClose`/fold); afterhours uses RC(today).
- **Traditional `prior_close`:** RC(previous trading date), from the 1D bar of **the previous trading date by calendar**, not `closes[len-2]`.
- **Medians:** computed on read from the last N facts rows before the date, per session (R1). Loaded at startup (about 50 rows per symbol) and refreshed when the day rolls. For a rewind, loaded for the requested date.

### D9. API surface
- `bgworker.WatchlistDataSource` gains `Rankings(q RankingQuery) (RankingResult, error)`. `RankingQuery = {Names, Session, AsOf}`, and the result carries each list's metadata. The old three methods stay and call it with an empty query. **ABI change:** the host and `watchlist.so` are rebuilt together, which `deploy.sh` already does.
- `frontend.WatchlistProvider` mirrors this. Resolution errors are exported sentinels (`ErrNotTradingDay`, `ErrSessionNotStarted`, `ErrNotInSession`, `ErrInvalidSession`, `ErrInvalidAsOf`). REST, gRPC and JSON-RPC map them to status codes per the rankings-api spec.
- Proto: `ListWatchlistsRequest` adds `session = 2`, `as_of = 3`. `WatchlistRanking` adds `basis`, `session`, `trading_date`, `window_start`, `window_end` (Unix seconds) and `complete`. These are additions only, so existing clients keep working.
- REST: the query parameters `session` and `as_of` on both watchlist endpoints.

### D10. Live rollover
At each tick, the ranking loop resolves the live window (D1). An extra tick is scheduled at each session boundary, so rollover doesn't wait up to one `ranking_interval_ms`. On a trading-date roll, baselines move forward: today's facts become the previous day's.

### D11. CLI timezone
`marketstore connect` gets `--config` (default: `./mkts.yml` when present) and `--timezone`. Local mode applies the timezone to `utils.InstanceConfig.Timezone` before opening the directory, and warns when it falls back to UTC. This goes in its own commit (Tier 3).

## Risks / Trade-offs

- **[Breaking meaning under the same field names]** During the regular session, ta-droid's "Top Gainers" (`PCT_CHANGE_UP`) silently switches from the move since yesterday's close to the move since the premarket close. → The ta-droid update ships in the same deploy. It maps "today's gainers" to `*_TRADITIONAL` and shows `basis`/`session`. Unknown list names already fall back to the raw name in ta-droid.
- **[Initial backfill cost]** A one-time scan of about 60 trading days of 1Min history (~11.8k symbols). → The offline rebuild runs before the new version serves traffic. It is resumable by date and can be limited to a date range.
- **[Fallback storm]** If facts are missing for many dates (e.g. the backfill was skipped), R6 would scan 1Min history when needed across the whole universe. → The migration order below makes this a non-issue in normal operation. The fallback cache is bounded, and a warning is logged once when the fallback rate is high.
- **[Lost invalidations]** Dirty marks are lost if the process crashes between a late write and the journal append. → The journal append happens in the trigger, before anything else. A daily reconcile recomputes the previous 3 trading dates as a safety net.
- **[Official close changes after the fact]** RC(today) comes from the 1Min ledger until the vendor's 1D bar lands, then from the 1D bar. So afterhours `prior_close`/`pct_change` can shift by a few cents once, when the 1D bar arrives. → Accepted. The value becomes more correct.
- **[Rewind load]** Rewinding over the whole universe reads one session of 1Min bars (up to ~4.6M rows for regular). → One rewind at a time, plus the LRU cache for complete windows. Live ranking runs on its own goroutine and is never blocked by a rewind.
- **[Memory]** Per-session accumulators add a few fields per symbol. The per-minute ledger is the same size as today (one trading date).
- **[Replica role]** A misconfigured replica with no `master_host` would act as a leader and try to write facts. → The same risk exists for all leader-only behavior today. Nothing new.

## Migration Plan

1. Merge and deploy the marketstore changes and the marketstore-watchlists changes together (ABI). `deploy.sh` rebuilds both.
2. With the leader (taichi) stopped, run `marketstore tool session-facts rebuild --from <today-60 trading days> --to <yesterday>` in offline mode. Start the leader. It writes facts from then on.
3. The replica (p1) receives the facts through replication. Restart p1 on the new build.
4. Deploy the ta-droid update.
5. **Rollback:** redeploy the previous marketstore and watchlist builds. The old code ignores `1D/SESSIONS`, and the bucket can stay. Roll ta-droid back at the same time, since the list names and meanings go back.

## Open Questions

- The grace period after afterhours ends before the daily facts job runs (it depends on how late the vendor's 1D bars and the end-of-day fills arrive). This can be tuned from logs after deploy without changing behavior.
