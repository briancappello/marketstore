## Context

See proposal.md (Why) for the motivation. These facts about the current code shape the approach:

- `uda/adjust` is registered in `sqlparser/registry.go` as `&adjust.Adjust{CatalogDir: catDir}`. It loads events through `planner` and `executor` from `<SYM>/1D/ACTIONS`, using ICE's column set (`TextNumber`, `Status`, `Rate`, ...). It imports `contrib/ice/enum`.
- `Adjust.Accum` adjusts only `float64` columns plus a column named `Volume`. It divides every `float64` column by the rate. In `mkts.example.yml`, OHLCV prices are `float32` (skipped), TRADE `Size` is `float64` (wrongly divided), and QUOTE sizes are `uint64` (skipped).
- `RateChangeCacheMap` is a plain package-level map with a 24h lifetime. Concurrent queries read and write it without a lock (a data race). A new event can stay invisible for up to 24h.
- Fixed-length buckets overwrite by epoch on write (`executor.WriteCSM`; the `ondiskagg` notes in `mkts.example.yml` rely on this). ICE wrote `ACTIONS` as variable-length, which appends. That is why ICE needed `TextNumber`-based dedup.
- The massive plugin writes either in-process (`executor.WriteCSM`) or through gRPC (`backfill.RPCWriter`, used by the standalone commands). Its REST helpers in `contrib/massive/api` already handle retries, gzip, `next_url` paging and `ErrAuthFailed`.
- The server timezone is `America/New_York` (`timezone` in `mkts.yml`). Massive split dates are calendar dates, and the split applies from the pre-market session of `execution_date`.

## Goals / Non-Goals

**Goals:**
- A core package with no dependency on any provider. The UDA and any feeder use it to read and write events.
- Adjust logic that only uses `PriceFactor` and `VolumeFactor`, so adding a kind later means adding a bucket, not changing math.
- No new external dependency. No change to the server's write path or to the replication protocol.

**Non-Goals:**
- Changing or deleting `contrib/ice`. It still compiles, but nothing reads its bucket.
- Adjusting data on the WebSocket push paths (watchlist, stream). Only the query-time `adjust()` function changes.
- A general "tombstone" or delete API in the executor. Withdrawn events are neutralized in place (see D3).

## Decisions

### D1. New core package `corpaction/`, top level
It holds:
- `Kind`: only `Splits` for now. Each kind has its attribute group name, `SPLITS`.
- `Subtype` constants.
- `Event{Symbol, Kind, Effective time.Time, PriceFactor, VolumeFactor, SplitFrom, SplitTo, Subtype}`.
- `EncodeCSM(events) (io.ColumnSeriesMap, error)`, which groups events by bucket, merges same-day duplicates (product of the factors) and sorts by epoch.
- `Load(catalogDir, symbol, kinds...) ([]Event, error)`.
- `Neutralize(stored, fresh []Event) []Event`, which returns rows with factors of 1.0 for each stored epoch that is not in `fresh`.

It depends only on `catalog`, `planner`, `executor` and `utils/io`. The package encodes but does not write. Callers pass the CSM to their own writer (`executor.WriteCSM` in-process, or `RPCWriter` from the standalone commands). This keeps the package free of gRPC.

*Alternatives:* putting it in `uda/adjust` would tie it to one consumer, and feeders importing a UDA package would read the wrong way round. Putting it in `contrib/massive` would not be provider-neutral, which the user asked for.

### D2. Storage: a fixed-length `<SYM>/1D/<KIND>` bucket, one per kind
Fixed-length 1D gives overwrite-by-day for free, which makes re-syncs idempotent with no ID column. A separate bucket per kind avoids the one collision that matters in practice: a split and a dividend on the same day. Same-kind, same-day collisions (a split plus a stock dividend) are merged by multiplying the factors before the write.

The `Epoch` is the effective date at 00:00 America/New_York. With `timezone: America/New_York`, that falls inside the same 1D slot, and the stored epoch is kept exactly. Task 1.3 checks this with a round-trip test, because the 1D slot boundary depends on the system timezone.

*Alternatives:*
- Variable-length `ACTIONS`, like ICE: appends, so it needs a dedup key, and Massive IDs are 65 characters (more than `string16` holds).
- A single bucket with a `Kind` column: same-day collisions across kinds.
- A Postgres table: adds an external dependency to the core query path, and does not replicate.

### D3. Withdrawn events are overwritten with factors of 1.0
MarketStore has no row delete. A sync loads the stored events for each symbol it syncs, and appends `Neutralize(stored, fresh)` rows to the same CSM write. `Load` filters out rows where both factors are 1.0, so callers never see them.

*Alternative:* destroying and rewriting the bucket. It leaves a window where the bucket is empty, so a query in that window would silently return unadjusted data.

### D4. Adjust math works on factors and column roles
`Accum`:
1. Loads the events for the requested kinds.
2. Drops every event with `Effective > now`. The clock is injected so tests can control it.
3. Walks the rows from newest to oldest, as today, building cumulative `pf` and `vf`.
4. Applies `x/pf` to price-role columns and `x*vf` to size-role columns, through a type switch over `[]float32`, `[]float64`, `[]int64` and `[]uint64`. The other columns pass through.

Column roles are fixed name sets in `uda/adjust`. Price `float` values are rounded to 4 decimal places, as today. Integer sizes use `math.Round`, where today they are truncated.

*Alternative:* deciding roles by type, as today. That is the source of the current bugs, because a `float64` can be either a price or a size.

### D5. Cache: a per-symbol TTL cache behind a lock, 30s by default
Replace `RateChangeCacheMap` with a small struct guarded by `sync.RWMutex`, keyed by `(symbol, kinds)`, with a 30s TTL. Events are loaded only from 1D buckets, so a miss costs very little. A 30s TTL meets the spec's 60s visibility bound even for events written through gRPC by another process. Neither of the two processes needs an invalidation hook.

*Alternative:* an `executor` write hook that invalidates the cache. That is precise, but it couples the executor to a UDA, for a gain the spec does not need.

### D6. Massive split sync is a separate scheduled loop
`api.ListSplits(client, params)` follows `ListTickers`: all pages, `limit=5000`, `sort=execution_date.asc`. A sync makes one market-wide request with no ticker filter. The full history is small (tens of thousands of rows, a few pages), and a full history is what D3 needs. Then the sync:
1. Builds the tracked set: the configured symbols, plus the `Symbol` items from `catalogDir.GatherCategoriesAndItems()`. With `*`, every ticker Massive returns.
2. Groups the results by ticker. For each tracked ticker, it loads the stored events, adds the neutralized rows and writes in batches of 500 symbols per `WriteCSM` call.

The loop runs on its own goroutine under `mf.wg` and `mf.ctx`, not through `backfillRunner`. A split sync must never wait behind a multi-hour backfill.

*Alternative:* `ticker.any_of` batches over the tracked set. That needs more requests for the same data, and it does not cover the `*` case.

### D7. Raw-only enforcement sits at the API boundary
`getAggregates` drops its `adjusted` parameter and always sends `adjusted=false`. `paginateAggregates` fails if any page reports `adjusted: true`. Every caller (`rest.Bars`, `fetchBarsWindow`, both commands) loses its `adjusted` argument, so the compiler finds every call site. `ValidateConfig` handles the `BackfillAdjusted *bool` pointer: `true` returns an error, `false` logs a warning. The field stays in the struct, so old configs still parse.

### D8. The repair command is a new `contrib/massive/backfill/repair/cmd`
It reuses the REST command's flag parsing helpers for the gRPC address, API key, parallelism and symbol filter, plus `rest.Bars`. It finds the stored buckets and their first bars by querying the server over gRPC (`ListSymbols` / `Query` with `limit_from_start=1`). It reads splits from `api.ListSplits` (spec: it works even before any server sync) and computes the ranges. For each range it fetches the raw bars and reads the stored bars over gRPC. Then, finest timeframe first, it rewrites through `RPCWriter` only the bars that match `raw / F` (within `-tolerance`, default 1%), where F is a cumulative product of the later splits' factors. `--dry-run` stops after the planning step.

The compare step was added after the first taichi run. Rewriting every bar in the range replaced 1.7M flat-file 1Min bars (5 to 6 price decimals) with REST bars (rounded to 4 decimals), and replaced flat-file 1D volumes with REST volumes. Only 53 bars (VSEE) were really split-adjusted. A plain "differs by more than 1%" check was still too broad: it rewrote 59 bars, and only 53 of them were split-adjusted. The other 6 were real store-versus-REST disagreements, and the repair should not change those. Matching against the split factors themselves rewrites only bars that a split explains.

*Alternative:* adding a mode to the existing REST command. That command is already 594 lines, and its sync-window bookkeeping (Postgres `sync_queries`) must not be updated by a repair.

## Risks / Trade-offs

- [Clients relied on adjusted bars from REST backfill] → Called out as BREAKING in the proposal. Clients must call `adjust('split')`. The README section is rewritten to show this.
- [A 1D slot computed in a timezone other than America/New_York shifts the ET-midnight epoch into the previous day] → Task 1.3 has a round-trip test with the configured timezone. `EncodeCSM` uses `America/New_York` explicitly, never `time.Local`.
- [ondiskagg's `current_period_only` means re-written 1Min history does not re-derive past 5Min/1H/1D bars] → The repair command fetches every stored timeframe directly from REST and does not depend on the trigger (D8).
- [Massive revises a split's date] → The old date is neutralized and the new date is written in the same sync (D3). Until the next sync, at most one day of drift.
- [Market-wide fetch grows over time] → It is still a few pages. If it ever matters, switch to `execution_date.gte=<last sync - 30d>` for daily runs and keep a weekly full sync for neutralization.
- [Replicas] → Event buckets replicate through the WAL like any other bucket. Only the instance that runs the massive bgworker writes them.
- [`adjust()` no longer sees ICE data] → Accepted by the user. No in-tree consumer uses ICE together with Massive.

## Migration Plan

1. Deploy the new binary and `massive.so` together. Remove `backfill_adjusted` from `mkts.yml`, or set it to `false`.
2. On startup, the split sync fills the `<SYM>/1D/SPLITS` buckets. Check the sync log line and the metric.
3. Run the repair command with `--dry-run`, review the affected buckets, then run it for real.
4. Switch clients that need adjusted prices to `adjust('split')`.

Rollback: the old binary ignores `SPLITS` buckets. Bars written raw after the rollout stay raw, which is consistent with the stream. Rolling back only brings back the old mixed-basis behavior for new REST backfills.

## Open Questions

- The exact name of the last-sync metric. It should follow `contrib/massive/metrics` conventions, and it can be chosen during implementation.
