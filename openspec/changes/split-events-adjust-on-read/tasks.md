## 0. Baseline

- [ ] 0.1 Record the pre-change test baseline: `go test ./uda/... ./sqlparser/... ./frontend/... ./executor/... ./contrib/massive/...`. Save the pass/fail counts in `notes.md`, so that failures that already existed are not blamed on this change.

## 1. Corporate-event store (`corpaction/`)

- [ ] 1.1 Create `corpaction/` with `Kind` (`Splits` → `SPLITS`), the `Subtype` constants, `Event` and `NewSplit(symbol, date, from, to, subtype)`. `NewSplit` rejects `from`/`to` values that are not positive and sets both factors to `to/from`. Verify with table tests for forward, reverse and stock-dividend splits and for invalid ratios (spec: Common event columns).
- [ ] 1.2 Add `EncodeCSM(events)`. It produces one fixed-length `<SYM>/1D/SPLITS` series per symbol with columns `Epoch, PriceFactor, VolumeFactor, SplitFrom, SplitTo, Subtype`, sorted by epoch, with same-day duplicates merged by multiplying the factors. Verify with unit tests, including the "Two events on the same date" scenario.
- [ ] 1.3 Add `Load(catalogDir, symbol, kinds...)`. It returns events sorted by epoch, skips rows where both factors are 1.0, and returns empty with no error when the bucket is missing. Verify with a write-then-load round-trip test under `America/New_York` that checks the stored `Epoch` is exactly 00:00 ET of the effective date. Also verify the overwrite-on-rewrite and the re-sync no-op scenarios.
- [ ] 1.4 Add `Neutralize(stored, fresh)`. Verify the "Provider deletes a split" scenario end to end: write, neutralize, then `Load` no longer returns the event.
- [ ] 1.5 Commit: `feat(corpaction): add provider-neutral corporate event store`.

## 2. Adjust on read (`uda/adjust`)

- [ ] 2.1 Write failing tests first, one per scenario in `specs/corporate-actions/adjust-on-read`: float32 OHLCV, trade size multiplied, reverse-split rounding, unknown columns, execution-date boundary, compounding, announced future split, no events, ICE bucket ignored, dividend-only. Confirm they fail on the current code.
- [ ] 2.2 Rewrite `Accum` to use `corpaction.Load`, the factor and role math (D4) and an injected clock. Remove `CARows`, `Actions`, `RateChange*` and the `contrib/ice/enum` import. Verify that the 2.1 tests pass and that `go vet ./uda/... ./sqlparser/...` is clean.
- [ ] 2.3 Replace the global cache with the locked 30s TTL cache (D5). Verify with a test that runs concurrent `Accum` calls under `go test -race ./uda/adjust/...`, and a test where an event written after a load becomes visible once the TTL expires (spec: New events become visible promptly).
- [ ] 2.4 Delete or rewrite `uda/adjust/caloader_test.go` and any ICE-shaped fixtures in `adjust_test.go`. Verify that `go test ./uda/... ./sqlparser/... ./frontend/...` matches the baseline, plus the new tests.
- [ ] 2.5 Commit: `fix(adjust): read corporate events from corpaction and adjust by column role`.

## 3. Massive: raw bars only

- [ ] 3.1 Remove the `adjusted` parameter from `api.GetHistoricAggregates`, `GetAggregatesWindow` and `getAggregates`, and always send `adjusted=false`. Make `paginateAggregates` fail when a page has `adjusted: true`. Verify with `httptest` tests for the query parameter and for the rejection.
- [ ] 3.2 Remove `adjusted` from every caller in `backfill/rest`, `massive.go` and `gapfill.go`, and from the REST command (drop the `-adjusted` flag). Verify that `go build ./contrib/massive/...` passes and that `grep -rn adjusted contrib/massive --include=*.go` shows only the API guard and the config validation.
- [ ] 3.3 In `ValidateConfig`, make `backfill_adjusted: true` an error and `false` a warning. Verify with tests for all three spec scenarios (true, false, missing).
- [ ] 3.4 Update `contrib/massive/README.md` and `mkts.example.yml`: remove the key and flag, and add an "Adjusted prices" section that points to `adjust('split')`. Verify by reading it back. The docs mention no adjusted backfill.
- [ ] 3.5 Commit: `feat(massive)!: always store unadjusted bars`.

## 4. Massive: split ingestion

- [ ] 4.1 Add `api.ListSplits` (all pages, `limit=5000`, ascending `execution_date`) and the `Split` response type. Verify with an `httptest` test for paging through `next_url`, and that 403 returns `ErrAuthFailed`.
- [ ] 4.2 Add a pure `splitsync.Plan(results, tracked, wildcard, stored)` that returns the events to write and the skipped results. It maps fields, skips malformed results with a reason, and adds neutralized rows. Verify with table tests for the mapping, malformed, untracked, wildcard and withdrawn scenarios.
- [ ] 4.3 Add `splits_sync` (default true) and `splits_sync_time` (default `03:30` ET) to `FetcherConfig` and validate them. Verify with config tests for an invalid time.
- [ ] 4.4 Wire the sync loop into `MassiveFetcher.Run`: startup sync plus daily sync on its own goroutine (D6), with the tracked set from config plus catalog symbols, writes through `executor.WriteCSM` in batches of 500, the count log line, and the last-success metric. `--no-backfill` does not turn it off. Verify with a test using a fake clock and an `httptest` server: a startup run happens; a 500 error is retried at the next schedule while streaming is not blocked; a 403 stops the run.
- [ ] 4.5 Commit: `feat(massive): sync split events into the corporate event store`.

## 5. Massive: split repair command

- [ ] 5.1 Add a pure `repair.PlanRanges(buckets, firstBars, splits, today)` that returns the affected `(bucket, from, to)` ranges, ignoring TRADE and QUOTE buckets. Verify with table tests for "split inside stored range", "split before stored range", the future split and multiple splits (the latest one wins).
- [ ] 5.2 Add `contrib/massive/backfill/repair/cmd` with these flags: gRPC address, API key, symbols filter, parallelism, `--dry-run`. It finds buckets and first bars over gRPC, reads splits through `api.ListSplits`, writes through `RPCWriter` from finest to coarsest timeframe, reports totals and exits non-zero if any bucket failed. Verify with a test against an in-process gRPC server and an `httptest` Massive: dry run writes nothing; a real run writes raw bars; a second run changes nothing; one failing symbol does not stop the others.
- [ ] 5.3 Document the repair procedure in `contrib/massive/README.md`: when to run it, dry run first, expected output. Verify by reading it back.
- [ ] 5.4 Commit: `feat(massive): add split repair command for adjusted history`.

## 6. End-to-end verification

- [ ] 6.1 Run a local server with `mkts.example.yml`, using a test API key or an `httptest` Massive stub. Check that `NVDA/1D/SPLITS` exists after startup, that an unadjusted 1D query for 2024-06-07 returns the raw close and that `adjust('split')` returns it divided by 10. Record the commands and outputs in `notes.md`.
- [ ] 6.2 Re-run the 0.1 test set and report the new failures and the pre-existing failures as separate counts. Run `go vet` on the touched packages. Run `openspec validate split-events-adjust-on-read --strict`.
