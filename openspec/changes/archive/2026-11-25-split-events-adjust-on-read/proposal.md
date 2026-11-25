## Why

The massive plugin writes split-adjusted REST bars (`backfill_adjusted` defaults to `true`) into the same buckets as raw WebSocket bars and raw flat-file bars. After a split, one bucket holds prices on two different share bases. Bars that were stored adjusted also go stale at the next split. MarketStore has an `adjust()` query function, but it reads split events only from the ICE `<SYM>/1D/ACTIONS` bucket. Nothing in our pipeline writes that bucket. The function also skips the `float32` OHLCV price columns, so even with events it would not adjust our bar prices.

## What Changes

- **New generalized corporate-event store.** Each event kind has its own fixed-length bucket per symbol: `<SYM>/1D/<KIND>`. This change adds only `SPLITS`. Every kind uses the same columns: effective time, a price factor, a volume factor, and kind-specific detail. A later kind, such as cash dividends, needs no schema migration and no change to the adjust logic. A core Go package owns the schema, the writes (idempotent by effective date) and the reads.
- **Adjust on read.** `adjust()` reads events from the new store instead of the ICE bucket:
  - Price columns are divided by the price factor and size columns are multiplied by the volume factor. Columns are identified by name, for every numeric type (`float32`, `float64`, `int64`, `uint64`).
  - Only events already in effect at query time apply. A split announced for a future date does not change current data.
  - Columns that are neither prices nor sizes pass through unchanged.
- **Massive ingests splits.** The plugin calls `GET /stocks/v1/splits` for the tracked symbols, on startup and once a day. It writes forward splits, reverse splits and stock dividends to the event store. If Massive drops an event it had published before, the stored copy is neutralized.
- **BREAKING: Massive always stores raw bars.** REST backfill, reconnect gap-fill and the standalone REST backfill command always request `adjusted=false`. The `backfill_adjusted` config key is removed: `false` is accepted with a deprecation warning, and `true` fails startup with a clear message. The `-adjusted` flag is removed from the REST backfill command.
- **Operator re-backfill tool.** A new command finds the symbols whose stored bars may be adjusted, meaning a split took effect inside the stored range. It re-fetches raw bars only for those symbols and ranges, for every configured bar timeframe. It supports `--dry-run`. Nothing runs on its own at startup.
- **BREAKING (accepted): ICE is no longer an input to `adjust()`.** `contrib/ice` is not changed, but its `<SYM>/1D/ACTIONS` bucket is no longer read by any query function.

Out of scope: cash dividends (the store is designed to add them later), symbol changes, and adjusting values that clients have already cached.

## Capabilities

### New Capabilities
- `corporate-actions/event-store`: the bucket layout, the common columns, idempotent writes, neutralizing withdrawn events, and reads by symbol and kind.
- `corporate-actions/adjust-on-read`: how `adjust()` applies stored events to query results: column roles, factor direction, the effective-time rule, and the argument handling.
- `massive/split-ingestion`: how the massive plugin fetches split events from Massive and keeps the event store in sync with them.
- `massive/raw-bars`: the massive plugin and its standalone tools only write unadjusted data, plus the config rules for the removed `backfill_adjusted` key.
- `massive/split-repair`: the operator-run tool that re-fetches raw bars for symbols affected by stored splits.

### Modified Capabilities
<!-- None: no specs exist yet in openspec/specs/. -->

## Impact

- **Code:**
  - new core package for the event store (proposed: `corpaction/`)
  - `uda/adjust` (rewritten to use the event store; drops the `contrib/ice/enum` import)
  - `sqlparser/registry.go` (wiring only, if the constructor changes)
  - `contrib/massive`: `api` (new splits client), `massive.go` and `gapfill.go` (raw requests, split sync scheduling), `massiveconfig` (key removal), `backfill/rest` and `backfill/rest/cmd` (raw only), a new repair command, `README.md`, `mkts.example.yml`
- **Config:** `backfill_adjusted: true` in an existing `mkts.yml` now fails startup. Operators must delete the key or set it to `false`.
- **Data:** new `<SYM>/1D/SPLITS` buckets. They replicate to replicas through the normal WAL path. Existing adjusted bars stay as they are until an operator runs the repair tool.
- **Clients:** clients that need adjusted prices must call `adjust('split')`. With the old default, bars already came back adjusted.
- **ICE users:** `adjust()` no longer sees ICE events.
