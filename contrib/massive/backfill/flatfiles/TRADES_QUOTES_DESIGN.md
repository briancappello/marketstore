# Design: Backfilling Trades & Quotes from Massive Flat Files

Status: Approved for implementation
Scope: Add tick-level **trades** and **quotes** backfill from Massive S3 flat files,
reusing the existing OHLCV flat-file backfiller infrastructure.

---

## 1. Goal & Workload

Add the ability to backfill nanosecond tick-level **trades** (`/stocks/trades`) and
**quotes** (`/stocks/quotes`) from Massive's daily S3 flat files into MarketStore.

**Target workload:** _a few symbols across many days_ (e.g. one month of AAPL+MSFT
trades). Explicitly **not** bulk full-universe history (the data volume is
impractical: ~155M trade rows and ~497M quote rows per day). Single-day E2E
validation is a subset of this.

This differs fundamentally from OHLCV bulk backfill: the bottleneck is
**downloading + decompressing many multi-GB day-files to extract a small symbol
subset**, not write throughput. Concurrency is therefore **across days**, with
hard symbol filtering and small per-symbol writes.

---

## 2. Source Data (verified against 2026-06-18 files)

S3 layout (same scheme as bars; `massiveconfig.DefaultS3Prefix = "us_stocks_sip"`):

```
us_stocks_sip/trades_v1/YYYY/MM/YYYY-MM-DD.csv.gz
us_stocks_sip/quotes_v1/YYYY/MM/YYYY-MM-DD.csv.gz
```

Files are **gzipped CSV, grouped by ticker (alphabetical), and within each ticker
sorted ascending by `sip_timestamp`** (verified: 100% of tickers in both files).

### Trades columns
`ticker, conditions, correction, exchange, id, participant_timestamp, price,
sequence_number, sip_timestamp, size, tape, trf_id, trf_timestamp`

- `conditions`: quoted, comma-separated **integers** (e.g. `"14,12,37,41"`). Max 4
  observed; up to 4 stored. Empty ~20% of rows.
- `size`: float (fractional shares occur, e.g. `0.9`, `0.014`).

### Quotes columns
`ticker, ask_exchange, ask_price, ask_size, bid_exchange, bid_price, bid_size,
conditions, indicators, participant_timestamp, sequence_number, sip_timestamp,
tape, trf_timestamp`

- `conditions`: up to **2** integers (different namespace from trade conditions).
- `indicators`: integer list, present ~88% of rows.
- `bid_size`/`ask_size`: **round lots before 2025-11-03, shares on/after** (SEC MDI).

---

## 3. Design Decisions (locked)

### 3.1 Timestamps & ordering — no schema change
- Timeframe `1Sec`; bucket keys `{sym}/1Sec/TRADE`, `{sym}/1Sec/QUOTE` (existing).
- Map `sip_timestamp` (int64 ns) → `time.Unix(0, ns)` → `Epoch=Unix()`, `Nanos=Nanosecond()`.
- **Nanosecond precision round-trips losslessly at the `1Sec` timeframe.** How it
  works on disk:
  - Write (`utils/io/timeindex.go:69` `GetIntervalTicks32Bit`): the intra-interval
    offset is packed into a 4-byte `uint32` tick count. At `1Sec`
    (`intervalsPerDay = 86400`), `ticksPerSecond = 86400 × (2^32/86400) = 2^32`, so
    one second spans the full `uint32` range. The write computes
    `uint32(2^32 × secondsSinceYearBase)`; although the float64 product exceeds
    2^53 mid-year, the lost precision is in the **high (whole-second) bits**, which
    the `uint32` truncation discards anyway.
  - Read (`executor/rewritebuffer.go:67` `GetTimeFromTicks`): the absolute second
    comes from the record's `intervalStart` (the bucket's exact epoch-second), and
    only the **sub-second** part is reconstructed from the tick:
    `nanos = round(1e9 × frac(ticks / 2^32))`. Because `frac(...) ∈ [0,1)`, the
    float64 math has full mantissa here — no large-magnitude cancellation.
  - The `subnanosecond = 1e8` rounding at line 83 rounds the **seconds** component
    (legacy C-compat), not the nanos; it does not coarsen the nanosecond output.
- **Verified empirically** (throwaway harness replicating both functions): 5,000,000
  random nanosecond timestamps spread across a full year round-tripped with **0 ns
  error**; a 10 ns-step sweep across seconds at year-start/mid/end also showed 0 ns
  error and no collisions. So storing `sip_timestamp` at `1Sec` is exact for
  nanosecond inputs; **no schema change and no wider timestamp column is needed.**
- **Preserve upstream file order** (already sip-sorted per ticker). No sorting.
- `sequence_number` is **ignored** (not stored, not used for ordering).
- **No ordering validation is performed.** Since we neither sort nor use
  `sequence_number`, we fully trust the file's per-ticker `sip_timestamp` ordering
  (verified 100% sorted on the 2026-06-18 sample, §2). A single out-of-order row in
  a future file would be written out of order on disk. Acceptable for the stated
  few-symbol/many-day workload; flagged here as a known assumption. (If this ever
  matters, add a cheap per-symbol monotonicity check in the parser that warns/sorts.)

### 3.2 Conditions
**Trades:** translate each Massive integer modifier → SIP ASCII char via the
glossary mapping table (e.g. `0→'@'`, `1→'A'`, `2→'W'`, `8→'6'`, `9→'X'`,
`10→'4'`, `11→'D'`, `12→'T'`, `14→'F'`, `37→'I'`, …). Store as `enum.TradeCondition`
in `Cond1..Cond4`. **Drop codes with no SIP mapping** (e.g. `41` Trade Thru Exempt);
they are not in `models.ConditionToUpdateInfo` and are functionally inert for bar
consolidation. This is required for `Bar.FromTrades`/`conditionToUpdateInfo` to work.

**Quotes:** store **raw Massive integers** (the quote-condition namespace has no SIP
mapping table). Requires schema extension (see 3.4).

### 3.3 Exchange & tape encoding (fix for both trades & quotes)
The CSV provides integer exchange ids and integer tape (1/2/3). The model's
`enum.Exchange`/`enum.Tape` are **SIP ASCII chars**. The current REST path stores
the raw int (a latent bug). Fix:
- `exchange` int → SIP char via `GET /v3/reference/exchanges` `participant_id`
  field. **Fetch at startup, cache for the run, static fallback table** if the call
  fails. Applies to trade `exchange` and quote `bid_exchange`/`ask_exchange`.
- `tape` int → char: static `1→'A'`, `2→'B'`, `3→'C'`.

### 3.4 Schema changes
**`models.Trade`:**
- `Size` changes type `[]enum.Size` (uint64) → `[]float64` (fractional shares).
- Add `Correction []byte` column (busted/cancelled/errored trade indicator).

**`models.Quote`:** (NOTE — the current Quote model differs structurally from
Trade; do not assume parity. See `models/quote.go:18-33`.)
- The model today has a **single `Cond []byte`** column (on-disk column name
  `"Cond"`, `quote.go:120`). There are **no numbered** quote conditions; `Cond1`
  does not exist.
- **Decision: keep the existing `Cond` column name** for the first/primary
  condition and **add a new `Cond2 []byte`** alongside it (do NOT rename `Cond`→
  `Cond1`). Rationale: renaming would be a gratuitous breaking column change to the
  `QUOTE` schema and to any live/REST writer that emits `Cond`, for no benefit. The
  asymmetry with Trade's `Cond1..Cond4` is acceptable and documented here.
  - Resulting condition columns: `Cond` (1st), `Cond2` (2nd). Max 2 observed.
- **Add `Indicators []byte`** (store the first indicator; the indicators namespace
  is separate — see §3.2). Column name `"Indicators"`.
- Quote condition/indicator values stored as **raw Massive ints** cast to byte
  (all observed values ≤ 92, fit in a byte).

**Buffer-allocation rework (REQUIRED for streaming — not just new params):**
- Trade uses **append-style** buffers (`make([]T, 0, cap)`, `append` in `Add`),
  which streams naturally. **Quote uses index-based pre-allocation**
  (`NewQuote(symbol, length)` → `make([]T, length)`, `Add` writes at `idx++`,
  `BuildCsm` slices `[:limit]`; `quote.go:41-99`). This requires the row count up
  front and is a **poor fit for the per-symbol streaming parser (§4.3)**, which
  does not know a symbol's row count until the ticker boundary.
- **Decision: convert `models.Quote` to append-style buffers** to match Trade and
  enable streaming: `NewQuote(symbol, capacity)` → `make([]T, 0, capacity)`; `Add`
  uses `append`; drop the `idx`/`limit`/`SetLimit` machinery (or keep `SetLimit` as
  a no-op shim if any caller depends on it — audit callers first). `BuildCsm` then
  emits the full slices (no `[:limit]`).
- **Caller audit required:** any existing code using `NewQuote(sym, N)` +
  index/`SetLimit` semantics (e.g. REST quotes path, tests) must be updated to the
  append API. List call sites before changing the signature.

**Unchanged:** `Bar.Volume` and quote `BidSize`/`AskSize` remain `uint64`.
`enum.Size` stays `uint64`.

**Schema-contract changes required (this is NOT just a Go struct change):**
The `Size` type change and new columns touch two pre-existing *contracts* even
though there is no on-disk tick data:

1. **`mkts.yml attrgroup_types` (config-enforced schema).** The deployment config
   pins the tick schemas:
   ```yaml
   TRADE:  { Size: uint64, ... }              # mkts.yml:40
   QUOTE:  { BidSize: uint64, AskSize: uint64 } # mkts.yml:54-55
   ```
   Per the documented behavior (`mkts.yml:14-17`), a feeder column with an
   incompatible type **raises an error**. The flat-file CLI loads this config
   (`utils.InstanceConfig = *config`). So the config MUST be updated in lockstep:
   - `TRADE.Size: uint64 → float64`; add `Correction: byte`.
   - `QUOTE`: add `Cond` (byte, if not already), `Cond2: byte`, `Indicators: byte`.
   - Update any other shipped configs (`contrib/massive/mkts.example.yml`,
     deployment manifests).

2. **On-disk reality (verified on this machine):** `data/` contains **only OHLCV
   buckets** — **0 TRADE and 0 QUOTE buckets** (83,592 OHLCV `.bin` files, none
   tick). `query_start.trades`/`quotes` are commented out (`mkts.yml:205-206`), and
   the REST tick path has never been run here. So no local migration is needed.

**Assumption + remedy for OTHER environments:** if the REST backfiller or live ws
handlers have **ever** written TRADE/QUOTE buckets in any deployment, those buckets
are the **old `uint64` schema** and are incompatible with the new FLOAT64 writer
(the on-disk header type-check will reject a mismatched write). Since that data was
produced by the buggy paths and has no faithful value, the remedy is **delete and
recreate** those buckets — there is no in-place migration. The implementer must
confirm target environments are clean (or wiped) before first tick backfill.

**Other existing producers found (audit — implementer awareness):**
- **Live ws handlers also write ticks** (`contrib/massive/handlers/handlers.go`,
  `handlers/writer.go`), not just REST:
  - Trade size truncated: `uint64(t.Size)` (`handlers.go:94`) — same fractional-loss
    bug as REST; should adopt float `Size` for consistency (out of strict scope, but
    will write an incompatible `uint64` column against the updated FLOAT64 config —
    **must be fixed or disabled** when the schema flips, or it will error on write).
  - **Quote timeframe mismatch:** the live handler writes quotes to
    **`{sym}/1Min/QUOTE`** (`handlers.go:116`), whereas this design and the model's
    `QuoteBucketKey` use **`{sym}/1Sec/QUOTE`** (`models/quote.go:14`). These are
    different bucket keys (won't collide), but it's a pre-existing inconsistency the
    implementer should be aware of; align the handler to `1Sec` if a single quote
    store is desired.
- **`mkts.example.yml` has no `attrgroup_types`** — only the deployment `mkts.yml`
  pins tick schemas. The config-update task (above) applies to deployment configs;
  optionally add tick schemas to the example for documentation.

### 3.5 Sizes
- **Trade `size`** → stored as `float64` (no scale factor).
- **Quote `bid_size`/`ask_size`** → **normalize to shares**: for rows dated before
  2025-11-03, multiply by the symbol's `round_lot` (fetched from Ticker Overview
  API, **fetch+fallback to 100**). On/after 2025-11-03, use as-is. Stored `uint64`.
- **`Bar.FromTrades` volume aggregation** (`models/bar.go:237`): change the local
  accumulator `var volume enum.Size` (line 247) to `var volume float64`; convert to
  `enum.Size` only at the two `bar.Add` flush sites (lines 263, 338) via:
  ```go
  func finalizeVolume(v float64) enum.Size {
      if v <= 0 { return 0 }
      if iv := enum.Size(v); iv == 0 { return 1 } // positive sub-1 -> 1
      return enum.Size(v) // truncate toward zero
  }
  ```
  Rationale: distinguish "something traded" (sub-1 fractional) from "nothing traded".

  **All `volume` touchpoints in `FromTrades` (audited) and how the refactor handles
  each:**
  - `volume += trades.Size[i]` (line 332, accumulation) → `float64 += float64`. ✓
  - **`volume = trades.Size[i]` (line 297, `MarketCenterOfficialClose` — assignment,
    not `+=`)** → `float64 = float64`. ✓ Handled: it assigns a single trade's float
    size; the round-up still applies because it flushes through the same `bar.Add`
    conversion site. (This is the case explicitly flagged in review — confirmed OK.)
  - `volume != 0` guards (lines 262, 337) → valid for float.
  - `volume = 0` reset (line 272) → valid for float.
  - Only the two `bar.Add(...)` calls need `finalizeVolume(volume)`; that is the
    single conversion point for both the accumulation and assignment paths.

  **⚠ This is a GLOBAL behavior change, not flat-file-local.** `FromTrades` is the
  shared consolidator. Its only production caller is
  `contrib/ondiskagg/aggtrigger/aggtrigger.go:356`, which runs on **every**
  `*/1Sec/TRADE` write (live ws, REST, and flat-file alike — see `mkts.yml:104`).
  So the new volume semantics apply to all trade sources. Two intended deltas:
  1. **Whole-share trades are unaffected:** a bucket of integer-size trades sums to
     the same integer volume as before (the old model truncated fractional at ingest,
     so whole shares behaved identically).
  2. **Pure-fractional buckets now EMIT a bar (volume 1) that the old code
     SUPPRESSED.** Previously fractional sizes were truncated to 0 at ingest, so a
     bucket containing only sub-1-share activity had `volume == 0` and was dropped by
     the `volume != 0` guard. Now it accumulates (e.g. `0.014`), passes the guard, and
     emits a bar with `finalizeVolume → 1`. This is the desired "something traded ≠
     nothing traded" semantic, but it is a real, intended behavioral change to bar
     output for all sources. Document it; cover it in tests (§5).

### 3.6 Memory & orchestration
- **Download: spill-to-temp-file.** Stream the S3 object body to a temp file,
  close the HTTP connection, then stream-decompress + parse from the local file.
  Bounds memory and removes mid-parse connection-reset risk under parallel
  multi-GB day downloads. Delete temp file when done. (Bar path keeps in-memory
  `io.ReadAll` buffering — small files.)
- **Per-symbol flush:** parser emits one symbol's CSM at each ticker boundary; no
  whole-day CSM accumulation. **No mid-symbol partial flush** (few-symbol workload).
- **Concurrency across days:** download/parse workers process distinct dates
  concurrently. Defaults (tick-specific): download parallelism 2–4, write
  concurrency 1–2.
- **Symbol filter `*` (full universe) — gated, non-interactive-safe:**
  - Add a **`--yes` (alias `-y`) / `--force`** flag as the non-interactive escape
    hatch. When set, proceed without prompting.
  - When `*` is given for a tick key and `--yes` is **not** set:
    - If **stdin is not a TTY** (CI/cron/piped): **refuse and exit non-zero** with a
      message instructing the user to pass `--yes`. Do **not** read stdin (never
      block/hang waiting for input that won't come).
    - If stdin **is** a TTY: print the data-volume warning and read an interactive
      `y/N` prompt; abort if declined.
  - TTY detection: reuse the existing idiom (`os.Stdin.Stat()` + `os.ModeCharDevice`,
    mirroring `progress.go:80` `resolveTTY`).
  - Rationale: an interactive-only gate hangs or misbehaves under automation; the
    flag makes intent explicit and keeps human runs safe.
- **Write mode:** same default as OHLCV (gRPC); `--no-rpc` available.
- **Checkpoint:** retain per-date `.flatfile_sync.json`, extended with `trades`/
  `quotes` keys.

### 3.7 Write mode: tick data MUST be variable-length (`isVariableLength = true`)

`executor.WriteCSM(csm, isVariableLength)` selects the on-disk record layout
(`executor/writer.go:278-294`):
- `true` → `io.VARIABLE`: each record carries a 4-byte `IntervalTicks`; the
  `Nanoseconds` column is stripped on write and reconstructed on read via
  `RewriteBuffer`. Multiple records per interval bucket are supported.
- `false` → `io.FIXED`: one record per interval index; data is aligned to the
  interval grid. **No per-record IntervalTicks.**

**Trades and quotes are tick data with many records per `1Sec` bucket, so they MUST
be written with `isVariableLength = true`.** Writing them FIXED is doubly wrong:
(1) multiple ticks in the same second collide on one interval slot, and (2) there is
no IntervalTicks, so the nanosecond round-trip established in §3.1 does not even
apply. This is the established convention: `models.Trade.Write`/`models.Quote.Write`
(`models/trade.go:150`, `models/quote.go:129`) and the live ws handlers
(`contrib/massive/handlers/writer.go:47,80`) all use `true`; bars use `false`.

**Two concrete consequences for implementation:**
1. **REST bug (latent):** `contrib/massive/backfill/rest/rest.go:273,275`
   (`writeModel`) writes trades AND quotes with `false`, contradicting the models'
   own `Write()` and the live handlers. Any trade/quote data written by the REST
   backfiller is in the wrong on-disk layout. Fix `writeModel` to take/forward an
   `isVariableLength` argument (see §4.8).
2. **Bar channel is NOT reusable as-is for ticks:** the flat-file bar write loop
   hardcodes `w.WriteCSM(job.csm, false)` (`backfill.go:216`). `BackfillTicks`
   (§4.5) must write with `true` — either via a separate write loop or by adding an
   `isVariableLength` field to `writeJob`.

---

## 4. File-Level Implementation Plan

### 4.1 New: condition/exchange mapping (`contrib/massive/mapping/`)
Single source of truth, shared by flat-file parser, REST backfiller, and (future)
live feed.

```
contrib/massive/mapping/conditions.go
  // TradeConditionToSIP maps a Massive trade condition modifier (int) to the
  // SIP ASCII char (enum.TradeCondition). ok=false for unmapped codes.
  func TradeConditionToSIP(code int) (enum.TradeCondition, bool)

  // TapeToChar maps tape 1/2/3 -> enum.Tape 'A'/'B'/'C'.
  func TapeToChar(tape int) enum.Tape

contrib/massive/mapping/exchanges.go
  type ExchangeMap struct{ m map[int]enum.Exchange }
  // LoadExchangeMap fetches /v3/reference/exchanges and builds id->participant_id
  // (SIP char) map; falls back to the static table on error.
  func LoadExchangeMap(client *http.Client) *ExchangeMap
  func (e *ExchangeMap) Get(id int) enum.Exchange   // fallback: UndefinedExchange
  var staticExchangeFallback = map[int]enum.Exchange{ ... }
```

**`staticExchangeFallback` is a named deliverable, not a placeholder.** It is the
**actual source of truth whenever the API call fails**, so it must be populated and
maintained, not left as `{ ... }`. Build it by calling `/v3/reference/exchanges`
**once at implementation time** and transcribing the `id → participant_id` pairs
for rows with `type` `exchange` or `TRF` (skip `SIP` rows). Endpoint shape is
**confirmed** (docs, verified): each result has `id` (int), `participant_id`
(string — "The ID used by SIP's to represent this exchange"), `mic`, `type`
(`exchange|TRF|SIP`). The single empty `participant_id` / unmapped id → map to
`enum.UndefinedExchange`. Add a unit test asserting the fallback table is non-empty
and covers the exchange ids observed in the 2026-06-18 sample (4, 8, 11, 12, 19,
21, and others).

Also add to `contrib/massive/api/api.go`:
```
type Exchange struct {
    ID int `json:"id"`; ParticipantID string `json:"participant_id"`
    MIC string `json:"mic"`; Type string `json:"type"`; ... }
func ListExchanges(client *http.Client) ([]Exchange, error)   // GET /v3/reference/exchanges

type TickerOverview struct{ Ticker string; RoundLot int `json:"round_lot"` ... }
func GetTickerRoundLot(client *http.Client, ticker string) (int, error) // Ticker Overview
```

### 4.2 Models (`models/trade.go`, `models/quote.go`, `models/bar.go`)
- `models/trade.go`: `Size []float64`; add `Correction []byte`; update `make`,
  `Add` (signature: `size float64`, add `correction` param), `GetCs` (add
  `"Correction"` column, `Size` now FLOAT64).
- `models/quote.go`: **structural rework, not just new params** (see §3.4):
  1. **Convert to append-style buffers.** `NewQuote(symbol, capacity)` →
     `make([]T, 0, capacity)`; `Add` appends; remove `idx`/`limit` fields and the
     `[:limit]` slicing in `BuildCsm`. Audit/remove or shim `SetLimit`.
  2. **Add columns:** new `Cond2 []byte` (keep existing `Cond`, do **not** rename to
     `Cond1`) and `Indicators []byte`.
  3. `Add` signature gains `cond2 byte` and `indicator byte` params (raw Massive
     ints); existing `cond` param stays. Conditions/indicators are passed as raw
     bytes here (no `enum.QuoteCondition` mapping — none exists).
  4. `BuildCsm` adds `"Cond2"` and `"Indicators"` columns; emits full slices.
  5. **Update all `NewQuote`/`Add`/`SetLimit` call sites** to the new append API.
- `models/bar.go`: `FromTrades` — float volume accumulator + `finalizeVolume`
  (see §3.5 for the full touchpoint audit, incl. the line-297 assignment case).
  The `Trade.Size[i]` reads are now float64. **Note this changes consolidation
  semantics globally** (only caller is ondiskagg, but it fires for all trade sources).
- Update `models/*_test.go` accordingly.

**Call-site audit (completed — these are ALL the call sites):**
- `Trade.Add` / `NewTrade` signature change (float `Size` + new `Correction`) affects:
  - `contrib/massive/backfill/rest/rest.go:483` (REST trades — §4.8).
  - `models/bar_test.go:18,65`.
  - **`contrib/ondiskagg/aggtrigger/aggtrigger.go:309-339` `convertCSToTrades`** —
    **easy to miss.** It reads the on-disk TRADE schema and rebuilds a `Trade`:
    - Line 314 type-asserts `cs.GetColumn("Size").([]uint64)`. With `Size` now
      FLOAT64 on disk, this assertion **fails** (`ok3=false`) and the
      trades→bars aggregation trigger errors out. Must change to `[]float64`.
    - Line 331 `trades.Add(..., modelsenum.Size(sizes[i]), ...)` must pass float
      size and the new `correction` arg (read a `"Correction"` column, or pass 0
      if the bucket predates it — guard the `GetColumn` like the others).
- `Quote.Add` / `NewQuote` append rework affects:
  - `contrib/massive/backfill/rest/rest.go:627` (REST quotes — §4.8).
  - **No other `NewQuote` callers.** `models.QuoteBucketKey` is used at
    `contrib/massive/massive.go:1049` but that's the key helper, not the model.
- **`SetLimit` has zero callers** → delete it outright (no shim needed).
- **No `models/quote_test.go` exists** → add one for the reworked model.

### 4.3 Flat-file tick parsers (`contrib/massive/backfill/flatfiles/`)
```
parser_trades.go
  func ParseTradesStream(r io.Reader, symbolSet map[string]bool, exMap *mapping.ExchangeMap,
      date time.Time, emit func(utilsio.ColumnSeriesMap)) (ParseStats, error)
parser_quotes.go
  func ParseQuotesStream(r io.Reader, symbolSet map[string]bool, exMap *mapping.ExchangeMap,
      roundLot func(sym string) int, date time.Time, emit func(utilsio.ColumnSeriesMap)) (ParseStats, error)
```
- Streaming `csv.Reader` (`ReuseRecord = true`), buffer increased for long quote rows.
- Parse trailing numeric columns positionally (the `conditions` field is quoted and
  contains commas; index by header but treat quoted fields correctly via the std
  csv reader which already handles quotes).
- Per-ticker accumulate `*models.Trade`/`*models.Quote`; on ticker change, `emit`
  the symbol CSM and reset. This **depends on the append-style Quote rework (§3.4,
  §4.2)** — the current index/pre-allocation Quote API cannot be used here because
  the per-symbol row count is unknown until the ticker boundary. Allocate with a
  modest starting capacity (e.g. `BarCapacity`-style heuristic per data type) and
  let `append` grow.
- Map conditions/exchange/tape via `mapping`. Normalize quote sizes to shares using
  `roundLot(sym)` and the 2025-11-03 cutoff.

### 4.4 S3 streaming download (`s3client.go`)
```
// DownloadToTempFile streams the object to a temp file and returns a path +
// cleanup func; caller stream-decompresses from the file.
func (c *S3Client) DownloadToTempFile(ctx, prefix, dataType string, date time.Time)
    (path string, cleanup func(), err error)
```
Existing `DownloadWithPrefix` (in-memory) retained for bars.

### 4.5 Orchestration (`backfill.go`)
Add a tick variant mirroring `BackfillDates` but: spill-to-temp download,
stream-parse with per-symbol `emit` into the write channel, across-days
concurrency, tick defaults. Reuse retry/backoff (`downloadAndParse` pattern),
high-water-mark + `OnProgress` checkpoint, progress bar.
```
func BackfillTicks(ctx, s3Client, w backfill.Writer, symbolSet map[string]bool,
    dataType string /* "trades"|"quotes" */, exMap *mapping.ExchangeMap,
    roundLot func(string) int, dates []time.Time, cfg BackfillConfig) (rows, symbols int64, err error)
```
- **Write with `isVariableLength = true` (§3.7).** Do NOT reuse the bar loop's
  hardcoded `w.WriteCSM(job.csm, false)` (`backfill.go:216`). Either give
  `BackfillTicks` its own writer loop or add an `isVariableLength bool` to
  `writeJob` and set it per data type.

### 4.6 DataTypes & checkpoint
- `backfill.go` `DataTypes`: add
  `"trades": {DefaultS3Prefix, "trades_v1"}`, `"quotes": {DefaultS3Prefix, "quotes_v1"}`.
- **⚠ `DataTypes` is now overloaded with two key kinds:** the existing keys (`1D`,
  `1Min`, `1D-index`) are **timeframes** consumed by the bar path
  `BackfillDates(..., timeframe, ...)`; the new `trades`/`quotes` keys are
  **data-type names** that map to a `1Sec` bucket and MUST route to `BackfillTicks`,
  **not** the bar path. The CLI routing (§4.7) must branch on key kind and never
  pass `trades`/`quotes` as a `timeframe` argument. Consider a helper
  `isTickKey(key) bool` (or a `Kind` field on `FlatFileType`) to make the
  bar-vs-tick dispatch explicit rather than string-matching in two places.
- `checkpoint.go`: no structural change (map keyed by data type); new keys flow through.

### 4.7 CLI (`contrib/massive/backfill/flatfiles/cmd/main.go`)
- Accept `trades`/`quotes` as `-from` keys.
- Route bar keys to `BackfillDates`, tick keys to `BackfillTicks`.
- Add **`--yes`/`-y`** (alias `--force`) flag (default false).
- `*` symbol pattern for a tick key (full-universe gate, §3.6):
  - `--yes` set → proceed.
  - else stdin not a TTY (`os.Stdin.Stat()` + `os.ModeCharDevice`, cf.
    `progress.go:80`) → **print error + exit non-zero** (instruct to pass `--yes`);
    never block on stdin.
  - else (TTY) → print volume warning + interactive `y/N`; abort if declined.
- Build `mapping.ExchangeMap` (one API call) and a `round_lot` resolver (cached
  per symbol) when any tick key is present.
- Tick concurrency defaults distinct from bar defaults.

### 4.8 REST bug fixes (`contrib/massive/backfill/rest/rest.go`)
- Trades: use `mapping.TradeConditionToSIP` + `mapping.TapeToChar` +
  `ExchangeMap.Get` instead of raw `enum.TradeCondition(c)` / `enum.Exchange(int)`.
  Pass float `size` and `correction` to the updated `Trade.Add`.
- Quotes: migrate to the **append-style `NewQuote`/`Add` API** (§4.2); populate the
  new `Cond2` + `Indicators`; map exchanges via `ExchangeMap.Get`; normalize sizes
  to shares.
- **`isVariableLength` fix (§3.7):** `writeModel` (`rest.go:271-276`) currently
  writes with `false` for everything. It must write **`true` for trades/quotes**
  and `false` for bars. Add an `isVariableLength bool` parameter to `writeModel`
  (callers: bars pass `false`, trades/quotes pass `true`), threading through both
  the `writer.WriteCSM` and the `executor.WriteCSM` fallback branches.

### 4.9 Config schema update (`mkts.yml attrgroup_types`) — REQUIRED
The deployment config pins tick schemas and **rejects incompatible feeder column
types** (`mkts.yml:14-17`). Update in lockstep with the model changes (§3.4):
- `TRADE`: `Size: uint64 → float64`; add `Correction: byte`.
- `QUOTE`: add `Cond2: byte` and `Indicators: byte` (keep existing `Cond`; ensure
  `BidSize`/`AskSize` stay `uint64`).
- Apply to all deployment configs that define `attrgroup_types`. (Note:
  `contrib/massive/mkts.example.yml` currently defines none — optionally add for docs.)
- **Pre-flight check:** with no existing tick buckets (verified locally: 0
  TRADE/QUOTE), the first write creates buckets from this config. Confirm the config
  is updated *before* the first tick backfill so buckets are created FLOAT64/with the
  new columns from the start.

### 4.10 Live ws handler alignment (`contrib/massive/handlers/`) — REQUIRED if enabled
The live ws trade/quote handlers write the old schema and will **error against the
updated FLOAT64 config** if enabled:
- `handlers.go:94` `uint64(t.Size)` → write float `Size` (drop truncation).
- `handlers.go:116` writes `1Min/QUOTE`; align to `1Sec/QUOTE` if a unified quote
  store is desired (see §3.4 note). At minimum, document the divergence.
- If the ws tick handlers are not enabled in a deployment (`ws_data_types` excludes
  `trades`/`quotes`, as in the current `mkts.yml`), this is non-blocking but should
  still be fixed to avoid a latent break when later enabled.

---

## 5. Test Plan
- Unit: `mapping` tables (trade cond, tape, exchange fallback), quote-size
  normalization across the 2025-11-03 cutoff, `finalizeVolume` edge cases
  (0, sub-1, ≥1), streaming parsers on small fixture CSVs (quoted conditions,
  empty conditions, fractional sizes, ticker-boundary flush).
- **`FromTrades` semantics (global change, §3.5):** (a) whole-share trades produce
  the same integer volume as before; (b) `MarketCenterOfficialClose` assignment case
  (`1D`, line 297) round-trips the float size and clamps sub-1→1; (c) a bucket of
  only fractional-share trades now emits a bar with volume 1 (previously suppressed).
  Extend `models/bar_test.go` (`TestFromTradesDailyRollup`, `TestFromTradesFieldExcludes`)
  to assert these.
- Model: round-trip `Trade`/`Quote` build → CSM → datashapes (FLOAT64 size, new columns).
- **Write-mode:** assert tick buckets are created as `io.VARIABLE` (not `FIXED`)
  and that multiple ticks within the same second all persist and read back (would
  collide under FIXED) — guards against the `isVariableLength` regression (§3.7).
- **E2E validation (deliverable):** backfill one symbol/day, query the TRADE/QUOTE
  bucket back, assert against source CSV rows — nanosecond timestamps, SIP-char
  conditions/exchange, fractional trade sizes, quote `Cond2`+`Indicators`,
  share-normalized quote sizes.
- **Legacy-schema read after dtype change (ties to §3.4 / finding #4):** create a
  TRADE bucket with the OLD schema (`Size uint64`, no `Correction`) — i.e. emulate
  data the pre-existing REST/ws path produced — then verify behavior after the model
  change. Assert one of: (a) it reads back without panic and the new code paths
  handle the missing `Correction` column / uint64 `Size` gracefully (e.g. ondiskagg
  `convertCSToTrades` guards the column type — §4.2), OR (b) if incompatible by
  design, the failure is a clear typed error (not a silent corruption), matching the
  "delete and recreate" remedy documented in §3.4. This pins down the actual
  behavior rather than assuming all environments are clean.
- `make fmt`, `go vet ./...`, `make build`, `make plugins`.

---

## 6. Out of Scope
- Bulk full-universe trades/quotes backfill (warned + gated, not optimized).
- Storing `participant_timestamp`, `trf_timestamp`, `id`, `trf_id`, quote `tape`,
  `sequence_number`.
- Historical `round_lot` accuracy (Overview API returns current value only).
- Live ws feed integration for trades/quotes (mapping helpers are reusable when added).
```
