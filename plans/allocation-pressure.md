# Root Cause Analysis: Allocation Pressure in Watchlist Ranking

## Summary

The process has done 90 TB of lifetime allocations and 232 billion mallocs over 12 days, driving 437K GC cycles (~25/min). The heap profile identified `Momentum.Rank` as a top allocator, but the true dominant source is the trigger `Fire` path during market hours.

This document analyzes both. Findings have been cross-referenced against the source after initial drafting; corrections and additions from that pass are marked inline.

## Methodology Note

The heap profile that flagged `Momentum.Rank` is assumed to be a `-inuse_space` or `-inuse_objects` view (live heap). The `-alloc_space` / `-alloc_objects` views would tell a different story and would be expected to show the `Fire` path dominating. **Before acting on these recommendations, capture both views to confirm which problem you are actually optimizing for** (heap RSS / pause time vs. GC cycle frequency / total allocation throughput).

Recommended baseline capture:

```
go tool pprof -alloc_objects http://<host>:<pprof-port>/debug/pprof/heap
go tool pprof -inuse_objects http://<host>:<pprof-port>/debug/pprof/heap
```

## 1. Invocation Frequency

The config field `ranking_interval_ms` is parsed at `contrib/watchlist/framework/config.go:80-82`. The default when zero/missing is **1000ms (1 second)**. The user's deployment is believed to set `60000ms` in `mkts.yml` but this should be **verified at runtime** by logging the loaded config on startup — if the override fails to apply, rankings fire 60x more frequently than this analysis assumes, which would close most of the per-cycle vs lifetime allocation gap on its own.

Assuming the configured 60s interval, over 12 days: `12 * 24 * 3600 / 60 = 17,280 ranking cycles`.

## 2. Symbol Universe Scale

The symbols come from `baseline.go:26` via `DiscoverSymbols(catalogDir)`, which walks the entire MarketStore catalog. The `symbols_query` at `mkts.yml:168` is `SELECT id, ticker, listed FROM asset WHERE is_active = TRUE AND delisted IS NULL` — this is the full US equity universe.

For a typical US equity feed this is **~10,000-14,000 symbols**. The curated subset (those passing `min_price: 0.10` and `min_dollar_vol_rate: 1000.0`) is likely **2,000-8,000 symbols** depending on market hours, with `N_curated` used below.

## 3. Number of Strategies

From `mkts.yml` and the watchlists directory, there are **6 registered strategies**: Momentum, RelativeVolume, Gap, SMACrossover, SectorAggregate, IndustryAggregate.

Each `RunRankings` call at `rankings.go:10-11` iterates all strategies, calling `.Rank()` on each.

## 4. Allocation Analysis — Full Call Chain

### 4a. `CuratedStates()` — `state.go:93-106` — called once per ranking cycle

```go
cp := make(map[string]*SymbolState, len(m.curated))
for sym := range m.curated {
    if s, ok := m.states[sym]; ok {
        cp[sym] = s
    }
}
```

**Allocations per call:**
- 1 map header allocation (pre-sized to `N_curated`)
- `N_curated` map bucket inserts (each inserts a string key + pointer value)

**Complexity:** O(N_curated). With 5,000 curated symbols, this creates a fresh map with 5,000 entries every cycle. The map itself is ~300-500 KB depending on load factor.

**Note:** Map iteration returns existing string headers (16-byte header — pointer + length); the keys themselves are not copied/reallocated. The cost is the map structure plus bucket inserts, not the strings.

**Avoidability:** Could return an iterator or a read-locked view instead of a copy. Alternatively, reuse a pre-allocated map and clear it between cycles.

### 4b. `RunRankings()` — `rankings.go:6-20` — called once per ranking cycle

```go
results := make(map[string][]RankedSymbol, len(mgr.strategies))
```

**Allocations:** 1 small map (6 entries). Negligible.

### 4c. `Momentum.Rank()` — `momentum.go:47-97` — TOP HEAP-RESIDENT ALLOCATOR

This is the function at the top of the heap profile. Line-by-line:

**Line 56: `var entries []entry`**
- Starts as nil. Grows via `append` in the loop. With 5,000 curated symbols, Go's growth strategy (2x up to 256, then ~1.25x) results in ~14 reallocations.
- The `entry` struct is 56 bytes (string=16, 3x float64=24, int64=8, padding=8).
- Final backing array ~= 280 KB.

**Lines 65-71: `entries = append(entries, entry{...})`**
- O(N_curated) iterations.

**Lines 75-77: `sort.Slice(entries, ...)`**
- `sort.Slice` allocates a closure. The sort itself is in-place. Minor allocation.

**Lines 84-96: `result := make([]framework.RankedSymbol, limit)`**
- Allocates a slice of 50 `RankedSymbol` structs.

**Lines 89-94: `Fields: map[string]interface{}{...}` — 4 entries per symbol**
- **Worst part.** Each `RankedSymbol` allocates a `map[string]interface{}` with 4 key-value pairs. **50 map allocations per `Rank()` call**, each with 4 entries.
- Each `interface{}` value boxes a `float64` or `int64`, causing **200 interface boxing allocations** (4 fields x 50 symbols).
- Map overhead: ~400 bytes per map x 50 = ~20 KB just in map headers.

**Total per `Momentum.Rank()` call:** ~265 allocations, ~300+ KB.

### 4d. `RelativeVolume.Rank()` — `relative_volume.go:44-85`

Identical pattern to Momentum. **~403 allocations per call** (limit=100, 3 fields).

### 4e. `Gap.Rank()` — `gap.go:61-119`

Same pattern. **~163 allocations per call** (limit=50, 2 fields).

### 4f. `SMACrossover.Rank()` — `sma_crossover.go:120-160`

Notable issues:
- **Line 203 + 227:** `state.Extra["sma_"+itoa(s.smaLength)] = smaCurrent` — string concatenation allocates on every call for every curated symbol. The `itoa()` function at lines 464-474 also allocates via repeated string concatenation (`digits = string(rune(...)) + digits`). The string `"sma_20"` is fully static for the lifetime of a strategy instance and should be precomputed once.
- **Lines 145-158:** Same `map[string]interface{}` pattern: 50 maps, 200 interface boxes.
- `closesPool` (sync.Pool) at line 183-193 is well-optimized for the temporary slice.

### 4g. `SectorAggregate.Rank()` — `sector_aggregate.go:52-121`

- ~11 sectors → small N → **~90 allocations per call**.

### 4h. `IndustryAggregate.Rank()` — `industry_aggregate.go:54-126`

- ~150 industries → **~1,350 allocations per call**.

### 4i. `SetWatchlistRanking()` — `state.go:142-146`

Stores the `[]RankedSymbol` slice directly. The previous slice (with all its maps and interface-boxed values) becomes unreachable. Every ranking cycle turns the previous cycle's entire output into garbage.

### 4j. `PushWatchlistUpdate()` — `push.go:56-80`

Called once per strategy per ranking cycle (6 times):

- `symbolMaps := make([]map[string]interface{}, len(symbols))` (line 57): **another complete copy** of the ranking data as `[]map[string]interface{}` — duplicates everything `Rank()` already allocated.
- `tbk := io.NewTimeBucketKey("WATCHLISTS/" + timeframe + "/" + watchlistName)` (line 78): string concat + `fmt.Sprintf` inside `NewTimeBucketKey` + struct alloc = 3 allocations per call. **The TBK string is static** (e.g., `"WATCHLISTS/1Min/MOMENTUM"`) — should be precomputed at strategy registration.

**Per-strategy push:** ~50-150 maps + interface boxes + TBK = ~500-1500 allocations.

### 4k. `PushCurationChange()` — `push.go:41-53`

Cold path (only fires when curation membership changes).

### 4l. `DetectCurationChanges()` — `rankings.go:25-38`

Zero-alloc when no changes. O(N_total) iteration under RLock.

## 5. Aggregate Per-Cycle Cost

Per ranking cycle with ~5,000 curated symbols and 6 strategies:

| Component | Allocations | Bytes |
|---|---|---|
| `CuratedStates()` map copy | ~1 + N buckets | ~300-500 KB |
| Momentum.Rank | ~265 | ~300 KB |
| RelativeVolume.Rank | ~403 | ~400 KB |
| Gap.Rank | ~163 | ~200 KB |
| SMACrossover.Rank | ~263 + N string concats | ~350 KB |
| SectorAggregate.Rank | ~90 | ~50 KB |
| IndustryAggregate.Rank | ~1,350 | ~300 KB |
| 6x PushWatchlistUpdate | ~3,000-6,000 | ~500 KB-1 MB |
| **Total per cycle** | **~5,500-8,500** | **~2-3 MB** |

At 1 cycle/min over 12 days: `17,280 * ~7,000 = 121 million mallocs` and `~43 GB`. At 1 cycle/sec: **60x both numbers** (~7.3 billion mallocs, ~2.6 TB) — within striking distance of the observed totals on its own.

## 6. The Real Multiplier: The Trigger Path (`Fire`)

`WatchlistTrigger.Fire` at `contrib/watchlist/framework/trigger.go:40-133` is called by the executor's trigger dispatcher (`executor/written.go:69-77`) **on every record write**. With ~10,000 symbols and 1-second bars during market hours, this is conservatively **5,000-30,000 calls/sec**, depending on tick liquidity distribution. (This is an estimate range; the actual rate should be measured.)

Per `Fire` call (cross-referenced against source — significantly heavier than originally estimated):

| Source line | Allocation |
|---|---|
| `trigger.go:55` | `make([]int64, len(records))` |
| `trigger.go:138` (via `parseKeyPath`) | `strings.Split` — slice + N substrings |
| `trigger.go:62` | `strings.Split` AGAIN — duplicate work, the year is parsed from the same path |
| `trigger.go:64` | `strings.Replace(fileName, ".bin", "", 1)` — new string |
| `trigger.go:71` | 3-way string concat |
| `trigger.go:72` | `io.NewTimeBucketKey` (`fmt.Sprintf` + struct alloc) |
| `trigger.go:73` | `utils.NewTimeframe` |
| `trigger.go:78-87` | `planner.NewQuery` + `AddTargetKey` + `Parse` (multiple allocs) |
| `trigger.go:89` | `executor.NewReader` |
| `trigger.go:95` | **`scanner.Read()` — full disk read, returns ColumnSeriesMap with column slices** |
| `trigger.go:147-159` | `columnSeriesToMap` — outer map + N `reflect.Value.Interface()` boxes + **`strings.ToLower(key)` per column** |
| `trigger.go:129` | map insert (no alloc on existing map) |
| `push.go:26-29` | outer `payload` map |
| `push.go:31` | string concat + `NewTimeBucketKey` |

**Realistic estimate per `Fire` call:** 40-60 allocations, ~5-10 KB, **plus a full disk-query pipeline** (open, mmap, range scan, column allocation). The disk read is almost certainly the dominant cost here — it dwarfs the heap allocations.

**Over 12 days during market hours (rough estimate):**
- ~6.5 trading hours × 23,400 sec/day × ~8 trading days in a 12-day window = ~5.5 billion `Fire` calls (assuming ~10K calls/sec average)
- × ~50 allocs = **~275 billion mallocs**
- × ~7 KB = **~38 TB**

This is in the right order of magnitude for the observed 232B mallocs / 90 TB.

**Why `Momentum.Rank` shows up as the top heap allocator:** The heap profile (assumed `inuse`) captures live allocations. `Momentum.Rank`'s output — the `[]RankedSymbol` with 50 `map[string]interface{}` — survives for a full ranking cycle (stored in `m.watchlists`). The `Fire` path allocations are individually small and short-lived (sub-millisecond), so they rarely appear in a heap snapshot despite dominating total allocation volume.

## 7. Trigger Fan-Out

`executor/written.go:76` calls `trig.Fire(key, records)` once per `(key, trigger)` pair. If multiple triggers are registered (watchlist + on-disk-aggregate + stream), the per-tick allocation cost is **summed across all of them**. The estimates above attribute everything to the watchlist trigger; if other triggers are active, the real total is correspondingly higher. Verify the running config.

## 8. Top Optimization Opportunities

Priorities ordered by combined impact-to-effort and dependence on profile interpretation. Items 1-3 target the `Fire` hot path (which dominates total allocation throughput / GC cycle frequency); items 4-7 target heap-resident allocations (which dominate heap RSS and pause times).

### Priority 1 — Eliminate `strings.ToLower` per column in `columnSeriesToMap`

`trigger.go:156`: called O(columns) per `Fire`, potentially millions of times per minute. Column names are static and known at the column-series level. Either lower-case the keys once when the column series is created, or skip the lowering entirely if downstream consumers can accept the canonical case.

### Priority 2 — Eliminate duplicate `strings.Split` in `Fire`

`trigger.go:43` calls `parseKeyPath` which already splits the path. `trigger.go:62` splits the *same* `keyPath` again to get the filename. Refactor `parseKeyPath` to return all four pieces (symbol, timeframe, attrGroup, fileName) in one pass.

Also avoid `strings.Replace(fileName, ".bin", "", 1)` — use `strings.TrimSuffix` (no allocation if the suffix isn't present, single allocation if it is) or parse the year directly from a byte slice.

### Priority 3 — Cache static TBKs at strategy registration

Each `PushWatchlistUpdate` call (`push.go:78`) constructs `"WATCHLISTS/" + timeframe + "/" + watchlistName` and then calls `io.NewTimeBucketKey`, which uses `fmt.Sprintf`. Both the string and the `*TimeBucketKey` are static per strategy — precompute them once at registration and store on the strategy or in a map keyed by strategy name.

The same applies to the per-tick TBK at `push.go:31` — cache by `(symbol, timeframe, attrGroup)` tuple. This requires bounded cache size (one entry per active symbol/tf/attr combo) but eliminates ~3 allocations from every tick.

### Priority 4 — Replace `Fields map[string]interface{}` with typed struct in `RankedSymbol`

Every strategy allocates a map per ranked symbol; `PushWatchlistUpdate` then copies it into *another* map. This is the biggest source of heap-resident allocations and the reason `Momentum.Rank` dominates the live-heap profile.

```go
type RankedFields struct {
    PctChange      float64
    Volume         int64
    VolumeMultiple float64
    MomentumScore  float64
    GapPct         float64
    // ...
}
```

A struct is stack-allocatable (or at least flat-heap-allocatable as part of the slice) and zero interface-boxing. Downstream serialization (in `PushWatchlistUpdate`) can iterate the struct fields directly with custom JSON marshaling, avoiding the second map copy entirely.

### Priority 5 — Reuse the `entries` slice in each strategy

Every strategy declares `var entries []entry` and grows it from nil. Make it a field on the strategy struct, reset with `entries = entries[:0]` each cycle. Note: strategies are not currently safe for concurrent `Rank()` calls if this change is made — verify the call site in `RunRankings` is serial (it is, per `rankings.go:10-18`).

### Priority 6 — Reuse the `CuratedStates()` snapshot map

`state.go:93-106` allocates a new `map[string]*SymbolState` of size N_curated every cycle. `RunRankings` is the only consumer (serial). Add a reusable `curatedSnapshot` field to `SymbolStateManager`, clear and refill it.

### Priority 7 — Cache `"sma_"+itoa(smaLength)` in `SMACrossover`

`sma_crossover.go:203,227`: replace with a precomputed `s.smaKey` field set at construction time. Also replace the hand-rolled `itoa` (lines 464-474) with `strconv.Itoa` regardless. The hand-rolled version does N string concatenations per call (each allocating).

## 9. Items Out of Scope

- **`AllWatchlistRankings`** (`state.go:167-177`) does a full deep copy of every ranking slice. Probably cold path (used by query handlers); flagged for future review but not a top priority.
- **HTTP/network costs in the massive plugin** are covered separately in `plans/os-thread-accumulation.md`. That plan addresses thread-creation pressure from CGo DNS and blocking syscalls, which is orthogonal to heap allocation pressure.
- **Postgres connections in `industry_aggregate.go` / `sector_aggregate.go`**: these source files are touched by both this plan and the OS thread plan, but at different lines and for different reasons. The thread plan addresses `sql.Open` DNS-driven thread creation; this plan addresses `Rank()` heap allocations. Fixes are independent.
- **GC tuning (`GOGC`, `GOMEMLIMIT`)** is a complementary mitigation. If the live set is small but allocation throughput is high, raising `GOGC` from 100 to 200-400 halves GC frequency at modest RSS cost. This is a zero-code-change first-line lever to reach for **before** code changes if pause time is tolerable.

## 10. Impact Summary

| Root Cause | Path | Frequency | Per-call allocs | 12-day estimated impact |
|---|---|---|---|---|
| `scanner.Read` disk pipeline | `Fire()` | every tick | many | dominant — measure separately |
| `columnSeriesToMap` reflect + ToLower | `Fire()` | every tick | ~10-15 | ~50-80B mallocs |
| Duplicate `strings.Split` | `Fire()` | every tick | ~4 | ~20B mallocs |
| Per-tick `NewTimeBucketKey` | `Fire()` + `PushTick` | every tick | ~3-5 | ~25B mallocs |
| `map[string]interface{}` Fields | `Rank()` + `PushWatchlistUpdate` | every cycle | ~5,000-8,000 | ~120M mallocs, ~43 GB (heap-resident) |
| Append-growing `[]entry` slices | All `Rank()` methods | every cycle | ~14 grows × 6 | ~1.5M mallocs |
| `CuratedStates()` map copy | `RunRankings()` | every cycle | 1 large map | ~17K mallocs |

The fixes with the highest impact-to-effort ratio:

1. **Priority 1-3 (Fire path):** Reduces total malloc count and GC cycle frequency. Most of the 437K GC cycles trace back here.
2. **Priority 4 (Fields struct):** Reduces heap RSS and shrinks `Momentum.Rank`'s footprint from the live-heap profile.

If the goal is fewer GC cycles, prioritize 1-3. If the goal is smaller heap or shorter pause times, prioritize 4. Both should be done eventually.
