package framework

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// ComputeBaselines queries daily OHLCV data from disk for every known symbol
// and populates MedianVolume50D and PriorClose in the state manager.
func ComputeBaselines(mgr *SymbolStateManager, lookbackDays, medianWindow int) {
	catalogDir := executor.ThisInstance.CatalogDir
	if catalogDir == nil {
		log.Warn("[watchlist] CatalogDir is nil, skipping baseline computation")
		return
	}

	// Walk the catalog to discover all symbols.
	symbols := DiscoverSymbols(catalogDir)
	log.Info("[watchlist] computing baselines for %d symbols (lookback=%d days, median=%d)",
		len(symbols), lookbackDays, medianWindow)

	now := time.Now()
	start := now.AddDate(0, 0, -lookbackDays)

	for _, symbol := range symbols {
		state := mgr.GetOrCreate(symbol)
		computeSymbolBaseline(catalogDir, symbol, state, start, now, medianWindow)
	}

	log.Info("[watchlist] baselines computed for %d symbols", len(symbols))
}

// DiscoverSymbols walks the catalog directory tree to find all symbol names
// that have data. It extracts unique symbol names from TimeBucketInfo paths.
func DiscoverSymbols(catalogDir *catalog.Directory) []string {
	if catalogDir == nil {
		return nil
	}

	tbInfos, err := catalogDir.GatherTimeBucketInfo()
	if err != nil {
		log.Error("[watchlist] failed to gather time bucket info: %v", err)
		return nil
	}

	// Deduplicate symbol names from the paths.
	// Path is an absolute path like "/data/root/AAPL/1D/OHLCV/2024.bin".
	// We need to extract the symbol by finding the catalog root and
	// taking the first relative segment. However, the simplest approach
	// is to look at the directory structure:
	// root/SYMBOL/TIMEFRAME/ATTRGROUP/YEAR.bin
	// The symbol is always the 4th-from-last segment.
	seen := make(map[string]struct{})
	var symbols []string
	for _, tbi := range tbInfos {
		parts := strings.Split(tbi.Path, "/")
		// Path ends with SYMBOL/TIMEFRAME/ATTRGROUP/YEAR.bin
		// so symbol is at index len-4.
		if len(parts) < 4 {
			continue
		}
		sym := parts[len(parts)-4]
		if _, ok := seen[sym]; !ok {
			seen[sym] = struct{}{}
			symbols = append(symbols, sym)
		}
	}

	return symbols
}

// computeSymbolBaseline reads daily data for a single symbol and sets
// MedianVolume50D, PriorClose, and seeds the running state.
//
// Seeding priority:
//  1. If today's intraday data exists on disk (from a backfill or prior run),
//     use it to compute accurate running state (open, high, low, cumulative
//     volume, last close) for the current day.
//  2. Otherwise, fall back to the most recent daily bar as an approximation.
//
// This ensures meaningful curation/watchlist results at startup regardless
// of whether the server starts during market hours, after hours, or mid-day
// after a backfill has populated today's intraday data.
func computeSymbolBaseline(
	catalogDir *catalog.Directory,
	symbol string,
	state *SymbolState,
	start, end time.Time,
	medianWindow int,
) {
	tbk := io.NewTimeBucketKey(symbol + "/1D/OHLCV")

	q := planner.NewQuery(catalogDir)
	q.AddTargetKey(tbk)
	q.SetRange(start, end)

	parsed, err := q.Parse()
	if err != nil {
		log.Debug("[watchlist] baseline query parse error for %s: %v", symbol, err)
		return
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		log.Debug("[watchlist] baseline reader error for %s: %v", symbol, err)
		return
	}

	csm, err := scanner.Read()
	if err != nil {
		log.Debug("[watchlist] baseline read error for %s: %v", symbol, err)
		return
	}

	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return
	}

	// Extract volume column for median calculation.
	volCol := cs.GetColumn("Volume")
	if volCol == nil {
		return
	}
	volumes := toFloat64Slice(volCol)
	if len(volumes) == 0 {
		return
	}

	// MedianVolume50D: median of the last N daily volumes.
	n := medianWindow
	if n > len(volumes) {
		n = len(volumes)
	}
	recentVols := volumes[len(volumes)-n:]
	state.MedianVolume50D = median(recentVols)

	// Extract close column for PriorClose.
	closeCol := cs.GetColumn("Close")
	closes := toFloat64Slice(closeCol)
	if len(closes) == 0 {
		return
	}
	state.PriorClose = closes[len(closes)-1]

	// Determine "today" for intraday queries.
	today := time.Now().Truncate(24 * time.Hour)

	// Try to seed from today's intraday data (most accurate).
	// If intraday data exists, SeededDay and LiveDay are both set to today.
	if seedFromIntraday(catalogDir, symbol, state, today) {
		state.SeededDay = today.Unix()
		return
	}

	// Fallback: seed from the most recent daily bar.
	// SeededDay is set to the date of the last daily bar (which is likely
	// yesterday or the last trading day). When the first live tick arrives
	// for a new day, the day-boundary check in updateSymbolState will detect
	// that LiveDay != SeededDay and call ResetDaily() before processing.
	epochs := cs.GetEpoch()
	if len(epochs) > 0 {
		lastBarDay := time.Unix(epochs[len(epochs)-1], 0).Truncate(24 * time.Hour)
		state.SeededDay = lastBarDay.Unix()
	} else {
		state.SeededDay = today.Unix()
	}

	seedFromDailyBar(state, closes, volumes,
		toFloat64Slice(cs.GetColumn("Open")),
		toFloat64Slice(cs.GetColumn("High")),
		toFloat64Slice(cs.GetColumn("Low")),
	)
}

// seedFromIntraday attempts to read today's 1Min OHLCV bars from disk and
// compute accurate running state from them. Returns true if intraday data
// was found and used.
func seedFromIntraday(
	catalogDir *catalog.Directory,
	symbol string,
	state *SymbolState,
	today time.Time,
) bool {
	tbk := io.NewTimeBucketKey(symbol + "/1Min/OHLCV")

	q := planner.NewQuery(catalogDir)
	q.AddTargetKey(tbk)
	q.SetRange(today, time.Now())

	parsed, err := q.Parse()
	if err != nil {
		return false
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		return false
	}

	csm, err := scanner.Read()
	if err != nil {
		return false
	}

	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return false
	}

	// We have today's intraday data. Compute running state from it.
	opens := toFloat64Slice(cs.GetColumn("Open"))
	highs := toFloat64Slice(cs.GetColumn("High"))
	lows := toFloat64Slice(cs.GetColumn("Low"))
	closes := toFloat64Slice(cs.GetColumn("Close"))
	volumes := toFloat64Slice(cs.GetColumn("Volume"))

	if len(closes) == 0 {
		return false
	}

	// DayOpen: the first bar's open.
	if len(opens) > 0 {
		state.DayOpen = opens[0]
	}

	// HighOfDay: max of all bar highs.
	state.HighOfDay = highs[0]
	for _, h := range highs[1:] {
		if h > state.HighOfDay {
			state.HighOfDay = h
		}
	}

	// LowOfDay: min of all bar lows.
	state.LowOfDay = lows[0]
	for _, l := range lows[1:] {
		if l < state.LowOfDay {
			state.LowOfDay = l
		}
	}

	// LastPrice / LastClose: the most recent bar's close.
	state.LastPrice = closes[len(closes)-1]
	state.LastClose = closes[len(closes)-1]

	// CumulativeVolume: sum of all bar volumes.
	var totalVol int64
	for _, v := range volumes {
		totalVol += int64(v)
	}
	state.CumulativeVolume = totalVol

	// TickCount: number of bars we've seen.
	state.TickCount = int64(len(closes))

	// LiveDay: we have real intraday data for today, mark it.
	state.LiveDay = today.Unix()

	// Compute derived metrics.
	if state.PriorClose != 0 {
		state.PctChange = (state.HighOfDay - state.PriorClose) / state.PriorClose * 100
	}
	if state.MedianVolume50D != 0 {
		state.VolumeMultipleOfMed = float64(state.CumulativeVolume) / state.MedianVolume50D
	}
	if state.TickCount > 0 && state.LastPrice > 0 {
		state.DollarVolumeRate = float64(state.CumulativeVolume) * state.LastPrice / float64(state.TickCount*60)
	}

	log.Debug("[watchlist] seeded %s from %d intraday bars", symbol, len(closes))
	return true
}

// seedFromDailyBar seeds running state from the most recent daily bar.
// This is a fallback when no intraday data exists for today (market closed,
// or server starting before backfill has run).
func seedFromDailyBar(state *SymbolState, closes, volumes, opens, highs, lows []float64) {
	lastIdx := len(closes) - 1

	state.LastPrice = closes[lastIdx]
	state.LastClose = closes[lastIdx]

	if len(opens) > lastIdx {
		state.DayOpen = opens[lastIdx]
	}
	if len(highs) > lastIdx {
		state.HighOfDay = highs[lastIdx]
	}
	if len(lows) > lastIdx {
		state.LowOfDay = lows[lastIdx]
	}
	if len(volumes) > lastIdx {
		state.CumulativeVolume = int64(volumes[lastIdx])
	}

	// Compute derived metrics.
	if state.PriorClose != 0 {
		state.PctChange = (state.HighOfDay - state.PriorClose) / state.PriorClose * 100
	}
	if state.MedianVolume50D != 0 {
		state.VolumeMultipleOfMed = float64(state.CumulativeVolume) / state.MedianVolume50D
	}
	// Estimate dollar volume rate from the daily bar.
	// A full trading day is ~6.5 hours = 23400 seconds.
	if state.CumulativeVolume > 0 && state.LastPrice > 0 {
		state.DollarVolumeRate = float64(state.CumulativeVolume) * state.LastPrice / 23400.0
	}
}

// toFloat64Slice converts a column value to []float64, supporting the
// common MarketStore column types.
func toFloat64Slice(col interface{}) []float64 {
	switch v := col.(type) {
	case []float64:
		return v
	case []float32:
		out := make([]float64, len(v))
		for i, f := range v {
			out[i] = float64(f)
		}
		return out
	case []int64:
		out := make([]float64, len(v))
		for i, n := range v {
			out[i] = float64(n)
		}
		return out
	case []int32:
		out := make([]float64, len(v))
		for i, n := range v {
			out[i] = float64(n)
		}
		return out
	case []uint64:
		out := make([]float64, len(v))
		for i, n := range v {
			out[i] = float64(n)
		}
		return out
	default:
		return nil
	}
}

// median returns the median of a float64 slice. The input is not modified.
func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)

	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

// Abs returns the absolute value of a float64. Provided as a convenience
// to avoid importing math in multiple places.
func Abs(f float64) float64 {
	return math.Abs(f)
}
