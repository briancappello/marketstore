package framework

import (
	"math"
	"sort"
	"strings"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

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
