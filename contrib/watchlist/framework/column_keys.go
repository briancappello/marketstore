package framework

import (
	"strings"
	"sync"
)

// columnKeyCache memoizes the lower-cased form of column names returned by
// io.ColumnSeries.GetColumns(). The set of distinct column names is tiny
// (Open, High, Low, Close, Volume, Epoch, Bid, Ask, Size, ...) and fixed
// per attribute group. Without caching, columnSeriesToMap calls
// strings.ToLower on every column on every tick, allocating a fresh string
// each time; with caching, each unique column name is lowered exactly once
// over the process lifetime.
var (
	columnKeyMu    sync.RWMutex
	columnKeyCache = make(map[string]string, 16)
)

// lowerColumnKey returns the lower-cased form of a column name, memoizing
// the result. Safe for concurrent use.
func lowerColumnKey(key string) string {
	columnKeyMu.RLock()
	v, ok := columnKeyCache[key]
	columnKeyMu.RUnlock()
	if ok {
		return v
	}

	v = strings.ToLower(key)

	columnKeyMu.Lock()
	// Double-check after upgrading the lock.
	if existing, ok := columnKeyCache[key]; ok {
		columnKeyMu.Unlock()
		return existing
	}
	columnKeyCache[key] = v
	columnKeyMu.Unlock()
	return v
}
