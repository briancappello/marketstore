package framework

import (
	"strings"
	"sync"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// tbkCacheT caches *io.TimeBucketKey values keyed by the
// "symbol/timeframe/attrGroup" string. Each TBK is otherwise constructed via
// fmt.Sprintf inside io.NewTimeBucketKey on every tick, which is allocated
// hundreds or thousands of times per second during market hours. The keys
// are static per (symbol, timeframe, attrGroup) tuple, so caching them is
// strictly correct.
//
// The cache is unbounded but bounded in practice by the cardinality of
// active (symbol, timeframe, attrGroup) tuples — at most one entry per
// active subscription. For a US equity feed with ~14k symbols across 1-3
// timeframes and 1-3 attribute groups, peak size is in the tens of
// thousands of entries: O(MB) of memory at most.
type tbkCacheT struct {
	mu sync.RWMutex
	m  map[string]*io.TimeBucketKey
}

// tbkCache is the package-level cache used by the Fire hot path.
var tbkCache = &tbkCacheT{m: make(map[string]*io.TimeBucketKey)}

// Get returns the cached *TimeBucketKey for the given (symbol, timeframe,
// attrGroup) tuple, constructing and caching one on first request.
//
// The fast path is a read-locked map lookup with a single string allocation
// for the cache key. The slow path (cache miss) constructs the underlying
// TBK once and stores it; subsequent calls return the same pointer.
func (c *tbkCacheT) Get(symbol, timeframe, attrGroup string) *io.TimeBucketKey {
	// Build the cache key. This is a single concatenation; the underlying
	// strings are typically interned (symbol from a state map, timeframe
	// and attrGroup from package-level constants).
	var sb strings.Builder
	sb.Grow(len(symbol) + 1 + len(timeframe) + 1 + len(attrGroup))
	sb.WriteString(symbol)
	sb.WriteByte('/')
	sb.WriteString(timeframe)
	sb.WriteByte('/')
	sb.WriteString(attrGroup)
	key := sb.String()

	c.mu.RLock()
	tbk, ok := c.m[key]
	c.mu.RUnlock()
	if ok {
		return tbk
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Double-check after acquiring the write lock.
	if tbk, ok = c.m[key]; ok {
		return tbk
	}
	tbk = io.NewTimeBucketKey(key)
	c.m[key] = tbk
	return tbk
}

// GetByItemKey returns the cached *TimeBucketKey for an arbitrary
// "/"-separated item key like "WATCHLISTS/1Min/MOMENTUM" or
// "CURATION/1Min/CHANGES". This avoids a per-push fmt.Sprintf in
// io.NewTimeBucketKey for the static publish keys.
func (c *tbkCacheT) GetByItemKey(itemKey string) *io.TimeBucketKey {
	c.mu.RLock()
	tbk, ok := c.m[itemKey]
	c.mu.RUnlock()
	if ok {
		return tbk
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if tbk, ok = c.m[itemKey]; ok {
		return tbk
	}
	tbk = io.NewTimeBucketKey(itemKey)
	c.m[itemKey] = tbk
	return tbk
}
