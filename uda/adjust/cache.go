package adjust

import (
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/corpaction"
)

// CacheTTL bounds how long loaded events are reused. New events become
// visible to adjust() within this delay.
var CacheTTL = 30 * time.Second

type cacheKey struct {
	dir    *catalog.Directory
	symbol string
	splits bool
}

type cacheEntry struct {
	events   []corpaction.Event
	loadedAt time.Time
}

var eventCache = struct {
	sync.RWMutex
	entries map[cacheKey]cacheEntry
}{entries: map[cacheKey]cacheEntry{}}

// ResetCache drops every cached event list.
func ResetCache() {
	eventCache.Lock()
	eventCache.entries = map[cacheKey]cacheEntry{}
	eventCache.Unlock()
}

func cachedEvents(dir *catalog.Directory, symbol string, kinds []corpaction.Kind) ([]corpaction.Event, error) {
	key := cacheKey{dir: dir, symbol: symbol}
	for _, k := range kinds {
		if k == corpaction.Splits {
			key.splits = true
		}
	}

	eventCache.RLock()
	entry, ok := eventCache.entries[key]
	eventCache.RUnlock()
	if ok && time.Since(entry.loadedAt) < CacheTTL {
		return entry.events, nil
	}

	events, err := corpaction.Load(dir, symbol, kinds...)
	if err != nil {
		return nil, err
	}
	eventCache.Lock()
	eventCache.entries[key] = cacheEntry{events: events, loadedAt: time.Now()}
	eventCache.Unlock()
	return events, nil
}
