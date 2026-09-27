package framework

import (
	"github.com/alpacahq/marketstore/v4/frontend/stream"
)

// MsgTypeBar is the msg_type for OHLCV bar messages.
const MsgTypeBar = "bar"

// MsgTypeTrade is the msg_type for trade messages.
const MsgTypeTrade = "trade"

// MsgTypeQuote is the msg_type for quote messages.
const MsgTypeQuote = "quote"

// MsgTypeCurationChange is the msg_type for curation change notifications.
const MsgTypeCurationChange = "curation_change"

// MsgTypeWatchlistUpdate is the msg_type for watchlist ranking updates.
const MsgTypeWatchlistUpdate = "watchlist_update"

// PushTick pushes a bar/trade/quote to the stream, using Push (broadcast)
// for curated symbols and PushDirect for non-curated symbols.
func PushTick(symbol, timeframe, attrGroup, msgType string, data map[string]interface{}, curated bool) {
	payload := map[string]interface{}{
		"msg_type": msgType,
		"payload":  data,
	}

	// Resolve a cached TBK to avoid the per-tick fmt.Sprintf inside
	// io.NewTimeBucketKey. The (symbol, timeframe, attrGroup) tuple is
	// stable, so this is safe.
	tbk := tbkCache.Get(symbol, timeframe, attrGroup)

	if curated {
		_ = stream.Push(*tbk, payload)
	} else {
		_ = stream.PushDirect(*tbk, payload)
	}
}

// PushCurationChange pushes a curation change notification.
func PushCurationChange(timeframe string, added, removed []CurationChangeEntry, curatedCount int) {
	payload := map[string]interface{}{
		"msg_type": MsgTypeCurationChange,
		"payload": map[string]interface{}{
			"added":         curationEntriesToMaps(added),
			"removed":       curationEntriesToMaps(removed),
			"curated_count": curatedCount,
		},
	}

	tbk := tbkCache.GetByItemKey("CURATION/" + timeframe + "/CHANGES")
	_ = stream.Push(*tbk, payload)
}

// PushWatchlistUpdate pushes a watchlist ranking update.
//
// Each entry is serialized as a fresh map[string]interface{} for the stream
// layer (which expects map-shaped payloads). The map is sized to the exact
// field count plus the symbol/rank/sector keys to avoid bucket growth, and
// each typed Field is unboxed into the map exactly once.
//
// Besides name, timeframe and symbols, the payload says which ranking this
// is: basis ("session" or "traditional"), session, trading_date (YYYY-MM-DD,
// America/New_York) and as_of (the window end, Unix seconds). A client can
// always tell what prior_close and pct_change mean from basis.
func PushWatchlistUpdate(timeframe string, list RankedList) {
	symbols := list.Symbols
	symbolMaps := make([]map[string]interface{}, len(symbols))
	for i, rs := range symbols {
		size := 2 + len(rs.Fields)
		if rs.Sector != "" {
			size++
		}
		m := make(map[string]interface{}, size)
		m["symbol"] = rs.Symbol
		m["rank"] = rs.Rank
		for _, f := range rs.Fields {
			m[f.Key] = f.Value
		}
		if rs.Sector != "" {
			m["sector"] = rs.Sector
		}
		symbolMaps[i] = m
	}

	payload := map[string]interface{}{
		"msg_type": MsgTypeWatchlistUpdate,
		"payload": map[string]interface{}{
			"name":         list.Name,
			"timeframe":    timeframe,
			"symbols":      symbolMaps,
			"basis":        list.Basis.String(),
			"session":      list.Window.Session.String(),
			"trading_date": list.Window.TradingDateString(),
			"as_of":        list.Window.End.Unix(),
			"complete":     list.Window.Complete,
		},
	}

	tbk := tbkCache.GetByItemKey("WATCHLISTS/" + timeframe + "/" + list.Name)
	_ = stream.Push(*tbk, payload)
}

// CurationChangeEntry describes a single symbol added to or removed from curation.
type CurationChangeEntry struct {
	Symbol string
	Reason string
}

func curationEntriesToMaps(entries []CurationChangeEntry) []map[string]interface{} {
	if len(entries) == 0 {
		return []map[string]interface{}{}
	}
	result := make([]map[string]interface{}, len(entries))
	for i, e := range entries {
		result[i] = map[string]interface{}{
			"symbol": e.Symbol,
			"reason": e.Reason,
		}
	}
	return result
}
