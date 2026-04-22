package framework

import (
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/utils/io"
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

	tbk := io.NewTimeBucketKey(symbol + "/" + timeframe + "/" + attrGroup)

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

	tbk := io.NewTimeBucketKey("CURATION/" + timeframe + "/CHANGES")
	_ = stream.Push(*tbk, payload)
}

// PushWatchlistUpdate pushes a watchlist ranking update.
func PushWatchlistUpdate(timeframe, watchlistName string, symbols []RankedSymbol) {
	symbolMaps := make([]map[string]interface{}, len(symbols))
	for i, rs := range symbols {
		m := map[string]interface{}{
			"symbol": rs.Symbol,
			"rank":   rs.Rank,
		}
		for k, v := range rs.Fields {
			m[k] = v
		}
		symbolMaps[i] = m
	}

	payload := map[string]interface{}{
		"msg_type": MsgTypeWatchlistUpdate,
		"payload": map[string]interface{}{
			"name":      watchlistName,
			"timeframe": timeframe,
			"symbols":   symbolMaps,
		},
	}

	tbk := io.NewTimeBucketKey("WATCHLISTS/" + timeframe + "/" + watchlistName)
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
