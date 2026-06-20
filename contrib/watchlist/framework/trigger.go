package framework

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// WatchlistTrigger is the MarketStore trigger plugin that processes every
// incoming tick, updates per-symbol state, evaluates curation, and pushes
// data to WebSocket subscribers.
type WatchlistTrigger struct {
	config TriggerConfig
}

// NewTrigger creates a new WatchlistTrigger from the raw plugin config.
func NewTrigger(conf map[string]interface{}) (trigger.Trigger, error) {
	cfg, err := ParseTriggerConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("watchlist trigger config error: %w", err)
	}
	return &WatchlistTrigger{config: *cfg}, nil
}

// Fire is called by MarketStore when data matching the trigger's "on:" pattern
// is written to disk. It runs in its own goroutine and may be called
// concurrently for different symbols.
//
// The implementation follows the same disk-query pattern as the stream trigger:
// read the latest row back from disk rather than parsing raw record bytes.
func (t *WatchlistTrigger) Fire(keyPath string, records []trigger.Record) {
	// Parse symbol/timeframe/attrgroup/fileName from the key path in a single
	// pass to avoid repeated allocations on this hot path. keyPath is like
	// "AAPL/1Min/OHLCV/2024.bin".
	symbol, timeframe, attrGroup, fileName, err := parseKeyPathFull(keyPath)
	if err != nil {
		log.Error("[watchlist] failed to parse key path %q: %v", keyPath, err)
		return
	}

	if Manager == nil {
		log.Warn("[watchlist] Manager not initialized, skipping fire for %s", symbol)
		return
	}

	// Find the max index from the written records to query the latest row.
	// Avoid the intermediate slice allocation on this per-tick hot path.
	tail := int64(0)
	for _, record := range records {
		if idx := record.Index(); idx > tail {
			tail = idx
		}
	}

	// Parse the year from the filename ("2024.bin" -> 2024). Use TrimSuffix
	// instead of Replace to avoid an allocation when the suffix is present.
	yearStr := strings.TrimSuffix(fileName, ".bin")
	year, err := strconv.ParseInt(yearStr, 10, 32)
	if err != nil {
		log.Error("[watchlist] get year from filename (%v)", err)
		return
	}

	// Resolve the cached TBK for this (symbol, timeframe, attrGroup) tuple.
	// Falls back to allocating one if not yet cached.
	tbk := tbkCache.Get(symbol, timeframe, attrGroup)
	tf := utils.NewTimeframe(timeframe)
	end := io.IndexToTime(tail, tf.Duration, int16(year))

	// Query the latest row from disk.
	cDir := executor.ThisInstance.CatalogDir
	q := planner.NewQuery(cDir)
	q.AddTargetKey(tbk)
	q.SetEnd(end)
	q.SetRowLimit(io.LAST, 1)

	parsed, err := q.Parse()
	if err != nil {
		log.Error("[watchlist] query parse error for %s: %v", symbol, err)
		return
	}

	scanner, err := executor.NewReader(parsed)
	if err != nil {
		log.Error("[watchlist] reader error for %s: %v", symbol, err)
		return
	}

	csm, err := scanner.Read()
	if err != nil {
		log.Error("[watchlist] read error for %s: %v", symbol, err)
		return
	}

	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return
	}

	// Extract bar data from the column series.
	data := columnSeriesToMap(cs)
	if data == nil {
		return
	}

	state := Manager.GetOrCreate(symbol)

	// Update running state.
	updateSymbolState(state, data)

	// Evaluate curation.
	curated := true
	if Manager.curator != nil {
		curated = Manager.curator.Evaluate(symbol, state)
	}
	state.IsCurated = curated
	Manager.UpdateCuration(symbol, curated)

	// Determine msg_type from the attribute group.
	msgType := attrGroupToMsgType(attrGroup)

	// Add symbol to the payload data.
	data["symbol"] = symbol

	// Push to stream.
	PushTick(symbol, timeframe, attrGroup, msgType, data, curated)
}

// parseKeyPathFull extracts symbol, timeframe, attribute group, and the
// trailing filename from a MarketStore key path like
// "AAPL/1Min/OHLCV/2024.bin", in a single pass without allocating a slice.
//
// This is on the per-tick Fire hot path; allocation discipline matters.
func parseKeyPathFull(keyPath string) (symbol, timeframe, attrGroup, fileName string, err error) {
	// Find the three '/' separators that split the four expected segments.
	first := strings.IndexByte(keyPath, '/')
	if first < 0 {
		return "", "", "", "", fmt.Errorf("key path has fewer than 3 segments: %q", keyPath)
	}
	second := strings.IndexByte(keyPath[first+1:], '/')
	if second < 0 {
		return "", "", "", "", fmt.Errorf("key path has fewer than 3 segments: %q", keyPath)
	}
	second += first + 1
	third := strings.IndexByte(keyPath[second+1:], '/')
	if third < 0 {
		// Three segments only (no filename); valid for some callers.
		return keyPath[:first], keyPath[first+1 : second], keyPath[second+1:], "", nil
	}
	third += second + 1
	return keyPath[:first], keyPath[first+1 : second], keyPath[second+1 : third], keyPath[third+1:], nil
}

// columnSeriesToMap extracts the first (most recent) record from a ColumnSeries
// into a map. Uses the same reflection approach as the stream trigger.
//
// Note: column keys are returned as-is from the ColumnSeries. The caller
// (updateSymbolState, downstream consumers) must use the canonical key case
// established by the data writer ("Open", "High", ...). Lower-casing per call
// allocates a fresh string for every column on every tick, which on a
// 5-symbol-per-second feed amounts to tens of millions of allocations per
// minute; we instead pre-lower the keys once on construction below.
func columnSeriesToMap(cs *io.ColumnSeries) map[string]interface{} {
	if cs == nil || cs.Len() == 0 {
		return nil
	}

	cols := cs.GetColumns()
	m := make(map[string]interface{}, len(cols))
	for key, col := range cols {
		s := reflect.ValueOf(col)
		if s.Len() > 0 {
			m[lowerColumnKey(key)] = s.Index(0).Interface()
		}
	}
	return m
}

// updateSymbolState updates the running state from extracted bar data.
// It handles day-boundary resets: if the incoming tick is for a different
// calendar day than the current LiveDay (or SeededDay if no live data has
// been seen yet), the running state is cleared before processing.
func updateSymbolState(state *SymbolState, data map[string]interface{}) {
	// Check for day boundary and reset if needed.
	if epoch, ok := getFloat(data, "epoch"); ok {
		tickDay := time.Unix(int64(epoch), 0).Truncate(24 * time.Hour).Unix()
		currentDay := state.LiveDay
		if currentDay == 0 {
			currentDay = state.SeededDay
		}
		if currentDay != 0 && tickDay != currentDay {
			// New trading day. The seeded/previous-day running state is stale.
			// Preserve PriorClose from the last known close before resetting.
			if state.LastClose != 0 {
				state.PriorClose = state.LastClose
			}
			state.ResetDaily()
		}
		state.LiveDay = tickDay
	}

	if v, ok := getFloat(data, "high"); ok {
		if state.HighOfDay == 0 || v > state.HighOfDay {
			state.HighOfDay = v
		}
	}
	if v, ok := getFloat(data, "low"); ok {
		if state.LowOfDay == 0 || v < state.LowOfDay {
			state.LowOfDay = v
		}
	}
	if v, ok := getFloat(data, "close"); ok {
		state.LastClose = v
		state.LastPrice = v
	}
	if v, ok := getFloat(data, "open"); ok {
		if state.DayOpen == 0 {
			state.DayOpen = v
		}
	}
	if v, ok := getFloat(data, "volume"); ok {
		state.CumulativeVolume += int64(v)
	}
	if v, ok := getFloat(data, "epoch"); ok {
		state.LastEpoch = int64(v)
	}
	state.TickCount++

	// Recompute derived metrics.
	if state.PriorClose != 0 {
		state.PctChange = (state.LastPrice - state.PriorClose) / state.PriorClose * 100
	}
	if state.MedianVolume50D != 0 {
		state.VolumeMultipleOfMed = float64(state.CumulativeVolume) / state.MedianVolume50D
	}
	// DollarVolumeRate: estimate as (cumulative_volume * last_price) / seconds_elapsed.
	// This is a rough approximation; a ring buffer would be more accurate.
	if state.TickCount > 0 && state.LastPrice > 0 {
		state.DollarVolumeRate = float64(state.CumulativeVolume) * state.LastPrice / float64(state.TickCount*60)
	}
}

// getFloat extracts a float64 from a map, handling both float64 and int64 values.
func getFloat(data map[string]interface{}, key string) (float64, bool) {
	v, ok := data[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	default:
		return 0, false
	}
}

// attrGroupToMsgType maps an attribute group name to a msg_type string.
func attrGroupToMsgType(attrGroup string) string {
	switch strings.ToUpper(attrGroup) {
	case "TRADE":
		return MsgTypeTrade
	case "QUOTE":
		return MsgTypeQuote
	default:
		return MsgTypeBar
	}
}
