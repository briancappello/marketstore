package framework

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

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
	if cfg.Curation.LookbackSecs > 0 {
		dollarVolLookback.Store(int64(cfg.Curation.LookbackSecs))
	}
	return &WatchlistTrigger{config: *cfg}, nil
}

// Fire is called by MarketStore when data matching the trigger's "on:" pattern
// is written to disk. Fires for one bucket arrive in write order; fires for
// different buckets may run concurrently.
//
// The newest written row is decoded straight from records, which carry the
// exact bytes just committed. The previous implementation read that row back
// from disk on every fire (roughly once per second per active symbol), which
// made it the largest CPU consumer in the server: the backward scan converts
// every record timestamp in a 256 KiB window to return a single row. The disk
// read remains as the fallback for variable-length buckets and anything that
// does not decode cleanly.
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

	if len(records) == 0 {
		return
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

	cs, err := writtenRows(tbk, tf.Duration, int16(year), records)
	if err != nil {
		log.Error("[watchlist] %s: %v", symbol, err)
		return
	}
	if cs == nil || cs.Len() == 0 {
		return
	}

	// Fold every written bar into the day's state (see day_state.go). Bars
	// from an earlier day (a backfill) do not touch today's state and are
	// not pushed.
	state := Manager.GetOrCreate(symbol)
	bars := barsFromColumnSeries(cs)
	if tf.Duration == time.Minute {
		// Session facts derive from 1Min bars; report ones that land after
		// their date's facts may have been written.
		markLateBars(symbol, bars, state.liveDay())
	}
	if !state.applyBars(bars, tf.Duration < time.Minute) {
		return
	}

	// Push the newest written bar.
	data := columnSeriesRowToMap(cs, newestRow(cs))
	if data == nil {
		return
	}

	// Evaluate curation.
	curated := true
	if Manager.curator != nil {
		curated = Manager.curator.Evaluate(symbol, state.curationSnapshot())
	}
	state.setCurated(curated)
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

// newestRow returns the index of the row with the latest epoch (the last
// one on ties, matching what is on disk).
func newestRow(cs *io.ColumnSeries) int {
	epochs := cs.GetEpoch()
	best := 0
	for i, e := range epochs {
		if e >= epochs[best] {
			best = i
		}
	}
	return best
}

// columnSeriesRowToMap extracts row i of a ColumnSeries into a map keyed by
// the lower-cased column names (pre-lowered once in column_keys.go; lowering
// per call would allocate for every column on every tick).
func columnSeriesRowToMap(cs *io.ColumnSeries, i int) map[string]interface{} {
	if cs == nil || i >= cs.Len() {
		return nil
	}
	cols := cs.GetColumns()
	m := make(map[string]interface{}, len(cols))
	for key, col := range cols {
		s := reflect.ValueOf(col)
		if s.Len() > i {
			m[lowerColumnKey(key)] = s.Index(i).Interface()
		}
	}
	return m
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
