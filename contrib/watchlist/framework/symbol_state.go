package framework

import "sync"

// SymbolState holds per-symbol state maintained across ticks.
// The framework updates the core fields on every tick; custom Curator
// and WatchlistStrategy implementations can use the Extra map for
// additional per-symbol state.
type SymbolState struct {
	// --- Baselines (set by BgWorker, refreshed daily) ---

	// MedianVolume50D is the 50-day rolling median of daily volume.
	MedianVolume50D float64

	// PriorClose is yesterday's closing price.
	PriorClose float64

	// --- Running state (updated per tick by Trigger) ---

	// DayOpen is the opening price of the current trading day.
	// Set on the first tick of the day.
	DayOpen float64

	// LastClose is the close price of the most recent bar.
	LastClose float64

	// LastPrice is an alias for LastClose (the most recent price).
	LastPrice float64

	// HighOfDay is the running maximum high of the current day.
	HighOfDay float64

	// LowOfDay is the running minimum low of the current day.
	LowOfDay float64

	// CumulativeVolume is the current day's volume: the sum of the volume
	// ledger below (see day_state.go), not a running sum of fires.
	CumulativeVolume int64

	// PremarketVolume is the running sum of premarket session volume.
	PremarketVolume int64

	// LastEpoch is the epoch timestamp of the most recent tick.
	LastEpoch int64

	// TickCount is the number of ticks received today.
	TickCount int64

	// --- Derived metrics (recomputed on each tick) ---

	// PctChange is (LastPrice - PriorClose) / PriorClose * 100.
	PctChange float64

	// VolumeMultipleOfMed is CumulativeVolume / MedianVolume50D.
	VolumeMultipleOfMed float64

	// DollarVolumeRate is the estimated dollar volume per second over
	// a recent lookback window.
	DollarVolumeRate float64

	// --- Day tracking ---

	// SeededDay is the New York trading day (Unix seconds at midnight) that the
	// running state was seeded from during baseline computation. When the
	// first live tick arrives for a different day, ResetDaily() is called
	// before processing the tick. This prevents stale seeded values from
	// contaminating live intraday state.
	SeededDay int64

	// LiveDay is the New York trading day of the most recent bar processed.
	// Used to detect day boundaries for intraday state resets.
	LiveDay int64

	// --- Curation status ---

	// IsCurated indicates whether this symbol is currently in the curated universe.
	IsCurated bool

	// WasCurated tracks the previous curation status for change detection.
	WasCurated bool

	// --- Extension point ---

	// Extra allows custom Curator and WatchlistStrategy implementations to
	// store arbitrary per-symbol state. The framework never reads or writes
	// this map; it is entirely owned by custom code.
	Extra map[string]interface{}

	// mu serializes updates; the 1Sec and 1Min triggers and the baseline
	// seeding can update the same symbol concurrently.
	mu sync.Mutex
	// minuteVol is the day's volume per minute (keyed by minute start) from
	// 1Min-or-coarser bars. subMinuteVol holds 1Sec bar volumes (minute ->
	// second -> volume) for minutes that have no 1Min bar yet.
	minuteVol    map[int64]int64
	subMinuteVol map[int64]map[int64]int64
	// dayOpenEpoch is the start of the bar DayOpen was taken from.
	dayOpenEpoch int64
}

// NewSymbolState creates a new SymbolState with initialized Extra map.
func NewSymbolState() *SymbolState {
	return &SymbolState{
		Extra: make(map[string]interface{}),
	}
}

// ResetDaily clears running state for a new trading day while preserving
// baselines and Extra state.
func (s *SymbolState) ResetDaily() {
	s.DayOpen = 0
	s.LastClose = 0
	s.LastPrice = 0
	s.HighOfDay = 0
	s.LowOfDay = 0
	s.CumulativeVolume = 0
	s.PremarketVolume = 0
	s.LastEpoch = 0
	s.TickCount = 0
	s.PctChange = 0
	s.VolumeMultipleOfMed = 0
	s.DollarVolumeRate = 0
	s.minuteVol = nil
	s.subMinuteVol = nil
	s.dayOpenEpoch = 0
}
