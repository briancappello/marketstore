package framework

import (
	"sync"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

// SymbolState holds per-symbol state maintained across ticks.
// The framework updates the core fields on every tick; custom Curator
// and WatchlistStrategy implementations can use the Extra map for
// additional per-symbol state.
type SymbolState struct {
	// --- Baselines (set by BgWorker, refreshed daily) ---

	// MedianVolume50D is the 50-day rolling median of daily volume.
	MedianVolume50D float64

	// PriorClose is the official close of the previous trading date: the
	// traditional baseline. It is never taken from a premarket or
	// afterhours print.
	PriorClose float64

	// PrevAfterhoursClose is the last afterhours 1Min close of the previous
	// trading date: the session baseline for premarket. Zero means the
	// previous afterhours session had no bars.
	PrevAfterhoursClose float64

	// SessionMedianVolume is each session's median volume over the median
	// window of trading dates before the current one, indexed by
	// calendar.Session. Zero means no history for that session.
	SessionMedianVolume [len(calendar.Sessions)]float64

	// OfficialClose is the current trading date's official close (its 1D
	// bar close) once the 1D bar exists; zero until then. It is the session
	// baseline for afterhours and becomes PriorClose when the day rolls.
	OfficialClose float64

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

	// PremarketVolume is the current trading date's premarket volume: the
	// volume so far during premarket, and the whole premarket session's
	// volume after it ends.
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

	// sessions holds the running values of each session of LiveDay,
	// indexed by calendar.Session.
	sessions [len(calendar.Sessions)]sessionAcc
	// bounds are LiveDay's session boundaries in Unix seconds: premarket
	// start, regular start, afterhours start, afterhours end. boundsDay is
	// the day they were computed for; boundsOK is false on non-trading days.
	bounds    [len(calendar.Sessions) + 1]int64
	boundsDay int64
	boundsOK  bool

	// pending holds baselines loaded for a trading date the state has not
	// reached yet; they are applied when the day rolls to that date.
	pending *Baselines
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
	s.sessions = [len(calendar.Sessions)]sessionAcc{}
}

// curationChange is the result of syncCuration.
type curationChange int

const (
	curationUnchanged curationChange = iota
	curationAdded
	curationRemoved
)

// setCurated records the curator's verdict under the state's lock.
func (s *SymbolState) setCurated(curated bool) {
	s.mu.Lock()
	s.IsCurated = curated
	s.mu.Unlock()
}

// syncCuration compares IsCurated with WasCurated, makes WasCurated match,
// and reports the change.
func (s *SymbolState) syncCuration() curationChange {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.IsCurated && !s.WasCurated:
		s.WasCurated = true
		return curationAdded
	case !s.IsCurated && s.WasCurated:
		s.WasCurated = false
		return curationRemoved
	}
	return curationUnchanged
}

// curationSnapshot returns a copy of the fields a Curator reads, taken
// under the lock, so the curator never reads fields a trigger is writing.
func (s *SymbolState) curationSnapshot() *SymbolState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &SymbolState{
		MedianVolume50D:     s.MedianVolume50D,
		PriorClose:          s.PriorClose,
		PrevAfterhoursClose: s.PrevAfterhoursClose,
		OfficialClose:       s.OfficialClose,
		SessionMedianVolume: s.SessionMedianVolume,
		DayOpen:             s.DayOpen,
		LastClose:           s.LastClose,
		LastPrice:           s.LastPrice,
		HighOfDay:           s.HighOfDay,
		LowOfDay:            s.LowOfDay,
		CumulativeVolume:    s.CumulativeVolume,
		PremarketVolume:     s.PremarketVolume,
		LastEpoch:           s.LastEpoch,
		TickCount:           s.TickCount,
		PctChange:           s.PctChange,
		VolumeMultipleOfMed: s.VolumeMultipleOfMed,
		DollarVolumeRate:    s.DollarVolumeRate,
		SeededDay:           s.SeededDay,
		LiveDay:             s.LiveDay,
		IsCurated:           s.IsCurated,
		WasCurated:          s.WasCurated,
		Extra:               s.Extra,
		sessions:            s.sessions,
	}
}
