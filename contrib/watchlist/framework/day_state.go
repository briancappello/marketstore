package framework

import (
	"strings"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

/*
Daily running state.

Volume used to be a running sum, CumulativeVolume += volume, applied on every
fire. That was wrong in several compounding ways (measured in production at
~14x the traded volume, and 0.2x after a restart):

  - The 1Sec and 1Min watchlist triggers both fed the same state, so every
    trade was counted in its 1Sec bar and again in its 1Min bar.
  - The 1Sec -> 1Min cascade rewrites the current minute every second with its
    running volume, and each rewrite was added again: a minute with n 1Sec
    bars contributed ~n/2 times its volume.
  - Only the newest bar of a multi-bar write was looked at, so backfills and
    outage fills added one bar.
  - A backfill writing yesterday then today looked like two day changes, and
    each reset discarded the day's volume.
  - Days were UTC days, which roll over at 20:00 EDT / 19:00 EST.

Volume is now a ledger keyed by bar start time. A 1Min (or coarser) bar
replaces whatever was recorded for its minute; 1Sec bars only count toward
minutes that have no 1Min bar yet (the in-progress minute, or deployments
without a 1Min feed). Rewrites therefore replace, batches count every bar,
and the order in which the two triggers run does not matter. Bars from a
day older than the current one are ignored rather than treated as a new day,
and days are New York trading days.
*/

// dollarVolLookback is the DollarVolumeRate window in seconds, from the
// bgworker's curation.lookback_secs.
var dollarVolLookback atomic.Int64

const defaultDollarVolLookback = 300

func init() { dollarVolLookback.Store(defaultDollarVolLookback) }

// bar is one OHLCV bar applied to a SymbolState. Price fields are zero when
// absent; hasVolume distinguishes a zero-volume bar from a missing column.
type bar struct {
	epoch                  int64
	open, high, low, close float64
	volume                 int64
	hasVolume              bool
}

// tradingDay returns the start (Unix seconds) of the New York calendar day
// containing epoch.
func tradingDay(epoch int64) int64 {
	tz := calendar.Nasdaq.Tz()
	y, m, d := time.Unix(epoch, 0).In(tz).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, tz).Unix()
}

// column returns a column as float64s, matching the name case-insensitively.
func column(cs *io.ColumnSeries, name string) []float64 {
	for _, n := range cs.GetColumnNames() {
		if strings.EqualFold(n, name) {
			return toFloat64Slice(cs.GetColumn(n))
		}
	}
	return nil
}

// barsFromColumnSeries converts every row of cs into a bar.
func barsFromColumnSeries(cs *io.ColumnSeries) []bar {
	epochs := cs.GetEpoch()
	if len(epochs) == 0 {
		return nil
	}
	o, h, l, c, v := column(cs, "Open"), column(cs, "High"), column(cs, "Low"), column(cs, "Close"), column(cs, "Volume")
	at := func(col []float64, i int) float64 {
		if i < len(col) {
			return col[i]
		}
		return 0
	}
	bars := make([]bar, len(epochs))
	for i, e := range epochs {
		bars[i] = bar{
			epoch: e, open: at(o, i), high: at(h, i), low: at(l, i), close: at(c, i),
			volume: int64(at(v, i)), hasVolume: i < len(v),
		}
	}
	return bars
}

// applyBars folds bars into the day's running state. subMinute is true for
// sources finer than one minute (1Sec). Bars may arrive in any order and may
// repeat; the result depends only on the latest version of each bar.
//
// Every bar also goes to the session (premarket, regular, afterhours) that
// contains it, so each session has its own open, high, low, last price and
// volume. Bars outside every session count only toward the day.
//
// It reports whether any bar belonged to the current trading day.
func (s *SymbolState) applyBars(bars []bar, subMinute bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	applied := false
	for _, b := range bars {
		day := tradingDay(b.epoch)
		current := s.LiveDay
		if current == 0 {
			current = s.SeededDay
		}
		switch {
		case current != 0 && day < current:
			continue // an earlier day being backfilled; not today's state
		case current != 0 && day > current:
			s.rollDay(day)
		}
		s.LiveDay = day
		s.ensureBounds(day)
		applied = true

		if b.hasVolume {
			minute, delta := s.recordVolume(b.epoch, b.volume, subMinute)
			s.CumulativeVolume += delta
			if i := s.sessionIndex(minute); i >= 0 {
				s.sessions[i].volume += delta
			}
		}
		if b.close != 0 {
			if b.high != 0 && (s.HighOfDay == 0 || b.high > s.HighOfDay) {
				s.HighOfDay = b.high
			}
			if b.low != 0 && (s.LowOfDay == 0 || b.low < s.LowOfDay) {
				s.LowOfDay = b.low
			}
			if b.open != 0 && (s.dayOpenEpoch == 0 || b.epoch < s.dayOpenEpoch) {
				s.DayOpen = b.open
				s.dayOpenEpoch = b.epoch
			}
			if b.epoch >= s.LastEpoch {
				s.LastClose = b.close
				s.LastPrice = b.close
				s.LastEpoch = b.epoch
			}
			if i := s.sessionIndex(b.epoch); i >= 0 {
				s.sessions[i].apply(b)
			}
		}
		s.TickCount++
	}
	if applied {
		s.PremarketVolume = s.sessions[calendar.Premarket].volume
		s.recomputeDerived()
	}
	return applied
}

// rollDay starts trading day newDay.
//
// When the bgworker has already loaded newDay's baselines they are applied.
// Otherwise the previous day's sessions become the new day's baselines where
// the definitions say so: its afterhours close is the next premarket's
// session baseline, and its official close is the next day's traditional
// baseline. The official close is the day's 1D close when known, and
// otherwise the last regular-session close. It is never the last print of
// the day, which is an afterhours price.
func (s *SymbolState) rollDay(newDay int64) {
	if p := s.pending; p != nil && p.Date.Unix() == newDay {
		s.pending = nil
		s.ResetDaily()
		s.applyBaselinesLocked(*p)
		return
	}
	s.pending = nil

	prevPost := s.sessions[calendar.Afterhours]
	prevReg := s.sessions[calendar.Regular]

	official := s.OfficialClose
	if official == 0 && prevReg.hasBars() {
		official = prevReg.last
	}
	if official != 0 {
		s.PriorClose = official
	}
	s.PrevAfterhoursClose = 0
	if prevPost.hasBars() {
		s.PrevAfterhoursClose = prevPost.last
	}
	s.OfficialClose = 0
	s.ResetDaily()
}

// SetBaselines installs baselines for trading date b.Date. They apply now
// when the state is on that date (or has no date yet), are held until the
// day rolls when the date is still ahead, and are ignored when stale.
func (s *SymbolState) SetBaselines(b Baselines) {
	s.mu.Lock()
	defer s.mu.Unlock()
	day := b.Date.Unix()
	current := s.LiveDay
	if current == 0 {
		current = s.SeededDay
	}
	switch {
	case current == 0:
		s.SeededDay = day
		s.applyBaselinesLocked(b)
	case current == day:
		s.applyBaselinesLocked(b)
	case day > current:
		cp := b
		s.pending = &cp
	}
}

func (s *SymbolState) applyBaselinesLocked(b Baselines) {
	s.PriorClose = b.PriorClose
	s.PrevAfterhoursClose = b.PrevAfterhoursClose
	s.OfficialClose = b.OfficialClose
	s.SessionMedianVolume = b.SessionMedianVolume
	s.MedianVolume50D = b.SessionMedianVolume[calendar.Regular]
	s.recomputeDerived()
}

// ensureBounds computes day's session boundaries once per day.
func (s *SymbolState) ensureBounds(day int64) {
	if s.boundsDay == day {
		return
	}
	s.boundsDay = day
	ds, err := calendar.Nasdaq.SessionBoundsAt(time.Unix(day, 0))
	if err != nil {
		s.boundsOK = false
		return
	}
	s.boundsOK = true
	s.bounds = [len(s.bounds)]int64{
		ds.Premarket.Start.Unix(), ds.Regular.Start.Unix(),
		ds.Afterhours.Start.Unix(), ds.Afterhours.End.Unix(),
	}
}

// sessionIndex returns the calendar.Session index containing epoch on
// LiveDay, or -1 when epoch lies outside every session.
func (s *SymbolState) sessionIndex(epoch int64) int {
	if !s.boundsOK || epoch < s.bounds[0] {
		return -1
	}
	for i := 1; i < len(s.bounds); i++ {
		if epoch < s.bounds[i] {
			return i - 1
		}
	}
	return -1
}

// sessionAcc holds one session's running values.
type sessionAcc struct {
	open      float64
	openEpoch int64
	high, low float64
	last      float64
	lastEpoch int64
	volume    int64
}

func (a *sessionAcc) hasBars() bool { return a.openEpoch != 0 || a.lastEpoch != 0 }

// apply folds a priced bar into the session. The open is the earliest bar's
// open and the last price the latest bar's close, whatever order bars
// arrive in.
func (a *sessionAcc) apply(b bar) {
	if b.high != 0 && (a.high == 0 || b.high > a.high) {
		a.high = b.high
	}
	if b.low != 0 && (a.low == 0 || b.low < a.low) {
		a.low = b.low
	}
	if b.open != 0 && (a.openEpoch == 0 || b.epoch < a.openEpoch) {
		a.open = b.open
		a.openEpoch = b.epoch
	}
	if b.epoch >= a.lastEpoch {
		a.last = b.close
		a.lastEpoch = b.epoch
	}
}

// SessionStats is one session's running values for a symbol.
type SessionStats struct {
	// Open is the open of the session's first bar; Last the close of its
	// latest bar. High, Low and Volume cover the session so far.
	Open, High, Low, Last float64
	Volume                int64
	// OpenEpoch and LastEpoch are the start times of those bars.
	OpenEpoch, LastEpoch int64
	// HasBars is false when no bar of the session has been seen.
	HasBars bool
}

// SessionStats returns the running values of session sess on LiveDay.
func (s *SymbolState) SessionStats(sess calendar.Session) SessionStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionStatsLocked(sess)
}

func (s *SymbolState) sessionStatsLocked(sess calendar.Session) SessionStats {
	a := s.sessions[sess]
	return SessionStats{
		Open: a.open, High: a.high, Low: a.low, Last: a.last,
		Volume: a.volume, OpenEpoch: a.openEpoch, LastEpoch: a.lastEpoch,
		HasBars: a.hasBars(),
	}
}

// recordVolume updates the volume ledger for one bar. It returns the start
// of the bar's minute and the change in that minute's volume.
func (s *SymbolState) recordVolume(epoch, volume int64, subMinute bool) (minute, delta int64) {
	if s.minuteVol == nil {
		s.minuteVol = make(map[int64]int64)
		s.subMinuteVol = make(map[int64]map[int64]int64)
	}
	minute = epoch - epoch%60
	if !subMinute {
		old, had := s.minuteVol[minute]
		if !had {
			for _, v := range s.subMinuteVol[minute] {
				old += v
			}
			delete(s.subMinuteVol, minute)
		}
		s.minuteVol[minute] = volume
		return minute, volume - old
	}
	if _, ok := s.minuteVol[minute]; ok {
		return minute, 0 // the minute's own bar is authoritative
	}
	secs := s.subMinuteVol[minute]
	if secs == nil {
		secs = make(map[int64]int64)
		s.subMinuteVol[minute] = secs
	}
	delta = volume - secs[epoch]
	secs[epoch] = volume
	return minute, delta
}

// minuteVolume is the ledger's volume for the minute starting at minute.
func (s *SymbolState) minuteVolume(minute int64) int64 {
	if v, ok := s.minuteVol[minute]; ok {
		return v
	}
	var t int64
	for _, v := range s.subMinuteVol[minute] {
		t += v
	}
	return t
}

// recomputeDerived refreshes the metrics derived from the running state.
func (s *SymbolState) recomputeDerived() {
	if s.PriorClose != 0 {
		s.PctChange = (s.LastPrice - s.PriorClose) / s.PriorClose * 100
	}
	if s.MedianVolume50D != 0 {
		s.VolumeMultipleOfMed = float64(s.CumulativeVolume) / s.MedianVolume50D
	}
	// Dollar volume per second over the lookback window ending at the most
	// recent bar, as the field is documented. The window is whole minutes:
	// the last bar's minute and the ones before it.
	lookback := dollarVolLookback.Load()
	if s.LastEpoch == 0 || s.LastPrice <= 0 || lookback <= 0 {
		s.DollarVolumeRate = 0
		return
	}
	last := s.LastEpoch - s.LastEpoch%60
	var vol int64
	for m := last; m > last-lookback; m -= 60 {
		vol += s.minuteVolume(m)
	}
	s.DollarVolumeRate = float64(vol) * s.LastPrice / float64(lookback)
}
