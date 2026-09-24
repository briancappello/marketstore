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
// trigger's curation.lookback_secs.
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
			if s.LastClose != 0 {
				s.PriorClose = s.LastClose
			}
			s.ResetDaily()
		}
		s.LiveDay = day
		applied = true

		if b.hasVolume {
			s.recordVolume(b.epoch, b.volume, subMinute)
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
		}
		s.TickCount++
	}
	if applied {
		s.recomputeDerived()
	}
	return applied
}

// recordVolume updates the volume ledger for one bar.
func (s *SymbolState) recordVolume(epoch, volume int64, subMinute bool) {
	if s.minuteVol == nil {
		s.minuteVol = make(map[int64]int64)
		s.subMinuteVol = make(map[int64]map[int64]int64)
	}
	minute := epoch - epoch%60
	if !subMinute {
		old, had := s.minuteVol[minute]
		if !had {
			for _, v := range s.subMinuteVol[minute] {
				old += v
			}
			delete(s.subMinuteVol, minute)
		}
		s.minuteVol[minute] = volume
		s.CumulativeVolume += volume - old
		return
	}
	if _, ok := s.minuteVol[minute]; ok {
		return // the minute's own bar is authoritative
	}
	secs := s.subMinuteVol[minute]
	if secs == nil {
		secs = make(map[int64]int64)
		s.subMinuteVol[minute] = secs
	}
	s.CumulativeVolume += volume - secs[epoch]
	secs[epoch] = volume
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
