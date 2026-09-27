package framework

import (
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
)

// Facts is the session facts service shared by the bgworker (which creates
// and runs it) and the trigger (which reports late bars to it). Nil until
// the bgworker starts, and in tests that do not need it.
var Facts *sessionfacts.Service

// now is the framework's clock. Tests replace it.
var now = time.Now

// markLateBars reports 1Min bars whose trading date's facts may already be
// written: bars for a date before the symbol's live day (a backfill), and
// bars for a date whose afterhours session has already ended (a fill after
// the close). The facts service recomputes those dates.
//
// liveDay is the symbol's LiveDay before the bars were applied.
func markLateBars(symbol string, bars []bar, liveDay int64) {
	facts := Facts
	if facts == nil || !facts.Leader() {
		return
	}
	t := now()
	var marked map[int64]struct{}
	for _, b := range bars {
		day := tradingDay(b.epoch)
		if _, done := marked[day]; done {
			continue
		}
		late := liveDay != 0 && day < liveDay
		if !late {
			ds, err := calendar.Nasdaq.SessionBoundsAt(time.Unix(day, 0))
			late = err == nil && !t.Before(ds.Afterhours.End)
		}
		if !late {
			continue
		}
		if marked == nil {
			marked = map[int64]struct{}{}
		}
		marked[day] = struct{}{}
		facts.MarkDirty(symbol, time.Unix(day, 0))
	}
}

// liveDay returns the state's LiveDay under its lock.
func (s *SymbolState) liveDay() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.LiveDay
}
