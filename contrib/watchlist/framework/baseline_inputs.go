package framework

import (
	"sort"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// Baseline inputs.
//
// Baselines (traditional prior close, previous afterhours close, official
// close, per-session volume medians) are derived from each symbol's 1D/OHLCV
// and 1D/SESSIONS rows. Those rows change after they were first read: the
// overnight backfill writes the official 1D close, the leader recomputes facts
// for late bars, and a replica applies its master's writes in whatever order
// and at whatever lag replication delivers them. The watchlist trigger, bound
// to */1D/OHLCV and */1D/SESSIONS, reports those writes here, and the worker
// reloads the affected symbols' baselines on its next ranking tick. So
// baselines follow the data, and a leader and its replica holding the same
// rows end up with the same baselines.

// isBaselineInput reports whether a bucket's rows feed baselines.
func isBaselineInput(timeframe, attrGroup string) bool {
	return timeframe == "1D" && (attrGroup == "OHLCV" || attrGroup == sessionfacts.AttributeGroup)
}

// dateSettled reports whether a 1D row for date (midnight New York) is a
// settled input at t: the date's afterhours session has ended, or it was not
// a trading day. A date's own 1D bar is rewritten all day while it forms, and
// it only becomes a baseline once the date is over.
func dateSettled(date int64, t time.Time) bool {
	ds, err := calendar.Nasdaq.SessionBoundsAt(time.Unix(date, 0))
	return err != nil || !t.Before(ds.Afterhours.End)
}

// inputChanges collects the symbols whose baseline inputs changed since the
// worker last reloaded them.
type inputChanges struct {
	mu        sync.Mutex
	symbols   map[string]struct{}
	listeners []func(earliestDate int64)
}

var changedInputs = &inputChanges{symbols: map[string]struct{}{}}

// onBaselineInputChange registers fn to be called with the earliest date of
// every settled baseline-input write. The rewinder drops its cached results
// from that date on.
func onBaselineInputChange(fn func(earliestDate int64)) {
	changedInputs.mu.Lock()
	defer changedInputs.mu.Unlock()
	changedInputs.listeners = append(changedInputs.listeners, fn)
}

// noteBaselineInputs records a write of records to symbol's 1D bucket of
// year. Writes dated on a day still in progress are ignored.
func noteBaselineInputs(symbol string, year int16, records []trigger.Record) {
	t := now()
	earliest := int64(0)
	for _, r := range records {
		date := dailyDate(io.IndexToTime(r.Index(), 24*time.Hour, year).Unix()).Unix()
		if dateSettled(date, t) && (earliest == 0 || date < earliest) {
			earliest = date
		}
	}
	if earliest == 0 {
		return
	}
	changedInputs.mu.Lock()
	changedInputs.symbols[symbol] = struct{}{}
	listeners := changedInputs.listeners
	changedInputs.mu.Unlock()
	for _, fn := range listeners {
		fn(earliest)
	}
}

// drainChangedInputs returns, sorted, and forgets the symbols whose inputs
// changed.
func drainChangedInputs() []string {
	changedInputs.mu.Lock()
	defer changedInputs.mu.Unlock()
	if len(changedInputs.symbols) == 0 {
		return nil
	}
	out := make([]string, 0, len(changedInputs.symbols))
	for s := range changedInputs.symbols {
		out = append(out, s)
	}
	changedInputs.symbols = map[string]struct{}{}
	sort.Strings(out)
	return out
}

// requeueChangedInputs puts symbols back, for a reload that could not run.
func requeueChangedInputs(symbols []string) {
	changedInputs.mu.Lock()
	defer changedInputs.mu.Unlock()
	for _, s := range symbols {
		changedInputs.symbols[s] = struct{}{}
	}
}

// reloadBaselines reloads symbols' baselines for every day the live states
// hold them for: each symbol's live (or seeded) day, and the day the worker
// loaded next (baselineDate, which may be ahead of it).
func reloadBaselines(mgr *SymbolStateManager, facts *sessionfacts.Service, symbols []string,
	baselineDate time.Time, medianWindow int, at time.Time,
) {
	catDir := executorCatalog()
	bounds := map[int64]*calendar.DaySessions{}
	dayOf := func(date int64) *calendar.DaySessions {
		if ds, ok := bounds[date]; ok {
			return ds
		}
		var p *calendar.DaySessions
		if ds, err := calendar.Nasdaq.SessionBoundsAt(time.Unix(date, 0)); err == nil {
			p = &ds
		}
		bounds[date] = p
		return p
	}
	for _, symbol := range symbols {
		st := mgr.Get(symbol)
		if st == nil {
			continue // not tracked: nothing holds its baselines
		}
		days := []int64{st.currentDay()}
		if !baselineDate.IsZero() && baselineDate.Unix() != days[0] {
			days = append(days, baselineDate.Unix())
		}
		for _, date := range days {
			ds := dayOf(date)
			if date == 0 || ds == nil {
				continue
			}
			if b, err := LoadBaselines(facts, catDir, symbol, *ds, medianWindow, at); err == nil {
				st.SetBaselines(b)
			}
		}
	}
}

// currentDay returns the trading day the state is on: its live day, or the
// day it was seeded for before any live bar.
func (s *SymbolState) currentDay() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.LiveDay != 0 {
		return s.LiveDay
	}
	return s.SeededDay
}
