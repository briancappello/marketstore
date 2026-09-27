package framework

import (
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

// Views.
//
// Strategies never see live SymbolState. For each ranking the framework
// builds a value snapshot of every curated symbol for one (session, basis):
// the fields strategies already read (PriorClose, PctChange, DayOpen,
// LastPrice, HighOfDay, LowOfDay, CumulativeVolume, VolumeMultipleOfMed,
// MedianVolume50D) are filled with that session's values and that basis's
// baseline. Strategies keep their code, and prior_close / pct_change get
// their meaning from the list the caller asked for.
//
// Snapshots are taken under the state's lock, so strategies no longer read
// fields that triggers are writing at the same time.

// priorCloseLocked returns the baseline for session sess under basis b, and
// false when the data it needs does not exist (ranking-basis spec).
func (s *SymbolState) priorCloseLocked(sess calendar.Session, b Basis) (float64, bool) {
	if b == BasisTraditional {
		// Traditional: the previous trading date's official close, and only
		// in the regular session.
		return s.PriorClose, sess == calendar.Regular && s.PriorClose != 0
	}
	switch sess {
	case calendar.Premarket:
		// The previous trading date's last afterhours close.
		return s.PrevAfterhoursClose, s.PrevAfterhoursClose != 0
	case calendar.Regular:
		// The same date's last premarket close.
		pre := s.sessions[calendar.Premarket]
		return pre.last, pre.hasBars()
	default:
		// The same date's official close: the 1D close once it exists,
		// otherwise the last regular-session close.
		if s.OfficialClose != 0 {
			return s.OfficialClose, true
		}
		reg := s.sessions[calendar.Regular]
		return reg.last, reg.hasBars()
	}
}

// PriorCloseFor returns the baseline for session sess under basis b, and
// false when it does not exist.
func (s *SymbolState) PriorCloseFor(sess calendar.Session, b Basis) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.priorCloseLocked(sess, b)
}

// view returns the snapshot of s for session sess of trading day day under
// basis b, or nil when the symbol drops out: it has no bars in the session,
// the basis baseline does not exist, or needMedian is set and the session
// has no volume median.
func (s *SymbolState) view(day int64, sess calendar.Session, b Basis, needMedian bool) *SymbolState {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.LiveDay != day {
		return nil // no bar of this trading date yet
	}
	a := s.sessions[sess]
	if !a.hasBars() {
		return nil
	}
	prior, ok := s.priorCloseLocked(sess, b)
	if !ok || prior == 0 {
		return nil
	}
	med := s.SessionMedianVolume[sess]
	if needMedian && med == 0 {
		return nil
	}

	v := &SymbolState{
		PriorClose:          prior,
		PrevAfterhoursClose: s.PrevAfterhoursClose,
		OfficialClose:       s.OfficialClose,
		SessionMedianVolume: s.SessionMedianVolume,
		MedianVolume50D:     med,

		DayOpen:          a.open,
		LastClose:        a.last,
		LastPrice:        a.last,
		HighOfDay:        a.high,
		LowOfDay:         a.low,
		CumulativeVolume: a.volume,
		PremarketVolume:  s.sessions[calendar.Premarket].volume,
		LastEpoch:        a.lastEpoch,
		TickCount:        s.TickCount,

		PctChange:        (a.last - prior) / prior * 100,
		DollarVolumeRate: s.DollarVolumeRate,

		SeededDay:  s.SeededDay,
		LiveDay:    s.LiveDay,
		IsCurated:  s.IsCurated,
		WasCurated: s.WasCurated,

		// Extra is shared on purpose: it is the strategies' own per-symbol
		// state and must persist across rankings.
		Extra: s.Extra,

		sessions:  s.sessions,
		bounds:    s.bounds,
		boundsDay: s.boundsDay,
		boundsOK:  s.boundsOK,
	}
	if med > 0 {
		v.VolumeMultipleOfMed = float64(a.volume) / med
	}
	return v
}

// buildView snapshots every state in states for (day, sess, b, needMedian),
// leaving out symbols that drop out.
func buildView(states map[string]*SymbolState, day int64, sess calendar.Session, b Basis,
	needMedian bool,
) map[string]*SymbolState {
	out := make(map[string]*SymbolState, len(states))
	for sym, st := range states {
		if v := st.view(day, sess, b, needMedian); v != nil {
			out[sym] = v
		}
	}
	return out
}
