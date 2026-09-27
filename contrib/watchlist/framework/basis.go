package framework

import (
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

// Basis selects what prior_close and pct_change mean in a watchlist (see the
// ranking-basis spec).
type Basis int

const (
	// BasisSession compares against the close of the session immediately
	// before: premarket against the previous day's afterhours, regular
	// against today's premarket, afterhours against today's official
	// close. Every session has it. Lists use the strategy's bare name.
	BasisSession Basis = iota
	// BasisTraditional compares against the previous trading date's
	// official close. Only the regular session has it. Lists use the
	// strategy's name plus TraditionalSuffix.
	BasisTraditional
)

// TraditionalSuffix is appended to a strategy's name for its traditional
// list.
const TraditionalSuffix = "_TRADITIONAL"

// String returns "session" or "traditional", the value used in APIs.
func (b Basis) String() string {
	if b == BasisTraditional {
		return "traditional"
	}
	return "session"
}

// Available reports whether basis b exists in session s.
func (b Basis) Available(s calendar.Session) bool {
	return b == BasisSession || s == calendar.Regular
}

// ListName is the published name of a strategy's list for basis b.
func ListName(strategyName string, b Basis) string {
	if b == BasisTraditional {
		return strategyName + TraditionalSuffix
	}
	return strategyName
}

// SplitListName returns the strategy name and basis of a published list
// name.
func SplitListName(list string) (strategyName string, b Basis) {
	if base, ok := strings.CutSuffix(list, TraditionalSuffix); ok {
		return base, BasisTraditional
	}
	return list, BasisSession
}

// BasisSupport is an optional WatchlistStrategy interface declaring which
// bases the strategy supports. A strategy without it supports both. A
// strategy that is traditional by definition (a gap from the previous
// close, a daily moving average) returns only BasisTraditional, and so is
// published only as NAME_TRADITIONAL, only in the regular session.
type BasisSupport interface {
	Bases() []Basis
}

// VolumeMedianUser is an optional WatchlistStrategy interface. A strategy
// whose ranking depends on relative volume (VolumeMultipleOfMed) returns
// true, and symbols without a volume median for the session are left out of
// its input instead of ranking with a multiple of zero.
type VolumeMedianUser interface {
	UsesVolumeMedian() bool
}

// RankContext tells a strategy which ranking it is producing.
type RankContext struct {
	// TradingDate is midnight America/New_York of the session's date.
	TradingDate time.Time
	Session     calendar.Session
	Basis       Basis
	// WindowEnd is the end of the ranking window: now for a live session in
	// progress, as_of for a partial rewind, the session end otherwise.
	WindowEnd time.Time
}

// ContextualRanker is an optional WatchlistStrategy interface for strategies
// that read data beyond the per-symbol state (e.g. daily closes from disk):
// they must read it as of ctx.TradingDate, not as of the wall clock, so a
// rewound ranking uses only data from before that date. When implemented,
// RankAt is called instead of Rank.
type ContextualRanker interface {
	RankAt(ctx RankContext, curated map[string]*SymbolState) []RankedSymbol
}

// basesOf returns the bases strategy s supports.
func basesOf(s WatchlistStrategy) []Basis {
	if bs, ok := s.(BasisSupport); ok {
		return bs.Bases()
	}
	return []Basis{BasisSession, BasisTraditional}
}

func usesVolumeMedian(s WatchlistStrategy) bool {
	u, ok := s.(VolumeMedianUser)
	return ok && u.UsesVolumeMedian()
}

// PublishedNames returns the list names strategy s publishes in session
// sess: its bare name for the session basis, NAME_TRADITIONAL for the
// traditional basis in the regular session, for each basis it supports.
func PublishedNames(s WatchlistStrategy, sess calendar.Session) []string {
	var out []string
	for _, b := range basesOf(s) {
		if b.Available(sess) {
			out = append(out, ListName(s.Name(), b))
		}
	}
	return out
}
