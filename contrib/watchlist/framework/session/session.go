// Package session resolves a requested trading session and as_of time into
// the window that watchlist rankings are computed over.
//
// The rules are the ones in the watchlist trading-sessions spec
// (openspec/changes/watchlist-sessions/specs/watchlist/trading-sessions).
// The package is pure: it reads the exchange calendar and the supplied "now",
// never the wall clock, so every rule is testable with fixed times.
package session

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

// Errors returned by Resolve and ParseAsOf. Callers map them to client
// errors (HTTP 400, gRPC InvalidArgument).
var (
	// ErrNotTradingDay: as_of falls on a weekend or an exchange holiday.
	ErrNotTradingDay = calendar.ErrNotTradingDay
	// ErrSessionNotStarted: the requested session has not started yet, or
	// as_of lies in the future.
	ErrSessionNotStarted = errors.New("session not started")
	// ErrInvalidSession: the session name is not one of premarket, regular
	// or afterhours.
	ErrInvalidSession = errors.New("invalid session")
	// ErrInvalidAsOf: as_of is not an ISO-8601 date or date-time.
	ErrInvalidAsOf = errors.New("invalid as_of")
	// ErrNotInSession: the requested watchlist is not available in the
	// resolved session (e.g. a _TRADITIONAL list outside the regular
	// session).
	ErrNotInSession = errors.New("not available in this session")
)

// Session re-exports the calendar session type so callers need one import.
type Session = calendar.Session

// Session values.
const (
	Premarket  = calendar.Premarket
	Regular    = calendar.Regular
	Afterhours = calendar.Afterhours
)

// Window is a resolved ranking window.
type Window struct {
	// TradingDate is midnight of the session's trading day, America/New_York.
	TradingDate time.Time
	Session     Session
	// SessionStart and SessionEnd are the full session's bounds.
	SessionStart, SessionEnd time.Time
	// Start and End are the ranking window: bars with Start <= epoch < End
	// are included. End is SessionEnd for a complete window, and otherwise
	// the as_of (or now) instant, exclusive of the bar that starts there.
	Start, End time.Time
	// Complete is true when the window covers the whole session.
	Complete bool
}

// TradingDateString returns the trading date as YYYY-MM-DD.
func (w Window) TradingDateString() string { return w.TradingDate.Format("2006-01-02") }

// AsOf is a parsed as_of value. The zero value means "not given".
type AsOf struct {
	// T is the instant. For a date-only value it is midnight of the date in
	// America/New_York.
	T time.Time
	// DateOnly is true when no time of day was given.
	DateOnly bool
	set      bool
}

// IsSet reports whether an as_of was given.
func (a AsOf) IsSet() bool { return a.set }

// At returns an AsOf for an instant.
func At(t time.Time) AsOf { return AsOf{T: t, set: true} }

// OnDate returns a date-only AsOf for the calendar date of t in
// America/New_York.
func OnDate(t time.Time) AsOf {
	y, m, d := t.In(tz()).Date()
	return AsOf{T: time.Date(y, m, d, 0, 0, 0, 0, tz()), DateOnly: true, set: true}
}

// tz is the exchange timezone.
func tz() *time.Location { return calendar.Nasdaq.Tz() }

// localLayouts are accepted as_of layouts without a UTC offset; they are
// read in America/New_York.
var localLayouts = []string{
	"2006-01-02T15:04:05",
	"2006-01-02T15:04",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
}

// ParseAsOf parses an ISO-8601 date or date-time. An empty string returns
// the zero AsOf ("not given"). A value without a UTC offset is read as
// America/New_York.
func ParseAsOf(s string) (AsOf, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return AsOf{}, nil
	}
	if t, err := time.ParseInLocation("2006-01-02", s, tz()); err == nil {
		return AsOf{T: t, DateOnly: true, set: true}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return At(t), nil
	}
	for _, layout := range localLayouts {
		if t, err := time.ParseInLocation(layout, s, tz()); err == nil {
			return At(t), nil
		}
	}
	return AsOf{}, fmt.Errorf("%w: %q (want YYYY-MM-DD or an ISO-8601 date-time)", ErrInvalidAsOf, s)
}

// ParseSession parses a session name. An empty string returns ok=false
// ("not given").
func ParseSession(name string) (s Session, ok bool, err error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, false, nil
	}
	s, err = calendar.ParseSession(name)
	if err != nil {
		return 0, false, fmt.Errorf("%w: %v", ErrInvalidSession, err)
	}
	return s, true, nil
}

// Query is a request for a ranking window. A nil Session means "not given".
type Query struct {
	Session *Session
	AsOf    AsOf
}

// Resolve applies the trading-sessions rules to q at the instant now.
func Resolve(q Query, now time.Time) (Window, error) {
	switch {
	case q.Session != nil && q.AsOf.IsSet():
		return resolveOnDate(*q.Session, q.AsOf, now)
	case q.Session != nil:
		return resolveLatest(*q.Session, now)
	case q.AsOf.IsSet() && q.AsOf.DateOnly:
		// A date without a session means that date's regular session.
		return resolveOnDate(Regular, q.AsOf, now)
	case q.AsOf.IsSet():
		return resolveContaining(q.AsOf.T, now)
	default:
		return resolveLive(now)
	}
}

// resolveOnDate resolves a given session on the trading date of asOf.
func resolveOnDate(s Session, asOf AsOf, now time.Time) (Window, error) {
	ds, err := calendar.Nasdaq.SessionBoundsAt(asOf.T)
	if err != nil {
		return Window{}, err
	}
	if asOf.T.After(now) {
		return Window{}, fmt.Errorf("%w: as_of %s is in the future", ErrSessionNotStarted,
			asOf.T.In(tz()).Format(time.RFC3339))
	}
	span := ds.Span(s)
	if span.Start.After(now) {
		return Window{}, fmt.Errorf("%w: %s %s starts at %s", ErrSessionNotStarted, s,
			ds.Date.Format("2006-01-02"), span.Start.Format(time.RFC3339))
	}

	end := span.End
	if !asOf.DateOnly && span.Contains(asOf.T) {
		// Inside the session: open to as_of.
		end = asOf.T
	}
	// After the session, before it (the caller wants that session of that
	// date), or a date only: the whole session. Every case is capped at now,
	// so a session still in progress runs to now rather than into the
	// future.
	return window(ds, s, end, now), nil
}

// resolveLatest resolves the most recent occurrence of s that has started.
func resolveLatest(s Session, now time.Time) (Window, error) {
	if ds, err := calendar.Nasdaq.SessionBoundsAt(now); err == nil && !ds.Span(s).Start.After(now) {
		return window(ds, s, ds.Span(s).End, now), nil
	}
	ds, err := prevSessions(now)
	if err != nil {
		return Window{}, err
	}
	return window(ds, s, ds.Span(s).End, now), nil
}

// resolveContaining resolves the session that contains t, or else the most
// recent session that ended at or before t.
func resolveContaining(t, now time.Time) (Window, error) {
	ds, err := calendar.Nasdaq.SessionBoundsAt(t)
	if err != nil {
		return Window{}, err
	}
	if t.After(now) {
		return Window{}, fmt.Errorf("%w: as_of %s is in the future", ErrSessionNotStarted,
			t.In(tz()).Format(time.RFC3339))
	}
	if s, ok := ds.SessionAt(t); ok {
		return window(ds, s, t, now), nil
	}
	return latestEndedBy(ds, t, now)
}

// resolveLive resolves the session in progress at now, or else the most
// recently completed one.
func resolveLive(now time.Time) (Window, error) {
	ds, err := calendar.Nasdaq.SessionBoundsAt(now)
	if err == nil {
		if s, ok := ds.SessionAt(now); ok {
			return window(ds, s, now, now), nil
		}
		return latestEndedBy(ds, now, now)
	}
	// Weekend or holiday: the previous trading day's afterhours.
	prev, err := prevSessions(now)
	if err != nil {
		return Window{}, err
	}
	return window(prev, Afterhours, prev.Afterhours.End, now), nil
}

// latestEndedBy returns the latest complete session of ds that ended at or
// before t, falling back to the previous trading day's afterhours when t is
// before ds's premarket ends.
func latestEndedBy(ds calendar.DaySessions, t, now time.Time) (Window, error) {
	for i := len(calendar.Sessions) - 1; i >= 0; i-- {
		s := calendar.Sessions[i]
		if !ds.Span(s).End.After(t) {
			return window(ds, s, ds.Span(s).End, now), nil
		}
	}
	prev, err := prevSessions(ds.Date)
	if err != nil {
		return Window{}, err
	}
	return window(prev, Afterhours, prev.Afterhours.End, now), nil
}

// prevSessions returns the sessions of the trading day before t's date.
func prevSessions(t time.Time) (calendar.DaySessions, error) {
	return calendar.Nasdaq.SessionBoundsAt(calendar.Nasdaq.PrevMarketDay(t))
}

// window builds the Window for session s of ds, ending at end, capped to the
// session and to now.
func window(ds calendar.DaySessions, s Session, end, now time.Time) Window {
	span := ds.Span(s)
	if end.After(span.End) {
		end = span.End
	}
	if end.After(now) {
		end = now
	}
	if end.Before(span.Start) {
		end = span.Start
	}
	return Window{
		TradingDate:  ds.Date,
		Session:      s,
		SessionStart: span.Start,
		SessionEnd:   span.End,
		Start:        span.Start,
		End:          end,
		Complete:     end.Equal(span.End),
	}
}
