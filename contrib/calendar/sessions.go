package calendar

import (
	"errors"
	"fmt"
	"time"
)

// ErrNotTradingDay reports that a date has no trading sessions: it is a
// weekend or an exchange holiday.
var ErrNotTradingDay = errors.New("not a trading day")

// Session identifies one of the three trading sessions of a trading day.
type Session int

const (
	// Premarket runs from the extended open (04:00) to the regular open.
	Premarket Session = iota
	// Regular runs from the regular open (09:30) to the regular close, which
	// is earlier on early-close days.
	Regular
	// Afterhours runs from the regular close to the regular close plus the
	// extended-hours offset (4h).
	Afterhours
)

// Sessions lists all sessions in the order they occur during a day.
var Sessions = [...]Session{Premarket, Regular, Afterhours}

// String returns the session name used in APIs: "premarket", "regular" or
// "afterhours".
func (s Session) String() string {
	switch s {
	case Premarket:
		return "premarket"
	case Regular:
		return "regular"
	case Afterhours:
		return "afterhours"
	default:
		return fmt.Sprintf("Session(%d)", int(s))
	}
}

// ParseSession parses a session name as returned by Session.String.
func ParseSession(name string) (Session, error) {
	for _, s := range Sessions {
		if s.String() == name {
			return s, nil
		}
	}
	return 0, fmt.Errorf("unknown session %q (want premarket, regular or afterhours)", name)
}

// Span is a half-open time interval [Start, End).
type Span struct {
	Start, End time.Time
}

// Contains reports whether t lies in [Start, End).
func (s Span) Contains(t time.Time) bool {
	return !t.Before(s.Start) && t.Before(s.End)
}

// DaySessions holds the three session spans of one trading day. The spans are
// consecutive: Premarket.End == Regular.Start and Regular.End ==
// Afterhours.Start.
type DaySessions struct {
	// Date is midnight of the trading day in the calendar's timezone.
	Date       time.Time
	Premarket  Span
	Regular    Span
	Afterhours Span
}

// Span returns the span of session s.
func (d DaySessions) Span(s Session) Span {
	switch s {
	case Premarket:
		return d.Premarket
	case Regular:
		return d.Regular
	default:
		return d.Afterhours
	}
}

// SessionAt returns the session containing t, and false when t lies outside
// every session of the day.
func (d DaySessions) SessionAt(t time.Time) (Session, bool) {
	for _, s := range Sessions {
		if d.Span(s).Contains(t) {
			return s, true
		}
	}
	return 0, false
}

// SessionBounds returns the sessions of the trading day year-month-day, as a
// date in the calendar's timezone. It returns ErrNotTradingDay for weekends
// and exchange holidays.
//
// The boundaries are wall-clock times in the calendar's timezone, so they do
// not move when daylight saving time starts or ends.
func (calendar *Calendar) SessionBounds(year int, month time.Month, day int) (DaySessions, error) {
	date := time.Date(year, month, day, 0, 0, 0, 0, calendar.tz)
	if wd := date.Weekday(); wd == time.Saturday || wd == time.Sunday {
		return DaySessions{}, fmt.Errorf("%s: %w", date.Format("2006-01-02"), ErrNotTradingDay)
	}

	ct := calendar.closeTime
	if state, found := calendar.days[julianDate(date)]; found {
		if state != EarlyClose { // Closed
			return DaySessions{}, fmt.Errorf("%s: %w", date.Format("2006-01-02"), ErrNotTradingDay)
		}
		ct = calendar.earlyCloseTime
	}

	at := func(t Time) time.Time {
		return time.Date(year, month, day, t.hour, t.minute, t.second, 0, calendar.tz)
	}
	preOpen, regOpen, regClose := at(calendar.openTime), at(calendar.regularOpen), at(ct)
	return DaySessions{
		Date:       date,
		Premarket:  Span{preOpen, regOpen},
		Regular:    Span{regOpen, regClose},
		Afterhours: Span{regClose, regClose.Add(extendedHoursOffset)},
	}, nil
}

// SessionBoundsAt returns the sessions of the trading day that contains t,
// taking the date of t in the calendar's timezone.
func (calendar *Calendar) SessionBoundsAt(t time.Time) (DaySessions, error) {
	return calendar.SessionBounds(t.In(calendar.tz).Date())
}

// PrevMarketDay returns the most recent trading day strictly before the
// calendar date of t (in the calendar's timezone), as midnight in the
// calendar's timezone.
func (calendar *Calendar) PrevMarketDay(t time.Time) time.Time {
	y, m, d := t.In(calendar.tz).Date()
	day := time.Date(y, m, d, 0, 0, 0, 0, calendar.tz)
	const maxDaysBack = 10 // worst case holiday + weekend spans
	for i := 1; i <= maxDaysBack; i++ {
		prev := day.AddDate(0, 0, -i)
		if calendar.IsMarketDay(prev) {
			return prev
		}
	}
	return day.AddDate(0, 0, -1)
}
