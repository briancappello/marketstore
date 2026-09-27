package session_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework/session"
)

var ny, _ = time.LoadLocation("America/New_York")

func at(y int, m time.Month, d, hh, mm int) time.Time { return time.Date(y, m, d, hh, mm, 0, 0, ny) }

func sess(s session.Session) *session.Session { return &s }

func TestParseAsOf(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in       string
		want     time.Time
		dateOnly bool
		set      bool
		wantErr  bool
	}{
		{in: "", set: false},
		{in: "2026-09-21", want: at(2026, 9, 21, 0, 0), dateOnly: true, set: true},
		{in: "2026-09-21T11:15", want: at(2026, 9, 21, 11, 15), set: true},
		{in: "2026-09-21T11:15:00", want: at(2026, 9, 21, 11, 15), set: true},
		{in: "2026-09-21 11:15", want: at(2026, 9, 21, 11, 15), set: true},
		{in: "2026-09-21T15:15:00Z", want: at(2026, 9, 21, 11, 15), set: true},
		{in: "2026-09-21T11:15:00-04:00", want: at(2026, 9, 21, 11, 15), set: true},
		{in: "yesterday", wantErr: true},
		{in: "2026-13-01", wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()
			got, err := session.ParseAsOf(tt.in)
			if tt.wantErr {
				assert.True(t, errors.Is(err, session.ErrInvalidAsOf), "err = %v", err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.set, got.IsSet())
			if tt.set {
				assert.True(t, tt.want.Equal(got.T), "got %v want %v", got.T, tt.want)
				assert.Equal(t, tt.dateOnly, got.DateOnly)
			}
		})
	}
}

func TestParseSession(t *testing.T) {
	t.Parallel()
	s, ok, err := session.ParseSession("afterhours")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, session.Afterhours, s)

	_, ok, err = session.ParseSession("")
	require.NoError(t, err)
	assert.False(t, ok)

	_, _, err = session.ParseSession("overnight")
	assert.True(t, errors.Is(err, session.ErrInvalidSession))
}

func TestResolve(t *testing.T) {
	t.Parallel()

	// "now" for past-date cases: well after 2026-09-21.
	later := at(2026, 9, 24, 12, 0)
	// A trading day (Tue 2026-09-22) at various times.
	tue := func(hh, mm int) time.Time { return at(2026, 9, 22, hh, mm) }

	tests := []struct {
		name        string
		q           session.Query
		now         time.Time
		wantErr     error
		wantDate    time.Time
		wantSession session.Session
		wantStart   time.Time
		wantEnd     time.Time
		complete    bool
	}{
		// --- session + as_of ---
		{
			name: "as_of inside the session",
			q:    session.Query{Session: sess(session.Regular), AsOf: session.At(at(2026, 9, 21, 11, 15))},
			now:  later, wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Regular,
			wantStart: at(2026, 9, 21, 9, 30), wantEnd: at(2026, 9, 21, 11, 15), complete: false,
		},
		{
			name: "as_of after the session",
			q:    session.Query{Session: sess(session.Premarket), AsOf: session.At(at(2026, 9, 21, 10, 0))},
			now:  later, wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Premarket,
			wantStart: at(2026, 9, 21, 4, 0), wantEnd: at(2026, 9, 21, 9, 30), complete: true,
		},
		{
			name: "past date 08:00 premarket covers 04:00-08:00",
			q:    session.Query{Session: sess(session.Premarket), AsOf: session.At(at(2026, 9, 21, 8, 0))},
			now:  later, wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Premarket,
			wantStart: at(2026, 9, 21, 4, 0), wantEnd: at(2026, 9, 21, 8, 0), complete: false,
		},
		{
			name: "past date 08:00 regular covers the whole session",
			q:    session.Query{Session: sess(session.Regular), AsOf: session.At(at(2026, 9, 21, 8, 0))},
			now:  later, wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Regular,
			wantStart: at(2026, 9, 21, 9, 30), wantEnd: at(2026, 9, 21, 16, 0), complete: true,
		},
		{
			name: "past date 08:00 afterhours covers the whole session",
			q:    session.Query{Session: sess(session.Afterhours), AsOf: session.At(at(2026, 9, 21, 8, 0))},
			now:  later, wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 9, 21, 16, 0), wantEnd: at(2026, 9, 21, 20, 0), complete: true,
		},
		{
			name: "date-only as_of",
			q:    session.Query{Session: sess(session.Afterhours), AsOf: session.OnDate(at(2026, 9, 21, 0, 0))},
			now:  later, wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 9, 21, 16, 0), wantEnd: at(2026, 9, 21, 20, 0), complete: true,
		},
		{
			name: "early close regular",
			q:    session.Query{Session: sess(session.Regular), AsOf: session.OnDate(at(2026, 11, 27, 0, 0))},
			now:  at(2026, 12, 1, 12, 0), wantDate: at(2026, 11, 27, 0, 0), wantSession: session.Regular,
			wantStart: at(2026, 11, 27, 9, 30), wantEnd: at(2026, 11, 27, 13, 0), complete: true,
		},
		{
			name: "today's session in progress with a date-only as_of runs to now",
			q:    session.Query{Session: sess(session.Regular), AsOf: session.OnDate(tue(0, 0))},
			now:  tue(11, 0), wantDate: tue(0, 0), wantSession: session.Regular,
			wantStart: tue(9, 30), wantEnd: tue(11, 0), complete: false,
		},

		// --- rejections ---
		{
			name:    "weekend",
			q:       session.Query{AsOf: session.At(at(2026, 9, 26, 10, 0))},
			now:     at(2026, 9, 28, 12, 0),
			wantErr: session.ErrNotTradingDay,
		},
		{
			name:    "weekend with session",
			q:       session.Query{Session: sess(session.Regular), AsOf: session.OnDate(at(2026, 9, 26, 0, 0))},
			now:     at(2026, 9, 28, 12, 0),
			wantErr: session.ErrNotTradingDay,
		},
		{
			name:    "holiday",
			q:       session.Query{Session: sess(session.Regular), AsOf: session.OnDate(at(2026, 11, 26, 0, 0))},
			now:     at(2026, 12, 1, 12, 0),
			wantErr: session.ErrNotTradingDay,
		},
		{
			name:    "today's session not started yet",
			q:       session.Query{Session: sess(session.Regular), AsOf: session.At(tue(8, 0))},
			now:     tue(8, 0),
			wantErr: session.ErrSessionNotStarted,
		},
		{
			name:    "future as_of",
			q:       session.Query{Session: sess(session.Regular), AsOf: session.At(tue(11, 0))},
			now:     tue(10, 0),
			wantErr: session.ErrSessionNotStarted,
		},
		{
			name:    "future date-only as_of",
			q:       session.Query{AsOf: session.OnDate(at(2026, 9, 23, 0, 0))},
			now:     tue(10, 0),
			wantErr: session.ErrSessionNotStarted,
		},

		// --- defaults ---
		{
			name: "no parameters during regular hours",
			q:    session.Query{}, now: tue(11, 0),
			wantDate: tue(0, 0), wantSession: session.Regular,
			wantStart: tue(9, 30), wantEnd: tue(11, 0), complete: false,
		},
		{
			name: "no parameters overnight",
			q:    session.Query{}, now: tue(22, 0),
			wantDate: tue(0, 0), wantSession: session.Afterhours,
			wantStart: tue(16, 0), wantEnd: tue(20, 0), complete: true,
		},
		{
			name: "no parameters before premarket uses the previous day's afterhours",
			q:    session.Query{}, now: tue(3, 0),
			wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 9, 21, 16, 0), wantEnd: at(2026, 9, 21, 20, 0), complete: true,
		},
		{
			name: "no parameters on a weekend uses friday's afterhours",
			q:    session.Query{}, now: at(2026, 9, 26, 12, 0),
			wantDate: at(2026, 9, 25, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 9, 25, 16, 0), wantEnd: at(2026, 9, 25, 20, 0), complete: true,
		},
		{
			name: "session only, not started today, uses the previous trading day",
			q:    session.Query{Session: sess(session.Afterhours)}, now: tue(11, 0),
			wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 9, 21, 16, 0), wantEnd: at(2026, 9, 21, 20, 0), complete: true,
		},
		{
			name: "session only, in progress, ends now",
			q:    session.Query{Session: sess(session.Regular)}, now: tue(11, 0),
			wantDate: tue(0, 0), wantSession: session.Regular,
			wantStart: tue(9, 30), wantEnd: tue(11, 0), complete: false,
		},
		{
			name: "session only, earlier today and complete",
			q:    session.Query{Session: sess(session.Premarket)}, now: tue(11, 0),
			wantDate: tue(0, 0), wantSession: session.Premarket,
			wantStart: tue(4, 0), wantEnd: tue(9, 30), complete: true,
		},
		{
			name: "as_of date-time without session, inside a session",
			q:    session.Query{AsOf: session.At(at(2026, 9, 21, 17, 30))}, now: later,
			wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 9, 21, 16, 0), wantEnd: at(2026, 9, 21, 17, 30), complete: false,
		},
		{
			name: "as_of date-time without session, after the last session",
			q:    session.Query{AsOf: session.At(at(2026, 9, 21, 21, 0))}, now: later,
			wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 9, 21, 16, 0), wantEnd: at(2026, 9, 21, 20, 0), complete: true,
		},
		{
			name: "as_of date-time without session, before premarket",
			q:    session.Query{AsOf: session.At(at(2026, 9, 22, 3, 0))}, now: later,
			wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 9, 21, 16, 0), wantEnd: at(2026, 9, 21, 20, 0), complete: true,
		},
		{
			name: "date-only as_of without session is the regular session",
			q:    session.Query{AsOf: session.OnDate(at(2026, 9, 21, 0, 0))}, now: later,
			wantDate: at(2026, 9, 21, 0, 0), wantSession: session.Regular,
			wantStart: at(2026, 9, 21, 9, 30), wantEnd: at(2026, 9, 21, 16, 0), complete: true,
		},
		{
			name: "EST afterhours 19:30 stays on its trading date",
			q:    session.Query{AsOf: session.At(at(2026, 1, 13, 19, 30).UTC())}, now: later,
			wantDate: at(2026, 1, 13, 0, 0), wantSession: session.Afterhours,
			wantStart: at(2026, 1, 13, 16, 0), wantEnd: at(2026, 1, 13, 19, 30), complete: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w, err := session.Resolve(tt.q, tt.now)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.wantErr), "err = %v, want %v", err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.True(t, tt.wantDate.Equal(w.TradingDate), "date: got %v want %v", w.TradingDate, tt.wantDate)
			assert.Equal(t, tt.wantSession, w.Session)
			assert.True(t, tt.wantStart.Equal(w.Start), "start: got %v want %v", w.Start, tt.wantStart)
			assert.True(t, tt.wantEnd.Equal(w.End), "end: got %v want %v", w.End, tt.wantEnd)
			assert.Equal(t, tt.complete, w.Complete)
		})
	}
}
