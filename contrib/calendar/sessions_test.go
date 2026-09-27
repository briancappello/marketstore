package calendar_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

var ny, _ = time.LoadLocation("America/New_York")

func at(y int, m time.Month, d, hh, mm int) time.Time { return time.Date(y, m, d, hh, mm, 0, 0, ny) }

func TestSessionBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		y                          int
		m                          time.Month
		d                          int
		pre, reg, post             [2]time.Time
		wantErr                    bool
		wantPreStartUTC, wantPostE string // pins DST handling in absolute time
	}{
		{
			name: "normal day (EDT)", y: 2026, m: 9, d: 22,
			pre:             [2]time.Time{at(2026, 9, 22, 4, 0), at(2026, 9, 22, 9, 30)},
			reg:             [2]time.Time{at(2026, 9, 22, 9, 30), at(2026, 9, 22, 16, 0)},
			post:            [2]time.Time{at(2026, 9, 22, 16, 0), at(2026, 9, 22, 20, 0)},
			wantPreStartUTC: "2026-09-22T08:00:00Z", wantPostE: "2026-09-23T00:00:00Z",
		},
		{
			name: "normal day (EST)", y: 2026, m: 1, d: 13,
			pre:             [2]time.Time{at(2026, 1, 13, 4, 0), at(2026, 1, 13, 9, 30)},
			reg:             [2]time.Time{at(2026, 1, 13, 9, 30), at(2026, 1, 13, 16, 0)},
			post:            [2]time.Time{at(2026, 1, 13, 16, 0), at(2026, 1, 13, 20, 0)},
			wantPreStartUTC: "2026-01-13T09:00:00Z", wantPostE: "2026-01-14T01:00:00Z",
		},
		{
			name: "early close", y: 2026, m: 11, d: 27,
			pre:  [2]time.Time{at(2026, 11, 27, 4, 0), at(2026, 11, 27, 9, 30)},
			reg:  [2]time.Time{at(2026, 11, 27, 9, 30), at(2026, 11, 27, 13, 0)},
			post: [2]time.Time{at(2026, 11, 27, 13, 0), at(2026, 11, 27, 17, 0)},
		},
		{name: "holiday", y: 2026, m: 11, d: 26, wantErr: true},
		{name: "saturday", y: 2026, m: 9, d: 26, wantErr: true},
		{name: "sunday", y: 2026, m: 9, d: 27, wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ds, err := calendar.Nasdaq.SessionBounds(tt.y, tt.m, tt.d)
			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, calendar.ErrNotTradingDay))
				return
			}
			require.NoError(t, err)
			check := func(name string, got calendar.Span, want [2]time.Time) {
				assert.True(t, got.Start.Equal(want[0]), "%s start: got %v want %v", name, got.Start, want[0])
				assert.True(t, got.End.Equal(want[1]), "%s end: got %v want %v", name, got.End, want[1])
			}
			check("premarket", ds.Premarket, tt.pre)
			check("regular", ds.Regular, tt.reg)
			check("afterhours", ds.Afterhours, tt.post)
			assert.True(t, ds.Date.Equal(at(tt.y, tt.m, tt.d, 0, 0)))
			if tt.wantPreStartUTC != "" {
				assert.Equal(t, tt.wantPreStartUTC, ds.Premarket.Start.UTC().Format(time.RFC3339))
				assert.Equal(t, tt.wantPostE, ds.Afterhours.End.UTC().Format(time.RFC3339))
			}
		})
	}
}

func TestSessionAt(t *testing.T) {
	t.Parallel()

	ds, err := calendar.Nasdaq.SessionBounds(2026, 1, 13) // EST
	require.NoError(t, err)

	tests := []struct {
		name string
		t    time.Time
		want calendar.Session
		ok   bool
	}{
		{"03:59 before premarket", at(2026, 1, 13, 3, 59), 0, false},
		{"04:00 premarket start", at(2026, 1, 13, 4, 0), calendar.Premarket, true},
		{"09:29 premarket", at(2026, 1, 13, 9, 29), calendar.Premarket, true},
		{"09:30 belongs to regular", at(2026, 1, 13, 9, 30), calendar.Regular, true},
		{"16:00 belongs to afterhours", at(2026, 1, 13, 16, 0), calendar.Afterhours, true},
		// 19:30 EST is 00:30 UTC on the next UTC day; it is still this
		// trading day's afterhours.
		{"19:30 EST afterhours", at(2026, 1, 13, 19, 30), calendar.Afterhours, true},
		{"20:00 after close", at(2026, 1, 13, 20, 0), 0, false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ds.SessionAt(tt.t)
			assert.Equal(t, tt.ok, ok)
			if tt.ok {
				assert.Equal(t, tt.want, got)
			}
		})
	}

	// SessionBoundsAt picks the trading day in the calendar timezone, even for
	// a UTC timestamp that falls on the next UTC day.
	late := at(2026, 1, 13, 19, 30).UTC()
	ds2, err := calendar.Nasdaq.SessionBoundsAt(late)
	require.NoError(t, err)
	assert.True(t, ds2.Date.Equal(ds.Date))
}

func TestParseSession(t *testing.T) {
	t.Parallel()
	for _, s := range calendar.Sessions {
		got, err := calendar.ParseSession(s.String())
		require.NoError(t, err)
		assert.Equal(t, s, got)
	}
	_, err := calendar.ParseSession("overnight")
	assert.Error(t, err)
}

func TestPrevMarketDay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		t    time.Time
		want time.Time
	}{
		{"tuesday -> monday", at(2026, 9, 22, 10, 0), at(2026, 9, 21, 0, 0)},
		{"monday -> friday", at(2026, 9, 21, 10, 0), at(2026, 9, 18, 0, 0)},
		// 2026-09-07 is Labor Day.
		{"after labor day -> previous friday", at(2026, 9, 8, 10, 0), at(2026, 9, 4, 0, 0)},
		{"saturday -> friday", at(2026, 9, 26, 10, 0), at(2026, 9, 25, 0, 0)},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := calendar.Nasdaq.PrevMarketDay(tt.t)
			assert.True(t, tt.want.Equal(got), "got %v want %v", got, tt.want)
		})
	}
}
