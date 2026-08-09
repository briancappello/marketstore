package calendar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var NY, _ = time.LoadLocation("America/New_York")

func TestCalendar(t *testing.T) {
	t.Parallel()
	// Weekend
	weekend := time.Date(2017, 1, 1, 11, 0, 0, 0, NY)
	assert.Equal(t, Nasdaq.IsMarketOpen(weekend), false)
	assert.Equal(t, Nasdaq.IsMarketDay(weekend), false)

	// MLK day 2018
	mlk := time.Date(2018, 1, 15, 11, 0, 0, 0, NY)
	assert.Equal(t, Nasdaq.IsMarketOpen(mlk), false)
	assert.Equal(t, Nasdaq.IsMarketDay(mlk), false)

	// July 3rd 2018 (early close day - regular close 1 PM, extended close 5 PM)
	julThirdAM := time.Date(2018, 7, 3, 11, 0, 0, 0, NY)
	julThirdPM := time.Date(2018, 7, 3, 16, 0, 0, 0, NY)   // 4 PM - within extended hours (1 PM + 4 hrs = 5 PM)
	julThirdLate := time.Date(2018, 7, 3, 18, 0, 0, 0, NY) // 6 PM - after extended close
	assert.True(t, Nasdaq.IsMarketOpen(julThirdAM))
	assert.True(t, Nasdaq.EpochIsMarketOpen(julThirdAM.Unix()))
	assert.True(t, Nasdaq.IsMarketDay(julThirdAM))

	assert.True(t, Nasdaq.IsMarketOpen(julThirdPM)) // Now true - within extended hours
	assert.True(t, Nasdaq.IsMarketDay(julThirdPM))

	assert.False(t, Nasdaq.IsMarketOpen(julThirdLate)) // After extended close
	assert.True(t, Nasdaq.IsMarketDay(julThirdLate))

	// normal day - extended hours: 4 AM to 8 PM
	bestDayMid := time.Date(2021, 8, 31, 11, 0, 0, 0, NY)
	bestDayPreMarket := time.Date(2021, 8, 31, 5, 0, 0, 0, NY)   // 5 AM - pre-market open
	bestDayTooEarly := time.Date(2021, 8, 31, 3, 0, 0, 0, NY)    // 3 AM - before pre-market
	bestDayAfterHours := time.Date(2021, 8, 31, 19, 0, 0, 0, NY) // 7 PM - after-hours trading
	bestDayTooLate := time.Date(2021, 8, 31, 21, 0, 0, 0, NY)    // 9 PM - after extended close

	assert.True(t, Nasdaq.IsMarketOpen(bestDayMid))
	assert.True(t, Nasdaq.IsMarketDay(bestDayMid))

	assert.True(t, Nasdaq.IsMarketOpen(bestDayPreMarket)) // 5 AM - within extended hours
	assert.True(t, Nasdaq.IsMarketDay(bestDayPreMarket))

	assert.False(t, Nasdaq.IsMarketOpen(bestDayTooEarly)) // 3 AM - before 4 AM open
	assert.True(t, Nasdaq.IsMarketDay(bestDayTooEarly))

	assert.True(t, Nasdaq.IsMarketOpen(bestDayAfterHours)) // 7 PM - within extended hours
	assert.True(t, Nasdaq.IsMarketDay(bestDayAfterHours))

	assert.False(t, Nasdaq.IsMarketOpen(bestDayTooLate)) // 9 PM - after 8 PM close
	assert.True(t, Nasdaq.IsMarketDay(bestDayTooLate))

	assert.Equal(t, Nasdaq.Tz().String(), "America/New_York")

	// Juneteenth (observed by Nasdaq since 2022).
	// June 19, 2022 = Sunday -> observed Monday June 20.
	assert.False(t, Nasdaq.IsMarketDay(time.Date(2022, 6, 20, 12, 0, 0, 0, NY)))
	// June 19, 2023 = Monday -> observed June 19.
	assert.False(t, Nasdaq.IsMarketDay(time.Date(2023, 6, 19, 12, 0, 0, 0, NY)))
	// June 19, 2024 = Wednesday -> observed June 19.
	assert.False(t, Nasdaq.IsMarketDay(time.Date(2024, 6, 19, 12, 0, 0, 0, NY)))
	// June 19, 2025 = Thursday -> observed June 19.
	assert.False(t, Nasdaq.IsMarketDay(time.Date(2025, 6, 19, 12, 0, 0, 0, NY)))
	// June 19, 2026 = Friday -> observed June 19.
	assert.False(t, Nasdaq.IsMarketDay(time.Date(2026, 6, 19, 12, 0, 0, 0, NY)))
	// June 19, 2027 = Saturday -> observed Friday June 18.
	assert.False(t, Nasdaq.IsMarketDay(time.Date(2027, 6, 18, 12, 0, 0, 0, NY)))
}

func Test_jd(t *testing.T) {
	now := time.Now()
	t.Log(julianDate(now))
	t.Log(julianDate(now.Add(24 * time.Hour)))
	t.Log(julianDate(now.AddDate(0, 1, 0)))
}

func TestLatestMarketTime(t *testing.T) {
	t.Parallel()

	// Extended hours: 4 AM to 8 PM (regular close 4 PM + 4 hours)
	// Early close days: 4 AM to 5 PM (early close 1 PM + 4 hours)
	tests := []struct {
		name     string
		now      time.Time
		expected time.Time
	}{
		{
			name:     "market currently open returns now",
			now:      time.Date(2021, 8, 31, 11, 30, 0, 0, NY), // Tuesday 11:30 AM
			expected: time.Date(2021, 8, 31, 11, 30, 0, 0, NY),
		},
		{
			name:     "during after-hours returns now",
			now:      time.Date(2021, 8, 31, 19, 0, 0, 0, NY), // Tuesday 7 PM (within extended hours)
			expected: time.Date(2021, 8, 31, 19, 0, 0, 0, NY),
		},
		{
			name:     "after extended close returns today's extended close",
			now:      time.Date(2021, 8, 31, 21, 0, 0, 0, NY), // Tuesday 9 PM (after 8 PM extended close)
			expected: time.Date(2021, 8, 31, 20, 0, 0, 0, NY), // 8 PM extended close
		},
		{
			name:     "before pre-market open returns previous day's extended close",
			now:      time.Date(2021, 9, 1, 3, 0, 0, 0, NY),   // Wednesday 3 AM (before 4 AM open)
			expected: time.Date(2021, 8, 31, 20, 0, 0, 0, NY), // Tuesday 8 PM extended close
		},
		{
			name:     "during pre-market returns now",
			now:      time.Date(2021, 9, 1, 5, 0, 0, 0, NY), // Wednesday 5 AM (pre-market)
			expected: time.Date(2021, 9, 1, 5, 0, 0, 0, NY),
		},
		{
			name:     "weekend returns Friday's extended close",
			now:      time.Date(2021, 9, 4, 12, 0, 0, 0, NY), // Saturday noon
			expected: time.Date(2021, 9, 3, 20, 0, 0, 0, NY), // Friday 8 PM extended close
		},
		{
			name:     "holiday returns previous trading day's extended close",
			now:      time.Date(2021, 9, 6, 12, 0, 0, 0, NY), // Monday noon (Labor Day - closed)
			expected: time.Date(2021, 9, 3, 20, 0, 0, 0, NY), // Friday 8 PM extended close
		},
		{
			name:     "early close day during extended hours returns now",
			now:      time.Date(2018, 7, 3, 15, 0, 0, 0, NY), // July 3rd 3 PM (within extended: 1 PM + 4 hrs = 5 PM)
			expected: time.Date(2018, 7, 3, 15, 0, 0, 0, NY),
		},
		{
			name:     "early close day after extended close returns extended close",
			now:      time.Date(2018, 7, 3, 18, 0, 0, 0, NY), // July 3rd 6 PM (after 5 PM extended close)
			expected: time.Date(2018, 7, 3, 17, 0, 0, 0, NY), // 5 PM extended close (1 PM + 4 hrs)
		},
		{
			name:     "early close day during regular hours returns now",
			now:      time.Date(2018, 7, 3, 11, 0, 0, 0, NY), // July 3rd 11 AM
			expected: time.Date(2018, 7, 3, 11, 0, 0, 0, NY),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := Nasdaq.LatestMarketTime(tt.now)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNextMarketDay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		now      time.Time
		expected time.Time
	}{
		{
			name:     "weekday to next weekday",
			now:      time.Date(2026, 4, 6, 12, 0, 0, 0, NY), // Monday
			expected: time.Date(2026, 4, 7, 12, 0, 0, 0, NY), // Tuesday
		},
		{
			name:     "Friday to Monday (skip weekend)",
			now:      time.Date(2026, 4, 3, 12, 0, 0, 0, NY), // Friday
			expected: time.Date(2026, 4, 6, 12, 0, 0, 0, NY), // Monday
		},
		{
			name:     "Saturday to Monday",
			now:      time.Date(2026, 4, 4, 12, 0, 0, 0, NY), // Saturday
			expected: time.Date(2026, 4, 6, 12, 0, 0, 0, NY), // Monday
		},
		{
			name:     "Sunday to Monday",
			now:      time.Date(2026, 4, 5, 12, 0, 0, 0, NY), // Sunday
			expected: time.Date(2026, 4, 6, 12, 0, 0, 0, NY), // Monday
		},
		{
			name:     "day before holiday skips holiday",
			now:      time.Date(2026, 7, 2, 12, 0, 0, 0, NY), // Thursday July 2
			expected: time.Date(2026, 7, 6, 12, 0, 0, 0, NY), // Monday July 6 (July 3 = holiday, 4-5 = weekend)
		},
		{
			name:     "Wednesday before Thanksgiving (Thu closed, Fri early close)",
			now:      time.Date(2026, 11, 25, 12, 0, 0, 0, NY), // Wednesday
			expected: time.Date(2026, 11, 27, 12, 0, 0, 0, NY), // Friday (early close, but still a market day)
		},
		{
			name:     "early close day is still a market day",
			now:      time.Date(2026, 11, 26, 12, 0, 0, 0, NY), // Thanksgiving (closed)
			expected: time.Date(2026, 11, 27, 12, 0, 0, 0, NY), // Fri (early close = market day)
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := Nasdaq.NextMarketDay(tt.now)
			assert.Equal(t, tt.expected.Year(), result.Year())
			assert.Equal(t, tt.expected.Month(), result.Month())
			assert.Equal(t, tt.expected.Day(), result.Day())
		})
	}
}

// TestEpochIsRegularMarketOpen pins the regular-session boundary and, crucially,
// its divergence from the extended-hours EpochIsMarketOpen. Daily OHLC
// aggregation depends on this distinction: using the extended-hours qualifier
// lets pre/post-market prints set the daily high, low and volume.
func TestEpochIsRegularMarketOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		t            time.Time
		wantRegular  bool
		wantExtended bool
	}{
		// Normal session day (Tue 2021-08-31).
		{"03:00 before pre-market", time.Date(2021, 8, 31, 3, 0, 0, 0, NY), false, false},
		{"05:00 pre-market", time.Date(2021, 8, 31, 5, 0, 0, 0, NY), false, true},
		{"09:29 just before open", time.Date(2021, 8, 31, 9, 29, 0, 0, NY), false, true},
		{"09:30 open (inclusive)", time.Date(2021, 8, 31, 9, 30, 0, 0, NY), true, true},
		{"11:00 midday", time.Date(2021, 8, 31, 11, 0, 0, 0, NY), true, true},
		{"15:59 just before close", time.Date(2021, 8, 31, 15, 59, 0, 0, NY), true, true},
		{"16:00 close (exclusive)", time.Date(2021, 8, 31, 16, 0, 0, 0, NY), false, true},
		{"19:00 after-hours", time.Date(2021, 8, 31, 19, 0, 0, 0, NY), false, true},
		{"21:00 after extended close", time.Date(2021, 8, 31, 21, 0, 0, 0, NY), false, false},

		// Early close day (2018-07-03, regular close 13:00, extended 17:00).
		{"early-close 11:00", time.Date(2018, 7, 3, 11, 0, 0, 0, NY), true, true},
		{"early-close 13:00 closed", time.Date(2018, 7, 3, 13, 0, 0, 0, NY), false, true},
		{"early-close 16:00 ext only", time.Date(2018, 7, 3, 16, 0, 0, 0, NY), false, true},

		// Non-trading days.
		{"weekend", time.Date(2017, 1, 1, 11, 0, 0, 0, NY), false, false},
		{"MLK holiday", time.Date(2018, 1, 15, 11, 0, 0, 0, NY), false, false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantRegular, Nasdaq.IsRegularMarketOpen(tt.t), "IsRegularMarketOpen")
			assert.Equal(t, tt.wantRegular, Nasdaq.EpochIsRegularMarketOpen(tt.t.Unix()),
				"EpochIsRegularMarketOpen must agree with IsRegularMarketOpen")
			assert.Equal(t, tt.wantExtended, Nasdaq.EpochIsMarketOpen(tt.t.Unix()), "EpochIsMarketOpen")
		})
	}
}

// TestRegularIsStrictSubsetOfExtended guards the invariant that makes the
// daily-bar filter meaningful: anything in the regular session is also in
// extended hours, and pre/post-market is in extended but not regular.
func TestRegularIsStrictSubsetOfExtended(t *testing.T) {
	t.Parallel()

	day := time.Date(2021, 8, 31, 0, 0, 0, 0, NY)
	var regular, extended, extendedOnly int
	for m := 0; m < 24*60; m++ {
		ts := day.Add(time.Duration(m) * time.Minute).Unix()
		r := Nasdaq.EpochIsRegularMarketOpen(ts)
		e := Nasdaq.EpochIsMarketOpen(ts)
		if r {
			regular++
			assert.True(t, e, "regular-open minute must also be extended-open")
		}
		if e {
			extended++
			if !r {
				extendedOnly++
			}
		}
	}
	assert.Equal(t, 390, regular, "regular session is 390 minutes (09:30-16:00)")
	assert.Equal(t, 960, extended, "extended session is 960 minutes (04:00-20:00)")
	assert.Equal(t, 570, extendedOnly, "570 extended-only minutes previously polluted daily bars")
}
