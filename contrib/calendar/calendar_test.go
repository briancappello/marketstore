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
