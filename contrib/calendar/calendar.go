// Package calendar provides market calendar, with which you can
// check if the market is open at specific point of time.
// Though the package is generalized to support different market
// calendars, only the NASDAQ is implemented at this moment.
// You can create your own calendar if you provide the calendar
// json string.  See nasdaq.go for the format.
package calendar

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

type MarketState int

const (
	Closed MarketState = iota
	EarlyClose
)

type Time struct {
	hour, minute, second int
}

// extendedHoursOffset is added to the regular close time to get the extended hours close.
// Extended hours run from 4 AM to (regular close + 4 hours), e.g., 4 AM to 8 PM on normal days.
const extendedHoursOffset = 4 * time.Hour

type Calendar struct {
	days           map[int]MarketState
	tz             *time.Location
	openTime       Time // Pre-market open (e.g., 4:00 AM for extended hours)
	closeTime      Time // Regular market close (e.g., 4:00 PM)
	earlyCloseTime Time // Early close time (e.g., 1:00 PM)
}

type calendarJSON struct {
	NonTradingDays []string `json:"non_trading_days"`
	EarlyCloses    []string `json:"early_closes"`
	Timezone       string   `json:"timezone"`
	OpenTime       string   `json:"open_time"`
	CloseTime      string   `json:"close_time"`
	EarlyCloseTime string   `json:"early_close_time"`
}

// Nasdaq implements market calendar for the NASDAQ.
var Nasdaq = New(NasdaqJSON)

func julianDate(t time.Time) int {
	// Note: Date() is faster than calling Hour(), Month(), and Day() separately
	year, m, day := t.Date()
	month := int(m)
	// nolint:gomnd // well-known algorithm to calculate julian date number
	return day - 32075 + 1461*(year+4800+(month-14)/12)/4 + 367*(month-2-(month-14)/12*12)/12 -
		3*((year+4900+(month-14)/12)/100)/4
}

func ParseTime(tstr string) Time {
	seps := strings.Split(tstr, ":")
	h, _ := strconv.Atoi(seps[0])
	m, _ := strconv.Atoi(seps[1])
	s, _ := strconv.Atoi(seps[2])
	return Time{h, m, s}
}

func New(calendarJSONStr string) *Calendar {
	cal := Calendar{days: map[int]MarketState{}}
	cmap := calendarJSON{}
	err := json.Unmarshal([]byte(calendarJSONStr), &cmap)
	if err != nil {
		log.Error(fmt.Sprintf("failed to unmarshal calendarJson:%s", calendarJSONStr))
		return nil
	}
	for _, dateString := range cmap.NonTradingDays {
		t, _ := time.Parse("2006-01-02", dateString)
		cal.days[julianDate(t)] = Closed
	}
	for _, dateString := range cmap.EarlyCloses {
		t, _ := time.Parse("2006-01-02", dateString)
		cal.days[julianDate(t)] = EarlyClose
	}
	cal.tz, _ = time.LoadLocation(cmap.Timezone)
	cal.openTime = ParseTime(cmap.OpenTime)
	cal.closeTime = ParseTime(cmap.CloseTime)
	cal.earlyCloseTime = ParseTime(cmap.EarlyCloseTime)
	return &cal
}

// IsMarketDay check if today is a trading day or not.
func (calendar *Calendar) IsMarketDay(t time.Time) bool {
	if t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
		return false
	}
	if state, ok := calendar.days[julianDate(t)]; ok {
		return state != Closed
	}
	return true
}

// EpochIsMarketOpen returns true if epoch in calendar's timezone is in the market hours.
func (calendar *Calendar) EpochIsMarketOpen(epoch int64) bool {
	t := time.Unix(epoch, 0).In(calendar.tz)
	return calendar.IsMarketOpen(t)
}

// IsMarketOpen returns true if t is in the extended market hours.
// Extended hours run from pre-market open (4 AM) to regular close + 4 hours (8 PM normally).
func (calendar *Calendar) IsMarketOpen(t time.Time) bool {
	wd := t.Weekday()
	if wd == time.Saturday || wd == time.Sunday {
		return false
	}

	year, month, day := t.Date()
	ot := calendar.openTime
	open := time.Date(year, month, day, ot.hour, ot.minute, ot.second, 0, calendar.tz)

	if state, ok := calendar.days[julianDate(t)]; ok {
		switch state {
		case EarlyClose:
			// Extended close = early close time + 4 hours
			et := calendar.earlyCloseTime
			regularClose := time.Date(year, month, day, et.hour, et.minute, et.second, 0, calendar.tz)
			extendedClose := regularClose.Add(extendedHoursOffset)
			if t.Before(open) || t.Equal(extendedClose) || t.After(extendedClose) {
				return false
			}
			return true
		default: // case Closed:
			return false
		}
	} else {
		// Extended close = regular close time + 4 hours
		ct := calendar.closeTime
		regularClose := time.Date(year, month, day, ct.hour, ct.minute, ct.second, 0, calendar.tz)
		extendedClose := regularClose.Add(extendedHoursOffset)
		if t.Before(open) || t.Equal(extendedClose) || t.After(extendedClose) {
			return false
		}
		return true
	}
}

// EpochMarketClose determines the market close time of the day that
// the supplied epoch timestamp occurs on. Returns nil if it is not
// a market day.
func (calendar *Calendar) EpochMarketClose(epoch int64) *time.Time {
	t := time.Unix(epoch, 0).In(calendar.tz)
	return calendar.MarketClose(t)
}

// MarketClose determines the market close time of the day that the
// supplied timestamp occurs on. Returns nil if it is not a market day.
func (calendar *Calendar) MarketClose(t time.Time) *time.Time {
	var mktClose *time.Time
	if state, ok := calendar.days[julianDate(t)]; ok {
		switch state {
		case EarlyClose:
			earlyClose := time.Date(
				t.Year(), t.Month(), t.Day(),
				calendar.earlyCloseTime.hour,
				calendar.earlyCloseTime.minute,
				calendar.earlyCloseTime.second,
				0, calendar.tz)

			mktClose = &earlyClose
		case Closed:
			return mktClose
		default:
			normalClose := time.Date(
				t.Year(), t.Month(), t.Day(),
				calendar.closeTime.hour,
				calendar.closeTime.minute,
				calendar.closeTime.second,
				0, calendar.tz)

			mktClose = &normalClose
		}
	}
	return mktClose
}

func (calendar *Calendar) Tz() *time.Location {
	return calendar.tz
}

// LatestMarketTime returns the most recent time when the market was or is open.
// If the market is currently open (including extended hours), it returns the current time.
// If the market is closed, it returns the extended hours close time of the most recent trading day.
func (calendar *Calendar) LatestMarketTime(now time.Time) time.Time {
	return calendar.latestMarketTime(now, true)
}

// LatestMarketTimeRegular returns the most recent time when the regular market session was or is open.
// Unlike LatestMarketTime, this uses regular market hours (9:30 AM - 4 PM) instead of extended hours.
// Use this for daily or longer timeframes where extended hours data is not relevant.
func (calendar *Calendar) LatestMarketTimeRegular(now time.Time) time.Time {
	return calendar.latestMarketTime(now, false)
}

// latestMarketTime is the internal implementation that supports both extended and regular hours.
func (calendar *Calendar) latestMarketTime(now time.Time, includeExtendedHours bool) time.Time {
	now = now.In(calendar.tz)

	// If market is currently open, return now.
	if includeExtendedHours {
		if calendar.IsMarketOpen(now) {
			return now
		}
	} else {
		if calendar.IsRegularMarketOpen(now) {
			return now
		}
	}

	// Helper to get close time for a given day.
	getCloseTime := func(t time.Time) time.Time {
		year, month, day := t.Date()
		var closeTime time.Time
		if state, ok := calendar.days[julianDate(t)]; ok && state == EarlyClose {
			closeTime = time.Date(year, month, day,
				calendar.earlyCloseTime.hour,
				calendar.earlyCloseTime.minute,
				calendar.earlyCloseTime.second,
				0, calendar.tz)
		} else {
			closeTime = time.Date(year, month, day,
				calendar.closeTime.hour,
				calendar.closeTime.minute,
				calendar.closeTime.second,
				0, calendar.tz)
		}
		if includeExtendedHours {
			closeTime = closeTime.Add(extendedHoursOffset)
		}
		return closeTime
	}

	// Check if today is a market day and we're past close.
	if calendar.IsMarketDay(now) {
		closeTime := getCloseTime(now)
		if now.After(closeTime) || now.Equal(closeTime) {
			// Today was a market day and market already closed.
			return closeTime
		}
	}

	// Walk backwards to find the most recent market day.
	// Start from yesterday since today either isn't a market day or hasn't opened yet.
	const maxDaysBack = 10 // Should never need more than this (worst case: holiday + weekend)
	for i := 1; i <= maxDaysBack; i++ {
		day := now.AddDate(0, 0, -i)
		if calendar.IsMarketDay(day) {
			return getCloseTime(day)
		}
	}

	// Fallback: return now if we somehow can't find a recent market day.
	return now
}

// IsRegularMarketOpen returns true if t is within regular market hours (9:30 AM - 4 PM ET).
func (calendar *Calendar) IsRegularMarketOpen(now time.Time) bool {
	wd := now.Weekday()
	if wd == time.Saturday || wd == time.Sunday {
		return false
	}

	year, month, day := now.Date()

	// Regular market opens at 9:30 AM.
	open := time.Date(year, month, day, 9, 30, 0, 0, calendar.tz)

	if state, ok := calendar.days[julianDate(now)]; ok {
		switch state {
		case EarlyClose:
			et := calendar.earlyCloseTime
			close := time.Date(year, month, day, et.hour, et.minute, et.second, 0, calendar.tz)
			return !now.Before(open) && now.Before(close)
		default: // Closed
			return false
		}
	}

	// Normal day: regular hours are 9:30 AM to 4:00 PM.
	ct := calendar.closeTime
	close := time.Date(year, month, day, ct.hour, ct.minute, ct.second, 0, calendar.tz)
	return !now.Before(open) && now.Before(close)
}
