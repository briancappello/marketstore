package framework

import (
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// Baselines are the values a trading date's rankings compare against, all
// derived from data before that date (except OfficialClose, which is the
// date's own official close once known). See the ranking-basis spec.
//
// A zero value means "no data": the symbol drops out of rankings that need
// it (no walk-back to earlier dates).
type Baselines struct {
	// Date is midnight of the trading date the baselines are for.
	Date time.Time
	// PriorClose is RC(previous trading date): the traditional baseline.
	PriorClose float64
	// PrevAfterhoursClose is the previous trading date's last afterhours
	// close: the session baseline for premarket.
	PrevAfterhoursClose float64
	// OfficialClose is RC(Date) when already known: the date's 1D close, or
	// for a completed date its last regular close. Zero while the date is in
	// progress and its 1D bar has not landed; callers then use the live
	// regular-session last price.
	OfficialClose float64
	// SessionMedianVolume is the median of each session's volume over the
	// median window of trading dates before Date, indexed by
	// calendar.Session. Only dates the symbol traded count.
	SessionMedianVolume [len(calendar.Sessions)]float64
}

// LoadBaselines loads symbol's baselines for trading date day from 1D bars
// and session facts. medianWindow is the number of trading dates the volume
// medians cover.
func LoadBaselines(facts *sessionfacts.Service, catDir *catalog.Directory, symbol string,
	day calendar.DaySessions, medianWindow int, at time.Time,
) (Baselines, error) {
	b := Baselines{Date: day.Date}
	if medianWindow < 1 {
		medianWindow = 1
	}

	// The median window: medianWindow trading dates before day, newest
	// first. The first is the previous trading date P.
	prior := make([]calendar.DaySessions, 0, medianWindow)
	d := day.Date
	for len(prior) < medianWindow {
		d = calendar.Nasdaq.PrevMarketDay(d)
		ds, err := calendar.Nasdaq.SessionBoundsAt(d)
		if err != nil {
			continue
		}
		prior = append(prior, ds)
	}
	prev := prior[0]

	// One facts read covers P, the median window and, when day is over, day.
	want := prior
	complete := !at.Before(day.Afterhours.End)
	if complete {
		want = append(append([]calendar.DaySessions(nil), prior...), day)
	}
	rows, err := facts.Rows(symbol, want)
	if err != nil {
		return b, err
	}
	prevRow := rows[0]

	daily, err := readDailyCloses(catDir, symbol, prev.Date, day.Date)
	if err != nil {
		return b, err
	}

	// RC(P): the official close, falling back to the last regular close.
	if c, ok := daily[prev.Date.Unix()]; ok {
		b.PriorClose = c
	} else if c, ok := prevRow.Close(calendar.Regular); ok {
		b.PriorClose = c
	}
	if c, ok := prevRow.Close(calendar.Afterhours); ok {
		b.PrevAfterhoursClose = c
	}
	// RC(day), when known.
	if c, ok := daily[day.Date.Unix()]; ok {
		b.OfficialClose = c
	} else if complete {
		if c, ok := rows[len(rows)-1].Close(calendar.Regular); ok {
			b.OfficialClose = c
		}
	}

	// Per-session medians over dates the symbol traded.
	for _, s := range calendar.Sessions {
		vals := make([]float64, 0, len(prior))
		for _, r := range rows[:len(prior)] {
			if r.Traded() {
				vals = append(vals, float64(r.Volume(s)))
			}
		}
		b.SessionMedianVolume[s] = median(vals)
	}
	return b, nil
}

// readDailyCloses returns symbol's 1D closes from..to (inclusive), keyed by
// midnight America/New_York of each bar's date.
func readDailyCloses(catDir *catalog.Directory, symbol string, from, to time.Time) (map[int64]float64, error) {
	tbk := io.NewTimeBucketKey(symbol + "/1D/OHLCV")
	if _, err := catDir.GetLatestTimeBucketInfoFromKey(tbk); err != nil {
		return nil, nil // no daily bars for this symbol
	}
	q := planner.NewQuery(catDir)
	q.AddTargetKey(tbk)
	q.SetRange(from, to.Add(24*time.Hour-time.Second))
	parsed, err := q.Parse()
	if err != nil {
		return nil, nil // nothing in range
	}
	reader, err := executor.NewReader(parsed)
	if err != nil {
		return nil, err
	}
	csm, err := reader.Read()
	if err != nil {
		return nil, err
	}
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return nil, nil
	}
	closes := column(cs, "Close")
	out := make(map[int64]float64, cs.Len())
	for i, e := range cs.GetEpoch() {
		if i < len(closes) && closes[i] != 0 {
			out[dailyDate(e).Unix()] = closes[i]
		}
	}
	return out, nil
}

// dailyDate converts a daily bar's epoch to midnight America/New_York of its
// date. Daily bars are indexed at midnight of the server timezone, so the
// date is read in that timezone.
func dailyDate(epoch int64) time.Time {
	y, m, d := time.Unix(epoch, 0).In(utils.InstanceConfig.Timezone).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, calendar.Nasdaq.Tz())
}
