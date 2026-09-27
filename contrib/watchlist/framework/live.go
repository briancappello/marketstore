package framework

import (
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// baselineRefreshDelay is how long after a trading day's afterhours ends the
// worker loads the next trading day's baselines. It leaves time for the
// session facts job (default grace 30m) to write the day's facts first; if
// it has not, the facts fall back to the 1Min bars, which is correct but
// slower.
const baselineRefreshDelay = time.Hour

// executorCatalog returns the running instance's catalog.
func executorCatalog() *catalog.Directory { return executor.ThisInstance.CatalogDir }

// readDayBars returns symbol's 1Min bars with start <= epoch < end.
func readDayBars(catDir *catalog.Directory, symbol string, start, end time.Time) []bar {
	tbk := io.NewTimeBucketKey(sessionfacts.SourceKey(symbol))
	if _, err := catDir.GetLatestTimeBucketInfoFromKey(tbk); err != nil {
		return nil
	}
	q := planner.NewQuery(catDir)
	q.AddTargetKey(tbk)
	q.SetRange(start, end.Add(-time.Second))
	parsed, err := q.Parse()
	if err != nil {
		return nil
	}
	reader, err := executor.NewReader(parsed)
	if err != nil {
		return nil
	}
	csm, err := reader.Read()
	if err != nil {
		return nil
	}
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return nil
	}
	bars := barsFromColumnSeries(cs)
	out := bars[:0]
	for _, b := range bars {
		if b.epoch >= start.Unix() && b.epoch < end.Unix() {
			out = append(out, b)
		}
	}
	return out
}

// seedDay positions every symbol's state on trading day day as of at: it
// loads the day's baselines and folds the day's 1Min bars up to at. The
// live session is then ranked from real session data whether the server
// starts during a session, after the close, or on a weekend (when the live
// window is the previous trading day's afterhours).
func seedDay(mgr *SymbolStateManager, facts *sessionfacts.Service, catDir *catalog.Directory,
	symbols []string, day calendar.DaySessions, medianWindow int, at time.Time,
) {
	end := day.Afterhours.End
	if at.Before(end) {
		end = at
	}
	seeded := 0
	for _, symbol := range symbols {
		st := mgr.GetOrCreate(symbol)
		if facts != nil {
			b, err := LoadBaselines(facts, catDir, symbol, day, medianWindow, at)
			if err != nil {
				log.Debug("[watchlist] baselines for %s: %v", symbol, err)
			} else {
				st.SetBaselines(b)
			}
		}
		if bars := readDayBars(catDir, symbol, day.Premarket.Start, end); len(bars) > 0 {
			st.applyBars(bars, false)
			seeded++
		}
	}
	log.Info("[watchlist] seeded %d of %d symbols from %s 1Min bars",
		seeded, len(symbols), day.Date.Format("2006-01-02"))
}

// loadBaselines loads baselines for trading day day for every symbol. States
// already on that day apply them at once; states still on an earlier day
// hold them until their first bar of day.
func loadBaselines(mgr *SymbolStateManager, facts *sessionfacts.Service, catDir *catalog.Directory,
	symbols []string, day calendar.DaySessions, medianWindow int, at time.Time,
) {
	start := time.Now()
	for _, symbol := range symbols {
		b, err := LoadBaselines(facts, catDir, symbol, day, medianWindow, at)
		if err != nil {
			log.Debug("[watchlist] baselines for %s: %v", symbol, err)
			continue
		}
		mgr.GetOrCreate(symbol).SetBaselines(b)
	}
	log.Info("[watchlist] loaded %s baselines for %d symbols in %v",
		day.Date.Format("2006-01-02"), len(symbols), time.Since(start).Round(time.Millisecond))
}

// baselineDay returns the trading day whose baselines the live states should
// hold at time t: today's until an hour after its afterhours ends, then the
// next trading day's (so they are in place before its premarket opens).
func baselineDay(t time.Time) (calendar.DaySessions, error) {
	if ds, err := calendar.Nasdaq.SessionBoundsAt(t); err == nil &&
		t.Before(ds.Afterhours.End.Add(baselineRefreshDelay)) {
		return ds, nil
	}
	return calendar.Nasdaq.SessionBoundsAt(calendar.Nasdaq.NextMarketDay(t))
}

// nextBoundary returns the first session boundary strictly after t: a
// premarket, regular or afterhours start, or an afterhours end.
func nextBoundary(t time.Time) time.Time {
	day := t
	for i := 0; i < 12; i++ {
		if ds, err := calendar.Nasdaq.SessionBoundsAt(day); err == nil {
			for _, b := range []time.Time{
				ds.Premarket.Start, ds.Regular.Start, ds.Afterhours.Start, ds.Afterhours.End,
			} {
				if b.After(t) {
					return b
				}
			}
		}
		y, m, d := day.In(calendar.Nasdaq.Tz()).Date()
		day = time.Date(y, m, d+1, 12, 0, 0, 0, calendar.Nasdaq.Tz())
	}
	return t.Add(24 * time.Hour)
}

// nextTick returns when the ranking loop should run next after t: after
// interval, or just past the next session boundary if that comes first, so
// the live rankings switch sessions at the boundary.
func nextTick(t time.Time, interval time.Duration) time.Time {
	next := t.Add(interval)
	if b := nextBoundary(t).Add(time.Second); b.Before(next) {
		return b
	}
	return next
}
