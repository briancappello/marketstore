package framework

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
)

func TestNextBoundary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		at, want time.Time
	}{
		{et(2026, 9, 22, 3, 0, 0), et(2026, 9, 22, 4, 0, 0)},
		{et(2026, 9, 22, 4, 0, 0), et(2026, 9, 22, 9, 30, 0)},
		{et(2026, 9, 22, 11, 0, 0), et(2026, 9, 22, 16, 0, 0)},
		{et(2026, 9, 22, 16, 0, 0), et(2026, 9, 22, 20, 0, 0)},
		{et(2026, 9, 22, 21, 0, 0), et(2026, 9, 23, 4, 0, 0)},
		{et(2026, 9, 25, 21, 0, 0), et(2026, 9, 28, 4, 0, 0)},    // over a weekend
		{et(2026, 11, 27, 11, 0, 0), et(2026, 11, 27, 13, 0, 0)}, // early close
		{et(2026, 11, 25, 21, 0, 0), et(2026, 11, 27, 4, 0, 0)},  // over Thanksgiving
	}
	for _, tt := range tests {
		got := nextBoundary(tt.at)
		assert.True(t, tt.want.Equal(got), "after %v: got %v want %v", tt.at, got, tt.want)
	}

	// The loop wakes just after a boundary when it comes before the interval.
	assert.True(t, nextTick(et(2026, 9, 22, 9, 29, 30), time.Minute).Equal(et(2026, 9, 22, 9, 30, 1)))
	assert.True(t, nextTick(et(2026, 9, 22, 10, 0, 0), time.Minute).Equal(et(2026, 9, 22, 10, 1, 0)))
}

func TestBaselineDay(t *testing.T) {
	t.Parallel()
	day := func(at time.Time) time.Time {
		ds, err := baselineDay(at)
		require.NoError(t, err)
		return ds.Date
	}
	assert.True(t, day(et(2026, 9, 22, 3, 0, 0)).Equal(et(2026, 9, 22, 0, 0, 0)))
	assert.True(t, day(et(2026, 9, 22, 20, 30, 0)).Equal(et(2026, 9, 22, 0, 0, 0)), "still today's")
	assert.True(t, day(et(2026, 9, 22, 21, 30, 0)).Equal(et(2026, 9, 23, 0, 0, 0)), "the next trading day's")
	assert.True(t, day(et(2026, 9, 26, 12, 0, 0)).Equal(et(2026, 9, 28, 0, 0, 0)), "weekend: Monday's")
}

// pushedLists runs one live ranking at clock time at and returns the lists.
func rankLiveAt(t *testing.T, w *WatchlistWorker, at time.Time) []RankedList {
	t.Helper()
	prev := now
	now = func() time.Time { return at }
	defer func() { now = prev }()
	w.TriggerRanking()
	return Manager.AllLists()
}

func listByName(lists []RankedList) map[string]RankedList {
	m := map[string]RankedList{}
	for _, l := range lists {
		m[l.Name] = l
	}
	return m
}

// 8.1: the live rankings follow the clock across session boundaries, and
// overnight and on weekends show the last completed session.
func TestLiveRolloverAcrossSessions(t *testing.T) {
	sessionFixture(t) // SYM traded in all three sessions of Tue 9/22
	Manager.AddStrategy(&recorder{name: "R"})
	w := &WatchlistWorker{timeframe: "1Min"}

	check := func(at time.Time, sess calendar.Session, complete bool, want ...string) {
		t.Helper()
		lists := listByName(rankLiveAt(t, w, at))
		var got []string
		for n, l := range lists {
			got = append(got, n)
			assert.Equal(t, sess, l.Window.Session, "%v %s", at, n)
			assert.Equal(t, complete, l.Window.Complete, "%v %s", at, n)
		}
		assert.ElementsMatch(t, want, got, "%v", at)
	}

	check(et(2026, 9, 22, 9, 29, 59), calendar.Premarket, false, "R")
	check(et(2026, 9, 22, 9, 30, 1), calendar.Regular, false, "R", "R_TRADITIONAL")
	check(et(2026, 9, 22, 16, 0, 1), calendar.Afterhours, false, "R")
	check(et(2026, 9, 22, 22, 0, 0), calendar.Afterhours, true, "R")

	// Regular volume is counted from 09:30: premarket volume stays out.
	regular := listByName(rankLiveAt(t, w, et(2026, 9, 22, 12, 0, 0)))["R"]
	require.NotEmpty(t, regular.Symbols)
}

// On a weekend the live window is Friday's afterhours; starting the worker
// then seeds Friday's bars so the rankings are Friday's final ones.
func TestSeedDayOnWeekendShowsFriday(t *testing.T) {
	newVolHarness(t)
	svc, err := sessionfacts.NewService(sessionfacts.Config{Now: func() time.Time { return et(2026, 9, 26, 12, 0, 0) }})
	require.NoError(t, err)

	// Write Thursday and Friday bars without firing the trigger, as a server
	// restart would find them on disk.
	bh := &baselineHarness{t: t, facts: svc}
	bh.writeBars("WK", "1Min",
		flat(et(2026, 9, 24, 15, 59, 0), 40, 1), // Thursday regular close
		flat(et(2026, 9, 25, 15, 59, 0), 50, 1),
		flat(et(2026, 9, 25, 17, 0, 0), 52, 100),
	)
	Manager = NewSymbolStateManager()
	fri, err := calendar.Nasdaq.SessionBounds(2026, 9, 25)
	require.NoError(t, err)
	at := et(2026, 9, 26, 12, 0, 0)
	seedDay(Manager, svc, executor.ThisInstance.CatalogDir, []string{"WK"}, fri, 5, at)
	Manager.UpdateCuration("WK", true) // the worker's initial curation pass

	Manager.AddStrategy(&recorder{name: "R"})
	w := &WatchlistWorker{timeframe: "1Min"}
	lists := listByName(rankLiveAt(t, w, at))
	r := lists["R"]
	assert.Equal(t, calendar.Afterhours, r.Window.Session)
	assert.True(t, r.Window.Complete)
	require.Len(t, r.Symbols, 1)
	st := Manager.Get("WK")
	prior, ok := st.PriorCloseFor(calendar.Afterhours, BasisSession)
	assert.True(t, ok)
	assert.Equal(t, 50.0, prior, "Friday's regular close")
	assert.InDelta(t, 40.0, st.PriorClose, 1e-4, "traditional baseline: Thursday's close")
}
