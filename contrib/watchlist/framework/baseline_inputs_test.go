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

// withReplicaFacts installs a read-only (replica) facts service and a clock.
func withReplicaFacts(t *testing.T, clock *time.Time) {
	t.Helper()
	svc, err := sessionfacts.NewService(sessionfacts.Config{Now: func() time.Time { return *clock }})
	require.NoError(t, err)
	prevFacts, prevNow := Facts, now
	Facts, now = svc, func() time.Time { return *clock }
	t.Cleanup(func() { Facts, now = prevFacts, prevNow })
	drainChangedInputs()
	t.Cleanup(func() { drainChangedInputs() })
}

func traditionalPrior(st *SymbolState) float64 {
	p, _ := st.PriorCloseFor(calendar.Regular, BasisTraditional)
	return p
}

// The 2026-11-25 incident: baselines for 11-27 were loaded while 11-25's 1D
// bar still held the ondiskagg close (the 15:59 1Min close, 8.13); the
// official close (8.15) landed afterwards and was never picked up, so the
// traditional lists ranked against a stale prior close all of 11-27. A 1D
// write for a settled date now reloads the baselines that depend on it.
func TestBaselinesFollowARewrittenDailyClose(t *testing.T) {
	h := newVolHarness(t)
	clock := et(2026, 11, 25, 21, 50, 0)
	withReplicaFacts(t, &clock)
	w := &WatchlistWorker{config: WorkerConfig{MedianWindow: 5}}

	h.write("BNAI", "1Min", flat(et(2026, 11, 25, 15, 59, 0), 8.13, 1000))
	h.write("BNAI", "1D", flat(et(2026, 11, 25, 0, 0, 0), 8.13, 1000)) // the ondiskagg close
	next, err := calendar.Nasdaq.SessionBounds(2026, 11, 27)
	require.NoError(t, err)
	loadBaselines(Manager, Facts, executor.ThisInstance.CatalogDir, []string{"BNAI"}, next, 5, clock)
	w.baselineDate = next.Date
	drainChangedInputs() // the writes above came before the load

	// The REST backfill writes the official close a second later.
	clock = clock.Add(time.Second)
	h.write("BNAI", "1D", flat(et(2026, 11, 25, 0, 0, 0), 8.15, 1000))
	w.reloadChangedBaselines()
	w.baselineMu.Lock() // wait for the background reload
	w.baselineMu.Unlock()

	// 11-27's first premarket bar rolls the day onto the reloaded baselines.
	clock = et(2026, 11, 27, 4, 0, 0)
	h.write("BNAI", "1Min", flat(et(2026, 11, 27, 4, 0, 0), 8.0, 10))
	h.write("BNAI", "1Min", flat(et(2026, 11, 27, 9, 30, 0), 7.8, 10))
	assert.InDelta(t, 8.15, traditionalPrior(Manager.Get("BNAI")), 1e-4,
		"the traditional prior close is the official close written after the load")

	// Later corrections to the prior date apply to the live day at once.
	clock = et(2026, 11, 27, 9, 31, 0)
	h.write("BNAI", "1D", flat(et(2026, 11, 25, 0, 0, 0), 8.16, 1000))
	w.reloadChangedBaselines()
	w.baselineMu.Lock()
	w.baselineMu.Unlock()
	assert.InDelta(t, 8.16, traditionalPrior(Manager.Get("BNAI")), 1e-4)
}

// A date's own 1D bar is rewritten all day while it forms (ondiskagg, every
// minute): those writes must not trigger reloads until the date is over.
func TestDailyBarOfTheDayInProgressIsNotAnInput(t *testing.T) {
	h := newVolHarness(t)
	clock := et(2026, 11, 30, 11, 0, 0)
	withReplicaFacts(t, &clock)

	h.write("LIVE", "1D", flat(et(2026, 11, 30, 0, 0, 0), 10, 1))
	assert.Empty(t, drainChangedInputs(), "the day in progress")

	clock = et(2026, 11, 30, 20, 0, 0) // afterhours has ended
	h.write("LIVE", "1D", flat(et(2026, 11, 30, 0, 0, 0), 10.5, 1))
	assert.Equal(t, []string{"LIVE"}, drainChangedInputs())

	clock = et(2026, 12, 1, 3, 0, 0) // the overnight backfill of the day before
	h.write("LIVE", "1D", flat(et(2026, 11, 30, 0, 0, 0), 10.6, 1))
	assert.Equal(t, []string{"LIVE"}, drainChangedInputs())
}

// A replica's rewind cache must drop results whose 1D inputs changed: its
// facts service never reports changes, as it does not write them.
func TestRewindCacheDropsDatesWhose1DInputsChanged(t *testing.T) {
	h := newVolHarness(t)
	clock := et(2026, 11, 28, 6, 0, 0)
	withReplicaFacts(t, &clock)
	r := newRewinder(5, nil, nil)
	r.store("11-24", et(2026, 11, 24, 0, 0, 0).Unix(), nil)
	r.store("11-27", et(2026, 11, 27, 0, 0, 0).Unix(), nil)

	h.write("RW", "1D", flat(et(2026, 11, 25, 0, 0, 0), 1, 1))
	_, kept := r.cached("11-24")
	_, dropped := r.cached("11-27")
	assert.True(t, kept, "a date before the change keeps its result")
	assert.False(t, dropped, "a date after the change, whose baselines use it, is recomputed")
}
