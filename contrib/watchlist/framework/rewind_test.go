package framework

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework/session"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

// pctRanker ranks every symbol by PctChange, descending, and reports the
// values a real strategy would.
type pctRanker struct{ name string }

func (p *pctRanker) Name() string                           { return p.name }
func (p *pctRanker) Configure(map[string]interface{}) error { return nil }
func (p *pctRanker) Rank(c map[string]*SymbolState) []RankedSymbol {
	syms := make([]string, 0, len(c))
	for s := range c {
		syms = append(syms, s)
	}
	sort.Slice(syms, func(i, j int) bool {
		if c[syms[i]].PctChange != c[syms[j]].PctChange {
			return c[syms[i]].PctChange > c[syms[j]].PctChange
		}
		return syms[i] < syms[j]
	})
	out := make([]RankedSymbol, len(syms))
	for i, s := range syms {
		st := c[s]
		out[i] = RankedSymbol{Symbol: s, Fields: []Field{
			{Key: "pct_change", Value: st.PctChange},
			{Key: "volume", Value: float64(st.CumulativeVolume)},
			{Key: "volume_multiple", Value: st.VolumeMultipleOfMed},
		}}
	}
	return out
}

func registerPct(t *testing.T) {
	t.Helper()
	ResetRegistry()
	RegisterWatchlist("PCT", func(map[string]interface{}) (WatchlistStrategy, error) {
		return &pctRanker{name: "PCT"}, nil
	})
	t.Cleanup(ResetRegistry)
}

// rewindFixture: history for 9/21 on disk, then 9/22 bars through the live
// trigger up to 11:00, with the live states seeded as the worker does.
func rewindFixture(t *testing.T) (*volHarness, *time.Time, *sessionfacts.Service) {
	h := newVolHarness(t)
	clock := et(2026, 9, 22, 3, 0, 0)
	facts := withLeaderFacts(t, &clock)
	registerPct(t)

	bh := &baselineHarness{t: t, facts: facts}
	for i, sym := range []string{"AAA", "BBB", "CCC"} {
		p := float32(10 * (i + 1))
		bh.writeBars(sym, "1Min",
			flat(et(2026, 9, 21, 8, 0, 0), p, 1_000),
			flat(et(2026, 9, 21, 12, 0, 0), p+1, 50_000),
			flat(et(2026, 9, 21, 18, 0, 0), p+2, 500),
		)
		bh.writeBars(sym, "1D", flat(et(2026, 9, 21, 0, 0, 0), p+1.05, 60_000))
		// That write was dispatched too; skip it so the harness pairs its
		// own writes with their own dispatches.
		h.fires[sym+"/1Min/OHLCV/2026.bin"]++
	}
	d, err := calendar.Nasdaq.SessionBounds(2026, 9, 22)
	require.NoError(t, err)
	seedDay(Manager, facts, executor.ThisInstance.CatalogDir, []string{"AAA", "BBB", "CCC"}, d, 5, clock)

	moves := map[string][]float32{"AAA": {12.5, 13, 14}, "BBB": {22, 21, 20}, "CCC": {32, 33.5, 35}}
	for sym, ps := range moves {
		h.write(sym, "1Min",
			flat(et(2026, 9, 22, 7, 0, 0), ps[0], 2_000),
			flat(et(2026, 9, 22, 9, 45, 0), ps[1], 40_000),
			flat(et(2026, 9, 22, 10, 30, 0), ps[2], 30_000),
		)
	}
	clock = et(2026, 9, 22, 11, 0, 0)
	return h, &clock, facts
}

func sortLists(lists []RankedList) []RankedList {
	out := append([]RankedList(nil), lists...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// 9.1: for the same bars, a rewind equals the live ranking at the window end.
func TestRewindEqualsLive(t *testing.T) {
	_, clock, _ := rewindFixture(t)
	Manager.AddStrategy(&pctRanker{name: "PCT"})

	live, err := session.Resolve(session.Query{}, *clock)
	require.NoError(t, err)
	liveLists := RunRankings(Manager, live)
	require.Len(t, liveLists, 2, "PCT and PCT_TRADITIONAL")
	for _, l := range liveLists {
		require.Len(t, l.Symbols, 3, l.Name)
	}

	w := &WatchlistWorker{timeframe: "1Min", config: WorkerConfig{MedianWindow: 5}}
	reg := calendar.Regular
	win, err := session.Resolve(session.Query{Session: &reg, AsOf: session.At(*clock)}, *clock)
	require.NoError(t, err)
	rewound, err := w.rewinder().rankingsAt(win)
	require.NoError(t, err)

	assert.Equal(t, sortLists(liveLists), sortLists(rewound))
}

// A rewind of an earlier session reads only that date: the regular session
// of 9/22 at 09:50 has only the 09:45 bar.
func TestRewindPartialWindow(t *testing.T) {
	_, clock, _ := rewindFixture(t)
	w := &WatchlistWorker{timeframe: "1Min", config: WorkerConfig{MedianWindow: 5}}
	res, err := w.RankingsFor(RankingQuery{Names: []string{"PCT_TRADITIONAL"}, Session: "regular", AsOf: "2026-09-22T09:50"})
	require.NoError(t, err)
	require.Len(t, res.Lists, 1)
	assert.False(t, res.Window.Complete)
	var aaa RankedSymbol
	for _, s := range res.Lists[0].Symbols {
		if s.Symbol == "AAA" {
			aaa = s
		}
	}
	require.Equal(t, "AAA", aaa.Symbol)
	for _, f := range aaa.Fields {
		switch f.Key {
		case "price":
			assert.InDelta(t, 13.0, f.Value, 1e-4, "the 09:45 close, not the 10:30 one")
		case "volume":
			assert.Equal(t, 40_000.0, f.Value)
		case "prior_close":
			assert.InDelta(t, 11.05, f.Value, 1e-4, "9/21's official (1D) close")
		}
	}
	_ = clock
}

func TestRankingsNameErrors(t *testing.T) {
	rewindFixture(t)
	w := &WatchlistWorker{timeframe: "1Min", config: WorkerConfig{MedianWindow: 5}}
	_, err := w.RankingsFor(RankingQuery{Names: []string{"NOPE"}})
	assert.ErrorIs(t, err, ErrUnknownList)
	_, err = w.RankingsFor(RankingQuery{Names: []string{"PCT_TRADITIONAL"}, Session: "afterhours", AsOf: "2026-09-21"})
	assert.ErrorIs(t, err, session.ErrNotInSession)
	_, err = w.RankingsFor(RankingQuery{AsOf: "2026-09-26T10:00"})
	assert.ErrorIs(t, err, session.ErrNotTradingDay)
}

// 9.2: identical concurrent requests compute once; complete windows are
// cached until the facts of their date change.
func TestRewindSingleFlightAndCache(t *testing.T) {
	_, clock, facts := rewindFixture(t)
	w := &WatchlistWorker{timeframe: "1Min", config: WorkerConfig{MedianWindow: 5}}
	r := w.rewinder()
	var computes atomic.Int64
	r.computeHook = func(session.Window) {
		computes.Add(1)
		time.Sleep(50 * time.Millisecond)
	}
	win, err := session.Resolve(session.Query{AsOf: session.OnDate(et(2026, 9, 21, 0, 0, 0))}, *clock)
	require.NoError(t, err)
	require.True(t, win.Complete)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := r.rankingsAt(win)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(1), computes.Load(), "five identical requests, one computation")

	_, err = r.rankingsAt(win)
	require.NoError(t, err)
	assert.Equal(t, int64(1), computes.Load(), "served from the cache")

	// A gap fill for 9/21 is recomputed into the facts; the cached result
	// for 9/21 goes.
	require.NoError(t, facts.Recompute([]sessionfacts.Entry{{Symbol: "AAA", Date: et(2026, 9, 21, 0, 0, 0)}}))
	_, err = r.rankingsAt(win)
	require.NoError(t, err)
	assert.Equal(t, int64(2), computes.Load(), "recomputed after the facts changed")
}

// A rewind in progress never delays the live ranking loop.
func TestRewindDoesNotBlockLive(t *testing.T) {
	_, clock, _ := rewindFixture(t)
	Manager.AddStrategy(&pctRanker{name: "PCT"})
	w := &WatchlistWorker{timeframe: "1Min", config: WorkerConfig{MedianWindow: 5}}
	r := w.rewinder()
	release := make(chan struct{})
	started := make(chan struct{})
	r.computeHook = func(session.Window) {
		close(started)
		<-release
	}
	win, err := session.Resolve(session.Query{AsOf: session.OnDate(et(2026, 9, 21, 0, 0, 0))}, *clock)
	require.NoError(t, err)
	rewound := make(chan struct{})
	go func() {
		defer close(rewound)
		_, _ = r.rankingsAt(win)
	}()
	<-started

	done := make(chan struct{})
	go func() {
		w.TriggerRanking()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the live ranking waited for the rewind")
	}
	close(release)
	<-rewound
	assert.NotEmpty(t, Manager.AllLists(), "live lists were produced")
}

// 10.1/10.4 at the plugin boundary: errors carry the bgworker class the host
// maps to a status, and a listing holds only the lists of the session.
func TestBoundaryRankingsClassifyAndFilter(t *testing.T) {
	rewindFixture(t)
	w := &WatchlistWorker{timeframe: "1Min", config: WorkerConfig{MedianWindow: 5}}

	_, err := w.Rankings(bgworker.WatchlistQuery{Names: []string{"NOPE"}})
	assert.ErrorIs(t, err, bgworker.ErrWatchlistNotFound)
	_, err = w.Rankings(bgworker.WatchlistQuery{AsOf: "2026-09-26"})
	assert.ErrorIs(t, err, bgworker.ErrWatchlistInvalid)
	_, err = w.Rankings(bgworker.WatchlistQuery{Session: "overnight"})
	assert.ErrorIs(t, err, bgworker.ErrWatchlistInvalid)
	_, err = w.Rankings(bgworker.WatchlistQuery{Names: []string{"PCT_TRADITIONAL"}, Session: "afterhours", AsOf: "2026-09-21"})
	assert.ErrorIs(t, err, bgworker.ErrWatchlistInvalid)

	post, err := w.Rankings(bgworker.WatchlistQuery{Session: "afterhours", AsOf: "2026-09-21"})
	require.NoError(t, err)
	require.Len(t, post.Lists, 1)
	assert.Equal(t, "PCT", post.Lists[0].Name)
	assert.Equal(t, "afterhours", post.Lists[0].Session)
	assert.Equal(t, "2026-09-21", post.Lists[0].TradingDate)
	assert.True(t, post.Lists[0].Complete)

	reg, err := w.Rankings(bgworker.WatchlistQuery{Session: "regular", AsOf: "2026-09-21"})
	require.NoError(t, err)
	var names []string
	for _, l := range reg.Lists {
		names = append(names, l.Name+"/"+l.Basis)
	}
	assert.Equal(t, []string{"PCT/session", "PCT_TRADITIONAL/traditional"}, names)
}

// tradOnlyRanker is traditional-only, like GAP and SMA_CROSS.
type tradOnlyRanker struct{ pctRanker }

func (tradOnlyRanker) Bases() []Basis { return []Basis{BasisTraditional} }

// A traditional-only strategy publishes only NAME_TRADITIONAL: its bare
// name is unknown (GET /v1/watchlists/GAP_UP is a 404), and its
// traditional list is unavailable outside the regular session.
func TestTraditionalOnlyBareNameIsUnknown(t *testing.T) {
	rewindFixture(t)
	RegisterWatchlist("GAPX", func(map[string]interface{}) (WatchlistStrategy, error) {
		return &tradOnlyRanker{pctRanker{name: "GAPX"}}, nil
	})
	w := &WatchlistWorker{timeframe: "1Min", config: WorkerConfig{MedianWindow: 5}}

	_, err := w.RankingsFor(RankingQuery{Names: []string{"GAPX"}, AsOf: "2026-09-21"})
	assert.ErrorIs(t, err, ErrUnknownList)
	_, err = w.RankingsFor(RankingQuery{Names: []string{"GAPX_TRADITIONAL"}, Session: "afterhours", AsOf: "2026-09-21"})
	assert.ErrorIs(t, err, session.ErrNotInSession)
	res, err := w.RankingsFor(RankingQuery{Names: []string{"GAPX_TRADITIONAL"}, AsOf: "2026-09-21"})
	require.NoError(t, err)
	require.Len(t, res.Lists, 1)
	assert.Equal(t, BasisTraditional, res.Lists[0].Basis)

	assert.Equal(t, []string{"GAPX_TRADITIONAL"}, PublishedNames(&tradOnlyRanker{pctRanker{name: "GAPX"}}, calendar.Regular))
	assert.Empty(t, PublishedNames(&tradOnlyRanker{pctRanker{name: "GAPX"}}, calendar.Premarket))
}
