package framework

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework/session"
)

// recorder is a strategy that ranks every symbol it is given and remembers
// the views and contexts it saw.
type recorder struct {
	name   string
	bases  []Basis
	median bool
	seen   map[Basis]map[string]seenVals
	ctxs   []RankContext
}

func (r *recorder) Name() string                                  { return r.name }
func (r *recorder) Configure(map[string]interface{}) error        { return nil }
func (r *recorder) UsesVolumeMedian() bool                        { return r.median }
func (r *recorder) Rank(c map[string]*SymbolState) []RankedSymbol { panic("RankAt is used") }

func (r *recorder) RankAt(ctx RankContext, c map[string]*SymbolState) []RankedSymbol {
	r.ctxs = append(r.ctxs, ctx)
	return r.rank(ctx.Basis, c)
}

func (r *recorder) rank(b Basis, c map[string]*SymbolState) []RankedSymbol {
	if r.seen == nil {
		r.seen = map[Basis]map[string]seenVals{}
	}
	m := map[string]seenVals{}
	var syms []string
	for sym, st := range c {
		m[sym] = seenVals{
			PriorClose: st.PriorClose, PctChange: st.PctChange, DayOpen: st.DayOpen,
			LastPrice: st.LastPrice, HighOfDay: st.HighOfDay, LowOfDay: st.LowOfDay,
			CumulativeVolume: st.CumulativeVolume, PremarketVolume: st.PremarketVolume,
			VolumeMultipleOfMed: st.VolumeMultipleOfMed, MedianVolume50D: st.MedianVolume50D,
		}
		syms = append(syms, sym)
	}
	r.seen[b] = m
	sort.Strings(syms)
	out := make([]RankedSymbol, len(syms))
	for i, s := range syms {
		out[i] = RankedSymbol{Symbol: s}
	}
	return out
}

// seenVals are the view values a strategy reads.
type seenVals struct {
	PriorClose, PctChange, DayOpen, LastPrice, HighOfDay, LowOfDay float64
	CumulativeVolume, PremarketVolume                              int64
	VolumeMultipleOfMed, MedianVolume50D                           float64
}

// basisRecorder limits the bases a recorder supports.
type basisRecorder struct{ *recorder }

func (b basisRecorder) Bases() []Basis { return b.bases }

func win(t *testing.T, at time.Time) session.Window {
	t.Helper()
	w, err := session.Resolve(session.Query{}, at)
	require.NoError(t, err)
	return w
}

func names(lists []RankedList) []string {
	var out []string
	for _, l := range lists {
		out = append(out, l.Name)
	}
	sort.Strings(out)
	return out
}

// fixture: SYM trades in all three sessions of Tue 9/22, with baselines.
func sessionFixture(t *testing.T) *SymbolState {
	h := newVolHarness(t)
	day := et(2026, 9, 22, 0, 0, 0)
	st := Manager.GetOrCreate("SYM")
	b := Baselines{Date: day, PriorClose: 100, PrevAfterhoursClose: 30}
	b.SessionMedianVolume = [3]float64{100_000, 2_000_000, 50_000}
	st.SetBaselines(b)
	h.write("SYM", "1Min",
		testBar{et(2026, 9, 22, 4, 0, 0), 31, 34, 31, 32, 100_000},
		testBar{et(2026, 9, 22, 9, 29, 0), 32, 33, 32, 33, 200_000}, // premarket close 33
		testBar{et(2026, 9, 22, 9, 30, 0), 52, 53, 51, 52.5, 1_000_000},
		testBar{et(2026, 9, 22, 15, 59, 0), 52.5, 56, 52, 55, 1_000_000}, // regular close 55
		testBar{et(2026, 9, 22, 16, 30, 0), 55, 55, 50, 52.25, 50_000},
	)
	return st
}

// 6.3 + 7.1: each session's view carries that session's values and the
// basis baseline from the ranking-basis spec.
func TestViewsPerSessionAndBasis(t *testing.T) {
	sessionFixture(t)
	curated := Manager.AllStates()
	r := &recorder{name: "R"}

	// Premarket (window ends 09:30): baseline = previous afterhours close 30.
	rankAll([]WatchlistStrategy{r}, curated, win(t, et(2026, 9, 22, 9, 30, 0).Add(-time.Second)))
	pre := r.seen[BasisSession]["SYM"]
	assert.Equal(t, 30.0, pre.PriorClose)
	assert.Equal(t, 33.0, pre.LastPrice)
	assert.InDelta(t, 10.0, pre.PctChange, 1e-9)
	assert.Equal(t, 31.0, pre.DayOpen)
	assert.Equal(t, int64(300_000), pre.CumulativeVolume)
	assert.Equal(t, int64(300_000), pre.PremarketVolume)
	assert.InDelta(t, 3.0, pre.VolumeMultipleOfMed, 1e-9, "premarket relative volume against the premarket median")

	// Regular: session baseline = premarket close 33; traditional = 100.
	r.seen = nil
	rankAll([]WatchlistStrategy{r}, curated, win(t, et(2026, 9, 22, 15, 59, 30)))
	reg := r.seen[BasisSession]["SYM"]
	assert.Equal(t, 33.0, reg.PriorClose)
	assert.InDelta(t, (55.0-33)/33*100, reg.PctChange, 1e-9)
	assert.Equal(t, 52.0, reg.DayOpen, "the 09:30 open")
	assert.Equal(t, int64(2_000_000), reg.CumulativeVolume)
	assert.InDelta(t, 1.0, reg.VolumeMultipleOfMed, 1e-9, "a normal regular session is 1.0x")
	assert.Equal(t, int64(300_000), reg.PremarketVolume, "carried into the regular session")
	trad := r.seen[BasisTraditional]["SYM"]
	assert.Equal(t, 100.0, trad.PriorClose)
	assert.InDelta(t, 55.0-100, trad.PctChange, 1e-9)
	assert.Equal(t, 52.0, trad.DayOpen)

	// Afterhours: baseline = today's official close. With no 1D bar yet it
	// is the last regular close (55); once known, the official close wins.
	r.seen = nil
	rankAll([]WatchlistStrategy{r}, curated, win(t, et(2026, 9, 22, 17, 0, 0)))
	post := r.seen[BasisSession]["SYM"]
	assert.Equal(t, 55.0, post.PriorClose)
	assert.InDelta(t, -5.0, post.PctChange, 1e-9)
	assert.NotContains(t, r.seen, BasisTraditional, "no traditional basis outside the regular session")

	Manager.Get("SYM").SetBaselines(Baselines{
		Date: et(2026, 9, 22, 0, 0, 0), PriorClose: 100, PrevAfterhoursClose: 30, OfficialClose: 55.05,
	})
	r.seen = nil
	rankAll([]WatchlistStrategy{r}, curated, win(t, et(2026, 9, 22, 17, 0, 0)))
	assert.Equal(t, 55.05, r.seen[BasisSession]["SYM"].PriorClose)
}

// 6.5: symbols without the data a basis needs drop out; no walk-back.
func TestViewsDropOut(t *testing.T) {
	h := newVolHarness(t)
	day := et(2026, 9, 22, 0, 0, 0)
	// NOPRE: no premarket bars today, has a traditional baseline.
	Manager.GetOrCreate("NOPRE").SetBaselines(Baselines{Date: day, PriorClose: 10})
	h.write("NOPRE", "1Min", flat(et(2026, 9, 22, 10, 0, 0), 11, 1))
	// NOPOST: no afterhours baseline from the previous day.
	Manager.GetOrCreate("NOPOST").SetBaselines(Baselines{Date: day, PriorClose: 10})
	h.write("NOPOST", "1Min", flat(et(2026, 9, 22, 5, 0, 0), 11, 1))
	// NOMED: everything but a volume median.
	Manager.GetOrCreate("NOMED").SetBaselines(Baselines{Date: day, PriorClose: 10, PrevAfterhoursClose: 10})
	h.write("NOMED", "1Min", flat(et(2026, 9, 22, 5, 0, 0), 11, 1))

	curated := Manager.AllStates()
	r := &recorder{name: "R"}
	rm := &recorder{name: "RM", median: true}

	rankAll([]WatchlistStrategy{r}, curated, win(t, et(2026, 9, 22, 10, 30, 0)))
	assert.NotContains(t, r.seen[BasisSession], "NOPRE", "no premarket today: out of bare regular rankings")
	assert.Contains(t, r.seen[BasisTraditional], "NOPRE", "still in the traditional list")

	r.seen = nil
	rankAll([]WatchlistStrategy{r, rm}, curated, win(t, et(2026, 9, 22, 6, 0, 0)))
	assert.NotContains(t, r.seen[BasisSession], "NOPOST", "no afterhours prints the day before")
	assert.Contains(t, r.seen[BasisSession], "NOMED", "a median is not needed here")
	assert.NotContains(t, rm.seen[BasisSession], "NOMED", "but it is for a relative-volume strategy")
}

// 7.2: which lists exist in which session, and how they are named.
func TestListsPerSession(t *testing.T) {
	sessionFixture(t)
	curated := Manager.AllStates()
	both := &recorder{name: "PCT"}
	tradOnly := basisRecorder{&recorder{name: "GAP", bases: []Basis{BasisTraditional}}}
	strategies := []WatchlistStrategy{both, tradOnly}

	pre := rankAll(strategies, curated, win(t, et(2026, 9, 22, 8, 0, 0)))
	assert.Equal(t, []string{"PCT"}, names(pre))

	reg := rankAll(strategies, curated, win(t, et(2026, 9, 22, 11, 0, 0)))
	assert.Equal(t, []string{"GAP_TRADITIONAL", "PCT", "PCT_TRADITIONAL"}, names(reg))
	for _, l := range reg {
		want := BasisSession
		if l.Name != "PCT" {
			want = BasisTraditional
		}
		assert.Equal(t, want, l.Basis, l.Name)
		assert.Equal(t, calendar.Regular, l.Window.Session)
	}

	post := rankAll(strategies, curated, win(t, et(2026, 9, 22, 17, 0, 0)))
	assert.Equal(t, []string{"PCT"}, names(post))

	n, b := SplitListName("GAP_TRADITIONAL")
	assert.Equal(t, "GAP", n)
	assert.Equal(t, BasisTraditional, b)
}

// 7.3: a ContextualRanker is told the trading date, session, basis and
// window end.
func TestContextualRankerGetsContext(t *testing.T) {
	sessionFixture(t)
	r := &recorder{name: "CTX"}
	w := win(t, et(2026, 9, 22, 11, 0, 0))
	rankAll([]WatchlistStrategy{r}, Manager.AllStates(), w)
	require.Len(t, r.ctxs, 2)
	for _, c := range r.ctxs {
		assert.True(t, c.TradingDate.Equal(et(2026, 9, 22, 0, 0, 0)))
		assert.Equal(t, calendar.Regular, c.Session)
		assert.True(t, c.WindowEnd.Equal(et(2026, 9, 22, 11, 0, 0)))
	}
	assert.Equal(t, BasisSession, r.ctxs[0].Basis)
	assert.Equal(t, BasisTraditional, r.ctxs[1].Basis)
}

// Views are snapshots: a trigger writing while a strategy reads is safe
// (run with -race).
func TestRankingConcurrentWithTriggers(t *testing.T) {
	h := newVolHarness(t)
	Manager.GetOrCreate("RC").SetBaselines(Baselines{Date: et(2026, 9, 22, 0, 0, 0), PriorClose: 10})
	h.write("RC", "1Min", flat(et(2026, 9, 22, 10, 0, 0), 11, 1))
	w := win(t, et(2026, 9, 22, 11, 0, 0))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			rankAll([]WatchlistStrategy{&recorder{name: "X"}}, Manager.AllStates(), w)
			DetectCurationChanges(Manager)
		}
	}()
	for i := 0; i < 50; i++ {
		h.write("RC", "1Min", flat(et(2026, 9, 22, 10, i, 0), 11, 1))
	}
	<-done
}
