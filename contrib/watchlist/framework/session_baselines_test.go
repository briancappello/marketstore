package framework

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// baselineHarness is an instance with a read-only facts service (the
// fallback computes rows from 1Min bars) and helpers to write bars directly.
type baselineHarness struct {
	t     *testing.T
	facts *sessionfacts.Service
}

func newBaselineHarness(t *testing.T, at time.Time) *baselineHarness {
	t.Helper()
	setupCapturingInstance(t)
	svc, err := sessionfacts.NewService(sessionfacts.Config{Now: func() time.Time { return at }})
	require.NoError(t, err)
	return &baselineHarness{t: t, facts: svc}
}

func (h *baselineHarness) writeBars(sym, tf string, bars ...testBar) {
	h.t.Helper()
	n := len(bars)
	ep, v := make([]int64, n), make([]int64, n)
	o, hi, lo, c := make([]float32, n), make([]float32, n), make([]float32, n), make([]float32, n)
	for i, b := range bars {
		ep[i], o[i], hi[i], lo[i], c[i], v[i] = b.t.Unix(), b.open, b.high, b.low, b.close, b.vol
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", ep)
	cs.AddColumn("Open", o)
	cs.AddColumn("High", hi)
	cs.AddColumn("Low", lo)
	cs.AddColumn("Close", c)
	cs.AddColumn("Volume", v)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(sym + "/" + tf + "/OHLCV"), cs)
	require.NoError(h.t, executor.WriteCSM(csm, false))
}

func (h *baselineHarness) load(sym string, y int, m time.Month, d, window int, at time.Time) Baselines {
	h.t.Helper()
	ds, err := calendar.Nasdaq.SessionBounds(y, m, d)
	require.NoError(h.t, err)
	b, err := LoadBaselines(h.facts, executor.ThisInstance.CatalogDir, sym, ds, window, at)
	require.NoError(h.t, err)
	return b
}

// 6.1: the official close is the 1D close when it exists, otherwise the last
// regular 1Min close; for a day in progress it is not known yet.
func TestOfficialClose(t *testing.T) {
	at := et(2026, 9, 24, 12, 0, 0)
	h := newBaselineHarness(t, at)
	// 9/22 has a 1D bar (the official close from the closing auction).
	h.writeBars("OC", "1Min", flat(et(2026, 9, 22, 15, 59, 0), 339.73, 1))
	h.writeBars("OC", "1D", flat(et(2026, 9, 22, 0, 0, 0), 339.75, 1))
	// 9/23 has no 1D bar yet.
	h.writeBars("OC", "1Min", flat(et(2026, 9, 23, 15, 59, 0), 336.95, 1), flat(et(2026, 9, 23, 17, 0, 0), 336.8, 1))

	assert.InDelta(t, 339.75, h.load("OC", 2026, 9, 22, 5, at).OfficialClose, 1e-4, "1D close wins")
	assert.InDelta(t, 336.95, h.load("OC", 2026, 9, 23, 5, at).OfficialClose, 1e-4,
		"no 1D bar: the last regular close, not the afterhours print")

	during := et(2026, 9, 23, 17, 30, 0)
	assert.Equal(t, 0.0, h.load("OC", 2026, 9, 23, 5, during).OfficialClose,
		"in progress: not known, the live regular last price is used instead")
}

// 6.2: the traditional baseline is RC(previous trading date by calendar).
// The old code took closes[len-2] from 1D bars, which is the day before
// that whenever the current day's 1D bar has not been written yet.
func TestTraditionalPriorCloseIsPreviousTradingDate(t *testing.T) {
	at := et(2026, 9, 23, 11, 0, 0)
	h := newBaselineHarness(t, at)
	h.writeBars("TP", "1D",
		flat(et(2026, 9, 21, 0, 0, 0), 100, 1),
		flat(et(2026, 9, 22, 0, 0, 0), 101, 1),
		// no 1D bar for 9/23 (today)
	)
	assert.InDelta(t, 101.0, h.load("TP", 2026, 9, 23, 5, at).PriorClose, 1e-4)

	// Across a weekend: Monday's prior is Friday.
	h.writeBars("TP", "1D", flat(et(2026, 9, 25, 0, 0, 0), 105, 1))
	assert.InDelta(t, 105.0, h.load("TP", 2026, 9, 28, 5, et(2026, 9, 28, 11, 0, 0)).PriorClose, 1e-4)

	// No 1D bar for the previous date: its last regular 1Min close.
	h.writeBars("TP", "1Min", flat(et(2026, 9, 28, 15, 59, 0), 106.5, 1))
	assert.InDelta(t, 106.5, h.load("TP", 2026, 9, 29, 5, et(2026, 9, 29, 11, 0, 0)).PriorClose, 1e-4)

	// No data for the previous date at all: no baseline (drop-out).
	assert.Equal(t, 0.0, h.load("NONE", 2026, 9, 29, 5, et(2026, 9, 29, 11, 0, 0)).PriorClose)
}

// 6.4: per-session medians over the median window of dates before D, from
// session facts; the window can change without a rebuild; D itself and
// days the symbol did not trade are excluded.
func TestSessionMedians(t *testing.T) {
	at := et(2026, 9, 28, 5, 0, 0)
	h := newBaselineHarness(t, at)
	// Five trading dates before Mon 9/28: 9/21..9/25. Regular volume 1..5
	// million; premarket 100k except one day; afterhours 10k.
	for i, d := range []int{21, 22, 23, 24, 25} {
		h.writeBars("MD", "1Min",
			flat(et(2026, 9, d, 8, 0, 0), 10, map[int]int64{2: 400_000}[i]+100_000),
			flat(et(2026, 9, d, 10, 0, 0), 10, int64(i+1)*1_000_000),
			flat(et(2026, 9, d, 17, 0, 0), 10, 10_000),
		)
	}
	// D itself trades a lot; it must not count.
	h.writeBars("MD", "1Min", flat(et(2026, 9, 28, 4, 30, 0), 10, 99_000_000))

	b := h.load("MD", 2026, 9, 28, 5, at)
	assert.Equal(t, 3_000_000.0, b.SessionMedianVolume[calendar.Regular])
	assert.Equal(t, 100_000.0, b.SessionMedianVolume[calendar.Premarket])
	assert.Equal(t, 10_000.0, b.SessionMedianVolume[calendar.Afterhours])

	// A 2-date window (9/24, 9/25) takes effect at once: no stored stat.
	b = h.load("MD", 2026, 9, 28, 2, at)
	assert.Equal(t, 4_500_000.0, b.SessionMedianVolume[calendar.Regular])

	// A 10-date window only counts the 5 dates the symbol traded.
	b = h.load("MD", 2026, 9, 28, 10, at)
	assert.Equal(t, 3_000_000.0, b.SessionMedianVolume[calendar.Regular])
}

// Baselines loaded for a date the state has not reached are held until the
// day rolls to it, then replace the carried-forward values.
func TestSetBaselinesPendingUntilRoll(t *testing.T) {
	h := newVolHarness(t)
	h.write("PB", "1Min", flat(et(2026, 9, 22, 15, 59, 0), 10, 1))
	st := Manager.Get("PB")

	next := Baselines{Date: et(2026, 9, 23, 0, 0, 0), PriorClose: 10.02, PrevAfterhoursClose: 9.9}
	next.SessionMedianVolume[calendar.Regular] = 1234
	st.SetBaselines(next)
	assert.Equal(t, 0.0, st.PrevAfterhoursClose, "not applied before the roll")

	h.write("PB", "1Min", flat(et(2026, 9, 23, 4, 0, 0), 11, 1))
	assert.Equal(t, 10.02, st.PriorClose, "the loaded official close, not the carried 1Min close")
	assert.Equal(t, 9.9, st.PrevAfterhoursClose)
	assert.Equal(t, 1234.0, st.MedianVolume50D)

	// Stale baselines are ignored.
	st.SetBaselines(Baselines{Date: et(2026, 9, 22, 0, 0, 0), PriorClose: 1})
	assert.Equal(t, 10.02, st.PriorClose)
}
