package framework

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend/stream"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type testBar struct {
	t                      time.Time
	open, high, low, close float32
	vol                    int64
}

// volHarness writes bars through the real WAL and fires the watchlist
// trigger with exactly the records the dispatcher produced.
type volHarness struct {
	t       *testing.T
	capture *captureTrigger
	trig    *WatchlistTrigger
	fires   map[string]int
}

func newVolHarness(t *testing.T) *volHarness {
	t.Helper()
	capture := setupCapturingInstance(t)
	stream.Initialize()
	Manager = NewSymbolStateManager()
	t.Cleanup(func() {
		stream.Shutdown()
		Manager = nil
	})
	trig, err := NewTrigger(map[string]interface{}{
		"curation": map[string]interface{}{"lookback_secs": 300},
	})
	require.NoError(t, err)
	return &volHarness{t: t, capture: capture, trig: trig.(*WatchlistTrigger), fires: map[string]int{}}
}

// write stores bars in one WriteCSM (one batch) and fires the watchlist.
func (h *volHarness) write(sym, tf string, bars ...testBar) {
	h.t.Helper()
	n := len(bars)
	ep := make([]int64, n)
	o, hi, lo, c := make([]float32, n), make([]float32, n), make([]float32, n), make([]float32, n)
	v := make([]int64, n)
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
	key := sym + "/" + tf + "/OHLCV"
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(key), cs)
	require.NoError(h.t, executor.WriteCSM(csm, false))

	keyPath := fmt.Sprintf("%s/%d.bin", key, bars[0].t.Year())
	recs := h.capture.next(h.t, keyPath, h.fires[keyPath])
	h.fires[keyPath]++
	h.trig.Fire(keyPath, recs)
}

func flat(t time.Time, price float32, vol int64) testBar {
	return testBar{t, price, price, price, price, vol}
}

func et(y int, m time.Month, d, hh, mm, ss int) time.Time {
	return time.Date(y, m, d, hh, mm, ss, 0, calendar.Nasdaq.Tz())
}

// TestVolumeLiveCascade models production: each second a 1Sec bar is
// written, and the 1Sec -> 1Min trigger rewrites the current minute with its
// running volume. Both watchlist triggers see both. Measured before this fix:
// ~22x the true volume.
func TestVolumeLiveCascade(t *testing.T) {
	h := newVolHarness(t)
	rng := rand.New(rand.NewSource(3))
	start := et(2026, 9, 24, 10, 0, 0)
	var truth int64
	var minuteVol int64
	for sec := 0; sec < 180; sec++ {
		if sec%60 == 0 {
			minuteVol = 0
		}
		if rng.Intn(3) == 0 {
			continue
		}
		ts := start.Add(time.Duration(sec) * time.Second)
		v := int64(1 + rng.Intn(1000))
		truth += v
		minuteVol += v
		h.write("LIVE", "1Sec", flat(ts, 100, v))
		h.write("LIVE", "1Min", flat(ts.Truncate(time.Minute), 100, minuteVol))
		require.Equal(t, truth, Manager.Get("LIVE").CumulativeVolume, "after %s", ts.Format("15:04:05"))
	}
}

// A 1Min bar rewritten many times (as the cascade does every second) counts
// its latest version once.
func TestVolumeRewriteCountsOnce(t *testing.T) {
	h := newVolHarness(t)
	m := et(2026, 9, 24, 10, 0, 0)
	for _, v := range []int64{10, 25, 40, 40, 55} {
		h.write("RW", "1Min", flat(m, 100, v))
	}
	h.write("RW", "1Min", flat(m.Add(time.Minute), 100, 7))
	assert.Equal(t, int64(62), Manager.Get("RW").CumulativeVolume)
}

// A single write carrying many bars (backfill, outage fill) counts them all.
func TestVolumeBatchCountsEveryBar(t *testing.T) {
	h := newVolHarness(t)
	start := et(2026, 9, 24, 10, 0, 0)
	var bars []testBar
	var truth int64
	for i := 0; i < 30; i++ {
		v := int64(100 + i)
		truth += v
		bars = append(bars, flat(start.Add(time.Duration(i)*time.Minute), 100, v))
	}
	h.write("BATCH", "1Min", bars...)
	assert.Equal(t, truth, Manager.Get("BATCH").CumulativeVolume)
}

// The startup backfill rewrites yesterday's bars and then today's. That must
// not look like two day changes and wipe today's volume (measured before this
// fix: 0.2x-0.6x of the day after a restart).
func TestVolumeRestartBackfillKeepsDay(t *testing.T) {
	h := newVolHarness(t)
	var today []testBar
	var truth int64
	for i := 0; i < 20; i++ {
		v := int64(1000 + i)
		truth += v
		today = append(today, flat(et(2026, 9, 24, 9, 30+i, 0), 100, v))
	}
	for _, b := range today {
		h.write("RS", "1Min", b)
	}
	st := Manager.Get("RS")
	require.Equal(t, truth, st.CumulativeVolume)
	lastClose := st.LastClose

	// Restart backfill: yesterday's session, then today's, in two batches.
	var yesterday []testBar
	for i := 0; i < 20; i++ {
		yesterday = append(yesterday, flat(et(2026, 9, 23, 9, 30+i, 0), 90, 5000))
	}
	h.write("RS", "1Min", yesterday...)
	h.write("RS", "1Min", today...)

	assert.Equal(t, truth, st.CumulativeVolume, "today's volume must survive the backfill")
	assert.Equal(t, lastClose, st.LastClose)
	assert.Equal(t, et(2026, 9, 24, 0, 0, 0).Unix(), st.LiveDay)
}

// The trading day is the New York calendar day. After 20:00 EDT it is
// already the next day in UTC, which previously reset the state mid-session
// (and in winter, at 19:00 EST, inside after-hours).
func TestVolumeTradingDayIsNewYork(t *testing.T) {
	h := newVolHarness(t)
	h.write("NY", "1Min", flat(et(2026, 1, 15, 18, 58, 0), 100, 10)) // 23:58 UTC
	h.write("NY", "1Min", flat(et(2026, 1, 15, 19, 30, 0), 100, 20)) // 00:30 UTC next day
	st := Manager.Get("NY")
	assert.Equal(t, int64(30), st.CumulativeVolume, "same trading day in New York")
	post := st.SessionStats(calendar.Afterhours)
	assert.Equal(t, int64(30), post.Volume, "19:30 EST is still this day's afterhours")

	// The next New York day does reset. Both prints were afterhours, so they
	// become the premarket session baseline, never the traditional one.
	h.write("NY", "1Min", flat(et(2026, 1, 16, 4, 0, 0), 101, 5))
	assert.Equal(t, int64(5), st.CumulativeVolume)
	assert.Equal(t, 100.0, st.PrevAfterhoursClose)
	assert.Equal(t, 0.0, st.PriorClose, "no regular close on day 1, so no traditional baseline")
}

// DollarVolumeRate is dollar volume per second over the configured lookback
// (300s), as documented, rather than a function of how many fires occurred.
func TestDollarVolumeRateUsesLookback(t *testing.T) {
	h := newVolHarness(t)
	start := et(2026, 9, 24, 10, 0, 0)
	for i := 0; i < 10; i++ {
		h.write("DV", "1Min", flat(start.Add(time.Duration(i)*time.Minute), 50, int64(100*(i+1))))
	}
	// Last bar starts 10:09; the 300s window covers the minutes 10:05-10:09,
	// whose volumes are 600..1000 (4,000 shares at $50).
	assert.InDelta(t, 4000*50.0/300.0, Manager.Get("DV").DollarVolumeRate, 0.001)
}

// A bar that arrives late (e.g. an outage fill) updates the day's totals but
// must not move the last price backwards.
func TestLateBarDoesNotRegressLastPrice(t *testing.T) {
	h := newVolHarness(t)
	h.write("LATE", "1Min", flat(et(2026, 9, 24, 10, 5, 0), 105, 10))
	h.write("LATE", "1Min", testBar{et(2026, 9, 24, 10, 1, 0), 99, 120, 90, 101, 20})
	st := Manager.Get("LATE")
	assert.Equal(t, 105.0, st.LastPrice)
	assert.Equal(t, 120.0, st.HighOfDay)
	assert.Equal(t, 90.0, st.LowOfDay)
	assert.Equal(t, 99.0, st.DayOpen, "the earliest bar sets the day's open")
	assert.Equal(t, int64(30), st.CumulativeVolume)
}

