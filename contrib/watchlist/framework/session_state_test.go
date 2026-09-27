package framework

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
)

// Per-session running state (watchlist ranking-basis spec, "Session-scoped
// running values" and "Premarket volume is reported"). Bars go through the
// real WAL dispatch, like the volume tests.

func TestSessionPremarketVolumeKeptOutOfRegular(t *testing.T) {
	h := newVolHarness(t)
	h.write("SV", "1Min",
		flat(et(2026, 9, 22, 8, 0, 0), 50, 60_000),
		flat(et(2026, 9, 22, 9, 0, 0), 51, 40_000),
	)
	h.write("SV", "1Min",
		flat(et(2026, 9, 22, 10, 0, 0), 52, 1_500_000),
		flat(et(2026, 9, 22, 11, 0, 0), 53, 500_000),
	)
	st := Manager.Get("SV")
	assert.Equal(t, int64(100_000), st.SessionStats(calendar.Premarket).Volume)
	assert.Equal(t, int64(2_000_000), st.SessionStats(calendar.Regular).Volume)
	assert.Equal(t, int64(0), st.SessionStats(calendar.Afterhours).Volume)
	assert.Equal(t, int64(2_100_000), st.CumulativeVolume, "the day still counts every session")
}

func TestSessionRegularOpenIsTheOpeningBar(t *testing.T) {
	h := newVolHarness(t)
	h.write("SO", "1Min",
		testBar{et(2026, 9, 22, 4, 0, 0), 50, 50, 50, 50, 10},
		testBar{et(2026, 9, 22, 9, 29, 0), 51, 51, 51, 51, 10},
		// A bar starting exactly at 09:30 belongs to regular.
		testBar{et(2026, 9, 22, 9, 30, 0), 52, 53, 51.5, 52.5, 10},
		testBar{et(2026, 9, 22, 9, 31, 0), 52.5, 54, 52, 53, 10},
	)
	st := Manager.Get("SO")

	pre := st.SessionStats(calendar.Premarket)
	assert.Equal(t, 50.0, pre.Open)
	assert.Equal(t, 51.0, pre.Last, "09:29 is the last premarket bar")
	assert.Equal(t, int64(20), pre.Volume)

	reg := st.SessionStats(calendar.Regular)
	assert.Equal(t, 52.0, reg.Open, "the regular open is the 09:30 bar")
	assert.Equal(t, et(2026, 9, 22, 9, 30, 0).Unix(), reg.OpenEpoch)
	assert.Equal(t, 54.0, reg.High)
	assert.Equal(t, 51.5, reg.Low)
	assert.Equal(t, 53.0, reg.Last)

	assert.Equal(t, 50.0, st.DayOpen, "the day's open is still the first bar of the day")
}

func TestSessionRewrittenMinuteCountsOnce(t *testing.T) {
	h := newVolHarness(t)
	m := et(2026, 9, 22, 10, 15, 0)
	for _, v := range []int64{1_000, 3_000, 5_000} {
		h.write("RW", "1Min", flat(m, 20, v))
	}
	st := Manager.Get("RW")
	assert.Equal(t, int64(5_000), st.SessionStats(calendar.Regular).Volume)
}

// 1Sec bars count toward a session until the minute's 1Min bar arrives, which
// replaces them; the order the two triggers run in does not matter.
func TestSessionSubMinuteAndMinuteDoNotDoubleCount(t *testing.T) {
	h := newVolHarness(t)
	m := et(2026, 9, 22, 16, 5, 0) // afterhours
	h.write("SM", "1Sec", flat(m, 30, 100), flat(m.Add(time.Second), 30, 200))
	st := Manager.Get("SM")
	assert.Equal(t, int64(300), st.SessionStats(calendar.Afterhours).Volume)

	h.write("SM", "1Min", flat(m, 30, 300))
	h.write("SM", "1Sec", flat(m.Add(2*time.Second), 30, 50)) // late 1Sec for a finished minute
	assert.Equal(t, int64(300), st.SessionStats(calendar.Afterhours).Volume)
	assert.Equal(t, int64(300), st.CumulativeVolume)
}

func TestSessionOrderIndependent(t *testing.T) {
	h := newVolHarness(t)
	// Newest first: the open must still be the earliest bar, the last price
	// the latest.
	h.write("OI", "1Min",
		testBar{et(2026, 9, 22, 9, 32, 0), 12, 12, 12, 12, 1},
		testBar{et(2026, 9, 22, 9, 30, 0), 10, 10, 10, 10, 1},
		testBar{et(2026, 9, 22, 9, 31, 0), 11, 11, 11, 11, 1},
	)
	reg := Manager.Get("OI").SessionStats(calendar.Regular)
	assert.Equal(t, 10.0, reg.Open)
	assert.Equal(t, 12.0, reg.Last)
}

func TestPremarketVolumeReportedAndCarried(t *testing.T) {
	h := newVolHarness(t)
	h.write("PV", "1Min", flat(et(2026, 9, 22, 7, 0, 0), 5, 40_000))
	st := Manager.Get("PV")
	assert.Equal(t, int64(40_000), st.PremarketVolume, "during premarket: the volume so far")

	h.write("PV", "1Min", flat(et(2026, 9, 22, 9, 0, 0), 5, 60_000))
	h.write("PV", "1Min", flat(et(2026, 9, 22, 10, 0, 0), 5, 900_000))
	h.write("PV", "1Min", flat(et(2026, 9, 22, 17, 0, 0), 5, 7_000))
	assert.Equal(t, int64(100_000), st.PremarketVolume, "after premarket: the whole premarket session")

	// A new day starts over.
	h.write("PV", "1Min", flat(et(2026, 9, 23, 4, 30, 0), 5, 1_000))
	assert.Equal(t, int64(1_000), st.PremarketVolume)
}

// A day roll never makes an afterhours print the traditional baseline.
func TestDayRollBaselinesComeFromTheirSessions(t *testing.T) {
	h := newVolHarness(t)
	h.write("DR", "1Min",
		flat(et(2026, 9, 22, 9, 0, 0), 9, 1),    // premarket
		flat(et(2026, 9, 22, 15, 59, 0), 10, 1), // last regular bar
		flat(et(2026, 9, 22, 19, 59, 0), 12, 1), // last afterhours bar
	)
	h.write("DR", "1Min", flat(et(2026, 9, 23, 4, 0, 0), 13, 1))
	st := Manager.Get("DR")
	require.NotNil(t, st)
	assert.Equal(t, 10.0, st.PriorClose, "previous regular close")
	assert.Equal(t, 12.0, st.PrevAfterhoursClose, "previous afterhours close")
	assert.False(t, st.SessionStats(calendar.Afterhours).HasBars, "the new day's sessions start empty")
	assert.True(t, st.SessionStats(calendar.Premarket).HasBars)
}

// When the day's official close is known it wins over the last regular bar.
func TestDayRollPrefersOfficialClose(t *testing.T) {
	h := newVolHarness(t)
	h.write("OC", "1Min", flat(et(2026, 9, 22, 15, 59, 0), 339.73, 1))
	st := Manager.Get("OC")
	st.OfficialClose = 339.75
	h.write("OC", "1Min", flat(et(2026, 9, 23, 4, 0, 0), 340, 1))
	assert.InDelta(t, 339.75, st.PriorClose, 1e-9)
	assert.Equal(t, 0.0, st.OfficialClose, "the new day's official close is not known yet")
}
