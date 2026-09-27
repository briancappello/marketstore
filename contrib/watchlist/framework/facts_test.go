package framework

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils"
)

// withLeaderFacts installs a leader facts service and a fake clock.
func withLeaderFacts(t *testing.T, clock *time.Time) *sessionfacts.Service {
	t.Helper()
	svc, err := sessionfacts.NewService(sessionfacts.Config{
		Write:    executor.WriteCSM,
		StateDir: sessionfacts.StateDirFor(utils.InstanceConfig.RootDirectory),
		Symbols:  func() []string { return DiscoverSymbols(executor.ThisInstance.CatalogDir) },
		Now:      func() time.Time { return *clock },
	})
	require.NoError(t, err)
	prevFacts, prevNow := Facts, now
	Facts, now = svc, func() time.Time { return *clock }
	t.Cleanup(func() { Facts, now = prevFacts, prevNow })
	return svc
}

func regVolume(t *testing.T, sym string, d calendar.DaySessions) int64 {
	t.Helper()
	rows, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, sym, d.Date, d.Date)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows[0].RegVolume
}

// R4: a gap fill for a completed day, arriving through the trigger, marks the
// day dirty, and the drain recomputes its facts.
func TestGapFillForCompletedDayUpdatesFacts(t *testing.T) {
	h := newVolHarness(t)
	clock := et(2026, 9, 22, 10, 0, 0)
	svc := withLeaderFacts(t, &clock)
	d, err := calendar.Nasdaq.SessionBounds(2026, 9, 22)
	require.NoError(t, err)

	// A normal live bar during the session is not late.
	h.write("GF", "1Min", flat(et(2026, 9, 22, 10, 0, 0), 10, 1_000))
	assert.Empty(t, svc.Journal().Pending(), "live bars are not late")

	// The day ends and its facts are written.
	clock = et(2026, 9, 22, 21, 0, 0)
	require.NoError(t, svc.RunDaily(d))
	assert.Equal(t, int64(1_000), regVolume(t, "GF", d))

	// Next day's live bar, then an outage fill for 14:00-14:30 of the day
	// before.
	clock = et(2026, 9, 23, 10, 0, 0)
	h.write("GF", "1Min", flat(et(2026, 9, 23, 10, 0, 0), 11, 5))
	var fill []testBar
	for i := 0; i < 30; i++ {
		fill = append(fill, flat(et(2026, 9, 22, 14, i, 0), 10, 100))
	}
	h.write("GF", "1Min", fill...)

	pending := svc.Journal().Pending()
	require.Len(t, pending, 1)
	assert.Equal(t, "GF", pending[0].Symbol)
	assert.True(t, pending[0].Date.Equal(d.Date))

	require.NoError(t, svc.DrainDirty())
	assert.Equal(t, int64(4_000), regVolume(t, "GF", d), "the filled bars are included")
	assert.Empty(t, svc.Journal().Pending())
}

// A fill for today that lands after today's afterhours ended is late too,
// even though it is still the symbol's live day.
func TestFillAfterTheCloseIsLate(t *testing.T) {
	h := newVolHarness(t)
	clock := et(2026, 9, 22, 21, 0, 0)
	svc := withLeaderFacts(t, &clock)
	h.write("AC", "1Min", flat(et(2026, 9, 22, 15, 0, 0), 10, 1))
	require.Len(t, svc.Journal().Pending(), 1)
}

// 1Sec bars never mark facts dirty: facts derive from 1Min bars.
func TestSubMinuteBarsDoNotMarkDirty(t *testing.T) {
	h := newVolHarness(t)
	clock := et(2026, 9, 22, 21, 0, 0)
	svc := withLeaderFacts(t, &clock)
	h.write("SS", "1Sec", flat(et(2026, 9, 22, 15, 0, 0), 10, 1))
	assert.Empty(t, svc.Journal().Pending())
}
