package sessionfacts_test

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// countingWriter wraps executor.WriteCSM and counts calls.
type countingWriter struct{ n atomic.Int64 }

func (c *countingWriter) write(csm io.ColumnSeriesMap, v bool) error {
	c.n.Add(1)
	return executor.WriteCSM(csm, v)
}

func newLeader(t *testing.T, root string, now func() time.Time, symbols ...string) (*sessionfacts.Service, *countingWriter) {
	t.Helper()
	w := &countingWriter{}
	svc, err := sessionfacts.NewService(sessionfacts.Config{
		Write:    w.write,
		StateDir: sessionfacts.StateDirFor(root),
		Symbols:  func() []string { return symbols },
		Now:      now,
	})
	require.NoError(t, err)
	return svc, w
}

func day(t *testing.T, y int, m time.Month, d int) calendar.DaySessions {
	t.Helper()
	ds, err := calendar.Nasdaq.SessionBounds(y, m, d)
	require.NoError(t, err)
	return ds
}

// computeFromBars is the reference: Compute over the day's bars on disk.
func computeFromBars(t *testing.T, sym string, ds calendar.DaySessions) sessionfacts.Row {
	t.Helper()
	bars, err := sessionfacts.ReadMinuteBars(executor.ThisInstance.CatalogDir, sym,
		ds.Premarket.Start, ds.Afterhours.End)
	require.NoError(t, err)
	return sessionfacts.Compute(ds, bars)
}

// R2: the daily job writes exactly what Compute derives from the bars.
func TestDailyJobMatchesCompute(t *testing.T) {
	root := startInstance(t)
	writeMinutes(t, "AA",
		minuteBar{et(2026, 9, 22, 7, 0), 10, 100},
		minuteBar{et(2026, 9, 22, 9, 30), 11, 5_000},
		minuteBar{et(2026, 9, 22, 18, 0), 12, 70},
	)
	writeMinutes(t, "BB", minuteBar{et(2026, 9, 22, 12, 0), 50, 9_000})
	d := day(t, 2026, 9, 22)

	svc, _ := newLeader(t, root, time.Now, "AA", "BB", "NODATA")
	require.NoError(t, svc.RunDaily(d))

	for _, sym := range []string{"AA", "BB"} {
		got, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, sym, d.Date, d.Date)
		require.NoError(t, err)
		require.Len(t, got, 1, sym)
		assert.Equal(t, computeFromBars(t, sym, d), got[0], sym)
	}
	none, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, "NODATA", d.Date, d.Date)
	require.NoError(t, err)
	assert.Empty(t, none, "a symbol without 1Min data gets no rows")
}

// The daily job runs once the grace period after afterhours has passed, and
// only once per day.
func TestTickRunsDailyJobOnceAfterGrace(t *testing.T) {
	root := startInstance(t)
	writeMinutes(t, "AA", minuteBar{et(2026, 9, 22, 10, 0), 10, 100})
	var now time.Time
	svc, w := newLeader(t, root, func() time.Time { return now }, "AA")

	now = et(2026, 9, 22, 20, 10) // inside the grace period
	due, err := svc.DueDay()
	require.NoError(t, err)
	assert.True(t, due.Date.Equal(et(2026, 9, 21, 0, 0)), "not due yet: the previous day is")

	now = et(2026, 9, 22, 20, 31)
	due, err = svc.DueDay()
	require.NoError(t, err)
	assert.True(t, due.Date.Equal(et(2026, 9, 22, 0, 0)))

	require.NoError(t, svc.Tick())
	writes := w.n.Load()
	assert.Positive(t, writes)
	rows, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, "AA", due.Date, due.Date)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(100), rows[0].RegVolume)

	now = et(2026, 9, 22, 23, 0)
	require.NoError(t, svc.Tick())
	assert.Equal(t, writes, w.n.Load(), "already run for this day")

	// A restart (new service over the same state) does not rerun it either.
	svc2, w2 := newLeader(t, root, func() time.Time { return now }, "AA")
	require.NoError(t, svc2.Tick())
	assert.Equal(t, int64(0), w2.n.Load())
}

// R3: a row written with another version is treated as missing.
func TestOldVersionRowIsNotUsed(t *testing.T) {
	root := startInstance(t)
	writeMinutes(t, "OV", minuteBar{et(2026, 9, 22, 10, 0), 10, 100})
	d := day(t, 2026, 9, 22)
	stale := sessionfacts.Row{Date: d.Date, RegVolume: 999_999, RegClose: 1, RegBars: 1, Version: sessionfacts.Version - 1}
	require.NoError(t, sessionfacts.Write(executor.WriteCSM, map[string][]sessionfacts.Row{"OV": {stale}}))

	svc, _ := newLeader(t, root, time.Now)
	rows, err := svc.Rows("OV", []calendar.DaySessions{d})
	require.NoError(t, err)
	assert.Equal(t, computeFromBars(t, "OV", d), rows[0], "the stale row is ignored and the bars are used")
}

// R6: a missing row is computed from bars; with no bars either, the symbol
// has no data for the day.
func TestMissingRowFallback(t *testing.T) {
	root := startInstance(t)
	writeMinutes(t, "FB", minuteBar{et(2026, 9, 22, 17, 0), 12.5, 42})
	d := day(t, 2026, 9, 22)
	svc, _ := newLeader(t, root, time.Now)

	fromFallback, err := svc.Rows("FB", []calendar.DaySessions{d})
	require.NoError(t, err)
	require.NoError(t, svc.RunDaily(d))
	fromStored, err := svc.Rows("FB", []calendar.DaySessions{d})
	require.NoError(t, err)
	assert.Equal(t, fromStored, fromFallback, "same result with and without the stored row")

	empty, err := svc.Rows("FB", []calendar.DaySessions{day(t, 2026, 9, 21)})
	require.NoError(t, err)
	assert.False(t, empty[0].Traded(), "no row and no bars: no data")
	_, ok := empty[0].Close(calendar.Afterhours)
	assert.False(t, ok)
}

// R7: a replica never writes or journals.
func TestReplicaNeverWrites(t *testing.T) {
	root := startInstance(t)
	writeMinutes(t, "RP", minuteBar{et(2026, 9, 22, 10, 0), 10, 100})
	svc, err := sessionfacts.NewService(sessionfacts.Config{
		Symbols: func() []string { return []string{"RP"} },
		Now:     func() time.Time { return et(2026, 9, 23, 12, 0) },
	})
	require.NoError(t, err)
	assert.False(t, svc.Leader())

	assert.True(t, errors.Is(svc.RunDaily(day(t, 2026, 9, 22)), sessionfacts.ErrReplica))
	assert.True(t, errors.Is(svc.Recompute([]sessionfacts.Entry{{Symbol: "RP", Date: et(2026, 9, 22, 0, 0)}}),
		sessionfacts.ErrReplica))
	svc.MarkDirty("RP", et(2026, 9, 22, 10, 0))
	require.NoError(t, svc.Tick())
	assert.Nil(t, svc.Journal())

	rows, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, "RP", et(2026, 9, 1, 0, 0), et(2026, 9, 30, 0, 0))
	require.NoError(t, err)
	assert.Empty(t, rows, "nothing was written")
	_, statErr := os.Stat(sessionfacts.StateDirFor(root))
	assert.True(t, os.IsNotExist(statErr), "no journal or state files")

	// Reading still works, through the fallback.
	got, err := svc.Rows("RP", []calendar.DaySessions{day(t, 2026, 9, 22)})
	require.NoError(t, err)
	assert.Equal(t, int64(100), got[0].RegVolume)
}

// R4 (storage half): the dirty journal survives a restart and a drain
// recomputes the entries.
func TestJournalSurvivesRestartAndDrains(t *testing.T) {
	root := startInstance(t)
	writeMinutes(t, "LJ", minuteBar{et(2026, 9, 22, 10, 0), 10, 100})
	d := day(t, 2026, 9, 22)
	svc, _ := newLeader(t, root, time.Now, "LJ")
	require.NoError(t, svc.RunDaily(d))

	// A gap fill lands after the day's facts were written.
	writeMinutes(t, "LJ", minuteBar{et(2026, 9, 22, 14, 0), 10, 900})
	svc.MarkDirty("LJ", et(2026, 9, 22, 14, 0))
	svc.MarkDirty("LJ", et(2026, 9, 22, 14, 1)) // same date: one entry

	// Restart before the drain.
	svc2, _ := newLeader(t, root, time.Now, "LJ")
	require.Len(t, svc2.Journal().Pending(), 1)

	var changed []sessionfacts.Entry
	svc2.OnChange(func(c []sessionfacts.Entry) { changed = append(changed, c...) })
	require.NoError(t, svc2.DrainDirty())
	assert.Empty(t, svc2.Journal().Pending())
	require.Len(t, changed, 1)

	rows, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, "LJ", d.Date, d.Date)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(1_000), rows[0].RegVolume, "the gap fill is included")

	// The emptied journal stays empty after another restart.
	j, err := sessionfacts.OpenJournal(filepath.Join(sessionfacts.StateDirFor(root), "sessionfacts.dirty"))
	require.NoError(t, err)
	assert.Empty(t, j.Pending())
}

// The daily job also recomputes the SafetyDays trading dates before the day,
// across weekends and holidays, as a net for lost invalidations.
func TestDailyJobRecomputesSafetyDays(t *testing.T) {
	root := startInstance(t)
	// Fri 2026-09-18 is 3 trading days before Wed 2026-09-23.
	writeMinutes(t, "SD", minuteBar{et(2026, 9, 18, 10, 0), 10, 7})
	svc, _ := newLeader(t, root, time.Now, "SD")
	require.NoError(t, svc.RunDaily(day(t, 2026, 9, 23)))

	rows, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, "SD", et(2026, 9, 17, 0, 0), et(2026, 9, 23, 0, 0))
	require.NoError(t, err)
	var dates []string
	for _, r := range rows {
		dates = append(dates, r.Date.Format("2006-01-02"))
	}
	assert.Equal(t, []string{"2026-09-18", "2026-09-21", "2026-09-22", "2026-09-23"}, dates)
	assert.Equal(t, int64(7), rows[0].RegVolume)
}

// Online rebuild: queued dates are journaled and recomputed by the drain; a
// replica cannot queue.
func TestQueueRebuildIsRecomputed(t *testing.T) {
	root := startInstance(t)
	writeMinutes(t, "QR",
		minuteBar{et(2026, 9, 21, 10, 0), 10, 5},
		minuteBar{et(2026, 9, 22, 10, 0), 11, 6},
	)
	svc, _ := newLeader(t, root, time.Now)
	n, err := svc.QueueRebuild(nil, et(2026, 9, 19, 0, 0), et(2026, 9, 22, 0, 0))
	require.NoError(t, err)
	assert.Equal(t, 2, n, "one symbol x two trading days (the 19th/20th are a weekend)")

	n, err = svc.QueueRebuild([]string{"QR"}, et(2026, 9, 21, 0, 0), et(2026, 9, 21, 0, 0))
	require.NoError(t, err)
	assert.Equal(t, 0, n, "already queued")

	require.NoError(t, svc.DrainDirty())
	rows, err := sessionfacts.Read(executor.ThisInstance.CatalogDir, "QR", et(2026, 9, 21, 0, 0), et(2026, 9, 22, 0, 0))
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, int64(5), rows[0].RegVolume)
	assert.Equal(t, int64(6), rows[1].RegVolume)
	assert.Empty(t, svc.Journal().Pending())

	replica, err := sessionfacts.NewService(sessionfacts.Config{})
	require.NoError(t, err)
	_, err = replica.QueueRebuild(nil, et(2026, 9, 21, 0, 0), et(2026, 9, 22, 0, 0))
	assert.True(t, errors.Is(err, sessionfacts.ErrReplica))
}

// A day still in progress is computed from the bars so far but not cached:
// a later read sees bars written since.
func TestIncompleteDayIsNotCached(t *testing.T) {
	root := startInstance(t)
	clock := et(2026, 9, 22, 11, 0)
	svc, _ := newLeader(t, root, func() time.Time { return clock })
	d := day(t, 2026, 9, 22)

	writeMinutes(t, "IC", minuteBar{et(2026, 9, 22, 10, 0), 10, 5})
	rows, err := svc.Rows("IC", []calendar.DaySessions{d})
	require.NoError(t, err)
	assert.Equal(t, int64(5), rows[0].RegVolume)

	writeMinutes(t, "IC", minuteBar{et(2026, 9, 22, 10, 30), 10, 7})
	rows, err = svc.Rows("IC", []calendar.DaySessions{d})
	require.NoError(t, err)
	assert.Equal(t, int64(12), rows[0].RegVolume, "not served from a cached partial row")
}
