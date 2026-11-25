package ledger_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/replication/ledger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// capture collects the transaction groups an executor commits.
type capture struct{ tgs [][]byte }

func (c *capture) LogTransactionGroup(_ int64, tg []byte)        { c.tgs = append(c.tgs, tg) }
func (c *capture) LogReplayedTransactionGroup(_ int64, _ []byte) {}

type master struct {
	t   *testing.T
	wal *executor.WALFileType
	w   *executor.Writer
	cap *capture
}

func newMaster(t *testing.T) *master {
	t.Helper()
	cfg := utils.NewDefaultConfig(t.TempDir())
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	wal := c.GetInitWALFile()
	executor.NewInstanceSetup(c.GetCatalogDir(), wal)
	w, err := executor.NewWriter(c.GetCatalogDir(), wal)
	require.NoError(t, err)
	m := &master{t: t, wal: wal, w: w, cap: &capture{}}
	wal.SetReplicationLog(m.cap)
	return m
}

// commit queues csms and flushes them as one transaction group.
func (m *master) commit(csms ...io.ColumnSeriesMap) []byte {
	m.t.Helper()
	for _, csm := range csms {
		variable := false
		for tbk := range csm {
			variable = tbk.GetItemInCategory("AttributeGroup") == "TRADE"
		}
		require.NoError(m.t, m.w.QueueCSM(csm, variable))
	}
	require.NoError(m.t, m.wal.FlushToWAL())
	return m.cap.tgs[len(m.cap.tgs)-1]
}

func bars(key string, epochs ...int64) io.ColumnSeriesMap {
	opens := make([]float32, len(epochs))
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(key), cs)
	return csm
}

func ticks(key string, sec int64, n int) io.ColumnSeriesMap {
	epochs := make([]int64, n)
	nanos := make([]int32, n)
	prices := make([]float64, n)
	for i := range epochs {
		epochs[i], nanos[i], prices[i] = sec, int32(i), float64(i)
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Nanoseconds", nanos)
	cs.AddColumn("Price", prices)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(key), cs)
	return csm
}

func day(y, m, d int) int64 { return time.Date(y, time.Month(m), d, 14, 30, 0, 0, time.UTC).Unix() }

func TestRecordsFixedAndVariableRanges(t *testing.T) {
	m := newMaster(t)
	e0 := day(2026, 11, 24)
	tg := m.commit(bars("AAPL/1Min/OHLCV", e0, e0+60, e0+120), ticks("AAPL/1Sec/TRADE", e0+5, 3))

	l := ledger.New(0)
	l.Record(1, 1, tg)
	c := l.ChangesSince(0)
	require.False(t, c.Incomplete)
	require.Equal(t, uint64(1), c.AsOf)
	require.Equal(t, []ledger.Bucket{
		{Key: "AAPL/1Min/OHLCV", Interval: 60, Ranges: []ledger.Range{
			{FirstSeq: 1, LastSeq: 1, EpochMin: e0, EpochMax: e0 + 179, LastWall: 1}}},
		{Key: "AAPL/1Sec/TRADE", Variable: true, Interval: 1, Ranges: []ledger.Range{
			{FirstSeq: 1, LastSeq: 1, EpochMin: e0 + 5, EpochMax: e0 + 5, LastWall: 1}}},
	}, c.Buckets)
}

func TestOverlappingAndAdjacentWritesCoalesce(t *testing.T) {
	m := newMaster(t)
	e := day(2026, 11, 20)
	l := ledger.New(0)
	l.Record(1, 1, m.commit(bars("NVDA/1Min/OHLCV", e, e+60)))
	l.Record(2, 2, m.commit(bars("NVDA/1Min/OHLCV", e+120)))   // adjacent
	l.Record(3, 3, m.commit(bars("NVDA/1Min/OHLCV", e+30*60))) // separate
	l.Record(4, 4, m.commit(bars("NVDA/1Min/OHLCV", e+60, e))) // overlaps the first

	rs := l.ChangesSince(0).Buckets[0].Ranges
	require.Equal(t, []ledger.Range{
		{FirstSeq: 1, LastSeq: 4, EpochMin: e, EpochMax: e + 179, LastWall: 4},
		{FirstSeq: 3, LastSeq: 3, EpochMin: e + 1800, EpochMax: e + 1859, LastWall: 3},
	}, rs)

	// Since 3: only the range touched afterwards.
	c := l.ChangesSince(3)
	require.Len(t, c.Buckets, 1)
	require.Equal(t, uint64(4), c.Buckets[0].Ranges[0].LastSeq)
	require.Len(t, c.Buckets[0].Ranges, 1)
	require.Empty(t, l.ChangesSince(4).Buckets)
}

func TestRangeCapMergesClosestRanges(t *testing.T) {
	m := newMaster(t)
	e := day(2026, 11, 2)
	l := ledger.New(3)
	// Disjoint single bars; the gaps after them grow, except the 4th-5th.
	for i, off := range []int64{0, 3600, 3 * 3600, 7 * 3600, 7*3600 + 300} {
		l.Record(uint64(i+1), int64(i+1), m.commit(bars("MSFT/1Min/OHLCV", e+off)))
	}
	rs := l.ChangesSince(0).Buckets[0].Ranges
	require.Len(t, rs, 3)
	// Every written epoch is still covered.
	for _, off := range []int64{0, 3600, 3 * 3600, 7 * 3600, 7*3600 + 300} {
		covered := false
		for _, r := range rs {
			covered = covered || (r.EpochMin <= e+off && e+off <= r.EpochMax)
		}
		require.True(t, covered, "epoch %d lost", e+off)
	}
	// The closest pairs merged: 4th+5th (300s apart), then 1st+2nd (3600s).
	require.Equal(t, e, rs[0].EpochMin)
	require.Equal(t, e+3600+59, rs[0].EpochMax)
	require.Equal(t, e+7*3600, rs[2].EpochMin)
}

func TestManyWritesOverTwoDaysStayBounded(t *testing.T) {
	m := newMaster(t)
	d1, d2 := day(2026, 11, 23), day(2026, 11, 24)
	l := ledger.New(0)
	rng := rand.New(rand.NewSource(1))
	const writes = 3000
	for i := 0; i < writes; i++ {
		base := d1
		if i%2 == 1 {
			base = d2
		}
		minute := int64(rng.Intn(390))
		l.Record(uint64(i+1), int64(i+1), m.commit(bars("AAPL/1Min/OHLCV", base+minute*60)))
	}
	rs := l.ChangesSince(0).Buckets[0].Ranges
	require.LessOrEqual(t, len(rs), ledger.DefaultMaxRanges)
	require.LessOrEqual(t, rs[0].EpochMin, d1)
	require.GreaterOrEqual(t, rs[len(rs)-1].EpochMax, d2)
	t.Logf("%d writes over two days -> %d ranges", writes, len(rs))
}

// The change-ledger spec's "Changes since a cursor" scenario.
func TestChangesSinceACursor(t *testing.T) {
	m := newMaster(t)
	l := ledger.New(0)
	for i := uint64(1); i <= 100; i++ {
		l.Record(i, int64(i), m.commit(bars("SPY/1Min/OHLCV", day(2026, 11, 1)+int64(i)*60)))
	}
	nov20, nov24 := day(2026, 11, 20), day(2026, 11, 24)
	l.Record(101, 101, m.commit(bars("NVDA/1Min/OHLCV", nov20, nov20+60)))
	l.Record(102, 102, m.commit(ticks("AAPL/1Sec/TRADE", nov24, 10)))

	c := l.ChangesSince(100)
	require.Len(t, c.Buckets, 2)
	byKey := map[string]ledger.Bucket{}
	for _, b := range c.Buckets {
		byKey[b.Key] = b
	}
	require.False(t, byKey["NVDA/1Min/OHLCV"].Variable)
	require.Equal(t, nov20, byKey["NVDA/1Min/OHLCV"].Ranges[0].EpochMin)
	require.True(t, byKey["AAPL/1Sec/TRADE"].Variable)
	require.Equal(t, nov24, byKey["AAPL/1Sec/TRADE"].Ranges[0].EpochMin)
}

func TestExpiryDropsOldRangesAndShortensHistory(t *testing.T) {
	m := newMaster(t)
	l := ledger.New(0)
	old := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	recent := time.Date(2026, 11, 24, 0, 0, 0, 0, time.UTC).UnixNano()
	l.Record(1, old, m.commit(bars("A/1Min/OHLCV", day(2026, 10, 1))))
	l.Record(2, recent, m.commit(bars("B/1Min/OHLCV", day(2026, 11, 24))))
	l.Expire(time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC).UnixNano())

	require.Equal(t, uint64(1), l.HistoryStart())
	c := l.ChangesSince(0)
	require.True(t, c.Incomplete, "changes up to 1 are forgotten")
	require.Len(t, c.Buckets, 1)
	require.Equal(t, "B/1Min/OHLCV", c.Buckets[0].Key)
	require.False(t, l.ChangesSince(1).Incomplete)
}
