package replication_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/ledger"
	"github.com/alpacahq/marketstore/v4/replication/replog"
)

// withoutWall drops LastWall: a live ledger stamps writes with the clock when
// they are recorded, a rebuilt one with their log time.
func withoutWall(c ledger.Changes) ledger.Changes {
	for i := range c.Buckets {
		rs := append([]ledger.Range(nil), c.Buckets[i].Ranges...)
		for j := range rs {
			rs[j].LastWall = 0
		}
		c.Buckets[i].Ranges = rs
	}
	return c
}

func (f *masterFixture) writeTGs(t *testing.T, from, n int) {
	t.Helper()
	const base = 1795600000
	for i := from; i < from+n; i++ {
		sym := []string{"AAPL", "MSFT", "NVDA"}[i%3]
		require.NoError(t, f.writer.QueueCSM(barsCSM(sym+"/1Min/OHLCV", base+int64(i)*3600, 2, float32(i)), false))
		if i%4 == 0 {
			require.NoError(t, f.writer.QueueCSM(ticksCSM(sym+"/1Sec/TRADE", base+int64(i)*3600, 2, float64(i)), true))
		}
		require.NoError(t, f.metadata.WALFile.FlushToWAL())
	}
}

func TestLedgerRebuildsFromSnapshotAndLog(t *testing.T) {
	f := newMasterFixture(t)
	snap := filepath.Join(t.TempDir(), "ledger.snap")
	f.writeTGs(t, 0, 50)
	replication.SaveLedger(f.ledger, snap, 0)
	f.writeTGs(t, 50, 30) // logged after the snapshot
	want := withoutWall(f.ledger.ChangesSince(0))
	require.Equal(t, uint64(80), want.AsOf)

	got := replication.RebuildLedger(snap, f.log, 0)
	require.Equal(t, want, withoutWall(got.ChangesSince(0)))
	require.Equal(t, withoutWall(f.ledger.ChangesSince(60)), withoutWall(got.ChangesSince(60)))

	// No snapshot at all: the whole log is replayed.
	got = replication.RebuildLedger(filepath.Join(t.TempDir(), "missing"), f.log, 0)
	require.Equal(t, want, withoutWall(got.ChangesSince(0)))
}

// Entries deleted by retention after the last snapshot leave a hole the
// ledger cannot fill: asking from before it must say so.
func TestLedgerRebuildPastTruncatedLogIsIncomplete(t *testing.T) {
	f := newMasterFixture(t)
	snap := filepath.Join(t.TempDir(), "ledger.snap")
	f.writeTGs(t, 0, 10)
	replication.SaveLedger(f.ledger, snap, 0)
	f.writeTGs(t, 10, 200)

	// Reopen the log with tight retention and drop the old segments.
	require.NoError(t, f.log.Sync())
	l, err := replog.Open(f.log.Dir(), replog.Options{SegmentBytes: 4096, MaxBytes: 1})
	require.NoError(t, err)
	defer l.Close()
	appendRaw := func() { // rotate past the old segment so retention can delete it
		es, err := f.log.Read(f.log.LastSeq(), 1, 0)
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			_, err := l.Append(es[0].TGID+int64(i+1), es[0].Data)
			require.NoError(t, err)
		}
	}
	appendRaw()
	l.EnforceRetention()
	first := l.FirstSeq()
	require.Greater(t, first, uint64(11))

	got := replication.RebuildLedger(snap, l, 0)
	require.Equal(t, first-1, got.HistoryStart())
	require.True(t, got.ChangesSince(10).Incomplete)
	c := got.ChangesSince(first - 1)
	require.False(t, c.Incomplete)
	require.NotEmpty(t, c.Buckets)
	require.Equal(t, l.LastSeq(), c.AsOf)
}

func TestLedgerExpiresByMaxAgeAcrossRestart(t *testing.T) {
	f := newMasterFixture(t)
	snap := filepath.Join(t.TempDir(), "ledger.snap")
	old := time.Now().Add(-40 * 24 * time.Hour).UnixNano()
	f.writeTGs(t, 0, 1)
	first, err := f.log.Read(1, 1, 0)
	require.NoError(t, err)
	stale := ledger.New(0)
	stale.Record(1, old, first[0].Data)
	replication.SaveLedger(stale, snap, 30*24*time.Hour)

	got, err := ledger.LoadSnapshot(snap, 0)
	require.NoError(t, err)
	require.Empty(t, got.ChangesSince(1).Buckets)
	require.Equal(t, uint64(1), got.HistoryStart(), "the expired range's history is gone")
	require.True(t, got.ChangesSince(0).Incomplete)
}
