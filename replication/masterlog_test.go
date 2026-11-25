package replication_test

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type masterFixture struct {
	root     string
	metadata *executor.InstanceMetadata
	writer   *executor.Writer
	log      *replog.Log
	full     atomic.Bool
}

func newMasterFixture(t *testing.T) *masterFixture {
	t.Helper()
	f := &masterFixture{root: t.TempDir()}
	cfg := utils.NewDefaultConfig(f.root)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	f.metadata = executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
	var err error
	f.writer, err = executor.NewWriter(c.GetCatalogDir(), f.metadata.WALFile)
	require.NoError(t, err)
	f.log, err = replog.Open(filepath.Join(f.root, "replication_log"), replog.Options{
		WriteAt: func(fp *os.File, b []byte, off int64) (int, error) {
			if f.full.Load() {
				return 0, syscall.ENOSPC
			}
			return fp.WriteAt(b, off)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.log.Close() })
	f.metadata.WALFile.SetReplicationLog(replication.NewMasterLog(f.log))
	return f
}

func (f *masterFixture) writeBar(t *testing.T, key string, epoch int64, open float32) {
	t.Helper()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{epoch})
	cs.AddColumn("Open", []float32{open})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(key), cs)
	require.NoError(t, f.writer.QueueCSM(csm, false))
	require.NoError(t, f.metadata.WALFile.FlushToWAL())
}

func (f *masterFixture) readOpens(t *testing.T, key string, epoch int64) []float32 {
	t.Helper()
	q := planner.NewQuery(f.metadata.CatalogDir)
	q.AddTargetKey(io.NewTimeBucketKey(key))
	q.SetRange(time.Unix(epoch-60, 0), time.Unix(epoch+60, 0))
	parsed, err := q.Parse()
	require.NoError(t, err)
	r, err := executor.NewReader(parsed)
	require.NoError(t, err)
	csm, err := r.Read()
	require.NoError(t, err)
	for _, cs := range csm {
		return cs.GetColumn("Open").([]float32)
	}
	return nil
}

func TestEveryCommittedGroupIsLoggedAndEmptyFlushesAreNot(t *testing.T) {
	f := newMasterFixture(t)
	const epoch = 1795600000
	f.writeBar(t, "AAPL/1Min/OHLCV", epoch, 1)
	require.NoError(t, f.metadata.WALFile.FlushToWAL()) // nothing queued
	f.writeBar(t, "AAPL/1Min/OHLCV", epoch+60, 2)
	require.NoError(t, f.metadata.WALFile.FlushToWAL())
	f.writeBar(t, "MSFT/1Min/OHLCV", epoch, 3)

	require.Equal(t, uint64(3), f.log.LastSeq())
	es, err := f.log.Read(1, 10, 0)
	require.NoError(t, err)
	require.Len(t, es, 3)
	var prevTG int64
	for i, e := range es {
		require.Equal(t, uint64(i+1), e.Seq)
		require.Greater(t, e.TGID, prevTG)
		prevTG = e.TGID
		tgID, sets := executor.ParseTGData(e.Data, f.root)
		require.Equal(t, e.TGID, tgID)
		require.Len(t, sets, 1)
	}
}

func TestFullLogDiskBreaksLogButNotWrites(t *testing.T) {
	f := newMasterFixture(t)
	const epoch = 1795600000
	f.writeBar(t, "AAPL/1Min/OHLCV", epoch, 1)

	f.full.Store(true)
	f.writeBar(t, "AAPL/1Min/OHLCV", epoch+60, 2) // FlushToWAL must still succeed
	f.writeBar(t, "AAPL/1Min/OHLCV", epoch+120, 3)
	require.Equal(t, []float32{1, 2, 3}, f.readOpens(t, "AAPL/1Min/OHLCV", epoch+60))
	broken, from := f.log.Broken()
	require.True(t, broken)
	require.Equal(t, uint64(2), from)

	f.full.Store(false)
	f.writeBar(t, "AAPL/1Min/OHLCV", epoch+180, 4)
	broken, _ = f.log.Broken()
	require.False(t, broken)
	require.Equal(t, uint64(4), f.log.LastSeq())
}
