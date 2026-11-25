package replication_test

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/ledger"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type masterFixture struct {
	root     string
	metadata *executor.InstanceMetadata
	writer   *executor.Writer
	log      *replog.Log
	ledger   *ledger.Ledger
	full     atomic.Bool
}

func newMasterFixture(t *testing.T) *masterFixture {
	t.Helper()
	f := &masterFixture{root: t.TempDir()}
	cfg := utils.NewDefaultConfig(f.root)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	f.metadata = ownInstance(c.GetCatalogDir(), c.GetInitWALFile())
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
	f.ledger = ledger.New(0)
	f.metadata.WALFile.SetReplicationLog(replication.NewMasterLog(f.log, f.ledger))
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

func snapshotDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	des, err := os.ReadDir(dir)
	require.NoError(t, err)
	out := map[string][]byte{}
	for _, de := range des {
		b, err := os.ReadFile(filepath.Join(dir, de.Name()))
		require.NoError(t, err)
		out[de.Name()] = b
	}
	return out
}

func restoreDir(t *testing.T, dir string, snap map[string][]byte) {
	t.Helper()
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.MkdirAll(dir, 0o700))
	for name, b := range snap {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), b, 0o600))
	}
}

// A master that dies after a transaction group's WAL commit but before its log
// entry is durable re-applies the group from the WAL on restart; replay must
// put it in the log exactly once, and must not re-log groups already there.
func TestWALReplayLogsGroupsMissingFromTheLogOnce(t *testing.T) {
	for _, lostAppend := range []bool{true, false} {
		t.Run(map[bool]string{true: "log entry lost", false: "log entry durable"}[lostAppend], func(t *testing.T) {
			f := newMasterFixture(t)
			const epoch = 1795600000
			logDir := f.log.Dir()

			f.writeBar(t, "AAPL/1Min/OHLCV", epoch, 1)
			require.NoError(t, f.log.Sync())
			beforeSecond := snapshotDir(t, logDir)
			f.writeBar(t, "AAPL/1Min/OHLCV", epoch+60, 2)
			want, err := f.log.Read(1, 10, 0)
			require.NoError(t, err)
			require.Len(t, want, 2)

			fi, err := f.metadata.WALFile.FilePtr.Stat()
			require.NoError(t, err)
			walBytes := make([]byte, fi.Size())
			_, err = f.metadata.WALFile.FilePtr.ReadAt(walBytes, 0)
			require.NoError(t, err)

			// The crash.
			require.NoError(t, f.log.Close())
			if lostAppend {
				restoreDir(t, logDir, beforeSecond)
			}

			// The restart: open the log, then replay the WAL left behind.
			l, err := replog.Open(logDir, replog.Options{})
			require.NoError(t, err)
			defer l.Close()
			if lostAppend {
				require.Equal(t, uint64(1), l.LastSeq())
			}
			for i := 0; i < 8; i++ {
				walBytes[3+i] = 1 // owned by the crashed instance
			}
			walPath := filepath.Join(f.root, "WALFile.1.walfile")
			require.NoError(t, os.WriteFile(walPath, walBytes, 0o600))
			cleaner := executor.NewWALCleaner("", 12345)
			cleaner.SetReplicationLog(replication.NewMasterLog(l, nil))
			require.NoError(t, cleaner.CleanupOldWALFiles([]string{walPath}))

			got, err := l.Read(1, 10, 0)
			require.NoError(t, err)
			require.Len(t, got, 2)
			for i := range got {
				require.Equal(t, want[i].Seq, got[i].Seq)
				require.Equal(t, want[i].TGID, got[i].TGID)
				require.Equal(t, want[i].Data, got[i].Data)
			}
		})
	}
}

// readOpensRange reads n one-minute bars of key starting at epoch0.
func (f *masterFixture) readOpensRange(t *testing.T, key string, epoch0 int64, n int) []float32 {
	t.Helper()
	q := planner.NewQuery(f.metadata.CatalogDir)
	q.AddTargetKey(io.NewTimeBucketKey(key))
	q.SetRange(time.Unix(epoch0, 0), time.Unix(epoch0+int64(n-1)*60, 0))
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

// ownInstance is executor.NewInstanceSetup without the aliasing: that returns
// the package-global ThisInstance, so every instance a test creates would be
// the last one created. Tests that run a master and replicas side by side
// need one InstanceMetadata each.
func ownInstance(catDir *catalog.Directory, wal *executor.WALFileType) *executor.InstanceMetadata {
	executor.NewInstanceSetup(catDir, wal)
	return &executor.InstanceMetadata{CatalogDir: catDir, WALFile: wal}
}
