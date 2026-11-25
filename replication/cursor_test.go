package replication_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func barsCSM(key string, epoch0 int64, n int, open float32) io.ColumnSeriesMap {
	epochs := make([]int64, n)
	opens := make([]float32, n)
	for i := range epochs {
		epochs[i] = epoch0 + int64(i)*60
		opens[i] = open
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Open", opens)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(key), cs)
	return csm
}

func TestCursorRoundTrip(t *testing.T) {
	root := t.TempDir()
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	md := executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	_, ok, err := replication.ReadCursor(md.CatalogDir)
	require.NoError(t, err)
	require.False(t, ok, "no cursor on a fresh replica")

	bw, err := executor.NewBatchWriter(md.CatalogDir, md.WALFile)
	require.NoError(t, err)
	for _, seq := range []uint64{42, 43, 1 << 40} {
		require.NoError(t, bw.QueueCSM(replication.CursorCSM(seq), false))
		require.NoError(t, bw.Commit())
		got, ok, err := replication.ReadCursor(md.CatalogDir)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, seq, got)
	}
}

// Batches committed while the WAL ticker flushes every millisecond and
// another writer queues its own writes must each land in one transaction
// group together with their cursor.
func TestCommittedBatchAndCursorShareOneTransactionGroup(t *testing.T) {
	root := t.TempDir()
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = true
	cfg.WALSyncInterval = time.Millisecond
	c := di.NewContainer(cfg)
	md := executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
	t.Cleanup(md.WALFile.Shutdown)

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // an unrelated writer
		defer wg.Done()
		w, err := executor.NewWriter(md.CatalogDir, md.WALFile)
		if err != nil {
			t.Error(err)
			return
		}
		for i := int64(0); !stop.Load(); i++ {
			if err := w.WriteCSM(barsCSM("OTHER/1Min/OHLCV", 1795600000+i*60, 1, 1), false); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	const batches, barsPerBatch = 50, 20
	bw, err := executor.NewBatchWriter(md.CatalogDir, md.WALFile)
	require.NoError(t, err)
	for k := 1; k <= batches; k++ {
		require.NoError(t, bw.QueueCSM(barsCSM("BATCH/1Min/OHLCV", 1795600000+int64(k)*3600, barsPerBatch, float32(k)), false))
		require.NoError(t, bw.QueueCSM(replication.CursorCSM(uint64(k)), false))
		require.NoError(t, bw.Commit())
	}
	stop.Store(true)
	wg.Wait()
	require.NoError(t, md.WALFile.FlushToWAL())

	seen := map[uint64]bool{}
	for _, tg := range readWALTransactionGroups(t, md.WALFile.FilePtr.Name()) {
		_, sets := executor.ParseTGData(tg, root)
		var cursors []uint64
		batchBars := 0
		for _, s := range sets {
			tbk, _, err := io.NewTimeBucketKeyFromWalKeyPath(s.FilePath)
			require.NoError(t, err)
			switch tbk.GetItemKey() {
			case replication.CursorKey:
				cursors = append(cursors, io.ToUInt64(s.Buffer.Payload()))
			case "BATCH/1Min/OHLCV":
				batchBars++
			}
		}
		if batchBars == 0 && len(cursors) == 0 {
			continue
		}
		require.Len(t, cursors, 1, "a batch's bars and its cursor must share a group")
		require.Equal(t, barsPerBatch, batchBars, "batch %d was split", cursors[0])
		seen[cursors[0]] = true
	}
	require.Len(t, seen, batches)
}

// The shared write channel is full; a batch five times its depth must still
// commit, without blocking, and take the queued writes along.
func TestBatchLargerThanFreeChannelCapacityCommits(t *testing.T) {
	root := t.TempDir()
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	catDir := c.GetCatalogDir()
	const depth = 100
	wf, err := executor.NewWALFile(root, 7, &executor.NopReplicationSender{}, false, &sync.WaitGroup{},
		executor.StartNewTriggerPluginDispatcher(nil), executor.NewTransactionPipeWithDepth(depth))
	require.NoError(t, err)
	md := executor.NewInstanceSetup(catDir, wf)

	w, err := executor.NewWriter(catDir, md.WALFile)
	require.NoError(t, err)
	require.NoError(t, w.QueueCSM(barsCSM("QUEUED/1Min/OHLCV", 1795600000, depth, 1), false)) // channel now full

	bw, err := executor.NewBatchWriter(catDir, md.WALFile)
	require.NoError(t, err)
	require.NoError(t, bw.QueueCSM(barsCSM("BIG/1Min/OHLCV", 1795600000, 5*depth, 2), false))
	require.NoError(t, bw.QueueCSM(replication.CursorCSM(9), false))
	require.Equal(t, 5*depth+1, bw.Len())

	done := make(chan error, 1)
	go func() { done <- bw.Commit() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Commit blocked on a full write channel")
	}

	f := &masterFixture{root: root, metadata: md}
	require.Len(t, f.readOpensRange(t, "BIG/1Min/OHLCV", 1795600000, 5*depth), 5*depth)
	require.Len(t, f.readOpensRange(t, "QUEUED/1Min/OHLCV", 1795600000, depth), depth)
	seq, ok, err := replication.ReadCursor(catDir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(9), seq)
}
