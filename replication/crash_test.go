package replication_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils"
)

// crashCopy copies a live replica's data directory: the WAL and primary files
// as a killed process leaves them, with nothing checkpointed.
func crashCopy(t *testing.T, from string) string {
	t.Helper()
	to := t.TempDir()
	require.NoError(t, filepath.Walk(from, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, p)
		dst := filepath.Join(to, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, 0o700)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o600)
	}))
	return to
}

// restart opens a replica on a crashed copy: WAL replay runs as at startup.
func restart(t *testing.T, root string) *replica {
	t.Helper()
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	return &replica{root: root, md: executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())}
}

// reference applies entries one at a time on a fresh replica.
func reference(t *testing.T, entries []replog.Entry) *replica {
	t.Helper()
	r := newReplica(t)
	w, err := executor.NewWriter(r.md.CatalogDir, r.md.WALFile)
	require.NoError(t, err)
	a := replication.NewReplayApplier(replication.NewReplayer(executor.ParseTGData, w.WriteCSM, r.root))
	for _, e := range entries {
		require.NoError(t, a.Apply(context.Background(), e.Seq, e.Data))
	}
	return r
}

func applyAll(t *testing.T, a *replication.BatchApplier, entries []replog.Entry) {
	t.Helper()
	for _, e := range entries {
		require.NoError(t, a.Apply(context.Background(), e.Seq, e.Data))
	}
}

func requireCursor(t *testing.T, r *replica, want uint64) {
	t.Helper()
	seq, ok, err := replication.ReadCursor(r.md.CatalogDir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, want, seq)
}

func TestReplicaCrashKeepsCursorAndTicksExact(t *testing.T) {
	burst := recordBurst(t, 300)
	at200 := dump(t, reference(t, burst[:200]).md.CatalogDir)
	final := dump(t, reference(t, burst).md.CatalogDir)

	live := newReplica(t)
	a, cancel, done := runBatchApplier(t, live, replication.BatchOptions{MaxEntries: 100, Wait: time.Hour})
	defer func() { cancel(); <-done }()
	applyAll(t, a, burst[:200]) // two full batches, committed
	waitCursor(t, a, 200)

	t.Run("killed after a batch flush", func(t *testing.T) {
		r := restart(t, crashCopy(t, live.root))
		requireCursor(t, r, 200)
		require.Equal(t, at200, dump(t, r.md.CatalogDir), "ticks once each, nothing missing")

		// It resumes at 201 and converges.
		a2, cancel2, done2 := runBatchApplier(t, r, replication.BatchOptions{MaxEntries: 100})
		defer func() { cancel2(); <-done2 }()
		c, ok, err := a2.Cursor()
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(200), c)
		applyAll(t, a2, burst[200:])
		waitCursor(t, a2, 300)
		require.Equal(t, final, dump(t, r.md.CatalogDir))
	})

	t.Run("killed while a batch is being built", func(t *testing.T) {
		applyAll(t, a, burst[200:250]) // held: the batch is not full and Wait is an hour
		time.Sleep(50 * time.Millisecond)
		c, _, err := a.Cursor()
		require.NoError(t, err)
		require.Equal(t, uint64(200), c, "the open batch must not be committed yet")

		r := restart(t, crashCopy(t, live.root))
		requireCursor(t, r, 200)
		require.Equal(t, at200, dump(t, r.md.CatalogDir))

		a2, cancel2, done2 := runBatchApplier(t, r, replication.BatchOptions{MaxEntries: 100})
		defer func() { cancel2(); <-done2 }()
		applyAll(t, a2, burst[200:]) // the batch is applied once, from 201
		waitCursor(t, a2, 300)
		require.Equal(t, final, dump(t, r.md.CatalogDir))
	})
}
