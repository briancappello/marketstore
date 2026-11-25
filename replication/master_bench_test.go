package replication_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils"
)

// TestMasterWriteLatencyDuringBurst measures the master's WriteCSM latency
// (what write_csm_duration observes) during a backfill-like burst: concurrent
// writers, each committing 500-bar writes, with the WAL flushing as on taichi.
//
// "replication off" stands for the old push design, whose only cost on the
// write path was a non-blocking channel send. "replication on" adds the log
// append, the ledger update and a group-commit fsync loop, with one healthy
// and one stalled replica stream reading the log.
//
// It runs only when MKTS_MASTER_BENCH_DIR names a scratch directory on the
// disk to measure.
func TestMasterWriteLatencyDuringBurst(t *testing.T) {
	dir := os.Getenv("MKTS_MASTER_BENCH_DIR")
	if dir == "" {
		t.Skip("set MKTS_MASTER_BENCH_DIR to run")
	}
	writes := 4000
	if s := os.Getenv("MKTS_MASTER_BENCH_WRITES"); s != "" {
		var err error
		writes, err = strconv.Atoi(s)
		require.NoError(t, err)
	}
	for round := 1; round <= 2; round++ {
		for _, on := range []bool{false, true} {
			d := runMasterBurst(t, dir, on, writes)
			name := map[bool]string{false: "replication off", true: "replication on "}[on]
			t.Logf("round %d %s: %d writes  p50 %6.2fms  p90 %6.2fms  p99 %6.2fms  max %7.2fms",
				round, name, len(d), ms(pct(d, 50)), ms(pct(d, 90)), ms(pct(d, 99)), ms(d[len(d)-1]))
		}
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func pct(sorted []time.Duration, p int) time.Duration {
	return sorted[(len(sorted)-1)*p/100]
}

func runMasterBurst(t *testing.T, dir string, replicate bool, writes int) []time.Duration {
	t.Helper()
	root, err := os.MkdirTemp(dir, "master-bench-")
	require.NoError(t, err)
	defer os.RemoveAll(root)

	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = true
	cfg.WALSyncInterval = 500 * time.Millisecond // taichi's (the default)
	cfg.Replication.Enabled = replicate
	c := di.NewContainer(cfg)
	wal := c.GetInitWALFile()
	md := ownInstance(c.GetCatalogDir(), wal)
	defer func() {
		wal.Shutdown()
		c.CloseReplicationLog()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if replicate {
		h := serveLogAndLedger(t, c.GetReplicationLog(), nil)
		_ = h.open(t, ctx, &pb.GetWALStreamRequest{Resumable: true}) // stalled: never read
		healthy := h.open(t, ctx, &pb.GetWALStreamRequest{Resumable: true})
		go func() {
			for {
				if _, err := healthy.Recv(); err != nil {
					return
				}
			}
		}()
	}

	const (
		writers = 8
		symbols = 25 // per writer
		bars    = 500
	)
	write := func(w *executor.Writer, writer, i int) time.Duration {
		key := fmt.Sprintf("W%dS%02d/1Min/OHLCV", writer, i%symbols)
		epoch := int64(1700000000 + (i/symbols)*bars*60)
		start := time.Now()
		require.NoError(t, w.WriteCSM(barsCSM(key, epoch, bars, float32(i)), false))
		return time.Since(start)
	}

	var (
		mu  sync.Mutex
		out []time.Duration
		wg  sync.WaitGroup
	)
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			w, err := executor.NewWriter(md.CatalogDir, md.WALFile)
			require.NoError(t, err)
			for i := 0; i < symbols; i++ { // warm up: create the buckets
				write(w, g, i)
			}
			local := make([]time.Duration, 0, writes/writers)
			for i := 0; i < writes/writers; i++ {
				local = append(local, write(w, g, symbols+i))
			}
			mu.Lock()
			out = append(out, local...)
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
