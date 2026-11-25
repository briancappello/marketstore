package replication_test

import (
	"bufio"
	"encoding/binary"
	goio "io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// readWALTransactionGroups returns the serialized transaction groups in a WAL
// file, in file order, stopping at the first torn or unknown message.
func readWALTransactionGroups(t *testing.T, path string) [][]byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	var tgs [][]byte
	for {
		mid, err := r.ReadByte()
		if err != nil {
			return tgs
		}
		switch executor.MIDEnum(mid) {
		case executor.TGDATA:
			var lenBuf [8]byte
			if _, err := goio.ReadFull(r, lenBuf[:]); err != nil {
				return tgs
			}
			n := int64(binary.LittleEndian.Uint64(lenBuf[:]))
			if n <= 0 || n > 1<<31 {
				return tgs
			}
			tg := make([]byte, n)
			if _, err := goio.ReadFull(r, tg); err != nil {
				return tgs
			}
			if _, err := r.Discard(16); err != nil { // md5
				return tgs
			}
			tgs = append(tgs, tg)
		case executor.TXNINFO, executor.STATUS:
			if _, err := r.Discard(10); err != nil {
				return tgs
			}
		default:
			return tgs
		}
	}
}

// TestReplayApplyThroughput measures replica apply throughput for transaction
// groups captured from a real WAL file: today's path (one WriteCSM, and so one
// WAL flush, per write set) against batches of transaction groups queued with
// QueueCSM and committed by a single flush.
//
// It runs only when MKTS_REPLAY_BENCH_WAL names a WAL file. The scratch
// database goes under MKTS_REPLAY_BENCH_DIR (default: next to the WAL file),
// so the numbers reflect that disk rather than a tmpfs.
func TestReplayApplyThroughput(t *testing.T) {
	walPath := os.Getenv("MKTS_REPLAY_BENCH_WAL")
	if walPath == "" {
		t.Skip("set MKTS_REPLAY_BENCH_WAL to a WAL file to run")
	}
	benchDir := os.Getenv("MKTS_REPLAY_BENCH_DIR")
	if benchDir == "" {
		benchDir = filepath.Dir(walPath)
	}
	limit := 2000
	if s := os.Getenv("MKTS_REPLAY_BENCH_TGS"); s != "" {
		var err error
		limit, err = strconv.Atoi(s)
		require.NoError(t, err)
	}

	tgs := readWALTransactionGroups(t, walPath)
	require.NotEmpty(t, tgs)
	if len(tgs) > limit {
		tgs = tgs[:limit]
	}
	var bytes, sets int
	for _, tg := range tgs {
		bytes += len(tg)
		sets += int(io.ToInt64(tg[8:16]))
	}
	t.Logf("workload: %d transaction groups, %d write sets, %.1f MiB", len(tgs), sets, float64(bytes)/(1<<20))

	root, err := os.MkdirTemp(benchDir, "replay-bench-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = true
	cfg.WALSyncInterval = 2 * time.Second // p1's wal_sync_interval
	c := di.NewContainer(cfg)
	metadata := executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())
	t.Cleanup(metadata.WALFile.Shutdown)
	writer, err := executor.NewWriter(c.GetCatalogDir(), metadata.WALFile)
	require.NoError(t, err)

	batched := func(batch int) func() error {
		return func() error {
			for start := 0; start < len(tgs); start += batch {
				end := min(start+batch, len(tgs))
				for _, tg := range tgs[start:end] {
					_, wtSets := executor.ParseTGData(tg, root)
					for i := range wtSets {
						csm, err := replication.WTSetToCSM(&wtSets[i])
						if err != nil {
							return err
						}
						if err := writer.QueueCSM(csm, wtSets[i].RecordType == io.VARIABLE); err != nil {
							return err
						}
					}
				}
				metadata.WALFile.RequestFlush()
			}
			return nil
		}
	}
	perSet := func() error {
		replayer := replication.NewReplayer(executor.ParseTGData, writer.WriteCSM, root)
		for _, tg := range tgs {
			if err := replayer.Replay(tg); err != nil {
				return err
			}
		}
		return nil
	}

	// Warm up: create every bucket and year file so the timed runs measure
	// steady-state apply, not catalog creation.
	require.NoError(t, batched(1000)())

	run := func(name string, fn func() error) float64 {
		start := time.Now()
		require.NoError(t, fn())
		el := time.Since(start)
		rate := float64(len(tgs)) / el.Seconds()
		t.Logf("%-22s %8.2fs  %10.1f TG/s  %12.1f sets/s", name, el.Seconds(), rate, float64(sets)/el.Seconds())
		return rate
	}
	base := run("per-set WriteCSM", perSet)
	b1 := run("batched 1 TG", batched(1))
	b100 := run("batched 100 TGs", batched(100))
	b1000 := run("batched 1000 TGs", batched(1000))
	t.Logf("speedup vs today: batch1 %.1fx, batch100 %.1fx, batch1000 %.1fx", b1/base, b100/base, b1000/base)
}
