package replication_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils"
)

// slowApplier throttles a replica, so the master's burst outruns it.
type slowApplier struct {
	replication.Applier
	delay time.Duration
}

func (s slowApplier) Apply(ctx context.Context, e replication.Received) error {
	time.Sleep(s.delay)
	return s.Applier.Apply(ctx, e)
}

// runningReplica is a replica process: its applier and receiver.
type runningReplica struct {
	r       *replica
	applier *replication.BatchApplier
	stopRecv,
	stopApplier context.CancelFunc
	applierDone chan error
}

func startReplica(t *testing.T, root string, f *masterFixture, h *streamHarness) *runningReplica {
	t.Helper()
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = true // as in production: ticker flushes race the batches
	rr := &runningReplica{r: restartWith(t, cfg)}
	var cancel context.CancelFunc
	rr.applier, cancel, rr.applierDone = runBatchApplier(t, rr.r, replication.BatchOptions{MaxEntries: 200})
	rr.stopApplier = cancel
	if _, ok, err := rr.applier.Cursor(); err == nil && !ok {
		require.NoError(t, rr.applier.SetCursor(context.Background(), 0)) // bootstrapped before the first entry
		waitCursor(t, rr.applier, 0)
	}
	healer := replication.NewLedgerHealer(h.client, rangeHealer(t, f, rr.r))
	rr.stopRecv, _ = runReceiver(h, slowApplier{Applier: rr.applier, delay: time.Millisecond}, healer)
	return rr
}

func (rr *runningReplica) stop() {
	rr.stopRecv()
	rr.stopApplier()
	<-rr.applierDone
}

// The 7.1 integration test: a 1M-row backfill burst with ticks against a
// throttled replica, a replica crash and restart in the middle of it, then an
// outage longer than the master's (tiny) log retention.
func TestGaplessReplicationEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy: a 1M-row burst")
	}
	f := newMasterFixtureWith(t, replog.Options{SegmentBytes: 1 << 20, MaxBytes: 8 << 20, HotTailBytes: 1 << 20})
	h := serveLogAndLedger(t, f.log, f.ledger)
	const (
		// 2026-09-21, minute-aligned. Every bar must be in the past: a heal only
		// takes closed bars, as the backfill does.
		base    = 1789999980
		symbols = 20
		perSym  = 50 // bars per symbol per transaction group
		groups  = 1000
	)
	burst := func(from, to int) {
		for i := from; i < to; i++ {
			for s := 0; s < symbols; s++ {
				key := fmt.Sprintf("SYM%02d/1Min/OHLCV", s)
				require.NoError(t, f.writer.QueueCSM(barsCSM(key, base+int64(i*perSym)*60, perSym, float32(i)), false))
			}
			require.NoError(t, f.writer.QueueCSM(ticksCSM("TICKA/1Sec/TRADE", base+int64(i), 10, float64(i)), true))
			require.NoError(t, f.metadata.WALFile.FlushToWAL())
		}
	}

	root := t.TempDir()
	rr := startReplica(t, root, f, h)

	// Phase 1: the first half of the burst, then a replica crash while it is
	// still behind, and a restart on what the crash left on disk.
	burst(0, groups/2)
	c, _, err := rr.applier.Cursor()
	require.NoError(t, err)
	require.Less(t, c, f.log.LastSeq(), "the throttled replica should be behind")
	crashed := crashCopy(t, root)
	rr.stop()
	rr = startReplica(t, crashed, f, h)
	defer func() { rr.stop() }()
	burst(groups/2, groups)
	waitCursor(t, rr.applier, f.log.LastSeq())

	want, got := dump(t, f.metadata.CatalogDir), dump(t, rr.r.md.CatalogDir)
	require.Len(t, want, symbols+1)
	require.Equal(t, want, got, "fixed buckets and ticks match after the burst and the crash")
	rows := 0
	for s := 0; s < symbols; s++ {
		rows += len(want[fmt.Sprintf("SYM%02d/1Min/OHLCV", s)]["Epoch"].([]int64))
	}
	require.Equal(t, groups*symbols*perSym, rows, "a 1M-row burst")
	require.Len(t, got["TICKA/1Sec/TRADE"]["Epoch"], groups*10, "every tick once")

	// Phase 2: an outage longer than retention, with an old correction and
	// new ticks inside it.
	rr.stopRecv()
	cursor := f.log.LastSeq()
	require.NoError(t, f.writer.QueueCSM(barsCSM("SYM00/1Min/OHLCV", base, perSym, 999), false))
	require.NoError(t, f.writer.QueueCSM(ticksCSM("TICKB/1Sec/TRADE", base, 25, 1), true))
	require.NoError(t, f.metadata.WALFile.FlushToWAL())
	for i := 0; i < 200; i++ { // filler, well past the 8 MiB retention
		for s := 0; s < symbols; s++ {
			key := fmt.Sprintf("SYM%02d/1Min/OHLCV", s)
			require.NoError(t, f.writer.QueueCSM(barsCSM(key, base+int64((groups+i)*perSym)*60, perSym, 1), false))
		}
		require.NoError(t, f.metadata.WALFile.FlushToWAL())
	}
	require.NoError(t, f.log.Sync())
	f.log.EnforceRetention()
	require.Greater(t, f.log.FirstSeq(), cursor+1, "the outage outlasted retention")

	before := testutil.ToFloat64(replication.UnrecoverableTickGapsMetric())
	healer := replication.NewLedgerHealer(h.client, rangeHealer(t, f, rr.r))
	rr.stopRecv, _ = runReceiver(h, slowApplier{Applier: rr.applier, delay: time.Millisecond}, healer)
	waitCursor(t, rr.applier, f.log.LastSeq())

	want, got = dump(t, f.metadata.CatalogDir), dump(t, rr.r.md.CatalogDir)
	for s := 0; s < symbols; s++ {
		key := fmt.Sprintf("SYM%02d/1Min/OHLCV", s)
		require.Equal(t, want[key], got[key], "%s, including the old correction", key)
	}
	require.Equal(t, float32(999), got["SYM00/1Min/OHLCV"]["Open"].([]float32)[0])
	require.Equal(t, want["TICKA/1Sec/TRADE"], got["TICKA/1Sec/TRADE"], "ticks within retention stay exact")
	require.NotContains(t, got, "TICKB/1Sec/TRADE", "ticks past retention are not re-pulled")
	require.Equal(t, before+1, testutil.ToFloat64(replication.UnrecoverableTickGapsMetric()),
		"the tick gap past retention is reported")
}
