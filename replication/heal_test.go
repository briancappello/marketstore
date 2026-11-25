package replication_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/replication/ledger"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// catalogAPI serves a catalog the way a master's query API does.
type catalogAPI struct {
	qs *frontend.QueryService
}

func (a catalogAPI) ListTBKs(context.Context) ([]string, error) { return nil, nil }

func (a catalogAPI) QueryRange(_ context.Context, tbk string, start, end int64) (io.ColumnSeriesMap, error) {
	csm, err := a.qs.ExecuteQuery(io.NewTimeBucketKey(tbk), time.Unix(start, 0).UTC(), time.Unix(end, 0).UTC(),
		0, false, nil)
	if errors.Is(err, frontend.ErrNoResults) {
		return io.NewColumnSeriesMap(), nil
	}
	return csm, err
}

func readFrom(cat *catalog.Directory) backfill.ReadFunc {
	api := catalogAPI{qs: frontend.NewQueryService(cat)}
	return func(ctx context.Context, tbk string, start, end int64) (io.ColumnSeriesMap, error) {
		return api.QueryRange(ctx, tbk, start, end)
	}
}

func rangeHealer(t *testing.T, master *masterFixture, r *replica) replication.RangeHealer {
	t.Helper()
	w, err := executor.NewWriter(r.md.CatalogDir, r.md.WALFile)
	require.NoError(t, err)
	api := catalogAPI{qs: frontend.NewQueryService(master.metadata.CatalogDir)}
	return func(ctx context.Context, tbk string, from, to int64) (int, error) {
		return backfill.HealRange(ctx, api, readFrom(r.md.CatalogDir), w.WriteCSM, tbk, from, to, time.Now().Unix())
	}
}

// The change-ledger spec, end to end: a replica away past the master's log
// retention heals an old fixed-bucket correction from the ledger, reports the
// tick range it cannot re-pull, and goes on streaming.
func TestGapPastRetentionHealsFixedAndReportsTicks(t *testing.T) {
	f := newMasterFixtureWith(t, replog.Options{SegmentBytes: 4 << 10, MaxBytes: 16 << 10})
	h := serveLogAndLedger(t, f.log, f.ledger)
	nov20 := time.Date(2026, 11, 20, 14, 30, 0, 0, time.UTC).Unix()
	nov24 := time.Date(2026, 11, 24, 14, 30, 0, 0, time.UTC).Unix()

	// Before the outage: history the replica receives.
	require.NoError(t, f.writer.QueueCSM(barsCSM("MSFT/1Min/OHLCV", nov20, 30, 1), false))
	require.NoError(t, f.writer.QueueCSM(ticksCSM("AAPL/1Sec/TRADE", nov20, 5, 1), true))
	require.NoError(t, f.metadata.WALFile.FlushToWAL())

	r := newReplica(t)
	a, cancelApplier, done := runBatchApplier(t, r, replication.BatchOptions{MaxEntries: 50})
	defer func() { cancelApplier(); <-done }()
	require.NoError(t, a.SetCursor(context.Background(), 0))
	waitCursor(t, a, 0)
	healer := replication.NewLedgerHealer(h.client, rangeHealer(t, f, r))
	cancelRecv, _ := runReceiver(h, a, healer)
	waitCursor(t, a, f.log.LastSeq())
	cancelRecv() // the outage
	cursor := f.log.LastSeq()

	// During the outage: a correction of old bars, new ticks, and enough
	// other traffic that retention deletes all of it from the log.
	require.NoError(t, f.writer.QueueCSM(barsCSM("MSFT/1Min/OHLCV", nov20, 30, 2), false))
	require.NoError(t, f.metadata.WALFile.FlushToWAL())
	require.NoError(t, f.writer.QueueCSM(ticksCSM("AAPL/1Sec/TRADE", nov24, 7, 9), true))
	require.NoError(t, f.metadata.WALFile.FlushToWAL())
	for i := 0; i < 400; i++ {
		require.NoError(t, f.writer.QueueCSM(barsCSM("SPY/1Min/OHLCV", nov24+int64(i)*60, 1, float32(i)), false))
		require.NoError(t, f.metadata.WALFile.FlushToWAL())
	}
	require.NoError(t, f.log.Sync())
	f.log.EnforceRetention()
	require.Greater(t, f.log.FirstSeq(), cursor+2, "the correction and the ticks must be past retention")

	before := testutil.ToFloat64(replication.UnrecoverableTickGapsMetric())
	cancelRecv, _ = runReceiver(h, a, healer)
	defer cancelRecv()
	waitCursor(t, a, f.log.LastSeq())

	want, got := dump(t, f.metadata.CatalogDir), dump(t, r.md.CatalogDir)
	require.Equal(t, want["MSFT/1Min/OHLCV"], got["MSFT/1Min/OHLCV"], "the old correction is healed")
	require.Equal(t, want["SPY/1Min/OHLCV"], got["SPY/1Min/OHLCV"])
	require.Len(t, got["AAPL/1Sec/TRADE"]["Price"], 5, "the missed ticks are not re-pulled")
	require.Len(t, want["AAPL/1Sec/TRADE"]["Price"], 12)
	require.Equal(t, before+1, testutil.ToFloat64(replication.UnrecoverableTickGapsMetric()))
}

// A replica whose cursor is older than the ledger's history logs that a full
// resync is required, raises the metric, and carries on.
func TestGapOlderThanLedgerRequiresFullResync(t *testing.T) {
	l, err := replog.Open(t.TempDir(), replog.Options{})
	require.NoError(t, err)
	defer l.Close()
	lg := ledger.New(0)
	lg.Forget(5000) // e.g. two months of history expired
	h := serveLogAndLedger(t, l, lg)

	healer := replication.NewLedgerHealer(h.client, func(context.Context, string, int64, int64) (int, error) {
		return 0, fmt.Errorf("nothing should be healed")
	})
	require.NoError(t, healer.HealGap(context.Background(), 100, 6000))
	require.Equal(t, float64(1), testutil.ToFloat64(replication.FullResyncRequiredMetric()))
}

func TestChangesSinceStreamsInChunks(t *testing.T) {
	l, err := replog.Open(t.TempDir(), replog.Options{})
	require.NoError(t, err)
	defer l.Close()
	f := newMasterFixture(t)
	lg := ledger.New(0)
	for i := 0; i < 2500; i++ {
		require.NoError(t, f.writer.QueueCSM(barsCSM(fmt.Sprintf("S%04d/1Min/OHLCV", i), 1795600000, 1, 1), false))
	}
	require.NoError(t, f.metadata.WALFile.FlushToWAL())
	es, err := f.log.Read(1, 1, 0)
	require.NoError(t, err)
	lg.Record(1, 1, es[0].Data)
	h := serveLogAndLedger(t, l, lg)

	s, err := h.client.ChangesSince(context.Background(), &pb.ChangesSinceRequest{SinceSeq: 0})
	require.NoError(t, err)
	var chunks, buckets int
	for {
		resp, err := s.Recv()
		if err != nil {
			break
		}
		chunks++
		buckets += len(resp.GetBuckets())
		require.Equal(t, uint64(1), resp.GetAsOfSeq())
	}
	require.Equal(t, 3, chunks)
	require.Equal(t, 2500, buckets)
}
