package replication_test

import (
	"context"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type replica struct {
	root string
	md   *executor.InstanceMetadata
}

func newReplica(t *testing.T) *replica {
	t.Helper()
	r := &replica{root: t.TempDir()}
	cfg := utils.NewDefaultConfig(r.root)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	r.md = ownInstance(c.GetCatalogDir(), c.GetInitWALFile())
	return r
}

func ticksCSM(key string, sec int64, n int, price0 float64) io.ColumnSeriesMap {
	epochs := make([]int64, n)
	nanos := make([]int32, n)
	prices := make([]float64, n)
	sizes := make([]uint64, n)
	for i := 0; i < n; i++ {
		epochs[i] = sec
		nanos[i] = int32(i * 1000)
		prices[i] = price0 + float64(i)
		sizes[i] = uint64(100 + i)
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Nanoseconds", nanos)
	cs.AddColumn("Price", prices)
	cs.AddColumn("Size", sizes)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(key), cs)
	return csm
}

// recordBurst writes n transaction groups on a master and returns its log
// entries: bar rewrites (later writes must win), ticks continuing the same
// second across groups, and groups mixing fixed and variable writes.
func recordBurst(t *testing.T, n int) []replog.Entry {
	t.Helper()
	f := newMasterFixture(t)
	const base = 1795600000
	for i := 0; i < n; i++ {
		sym := []string{"AAPL", "MSFT", "NVDA"}[i%3]
		require.NoError(t, f.writer.QueueCSM(barsCSM(sym+"/1Min/OHLCV", base+int64(i%50)*60, 3, float32(i)), false))
		if i%2 == 0 {
			require.NoError(t, f.writer.QueueCSM(ticksCSM(sym+"/1Sec/TRADE", base+int64(i/10), 4, float64(i)), true))
		}
		require.NoError(t, f.metadata.WALFile.FlushToWAL())
	}
	es, err := f.log.Read(1, n, 0)
	require.NoError(t, err)
	require.Len(t, es, n)
	return es
}

// dump reads every user bucket in full.
func dump(t *testing.T, catDir *catalog.Directory) map[string]map[string]interface{} {
	t.Helper()
	out := map[string]map[string]interface{}{}
	keys := catalog.ListTimeBucketKeyNames(catDir)
	sort.Strings(keys)
	for _, k := range keys {
		if strings.HasPrefix(k, "__") {
			continue
		}
		q := planner.NewQuery(catDir)
		q.AddTargetKey(io.NewTimeBucketKey(k))
		q.SetRange(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
		parsed, err := q.Parse()
		require.NoError(t, err)
		r, err := executor.NewReader(parsed)
		require.NoError(t, err)
		csm, err := r.Read()
		require.NoError(t, err)
		for _, cs := range csm {
			cols := map[string]interface{}{}
			for _, name := range cs.GetColumnNames() {
				cols[name] = cs.GetColumn(name)
			}
			out[k] = cols
		}
	}
	return out
}

func runBatchApplier(t *testing.T, r *replica, opts replication.BatchOptions,
) (*replication.BatchApplier, context.CancelFunc, chan error) {
	t.Helper()
	a, err := replication.NewBatchApplier(r.md.CatalogDir, r.md.WALFile, r.root, opts)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	return a, cancel, done
}

// waitCursor waits until a's cursor is want. It fails only when the cursor
// stops moving for 30s, so a slow run (under -race, say) is not mistaken for
// a stuck one, and it says where the cursor stopped.
func waitCursor(t *testing.T, a replication.Applier, want uint64) {
	t.Helper()
	const stall = 30 * time.Second
	var last uint64
	lastOK := false
	moved := time.Now()
	for {
		c, ok, err := a.Cursor()
		require.NoError(t, err)
		if ok && c == want {
			return
		}
		if ok != lastOK || c != last {
			last, lastOK, moved = c, ok, time.Now()
		}
		if time.Since(moved) > stall {
			t.Fatalf("cursor stuck at %d (set: %v) for %v, want %d", c, ok, stall, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBatchedReplayMatchesPerEntryReplay(t *testing.T) {
	burst := recordBurst(t, 300)

	perEntry := newReplica(t)
	w, err := executor.NewWriter(perEntry.md.CatalogDir, perEntry.md.WALFile)
	require.NoError(t, err)
	pa := replication.NewReplayApplier(replication.NewReplayer(executor.ParseTGData, w.WriteCSM, perEntry.root))
	for _, e := range burst {
		require.NoError(t, pa.Apply(context.Background(), replication.Received{Seq: e.Seq, Wallclock: e.Wallclock, TG: e.Data}))
	}

	batched := newReplica(t)
	ba, cancel, done := runBatchApplier(t, batched, replication.BatchOptions{MaxEntries: 64})
	for _, e := range burst {
		require.NoError(t, ba.Apply(context.Background(), replication.Received{Seq: e.Seq, Wallclock: e.Wallclock, TG: e.Data}))
	}
	waitCursor(t, ba, 300)
	cancel()
	require.NoError(t, <-done)
	require.GreaterOrEqual(t, ba.Stats().Batches, 5)

	want := dump(t, perEntry.md.CatalogDir)
	require.Len(t, want, 6)
	require.Equal(t, want, dump(t, batched.md.CatalogDir))
	seq, ok, err := replication.ReadCursor(batched.md.CatalogDir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(300), seq)
}

func TestBurstIsDrainedInFullBatches(t *testing.T) {
	burst := recordBurst(t, 5000)
	r := newReplica(t)
	a, cancel, done := runBatchApplier(t, r, replication.BatchOptions{})
	defer func() { cancel(); <-done }()
	for _, e := range burst {
		require.NoError(t, a.Apply(context.Background(), replication.Received{Seq: e.Seq, Wallclock: e.Wallclock, TG: e.Data}))
	}
	waitCursor(t, a, 5000)
	st := a.Stats()
	t.Logf("5000 entries in %d batches", st.Batches)
	require.LessOrEqual(t, st.Batches, 5)
	require.Equal(t, 5000, st.Entries)
}

func TestLoneEntryIsAppliedWithinTheWait(t *testing.T) {
	burst := recordBurst(t, 2)
	r := newReplica(t)
	const wait = 50 * time.Millisecond
	a, cancel, done := runBatchApplier(t, r, replication.BatchOptions{Wait: wait})
	defer func() { cancel(); <-done }()

	require.NoError(t, a.Apply(context.Background(), replication.Received{Seq: 1, TG: burst[0].Data}))
	waitCursor(t, a, 1) // warm up: bucket creation is not the case under test

	start := time.Now()
	require.NoError(t, a.Apply(context.Background(), replication.Received{Seq: 2, TG: burst[1].Data}))
	waitCursor(t, a, 2)
	took := time.Since(start)
	t.Logf("lone entry applied after %v", took)
	require.GreaterOrEqual(t, took, wait-5*time.Millisecond, "it waited for company")
	require.Less(t, took, wait+250*time.Millisecond, "within the wait plus one flush")
}

// Master writes, log, gRPC stream, receiver and batch applier, end to end:
// the replica ends with the master's data and cursor.
func TestReplicaConvergesThroughStreamAndBatches(t *testing.T) {
	f := newMasterFixture(t)
	h := serveLog(t, f.log)
	r := newReplica(t)
	a, cancelApplier, done := runBatchApplier(t, r, replication.BatchOptions{MaxEntries: 50})
	defer func() { cancelApplier(); <-done }()
	require.NoError(t, a.SetCursor(context.Background(), 0)) // a replica bootstrapped before the first entry
	waitCursor(t, a, 0)
	cancelRecv, _ := runReceiver(h, a, &fakeHealer{})
	defer cancelRecv()

	const base = 1795600000
	for i := 0; i < 400; i++ {
		sym := []string{"AAPL", "MSFT"}[i%2]
		require.NoError(t, f.writer.QueueCSM(barsCSM(sym+"/1Min/OHLCV", base+int64(i%30)*60, 2, float32(i)), false))
		require.NoError(t, f.writer.QueueCSM(ticksCSM(sym+"/1Sec/TRADE", base+int64(i/7), 3, float64(i)), true))
		require.NoError(t, f.metadata.WALFile.FlushToWAL())
	}
	waitCursor(t, a, f.log.LastSeq())
	require.Equal(t, dump(t, f.metadata.CatalogDir), dump(t, r.md.CatalogDir))
	t.Logf("master last seq %d, first %d, tail first %d", f.log.LastSeq(), f.log.FirstSeq(), f.log.HotTailFirstSeq())
	last, err := f.log.Read(f.log.LastSeq(), 1, 0)
	require.NoError(t, err)
	require.Len(t, last, 1)
	require.Equal(t, last[0].Wallclock, a.Stats().NewestWallclock, "the master's log time reaches the replica")
}

func TestReplayMetricsShowDrainAndLag(t *testing.T) {
	burst := recordBurst(t, 300)
	r := newReplica(t)
	a, err := replication.NewBatchApplier(r.md.CatalogDir, r.md.WALFile, r.root, replication.BatchOptions{MaxEntries: 100})
	require.NoError(t, err)
	reg := prometheus.NewRegistry()
	clock := time.Unix(0, burst[len(burst)-1].Wallclock).Add(2 * time.Second)
	require.NoError(t, a.RegisterMetrics(reg, func() time.Time { return clock }))
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() { cancel(); <-done }()
	for _, e := range burst {
		require.NoError(t, a.Apply(context.Background(), replication.Received{Seq: e.Seq, Wallclock: e.Wallclock, TG: e.Data}))
	}
	waitCursor(t, a, 300)

	body := scrape(t, srv.URL)
	require.Equal(t, "300", metricValue(t, body, "replication_replay_applied_total"))
	require.Equal(t, "3", metricValue(t, body, "replication_replay_batches_total"))
	require.Equal(t, "3", metricValue(t, body, "replication_replay_batch_entries_count"))
	require.Equal(t, "300", metricValue(t, body, "replication_replay_batch_entries_sum"))
	require.Equal(t, "300", metricValue(t, body, "replication_replica_cursor"))
	require.Equal(t, "2", metricValue(t, body, "replication_replay_newest_applied_age_seconds"))
}
