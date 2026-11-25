package replication_test

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/alpacahq/marketstore/v4/metrics"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/replog"
)

// streamHarness runs a real replication gRPC server over an in-memory
// listener, so stream flow control behaves as it does over TCP.
type streamHarness struct {
	log    *replog.Log
	server *replication.GRPCReplicationServer
	client pb.ReplicationClient
}

func newStreamHarness(t *testing.T, opts replog.Options) *streamHarness {
	t.Helper()
	l, err := replog.Open(t.TempDir(), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	return serveLog(t, l)
}

// serveLog serves l over an in-memory gRPC connection.
func serveLog(t *testing.T, l *replog.Log) *streamHarness {
	t.Helper()
	h := &streamHarness{log: l, server: replication.NewGRPCReplicationServer(l)}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterReplicationServer(srv, h.server)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	h.client = pb.NewReplicationClient(conn)
	return h
}

func (h *streamHarness) appendN(t *testing.T, from, n, size int) {
	t.Helper()
	for i := from; i < from+n; i++ {
		data := make([]byte, size)
		copy(data, fmt.Sprintf("tg-%08d", i))
		_, err := h.log.Append(int64(i), data)
		require.NoError(t, err)
	}
}

func (h *streamHarness) open(t *testing.T, ctx context.Context, req *pb.GetWALStreamRequest,
) pb.Replication_GetWALStreamClient {
	t.Helper()
	s, err := h.client.GetWALStream(ctx, req)
	require.NoError(t, err)
	return s
}

// recvSeqs receives n transaction groups and checks each carries its sequence.
func recvSeqs(t *testing.T, s pb.Replication_GetWALStreamClient, n int) []uint64 {
	t.Helper()
	out := make([]uint64, 0, n)
	for len(out) < n {
		resp, err := s.Recv()
		require.NoError(t, err)
		require.NotNil(t, resp.GetTransactionGroup(), "unexpected %v", resp)
		require.Equal(t, fmt.Sprintf("tg-%08d", resp.GetSeq()), string(resp.GetTransactionGroup()[:11]))
		out = append(out, resp.GetSeq())
	}
	return out
}

func requireConsecutive(t *testing.T, seqs []uint64, first uint64) {
	t.Helper()
	for i, s := range seqs {
		require.Equal(t, first+uint64(i), s)
	}
}

// One replica that stops reading must not slow the master's appends or
// another replica's stream.
func TestStalledStreamDelaysNeitherMasterNorOtherStreams(t *testing.T) {
	h := newStreamHarness(t, replog.Options{HotTailBytes: 1 << 20})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_ = h.open(t, ctx, &pb.GetWALStreamRequest{StartSeq: 1, Resumable: true}) // never read
	healthy := h.open(t, ctx, &pb.GetWALStreamRequest{StartSeq: 1, Resumable: true})

	const n = 20000
	got := make(chan []uint64, 1)
	go func() { got <- recvSeqs(t, healthy, n) }()

	// 20k x 1 KiB is far beyond the stalled stream's flow-control window, so
	// its server goroutine is blocked in Send for most of this burst.
	start := time.Now()
	var slowest time.Duration
	for i := 1; i <= n; i++ {
		t0 := time.Now()
		data := make([]byte, 1024)
		copy(data, fmt.Sprintf("tg-%08d", i))
		_, err := h.log.Append(int64(i), data)
		require.NoError(t, err)
		if d := time.Since(t0); d > slowest {
			slowest = d
		}
	}
	t.Logf("appended %d entries in %v, slowest append %v", n, time.Since(start), slowest)
	require.Less(t, slowest, 250*time.Millisecond, "an append waited on a replica")

	select {
	case seqs := <-got:
		requireConsecutive(t, seqs, 1)
	case <-time.After(30 * time.Second):
		t.Fatal("the healthy stream was held up by the stalled one")
	}
}

// A replica built before sequencing sends an empty request and gets live
// entries from the tail, as before.
func TestLegacyStreamGetsTheTail(t *testing.T) {
	h := newStreamHarness(t, replog.Options{})
	h.appendN(t, 1, 10, 32)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := h.open(t, ctx, &pb.GetWALStreamRequest{})

	// The stream is registered once its first read has run; appends from
	// then on are live. Appending in a loop until one arrives avoids racing
	// the server goroutine's start.
	first := make(chan uint64, 1)
	go func() {
		resp, err := s.Recv()
		if err == nil {
			first <- resp.GetSeq()
		}
	}()
	next := 11
	var seq uint64
	for seq == 0 {
		h.appendN(t, next, 1, 32)
		next++
		select {
		case seq = <-first:
		case <-time.After(20 * time.Millisecond):
		}
	}
	require.Greater(t, seq, uint64(10), "a tail stream must not replay old entries")
	h.appendN(t, next, 5, 32)
	requireConsecutive(t, recvSeqs(t, s, int(uint64(next+4)-seq)), seq+1)
}

// A burst far bigger than the memory tail, read by a replica slower than the
// master, arrives complete and in order.
func TestNoEntryDroppedAcrossA100kBurst(t *testing.T) {
	h := newStreamHarness(t, replog.Options{HotTailBytes: 256 << 10, SegmentBytes: 4 << 20})
	const n = 100000
	h.appendN(t, 1, n/2, 200)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := h.open(t, ctx, &pb.GetWALStreamRequest{StartSeq: 1, Resumable: true})

	got := make(chan []uint64, 1)
	go func() { got <- recvSeqs(t, s, n) }()
	h.appendN(t, n/2+1, n/2, 200)

	select {
	case seqs := <-got:
		requireConsecutive(t, seqs, 1)
	case <-time.After(60 * time.Second):
		t.Fatal("burst not delivered")
	}
	require.Greater(t, h.log.DiskReads(), int64(0), "the burst should have outrun the memory tail")
}

func recvGap(t *testing.T, s pb.Replication_GetWALStreamClient) *pb.Gap {
	t.Helper()
	resp, err := s.Recv()
	require.NoError(t, err)
	gap := resp.GetGap()
	require.NotNil(t, gap, "expected a gap notice, got %v", resp)
	_, err = s.Recv()
	require.Error(t, err, "the stream ends after a gap notice")
	return gap
}

func TestGapNoticeForCursorBeforeRetention(t *testing.T) {
	h := newStreamHarness(t, replog.Options{SegmentBytes: 400, MaxBytes: 800})
	h.appendN(t, 1, 100, 16)
	require.NoError(t, h.log.Sync())
	h.log.EnforceRetention()
	first := h.log.FirstSeq()
	require.Greater(t, first, uint64(1))

	s := h.open(t, context.Background(), &pb.GetWALStreamRequest{StartSeq: 2, Resumable: true})
	gap := recvGap(t, s)
	require.Equal(t, pb.Gap_RETENTION, gap.GetReason())
	require.Equal(t, uint64(2), gap.GetRequested())
	require.Equal(t, first, gap.GetFirstAvailable())

	// Reconnecting at first_available streams from there.
	s = h.open(t, context.Background(), &pb.GetWALStreamRequest{StartSeq: first, Resumable: true})
	requireConsecutive(t, recvSeqs(t, s, int(100-first+1)), first)
}

func TestGapNoticeInsideBrokenRange(t *testing.T) {
	full := false
	h := newStreamHarness(t, replog.Options{WriteAt: func(f *os.File, b []byte, off int64) (int, error) {
		if full {
			return 0, syscall.ENOSPC
		}
		return f.WriteAt(b, off)
	}})
	h.appendN(t, 1, 5, 16)
	full = true
	for i := 0; i < 3; i++ {
		_, err := h.log.Append(int64(100+i), []byte("lost"))
		require.Error(t, err)
	}
	full = false
	h.appendN(t, 9, 2, 16)

	s := h.open(t, context.Background(), &pb.GetWALStreamRequest{StartSeq: 6, Resumable: true})
	gap := recvGap(t, s)
	require.Equal(t, pb.Gap_LOG_BROKEN, gap.GetReason())
	require.Equal(t, uint64(9), gap.GetFirstAvailable())
}

// An old replica cannot parse a gap notice, so its stream skips the gap.
func TestLegacyStreamSkipsGaps(t *testing.T) {
	h := newStreamHarness(t, replog.Options{SegmentBytes: 400, MaxBytes: 800})
	h.appendN(t, 1, 100, 16)
	require.NoError(t, h.log.Sync())
	h.log.EnforceRetention()
	first := h.log.FirstSeq()

	dropped := testutil.ToFloat64(metrics.ReplicationDroppedMessages)
	s := h.open(t, context.Background(), &pb.GetWALStreamRequest{StartSeq: 2})
	requireConsecutive(t, recvSeqs(t, s, int(100-first+1)), first)
	require.Equal(t, dropped+float64(first-2), testutil.ToFloat64(metrics.ReplicationDroppedMessages))
}

func TestReplicaLagMetricsPerStream(t *testing.T) {
	h := newStreamHarness(t, replog.Options{HotTailBytes: 1 << 20})
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(h.server))
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = h.open(t, ctx, &pb.GetWALStreamRequest{StartSeq: 1, Resumable: true}) // stalls
	healthy := h.open(t, ctx, &pb.GetWALStreamRequest{StartSeq: 1, Resumable: true})
	const n = 5000
	h.appendN(t, 1, n, 1024)
	requireConsecutive(t, recvSeqs(t, healthy, n), 1)

	lagOf := func(body, name string) map[string]float64 {
		out := map[string]float64{}
		re := regexp.MustCompile(`(?m)^alpaca_marketstore_` + name + `\{replica="[^"]*",stream="(\d+)"\} (\S+)$`)
		for _, m := range re.FindAllStringSubmatch(body, -1) {
			v, err := strconv.ParseFloat(m[2], 64)
			require.NoError(t, err)
			out[m[1]] = v
		}
		return out
	}
	var seqLag, byteLag map[string]float64
	require.Eventually(t, func() bool {
		body := scrape(t, srv.URL)
		seqLag = lagOf(body, "replication_replica_lag_seq")
		byteLag = lagOf(body, "replication_replica_lag_bytes")
		return len(seqLag) == 2 && (seqLag["1"] == 0 || seqLag["2"] == 0)
	}, 10*time.Second, 10*time.Millisecond)

	stalled, caughtUp := "1", "2"
	if seqLag["1"] == 0 {
		stalled, caughtUp = "2", "1"
	}
	require.Zero(t, seqLag[caughtUp])
	require.Zero(t, byteLag[caughtUp])
	require.Greater(t, seqLag[stalled], float64(1000), "the stalled stream is far behind")
	require.InDelta(t, seqLag[stalled]*(32+1024), byteLag[stalled], 1, "byte lag matches the entries left")
}
