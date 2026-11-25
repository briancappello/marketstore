package replication_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

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
