package replication_test

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	tool "github.com/alpacahq/marketstore/v4/cmd/tool/replication"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/internal/di"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/replication/ledger"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/sqlparser"
	"github.com/alpacahq/marketstore/v4/utils"
)

type instance struct {
	c   *di.Container
	wal *executor.WALFileType
}

func open(t *testing.T, root string) instance {
	t.Helper()
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	wal := c.GetInitWALFile()
	executor.NewInstanceSetup(c.GetCatalogDir(), wal)
	return instance{c: c, wal: wal}
}

func (in instance) setCursor(t *testing.T, seq uint64) {
	t.Helper()
	bw, err := executor.NewBatchWriter(in.c.GetCatalogDir(), in.wal)
	require.NoError(t, err)
	require.NoError(t, bw.QueueCSM(replication.CursorCSM(seq), false))
	require.NoError(t, bw.Commit())
}

// A replica stopped uncleanly leaves a WAL with an older cursor in it.
// set-cursor must replay that first, so a later start cannot undo it.
func TestSetCursorSurvivesAPendingWAL(t *testing.T) {
	root := t.TempDir()
	open(t, root).setCursor(t, 10) // never checkpointed: the WAL still holds it

	require.NoError(t, tool.SetCursor(root, 500))

	seq, ok, err := replication.ReadCursor(open(t, root).c.GetCatalogDir()) // a restart replays WALs
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(500), seq)
}

func serve(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestStatusShowsCursorMasterAndLag(t *testing.T) {
	// The replica, cursor 40, behind its gRPC query API.
	root := t.TempDir()
	r := open(t, root)
	r.setCursor(t, 40)
	atomic.StoreUint32(&frontend.Queryable, 1)
	w, err := executor.NewWriter(r.c.GetCatalogDir(), r.wal)
	require.NoError(t, err)
	gs := frontend.NewGRPCService(root, r.c.GetCatalogDir(), sqlparser.NewAggRunner(nil), w,
		frontend.NewQueryService(r.c.GetCatalogDir()))
	replicaConn := serve(t, func(s *grpc.Server) { pb.RegisterMarketstoreServer(s, gs) })

	// The master, last sequence 100.
	l, err := replog.Open(t.TempDir(), replog.Options{})
	require.NoError(t, err)
	defer l.Close()
	lg := ledger.New(0)
	lg.Forget(100)
	masterConn := serve(t, func(s *grpc.Server) {
		pb.RegisterReplicationServer(s, replication.NewGRPCReplicationServer(l, lg))
	})

	var out bytes.Buffer
	require.NoError(t, tool.Status(context.Background(), &out, backfill.NewGRPCClient(replicaConn),
		pb.NewReplicationClient(masterConn)))
	require.Equal(t, "cursor: 40\nmaster last sequence: 100\nlag: 60\n", out.String())
}
