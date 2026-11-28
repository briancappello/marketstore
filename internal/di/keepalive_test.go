package di

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

func TestKeepaliveClientNeverPingsFasterThanTheServerAllows(t *testing.T) {
	assert.GreaterOrEqual(t, keepaliveTime, 3*keepaliveMinTime,
		"a client pinging faster than the server's MinTime is closed with too_many_pings")
	assert.Less(t, keepaliveTimeout, keepaliveTime)
}

// idleConnSurvives dials a server built with opts, with a client that pings
// every 10 s (gRPC's minimum), and reports whether the idle connection is
// still up after d.
func idleConnSurvives(t *testing.T, d time.Duration, opts ...grpc.ServerOption) bool {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer(opts...)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 10 * time.Second, Timeout: keepaliveTimeout, PermitWithoutStream: true,
		}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	conn.Connect()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for conn.GetState() != connectivity.Ready {
		require.True(t, conn.WaitForStateChange(ctx, conn.GetState()), "never connected")
	}
	ctx, cancel = context.WithTimeout(context.Background(), d)
	defer cancel()
	return !conn.WaitForStateChange(ctx, connectivity.Ready) // true only if it dropped
}

// The idle replication stream must survive the client's keepalive pings: with
// gRPC's default enforcement (5 min) the server closes it for too_many_pings.
func TestIdleConnectionSurvivesKeepalivePings(t *testing.T) {
	if testing.Short() {
		t.Skip("takes about 80 s")
	}
	t.Run("default server policy", func(t *testing.T) {
		t.Parallel()
		assert.False(t, idleConnSurvives(t, 80*time.Second), "closed for too_many_pings")
	})
	t.Run("serverKeepalive", func(t *testing.T) {
		t.Parallel()
		assert.True(t, idleConnSurvives(t, 80*time.Second, serverKeepalive()...), "accepts the pings")
	})
}
