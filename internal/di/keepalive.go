package di

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// gRPC keepalive for replication.
//
// A replica's connections to its master sit idle for hours at night and over
// weekends. Without application-level pings, a connection that dies
// underneath (a Wi-Fi stall, a reset lost on the way) is only noticed when
// one side next writes, and a closed stream carries no reason. Both sides
// therefore ping every keepaliveTime while idle and give up after
// keepaliveTimeout without an ack; the resumable stream then reconnects from
// its cursor.
//
// The server must accept the client's pings: gRPC closes a connection with
// GOAWAY "too_many_pings" when a client pings more often than the server's
// enforcement MinTime (default 5 minutes). keepaliveMinTime leaves the client
// interval a margin of 3x.
const (
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 20 * time.Second
	keepaliveMinTime = 10 * time.Second
)

func clientKeepalive() grpc.DialOption {
	return grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                keepaliveTime,
		Timeout:             keepaliveTimeout,
		PermitWithoutStream: true,
	})
}

func serverKeepalive() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             keepaliveMinTime,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    keepaliveTime,
			Timeout: keepaliveTimeout,
		}),
	}
}
