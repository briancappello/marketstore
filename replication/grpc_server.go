package replication

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/alpacahq/marketstore/v4/metrics"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	defaultReplicationStreamChannelSize = 500
)

type GRPCReplicationServer struct {
	pb.UnimplementedReplicationServer
	CertFile    string
	CertKeyFile string
	mu          sync.Mutex // guards streamChannels
	// Key: IPAddr (e.g. "192.125.18.1:25"), Value: channel for messages sent to each gRPC stream
	streamChannels map[string]chan []byte
}

func NewGRPCReplicationServer() *GRPCReplicationServer {
	return &GRPCReplicationServer{
		streamChannels: map[string]chan []byte{},
	}
}

// Register creates and tracks the outbound buffer for one replica stream.
func (rs *GRPCReplicationServer) Register(addr string) chan []byte {
	ch := make(chan []byte, defaultReplicationStreamChannelSize)
	rs.mu.Lock()
	rs.streamChannels[addr] = ch
	rs.mu.Unlock()
	return ch
}

// Unregister stops tracking a replica stream. The channel is deliberately NOT
// closed: SendReplicationMessage may hold a snapshot referencing it, and a
// send on a closed channel panics. An untracked channel is simply GC'd.
func (rs *GRPCReplicationServer) Unregister(addr string) {
	rs.mu.Lock()
	delete(rs.streamChannels, addr)
	rs.mu.Unlock()
}

func getClientAddr(stream grpc.ServerStream) (string, error) {
	ctx := stream.Context()

	pr, ok := peer.FromContext(ctx)
	if !ok {
		return "", errors.New("failed to get client IP address")
	}
	return pr.Addr.String(), nil
}

func (rs *GRPCReplicationServer) GetWALStream(_ *pb.GetWALStreamRequest, stream pb.Replication_GetWALStreamServer,
) error {
	// prepare a channel to send messages
	clientAddr, err := getClientAddr(stream)
	if err != nil {
		return errors.Wrap(err, "failed to get client IP address")
	}
	log.Info(fmt.Sprintf("new replica connection from:%s", clientAddr))

	streamChannel := rs.Register(clientAddr)

	// infinite loop
	for {
		log.Debug("[master] waiting for write requests...")
		transactionGroup := <-streamChannel
		if transactionGroup == nil {
			log.Info("streamChannel for replication is closed.")
			break
		}

		err := stream.Send(&pb.GetWALStreamResponse{TransactionGroup: transactionGroup})
		if err != nil {
			log.Error(fmt.Sprintf("an error occurred while sending replication message:%s", err))
			break
		}
		log.Debug("successfully sent a replication message")
	}

	// when an error occurred / client connection is closed, stop tracking the
	// channel. It is deliberately NOT closed here: SendReplicationMessage may
	// hold a snapshot referencing it, and a send on a closed channel panics.
	rs.Unregister(clientAddr)
	log.Info(fmt.Sprintf("[master] closed replication connection: %v", clientAddr))

	return nil
}

func (rs *GRPCReplicationServer) SendReplicationMessage(transactionGroup []byte) {
	// Snapshot under lock, deliver outside the lock so a slow replica never
	// holds the map or blocks peers.
	rs.mu.Lock()
	targets := make(map[string]chan []byte, len(rs.streamChannels))
	for ip, ch := range rs.streamChannels {
		targets[ip] = ch
	}
	rs.mu.Unlock()

	for ip, channel := range targets {
		select {
		case channel <- transactionGroup:
			log.Debug("sending a replication message to %s", ip)
		default:
			// Replica too slow: drop for this replica only. Its gap is healed
			// by its backfill reconciler. Never block the master or peers.
			metrics.ReplicationDroppedMessages.Inc()
			log.Debug("replication stream buffer full for %s; dropped a transaction group", ip)
		}
	}
}
