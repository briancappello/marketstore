package replication

import (
	"errors"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/alpacahq/marketstore/v4/metrics"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

const (
	// One read from the log per this many entries or bytes, whichever first.
	streamBatchEntries = 256
	streamBatchBytes   = 4 << 20
)

// GRPCReplicationServer serves the master's replication log. Every stream
// reads the log from its own cursor at its own pace: a slow or stalled replica
// only blocks its own stream goroutine (in gRPC flow control), never the
// master's write path or another replica.
type GRPCReplicationServer struct {
	pb.UnimplementedReplicationServer
	CertFile    string
	CertKeyFile string

	log *replog.Log
}

func NewGRPCReplicationServer(l *replog.Log) *GRPCReplicationServer {
	return &GRPCReplicationServer{log: l}
}

func getClientAddr(stream grpc.ServerStream) string {
	if pr, ok := peer.FromContext(stream.Context()); ok {
		return pr.Addr.String()
	}
	return "unknown"
}

// GetWALStream streams log entries from req.start_seq, or from the current
// tail when it is 0. A resumable replica is told about a gap (and the stream
// ends, so it can heal and reconnect from the gap's end); an older replica
// skips it, as it used to drop messages.
func (rs *GRPCReplicationServer) GetWALStream(req *pb.GetWALStreamRequest, stream pb.Replication_GetWALStreamServer,
) error {
	addr := getClientAddr(stream)
	ctx := stream.Context()
	next := req.GetStartSeq()
	if next == 0 {
		next = rs.log.NextSeq()
	}
	log.Info("replica %s connected: streaming from sequence %d (resumable=%v)", addr, next, req.GetResumable())
	defer log.Info("[master] closed replication connection: %v", addr)

	for {
		// Take the notification channel before reading, so an append that
		// lands between the read and the wait still wakes us.
		notify := rs.log.Notify()
		entries, err := rs.log.Read(next, streamBatchEntries, streamBatchBytes)
		var gap *replog.GapError
		switch {
		case errors.As(err, &gap):
			if !req.GetResumable() {
				metrics.ReplicationDroppedMessages.Add(float64(gap.Next - next))
				log.Warn("replica %s: skipping sequences %d-%d (%s)", addr, next, gap.Next-1, gap.Reason)
				next = gap.Next
				continue
			}
			log.Warn("replica %s: sequences %d-%d unavailable (%s); sent a gap notice", addr, next, gap.Next-1, gap.Reason)
			return stream.Send(&pb.GetWALStreamResponse{Body: &pb.GetWALStreamResponse_Gap{Gap: &pb.Gap{
				Requested:      next,
				FirstAvailable: gap.Next,
				Reason:         gapReason(gap.Reason),
			}}})
		case err != nil:
			return fmt.Errorf("read replication log at %d: %w", next, err)
		}

		for _, e := range entries {
			if err := stream.Send(&pb.GetWALStreamResponse{
				Seq:  e.Seq,
				Body: &pb.GetWALStreamResponse_TransactionGroup{TransactionGroup: e.Data},
			}); err != nil {
				log.Info("replica %s: send failed: %v", addr, err)
				return err
			}
		}
		if len(entries) > 0 {
			next = entries[len(entries)-1].Seq + 1
			continue
		}
		select {
		case <-notify:
		case <-ctx.Done():
			return nil
		}
	}
}

func gapReason(r replog.GapReason) pb.Gap_Reason {
	switch r {
	case replog.GapRetention:
		return pb.Gap_RETENTION
	case replog.GapBroken:
		return pb.Gap_LOG_BROKEN
	}
	return pb.Gap_REASON_UNSPECIFIED
}
