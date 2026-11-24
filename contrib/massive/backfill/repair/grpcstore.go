package repair

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/alpacahq/marketstore/v4/frontend"
	pb "github.com/alpacahq/marketstore/v4/proto"
)

// GRPCStore reads buckets from a running MarketStore server.
type GRPCStore struct {
	cli pb.MarketstoreClient
}

// NewGRPCStore wraps a gRPC connection to the server.
func NewGRPCStore(cc grpc.ClientConnInterface) *GRPCStore {
	return &GRPCStore{cli: pb.NewMarketstoreClient(cc)}
}

// ListTBKs returns every bucket as "Symbol/Timeframe/AttrGroup".
func (s *GRPCStore) ListTBKs(ctx context.Context) ([]string, error) {
	resp, err := s.cli.ListSymbols(ctx, &pb.ListSymbolsRequest{Format: pb.ListSymbolsRequest_TIME_BUCKET_KEY})
	if err != nil {
		return nil, fmt.Errorf("list symbols: %w", err)
	}
	return resp.GetResults(), nil
}

// FirstBar returns the timestamp of the bucket's first record.
func (s *GRPCStore) FirstBar(ctx context.Context, tbk string) (time.Time, bool, error) {
	resp, err := s.cli.Query(ctx, &pb.MultiQueryRequest{
		Requests: []*pb.QueryRequest{{
			Destination:      tbk,
			EpochStart:       0,
			EpochEnd:         time.Now().Add(24 * time.Hour).Unix(),
			LimitRecordCount: 1,
			LimitFromStart:   true,
		}},
	})
	if err != nil {
		return time.Time{}, false, fmt.Errorf("query %s: %w", tbk, err)
	}
	for _, r := range resp.GetResponses() {
		if r.GetResult() == nil {
			continue
		}
		csm, err := frontend.ToNumpyMultiDataSet(r.GetResult()).ToColumnSeriesMap()
		if err != nil {
			return time.Time{}, false, fmt.Errorf("decode %s: %w", tbk, err)
		}
		for _, cs := range csm {
			if epochs := cs.GetEpoch(); len(epochs) > 0 {
				return time.Unix(epochs[0], 0), true, nil
			}
		}
	}
	return time.Time{}, false, nil
}
