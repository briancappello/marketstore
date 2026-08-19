package backfill

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/alpacahq/marketstore/v4/frontend"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// MasterAPI is the subset of the master's main gRPC service the backfill needs.
type MasterAPI interface {
	ListTBKs(ctx context.Context) ([]string, error)
	QueryRange(ctx context.Context, tbk string, startEpoch, endEpoch int64) (io.ColumnSeriesMap, error)
}

// GRPCClient reads from the master's main Marketstore gRPC service.
type GRPCClient struct {
	cli pb.MarketstoreClient
}

func NewGRPCClient(cc grpc.ClientConnInterface) *GRPCClient {
	return &GRPCClient{cli: pb.NewMarketstoreClient(cc)}
}

// ListTBKs returns every bucket on the master as "Symbol/Timeframe/AttrGroup".
func (c *GRPCClient) ListTBKs(ctx context.Context) ([]string, error) {
	resp, err := c.cli.ListSymbols(ctx, &pb.ListSymbolsRequest{
		Format: pb.ListSymbolsRequest_TIME_BUCKET_KEY,
	})
	if err != nil {
		return nil, fmt.Errorf("list symbols: %w", err)
	}
	return resp.GetResults(), nil
}

// QueryRange reads [startEpoch, endEpoch] (inclusive of start) for one bucket.
func (c *GRPCClient) QueryRange(ctx context.Context, tbk string, startEpoch, endEpoch int64) (io.ColumnSeriesMap, error) {
	resp, err := c.cli.Query(ctx, &pb.MultiQueryRequest{
		Requests: []*pb.QueryRequest{{
			Destination: tbk,
			EpochStart:  startEpoch,
			EpochEnd:    endEpoch,
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", tbk, err)
	}
	csm := io.NewColumnSeriesMap()
	for _, r := range resp.GetResponses() {
		if r.GetResult() == nil {
			continue
		}
		part, err := frontend.ToNumpyMultiDataSet(r.GetResult()).ToColumnSeriesMap()
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", tbk, err)
		}
		for k, v := range part {
			csm[k] = v
		}
	}
	return csm, nil
}
