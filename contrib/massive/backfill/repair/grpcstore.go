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

// Bars returns the bucket's OHLCV bars that start within [from, to], keyed
// by epoch second.
func (s *GRPCStore) Bars(ctx context.Context, tbk string, from, to time.Time) (map[int64]Bar, error) {
	resp, err := s.cli.Query(ctx, &pb.MultiQueryRequest{
		Requests: []*pb.QueryRequest{{Destination: tbk, EpochStart: from.Unix(), EpochEnd: to.Unix()}},
	})
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", tbk, err)
	}
	out := map[int64]Bar{}
	for _, r := range resp.GetResponses() {
		if r.GetResult() == nil {
			continue
		}
		csm, err := frontend.ToNumpyMultiDataSet(r.GetResult()).ToColumnSeriesMap()
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", tbk, err)
		}
		for _, cs := range csm {
			ep := cs.GetEpoch()
			o, _ := cs.GetColumn("Open").([]float32)
			h, _ := cs.GetColumn("High").([]float32)
			l, _ := cs.GetColumn("Low").([]float32)
			c, _ := cs.GetColumn("Close").([]float32)
			if len(o) != len(ep) || len(h) != len(ep) || len(l) != len(ep) || len(c) != len(ep) {
				return nil, fmt.Errorf("%s: unexpected OHLCV layout %v", tbk, cs.GetColumnNames())
			}
			for i, e := range ep {
				out[e] = Bar{Open: float64(o[i]), High: float64(h[i]), Low: float64(l[i]), Close: float64(c[i])}
			}
		}
	}
	return out, nil
}
