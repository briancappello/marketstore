package backfill_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication/backfill"
)

// listConn answers ListSymbols with a fixed list.
type listConn struct{ results []string }

func (c listConn) Invoke(_ context.Context, _ string, _, reply interface{}, _ ...grpc.CallOption) error {
	reply.(*pb.ListSymbolsResponse).Results = c.results
	return nil
}

func (listConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, nil
}

func TestListTBKsSkipsInternalBuckets(t *testing.T) {
	c := backfill.NewGRPCClient(listConn{results: []string{"AAPL/1Min/OHLCV", "__replication__/1D/CURSOR"}})
	got, err := c.ListTBKs(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"AAPL/1Min/OHLCV"}, got)
}
