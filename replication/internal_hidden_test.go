package replication_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/sqlparser"
)

// The replica's cursor bucket is bookkeeping: no symbol listing, symbol
// universe or wildcard query may show it, while the cursor itself still reads.
func TestCursorBucketIsHiddenFromListings(t *testing.T) {
	r := newReplica(t)
	bw, err := executor.NewBatchWriter(r.md.CatalogDir, r.md.WALFile)
	require.NoError(t, err)
	require.NoError(t, bw.QueueCSM(barsCSM("AAPL/1D/OHLCV", 1795600000, 1, 1), false))
	require.NoError(t, bw.QueueCSM(replication.CursorCSM(7), false))
	require.NoError(t, bw.Commit())
	cat := r.md.CatalogDir

	require.Equal(t, []string{"AAPL/1D/OHLCV"}, catalog.ListTimeBucketKeyNames(cat))
	items, err := cat.GatherCategoriesAndItems()
	require.NoError(t, err)
	require.NotContains(t, items["Symbol"], "__replication__")
	infos, err := cat.GatherTimeBucketInfo()
	require.NoError(t, err)
	require.Len(t, infos, 1)
	for _, d := range cat.GetListOfSubDirs() {
		require.False(t, catalog.IsInternal(d.GetName()))
	}

	atomic.StoreUint32(&frontend.Queryable, 1)
	qs := frontend.NewQueryService(cat)
	w, err := executor.NewWriter(cat, r.md.WALFile)
	require.NoError(t, err)
	ds := frontend.NewDataService(r.root, cat, sqlparser.NewAggRunner(nil), w, qs)
	for _, req := range []*frontend.ListSymbolsRequest{{}, {Format: "tbk"}, {Timeframe: "1D"}} {
		var resp frontend.ListSymbolsResponse
		require.NoError(t, ds.ListSymbols(nil, req, &resp))
		for _, s := range resp.Results {
			require.NotContains(t, s, "__replication__", "request %+v", req)
		}
		require.NotEmpty(t, resp.Results)
	}
	gs := frontend.NewGRPCService(r.root, cat, sqlparser.NewAggRunner(nil), w, qs)
	for _, f := range []proto.ListSymbolsRequest_Format{proto.ListSymbolsRequest_SYMBOL, proto.ListSymbolsRequest_TIME_BUCKET_KEY} {
		resp, err := gs.ListSymbols(context.Background(), &proto.ListSymbolsRequest{Format: f})
		require.NoError(t, err)
		require.Len(t, resp.GetResults(), 1)
	}

	require.Equal(t, []string{"AAPL"}, framework.DiscoverSymbols(cat))

	// A wildcard query expands to listed symbols only.
	resp, err := gs.Query(context.Background(), &proto.MultiQueryRequest{Requests: []*proto.QueryRequest{{
		Destination: "*/1D/CURSOR", EpochStart: 1, EpochEnd: 2000000000,
	}}})
	require.ErrorIs(t, err, frontend.ErrNoResults, "the wildcard expanded to AAPL only, got %v", resp)

	seq, ok, err := replication.ReadCursor(cat)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(7), seq)
}
