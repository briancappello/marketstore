package repair_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/repair"
	"github.com/alpacahq/marketstore/v4/corpaction"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/internal/di"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// startServer runs a real gRPC server over a temp store and returns its address.
func startServer(t *testing.T) string {
	t.Helper()
	prev := utils.InstanceConfig
	t.Cleanup(func() { utils.InstanceConfig = prev })
	cfg := utils.NewDefaultConfig(t.TempDir())
	cfg.BackgroundSync = false
	cfg.WALBypass = true
	cfg.Timezone = ny
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	atomic.StoreUint32(&frontend.Queryable, 1)
	t.Cleanup(func() { atomic.StoreUint32(&frontend.Queryable, 0) })

	srv := c.GetGRPCServer()
	pb.RegisterMarketstoreServer(srv, c.GetGRPCService())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func writeDailyBars(t *testing.T, sym string, bars map[time.Time]float32) {
	t.Helper()
	var ep []int64
	var p []float32
	var v []int64
	for _, d := range []time.Time{et(2024, 6, 5, 0, 0), et(2024, 6, 6, 0, 0), et(2024, 6, 7, 0, 0), et(2024, 6, 10, 0, 0)} {
		if c, ok := bars[d]; ok {
			ep, p, v = append(ep, d.Unix()), append(p, c), append(v, 1000)
		}
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", ep)
	cs.AddColumn("Open", p)
	cs.AddColumn("High", append([]float32(nil), p...))
	cs.AddColumn("Low", append([]float32(nil), p...))
	cs.AddColumn("Close", append([]float32(nil), p...))
	cs.AddColumn("Volume", v)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKeyFromString(sym + "/1D/OHLCV"), cs)
	require.NoError(t, executor.WriteCSM(csm, false))
}

// fakeMassive serves NVDA's 2024-06-10 1:10 split and raw daily bars.
func fakeMassive(t *testing.T) {
	t.Helper()
	raw := map[time.Time]float64{
		et(2024, 6, 5, 0, 0): 118.1, // stored with more decimals than REST returns
		et(2024, 6, 6, 0, 0): 120.0, et(2024, 6, 7, 0, 0): 121.0, et(2024, 6, 10, 0, 0): 121.8,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/stocks/v1/splits":
			_, _ = w.Write([]byte(`{"status":"OK","results":[{"ticker":"NVDA","execution_date":"2024-06-10",` +
				`"adjustment_type":"forward_split","split_from":1,"split_to":10}]}`))
		case strings.HasPrefix(r.URL.Path, "/v2/aggs/ticker/NVDA/range/1/day/"):
			assert.Equal(t, "false", r.URL.Query().Get("adjusted"))
			parts := strings.Split(r.URL.Path, "/")
			var from, to int64
			_, _ = fmt.Sscan(parts[len(parts)-2], &from)
			_, _ = fmt.Sscan(parts[len(parts)-1], &to)
			var bars []string
			for d, c := range raw {
				ms := d.UnixMilli()
				if ms >= from && ms <= to {
					bars = append(bars, fmt.Sprintf(`{"o":%v,"h":%v,"l":%v,"c":%v,"v":1000,"t":%d}`, c, c, c, c, ms))
				}
			}
			_, _ = fmt.Fprintf(w, `{"ticker":"NVDA","status":"OK","adjusted":false,"results":[%s]}`, strings.Join(bars, ","))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	api.SetBaseURL(srv.URL)
	t.Cleanup(func() { api.SetBaseURL("https://api.massive.com") })
}

func queryCloses(t *testing.T, addr string, functions ...string) []float32 {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	resp, err := pb.NewMarketstoreClient(conn).Query(context.Background(), &pb.MultiQueryRequest{
		Requests: []*pb.QueryRequest{{
			Destination: "NVDA/1D/OHLCV", EpochStart: 0, EpochEnd: time.Now().Unix(), Functions: functions,
		}},
	})
	require.NoError(t, err)
	csm, err := frontend.ToNumpyMultiDataSet(resp.Responses[0].Result).ToColumnSeriesMap()
	require.NoError(t, err)
	for _, cs := range csm {
		return cs.GetColumn("Close").([]float32)
	}
	return nil
}

func TestRepairEndToEnd(t *testing.T) {
	addr := startServer(t)
	fakeMassive(t)
	// Adjusted history before the split, raw bar after it.
	writeDailyBars(t, "NVDA", map[time.Time]float32{
		et(2024, 6, 5, 0, 0): 118.10004,
		et(2024, 6, 6, 0, 0): 12.0, et(2024, 6, 7, 0, 0): 12.1, et(2024, 6, 10, 0, 0): 121.8,
	})
	split, err := corpaction.NewSplit("NVDA", corpaction.EffectiveDate(2024, 6, 10), 1, 10, corpaction.ForwardSplit)
	require.NoError(t, err)
	csm, err := corpaction.EncodeCSM([]corpaction.Event{split})
	require.NoError(t, err)
	require.NoError(t, executor.WriteCSM(csm, false))

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	store := repair.NewGRPCStore(conn)
	splits, err := api.ListSplits(http.DefaultClient, api.SplitsQuery{})
	require.NoError(t, err)

	ctx := context.Background()
	ranges, err := repair.Plan(ctx, store, splits, repair.Options{Now: now})
	require.NoError(t, err)
	require.Len(t, ranges, 1, "dry run plan: only NVDA/1D/OHLCV")
	assert.Equal(t, "NVDA/1D/OHLCV", ranges[0].TBK)
	assert.Equal(t, []float32{118.10004, 12.0, 12.1, 121.8}, queryCloses(t, addr), "planning writes nothing")

	writer, err := backfill.NewRPCWriter(ctx, addr)
	require.NoError(t, err)
	defer writer.Close()
	rw := &repair.Rewriter{Client: http.DefaultClient, Store: store, Writer: writer, Limit: 50000,
		Tolerance: repair.DefaultTolerance, Splits: repair.SplitEvents(splits, now)}

	rep := repair.Run(ctx, ranges, rw.Fetch, 2)
	require.Empty(t, rep.Failures)
	assert.Equal(t, 1, rep.Repaired)
	assert.Equal(t, 2, rep.Bars, "only the two adjusted bars are rewritten")
	assert.Equal(t, []float32{118.10004, 120.0, 121.0, 121.8}, queryCloses(t, addr),
		"the higher-precision raw bar is kept")
	assert.Equal(t, []float32{11.81, 12.0, 12.1, 121.8}, queryCloses(t, addr, "adjust('split')"))

	// Running again changes nothing.
	rep = repair.Run(ctx, ranges, rw.Fetch, 2)
	require.Empty(t, rep.Failures)
	assert.Equal(t, 0, rep.Bars)
	assert.Equal(t, []float32{118.10004, 120.0, 121.0, 121.8}, queryCloses(t, addr))
}
