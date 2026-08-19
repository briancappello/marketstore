package backfill_test

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type listAPI struct {
	tbks    []string
	mu      sync.Mutex
	queried []string
}

func (l *listAPI) ListTBKs(_ context.Context) ([]string, error) { return l.tbks, nil }
func (l *listAPI) QueryRange(_ context.Context, tbk string, _, _ int64) (io.ColumnSeriesMap, error) {
	l.mu.Lock()
	l.queried = append(l.queried, tbk)
	l.mu.Unlock()
	tk := io.NewTimeBucketKey(tbk)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{10})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tk, cs)
	return csm, nil
}

func TestDriverReconcileBackfillsEveryBucket(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV", "MSFT/1D/OHLCV"}}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	write := func(io.ColumnSeriesMap, bool) error { return nil }

	d := backfill.NewDriver(api, write, wm, 4, 0, func(string) bool { return false })
	require.Nil(t, d.Reconcile(context.Background(), 1000))

	sort.Strings(api.queried)
	assert.Equal(t, []string{"AAPL/1Min/OHLCV", "MSFT/1D/OHLCV"}, api.queried)
	assert.Equal(t, int64(10), wm.Get("AAPL/1Min/OHLCV"))
	assert.Equal(t, int64(10), wm.Get("MSFT/1D/OHLCV"))
}
