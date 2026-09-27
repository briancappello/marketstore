package sessionfacts_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

var ny, _ = time.LoadLocation("America/New_York")

func et(y int, m time.Month, d, hh, mm int) time.Time { return time.Date(y, m, d, hh, mm, 0, 0, ny) }

// startInstance starts an America/New_York instance over a temp directory
// with no triggers, and returns its root directory.
func startInstance(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	cfg := utils.NewDefaultConfig(root)
	cfg.BackgroundSync = false
	cfg.Timezone = ny
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)
	walFile, err := executor.NewWALFile(c.GetAbsRootDir(), c.GetInitInstanceID(),
		&executor.NopReplicationSender{}, false, &sync.WaitGroup{},
		executor.StartNewTriggerPluginDispatcherWithWorkers(nil, 1), executor.NewTransactionPipe())
	require.NoError(t, err)
	executor.NewInstanceSetup(c.GetCatalogDir(), walFile)
	return root
}

// minuteBar is a 1Min OHLCV bar with a flat price.
type minuteBar struct {
	t     time.Time
	close float32
	vol   int64
}

// writeMinutes writes 1Min OHLCV bars for sym.
func writeMinutes(t testing.TB, sym string, bars ...minuteBar) {
	t.Helper()
	n := len(bars)
	ep, v := make([]int64, n), make([]int64, n)
	p := make([]float32, n)
	for i, b := range bars {
		ep[i], p[i], v[i] = b.t.Unix(), b.close, b.vol
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", ep)
	cs.AddColumn("Open", p)
	cs.AddColumn("High", p)
	cs.AddColumn("Low", p)
	cs.AddColumn("Close", p)
	cs.AddColumn("Volume", v)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(sym + "/1Min/OHLCV"), cs)
	require.NoError(t, executor.WriteCSM(csm, false))
}
