package aggtrigger

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// BenchmarkFireDailyRewrite measures the live stage-3 steady state: the day's
// 1Min bars are already cached and each fire rewrites the current minute, as
// the 1Sec -> 1Min cascade does roughly once per second per active symbol.
// cached is how many 1Min bars of the day precede the rewrite.
func BenchmarkFireDailyRewrite(b *testing.B) {
	for _, cached := range []int{60, 390, 960} {
		b.Run(fmt.Sprintf("cached=%d", cached), func(b *testing.B) {
			benchFireDailyRewrite(b, cached)
		})
	}
}

func benchFireDailyRewrite(b *testing.B, cached int) {
	utils.InstanceConfig.Timezone, _ = time.LoadLocation("America/New_York")
	tz := utils.InstanceConfig.Timezone

	rootDir := filepath.Join(b.TempDir(), "mktsdb")
	_ = os.MkdirAll(rootDir, 0o777)
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	executor.NewInstanceSetup(c.GetCatalogDir(), c.GetInitWALFile())

	// Stage 3 as configured in production, minus current_period_only (which
	// depends on the wall clock and would make the benchmark date-sensitive).
	trig, err := NewTrigger(map[string]interface{}{
		"destinations": []string{"1D"},
		"filter":       "nasdaq",
	})
	require.Nil(b, err)

	tbk := io.NewTimeBucketKey("BENCH/1Min/OHLCV")
	start := time.Date(2024, 3, 5, 4, 0, 0, 0, tz) // Tuesday, pre-market open

	bars := func(from, n int, vol float32) *io.ColumnSeries {
		ep := make([]int64, n)
		o := make([]float32, n)
		v := make([]float32, n)
		for i := range ep {
			ep[i] = start.Add(time.Duration(from+i) * time.Minute).Unix()
			o[i] = 10
			v[i] = vol
		}
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", ep)
		cs.AddColumn("Open", o)
		cs.AddColumn("High", o)
		cs.AddColumn("Low", o)
		cs.AddColumn("Close", o)
		cs.AddColumn("Volume", v)
		return cs
	}

	// Persist the day so far and fire once to populate the cache via the
	// query path.
	day := bars(0, cached, 100)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, day)
	require.Nil(b, executor.WriteCSM(csm, false))
	trig.Fire("BENCH/1Min/OHLCV/2024.bin", recordsFor(b, bars(cached-1, 1, 100), tbk))

	rewrite := recordsFor(b, bars(cached-1, 1, 200), tbk)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		trig.Fire("BENCH/1Min/OHLCV/2024.bin", rewrite)
	}
}
