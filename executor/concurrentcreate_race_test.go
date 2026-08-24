package executor_test

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

// TestConcurrentSameBucketCreationRace reproduces the remaining Go-1.26 crash:
// two subsystems (streaming + REST backfill) concurrently WriteCSM to the SAME
// current-day bucket. Uses BackgroundSync=true to match production (taichi), so
// the background WAL writer runs (wf.hasWriter=true) and RequestFlush takes the
// channel path — exercising the real write path. Run with -race.
func TestConcurrentSameBucketCreationRace(t *testing.T) {
	rootDir := t.TempDir()
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = true // production default; starts the background WAL writer
	c := di.NewContainer(cfg)
	catalogDir := c.GetCatalogDir()
	walFile := c.GetInitWALFile() // starts SyncWAL goroutine -> wf.hasWriter=true
	require.NotNil(t, walFile)
	// Tear the background writer down at the end so its SyncWAL goroutine and
	// tickers don't leak into the rest of the test binary. (hasWriter is now
	// per-instance, so a leak can no longer wedge another test's flush, but
	// leaving a live writer goroutine around is still untidy.)
	t.Cleanup(walFile.Shutdown)

	base := time.Date(2026, 8, 19, 9, 30, 0, 0, time.UTC)
	const workers = 48
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			<-start
			for j := 0; j < 200; j++ {
				ts := base.Add(time.Duration((seed+j)%960) * time.Minute)
				cs := io.NewColumnSeries()
				cs.AddColumn("Epoch", []int64{ts.Unix()})
				cs.AddColumn("Open", []float32{float32(seed)})
				cs.AddColumn("High", []float32{float32(seed)})
				cs.AddColumn("Low", []float32{float32(seed)})
				cs.AddColumn("Close", []float32{float32(seed)})
				cs.AddColumn("Volume", []int64{int64(j)})
				csm := io.NewColumnSeriesMap()
				csm.AddColumnSeries(*io.NewTimeBucketKey("CONC/1Min/OHLCV"), cs)

				w, err := executor.NewWriter(catalogDir, walFile)
				require.Nil(t, err)
				_ = w.WriteCSM(csm, false)
			}
		}(i)
	}
	close(start)
	wg.Wait()
}
