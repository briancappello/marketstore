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

// TestConcurrentInlineFlushes writes from many goroutines with no SyncWAL
// loop, so every WriteCSM flushes the WAL inline. Unserialized flushes share
// the file offset: one reads another's appends as the status header and
// panics ("Failed attempt to write to WAL", "slice bounds out of range").
// The same window exists in production before SyncWAL starts and during
// shutdown.
func TestConcurrentInlineFlushes(t *testing.T) {
	cfg := utils.NewDefaultConfig(t.TempDir())
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	catalogDir := c.GetCatalogDir()
	walFile := c.GetInitWALFile()
	require.NotNil(t, walFile)
	require.False(t, walFile.WALBypass)

	base := time.Date(2026, 8, 19, 9, 30, 0, 0, time.UTC)
	const workers = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				ts := base.Add(time.Duration((seed*100+j)%960) * time.Minute)
				cs := io.NewColumnSeries()
				cs.AddColumn("Epoch", []int64{ts.Unix()})
				cs.AddColumn("Open", []float32{float32(seed)})
				cs.AddColumn("High", []float32{float32(seed)})
				cs.AddColumn("Low", []float32{float32(seed)})
				cs.AddColumn("Close", []float32{float32(seed)})
				cs.AddColumn("Volume", []int64{int64(j)})
				csm := io.NewColumnSeriesMap()
				csm.AddColumnSeries(*io.NewTimeBucketKey("FLUSH/1Min/OHLCV"), cs)

				w, err := executor.NewWriter(catalogDir, walFile)
				if err != nil {
					errs <- err
					return
				}
				if err := w.WriteCSM(csm, false); err != nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	canWrite, err := walFile.CanWrite(walFile.OwningInstanceID)
	require.NoError(t, err)
	require.True(t, canWrite, "the WAL header is intact after concurrent flushes")
}
