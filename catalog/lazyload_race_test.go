package catalog_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/test"
)

// TestLazyTimeBucketInfoRaceOnGetLatestYearFile reproduces the data race that
// crashes concurrent backfill under Go 1.26: a TimeBucketInfo lazily loads its
// header (load() writes f.Year) on first field access via sync.Once, while
// catalog.GetLatestYearFile reads f.Year unsynchronized. Two concurrent
// WriteCSM paths hit both. Run with -race: it fails before the fix that stops
// load() from rewriting the construction-owned Year/Path fields.
func TestLazyTimeBucketInfoRaceOnGetLatestYearFile(t *testing.T) {
	rootDir := t.TempDir()
	test.MakeDummyCurrencyDir(rootDir, true, false)
	catDir, err := catalog.NewDirectory(rootDir)
	require.Nil(t, err)

	tbk := io.NewTimeBucketKey("EURUSD/1Min/OHLC")

	const workers = 48
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for j := 0; j < 500; j++ {
				tbi, qerr := catDir.GetLatestTimeBucketInfoFromKey(tbk) // reads f.Year (GetLatestYearFile)
				if qerr != nil || tbi == nil {
					continue
				}
				if id%2 == 0 {
					_ = tbi.GetDataShapesWithEpoch() // triggers lazy load -> writes f.Year
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
}
