package backfill_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
)

func TestWatermarksPersistAndAdvanceOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wm.json")

	w, err := backfill.NewWatermarks(path)
	require.Nil(t, err)
	assert.Equal(t, int64(0), w.Get("AAPL/1Min/OHLCV"))

	require.Nil(t, w.Set("AAPL/1Min/OHLCV", 100))
	assert.Equal(t, int64(100), w.Get("AAPL/1Min/OHLCV"))

	// Lower values never regress the watermark.
	require.Nil(t, w.Set("AAPL/1Min/OHLCV", 50))
	assert.Equal(t, int64(100), w.Get("AAPL/1Min/OHLCV"))

	// Reload from disk: value survived.
	require.Nil(t, w.Flush())
	w2, err := backfill.NewWatermarks(path)
	require.Nil(t, err)
	assert.Equal(t, int64(100), w2.Get("AAPL/1Min/OHLCV"))
}

// Set must not touch the disk. It used to persist the WHOLE map on every call:
// with 35k buckets that is a ~1 MB json.Marshal + rewrite per advanced bucket,
// so one reconcile pass advancing ~10k watermarks wrote ~9.7 GB -- 99.5% of all
// bytes the replica wrote, against ~14 MB of actual market data. It is O(n^2)
// in bucket count per pass.
//
// Batching is safe: the watermark is only a resume hint, and re-pulling from a
// stale one is idempotent by epoch.
func TestWatermarksSetDoesNotWriteToDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wm.json")
	w, err := backfill.NewWatermarks(path)
	require.Nil(t, err)

	for i := 0; i < 100; i++ {
		require.Nil(t, w.Set(fmt.Sprintf("SYM%d/1Min/OHLCV", i), int64(i+1)))
	}

	_, statErr := os.Stat(path)
	assert.True(t, os.IsNotExist(statErr), "Set must not write the watermark file; got %v", statErr)

	require.Nil(t, w.Flush())
	fi, err := os.Stat(path)
	require.Nil(t, err)
	assert.Positive(t, fi.Size(), "Flush must persist")
}

// Flush with nothing pending must not rewrite the file. A reconcile pass that
// advances no watermark should cost zero bytes.
func TestWatermarksFlushIsNoOpWhenClean(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wm.json")
	w, err := backfill.NewWatermarks(path)
	require.Nil(t, err)
	require.Nil(t, w.Set("AAPL/1Min/OHLCV", 100))
	require.Nil(t, w.Flush())

	fi1, err := os.Stat(path)
	require.Nil(t, err)

	// A rejected (regressive) Set leaves nothing to persist.
	require.Nil(t, w.Set("AAPL/1Min/OHLCV", 50))
	require.Nil(t, w.Flush())

	fi2, err := os.Stat(path)
	require.Nil(t, err)
	assert.Equal(t, fi1.ModTime(), fi2.ModTime(), "clean Flush must not rewrite the file")
}
