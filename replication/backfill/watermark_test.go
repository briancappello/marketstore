package backfill_test

import (
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
	w2, err := backfill.NewWatermarks(path)
	require.Nil(t, err)
	assert.Equal(t, int64(100), w2.Get("AAPL/1Min/OHLCV"))
}
