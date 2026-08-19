package backfill_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// setupCatalogWithFixedAndVariable builds a catalog under t.TempDir() holding
// one fixed bucket (AAPL/1Min/OHLCV) and one variable bucket (AAPL/1Sec/TRADE).
func setupCatalogWithFixedAndVariable(t *testing.T) *catalog.Directory {
	t.Helper()

	rootDir := t.TempDir()
	cfg := utils.NewDefaultConfig(rootDir)
	cfg.BackgroundSync = false
	c := di.NewContainer(cfg)
	catDir := c.GetCatalogDir()

	addBucket := func(tbkStr string, dsv []io.DataShape, rt io.EnumRecordType) {
		tbk := io.NewTimeBucketKey(tbkStr)
		tf, err := tbk.GetTimeFrame()
		require.Nil(t, err)
		tbi := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "test", 2020, dsv, rt)
		require.Nil(t, catDir.AddTimeBucket(tbk, tbi))
	}

	addBucket("AAPL/1Min/OHLCV",
		io.NewDataShapeVector(
			[]string{"Open", "High", "Low", "Close", "Volume"},
			[]io.EnumElementType{io.FLOAT32, io.FLOAT32, io.FLOAT32, io.FLOAT32, io.INT64},
		), io.FIXED)

	addBucket("AAPL/1Sec/TRADE",
		io.NewDataShapeVector(
			[]string{"Price", "Size"},
			[]io.EnumElementType{io.FLOAT64, io.UINT64},
		), io.VARIABLE)

	return catDir
}

func TestIsVariableTBK(t *testing.T) {
	catDir := setupCatalogWithFixedAndVariable(t)
	assert.False(t, backfill.IsVariableTBK(catDir, "AAPL/1Min/OHLCV"))
	assert.True(t, backfill.IsVariableTBK(catDir, "AAPL/1Sec/TRADE"))
	// Unknown bucket defaults to fixed (false).
	assert.False(t, backfill.IsVariableTBK(catDir, "NOPE/1Min/OHLCV"))
}
