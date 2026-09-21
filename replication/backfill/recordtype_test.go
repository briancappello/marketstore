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

// noConfig resolves no attrgroup to a configured record type.
func noConfig(string) string { return "" }

func TestIsVariableTBK(t *testing.T) {
	catDir := setupCatalogWithFixedAndVariable(t)
	assert.False(t, backfill.IsVariableTBK(catDir, "AAPL/1Min/OHLCV", noConfig))
	assert.True(t, backfill.IsVariableTBK(catDir, "AAPL/1Sec/TRADE", noConfig))
	// Unknown bucket with no config to fall back on defaults to fixed.
	assert.False(t, backfill.IsVariableTBK(catDir, "NOPE/1Min/OHLCV", noConfig))
}

// TestIsVariableTBK_UnknownBucketFallsBackToConfig covers the repair scenario
// that made this matter.
//
// After a bad tick bucket is deleted, it does not exist locally. If "absent"
// were read as "fixed", the backfill reconciler would stop skipping it, pull
// the master's tick history, and write it down the fixed path -- recreating a
// broken bucket before the live stream could create it correctly. The
// configured attrgroup schema is what the bucket will become, so it decides.
func TestIsVariableTBK_UnknownBucketFallsBackToConfig(t *testing.T) {
	catDir := setupCatalogWithFixedAndVariable(t)

	configured := func(attrGroup string) string {
		switch attrGroup {
		case "QUOTE", "TRADE":
			return "variable"
		case "OHLCV":
			return "fixed"
		}
		return ""
	}

	// Absent locally, but configured variable: must be treated as variable.
	assert.True(t, backfill.IsVariableTBK(catDir, "AAPL/1Sec/QUOTE", configured))
	assert.True(t, backfill.IsVariableTBK(catDir, "TSLA/1Sec/TRADE", configured))

	// Absent locally and configured fixed: still fixed.
	assert.False(t, backfill.IsVariableTBK(catDir, "TSLA/1Min/OHLCV", configured))

	// An existing local bucket stays authoritative over config.
	assert.False(t, backfill.IsVariableTBK(catDir, "AAPL/1Min/OHLCV", configured))
	assert.True(t, backfill.IsVariableTBK(catDir, "AAPL/1Sec/TRADE", configured))
}

func TestLocalRecordTypeReportsUnknown(t *testing.T) {
	catDir := setupCatalogWithFixedAndVariable(t)

	rt, known := backfill.LocalRecordType(catDir, "AAPL/1Sec/TRADE")
	assert.True(t, known)
	assert.Equal(t, io.VARIABLE, rt)

	_, known = backfill.LocalRecordType(catDir, "NOPE/1Min/OHLCV")
	assert.False(t, known, "an absent bucket must be reported as unknown, not as fixed")
}
