package backfill_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

func csmOf(tbk string, epochs []int64, closes []float32) io.ColumnSeriesMap {
	k := io.NewTimeBucketKey(tbk)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	cs.AddColumn("Close", closes)
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*k, cs)
	return csm
}

// A deep pass re-pulls 24h for every bucket to catch master-side corrections.
// Almost none of it has changed, so writing it back rewrites ~2 MB of
// index-addressed file space per symbol for nothing (5.6 GB per pass measured
// on 11,615 1Sec buckets). CSMEqual lets the caller skip the identical case.
func TestCSMEqualDetectsUnchangedData(t *testing.T) {
	a := csmOf("AAPL/1Sec/OHLCV", []int64{1, 2, 3}, []float32{10, 11, 12})
	b := csmOf("AAPL/1Sec/OHLCV", []int64{1, 2, 3}, []float32{10, 11, 12})
	assert.True(t, backfill.CSMEqual(a, b), "byte-identical ranges must compare equal")
}

func TestCSMEqualDetectsCorrection(t *testing.T) {
	a := csmOf("AAPL/1Sec/OHLCV", []int64{1, 2, 3}, []float32{10, 11, 12})
	corrected := csmOf("AAPL/1Sec/OHLCV", []int64{1, 2, 3}, []float32{10, 99, 12})
	assert.False(t, backfill.CSMEqual(a, corrected), "a corrected bar must not compare equal")
}

func TestCSMEqualDetectsMissingAndExtraRows(t *testing.T) {
	a := csmOf("AAPL/1Sec/OHLCV", []int64{1, 2, 3}, []float32{10, 11, 12})
	shorter := csmOf("AAPL/1Sec/OHLCV", []int64{1, 2}, []float32{10, 11})
	assert.False(t, backfill.CSMEqual(a, shorter), "differing row counts must not compare equal")

	shifted := csmOf("AAPL/1Sec/OHLCV", []int64{1, 2, 4}, []float32{10, 11, 12})
	assert.False(t, backfill.CSMEqual(a, shifted), "differing epochs must not compare equal")
}

func TestCSMEqualDetectsMissingBucketOrColumn(t *testing.T) {
	a := csmOf("AAPL/1Sec/OHLCV", []int64{1}, []float32{10})
	assert.False(t, backfill.CSMEqual(a, io.NewColumnSeriesMap()), "empty local must not compare equal")

	k := io.NewTimeBucketKey("AAPL/1Sec/OHLCV")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{1}) // no Close column
	noClose := io.NewColumnSeriesMap()
	noClose.AddColumnSeries(*k, cs)
	assert.False(t, backfill.CSMEqual(a, noClose), "a missing column must not compare equal")
}
