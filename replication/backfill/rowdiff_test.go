package backfill_test

import (
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// csmWith builds a one-bucket map from an epoch column plus named columns.
func csmWith(tbk string, epochs []int64, cols map[string]interface{}) io.ColumnSeriesMap {
	k := io.NewTimeBucketKey(tbk)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epochs)
	// Deterministic column order so comparator construction is reproducible.
	names := make([]string, 0, len(cols))
	for n := range cols {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		cs.AddColumn(n, cols[n])
	}
	m := io.NewColumnSeriesMap()
	m.AddColumnSeries(*k, cs)
	return m
}

func diffOf(t *testing.T, master, local io.ColumnSeriesMap, tbk string) *backfill.RowDiff {
	t.Helper()
	d := backfill.CSMRowDiff(master, local)[*io.NewTimeBucketKey(tbk)]
	require.NotNil(t, d)
	return d
}

func sorted(v []int64) []int64 {
	out := append([]int64(nil), v...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

const diffTBK = "AAPL/1Min/OHLCV"

func TestCSMRowDiff_ClassifiesRows(t *testing.T) {
	t.Parallel()

	master := csmWith(diffTBK, []int64{10, 20, 30, 40}, map[string]interface{}{
		"Close": []float32{1, 99, 3, 4},
	})
	local := csmWith(diffTBK, []int64{10, 20, 30, 50}, map[string]interface{}{
		"Close": []float32{1, 2, 3, 5},
	})

	d := diffOf(t, master, local, diffTBK)
	assert.Empty(t, d.SchemaIssue)
	assert.Equal(t, []int64{40}, sorted(d.Missing), "40 is on master only")
	assert.Equal(t, []int64{20}, sorted(d.Differing), "20 was revised")
	assert.Equal(t, []int64{50}, sorted(d.LocalOnly), "50 is on disk only")
	assert.Equal(t, 2, d.Identical, "10 and 30 match")
	assert.Equal(t, []int64{20, 40}, sorted(d.NeedsWrite()))
	assert.False(t, d.InSync())
}

// TestCSMRowDiff_NaNIsEqualToNaN is the most consequential case here.
//
// OHLCV columns carry NaN where a bar has no value, and Go's == reports
// NaN != NaN. Without explicit handling every such row is "revised" on every
// pass and is rewritten forever -- a permanent write-amplification floor that
// would silently defeat the entire point of diffing rows.
func TestCSMRowDiff_NaNIsEqualToNaN(t *testing.T) {
	t.Parallel()

	nan32 := float32(math.NaN())
	master := csmWith(diffTBK, []int64{10, 20}, map[string]interface{}{
		"Close": []float32{nan32, 2},
	})
	local := csmWith(diffTBK, []int64{10, 20}, map[string]interface{}{
		"Close": []float32{nan32, 2},
	})

	d := diffOf(t, master, local, diffTBK)
	assert.True(t, d.InSync(), "two absent values are the same absent value")
	assert.Empty(t, d.NeedsWrite())
	assert.Equal(t, 2, d.Identical)
}

func TestCSMRowDiff_NaNVersusRealValueStillDiffers(t *testing.T) {
	t.Parallel()

	master := csmWith(diffTBK, []int64{10}, map[string]interface{}{
		"Close": []float64{1.5},
	})
	local := csmWith(diffTBK, []int64{10}, map[string]interface{}{
		"Close": []float64{math.NaN()},
	})

	d := diffOf(t, master, local, diffTBK)
	assert.Equal(t, []int64{10}, d.Differing,
		"NaN tolerance must not swallow a real value replacing an absent one")
}

// TestCSMRowDiff_MatchesByEpochNotPosition covers why this replaced CSMDiff.
// CSMDiff compares column slices with reflect.DeepEqual, which assumes both
// sides hold the same rows in the same order. They need not.
func TestCSMRowDiff_MatchesByEpochNotPosition(t *testing.T) {
	t.Parallel()

	master := csmWith(diffTBK, []int64{10, 20, 30}, map[string]interface{}{
		"Close": []float32{1, 2, 3},
	})
	// Same data, different row order.
	local := csmWith(diffTBK, []int64{30, 10, 20}, map[string]interface{}{
		"Close": []float32{3, 1, 2},
	})

	d := diffOf(t, master, local, diffTBK)
	assert.True(t, d.InSync(), "identical data in a different order is still identical")
	assert.Equal(t, 3, d.Identical)
}

func TestCSMRowDiff_ExtraLocalColumnsAreIgnored(t *testing.T) {
	t.Parallel()

	master := csmWith(diffTBK, []int64{10}, map[string]interface{}{
		"Close": []float32{1},
	})
	local := csmWith(diffTBK, []int64{10}, map[string]interface{}{
		"Close": []float32{1},
		"Extra": []int64{42},
	})

	d := diffOf(t, master, local, diffTBK)
	assert.True(t, d.InSync(), "only columns the master has are compared")
}

// TestCSMRowDiff_SchemaDivergenceDoesNotChurn is the guard against turning a
// schema problem into infinite rewriting.
//
// A column the master has but local lacks, or the same column with different
// types, cannot be reconciled by writing: the write either fails the column
// check or is coerced straight back to the local type. Reporting those rows as
// "differing" would rewrite the bucket on every pass forever.
func TestCSMRowDiff_SchemaDivergenceDoesNotChurn(t *testing.T) {
	t.Parallel()

	t.Run("column missing locally", func(t *testing.T) {
		t.Parallel()
		master := csmWith(diffTBK, []int64{10}, map[string]interface{}{
			"Close": []float32{1}, "Volume": []int64{7},
		})
		local := csmWith(diffTBK, []int64{10}, map[string]interface{}{
			"Close": []float32{1},
		})

		d := diffOf(t, master, local, diffTBK)
		assert.Equal(t, "column-missing-locally:Volume", d.SchemaIssue)
		assert.Empty(t, d.NeedsWrite(), "a schema problem must not be answered with a rewrite")
		assert.Empty(t, d.Differing)
	})

	t.Run("column type differs", func(t *testing.T) {
		t.Parallel()
		master := csmWith(diffTBK, []int64{10}, map[string]interface{}{
			"Close": []float64{1},
		})
		local := csmWith(diffTBK, []int64{10}, map[string]interface{}{
			"Close": []float32{1},
		})

		d := diffOf(t, master, local, diffTBK)
		assert.Equal(t, "column-type-differs:Close", d.SchemaIssue)
		assert.Empty(t, d.NeedsWrite())
	})
}

func TestCSMRowDiff_AbsentLocalBucketIsAllMissing(t *testing.T) {
	t.Parallel()

	master := csmWith(diffTBK, []int64{10, 20}, map[string]interface{}{
		"Close": []float32{1, 2},
	})

	// An empty local map means nothing is on disk yet. That is distinct from a
	// schema disagreement: there is no local schema to disagree with, so every
	// master row is simply new.
	d := diffOf(t, master, io.NewColumnSeriesMap(), diffTBK)
	assert.Empty(t, d.SchemaIssue)
	assert.Equal(t, []int64{10, 20}, sorted(d.Missing))
	assert.Equal(t, []int64{10, 20}, sorted(d.NeedsWrite()))
}

func TestCSMRowDiff_IntegerColumnsCompareExactly(t *testing.T) {
	t.Parallel()

	master := csmWith(diffTBK, []int64{10, 20}, map[string]interface{}{
		"Volume": []int64{100, 200},
	})
	local := csmWith(diffTBK, []int64{10, 20}, map[string]interface{}{
		"Volume": []int64{100, 201},
	})

	d := diffOf(t, master, local, diffTBK)
	assert.Equal(t, []int64{20}, d.Differing)
	assert.Equal(t, 1, d.Identical)
}

func TestFilterCSMByBucket_KeepsPerBucketEpochSets(t *testing.T) {
	t.Parallel()

	a := io.NewTimeBucketKey("AAA/1Min/OHLCV")
	b := io.NewTimeBucketKey("BBB/1Min/OHLCV")

	csm := io.NewColumnSeriesMap()
	for _, k := range []*io.TimeBucketKey{a, b} {
		cs := io.NewColumnSeries()
		cs.AddColumn("Epoch", []int64{1, 2, 3})
		cs.AddColumn("Close", []float32{1, 2, 3})
		csm.AddColumnSeries(*k, cs)
	}

	// Each bucket keeps a different epoch. A single shared predicate would
	// union these and write rows neither bucket asked for.
	out := backfill.FilterCSMByBucket(csm, func(tbk io.TimeBucketKey, e int64) bool {
		if tbk == *a {
			return e == 1
		}
		return e == 3
	})

	assert.Equal(t, []int64{1}, out[*a].GetEpoch())
	assert.Equal(t, []int64{3}, out[*b].GetEpoch())
}
