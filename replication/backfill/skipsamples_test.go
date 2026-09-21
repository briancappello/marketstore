package backfill

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSkipSamples_NamesBuckets covers the diagnostic gap that made a real
// incident hard to act on: the pass summary reported "local-read-error:12"
// with no indication of WHICH buckets failed, so there was nothing to
// investigate. Counts alone are not actionable.
func TestSkipSamples_NamesBuckets(t *testing.T) {
	// Not parallel: the tallies are package-level and drained destructively.
	DrainSkipReasons()
	DrainSkipSamples()

	recordSkipReason("local-read-error", "AAPL/1Sec/QUOTE")
	recordSkipReason("local-read-error", "MSFT/1Sec/QUOTE")
	recordSkipReason("tail-only", "NVDA/1Min/OHLCV")

	counts := DrainSkipReasons()
	assert.Equal(t, int64(2), counts["local-read-error"])
	assert.Equal(t, int64(1), counts["tail-only"])

	samples := DrainSkipSamples()
	assert.ElementsMatch(t, []string{"AAPL/1Sec/QUOTE", "MSFT/1Sec/QUOTE"}, samples["local-read-error"])
	assert.Equal(t, []string{"NVDA/1Min/OHLCV"}, samples["tail-only"])
}

// TestSkipSamples_BoundedAndDrained makes sure the sample set cannot grow with
// the bucket count. A deep pass touches tens of thousands of buckets, so an
// unbounded diagnostic would be a memory leak that only shows up in production.
func TestSkipSamples_BoundedAndDrained(t *testing.T) {
	DrainSkipReasons()
	DrainSkipSamples()

	const recorded = maxSkipSamples * 10
	for i := 0; i < recorded; i++ {
		recordSkipReason("unchanged", fmt.Sprintf("SYM%03d/1Sec/OHLCV", i))
	}

	counts := DrainSkipReasons()
	assert.Equal(t, int64(recorded), counts["unchanged"], "every occurrence must still be counted")

	samples := DrainSkipSamples()
	assert.Len(t, samples["unchanged"], maxSkipSamples, "samples must be capped")

	// A drain resets, so the next pass starts clean rather than reporting
	// buckets from a previous one.
	assert.Empty(t, DrainSkipSamples())
	assert.Empty(t, DrainSkipReasons())
}

// TestRowStats_AccountForEveryComparedRow checks the counters that make write
// amplification observable rather than inferred.
//
// The bucket-level reason tally cannot distinguish "one row changed" from "the
// whole window changed", which is exactly the distinction that matters here.
func TestRowStats_AccountForEveryComparedRow(t *testing.T) {
	DrainRowStats()

	recordRowDiff(&RowDiff{
		Missing:   []int64{1, 2},
		Differing: []int64{3},
		LocalOnly: []int64{9},
		Identical: 10,
	})
	recordRowDiff(&RowDiff{Identical: 5})
	recordRowDiff(&RowDiff{SchemaIssue: "column-type-differs:Close"})

	rs := DrainRowStats()
	assert.Equal(t, int64(18), rs.Compared, "10+2+1 plus 5 identical")
	assert.Equal(t, int64(15), rs.Identical)
	assert.Equal(t, int64(2), rs.Missing)
	assert.Equal(t, int64(1), rs.Revised)
	assert.Equal(t, int64(3), rs.Written(), "only missing+revised rows are written")
	assert.Equal(t, int64(1), rs.LocalOnly)
	assert.Equal(t, int64(1), rs.Schema)

	// A schema-skipped bucket contributes no compared rows, because it was not
	// compared: counting it would understate the identical ratio.
	assert.Equal(t, RowStats{}, DrainRowStats(), "drain must reset")
}

func TestDiffReason_Labels(t *testing.T) {
	assert.Equal(t, "in-sync", diffReason(&RowDiff{Identical: 3}))
	assert.Equal(t, "rows-missing", diffReason(&RowDiff{Missing: []int64{1}}))
	assert.Equal(t, "rows-revised", diffReason(&RowDiff{Differing: []int64{1}}))
	assert.Equal(t, "rows-missing+revised",
		diffReason(&RowDiff{Missing: []int64{1}, Differing: []int64{2}}))
	assert.Equal(t, "in-sync+local-only", diffReason(&RowDiff{LocalOnly: []int64{1}}))
	assert.Equal(t, "schema:column-type-differs:Close",
		diffReason(&RowDiff{SchemaIssue: "column-type-differs:Close"}))
}

// TestSkipSamples_ConcurrentRecording exercises the path as it actually runs:
// BackfillBucket is called from a worker pool, so recording races across
// goroutines.
func TestSkipSamples_ConcurrentRecording(t *testing.T) {
	DrainSkipReasons()
	DrainSkipSamples()

	const goroutines = 16
	const perGoroutine = 50

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				recordSkipReason("correction:column-differs:Close",
					fmt.Sprintf("SYM%02d-%02d/1Min/OHLCV", g, i))
			}
		}(g)
	}
	wg.Wait()

	counts := DrainSkipReasons()
	require.Equal(t, int64(goroutines*perGoroutine), counts["correction:column-differs:Close"])

	samples := DrainSkipSamples()
	assert.Len(t, samples["correction:column-differs:Close"], maxSkipSamples)
}
