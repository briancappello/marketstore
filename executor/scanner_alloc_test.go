package executor_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	. "github.com/alpacahq/marketstore/v4/planner"
	. "github.com/alpacahq/marketstore/v4/utils/io"
)

// The packing scan reads whole blocks of records at a time so it can skip holes
// cheaply, which makes its two working buffers recordsPerRead*RecordLen bytes
// each -- 512 KiB for the pair at a 32-byte record. That size belongs to the
// scan, not to the result, and nothing Read returns aliases those buffers.
//
// They used to be allocated per Reader in NewReader, so a caller that wanted a
// single row still paid the full pair. That is exactly what a trigger does
// (SetRowLimit(LAST, 1)), once per bucket write, and on a live server it made
// NewReader responsible for ~90% of all heap allocation and left GC as the
// single largest consumer of CPU in the process.
//
// This test fails if the buffers go back to being allocated per Reader.
func TestReaderScratchBuffersAreNotAllocatedPerReader(t *testing.T) {
	_, _, metadata := setup(t)

	newReader := func() *executor.Reader {
		q := NewQuery(metadata.CatalogDir)
		q.AddRestriction("Symbol", "NZDUSD")
		q.AddRestriction("AttributeGroup", "OHLC")
		q.AddRestriction("Timeframe", "1Min")
		q.SetRowLimit(LAST, 1)
		parsed, err := q.Parse()
		require.Nil(t, err)
		r, err := executor.NewReader(parsed)
		require.Nil(t, err)
		return r
	}

	// Derive the scratch-pair size from the real record length rather than
	// hardcoding it, so the bound stays meaningful if the schema changes.
	var recordLen int32
	for _, iop := range newReader().IOPMap {
		if iop.RecordLen > recordLen {
			recordLen = iop.RecordLen
		}
	}
	require.NotZero(t, recordLen, "fixture produced no qualified files")
	const recordsPerRead = 8192
	scratchPairBytes := uint64(2 * recordsPerRead * recordLen)

	// Warm the pool and any lazily initialised state so the measurement below
	// reflects steady state rather than first-call setup.
	for i := 0; i < 20; i++ {
		_, err := newReader().Read()
		require.Nil(t, err)
	}

	const iterations = 200
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < iterations; i++ {
		_, err := newReader().Read()
		require.Nil(t, err)
	}
	runtime.ReadMemStats(&after)

	perRead := (after.TotalAlloc - before.TotalAlloc) / iterations
	t.Logf("single-row read allocates %d B/op; unpooled scratch pair alone would be %d B/op",
		perRead, scratchPairBytes)

	if perRead >= scratchPairBytes {
		t.Fatalf("single-row read allocated %d B/op, at or above the %d B scratch pair: "+
			"the scan buffers are being allocated per Reader again instead of pooled",
			perRead, scratchPairBytes)
	}
}

// Read must be safe to call more than once on the same Reader: it borrows the
// scratch buffers on entry and returns them on exit, so a second call has to
// re-borrow rather than run with nil buffers.
func TestReaderReadIsRepeatable(t *testing.T) {
	_, _, metadata := setup(t)

	q := NewQuery(metadata.CatalogDir)
	q.AddRestriction("Symbol", "NZDUSD")
	q.AddRestriction("AttributeGroup", "OHLC")
	q.AddRestriction("Timeframe", "1Min")
	q.SetRowLimit(LAST, 10)
	parsed, err := q.Parse()
	require.Nil(t, err)

	r, err := executor.NewReader(parsed)
	require.Nil(t, err)

	first, err := r.Read()
	require.Nil(t, err)
	second, err := r.Read()
	require.Nil(t, err)

	require.Equal(t, len(first), len(second))
	for key, cs := range first {
		other, ok := second[key]
		require.True(t, ok, "key %v missing from second read", key)
		require.Equal(t, cs.GetEpoch(), other.GetEpoch(), "key %v differs between reads", key)
	}
}
