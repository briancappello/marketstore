package executor_test

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// TestWriteCSMVariableLengthWithNanosecondsInConfig reproduces the bug where a
// variable-length attrgroup whose config schema lists "Nanoseconds" failed to
// create its bucket: WriteCSM strips the Nanoseconds column for variable records,
// but the schema-merge then reported it as a missing required column.
//
// The fix drops "Nanoseconds" from the config schema before the merge for
// variable-length writes, so the first write that creates the bucket succeeds.
func TestWriteCSMVariableLengthWithNanosecondsInConfig(t *testing.T) {
	_, _, metadata := setup(t)

	// Configure a QUOTE attrgroup schema (variable) that explicitly lists
	// Nanoseconds, mirroring the massive plugin's mkts config.
	prevCfg := utils.InstanceConfig.AttrGroupTypes
	utils.InstanceConfig.AttrGroupTypes = map[string]*utils.AttrGroupConfig{
		"QUOTE": {
			RecordType: "variable",
			Columns: map[string]string{
				"Nanoseconds": "int32",
				"BidPrice":    "float64",
				"AskPrice":    "float64",
				"BidSize":     "uint64",
				"AskSize":     "uint64",
			},
		},
	}
	t.Cleanup(func() { utils.InstanceConfig.AttrGroupTypes = prevCfg })

	tbk := io.NewTimeBucketKey("AAPL/1Sec/QUOTE")
	cs := io.NewColumnSeries()
	ts := time.Date(2026, 6, 22, 14, 30, 0, 500_000_000, time.UTC)
	cs.AddColumn("Epoch", []int64{ts.Unix()})
	cs.AddColumn("Nanoseconds", []int32{int32(ts.Nanosecond())})
	cs.AddColumn("BidPrice", []float64{114.125})
	cs.AddColumn("AskPrice", []float64{114.128})
	cs.AddColumn("BidSize", []uint64{100})
	cs.AddColumn("AskSize", []uint64{160})

	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	writer, err := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
	require.Nil(t, err)

	// This previously failed with: missing required column "Nanoseconds".
	err = writer.WriteCSM(csm, true)
	require.Nil(t, err, "variable-length write with Nanoseconds in config should succeed")

	require.Nil(t, metadata.WALFile.FlushToWAL())
	require.Nil(t, metadata.WALFile.CreateCheckpoint())

	// The bucket should now exist with the configured (minus Nanoseconds-as-
	// stored-column handled internally) schema. Confirm we can read it back.
	tbi, err := metadata.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	require.Nil(t, err)
	colNames := tbi.GetDataShapes()
	names := make([]string, 0, len(colNames))
	for _, ds := range colNames {
		names = append(names, ds.Name)
	}
	// BidPrice etc. must be present; the write must not have errored out.
	assert.Contains(t, names, "BidPrice")
	assert.Contains(t, names, "AskPrice")
}

// TestVariableConcurrentReadWriteNoCorruption reproduces the "snappy: corrupt
// input" read error seen when a trigger reads a variable-length tick bucket
// while the writer is appending more ticks to the same interval.
//
// The writer repeatedly appends rows to a single 1Sec interval (forcing
// continuation writes that rewrite/overwrite the interval's compressed block),
// while reader goroutines continuously query the latest row. Before the fix,
// the in-place overwrite of the block let a reader read a half-written /
// stale-length compressed block, producing "snappy: corrupt input".
func TestVariableConcurrentReadWriteNoCorruption(t *testing.T) {
	rootDir, _, metadata := setup(t)
	_ = rootDir

	tbk := io.NewTimeBucketKey("TESTTICK/1Sec/TICK")

	// Base time: all writes target the SAME second so every write after the
	// first is an in-place continuation of one interval's variable block.
	base := time.Date(2026, 6, 22, 14, 30, 0, 0, time.UTC)

	writeOne := func(seq int) error {
		cs := io.NewColumnSeries()
		// Distinct nanos so rows are unique; same Epoch second.
		ts := base.Add(time.Duration(seq) * time.Microsecond)
		cs.AddColumn("Epoch", []int64{base.Unix()})
		cs.AddColumn("Nanoseconds", []int32{int32(ts.Nanosecond())})
		cs.AddColumn("Bid", []float64{float64(seq)})
		cs.AddColumn("Ask", []float64{float64(seq) + 0.01})
		csm := io.NewColumnSeriesMap()
		csm.AddColumnSeries(*tbk, cs)

		w, err := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
		if err != nil {
			return err
		}
		if err = w.WriteCSM(csm, true); err != nil {
			return err
		}
		return metadata.WALFile.FlushToWAL()
	}

	// Seed the bucket with the first write so it exists for readers.
	require.NoError(t, writeOne(0))

	const writes = 400
	var (
		readErr   atomic.Value // stores error string
		stop      atomic.Bool
		readerWG  sync.WaitGroup
		readCount atomic.Int64
	)

	reader := func() {
		defer readerWG.Done()
		for !stop.Load() {
			q := planner.NewQuery(metadata.CatalogDir)
			q.AddTargetKey(tbk)
			q.SetRowLimit(io.LAST, 1)
			parsed, err := q.Parse()
			if err != nil {
				continue // bucket may be mid-update; not the corruption we test
			}
			r, err := executor.NewReader(parsed)
			if err != nil {
				continue
			}
			if _, err := r.Read(); err != nil {
				if strings.Contains(err.Error(), "snappy") {
					readErr.Store(err.Error())
					return
				}
			}
			readCount.Add(1)
		}
	}

	const numReaders = 4
	readerWG.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go reader()
	}

	for seq := 1; seq < writes; seq++ {
		require.NoError(t, writeOne(seq))
		if v := readErr.Load(); v != nil {
			break
		}
	}
	stop.Store(true)
	readerWG.Wait()

	if v := readErr.Load(); v != nil {
		t.Fatalf("reader observed corruption during concurrent variable write: %v (after %d clean reads)",
			v, readCount.Load())
	}
	t.Logf("completed %d concurrent reads with no corruption", readCount.Load())
}

// TestVariableStaleIndexBlockRemainsDecodable deterministically reproduces the
// torn-read hazard behind "snappy: corrupt input": a reader captures a bucket's
// index slot {offset, len1} (read stage 1), then the writer appends more rows to
// the SAME interval. If the writer overwrites the block in place, the bytes at
// `offset` are now a different (larger) compressed block, so the reader's stage-2
// ReadAt(offset, len1) yields a truncated block that fails snappy.Decode.
//
// With an append-only write strategy, the original block at `offset` is never
// mutated, so a reader holding the stale index still decodes valid data.
func TestVariableStaleIndexBlockRemainsDecodable(t *testing.T) {
	rootDir, _, metadata := setup(t)

	tbk := io.NewTimeBucketKey("TESTSTALE/1Sec/TICK")
	tf := utils.TimeframeFromString("1Sec")
	// Bucket schema excludes Nanoseconds (it is stored in the variable record
	// index, not as a column); the written CSM still carries a Nanoseconds
	// column which the writer strips, mirroring the tick data path.
	dsv := io.NewDataShapeVector(
		[]string{"Epoch", "Bid", "Ask"},
		[]io.EnumElementType{io.INT64, io.FLOAT64, io.FLOAT64},
	)
	tbi := io.NewTimeBucketInfo(*tf, tbk.GetPathToYearFiles(rootDir), "stale-test", int16(2026), dsv, io.VARIABLE)
	require.NoError(t, metadata.CatalogDir.AddTimeBucket(tbk, tbi))

	tbiOnDisk, err := metadata.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	require.NoError(t, err)
	dataPath := tbiOnDisk.Path
	primaryOffset := io.IndexToOffset(
		io.TimeToIndex(time.Date(2026, 6, 22, 14, 30, 0, 0, time.UTC), tf.Duration),
		tbiOnDisk.GetRecordLength(),
	)

	writeRows := func(n int) {
		cs := io.NewColumnSeries()
		base := time.Date(2026, 6, 22, 14, 30, 0, 0, time.UTC)
		epochs := make([]int64, n)
		nanos := make([]int32, n)
		bids := make([]float64, n)
		asks := make([]float64, n)
		for i := 0; i < n; i++ {
			ts := base.Add(time.Duration(i) * time.Microsecond)
			epochs[i] = base.Unix()
			nanos[i] = int32(ts.Nanosecond())
			bids[i] = float64(i)
			asks[i] = float64(i) + 0.5
		}
		cs.AddColumn("Epoch", epochs)
		cs.AddColumn("Bid", bids)
		cs.AddColumn("Ask", asks)
		cs.AddColumn("Nanoseconds", nanos)
		csm := io.NewColumnSeriesMap()
		csm.AddColumnSeries(*tbk, cs)
		w, werr := executor.NewWriter(metadata.CatalogDir, metadata.WALFile)
		require.NoError(t, werr)
		require.NoError(t, w.WriteCSM(csm, true))
		require.NoError(t, metadata.WALFile.FlushToWAL())
		require.NoError(t, metadata.WALFile.CreateCheckpoint())
	}

	// First write creates the interval's block. Capture the (offset, len) that a
	// reader's first stage would read from the index slot.
	writeRows(8)
	staleOffset, staleLen := readIndexSlot(t, dataPath, primaryOffset)
	require.NotZero(t, staleLen, "expected a non-empty block after first write")

	staleBlock := make([]byte, staleLen)
	readAt(t, dataPath, staleBlock, staleOffset)
	_, err = snappy.Decode(nil, staleBlock)
	require.NoError(t, err, "freshly written block must decode")

	// Second write appends many more rows to the SAME interval, forcing the
	// block to grow. A reader still holding the stale {offset,len} must be able
	// to decode the bytes at that location.
	writeRows(64)

	staleBlock2 := make([]byte, staleLen)
	readAt(t, dataPath, staleBlock2, staleOffset)
	_, err = snappy.Decode(nil, staleBlock2)
	assert.NoError(t, err, "block at the stale offset/len must remain a valid snappy block after an append")
}

// readIndexSlot reads the {Index, Offset, Len} variable index slot at primaryOffset.
func readIndexSlot(t *testing.T, path string, primaryOffset int64) (offset, length int64) {
	t.Helper()
	buf := make([]byte, 24)
	readAt(t, path, buf, primaryOffset)
	offset = io.ToInt64(buf[8:16])
	length = io.ToInt64(buf[16:24])
	return offset, length
}

func readAt(t *testing.T, path string, buf []byte, offset int64) {
	t.Helper()
	fp, err := os.Open(path)
	require.NoError(t, err)
	defer fp.Close()
	_, err = fp.ReadAt(buf, offset)
	require.NoError(t, err)
}
