package replication

import (
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// CursorKey is the bucket in which a replica keeps its replication cursor: the
// sequence of the last transaction group it applied. The cursor is written in
// the same WAL transaction group as the data it covers, so WAL replay after a
// crash restores both or neither. Names starting with "__" are internal and
// hidden from symbol listings.
const CursorKey = "__replication__/1D/CURSOR"

// cursorEpoch is the one row's constant timestamp. Not January 1: a 1D
// bucket indexes days from 0 (io.TimeToIndex), and index 0 reads as an empty
// slot, so a 1D row on January 1 is written but never read back.
var cursorEpoch = time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)

// CursorCSM is the write that sets the cursor to seq.
func CursorCSM(seq uint64) io.ColumnSeriesMap {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{cursorEpoch.Unix()})
	cs.AddColumn("Seq", []uint64{seq})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*io.NewTimeBucketKey(CursorKey), cs)
	return csm
}

// ReadCursor returns the cursor stored in catDir, with ok=false when the
// replica has none yet.
func ReadCursor(catDir *catalog.Directory) (seq uint64, ok bool, err error) {
	tbk := io.NewTimeBucketKey(CursorKey)
	if _, err := catDir.GetLatestTimeBucketInfoFromKey(tbk); err != nil {
		return 0, false, nil // no cursor bucket yet
	}
	q := planner.NewQuery(catDir)
	q.AddTargetKey(tbk)
	q.SetRange(cursorEpoch.Add(-time.Hour), cursorEpoch.Add(time.Hour))
	parsed, err := q.Parse()
	if err != nil {
		return 0, false, fmt.Errorf("query replication cursor: %w", err)
	}
	r, err := executor.NewReader(parsed)
	if err != nil {
		return 0, false, fmt.Errorf("read replication cursor: %w", err)
	}
	csm, err := r.Read()
	if err != nil {
		return 0, false, fmt.Errorf("read replication cursor: %w", err)
	}
	return CursorFromCSM(csm)
}

// CursorQuery is the epoch range to query CursorKey over, for reading a
// replica's cursor through its query API.
func CursorQuery() (start, end int64) {
	return cursorEpoch.Add(-time.Hour).Unix(), cursorEpoch.Add(time.Hour).Unix()
}

// CursorFromCSM extracts the cursor from a query result for CursorKey.
func CursorFromCSM(csm io.ColumnSeriesMap) (seq uint64, ok bool, err error) {
	for _, cs := range csm {
		seqs, isU64 := cs.GetColumn("Seq").([]uint64)
		if !isU64 {
			return 0, false, fmt.Errorf("replication cursor: Seq column is %T", cs.GetColumn("Seq"))
		}
		if len(seqs) > 0 {
			return seqs[len(seqs)-1], true, nil
		}
	}
	return 0, false, nil
}
