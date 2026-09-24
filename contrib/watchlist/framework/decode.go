package framework

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// bucketLayout is what Fire needs to decode a fixed-length record without
// touching disk. A bucket's schema is fixed when it is created, so layouts
// are cached for the life of the process.
type bucketLayout struct {
	shapes   []io.DataShape // including Epoch, in on-disk column order
	recLen   int            // bytes per trigger record: 8-byte index + fields, unpadded
	variable bool
}

var layoutCache sync.Map // tbk.String() -> *bucketLayout

// diskFallbacks counts fires that could not be decoded from their records
// and read the row back from disk instead.
var diskFallbacks atomic.Int64

func layoutFor(tbk *io.TimeBucketKey) (*bucketLayout, error) {
	key := tbk.String()
	if v, ok := layoutCache.Load(key); ok {
		return v.(*bucketLayout), nil
	}
	tbi, err := executor.ThisInstance.CatalogDir.GetLatestTimeBucketInfoFromKey(tbk)
	if err != nil {
		return nil, fmt.Errorf("bucket info for %s: %w", key, err)
	}
	shapes := tbi.GetDataShapesWithEpoch()
	// Not tbi.GetRecordLength(): on-disk records are padded to 8-byte
	// alignment, but trigger records carry the unpadded index + fields.
	recLen := 0
	for _, s := range shapes {
		recLen += s.Len()
	}
	l := &bucketLayout{
		shapes:   shapes,
		recLen:   recLen,
		variable: tbi.GetRecordType() == io.VARIABLE,
	}
	layoutCache.Store(key, l)
	return l, nil
}

// newestRecord returns the record with the highest index. Records arrive in
// write order, so on a tie the later one wins, matching what is on disk.
func newestRecord(records []trigger.Record) trigger.Record {
	best := records[0]
	for _, r := range records[1:] {
		if r.Index() >= best.Index() {
			best = r
		}
	}
	return best
}

// latestRow returns the newest written row as a one-row ColumnSeries: the
// same row, columns and types that a LAST-1 disk query ending at the newest
// record's time returns.
func latestRow(tbk *io.TimeBucketKey, tf time.Duration, year int16, records []trigger.Record) (*io.ColumnSeries, error) {
	rec := newestRecord(records)
	if l, err := layoutFor(tbk); err == nil && !l.variable && len(rec) == l.recLen {
		cs, err := trigger.RecordsToColumnSeries(*tbk, l.shapes, tf, year, []trigger.Record{rec})
		if err == nil {
			return cs, nil
		}
	}
	diskFallbacks.Add(1)
	return readLatestFromDisk(tbk, io.IndexToTime(rec.Index(), tf, year))
}

// writtenRows returns every row carried by records, in record order. When
// the records cannot be decoded (variable-length buckets, unexpected sizes)
// it falls back to the newest row read from disk.
func writtenRows(tbk *io.TimeBucketKey, tf time.Duration, year int16, records []trigger.Record) (*io.ColumnSeries, error) {
	if l, err := layoutFor(tbk); err == nil && !l.variable {
		ok := true
		for _, r := range records {
			if len(r) != l.recLen {
				ok = false
				break
			}
		}
		if ok {
			if cs, err := trigger.RecordsToColumnSeries(*tbk, l.shapes, tf, year, records); err == nil {
				return cs, nil
			}
		}
	}
	return latestRow(tbk, tf, year, records)
}

// readLatestFromDisk is the original read-back path, kept for variable-length
// buckets (whose records may pack several rows per index) and for anything
// that fails to decode.
func readLatestFromDisk(tbk *io.TimeBucketKey, end time.Time) (*io.ColumnSeries, error) {
	q := planner.NewQuery(executor.ThisInstance.CatalogDir)
	q.AddTargetKey(tbk)
	q.SetEnd(end)
	q.SetRowLimit(io.LAST, 1)

	parsed, err := q.Parse()
	if err != nil {
		return nil, fmt.Errorf("query parse: %w", err)
	}
	scanner, err := executor.NewReader(parsed)
	if err != nil {
		return nil, fmt.Errorf("reader: %w", err)
	}
	csm, err := scanner.Read()
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}
	return csm[*tbk], nil
}
