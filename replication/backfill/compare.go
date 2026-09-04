package backfill

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// CSMEqual reports whether master holds exactly the same rows as local, for
// every bucket and column present in master.
//
// This exists to keep a deep pass from rewriting data that has not changed. A
// deep pass re-pulls the whole lookback window for every bucket to catch
// master-side corrections, but corrections are rare: measured on the p1 replica,
// a pass rewrote ~2 MB of index-addressed file space for each of 11,615 1Sec
// buckets (5.6 GB) with virtually every bar byte-identical to what was already
// on disk. Reads are far cheaper than writes, so comparing first is a win
// whenever the data is unchanged, which is nearly always.
//
// Only columns present in master are compared: local may legitimately carry
// extra columns, but any column master has must match exactly.
func CSMEqual(master, local io.ColumnSeriesMap) bool {
	eq, _ := CSMDiff(master, local)
	return eq
}

// CSMDiff is CSMEqual plus a short reason when they differ, so a caller can
// report WHY a comparison failed. Without it, a mismatch is indistinguishable
// from a real correction and the skip silently stops working.
func CSMDiff(master, local io.ColumnSeriesMap) (equal bool, reason string) {
	if len(master) != len(local) {
		return false, fmt.Sprintf("bucket-count %d vs %d", len(master), len(local))
	}
	for tbk, mcs := range master {
		lcs, ok := local[tbk]
		if !ok {
			return false, "key-missing"
		}
		if mcs == nil || lcs == nil {
			return false, "nil-series"
		}
		if mcs.Len() != lcs.Len() {
			return false, fmt.Sprintf("row-count %d vs %d", mcs.Len(), lcs.Len())
		}
		for _, name := range mcs.GetColumnNames() {
			lcol := lcs.GetColumn(name)
			if lcol == nil {
				return false, "column-missing:" + name
			}
			if !reflect.DeepEqual(mcs.GetColumn(name), lcol) {
				return false, "column-differs:" + name
			}
		}
	}
	return true, ""
}

// FilterCSM returns a copy of csm keeping only rows whose epoch satisfies keep.
// Empty series are retained so the key sets of two filtered maps stay
// comparable.
func FilterCSM(csm io.ColumnSeriesMap, keep func(epoch int64) bool) io.ColumnSeriesMap {
	out := io.NewColumnSeriesMap()
	for tbk, cs := range csm {
		if cs == nil {
			continue
		}
		out[tbk] = cs.ApplyTimeQual(keep)
	}
	return out
}

// CSMRows counts rows across every series in csm.
func CSMRows(csm io.ColumnSeriesMap) int {
	n := 0
	for _, cs := range csm {
		if cs != nil {
			n += cs.Len()
		}
	}
	return n
}

// skipReasons tallies why deep-pass comparisons did or did not match, so a pass
// can report it. Package-level because BackfillBucket is a free function; the
// Driver resets and drains it per pass.
var skipReasons sync.Map // reason string -> *int64

func recordSkipReason(reason string) {
	v, _ := skipReasons.LoadOrStore(reason, new(int64))
	if c, ok := v.(*int64); ok {
		atomic.AddInt64(c, 1)
	}
}

// DrainSkipReasons returns the tally since the last drain and resets it.
func DrainSkipReasons() map[string]int64 {
	out := map[string]int64{}
	skipReasons.Range(func(k, v any) bool {
		key, kok := k.(string)
		c, vok := v.(*int64)
		if kok && vok {
			if n := atomic.SwapInt64(c, 0); n > 0 {
				out[key] = n
			}
		}
		return true
	})
	return out
}
