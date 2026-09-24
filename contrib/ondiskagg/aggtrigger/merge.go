package aggtrigger

import (
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

/*
Hot-path helpers for Fire.

The 1Sec -> 1Min cascade rewrites the current minute on every 1Sec batch, so
stage 3 (1Min -> 1D) fires roughly once per second per active symbol, each
time carrying one or two bars, against a cache holding the whole day so far
(up to ~960 extended-hours bars).

The generic io.ColumnSeriesUnion and ColumnSeries.ApplyTimeQual it used to go
through were O(cached bars) in reflect.Append calls and allocations per fire,
plus a calendar evaluation (several time.Date conversions) per cached bar.
That made ondiskagg ~60% of the server's CPU. These replacements do the same
thing in O(batch * log n) for the merge and O(log n) for the session filter.
*/

type mergeKind uint8

const (
	mergeOverwrite mergeKind = iota
	mergeAppend
	mergeInsert
)

type mergeOp struct {
	kind mergeKind
	pos  int // destination index (overwrite/insert)
	src  int // batch row
}

// mergeBatch returns cache ∪ batch with batch rows winning on duplicate
// epochs (and later batch rows winning over earlier ones), ordered by epoch
// -- the same result as io.ColumnSeriesUnion(cache, batch).
//
// cache must be sorted by epoch with unique epochs, which holds for every
// series this trigger caches (query results and prior merges). Column
// slices of cache are reused: overwrites and appends write into cache's
// backing arrays. Callers must hold the per-bucket lock and must not keep
// other views of cache's columns alive across the call. Inserts (a batch bar
// older than the cache's newest) copy, so an older view never sees a shifted
// column.
//
// The result is a new ColumnSeries whose column ORDER matches cache. That
// order is load-bearing: Fire decodes incoming records with
// cache.GetDataShapes(), so reordering columns would misread every field.
//
// Falls back to io.ColumnSeriesUnion if the two series do not have the same
// columns and element types, or a column type is not handled here.
func mergeBatch(cache, batch *io.ColumnSeries) *io.ColumnSeries {
	names := cache.GetColumnNames()
	if !sameShape(cache, batch) {
		return io.ColumnSeriesUnion(cache, batch)
	}

	cacheEp := cache.GetEpoch()
	batchEp := batch.GetEpoch()
	if cacheEp == nil || batchEp == nil {
		return io.ColumnSeriesUnion(cache, batch)
	}

	// Plan the ops against a working copy of the epoch index only, so each
	// op's position accounts for earlier inserts/appends. Applying the same
	// op sequence to every column (including Epoch) keeps rows aligned.
	//
	// Fast path (the live case): every batch epoch either matches an existing
	// row or is newer than all of them, so the index is cacheEp followed by
	// the appended epochs and nothing is materialized. The first out-of-order
	// bar switches to a materialized copy of the index.
	ops := make([]mergeOp, 0, len(batchEp))
	var (
		appended []int64 // fast path: epochs appended after cacheEp
		index    []int64 // slow path: full materialized index
	)
	n := len(cacheEp)
	epochAt := func(i int) int64 {
		if index != nil {
			return index[i]
		}
		if i < len(cacheEp) {
			return cacheEp[i]
		}
		return appended[i-len(cacheEp)]
	}
	for j, e := range batchEp {
		pos := sort.Search(n, func(i int) bool { return epochAt(i) >= e })
		switch {
		case pos < n && epochAt(pos) == e:
			ops = append(ops, mergeOp{kind: mergeOverwrite, pos: pos, src: j})
		case pos == n:
			if index != nil {
				index = append(index, e)
			} else {
				appended = append(appended, e)
			}
			ops = append(ops, mergeOp{kind: mergeAppend, pos: pos, src: j})
			n++
		default:
			if index == nil {
				index = make([]int64, 0, n+len(batchEp))
				index = append(index, cacheEp...)
				index = append(index, appended...)
			}
			index = append(index, 0)
			copy(index[pos+1:], index[pos:n])
			index[pos] = e
			ops = append(ops, mergeOp{kind: mergeInsert, pos: pos, src: j})
			n++
		}
	}
	copyFirst := index != nil

	out := io.NewColumnSeries()
	for _, name := range names {
		merged, ok := applyOpsAny(cache.GetColumn(name), batch.GetColumn(name), ops, copyFirst)
		if !ok {
			return io.ColumnSeriesUnion(cache, batch)
		}
		out.AddColumn(name, merged)
	}
	return out
}

// sameShape reports whether a and b have the same column names (in the same
// order) with the same element types.
func sameShape(a, b *io.ColumnSeries) bool {
	an, bn := a.GetColumnNames(), b.GetColumnNames()
	if len(an) != len(bn) {
		return false
	}
	for i := range an {
		if an[i] != bn[i] {
			return false
		}
		if reflect.TypeOf(a.GetColumn(an[i])) != reflect.TypeOf(b.GetColumn(bn[i])) {
			return false
		}
	}
	return true
}

func applyOpsAny(dst, src interface{}, ops []mergeOp, copyFirst bool) (interface{}, bool) {
	switch d := dst.(type) {
	case []int64:
		return applyOps(d, src.([]int64), ops, copyFirst), true
	case []float32:
		return applyOps(d, src.([]float32), ops, copyFirst), true
	case []float64:
		return applyOps(d, src.([]float64), ops, copyFirst), true
	case []int32:
		return applyOps(d, src.([]int32), ops, copyFirst), true
	case []int16:
		return applyOps(d, src.([]int16), ops, copyFirst), true
	case []int8:
		return applyOps(d, src.([]int8), ops, copyFirst), true
	case []uint64:
		return applyOps(d, src.([]uint64), ops, copyFirst), true
	case []uint32:
		return applyOps(d, src.([]uint32), ops, copyFirst), true
	case []uint16:
		return applyOps(d, src.([]uint16), ops, copyFirst), true
	case []uint8:
		return applyOps(d, src.([]uint8), ops, copyFirst), true
	case []bool:
		return applyOps(d, src.([]bool), ops, copyFirst), true
	default:
		return nil, false
	}
}

func applyOps[T any](dst, src []T, ops []mergeOp, copyFirst bool) []T {
	if copyFirst {
		c := make([]T, len(dst), len(dst)+len(ops))
		copy(c, dst)
		dst = c
	}
	for _, op := range ops {
		v := src[op.src]
		switch op.kind {
		case mergeOverwrite:
			dst[op.pos] = v
		case mergeAppend:
			dst = append(dst, v)
		case mergeInsert:
			var zero T
			dst = append(dst, zero)
			copy(dst[op.pos+1:], dst[op.pos:len(dst)-1])
			dst[op.pos] = v
		}
	}
	return dst
}

// regularSession returns the rows of cs inside the NASDAQ regular session,
// identical to cs.ApplyTimeQual(calendar.Nasdaq.EpochIsRegularMarketOpen).
//
// When every row falls on one calendar day (always true for the in-progress
// daily aggregate) it evaluates the calendar once and binary-searches the
// sorted epochs, returning zero-copy sub-slices. Otherwise it falls back to
// ApplyTimeQual.
func regularSession(cs *io.ColumnSeries) *io.ColumnSeries {
	fallback := func() *io.ColumnSeries {
		return cs.ApplyTimeQual(calendar.Nasdaq.EpochIsRegularMarketOpen)
	}
	ep := cs.GetEpoch()
	if len(ep) == 0 {
		return fallback()
	}
	tz := calendar.Nasdaq.Tz()
	first := time.Unix(ep[0], 0).In(tz)
	last := time.Unix(ep[len(ep)-1], 0).In(tz)
	fy, fm, fd := first.Date()
	ly, lm, ld := last.Date()
	if fy != ly || fm != lm || fd != ld || !slices.IsSorted(ep) {
		return fallback()
	}

	open, closeT, ok := calendar.Nasdaq.RegularSessionBounds(first)
	lo, hi := 0, 0
	if ok {
		o, c := open.Unix(), closeT.Unix()
		lo = sort.Search(len(ep), func(i int) bool { return ep[i] >= o })
		hi = sort.Search(len(ep), func(i int) bool { return ep[i] >= c })
	}

	out := io.NewColumnSeries()
	for _, name := range cs.GetColumnNames() {
		col := reflect.ValueOf(cs.GetColumn(name))
		out.AddColumn(name, col.Slice(lo, hi).Interface())
	}
	return out
}
