package backfill

import (
	"math"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

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

// RowDiff reports, for one bucket, how the master's rows compare with what is
// already on disk.
//
// This exists so a pass can write only the epochs that actually disagree.
// Deciding at whole-window granularity meant a single revised bar rewrote the
// entire lookback window: on a 1Sec bucket that is ~86400 records dragged
// through the WAL and back out to index-addressed file space to correct one.
type RowDiff struct {
	// Missing are epochs the master holds that are absent locally.
	Missing []int64
	// Differing are epochs present on both sides where a compared column
	// disagrees.
	Differing []int64
	// LocalOnly are epochs held locally that the master did not return. A
	// write cannot delete a row, so these can only be reported. They are worth
	// reporting: a replica holding rows its master does not is either ahead of
	// a trim or has invented data, and silence makes both invisible.
	LocalOnly []int64
	// Identical counts epochs that matched, so a pass can show how much work it
	// avoided rather than only what it did.
	Identical int
	// SchemaIssue is non-empty when the two sides cannot be compared row by row
	// at all: a column the master has is missing locally, the same column has
	// different types, or a column's length disagrees with its Epoch column.
	//
	// Such rows are deliberately NOT reported as Differing. Rewriting cannot
	// fix a schema disagreement -- the write either fails the column check or
	// is coerced back to the local type -- so treating them as differing would
	// rewrite the bucket on every pass forever, which is the exact failure this
	// type exists to remove.
	SchemaIssue string
}

// NeedsWrite returns the epochs that must be written to bring local into line
// with the master. It is empty when the bucket is already in sync, and also
// when a SchemaIssue makes writing pointless.
func (d *RowDiff) NeedsWrite() []int64 {
	if d.SchemaIssue != "" {
		return nil
	}
	if len(d.Missing) == 0 && len(d.Differing) == 0 {
		return nil
	}
	out := make([]int64, 0, len(d.Missing)+len(d.Differing))
	out = append(out, d.Missing...)
	out = append(out, d.Differing...)
	return out
}

// InSync reports whether the bucket needs no write at all.
func (d *RowDiff) InSync() bool {
	return d.SchemaIssue == "" && len(d.Missing) == 0 && len(d.Differing) == 0
}

// diffReason renders a short, greppable label for the pass summary. It keeps
// the per-reason bucket sampling useful now that the old
// correction/tail-only/unchanged vocabulary no longer describes what happens.
func diffReason(d *RowDiff) string {
	switch {
	case d.SchemaIssue != "":
		return "schema:" + d.SchemaIssue
	case len(d.Missing) > 0 && len(d.Differing) > 0:
		return "rows-missing+revised"
	case len(d.Missing) > 0:
		return "rows-missing"
	case len(d.Differing) > 0:
		return "rows-revised"
	case len(d.LocalOnly) > 0:
		return "in-sync+local-only"
	default:
		return "in-sync"
	}
}

// CSMRowDiff compares the master's rows against local, per bucket, keyed by
// epoch.
//
// Keyed rather than positional on purpose: CSMDiff compares column slices with
// reflect.DeepEqual, which silently assumes both sides hold the same rows in
// the same order. Any difference in ordering or coverage makes every row look
// wrong. Matching on epoch compares the same bar to the same bar.
//
// Only columns the master has are compared; local may carry extra columns.
func CSMRowDiff(master, local io.ColumnSeriesMap) map[io.TimeBucketKey]*RowDiff {
	out := make(map[io.TimeBucketKey]*RowDiff, len(master))

	for tbk, mcs := range master {
		d := &RowDiff{}
		out[tbk] = d
		if mcs == nil {
			continue
		}
		mEpochs := mcs.GetEpoch()
		if mEpochs == nil {
			d.SchemaIssue = "no-epoch-column"
			continue
		}

		lcs, ok := local[tbk]
		if !ok || lcs == nil || lcs.Len() == 0 {
			// Nothing on disk for this bucket yet, so every master row is new.
			// This is distinct from a schema disagreement: there is no local
			// schema to disagree with.
			d.Missing = append(d.Missing, mEpochs...)
			continue
		}
		lEpochs := lcs.GetEpoch()
		if lEpochs == nil {
			d.SchemaIssue = "local-no-epoch-column"
			continue
		}

		cmps, issue := buildComparators(mcs, lcs, len(mEpochs), len(lEpochs))
		if issue != "" {
			d.SchemaIssue = issue
			continue
		}

		// First local index wins for a duplicated epoch. Fixed-length buckets
		// hold one row per epoch, so a duplicate means the local read is
		// malformed; comparing against the first is stable either way.
		lIndex := make(map[int64]int, len(lEpochs))
		for i, e := range lEpochs {
			if _, dup := lIndex[e]; !dup {
				lIndex[e] = i
			}
		}

		seen := make(map[int64]struct{}, len(mEpochs))
		for i, e := range mEpochs {
			seen[e] = struct{}{}
			j, found := lIndex[e]
			if !found {
				d.Missing = append(d.Missing, e)
				continue
			}
			same := true
			for _, eq := range cmps {
				if !eq(i, j) {
					same = false
					break
				}
			}
			if same {
				d.Identical++
			} else {
				d.Differing = append(d.Differing, e)
			}
		}
		for _, e := range lEpochs {
			if _, ok := seen[e]; !ok {
				d.LocalOnly = append(d.LocalOnly, e)
			}
		}
	}

	return out
}

// buildComparators returns one comparison function per compared column, so the
// type switch is paid once per column instead of once per value. A pass
// compares millions of values, so per-value reflection is not affordable.
func buildComparators(mcs, lcs *io.ColumnSeries, mRows, lRows int) (cmps []func(i, j int) bool, issue string) {
	for _, name := range mcs.GetColumnNames() {
		if name == epochColumn {
			continue // the join key, already matched
		}
		mcol := mcs.GetColumn(name)
		lcol := lcs.GetColumn(name)
		if lcol == nil {
			return nil, "column-missing-locally:" + name
		}
		if sliceLen(mcol) != mRows || sliceLen(lcol) != lRows {
			return nil, "column-length-mismatch:" + name
		}
		eq, ok := columnEqualFunc(mcol, lcol)
		if !ok {
			return nil, "column-type-differs:" + name
		}
		cmps = append(cmps, eq)
	}
	return cmps, ""
}

const epochColumn = "Epoch"

func sliceLen(col interface{}) int {
	v := reflect.ValueOf(col)
	if v.Kind() != reflect.Slice {
		return -1
	}
	return v.Len()
}

// columnEqualFunc returns an element comparator for a master/local column
// pair, or ok=false when their types differ.
func columnEqualFunc(mcol, lcol interface{}) (eq func(i, j int) bool, ok bool) {
	switch m := mcol.(type) {
	case []float64:
		l, k := lcol.([]float64)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return float64Equal(m[i], l[j]) }, true
	case []float32:
		l, k := lcol.([]float32)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return float32Equal(m[i], l[j]) }, true
	case []int64:
		l, k := lcol.([]int64)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return m[i] == l[j] }, true
	case []int32:
		l, k := lcol.([]int32)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return m[i] == l[j] }, true
	case []int16:
		l, k := lcol.([]int16)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return m[i] == l[j] }, true
	case []uint64:
		l, k := lcol.([]uint64)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return m[i] == l[j] }, true
	case []uint32:
		l, k := lcol.([]uint32)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return m[i] == l[j] }, true
	case []uint16:
		l, k := lcol.([]uint16)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return m[i] == l[j] }, true
	case []uint8: // also covers []byte
		l, k := lcol.([]uint8)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return m[i] == l[j] }, true
	case []bool:
		l, k := lcol.([]bool)
		if !k {
			return nil, false
		}
		return func(i, j int) bool { return m[i] == l[j] }, true
	default:
		mv := reflect.ValueOf(mcol)
		lv := reflect.ValueOf(lcol)
		if mv.Kind() != reflect.Slice || lv.Kind() != reflect.Slice || mv.Type() != lv.Type() {
			return nil, false
		}
		return func(i, j int) bool {
			return reflect.DeepEqual(mv.Index(i).Interface(), lv.Index(j).Interface())
		}, true
	}
}

// float64Equal treats NaN as equal to NaN.
//
// This is not a nicety. OHLCV columns carry NaN where a bar has no value, and
// Go's == reports NaN != NaN, so a plain comparison marks every such row as
// differing on every pass. Under a row-level diff that would rewrite those rows
// forever -- a permanent amplification floor that would quietly defeat the
// whole point of diffing. Two absent values are the same absent value.
func float64Equal(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return a == b
}

func float32Equal(a, b float32) bool {
	return float64Equal(float64(a), float64(b))
}

// FilterCSMByBucket keeps, for each bucket, only the epochs that bucket's own
// predicate accepts.
//
// FilterCSM applies one predicate to every bucket, which would union the write
// sets when a map holds more than one. Each bucket's diff is its own.
func FilterCSMByBucket(csm io.ColumnSeriesMap, keep func(tbk io.TimeBucketKey, epoch int64) bool) io.ColumnSeriesMap {
	out := io.NewColumnSeriesMap()
	for tbk, cs := range csm {
		if cs == nil {
			continue
		}
		tbk := tbk
		out[tbk] = cs.ApplyTimeQual(func(e int64) bool { return keep(tbk, e) })
	}
	return out
}

// skipReasons tallies why deep-pass comparisons did or did not match, so a pass
// can report it. Package-level because BackfillBucket is a free function; the
// Driver resets and drains it per pass.
var skipReasons sync.Map // reason string -> *int64

// skipSamples holds a bounded set of example buckets per reason.
//
// The tally alone is not actionable: "local-read-error:12" names no bucket, so
// there is nothing to go and look at. Keeping a few identifiers per reason
// turns each line of the pass summary into something an operator can actually
// investigate, at a fixed memory cost.
var skipSamples sync.Map // reason string -> *sampleSet

// maxSkipSamples caps the identifiers retained per reason. Small on purpose:
// this is a lead to follow, not an inventory.
const maxSkipSamples = 5

type sampleSet struct {
	mu   sync.Mutex
	tbks []string
}

func (s *sampleSet) add(tbk string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tbks) >= maxSkipSamples {
		return
	}
	s.tbks = append(s.tbks, tbk)
}

func (s *sampleSet) drain() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.tbks
	s.tbks = nil
	return out
}

func recordSkipReason(reason, tbk string) {
	v, _ := skipReasons.LoadOrStore(reason, new(int64))
	if c, ok := v.(*int64); ok {
		atomic.AddInt64(c, 1)
	}
	sv, _ := skipSamples.LoadOrStore(reason, &sampleSet{})
	if s, ok := sv.(*sampleSet); ok {
		s.add(tbk)
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

// RowStats aggregates a pass's row-level comparison outcome.
//
// The reason tally says how many BUCKETS took each path; this says how many
// ROWS were actually touched. Write amplification is a row-count property, so
// without this it can only be inferred from disk-byte totals after the fact.
type RowStats struct {
	Compared  int64 // master rows examined
	Identical int64 // matched local, not written
	Missing   int64 // absent locally, written
	Revised   int64 // present but different, written
	LocalOnly int64 // held locally, not offered by master; cannot be repaired
	Schema    int64 // buckets skipped because their schemas cannot be compared
}

// Written is the number of rows a pass actually had to write.
func (s RowStats) Written() int64 { return s.Missing + s.Revised }

var rowStats struct {
	compared  atomic.Int64
	identical atomic.Int64
	missing   atomic.Int64
	revised   atomic.Int64
	localOnly atomic.Int64
	schema    atomic.Int64
}

// recordRowDiff folds one bucket's diff into the pass totals.
func recordRowDiff(d *RowDiff) {
	if d.SchemaIssue != "" {
		rowStats.schema.Add(1)
		return
	}
	rowStats.compared.Add(int64(d.Identical + len(d.Missing) + len(d.Differing)))
	rowStats.identical.Add(int64(d.Identical))
	rowStats.missing.Add(int64(len(d.Missing)))
	rowStats.revised.Add(int64(len(d.Differing)))
	rowStats.localOnly.Add(int64(len(d.LocalOnly)))
}

// DrainRowStats returns the row totals since the last drain and resets them.
func DrainRowStats() RowStats {
	return RowStats{
		Compared:  rowStats.compared.Swap(0),
		Identical: rowStats.identical.Swap(0),
		Missing:   rowStats.missing.Swap(0),
		Revised:   rowStats.revised.Swap(0),
		LocalOnly: rowStats.localOnly.Swap(0),
		Schema:    rowStats.schema.Swap(0),
	}
}

// DrainSkipSamples returns up to maxSkipSamples example buckets per reason
// since the last drain, and resets them.
func DrainSkipSamples() map[string][]string {
	out := map[string][]string{}
	skipSamples.Range(func(k, v any) bool {
		key, kok := k.(string)
		s, vok := v.(*sampleSet)
		if kok && vok {
			if tbks := s.drain(); len(tbks) > 0 {
				out[key] = tbks
			}
		}
		return true
	})
	return out
}
