package sessionfacts

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/planner"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// Writer persists a ColumnSeriesMap. executor.WriteCSM satisfies it; tests
// and the replica guard substitute their own.
type Writer func(csm io.ColumnSeriesMap, isVariableLength bool) error

// ToColumnSeries converts rows (for one symbol) into the bucket's columns.
func ToColumnSeries(rows []Row) *io.ColumnSeries {
	n := len(rows)
	epoch := make([]int64, n)
	preV, regV, postV := make([]int64, n), make([]int64, n), make([]int64, n)
	preC, regC, postC := make([]float64, n), make([]float64, n), make([]float64, n)
	preB, regB, postB := make([]int32, n), make([]int32, n), make([]int32, n)
	ver := make([]int32, n)
	sorted := append([]Row(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Date.Before(sorted[j].Date) })
	for i, r := range sorted {
		epoch[i] = r.Date.Unix()
		preV[i], regV[i], postV[i] = r.PreVolume, r.RegVolume, r.PostVolume
		preC[i], regC[i], postC[i] = r.PreClose, r.RegClose, r.PostClose
		preB[i], regB[i], postB[i] = r.PreBars, r.RegBars, r.PostBars
		ver[i] = r.Version
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("PreVolume", preV)
	cs.AddColumn("RegVolume", regV)
	cs.AddColumn("PostVolume", postV)
	cs.AddColumn("PreClose", preC)
	cs.AddColumn("RegClose", regC)
	cs.AddColumn("PostClose", postC)
	cs.AddColumn("PreBars", preB)
	cs.AddColumn("RegBars", regB)
	cs.AddColumn("PostBars", postB)
	cs.AddColumn("Version", ver)
	return cs
}

// Write persists rows keyed by symbol in a single write.
func Write(w Writer, rows map[string][]Row) error {
	csm := io.NewColumnSeriesMap()
	for sym, rs := range rows {
		if len(rs) == 0 {
			continue
		}
		csm.AddColumnSeries(*io.NewTimeBucketKey(Key(sym)), ToColumnSeries(rs))
	}
	if len(csm) == 0 {
		return nil
	}
	if err := w(csm, false); err != nil {
		return fmt.Errorf("write session facts: %w", err)
	}
	return nil
}

// Read returns symbol's stored rows with from <= Date <= to, in date order,
// whatever their version. A symbol with no SESSIONS bucket has no rows.
func Read(catDir *catalog.Directory, symbol string, from, to time.Time) ([]Row, error) {
	cs, err := query(catDir, Key(symbol), from, to)
	if err != nil || cs == nil {
		return nil, err
	}
	epochs := cs.GetEpoch()
	preV, regV, postV := ints(cs, "PreVolume"), ints(cs, "RegVolume"), ints(cs, "PostVolume")
	preC, regC, postC := floats(cs, "PreClose"), floats(cs, "RegClose"), floats(cs, "PostClose")
	preB, regB, postB := ints(cs, "PreBars"), ints(cs, "RegBars"), ints(cs, "PostBars")
	ver := ints(cs, "Version")
	if len(preV) != len(epochs) || len(ver) != len(epochs) || len(regC) != len(epochs) {
		return nil, fmt.Errorf("%s: unexpected column layout %v", Key(symbol), cs.GetColumnNames())
	}
	rows := make([]Row, len(epochs))
	for i, e := range epochs {
		rows[i] = Row{
			Date:      dateOf(e),
			PreVolume: preV[i], RegVolume: regV[i], PostVolume: postV[i],
			PreClose: preC[i], RegClose: regC[i], PostClose: postC[i],
			PreBars: int32(preB[i]), RegBars: int32(regB[i]), PostBars: int32(postB[i]),
			Version: int32(ver[i]),
		}
	}
	return rows, nil
}

// ReadMinuteBars returns symbol's 1Min bars with start <= epoch < end.
func ReadMinuteBars(catDir *catalog.Directory, symbol string, start, end time.Time) ([]Bar, error) {
	cs, err := query(catDir, SourceKey(symbol), start, end.Add(-time.Second))
	if err != nil || cs == nil {
		return nil, err
	}
	epochs := cs.GetEpoch()
	closes, vols := floats(cs, "Close"), ints(cs, "Volume")
	bars := make([]Bar, 0, len(epochs))
	for i, e := range epochs {
		if e < start.Unix() || e >= end.Unix() {
			continue
		}
		b := Bar{Epoch: e}
		if i < len(closes) {
			b.Close = closes[i]
		}
		if i < len(vols) {
			b.Volume = vols[i]
		}
		bars = append(bars, b)
	}
	return bars, nil
}

// ComputeRange derives rows for symbol on each of days from its 1Min bars on
// disk, reading the whole span once. It returns one row per day, including
// days with no bars (all counts zero).
func ComputeRange(catDir *catalog.Directory, symbol string, days []calendar.DaySessions) ([]Row, error) {
	if len(days) == 0 {
		return nil, nil
	}
	sorted := append([]calendar.DaySessions(nil), days...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Date.Before(sorted[j].Date) })
	bars, err := ReadMinuteBars(catDir, symbol,
		sorted[0].Premarket.Start, sorted[len(sorted)-1].Afterhours.End)
	if err != nil {
		return nil, err
	}
	// Bars are in time order and days do not overlap, so one pass splits
	// them by day.
	rows := make([]Row, len(sorted))
	j := 0
	for i, ds := range sorted {
		start, end := ds.Premarket.Start.Unix(), ds.Afterhours.End.Unix()
		for j < len(bars) && bars[j].Epoch < start {
			j++
		}
		k := j
		for k < len(bars) && bars[k].Epoch < end {
			k++
		}
		rows[i] = Compute(ds, bars[j:k])
		j = k
	}
	return rows, nil
}

// HasSource reports whether symbol has a 1Min bucket.
func HasSource(catDir *catalog.Directory, symbol string) bool {
	_, err := catDir.GetLatestTimeBucketInfoFromKey(io.NewTimeBucketKey(SourceKey(symbol)))
	return err == nil
}

// query reads key over [from, to] (both inclusive). It returns nil, nil when
// the bucket does not exist or holds nothing in range.
func query(catDir *catalog.Directory, key string, from, to time.Time) (*io.ColumnSeries, error) {
	if catDir == nil {
		return nil, fmt.Errorf("catalog directory not initialized")
	}
	tbk := io.NewTimeBucketKey(key)
	if _, err := catDir.GetLatestTimeBucketInfoFromKey(tbk); err != nil {
		return nil, nil // no such bucket
	}
	q := planner.NewQuery(catDir)
	q.AddTargetKey(tbk)
	q.SetRange(from, to)
	parsed, err := q.Parse()
	if err != nil {
		// A key whose bucket exists but has no file covering the range is
		// reported as a parse error by the planner; it holds no rows.
		if strings.Contains(err.Error(), "no files returned") {
			return nil, nil
		}
		return nil, fmt.Errorf("query %s: %w", key, err)
	}
	reader, err := executor.NewReader(parsed)
	if err != nil {
		return nil, fmt.Errorf("reader %s: %w", key, err)
	}
	csm, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", key, err)
	}
	cs := csm[*tbk]
	if cs == nil || cs.Len() == 0 {
		return nil, nil
	}
	return cs, nil
}

// dateOf converts a stored daily epoch to midnight America/New_York of the
// same date. Daily epochs are stored at midnight of the server timezone, so
// the date is read in that timezone.
func dateOf(epoch int64) time.Time {
	y, m, d := time.Unix(epoch, 0).In(utils.InstanceConfig.Timezone).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, calendar.Nasdaq.Tz())
}

func column(cs *io.ColumnSeries, name string) interface{} {
	for _, n := range cs.GetColumnNames() {
		if strings.EqualFold(n, name) {
			return cs.GetColumn(n)
		}
	}
	return nil
}

func floats(cs *io.ColumnSeries, name string) []float64 {
	switch v := column(cs, name).(type) {
	case []float64:
		return v
	case []float32:
		out := make([]float64, len(v))
		for i, f := range v {
			out[i] = float64(f)
		}
		return out
	default:
		return nil
	}
}

func ints(cs *io.ColumnSeries, name string) []int64 {
	switch v := column(cs, name).(type) {
	case []int64:
		return v
	case []int32:
		out := make([]int64, len(v))
		for i, n := range v {
			out[i] = int64(n)
		}
		return out
	case []uint64:
		out := make([]int64, len(v))
		for i, n := range v {
			out[i] = int64(n)
		}
		return out
	case []float64:
		out := make([]int64, len(v))
		for i, f := range v {
			out[i] = int64(f)
		}
		return out
	case []float32:
		out := make([]int64, len(v))
		for i, f := range v {
			out[i] = int64(f)
		}
		return out
	default:
		return nil
	}
}
