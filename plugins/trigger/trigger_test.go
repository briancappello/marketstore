package trigger_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type EmptyTrigger struct{}

func (t *EmptyTrigger) Fire(keyPath string, records []trigger.Record) {
	// do nothing
}

func TestMatch(t *testing.T) {
	trig := &EmptyTrigger{}
	matcher := trigger.NewMatcher(trig, "*/1Min/OHLC")
	var matched bool
	matched = matcher.Match("TSLA/1Min/OHLC")
	assert.True(t, matched)
	matched = matcher.Match("TSLA/5Min/OHLC")
	assert.False(t, matched)
}

// The "on" condition names a bucket while the key path being matched carries a
// trailing year file, so the match has to be anchored at the start and end on a
// path boundary. An unanchored match fired on any key that merely contained the
// pattern, which silently ran triggers against buckets they never targeted.
func TestMatchIsAnchoredToPathBoundaries(t *testing.T) {
	matcher := trigger.NewMatcher(&EmptyTrigger{}, "*/1Min/OHLCV")

	for _, tc := range []struct {
		keyPath string
		want    bool
		why     string
	}{
		{"AAPL/1Min/OHLCV", true, "exact bucket"},
		{"AAPL/1Min/OHLCV/2024.bin", true, "bucket plus year file"},
		{"AAPL/1Min/OHLCVEXTRA", false, "different bucket sharing a prefix"},
		{"AAPL/1Min/OHLCVEXTRA/2024.bin", false, "different bucket sharing a prefix, with year file"},
		{"NESTED/AAPL/1Min/OHLCV/2024.bin", false, "pattern appearing mid-path"},
		{"AAPL/5Min/OHLCV/2024.bin", false, "different timeframe"},
		{"A/B/1Min/OHLCV", false, "wildcard must not span a path separator"},
	} {
		assert.Equal(t, tc.want, matcher.Match(tc.keyPath), "%s: %s", tc.keyPath, tc.why)
	}
}

// "on" is documented as a glob whose only metacharacter is "*". Everything else
// must be literal; previously the string was fed to the regex engine as-is, so
// a "." in a bucket name behaved as "any character".
func TestMatchTreatsRegexMetacharactersAsLiteral(t *testing.T) {
	matcher := trigger.NewMatcher(&EmptyTrigger{}, "AAPL.B/1Min/OHLCV")

	assert.True(t, matcher.Match("AAPL.B/1Min/OHLCV/2024.bin"))
	assert.False(t, matcher.Match("AAPLXB/1Min/OHLCV/2024.bin"))
}

// Match runs once per registered matcher for every key written, so compiling
// the pattern inside it put regex compilation on the write hot path -- it was a
// measurable share of both CPU and total heap allocation on a live server.
func TestMatchDoesNotRecompilePattern(t *testing.T) {
	matcher := trigger.NewMatcher(&EmptyTrigger{}, "*/1Min/OHLCV")

	allocs := testing.AllocsPerRun(100, func() {
		matcher.Match("AAPL/1Min/OHLCV/2024.bin")
	})

	// Compiling a pattern costs on the order of tens of allocations; matching
	// with a prebuilt one costs none.
	const maxAllocsPerMatch = 2
	if allocs > maxAllocsPerMatch {
		t.Fatalf("Match allocated %.1f objects per call (limit %d): the pattern is being recompiled",
			allocs, maxAllocsPerMatch)
	}
}

func TestRecordsToColumnSeries(t *testing.T) {
	epoch := []int64{
		time.Date(2017, 12, 14, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 14, 10, 4, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 14, 10, 5, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 14, 10, 6, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 14, 10, 10, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 3, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 4, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 5, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 6, 0, 0, utils.InstanceConfig.Timezone).Unix(),
		time.Date(2017, 12, 15, 10, 10, 0, 0, utils.InstanceConfig.Timezone).Unix(),
	}
	open := []float32{1., 2., 3., 4., 5., 1., 2., 3., 4., 5.}
	high := []float32{1.1, 2.1, 3.1, 4.1, 5.1, 1.1, 2.1, 3.1, 4.1, 5.1}
	low := []float32{0.9, 1.9, 2.9, 3.9, 4.9, 0.9, 1.9, 2.9, 3.9, 4.9}
	clos := []float32{1.05, 2.05, 3.05, 4.05, 5.05, 1.05, 2.05, 3.05, 4.05, 5.05}

	tbk := io.NewTimeBucketKey("TEST/1Min/OHLC")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", clos)

	rs, err := cs.ToRowSeries(*tbk, true)
	assert.Nil(t, err)
	rowData := rs.GetData()
	times, _ := rs.GetTime()
	numRows := len(times)
	rowLen := len(rowData) / numRows

	records := make([]trigger.Record, numRows)

	for i := 0; i < numRows; i++ {
		pos := i * rowLen
		record := rowData[pos : pos+rowLen]
		index := io.TimeToIndex(times[i], time.Minute)

		buf, _ := io.Serialize(nil, index)
		buf = append(buf, record[8:]...)

		records[i] = trigger.Record(buf)
	}

	testCS, err := trigger.RecordsToColumnSeries(
		*tbk, cs.GetDataShapes(),
		time.Minute, int16(2017),
		records)
	assert.Nil(t, err)

	for name, col := range cs.GetColumns() {
		testCol := testCS.GetColumn(name)

		cV := reflect.ValueOf(col)
		tcV := reflect.ValueOf(testCol)

		assert.Equal(t, cV.Len(), tcV.Len())
	}

	assert.Equal(t, len(cs.GetEpoch()), len(testCS.GetEpoch()))
	for i := 0; i < len(epoch); i++ {
		assert.Equal(t, cs.GetEpoch()[i], testCS.GetEpoch()[i])
	}
}
