package aggtrigger

import (
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// mixedSeries builds a series with a spread of element types, in a
// deliberately non-alphabetical column order, from the given epochs.
func mixedSeries(rng *rand.Rand, epochs []int64) *io.ColumnSeries {
	n := len(epochs)
	f32 := make([]float32, n)
	f64 := make([]float64, n)
	i64 := make([]int64, n)
	u8 := make([]uint8, n)
	i32 := make([]int32, n)
	for i := 0; i < n; i++ {
		f32[i] = rng.Float32()
		f64[i] = rng.Float64()
		i64[i] = rng.Int63()
		u8[i] = uint8(rng.Intn(256))
		i32[i] = rng.Int31()
	}
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", append([]int64(nil), epochs...))
	cs.AddColumn("Open", f32)
	cs.AddColumn("Volume", i64)
	cs.AddColumn("Close", f64)
	cs.AddColumn("Exchange", u8)
	cs.AddColumn("Count", i32)
	return cs
}

func deepCopy(cs *io.ColumnSeries) *io.ColumnSeries {
	out := io.NewColumnSeries()
	for _, name := range cs.GetColumnNames() {
		v := reflect.ValueOf(cs.GetColumn(name))
		c := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		reflect.Copy(c, v)
		out.AddColumn(name, c.Interface())
	}
	return out
}

func assertSameSeries(t *testing.T, want, got *io.ColumnSeries, msg string) {
	t.Helper()
	require.Equal(t, want.GetColumnNames(), got.GetColumnNames(), "%s: column order", msg)
	require.Equal(t, want.GetDataShapes(), got.GetDataShapes(), "%s: data shapes", msg)
	for _, name := range want.GetColumnNames() {
		require.Equal(t, want.GetColumn(name), got.GetColumn(name), "%s: column %s", msg, name)
	}
}

// TestMergeBatchMatchesColumnSeriesUnion checks mergeBatch against the
// function it replaces over random caches and batches: pure appends (the live
// case), rewrites of existing rows, older-than-cache inserts, duplicate epochs
// within one batch, and empty caches.
func TestMergeBatchMatchesColumnSeriesUnion(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const base = int64(1709625600) // 2024-03-05 08:00 UTC

	for iter := 0; iter < 5000; iter++ {
		// Sorted unique cache on a 60s grid with random gaps.
		cacheN := rng.Intn(50)
		var cacheEp []int64
		e := base
		for i := 0; i < cacheN; i++ {
			e += int64(60 * (1 + rng.Intn(3)))
			cacheEp = append(cacheEp, e)
		}
		maxEp := e

		// Batch: mixture of rewrites, appends, inserts, duplicates.
		batchN := 1 + rng.Intn(5)
		batchEp := make([]int64, batchN)
		for i := range batchEp {
			switch r := rng.Intn(4); {
			case r == 0 && cacheN > 0: // rewrite an existing row
				batchEp[i] = cacheEp[rng.Intn(cacheN)]
			case r == 1: // newer than everything
				batchEp[i] = maxEp + int64(60*(1+rng.Intn(3)))
			case r == 2 && i > 0: // duplicate within the batch
				batchEp[i] = batchEp[rng.Intn(i)]
			default: // anywhere, possibly an insert into a gap
				batchEp[i] = base + int64(60*rng.Intn(cacheN*3+3))
			}
		}

		cache := mixedSeries(rng, cacheEp)
		batch := mixedSeries(rng, batchEp)

		want := io.ColumnSeriesUnion(deepCopy(cache), deepCopy(batch))
		got := mergeBatch(cache, batch)
		assertSameSeries(t, want, got, "iteration")
		if t.Failed() {
			t.Fatalf("iteration %d: cache=%v batch=%v", iter, cacheEp, batchEp)
		}
	}
}

// TestMergeBatchInsertDoesNotDisturbCache: an out-of-order bar must not shift
// rows inside the caller's view of the cache (the cache is only replaced on a
// successful write, so a stale view can survive).
func TestMergeBatchInsertDoesNotDisturbCache(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	cache := mixedSeries(rng, []int64{60, 120, 240, 300})
	before := deepCopy(cache)
	batch := mixedSeries(rng, []int64{180, 360})

	got := mergeBatch(cache, batch)

	assert.Equal(t, []int64{60, 120, 180, 240, 300, 360}, got.GetEpoch())
	assertSameSeries(t, before, cache, "cache after insert")
}

// TestMergeBatchKeepsColumnOrder guards the record layout: Fire decodes
// incoming records with the cached series' data shapes.
func TestMergeBatchKeepsColumnOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	cache := mixedSeries(rng, []int64{60, 120})
	batch := mixedSeries(rng, []int64{120, 180})
	got := mergeBatch(cache, batch)
	assert.Equal(t, cache.GetDataShapes(), got.GetDataShapes())
}

// TestMergeBatchFallsBackOnShapeMismatch: a batch whose shape differs from the
// cache (here, column order) must still produce the ColumnSeriesUnion result.
func TestMergeBatchFallsBackOnShapeMismatch(t *testing.T) {
	cache := io.NewColumnSeries()
	cache.AddColumn("Epoch", []int64{60, 120})
	cache.AddColumn("Open", []float32{1, 2})
	cache.AddColumn("Close", []float32{3, 4})
	batch := io.NewColumnSeries()
	batch.AddColumn("Epoch", []int64{120, 180})
	batch.AddColumn("Close", []float32{9, 10}) // swapped order
	batch.AddColumn("Open", []float32{7, 8})

	want := io.ColumnSeriesUnion(deepCopy(cache), deepCopy(batch))
	got := mergeBatch(cache, batch)
	assertSameSeries(t, want, got, "fallback")
}

// TestRegularSessionMatchesApplyTimeQual checks the binary-searched session
// filter against the per-bar calendar filter it replaces, on normal, early
// close, holiday and weekend days, across day boundaries, and on unsorted
// input.
func TestRegularSessionMatchesApplyTimeQual(t *testing.T) {
	tz := calendar.Nasdaq.Tz()
	rng := rand.New(rand.NewSource(4))
	days := []time.Time{
		time.Date(2021, 8, 31, 0, 0, 0, 0, tz), // normal Tuesday
		time.Date(2018, 7, 3, 0, 0, 0, 0, tz),  // early close 13:00
		time.Date(2018, 1, 15, 0, 0, 0, 0, tz), // MLK holiday
		time.Date(2021, 8, 28, 0, 0, 0, 0, tz), // Saturday
		time.Date(2024, 3, 8, 0, 0, 0, 0, tz),  // Friday before DST change
		time.Date(2024, 3, 11, 0, 0, 0, 0, tz), // Monday after DST change
	}

	check := func(epochs []int64, label string) {
		t.Helper()
		cs := mixedSeries(rng, epochs)
		want := cs.ApplyTimeQual(calendar.Nasdaq.EpochIsRegularMarketOpen)
		got := regularSession(cs)
		assertSameSeries(t, want, got, label)
	}

	for _, day := range days {
		// Every minute of the day, and every second around open/close.
		var full []int64
		for m := 0; m < 24*60; m++ {
			full = append(full, day.Add(time.Duration(m)*time.Minute).Unix())
		}
		check(full, day.Format("2006-01-02")+" full day")

		for _, hhmm := range [][2]int{{9, 30}, {13, 0}, {16, 0}} {
			edge := time.Date(day.Year(), day.Month(), day.Day(), hhmm[0], hhmm[1], 0, 0, tz)
			var around []int64
			for s := -3; s <= 3; s++ {
				around = append(around, edge.Add(time.Duration(s)*time.Second).Unix())
			}
			check(around, edge.Format(time.RFC3339)+" edge")
		}

		// Random sorted subsets within the day.
		for i := 0; i < 200; i++ {
			var sub []int64
			for _, e := range full {
				if rng.Intn(20) == 0 {
					sub = append(sub, e)
				}
			}
			check(sub, day.Format("2006-01-02")+" subset")
		}
	}

	// Spanning two days (fallback path) and unsorted input (fallback path).
	span := []int64{days[0].Add(15 * time.Hour).Unix(), days[0].Add(33 * time.Hour).Unix()}
	check(span, "two days")
	unsorted := []int64{days[0].Add(11 * time.Hour).Unix(), days[0].Add(10 * time.Hour).Unix()}
	check(unsorted, "unsorted")
	check(nil, "empty")

	// Only pre-market: must be empty, not "everything".
	pre := []int64{days[0].Add(5 * time.Hour).Unix(), days[0].Add(6 * time.Hour).Unix()}
	got := regularSession(mixedSeries(rng, pre))
	assert.Empty(t, got.GetEpoch())
}
