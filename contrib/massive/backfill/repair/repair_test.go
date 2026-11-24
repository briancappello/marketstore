package repair_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/repair"
	"github.com/alpacahq/marketstore/v4/corpaction"
)

var ny, _ = time.LoadLocation("America/New_York")

func et(y int, m time.Month, d, hh, mm int) time.Time { return time.Date(y, m, d, hh, mm, 0, 0, ny) }

var now = et(2026, 11, 23, 18, 0)

func fwd(sym, date string, to float64) api.Split {
	return api.Split{Ticker: sym, ExecutionDate: date, SplitFrom: 1, SplitTo: to, AdjustmentType: "forward_split"}
}

func TestParseBucketIgnoresTicks(t *testing.T) {
	b, ok := repair.ParseBucket("NVDA/1Min/OHLCV")
	require.True(t, ok)
	assert.Equal(t, "NVDA", b.Symbol)
	assert.Equal(t, "1Min", b.Timeframe)
	for _, tbk := range []string{"NVDA/1Sec/TRADE", "NVDA/1Sec/QUOTE", "NVDA/1D/SPLITS", "NVDA/1D/SESSIONS"} {
		_, ok := repair.ParseBucket(tbk)
		assert.False(t, ok, tbk)
	}
}

func TestPlanRanges(t *testing.T) {
	b := func(tbk string) repair.Bucket { r, _ := repair.ParseBucket(tbk); return r }
	buckets := []repair.Bucket{
		b("NVDA/1D/OHLCV"), b("NVDA/1Min/OHLCV"), b("AAPL/1Min/OHLCV"), b("XYZ/1D/OHLCV"),
		b("FUT/1D/OHLCV"), b("NEW/1D/OHLCV"),
	}
	first := map[string]time.Time{
		"NVDA/1D/OHLCV":   et(2016, 1, 4, 0, 0),
		"NVDA/1Min/OHLCV": et(2024, 1, 2, 4, 0),
		"AAPL/1Min/OHLCV": et(2024, 1, 2, 4, 0),
		"XYZ/1D/OHLCV":    et(2019, 1, 2, 0, 0),
		"FUT/1D/OHLCV":    et(2020, 1, 2, 0, 0),
	}
	splits := repair.SplitDates([]api.Split{
		fwd("NVDA", "2021-07-20", 4), fwd("NVDA", "2024-06-10", 10),
		fwd("AAPL", "2020-08-31", 4),
		fwd("XYZ", "2020-01-02", 2), fwd("XYZ", "2022-01-03", 3),
		fwd("FUT", "2026-12-01", 2),
		fwd("NEW", "2025-01-02", 2),
	})
	got := repair.PlanRanges(buckets, first, splits, now)

	var tbks []string
	for _, r := range got {
		tbks = append(tbks, r.TBK)
	}
	// Ordered by symbol, then finest timeframe first. AAPL's split is before
	// its stored range, FUT's split has not happened yet, NEW has no data.
	assert.Equal(t, []string{"NVDA/1Min/OHLCV", "NVDA/1D/OHLCV", "XYZ/1D/OHLCV"}, tbks)

	nvdaMin := got[0]
	assert.Equal(t, et(2024, 1, 2, 4, 0), nvdaMin.From)
	assert.Equal(t, et(2024, 6, 9, 23, 59).Add(59*time.Second), nvdaMin.To,
		"the range ends just before the split's effective time (bars through 2024-06-07)")
	assert.Equal(t, corpaction.EffectiveDate(2024, 6, 10), got[1].Split, "the latest split wins")
	assert.Equal(t, corpaction.EffectiveDate(2022, 1, 3), got[2].Split)
}

type fakeStore struct {
	tbks  []string
	first map[string]time.Time
	asked []string
	mu    sync.Mutex
}

func (f *fakeStore) ListTBKs(context.Context) ([]string, error) { return f.tbks, nil }
func (f *fakeStore) FirstBar(_ context.Context, tbk string) (time.Time, bool, error) {
	f.mu.Lock()
	f.asked = append(f.asked, tbk)
	f.mu.Unlock()
	t, ok := f.first[tbk]
	return t, ok, nil
}

func TestPlanOnlyLooksUpCandidatesAndFilters(t *testing.T) {
	store := &fakeStore{
		tbks: []string{"NVDA/1Min/OHLCV", "NVDA/1Sec/OHLCV", "NVDA/1Sec/TRADE", "MSFT/1Min/OHLCV", "TSLA/1Min/OHLCV"},
		first: map[string]time.Time{
			"NVDA/1Min/OHLCV": et(2024, 1, 2, 4, 0), "NVDA/1Sec/OHLCV": et(2024, 1, 2, 4, 0),
			"TSLA/1Min/OHLCV": et(2022, 1, 3, 4, 0),
		},
	}
	splits := []api.Split{fwd("NVDA", "2024-06-10", 10), fwd("TSLA", "2022-08-25", 3)}

	ranges, err := repair.Plan(context.Background(), store, splits, repair.Options{
		Now: now, Timeframes: map[string]bool{"1Min": true}, Symbols: map[string]bool{"NVDA": true, "MSFT": true},
	})
	require.NoError(t, err)
	require.Len(t, ranges, 1)
	assert.Equal(t, "NVDA/1Min/OHLCV", ranges[0].TBK)
	assert.Equal(t, []string{"NVDA/1Min/OHLCV"}, store.asked, "MSFT has no split; filtered buckets are not queried")
}

func TestRunReportsFailuresAndContinues(t *testing.T) {
	ranges := []repair.Range{
		{Bucket: repair.Bucket{TBK: "A/1Min/OHLCV", Symbol: "A", Timeframe: "1Min"}},
		{Bucket: repair.Bucket{TBK: "A/1D/OHLCV", Symbol: "A", Timeframe: "1D"}},
		{Bucket: repair.Bucket{TBK: "BAD/1D/OHLCV", Symbol: "BAD", Timeframe: "1D"}},
		{Bucket: repair.Bucket{TBK: "C/1D/OHLCV", Symbol: "C", Timeframe: "1D"}},
	}
	var mu sync.Mutex
	var aOrder []string
	rep := repair.Run(context.Background(), ranges, func(_ context.Context, sym, tf string, _, _ time.Time) (int, error) {
		if sym == "BAD" {
			return 0, errors.New("HTTP 500")
		}
		if sym == "A" {
			mu.Lock()
			aOrder = append(aOrder, tf)
			mu.Unlock()
		}
		return 10, nil
	}, 3)
	assert.Equal(t, 3, rep.Repaired)
	assert.Equal(t, 30, rep.Bars)
	require.Len(t, rep.Failures, 1)
	assert.Equal(t, "BAD", rep.Failures[0].Range.Symbol)
	assert.Equal(t, []string{"1Min", "1D"}, aOrder, "a symbol's ranges run in order")
}

func mustSplit(t *testing.T, sym string, y int, m time.Month, d int, from, to float64) corpaction.Event {
	t.Helper()
	st := corpaction.ForwardSplit
	if to < from {
		st = corpaction.ReverseSplit
	}
	e, err := corpaction.NewSplit(sym, corpaction.EffectiveDate(y, m, d), from, to, st)
	require.NoError(t, err)
	return e
}

func bar(ts time.Time, o, c float64) api.AggResult {
	return api.AggResult{EpochMilliseconds: ts.Unix() * 1000, Open: o, High: o, Low: c, Close: c}
}

func TestAdjustedOnlyMatchesSplitFactors(t *testing.T) {
	splits := []corpaction.Event{
		mustSplit(t, "X", 2024, 1, 2, 1, 1.05), // 5% stock dividend
		mustSplit(t, "X", 2024, 6, 10, 1, 10),
		mustSplit(t, "X", 2026, 9, 24, 80, 1), // 1:80 reverse
	}
	d := func(y int, m time.Month, dd int) time.Time { return et(y, m, dd, 0, 0) }
	raw := []api.AggResult{
		bar(d(2023, 12, 1), 10.5, 10.5),       // stored adjusted for all three splits
		bar(d(2023, 12, 4), 10.5, 10.5),       // stored adjusted for the first split only
		bar(d(2024, 6, 7), 1208.9, 1208.88),   // stored adjusted 1:10, the only split after it
		bar(d(2024, 6, 6), 0.700001, 0.70001), // stored raw with more decimals
		bar(d(2023, 3, 23), 3.49, 4.52),       // vendor disagreement: not a split factor
		bar(d(2026, 9, 23), 0.0199, 0.0199),   // stored adjusted for the reverse split
		bar(d(2026, 9, 25), 1.6, 1.6),         // after every split: never adjusted
		bar(d(2026, 9, 28), 1.6, 1.6),         // not stored
	}
	all := 1.05 * 10 / 80
	stored := map[int64]repair.Bar{
		d(2023, 12, 1).Unix(): {Open: 10.5 / all, Close: 10.5 / all},
		d(2023, 12, 4).Unix(): {Open: 10.5 / 1.05, Close: 10.5 / 1.05},
		d(2024, 6, 7).Unix():  {Open: 120.89, Close: 120.888},
		d(2024, 6, 6).Unix():  {Open: 0.7, Close: 0.7},
		d(2023, 3, 23).Unix(): {Open: 3.49, Close: 4.0},
		d(2026, 9, 23).Unix(): {Open: 1.592, Close: 1.592},
		d(2026, 9, 25).Unix(): {Open: 1.5, Close: 1.5},
	}
	got := repair.Adjusted(stored, raw, splits, repair.DefaultTolerance)
	var dates []string
	for _, r := range got {
		dates = append(dates, time.Unix(r.EpochMilliseconds/1000, 0).In(ny).Format("2006-01-02"))
	}
	assert.Equal(t, []string{"2023-12-01", "2023-12-04", "2024-06-07", "2026-09-23"}, dates)
}

func TestSplitEventsDropsFutureSplits(t *testing.T) {
	got := repair.SplitEvents([]api.Split{
		fwd("X", "2026-12-01", 2), fwd("X", "2024-06-10", 10), fwd("X", "2021-07-20", 4),
	}, now)
	require.Len(t, got["X"], 2)
	assert.Equal(t, corpaction.EffectiveDate(2021, 7, 20), got["X"][0].Effective)
}
