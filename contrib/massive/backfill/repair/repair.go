// Package repair replaces split-adjusted bars, written before bars were always
// stored raw, with unadjusted bars. It only touches the bar buckets and date
// ranges that a split could have affected.
package repair

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/splitsync"
	"github.com/alpacahq/marketstore/v4/utils"
)

// Bucket is one stored OHLCV bar bucket.
type Bucket struct {
	TBK       string
	Symbol    string
	Timeframe string
}

// Range is the part of a bucket that must be rewritten with raw bars:
// every bar that starts within [From, To].
type Range struct {
	Bucket
	From time.Time
	To   time.Time
	// Split is the effective date of the latest split inside the bucket.
	Split time.Time
}

// ParseBucket parses "SYM/TF/OHLCV". ok is false for any other bucket,
// including trade and quote buckets, which Massive never adjusts.
func ParseBucket(tbk string) (Bucket, bool) {
	parts := strings.Split(tbk, "/")
	if len(parts) != 3 || parts[2] != "OHLCV" {
		return Bucket{}, false
	}
	return Bucket{TBK: tbk, Symbol: parts[0], Timeframe: parts[1]}, true
}

// SplitDates maps Massive split results to effective dates per symbol.
// Results that cannot be mapped are skipped.
func SplitDates(results []api.Split) map[string][]time.Time {
	out := map[string][]time.Time{}
	for _, s := range results {
		e, err := splitsync.ToEvent(s)
		if err != nil {
			continue
		}
		out[e.Symbol] = append(out[e.Symbol], e.Effective)
	}
	return out
}

// latestSplitAfter returns the latest split that is after first and not
// after now.
func latestSplitAfter(splits []time.Time, first, now time.Time) (time.Time, bool) {
	var best time.Time
	found := false
	for _, s := range splits {
		if s.After(first) && !s.After(now) && (!found || s.After(best)) {
			best, found = s, true
		}
	}
	return best, found
}

// Candidates returns the buckets that have a split on or before now, so the
// caller only needs to look up the first bar of those.
func Candidates(buckets []Bucket, splits map[string][]time.Time, now time.Time) []Bucket {
	var out []Bucket
	for _, b := range buckets {
		for _, s := range splits[b.Symbol] {
			if !s.After(now) {
				out = append(out, b)
				break
			}
		}
	}
	return out
}

// PlanRanges returns the affected range of every bucket whose stored data
// starts before a split that has taken effect by now. firstBars holds the
// first stored bar of each bucket; buckets without one are skipped.
func PlanRanges(buckets []Bucket, firstBars map[string]time.Time, splits map[string][]time.Time,
	now time.Time,
) []Range {
	var out []Range
	for _, b := range buckets {
		first, ok := firstBars[b.TBK]
		if !ok {
			continue
		}
		split, ok := latestSplitAfter(splits[b.Symbol], first, now)
		if !ok {
			continue
		}
		out = append(out, Range{Bucket: b, From: first, To: split.Add(-time.Second), Split: split})
	}
	sortRanges(out)
	return out
}

// sortRanges orders ranges by symbol, then finest to coarsest timeframe, so
// a symbol's 1D bar is always written after its intraday bars.
func sortRanges(rs []Range) {
	sort.SliceStable(rs, func(i, j int) bool {
		if rs[i].Symbol != rs[j].Symbol {
			return rs[i].Symbol < rs[j].Symbol
		}
		return timeframeRank(rs[i].Timeframe) < timeframeRank(rs[j].Timeframe)
	})
}

// timeframeRank orders timeframes finest first, matching
// massiveconfig.OrderedBackfillKeys.
func timeframeRank(tf string) int {
	if d, err := utils.CandleDurationFromString(tf); err == nil {
		return int(d.Duration() / time.Second)
	}
	return math.MaxInt32
}

// Store reads the server's buckets.
type Store interface {
	ListTBKs(ctx context.Context) ([]string, error)
	// FirstBar returns the timestamp of the bucket's first bar; ok is false
	// when the bucket is empty.
	FirstBar(ctx context.Context, tbk string) (t time.Time, ok bool, err error)
}

// Fetcher fetches raw bars of one symbol and writes them to the bucket.
// It returns the number of bars written.
type Fetcher func(ctx context.Context, symbol, timeframe string, from, to time.Time) (int, error)

// Options filter the repair.
type Options struct {
	// Symbols limits the repair to these symbols (all when empty).
	Symbols map[string]bool
	// Timeframes limits the repair to these timeframes (all when empty).
	Timeframes map[string]bool
	// Parallelism is the number of symbols repaired at once.
	Parallelism int
	DryRun      bool
	Now         time.Time
}

// Failure is one bucket that could not be repaired.
type Failure struct {
	Range Range
	Err   error
}

// Report is the outcome of a repair.
type Report struct {
	Ranges   []Range
	Repaired int
	Bars     int
	Failures []Failure
}

// Plan lists the buckets and finds the affected ranges.
func Plan(ctx context.Context, store Store, splitResults []api.Split, opts Options) ([]Range, error) {
	tbks, err := store.ListTBKs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list buckets: %w", err)
	}
	var buckets []Bucket
	for _, tbk := range tbks {
		b, ok := ParseBucket(tbk)
		if !ok {
			continue
		}
		if len(opts.Symbols) > 0 && !opts.Symbols[b.Symbol] {
			continue
		}
		if len(opts.Timeframes) > 0 && !opts.Timeframes[b.Timeframe] {
			continue
		}
		buckets = append(buckets, b)
	}

	splits := SplitDates(splitResults)
	firstBars := map[string]time.Time{}
	for _, b := range Candidates(buckets, splits, opts.Now) {
		first, ok, err := store.FirstBar(ctx, b.TBK)
		if err != nil {
			return nil, fmt.Errorf("first bar of %s: %w", b.TBK, err)
		}
		if ok {
			firstBars[b.TBK] = first
		}
	}
	return PlanRanges(buckets, firstBars, splits, opts.Now), nil
}

// Run rewrites every range with raw bars. Symbols are repaired in parallel;
// the ranges of one symbol run in order. A failed range does not stop the
// others.
func Run(ctx context.Context, ranges []Range, fetch Fetcher, parallelism int) Report {
	rep := Report{Ranges: ranges}
	if parallelism <= 0 {
		parallelism = 1
	}
	bySymbol := map[string][]Range{}
	var order []string
	for _, r := range ranges {
		if _, ok := bySymbol[r.Symbol]; !ok {
			order = append(order, r.Symbol)
		}
		bySymbol[r.Symbol] = append(bySymbol[r.Symbol], r)
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, parallelism)
	for _, sym := range order {
		select {
		case <-ctx.Done():
			wg.Wait()
			return rep
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(rs []Range) {
			defer wg.Done()
			defer func() { <-sem }()
			for _, r := range rs {
				n, err := fetch(ctx, r.Symbol, r.Timeframe, r.From, r.To)
				mu.Lock()
				if err != nil {
					rep.Failures = append(rep.Failures, Failure{Range: r, Err: err})
				} else {
					rep.Repaired++
					rep.Bars += n
				}
				mu.Unlock()
			}
		}(bySymbol[sym])
	}
	wg.Wait()
	return rep
}
