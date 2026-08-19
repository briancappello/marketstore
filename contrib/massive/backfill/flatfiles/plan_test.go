package flatfiles

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
)

// d builds a UTC date at midnight, the representation used for flat file dates.
func d(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

// ptr returns a pointer to t, for building SyncWindow/ListingDate fields.
func ptr(t time.Time) *time.Time { return &t }

// covered builds a SymbolCoverage with an existing confirmed sync window.
func covered(symbol string, id int64, oldest, newest time.Time) SymbolCoverage {
	return SymbolCoverage{
		Info:   massiveconfig.SymbolInfo{Symbol: symbol, ID: id},
		Window: massiveconfig.SyncWindow{Oldest: ptr(oldest), Newest: ptr(newest)},
	}
}

// fresh builds a SymbolCoverage with no sync record yet (first run for it).
func fresh(symbol string, id int64) SymbolCoverage {
	return SymbolCoverage{Info: massiveconfig.SymbolInfo{Symbol: symbol, ID: id}}
}

// TestBuildPlan_NewSymbolGetsHistoryEvenWhenOthersAreCovered is the AAPL
// regression: a symbol added to the universe after the historical range was
// already downloaded must still get that range, and the dates must be
// requested only for the symbols that actually need them.
func TestBuildPlan_NewSymbolGetsHistoryEvenWhenOthersAreCovered(t *testing.T) {
	t.Parallel()

	start, end := d(2024, 1, 2), d(2024, 1, 5)
	plan := BuildPlan([]SymbolCoverage{
		covered("YINN", 1, start, end),
		fresh("AAPL", 2),
	}, start, end)

	require.NotEmpty(t, plan.Dates, "new symbol must force its history to be downloaded")
	assert.Equal(t, start, plan.Dates[0])
	assert.Equal(t, end, plan.Dates[len(plan.Dates)-1])

	// Only the uncovered symbol needs these dates; YINN must not be rewritten.
	assert.Equal(t, map[string]bool{"AAPL": true}, plan.SymbolsFor(start))
	assert.Equal(t, map[string]bool{"AAPL": true}, plan.SymbolsFor(end))
}

// TestBuildPlan_PreListingDatesAreNotRequested covers the IPO distinction:
// dates before a symbol's listing date are not missing data, so they must
// never be requested for that symbol.
func TestBuildPlan_PreListingDatesAreNotRequested(t *testing.T) {
	t.Parallel()

	listing := d(2024, 1, 4)
	sym := fresh("IPO", 1)
	sym.Info.ListingDate = ptr(listing)

	plan := BuildPlan([]SymbolCoverage{sym}, d(2024, 1, 2), d(2024, 1, 5))

	assert.Empty(t, plan.SymbolsFor(d(2024, 1, 2)), "pre-listing date must not be requested")
	assert.Empty(t, plan.SymbolsFor(d(2024, 1, 3)), "pre-listing date must not be requested")
	assert.Equal(t, map[string]bool{"IPO": true}, plan.SymbolsFor(listing))

	require.NotEmpty(t, plan.Dates)
	assert.Equal(t, listing, plan.Dates[0], "download must start at the listing date, not config start")
}

// TestBuildPlan_FullyCoveredSymbolHasNoWork ensures we do not re-download data
// whose per-symbol coverage already spans the requested range.
func TestBuildPlan_FullyCoveredSymbolHasNoWork(t *testing.T) {
	t.Parallel()

	start, end := d(2024, 1, 2), d(2024, 1, 5)
	plan := BuildPlan([]SymbolCoverage{covered("YINN", 1, start, end)}, start, end)

	assert.Empty(t, plan.Dates)
}

// TestBuildPlan_BackwardGap covers query_start being moved earlier than the
// symbol's confirmed oldest boundary.
func TestBuildPlan_BackwardGap(t *testing.T) {
	t.Parallel()

	plan := BuildPlan([]SymbolCoverage{
		covered("YINN", 1, d(2024, 1, 4), d(2024, 1, 5)),
	}, d(2024, 1, 2), d(2024, 1, 5))

	assert.Equal(t, []time.Time{d(2024, 1, 2), d(2024, 1, 3)}, plan.Dates)
}

// TestBuildPlan_ForwardGap covers new market days since the last sync.
func TestBuildPlan_ForwardGap(t *testing.T) {
	t.Parallel()

	plan := BuildPlan([]SymbolCoverage{
		covered("YINN", 1, d(2024, 1, 2), d(2024, 1, 3)),
	}, d(2024, 1, 2), d(2024, 1, 5))

	assert.Equal(t, []time.Time{d(2024, 1, 4), d(2024, 1, 5)}, plan.Dates)
}

// TestPlanCoverage_OnlyReportsSymbolsThatHadWork ensures per-symbol coverage is
// persisted for backfilled symbols only, bounded by the contiguous high-water
// mark actually reached.
func TestPlanCoverage_OnlyReportsSymbolsThatHadWork(t *testing.T) {
	t.Parallel()

	start, end := d(2024, 1, 2), d(2024, 1, 5)
	plan := BuildPlan([]SymbolCoverage{
		covered("YINN", 1, start, end),
		fresh("AAPL", 2),
	}, start, end)

	updates := plan.Coverage(end)

	require.Len(t, updates, 1, "only the symbol that had work gets a coverage write")
	assert.Equal(t, "AAPL", updates[0].Info.Symbol)
	assert.Equal(t, start, updates[0].Oldest)
	assert.Equal(t, end, updates[0].Newest)
}

// TestPlanCoverage_NewestBoundedByHighWater ensures an interrupted run never
// claims coverage past the last contiguously completed date.
func TestPlanCoverage_NewestBoundedByHighWater(t *testing.T) {
	t.Parallel()

	start, end := d(2024, 1, 2), d(2024, 1, 5)
	plan := BuildPlan([]SymbolCoverage{fresh("AAPL", 1)}, start, end)

	updates := plan.Coverage(d(2024, 1, 3))

	require.Len(t, updates, 1)
	assert.Equal(t, d(2024, 1, 3), updates[0].Newest)
}

// TestResolveSymbolsForDate covers the seam BackfillDates uses to narrow a
// date-major flat file to the symbols that actually need that date.
func TestResolveSymbolsForDate(t *testing.T) {
	t.Parallel()

	static := map[string]bool{"AAPL": true, "YINN": true}

	t.Run("falls back to the static set when no resolver is configured", func(t *testing.T) {
		got := resolveSymbolsForDate(BackfillConfig{}, static, d(2024, 1, 2))
		assert.Equal(t, static, got)
	})

	t.Run("uses the per-date resolver when configured", func(t *testing.T) {
		cfg := BackfillConfig{SymbolsForDate: func(date time.Time) map[string]bool {
			if date.Equal(d(2024, 1, 2)) {
				return map[string]bool{"AAPL": true}
			}
			return nil
		}}
		assert.Equal(t, map[string]bool{"AAPL": true}, resolveSymbolsForDate(cfg, static, d(2024, 1, 2)))
		assert.Empty(t, resolveSymbolsForDate(cfg, static, d(2024, 1, 3)))
	})
}
