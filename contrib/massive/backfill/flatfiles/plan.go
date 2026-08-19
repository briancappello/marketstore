package flatfiles

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill"
	"github.com/alpacahq/marketstore/v4/contrib/massive/massiveconfig"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// SymbolCoverage pairs a symbol with the sync window already confirmed for it,
// as read from the per-symbol sync store (sync_queries / asset_data_vendor).
// A zero Window means no sync record exists yet.
type SymbolCoverage struct {
	Info   massiveconfig.SymbolInfo
	Window massiveconfig.SyncWindow
}

// CoverageUpdate is the per-symbol sync window to persist after a backfill run.
type CoverageUpdate struct {
	Info   massiveconfig.SymbolInfo
	Oldest time.Time
	Newest time.Time
}

// dateRange is an inclusive [start, end] range of calendar dates.
type dateRange struct {
	start, end time.Time
}

// symbolWork is the outstanding date ranges for a single symbol.
type symbolWork struct {
	info   massiveconfig.SymbolInfo
	ranges []dateRange
	oldest time.Time // earliest date needed, persisted as the new oldest boundary
	newest time.Time // latest date needed, capped by the high-water mark on persist
}

// Plan describes which flat file dates must be downloaded and, for each date,
// which symbols still need it.
//
// Flat files are date-major (one file per date covering every symbol) while
// coverage is tracked per symbol. Plan reconciles the two: Dates is the union
// of every symbol's outstanding dates, so each file is downloaded at most once,
// and SymbolsFor narrows a downloaded file to only the symbols that need it.
type Plan struct {
	// Dates is the sorted union of market days that must be downloaded.
	Dates []time.Time

	works []symbolWork
}

// BuildPlan computes the outstanding flat file work for every symbol.
//
// For each symbol the start is floored at its listing date (via
// massiveconfig.EffectiveBackfillStart), so dates before a symbol existed are
// never requested: absence there is expected, not missing data. Symbols whose
// confirmed window already spans [configStart, endDate] produce no work.
func BuildPlan(symbols []SymbolCoverage, configStart, endDate time.Time) Plan {
	configStart = truncateToDate(configStart)
	endDate = truncateToDate(endDate)

	var (
		plan     Plan
		dateSet  = make(map[time.Time]bool)
		addRange = func(w *symbolWork, start, end time.Time) {
			if start.After(end) {
				return
			}
			days := MarketDays(start, end)
			if len(days) == 0 {
				return
			}
			w.ranges = append(w.ranges, dateRange{start: days[0], end: days[len(days)-1]})
			for _, day := range days {
				dateSet[day] = true
			}
		}
	)

	for _, sc := range symbols {
		effectiveStart := truncateToDate(
			massiveconfig.EffectiveBackfillStart(configStart, sc.Info.ListingDate))

		// Listing date beyond the requested range: nothing can exist yet.
		if effectiveStart.After(endDate) {
			continue
		}

		w := symbolWork{info: sc.Info}

		switch {
		case sc.Window.Oldest == nil || sc.Window.Newest == nil:
			// No confirmed coverage: the whole range is outstanding.
			addRange(&w, effectiveStart, endDate)
		default:
			oldest := truncateToDate(*sc.Window.Oldest)
			newest := truncateToDate(*sc.Window.Newest)

			// Backward gap: the requested start predates confirmed coverage.
			if effectiveStart.Before(oldest) {
				addRange(&w, effectiveStart, oldest.AddDate(0, 0, -1))
			}
			// Forward gap: new market days since the last confirmed sync.
			if newest.Before(endDate) {
				addRange(&w, newest.AddDate(0, 0, 1), endDate)
			}
		}

		if len(w.ranges) == 0 {
			continue
		}

		w.oldest = w.ranges[0].start
		w.newest = w.ranges[0].end
		for _, r := range w.ranges[1:] {
			if r.start.Before(w.oldest) {
				w.oldest = r.start
			}
			if r.end.After(w.newest) {
				w.newest = r.end
			}
		}
		plan.works = append(plan.works, w)
	}

	plan.Dates = make([]time.Time, 0, len(dateSet))
	for day := range dateSet {
		plan.Dates = append(plan.Dates, day)
	}
	sort.Slice(plan.Dates, func(i, j int) bool { return plan.Dates[i].Before(plan.Dates[j]) })

	return plan
}

// SymbolsFor returns the set of symbols that still need the given date. A file
// downloaded for one symbol's gap must not be rewritten for symbols that
// already have that date.
func (p Plan) SymbolsFor(date time.Time) map[string]bool {
	date = truncateToDate(date)
	symbols := make(map[string]bool)
	for _, w := range p.works {
		for _, r := range w.ranges {
			if !date.Before(r.start) && !date.After(r.end) {
				symbols[w.info.Symbol] = true
				break
			}
		}
	}
	return symbols
}

// Coverage returns the per-symbol windows to persist after a run that
// contiguously completed dates through highWater.
//
// Newest is capped at highWater so an interrupted run never claims coverage for
// dates it did not write. Symbols with no outstanding work are omitted: their
// stored window is already correct.
func (p Plan) Coverage(highWater time.Time) []CoverageUpdate {
	highWater = truncateToDate(highWater)

	updates := make([]CoverageUpdate, 0, len(p.works))
	for _, w := range p.works {
		// Nothing for this symbol was reached before the run stopped.
		if highWater.Before(w.oldest) {
			continue
		}
		newest := w.newest
		if highWater.Before(newest) {
			newest = highWater
		}
		updates = append(updates, CoverageUpdate{
			Info:   w.info,
			Oldest: w.oldest,
			Newest: newest,
		})
	}
	return updates
}

// HasWork reports whether any symbol needs a download.
func (p Plan) HasWork() bool { return len(p.Dates) > 0 }

// RunSyncedBackfill backfills one flat file data type using per-symbol coverage
// read from (and written back to) the sync_queries store, the same source of
// truth the REST backfill uses. It is shared by the massive.go bgworker and the
// flatfiles CLI so both paths behave identically.
//
// Flat files are date-major (one file per date covering every symbol) while
// coverage is per symbol. BuildPlan reconciles the two: each outstanding date
// is downloaded once and only the symbols that still need it are written from
// it. Coverage is persisted once per symbol after the run, bounded by the
// contiguous high-water mark, so an interrupted run never records coverage for
// dates it did not write.
//
// cfg supplies tuning (Parallelism, write concurrency/buffer, progress bar);
// SymbolsForDate and OnProgress are set internally, so any caller-supplied
// values for those two fields are ignored.
func RunSyncedBackfill(
	ctx context.Context,
	s3Client *S3Client,
	w backfill.Writer,
	db massiveconfig.PGDB,
	symbolInfos []massiveconfig.SymbolInfo,
	syncQueries massiveconfig.SyncQuerySet,
	timeframe string,
	ffType FlatFileType,
	startDate, endDate time.Time,
	cfg BackfillConfig,
) {
	parallelism := cfg.Parallelism
	if parallelism <= 0 {
		parallelism = 8
	}

	// Read every symbol's confirmed window in parallel (pgxpool is safe).
	coverages := make([]SymbolCoverage, len(symbolInfos))
	readWP := worker.NewWorkerPool(ctx, parallelism)
	for i, si := range symbolInfos {
		if si.Symbol == "*" {
			continue
		}
		idx, info := i, si
		readWP.Do(func() {
			cov := SymbolCoverage{Info: info}
			// ID 0 = symbol has no row in the sync store (static/CLI list):
			// treat as no confirmed coverage so the full range is fetched.
			if info.ID != 0 {
				cov.Window = massiveconfig.ReadSyncWindow(ctx, db, syncQueries.Read, info.ID)
			}
			coverages[idx] = cov
		})
	}
	readWP.CloseAndWait()

	// Drop entries skipped above (wildcard symbols leave a zero value).
	tracked := coverages[:0]
	for _, cov := range coverages {
		if cov.Info.Symbol != "" {
			tracked = append(tracked, cov)
		}
	}

	plan := BuildPlan(tracked, startDate, endDate)
	if !plan.HasWork() {
		log.Info("[flatfiles] %s: all %d symbols already covered through %s",
			timeframe, len(tracked), endDate.Format(dateFormat))
		return
	}

	log.Info("[flatfiles] %s: %d dates outstanding across %d symbols (%s to %s)",
		timeframe, len(plan.Dates), len(tracked),
		plan.Dates[0].Format(dateFormat), plan.Dates[len(plan.Dates)-1].Format(dateFormat))

	// Contiguous high-water mark so an interrupted run never records coverage
	// for dates it did not write. OnProgress fires from writer goroutines.
	var (
		hwMu      sync.Mutex
		highWater time.Time
	)

	// The universe is passed for logging and as the fallback set; SymbolsForDate
	// narrows it to the symbols actually outstanding for each date.
	universe := make(map[string]bool, len(tracked))
	for _, cov := range tracked {
		universe[cov.Info.Symbol] = true
	}

	cfg.SymbolsForDate = plan.SymbolsFor
	cfg.OnProgress = func(date time.Time) {
		hwMu.Lock()
		if date.After(highWater) {
			highWater = date
		}
		hwMu.Unlock()
	}

	_, _, err := BackfillDates(ctx, s3Client, w, universe, timeframe,
		ffType.S3Prefix, ffType.S3DataType, plan.Dates, cfg)
	if err != nil && ctx.Err() == nil {
		log.Warn("[flatfiles] %s backfill encountered errors: %v", timeframe, err)
	}

	hwMu.Lock()
	reached := highWater
	hwMu.Unlock()

	if reached.IsZero() {
		log.Warn("[flatfiles] %s: no dates completed, coverage unchanged", timeframe)
		return
	}

	// ponytail: coverage is persisted once per symbol after the run rather than
	// per date. Per-date writes would be symbols x dates UPDATEs; re-running an
	// interrupted backfill is idempotent (WriteCSM overwrites by epoch), so the
	// only cost of a crash is redoing work, never wrong coverage. Coverage()
	// omits symbols with no outstanding work, so untouched rows are never
	// rewritten -- the "update only if necessary" guarantee.
	updates := plan.Coverage(reached)
	writeWP := worker.NewWorkerPool(ctx, parallelism)
	for _, u := range updates {
		if u.Info.ID == 0 {
			continue
		}
		upd := u
		writeWP.Do(func() {
			if err := massiveconfig.WriteSyncTimestamp(
				ctx, db, syncQueries.WriteOldest, upd.Info.ID, upd.Oldest); err != nil {
				log.Warn("[flatfiles] failed to write oldest sync for %s (%s): %v", upd.Info.Symbol, timeframe, err)
			}
			if err := massiveconfig.WriteSyncTimestamp(
				ctx, db, syncQueries.WriteNewest, upd.Info.ID, upd.Newest); err != nil {
				log.Warn("[flatfiles] failed to write newest sync for %s (%s): %v", upd.Info.Symbol, timeframe, err)
			}
		})
	}
	writeWP.CloseAndWait()

	log.Info("[flatfiles] %s: completed through %s, coverage updated for %d symbols",
		timeframe, reached.Format(dateFormat), len(updates))
}

// truncateToDate reduces a timestamp to its calendar date at midnight UTC,
// the representation used for flat file dates. Sync windows are stored as
// TIMESTAMPTZ (a 1D bar epoch is midnight ET, e.g. 2024-01-05T05:00:00Z), so
// they must be reduced to a date before being compared with file dates.
func truncateToDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
