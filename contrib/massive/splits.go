package main

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/metrics"
	"github.com/alpacahq/marketstore/v4/contrib/massive/splitsync"
	"github.com/alpacahq/marketstore/v4/corpaction"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/utils/io"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// splitWriteBatch is the number of symbols written per WriteCSM call.
const splitWriteBatch = 500

// Variables so tests can stub the API and the store.
var (
	listSplits     = api.ListSplits
	writeSplitsCSM = func(csm io.ColumnSeriesMap) error { return executor.WriteCSM(csm, false) }
	splitsCatalog  = func() *catalog.Directory {
		if executor.ThisInstance == nil {
			return nil
		}
		return executor.ThisInstance.CatalogDir
	}
)

// splitSyncResult summarizes one sync.
type splitSyncResult struct {
	results, written, neutralized, unchanged, skipped int
}

// runSplitSyncLoop syncs split events at startup and then daily at the
// configured time. A failed sync is retried at the next scheduled time.
func (mf *MassiveFetcher) runSplitSyncLoop(now func() time.Time, after func(time.Duration) <-chan time.Time) {
	defer mf.wg.Done()
	hour, minute, err := mf.config.SplitsSyncClock()
	if err != nil {
		log.Error("[massive] split sync disabled: %v", err)
		return
	}
	for {
		mf.syncSplitsOnce()
		next := nextSplitSync(now(), hour, minute)
		select {
		case <-mf.ctx.Done():
			return
		case <-after(next.Sub(now())):
		}
	}
}

func (mf *MassiveFetcher) syncSplitsOnce() {
	started := time.Now()
	res, err := mf.syncSplits(mf.httpClient)
	if err != nil {
		if errors.Is(err, api.ErrAuthFailed) {
			log.Error("[massive] split sync stopped: %v (retrying at the next scheduled time)", err)
		} else {
			log.Error("[massive] split sync failed: %v (retrying at the next scheduled time)", err)
		}
		return
	}
	metrics.MassiveSplitsLastSync.SetToCurrentTime()
	log.Info("[massive] split sync done in %s: %d results, %d events written, %d neutralized, "+
		"%d symbols unchanged, %d results skipped",
		time.Since(started).Round(time.Millisecond), res.results, res.written, res.neutralized,
		res.unchanged, res.skipped)
}

// syncSplits reads Massive's full split history and brings the SPLITS
// buckets of every tracked symbol in line with it.
func (mf *MassiveFetcher) syncSplits(client *http.Client) (splitSyncResult, error) {
	dir := splitsCatalog()
	if dir == nil {
		return splitSyncResult{}, errors.New("no catalog directory")
	}

	results, err := listSplits(client, api.SplitsQuery{})
	if err != nil {
		return splitSyncResult{}, fmt.Errorf("list splits: %w", err)
	}

	tracked, wildcard := mf.trackedSymbols(dir)
	candidates := map[string]struct{}{}
	for sym := range tracked {
		candidates[sym] = struct{}{}
	}
	if wildcard {
		for _, s := range results {
			candidates[s.Ticker] = struct{}{}
		}
	}
	stored := map[string][]corpaction.Event{}
	for sym := range candidates {
		events, err := corpaction.LoadAll(dir, sym, corpaction.Splits)
		if err != nil {
			return splitSyncResult{}, fmt.Errorf("load stored splits for %s: %w", sym, err)
		}
		if len(events) > 0 {
			stored[sym] = events
		}
	}

	plan := splitsync.Build(results, func(s string) bool { _, ok := tracked[s]; return ok }, wildcard, stored)
	for _, sk := range plan.Skipped {
		log.Warn("[massive] split sync: skipped %s %s (%s): %s",
			sk.Split.Ticker, sk.Split.ExecutionDate, sk.Split.ID, sk.Reason)
	}
	if err := writeSplitBatches(plan.Events); err != nil {
		return splitSyncResult{}, err
	}
	return splitSyncResult{
		results:     len(results),
		written:     plan.Written,
		neutralized: plan.Neutralized,
		unchanged:   plan.Unchanged,
		skipped:     len(plan.Skipped),
	}, nil
}

// trackedSymbols returns the configured symbols plus every symbol that has
// data in this instance, and whether the configured set is the wildcard.
func (mf *MassiveFetcher) trackedSymbols(dir *catalog.Directory) (map[string]struct{}, bool) {
	tracked := map[string]struct{}{}
	wildcard := false
	for _, si := range mf.config.SymbolInfos {
		if si.Symbol == "*" {
			wildcard = true
			continue
		}
		tracked[si.Symbol] = struct{}{}
	}
	for _, s := range mf.config.Symbols {
		if s == "*" {
			wildcard = true
		}
	}
	items, err := dir.GatherCategoriesAndItems()
	if err != nil {
		log.Warn("[massive] split sync: list catalog symbols: %v", err)
		return tracked, wildcard
	}
	for sym := range items["Symbol"] {
		tracked[sym] = struct{}{}
	}
	return tracked, wildcard
}

// writeSplitBatches writes events grouped by symbol, splitWriteBatch symbols
// per write. Events must be grouped by symbol (splitsync.Plan.Events is).
func writeSplitBatches(events []corpaction.Event) error {
	var batch []corpaction.Event
	symbols := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		csm, err := corpaction.EncodeCSM(batch)
		if err != nil {
			return fmt.Errorf("encode splits: %w", err)
		}
		if err := writeSplitsCSM(csm); err != nil {
			return fmt.Errorf("write splits: %w", err)
		}
		batch, symbols = batch[:0], 0
		return nil
	}
	for i, e := range events {
		if i == 0 || events[i-1].Symbol != e.Symbol {
			if symbols == splitWriteBatch {
				if err := flush(); err != nil {
					return err
				}
			}
			symbols++
		}
		batch = append(batch, e)
	}
	return flush()
}

// nextSplitSync returns the next hour:minute America/New_York after now.
func nextSplitSync(now time.Time, hour, minute int) time.Time {
	et := now.In(calendar.Nasdaq.Tz())
	next := time.Date(et.Year(), et.Month(), et.Day(), hour, minute, 0, 0, et.Location())
	if !next.After(et) {
		next = time.Date(et.Year(), et.Month(), et.Day()+1, hour, minute, 0, 0, et.Location())
	}
	return next
}
