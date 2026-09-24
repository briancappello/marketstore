package main

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/massive/api"
	"github.com/alpacahq/marketstore/v4/contrib/massive/backfill/rest"
	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

/*
Stream outage recovery.

When the WebSocket drops, every bar streamed during the outage is lost. The
previous recovery re-ran the full startup backfill on every reconnect, with
nothing preventing runs from overlapping. That refetched two days of 1Min bars
for every symbol (~5 minutes of REST calls) to repair a gap of a minute or two,
and never refetched the streamed 1Sec bars at all, so the gap stayed in them.

Now:
  - All backfills go through one backfillRunner, so at most one runs at a
    time. Requests that arrive while one is running are merged and run once
    afterwards.
  - A reconnect after a short intraday outage fetches just the outage window,
    for the bar timeframes the stream delivers (1Sec in production), using
    exact millisecond bounds. The on-disk triggers then derive 1Min and 1D
    from those bars exactly as they do for live data.
  - An outage that is long or spans a date (e.g. the nightly provider drop)
    still gets the full backfill.
*/

const (
	// gapFillMaxOutage is the longest outage filled with a windowed fetch.
	// Longer outages get the full backfill instead.
	gapFillMaxOutage = 2 * time.Hour
	// gapFillLead re-fetches from the start of the minute before the last
	// received message. Bars arrive after their second ends and messages
	// buffered at the moment of the drop may be lost, so the last message
	// time is not a precise lower bound.
	gapFillLead = time.Minute
	// gapFillOverlap extends the window past the reconnect, so bars in flight
	// while the new connection was subscribing are covered too.
	gapFillOverlap = 5 * time.Second
	// gapFillSettle keeps the window clear of the second still in progress,
	// which the vendor may not have finalized yet.
	gapFillSettle = 2 * time.Second
)

// outage is a time window with missing streamed data.
type outage struct {
	from, to time.Time
}

func (o outage) merge(other outage) outage {
	if other.from.Before(o.from) {
		o.from = other.from
	}
	if other.to.After(o.to) {
		o.to = other.to
	}
	return o
}

// backfillRequest asks for the full backfill, an outage fill, or both.
type backfillRequest struct {
	full bool
	gap  *outage
}

func (r backfillRequest) empty() bool { return !r.full && r.gap == nil }

func (r backfillRequest) merge(other backfillRequest) backfillRequest {
	r.full = r.full || other.full
	switch {
	case r.gap == nil:
		r.gap = other.gap
	case other.gap != nil:
		m := r.gap.merge(*other.gap)
		r.gap = &m
	}
	return r
}

// backfillRunner runs backfill requests one at a time.
type backfillRunner struct {
	mu      sync.Mutex
	running bool
	pending backfillRequest
	run     func(backfillRequest)
	wg      *sync.WaitGroup // tracks the worker goroutine for shutdown; may be nil
}

func newBackfillRunner(run func(backfillRequest), wg *sync.WaitGroup) *backfillRunner {
	return &backfillRunner{run: run, wg: wg}
}

// submit queues req. If a backfill is already running, req is merged with
// anything else waiting and runs after it finishes.
func (b *backfillRunner) submit(req backfillRequest) {
	if b == nil || req.empty() {
		return
	}
	b.mu.Lock()
	b.pending = b.pending.merge(req)
	if b.running {
		b.mu.Unlock()
		return
	}
	b.running = true
	if b.wg != nil {
		b.wg.Add(1)
	}
	b.mu.Unlock()
	go b.loop()
}

func (b *backfillRunner) loop() {
	if b.wg != nil {
		defer b.wg.Done()
	}
	for {
		b.mu.Lock()
		req := b.pending
		b.pending = backfillRequest{}
		if req.empty() {
			b.running = false
			b.mu.Unlock()
			return
		}
		b.mu.Unlock()
		b.run(req)
	}
}

// planReconnectBackfill decides what a reconnect needs. lastData is when the
// last data message arrived before the drop (zero if none ever did).
func planReconnectBackfill(lastData, reconnectedAt time.Time) backfillRequest {
	if lastData.IsZero() {
		// Nothing was ever received, so there is no known start for the gap.
		return backfillRequest{full: true}
	}
	o := outage{
		from: lastData.Truncate(time.Minute).Add(-gapFillLead),
		to:   reconnectedAt.Add(gapFillOverlap),
	}
	tz := calendar.Nasdaq.Tz()
	fy, fm, fd := o.from.In(tz).Date()
	ty, tm, td := o.to.In(tz).Date()
	if o.to.Sub(o.from) > gapFillMaxOutage || fy != ty || fm != tm || fd != td {
		return backfillRequest{full: true}
	}
	return backfillRequest{gap: &o}
}

// streamedBarTimeframes returns the bar timeframes the stream delivers, in
// the order they should be filled (finest first).
func (mf *MassiveFetcher) streamedBarTimeframes() []string {
	var tfs []string
	for _, tf := range []string{"1Sec", "1Min"} {
		if _, ok := mf.wsDataTypes[tf]; ok {
			tfs = append(tfs, tf)
		}
	}
	return tfs
}

// fetchBarsWindow is rest.BarsWindow; a variable so tests can stub the API.
var fetchBarsWindow = rest.BarsWindow

// fillOutage re-fetches the streamed bar timeframes for every symbol over o.
func (mf *MassiveFetcher) fillOutage(o outage) {
	if latest := time.Now().Add(-gapFillSettle); o.to.After(latest) {
		o.to = latest
	}
	tfs := mf.streamedBarTimeframes()
	if len(tfs) == 0 || !o.to.After(o.from) {
		return
	}

	adjusted := true
	if mf.config.BackfillAdjusted != nil {
		adjusted = *mf.config.BackfillAdjusted
	}
	limit := mf.config.BackfillBatchSize
	if limit <= 0 {
		limit = defaultBackfillBatchSize
	}
	parallelism := mf.config.BackfillParallelism
	if parallelism <= 0 {
		parallelism = runtime.NumCPU()
	}

	symbols := make([]string, 0, len(mf.config.SymbolInfos))
	for _, si := range mf.config.SymbolInfos {
		if si.Symbol != "*" {
			symbols = append(symbols, si.Symbol)
		}
	}
	sort.Strings(symbols)

	start := time.Now()
	log.Info("[massive] filling stream outage %s to %s (%s) for %v, %d symbols",
		o.from.Format(time.RFC3339), o.to.Format(time.RFC3339), o.to.Sub(o.from).Round(time.Second), tfs, len(symbols))

	var bars, withData, failed atomic.Int64
	wp := worker.NewWorkerPool(mf.ctx, parallelism)
	for _, sym := range symbols {
		sym := sym
		if !wp.Do(func() {
			got := false
			for _, tf := range tfs {
				n, err := fetchBarsWindow(mf.ctx, mf.httpClient, sym, tf, o.from, o.to, limit, adjusted, nil)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					if errors.Is(err, api.ErrAuthFailed) {
						log.Error("[massive] API authentication failed during outage fill: %v", err)
						mf.cancel()
						return
					}
					failed.Add(1)
					log.Warn("[massive] outage fill %s %s: %v", sym, tf, err)
					continue
				}
				if n > 0 {
					got = true
					bars.Add(int64(n))
				}
			}
			if got {
				withData.Add(1)
			}
		}) {
			break // pool closed: shutting down
		}
	}
	wp.CloseAndWait()

	log.Info("[massive] stream outage fill complete: %d bars for %d symbols (%d failed requests) in %s",
		bars.Load(), withData.Load(), failed.Load(), time.Since(start).Round(time.Millisecond))
}

// runBackfillRequest is the backfillRunner's run function.
func (mf *MassiveFetcher) runBackfillRequest(req backfillRequest) {
	if req.full {
		if err := mf.runBackfill(); err != nil && !errors.Is(err, context.Canceled) {
			log.Warn("[massive] backfill failed: %v", err)
		}
	}
	if req.gap != nil && mf.ctx.Err() == nil {
		mf.fillOutage(*req.gap)
	}
}

// referenceSymbols trade nearly every second of every session, so their
// newest streamed bar on disk marks when the previous process stopped
// receiving the stream.
var referenceSymbols = []string{"SPY", "QQQ", "AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "IWM"}

// lastTimestampOf is findLastTimestamp; a variable so tests can stub storage.
var lastTimestampOf = findLastTimestamp

// lastStreamedOnDisk estimates when the stream last delivered data before
// this process started: the newest bar, in the finest streamed timeframe,
// across the configured reference symbols. Zero if there is none.
//
// A restart is an outage too (every deploy is one), but a new process has no
// record of when its predecessor's stream stopped. This recovers it from
// disk. It must run before streaming starts, or it would see new bars.
func (mf *MassiveFetcher) lastStreamedOnDisk() time.Time {
	tfs := mf.streamedBarTimeframes()
	if len(tfs) == 0 {
		return time.Time{}
	}
	configured := make(map[string]bool, len(mf.config.SymbolInfos))
	for _, si := range mf.config.SymbolInfos {
		configured[si.Symbol] = true
	}
	var latest time.Time
	for _, sym := range referenceSymbols {
		if !configured[sym] {
			continue
		}
		if ts := lastTimestampOf(tbkForDataType(sym, tfs[0])); ts.After(latest) {
			latest = ts
		}
	}
	return latest
}

// startupBackfill builds the request made when the fetcher starts: the full
// backfill (if query_start is configured) plus, after a short intraday
// restart, a fill of the stream outage the restart caused.
func (mf *MassiveFetcher) startupBackfill(now time.Time) backfillRequest {
	req := backfillRequest{full: len(mf.config.QueryStart) > 0}
	last := mf.lastStreamedOnDisk()
	if last.IsZero() {
		return req
	}
	if plan := planReconnectBackfill(last, now); plan.gap != nil {
		req.gap = plan.gap
		log.Info("[massive] stream last delivered data at %s before this start; queuing outage fill %s to %s",
			last.Format(time.RFC3339), plan.gap.from.Format(time.RFC3339), plan.gap.to.Format(time.RFC3339))
	}
	return req
}
