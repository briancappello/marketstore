package framework

import (
	"container/list"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework/session"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// ErrUnknownList: no registered strategy publishes a list with that name.
var ErrUnknownList = errors.New("no such watchlist")

// RankingQuery asks for rankings. Empty Session and AsOf mean the live
// session; see the trading-sessions spec for how they resolve. Empty Names
// means every list available in the resolved session.
type RankingQuery struct {
	Names   []string
	Session string
	AsOf    string
}

// RankingResult is the answer to a RankingQuery.
type RankingResult struct {
	Window session.Window
	Lists  []RankedList
}

// RankingsFor answers q: the live lists when q resolves to the live
// session, and a rewind otherwise.
func (w *WatchlistWorker) RankingsFor(q RankingQuery) (RankingResult, error) {
	sq, err := parseQuery(q)
	if err != nil {
		return RankingResult{}, err
	}
	at := now()
	win, err := session.Resolve(sq, at)
	if err != nil {
		return RankingResult{}, err
	}
	strategies, err := w.knownStrategies()
	if err != nil {
		return RankingResult{}, err
	}
	if err = checkNames(q.Names, strategies, win.Session); err != nil {
		return RankingResult{}, err
	}

	var lists []RankedList
	if live, err2 := session.Resolve(session.Query{}, at); err2 == nil && isLive(win, live) && Manager != nil {
		lists = Manager.AllLists()
	} else {
		if lists, err = w.rewinder().rankingsAt(win); err != nil {
			return RankingResult{}, err
		}
	}
	return RankingResult{Window: win, Lists: filterLists(lists, q.Names)}, nil
}

func parseQuery(q RankingQuery) (session.Query, error) {
	var sq session.Query
	s, ok, err := session.ParseSession(q.Session)
	if err != nil {
		return sq, err
	}
	if ok {
		sq.Session = &s
	}
	if sq.AsOf, err = session.ParseAsOf(q.AsOf); err != nil {
		return sq, err
	}
	return sq, nil
}

// isLive reports whether window w is what the live rankings show: the same
// session of the same date, still in progress up to now, or complete in
// both.
func isLive(w, live session.Window) bool {
	return w.TradingDate.Equal(live.TradingDate) && w.Session == live.Session &&
		w.Complete == live.Complete && (w.Complete || w.End.Equal(live.End))
}

// checkNames validates requested list names against the lists the
// strategies publish: a name no strategy publishes in any session is
// unknown (for example GAP_UP, whose strategy is traditional-only and
// publishes GAP_UP_TRADITIONAL), and a name published only in other
// sessions is not available in s.
func checkNames(names []string, strategies []WatchlistStrategy, s calendar.Session) error {
	if len(names) == 0 {
		return nil
	}
	inSession := map[string]bool{}
	anywhere := map[string]bool{}
	for _, st := range strategies {
		for _, sess := range calendar.Sessions {
			for _, n := range PublishedNames(st, sess) {
				anywhere[n] = true
				if sess == s {
					inSession[n] = true
				}
			}
		}
	}
	for _, name := range names {
		switch {
		case !anywhere[name]:
			return fmt.Errorf("%w: %s", ErrUnknownList, name)
		case !inSession[name]:
			return fmt.Errorf("%w: %s is only available in the regular session", session.ErrNotInSession, name)
		}
	}
	return nil
}

// knownStrategies returns strategy instances to validate names against: the
// live ones, or the rewind's own when the live manager is not running.
func (w *WatchlistWorker) knownStrategies() ([]WatchlistStrategy, error) {
	if Manager != nil && len(Manager.strategies) > 0 {
		return Manager.strategies, nil
	}
	r := w.rewinder()
	r.runMu.Lock()
	defer r.runMu.Unlock()
	if err := r.build(); err != nil {
		return nil, err
	}
	return r.strategies, nil
}

// filterLists keeps the named lists, in the order asked; all lists sorted
// by name when names is empty. A requested list that produced nothing is
// returned empty rather than dropped.
func filterLists(lists []RankedList, names []string) []RankedList {
	byName := make(map[string]RankedList, len(lists))
	for _, l := range lists {
		byName[l.Name] = l
	}
	if len(names) == 0 {
		out := append([]RankedList(nil), lists...)
		sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
		return out
	}
	out := make([]RankedList, 0, len(names))
	for _, n := range names {
		l, ok := byName[n]
		if !ok {
			_, b := SplitListName(n)
			l = RankedList{Name: n, Basis: b}
		}
		out = append(out, l)
	}
	return out
}

// --- rewind engine ---

// rewindCacheSize bounds the cache of complete-window results.
const rewindCacheSize = 64

// rewinder computes rankings for windows other than the live one. It never
// touches live states or live strategy instances: it folds bars from disk
// into fresh states and ranks them with its own strategy instances.
type rewinder struct {
	medianWindow   int
	strategyConfig map[string]map[string]interface{}

	// runMu lets one rewind compute at a time, bounding the extra read load.
	runMu sync.Mutex
	// strategies and curator are the rewind's own instances, created on
	// first use and only used under runMu.
	strategies []WatchlistStrategy
	curator    Curator
	built      bool

	flightMu sync.Mutex
	flight   map[string]*rewindCall

	cacheMu sync.Mutex
	cache   map[string]*list.Element
	lru     *list.List

	// computeHook, when set (tests), is called at the start of each
	// computation.
	computeHook func(session.Window)
}

type rewindCall struct {
	done  chan struct{}
	lists []RankedList
	err   error
}

type rewindCacheItem struct {
	key   string
	day   int64
	lists []RankedList
}

func newRewinder(medianWindow int, strategyConfig map[string]map[string]interface{}) *rewinder {
	r := &rewinder{
		medianWindow:   medianWindow,
		strategyConfig: strategyConfig,
		flight:         map[string]*rewindCall{},
		cache:          map[string]*list.Element{},
		lru:            list.New(),
	}
	if Facts != nil {
		Facts.OnChange(r.invalidate)
	}
	return r
}

// rewinder returns the worker's rewind engine, creating it on first use.
func (w *WatchlistWorker) rewinder() *rewinder {
	w.rewindOnce.Do(func() {
		w.rewind = newRewinder(w.config.MedianWindow, w.config.StrategyConfig)
	})
	return w.rewind
}

func windowKey(win session.Window) string {
	return fmt.Sprintf("%s|%s|%d|%d|v%d", win.TradingDateString(), win.Session, win.Start.Unix(),
		win.End.Unix(), sessionfacts.Version)
}

// rankingsAt returns the lists for win. Identical concurrent requests share
// one computation; complete windows are cached until the facts of their
// date change.
func (r *rewinder) rankingsAt(win session.Window) ([]RankedList, error) {
	key := windowKey(win)
	if win.Complete {
		if lists, ok := r.cached(key); ok {
			return lists, nil
		}
	}

	r.flightMu.Lock()
	if c, ok := r.flight[key]; ok {
		r.flightMu.Unlock()
		<-c.done
		return c.lists, c.err
	}
	c := &rewindCall{done: make(chan struct{})}
	r.flight[key] = c
	r.flightMu.Unlock()

	c.lists, c.err = r.compute(win)
	if c.err == nil && win.Complete {
		r.store(key, win.TradingDate.Unix(), c.lists)
	}
	r.flightMu.Lock()
	delete(r.flight, key)
	r.flightMu.Unlock()
	close(c.done)
	return c.lists, c.err
}

// compute folds every symbol's bars for win into fresh states and ranks them.
func (r *rewinder) compute(win session.Window) ([]RankedList, error) {
	r.runMu.Lock()
	defer r.runMu.Unlock()
	if r.computeHook != nil {
		r.computeHook(win)
	}
	if Facts == nil {
		return nil, errors.New("session facts are not running")
	}
	if err := r.build(); err != nil {
		return nil, err
	}
	day, err := calendar.Nasdaq.SessionBoundsAt(win.TradingDate)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	catDir := executorCatalog()
	states := foldDay(Facts, catDir, DiscoverSymbols(catDir), day, win.End, r.medianWindow, now())

	curated := make(map[string]*SymbolState, len(states))
	for sym, st := range states {
		if r.curator == nil || r.curator.Evaluate(sym, st.curationSnapshot()) {
			curated[sym] = st
		}
	}
	lists := rankAll(r.strategies, curated, win)
	log.Info("[watchlist] rewind %s %s to %s: %d symbols, %d curated, %v",
		win.TradingDateString(), win.Session, win.End.In(calendar.Nasdaq.Tz()).Format("15:04"),
		len(states), len(curated), time.Since(start).Round(time.Millisecond))
	return lists, nil
}

// build creates the rewind's own curator and strategy instances.
func (r *rewinder) build() error {
	if r.built {
		return nil
	}
	if f := GetCuratorFactory(); f != nil {
		c, err := f(nil)
		if err != nil {
			return fmt.Errorf("create rewind curator: %w", err)
		}
		r.curator = c
	}
	for name, f := range GetAllWatchlistFactories() {
		var conf map[string]interface{}
		if r.strategyConfig != nil {
			conf = r.strategyConfig[name]
		}
		s, err := f(conf)
		if err != nil {
			log.Error("[watchlist] rewind: create %q: %v", name, err)
			continue
		}
		r.strategies = append(r.strategies, s)
	}
	sort.Slice(r.strategies, func(i, j int) bool { return r.strategies[i].Name() < r.strategies[j].Name() })
	r.built = true
	return nil
}

// foldDay builds a fresh state per symbol positioned on trading day day:
// the day's baselines, and its 1Min bars before end. Symbols are processed
// in parallel.
func foldDay(facts *sessionfacts.Service, catDir *catalog.Directory, symbols []string,
	day calendar.DaySessions, end time.Time, medianWindow int, at time.Time,
) map[string]*SymbolState {
	out := make(map[string]*SymbolState, len(symbols))
	var mu sync.Mutex
	work := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for symbol := range work {
				bars := readDayBars(catDir, symbol, day.Premarket.Start, end)
				if len(bars) == 0 {
					continue // no bars that day: nothing to rank
				}
				st := NewSymbolState()
				b, err := LoadBaselines(facts, catDir, symbol, day, medianWindow, at)
				if err != nil {
					log.Debug("[watchlist] rewind baselines for %s: %v", symbol, err)
				} else {
					st.SetBaselines(b)
				}
				st.applyBars(bars, false)
				mu.Lock()
				out[symbol] = st
				mu.Unlock()
			}
		}()
	}
	for _, s := range symbols {
		work <- s
	}
	close(work)
	wg.Wait()
	return out
}

func (r *rewinder) cached(key string) ([]RankedList, bool) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	el, ok := r.cache[key]
	if !ok {
		return nil, false
	}
	r.lru.MoveToFront(el)
	return el.Value.(*rewindCacheItem).lists, true
}

func (r *rewinder) store(key string, day int64, lists []RankedList) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	if el, ok := r.cache[key]; ok {
		r.lru.MoveToFront(el)
		return
	}
	r.cache[key] = r.lru.PushFront(&rewindCacheItem{key: key, day: day, lists: lists})
	for r.lru.Len() > rewindCacheSize {
		old := r.lru.Back()
		r.lru.Remove(old)
		delete(r.cache, old.Value.(*rewindCacheItem).key)
	}
}

// invalidate drops cached results for trading dates whose facts changed.
// Facts of date D feed rankings of D (its own bars) and of later dates (as
// baselines), so every cached date >= the earliest changed date goes.
func (r *rewinder) invalidate(changed []sessionfacts.Entry) {
	if len(changed) == 0 {
		return
	}
	earliest := changed[0].Date.Unix()
	for _, e := range changed[1:] {
		if d := e.Date.Unix(); d < earliest {
			earliest = d
		}
	}
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	for key, el := range r.cache {
		if el.Value.(*rewindCacheItem).day >= earliest {
			r.lru.Remove(el)
			delete(r.cache, key)
		}
	}
}
