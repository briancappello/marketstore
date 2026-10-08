package framework

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/calendar"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/framework/session"
	"github.com/alpacahq/marketstore/v4/contrib/watchlist/sessionfacts"
	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// WatchlistWorker is the MarketStore background worker that precomputes
// baselines and runs the periodic ranking goroutine.
type WatchlistWorker struct {
	config WorkerConfig
	ctx    context.Context
	cancel context.CancelFunc

	// rankingMu protects concurrent calls to TriggerRanking.
	rankingMu sync.Mutex

	// baselineDate is the trading day whose baselines were last loaded.
	baselineDate time.Time

	// rewind serves rankings for windows other than the live one.
	rewindOnce sync.Once
	rewind     *rewinder

	// timeframe is the timeframe used for watchlist/curation push keys.
	// Defaults to "1Min" but could be made configurable.
	timeframe string
}

// NewBgWorker creates a new WatchlistWorker from the raw plugin config.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	cfg, err := ParseWorkerConfig(conf)
	if err != nil {
		return nil, fmt.Errorf("watchlist worker config error: %w", err)
	}
	// Validated by ParseWorkerConfig. Set here, before Run, so no state
	// computes DollarVolumeRate over the default window first.
	if secs, ok, _ := cfg.lookbackSecs(); ok {
		dollarVolLookback.Store(secs)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &WatchlistWorker{
		config:    *cfg,
		ctx:       ctx,
		cancel:    cancel,
		timeframe: "1Min",
	}, nil
}

// Run is called by MarketStore in a goroutine at server startup.
// It initializes the shared state manager, computes baselines, creates
// the Curator and WatchlistStrategy instances, and starts the ranking loop.
func (w *WatchlistWorker) Run() {
	log.Info("[watchlist] worker starting")

	// Initialize the shared state manager.
	Manager = NewSymbolStateManager()

	// Session facts: read everywhere, written only by the leader.
	w.startSessionFacts()

	// Create the Curator.
	if curator, err := newCurator(w.config.Curation); err != nil {
		log.Error("[watchlist] failed to create curator: %v", err)
	} else if curator != nil {
		Manager.SetCurator(curator)
		log.Info("[watchlist] curator registered (curation config: %v)", w.config.Curation)
	} else {
		log.Warn("[watchlist] no curator registered, all symbols will be curated")
	}

	// Create WatchlistStrategy instances from registered factories.
	for name, factory := range GetAllWatchlistFactories() {
		var strategyConf map[string]interface{}
		if w.config.StrategyConfig != nil {
			strategyConf = w.config.StrategyConfig[name]
		}
		if strategyConf == nil {
			strategyConf = map[string]interface{}{}
		}
		strategy, err := factory(strategyConf)
		if err != nil {
			log.Error("[watchlist] failed to create watchlist %q: %v", name, err)
			continue
		}
		Manager.AddStrategy(strategy)
		log.Info("[watchlist] watchlist strategy registered: %s", strategy.Name())
	}

	// Position every symbol on the live session's trading date: load that
	// date's baselines and fold its 1Min bars so far. On a weekend or
	// overnight this is the last completed session's date.
	catDir := executor.ThisInstance.CatalogDir
	symbols := DiscoverSymbols(catDir)
	if live, err := session.Resolve(session.Query{}, now()); err != nil {
		log.Error("[watchlist] resolve live session: %v", err)
	} else if day, err := calendar.Nasdaq.SessionBoundsAt(live.TradingDate); err == nil {
		seedDay(Manager, Facts, catDir, symbols, day, w.config.MedianWindow, now())
		w.baselineDate = day.Date
	}
	// Overnight or on a weekend, also load the next trading day's baselines
	// so they are in place when its premarket opens.
	w.baselineDate = w.refreshBaselines(w.baselineDate, symbols)

	// Initialize the curator with computed states.
	if Manager.curator != nil {
		Manager.curator.Init(Manager.AllStates())
	}

	// Run initial curation pass using the seeded state. This evaluates every
	// symbol against the curator so that the curated set is populated at startup
	// without waiting for live ticks. Clients connecting during market-closed
	// hours will see the curated universe and watchlist rankings immediately.
	initialCurationPass(Manager)

	// Run one initial ranking cycle so watchlists are populated before any
	// clients connect or ticks arrive.
	w.TriggerRanking()
	log.Info("[watchlist] initial curation: %d symbols curated out of %d total",
		Manager.CuratedCount(), Manager.SymbolCount())

	// Start the ranking loop. Besides every interval, it runs just after
	// each session boundary so live rankings switch sessions on time.
	interval := time.Duration(w.config.RankingIntervalMs) * time.Millisecond
	log.Info("[watchlist] ranking loop started (interval=%v)", interval)

	for {
		timer := time.NewTimer(time.Until(nextTick(now(), interval)))
		select {
		case <-timer.C:
			w.TriggerRanking()
			w.baselineDate = w.refreshBaselines(w.baselineDate, nil)
		case <-w.ctx.Done():
			timer.Stop()
			log.Info("[watchlist] worker shutting down")
			return
		}
	}
}

// refreshBaselines loads baselines for the trading day the live states
// should hold (see baselineDay) when it differs from loaded, and returns the
// day now loaded. symbols defaults to every symbol in the catalog. Loading
// runs in the background so the ranking loop is never delayed by it.
func (w *WatchlistWorker) refreshBaselines(loaded time.Time, symbols []string) time.Time {
	if Facts == nil {
		return loaded
	}
	day, err := baselineDay(now())
	if err != nil || day.Date.Equal(loaded) {
		return loaded
	}
	catDir := executor.ThisInstance.CatalogDir
	if symbols == nil {
		symbols = DiscoverSymbols(catDir)
	}
	mgr, facts, at, window := Manager, Facts, now(), w.config.MedianWindow
	go loadBaselines(mgr, facts, catDir, symbols, day, window, at)
	return day.Date
}

// startSessionFacts creates the session facts service and, on the leader,
// runs its daily job and late-data recompute until shutdown. A replica gets
// a read-only service: it never writes facts, it reads the ones replicated
// from the leader.
func (w *WatchlistWorker) startSessionFacts() {
	cfg := sessionfacts.Config{
		Symbols: func() []string { return DiscoverSymbols(executor.ThisInstance.CatalogDir) },
	}
	if w.config.SessionFactsGrace != "" {
		grace, err := time.ParseDuration(w.config.SessionFactsGrace)
		if err != nil {
			log.Error("[watchlist] invalid session_facts_grace %q, using default: %v",
				w.config.SessionFactsGrace, err)
		} else {
			cfg.Grace = grace
		}
	}
	leader := !utils.InstanceConfig.Replication.IsReplica()
	if leader {
		cfg.Write = executor.WriteCSM
		cfg.StateDir = sessionfacts.StateDirFor(utils.InstanceConfig.RootDirectory)
	}
	svc, err := sessionfacts.NewService(cfg)
	if err != nil {
		log.Error("[watchlist] session facts disabled: %v", err)
		return
	}
	Facts = svc
	if leader {
		go svc.Run(w.ctx, time.Minute)
		log.Info("[watchlist] session facts: leader, writing daily facts")
	} else {
		log.Info("[watchlist] session facts: replica, reading replicated facts only")
	}
}

// TriggerRanking runs one cycle of watchlist ranking and curation change
// detection. It is called periodically by the ranking loop, and can also
// be called directly for deterministic testing.
func (w *WatchlistWorker) TriggerRanking() {
	w.rankingMu.Lock()
	defer w.rankingMu.Unlock()

	if Manager == nil {
		return
	}

	// Detect curation changes.
	added, removed := DetectCurationChanges(Manager)
	if len(added) > 0 || len(removed) > 0 {
		PushCurationChange(w.timeframe, added, removed, Manager.CuratedCount())
		log.Info("[watchlist] curation change: +%d -%d (total=%d)",
			len(added), len(removed), Manager.CuratedCount())
	}

	// Rank the live window: the session in progress, or the most recently
	// completed one when no session is in progress.
	win, err := session.Resolve(session.Query{}, now())
	if err != nil {
		log.Error("[watchlist] resolve live session: %v", err)
		return
	}
	for _, list := range RunRankings(Manager, win) {
		PushWatchlistUpdate(w.timeframe, list)
	}
}

// newCurator builds a curator from the registered factory with the
// bgworker's curation config. It returns nil, nil when no factory is
// registered.
func newCurator(config map[string]interface{}) (Curator, error) {
	f := GetCuratorFactory()
	if f == nil {
		return nil, nil
	}
	if config == nil {
		config = map[string]interface{}{}
	}
	return f(config)
}

// initialCurationPass evaluates every symbol against the curator using the
// seeded baseline state. This populates the curated set at startup so that
// watchlists and curation-aware routing work immediately, even before any
// live ticks arrive.
func initialCurationPass(mgr *SymbolStateManager) {
	if mgr == nil || mgr.curator == nil {
		return
	}

	states := mgr.AllStates()
	for symbol, state := range states {
		curated := mgr.curator.Evaluate(symbol, state.curationSnapshot())
		state.setCurated(curated)
		mgr.UpdateCuration(symbol, curated)
	}
}

// Shutdown is called by MarketStore during server shutdown.
func (w *WatchlistWorker) Shutdown() {
	w.cancel()
}

// ListWatchlistNames returns the names of the live lists.
// Implements bgworker.WatchlistDataSource.
func (w *WatchlistWorker) ListWatchlistNames() []string {
	res, err := w.Rankings(bgworker.WatchlistQuery{})
	if err != nil {
		return nil
	}
	names := make([]string, len(res.Lists))
	for i, l := range res.Lists {
		names[i] = l.Name
	}
	return names
}

// GetWatchlistRanking returns the live ranking for a named watchlist, or
// nil when it does not exist or is not available in the live session.
// Implements bgworker.WatchlistDataSource.
func (w *WatchlistWorker) GetWatchlistRanking(name string) []bgworker.WatchlistRankingEntry {
	res, err := w.Rankings(bgworker.WatchlistQuery{Names: []string{name}})
	if err != nil || len(res.Lists) == 0 {
		return nil
	}
	return res.Lists[0].Entries
}

// AllWatchlistRankings returns the live rankings.
// Implements bgworker.WatchlistDataSource.
func (w *WatchlistWorker) AllWatchlistRankings() map[string][]bgworker.WatchlistRankingEntry {
	res, err := w.Rankings(bgworker.WatchlistQuery{})
	if err != nil {
		return nil
	}
	out := make(map[string][]bgworker.WatchlistRankingEntry, len(res.Lists))
	for _, l := range res.Lists {
		out[l.Name] = l.Entries
	}
	return out
}

// Rankings answers a live or rewind query at the plugin/host boundary.
// Errors wrap bgworker.ErrWatchlistInvalid or bgworker.ErrWatchlistNotFound
// so the host can classify them. Implements bgworker.WatchlistDataSource.
func (w *WatchlistWorker) Rankings(q bgworker.WatchlistQuery) (bgworker.WatchlistResult, error) {
	if Manager == nil {
		return bgworker.WatchlistResult{}, nil
	}
	res, err := w.RankingsFor(RankingQuery{Names: q.Names, Session: q.Session, AsOf: q.AsOf})
	if err != nil {
		return bgworker.WatchlistResult{}, classify(err)
	}
	out := bgworker.WatchlistResult{Lists: make([]bgworker.WatchlistList, len(res.Lists))}
	for i, l := range res.Lists {
		win := l.Window
		if win.TradingDate.IsZero() {
			win = res.Window // a requested list that produced nothing
		}
		out.Lists[i] = bgworker.WatchlistList{
			Name:        l.Name,
			Basis:       l.Basis.String(),
			Session:     win.Session.String(),
			TradingDate: win.TradingDateString(),
			WindowStart: win.Start,
			WindowEnd:   win.End,
			Complete:    win.Complete,
			Entries:     toBgWorkerEntries(l.Symbols),
		}
	}
	return out, nil
}

// classify wraps err in the bgworker error the host maps to a status.
func classify(err error) error {
	switch {
	case errors.Is(err, ErrUnknownList):
		return fmt.Errorf("%w: %v", bgworker.ErrWatchlistNotFound, err)
	case errors.Is(err, session.ErrNotTradingDay), errors.Is(err, session.ErrSessionNotStarted),
		errors.Is(err, session.ErrInvalidSession), errors.Is(err, session.ErrInvalidAsOf),
		errors.Is(err, session.ErrNotInSession):
		return fmt.Errorf("%w: %v", bgworker.ErrWatchlistInvalid, err)
	default:
		return err
	}
}

// toBgWorkerEntries converts framework RankedSymbol values to the
// equivalent bgworker types at the plugin/host boundary.
func toBgWorkerEntries(ranking []RankedSymbol) []bgworker.WatchlistRankingEntry {
	entries := make([]bgworker.WatchlistRankingEntry, len(ranking))
	for i, rs := range ranking {
		fields := make([]bgworker.WatchlistRankingField, len(rs.Fields))
		for j, f := range rs.Fields {
			fields[j] = bgworker.WatchlistRankingField{Key: f.Key, Value: f.Value}
		}
		entries[i] = bgworker.WatchlistRankingEntry{
			Symbol: rs.Symbol,
			Rank:   rs.Rank,
			Fields: fields,
			Sector: rs.Sector,
		}
	}
	return entries
}

// QueueSessionFactsRebuild queues a rebuild of session facts for symbols
// (every symbol with 1Min data when empty) on every trading day from..to.
// Implements bgworker.SessionFactsRebuilder.
func (w *WatchlistWorker) QueueSessionFactsRebuild(symbols []string, from, to time.Time) (int, error) {
	if Facts == nil {
		return 0, fmt.Errorf("session facts are not running")
	}
	return Facts.QueueRebuild(symbols, from, to)
}
