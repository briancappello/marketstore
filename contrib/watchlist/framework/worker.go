package framework

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
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

	// Create the Curator.
	factory := GetCuratorFactory()
	if factory != nil {
		curator, err := factory(nil) // TODO: pass curation config from trigger
		if err != nil {
			log.Error("[watchlist] failed to create curator: %v", err)
		} else {
			Manager.SetCurator(curator)
			log.Info("[watchlist] curator registered")
		}
	} else {
		log.Warn("[watchlist] no curator registered, all symbols will be curated")
	}

	// Create WatchlistStrategy instances from registered factories.
	for name, factory := range GetAllWatchlistFactories() {
		strategy, err := factory(nil) // TODO: pass per-watchlist config
		if err != nil {
			log.Error("[watchlist] failed to create watchlist %q: %v", name, err)
			continue
		}
		Manager.AddStrategy(strategy)
		log.Info("[watchlist] watchlist strategy registered: %s", strategy.Name())
	}

	// Compute baselines. This also seeds the running state (LastPrice,
	// HighOfDay, DollarVolumeRate, etc.) from the most recent daily bar so
	// that curation and watchlists produce meaningful results immediately,
	// even when the market is closed.
	ComputeBaselines(Manager, w.config.BaselineLookbackDays, w.config.MedianWindow)

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

	// Start the ranking loop.
	interval := time.Duration(w.config.RankingIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Info("[watchlist] ranking loop started (interval=%v)", interval)

	for {
		select {
		case <-ticker.C:
			w.TriggerRanking()
		case <-w.ctx.Done():
			log.Info("[watchlist] worker shutting down")
			return
		}
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

	// Run all watchlist rankings.
	results := RunRankings(Manager)
	for name, ranking := range results {
		PushWatchlistUpdate(w.timeframe, name, ranking)
	}
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
		curated := mgr.curator.Evaluate(symbol, state)
		state.IsCurated = curated
		mgr.UpdateCuration(symbol, curated)
	}
}

// Shutdown is called by MarketStore during server shutdown.
func (w *WatchlistWorker) Shutdown() {
	w.cancel()
}

// ListWatchlistNames returns the names of all available watchlists.
// Implements bgworker.WatchlistDataSource.
func (w *WatchlistWorker) ListWatchlistNames() []string {
	if Manager == nil {
		return nil
	}
	return Manager.ListWatchlistNames()
}

// GetWatchlistRanking returns the current ranking for a named watchlist.
// Implements bgworker.WatchlistDataSource.
func (w *WatchlistWorker) GetWatchlistRanking(name string) []bgworker.WatchlistRankingEntry {
	if Manager == nil {
		return nil
	}
	ranking := Manager.GetWatchlistRanking(name)
	entries := make([]bgworker.WatchlistRankingEntry, len(ranking))
	for i, rs := range ranking {
		entries[i] = bgworker.WatchlistRankingEntry{
			Symbol: rs.Symbol,
			Rank:   rs.Rank,
			Fields: rs.Fields,
		}
	}
	return entries
}

// AllWatchlistRankings returns all current watchlist rankings.
// Implements bgworker.WatchlistDataSource.
func (w *WatchlistWorker) AllWatchlistRankings() map[string][]bgworker.WatchlistRankingEntry {
	if Manager == nil {
		return nil
	}
	all := Manager.AllWatchlistRankings()
	result := make(map[string][]bgworker.WatchlistRankingEntry, len(all))
	for name, ranking := range all {
		entries := make([]bgworker.WatchlistRankingEntry, len(ranking))
		for i, rs := range ranking {
			entries[i] = bgworker.WatchlistRankingEntry{
				Symbol: rs.Symbol,
				Rank:   rs.Rank,
				Fields: rs.Fields,
			}
		}
		result[name] = entries
	}
	return result
}
