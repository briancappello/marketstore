package start

import (
	"errors"
	"fmt"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/plugins"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// RunBgWorkers initializes and starts all configured background worker plugins.
// It returns the list of successfully created workers so the caller can shut
// them down during server shutdown.
func RunBgWorkers(bgWorkers []*utils.BgWorkerSetting) []bgworker.BgWorker {
	log.Info("InitializeBgWorkers")
	var workers []bgworker.BgWorker
	for _, bgWorkerSetting := range bgWorkers {
		// bgWorkerSetting may contain sensitive data such as a password or token.
		log.Debug("bgWorkerSetting = %v", bgWorkerSetting)
		bgWorker := NewBgWorker(bgWorkerSetting)
		if bgWorker != nil {
			log.Info("Start running BgWorker %s...", bgWorkerSetting.Name)
			workers = append(workers, bgWorker)

			// If the worker implements WatchlistDataSource, register it
			// as the frontend's WatchlistProvider so RPC calls can read
			// watchlist ranking data.
			if wds, ok := bgWorker.(bgworker.WatchlistDataSource); ok {
				frontend.RegisterWatchlistProvider(&watchlistAdapter{src: wds})
				log.Info("Registered watchlist data source from BgWorker %s", bgWorkerSetting.Name)
			}

			// If the worker implements SubscriptionController, register it as
			// the frontend's controller so Subscribe RPC calls can drive live
			// tick subscriptions at runtime.
			if sc, ok := bgWorker.(bgworker.SubscriptionController); ok {
				frontend.RegisterSubscriptionController(&subscriptionAdapter{src: sc})
				log.Info("Registered subscription controller from BgWorker %s", bgWorkerSetting.Name)
			}

			// If the worker implements SessionFactsRebuilder, register it so
			// the RebuildSessionFacts RPC can queue rebuilds. The interfaces
			// have the same method set, so no adapter is needed.
			if rb, ok := bgWorker.(bgworker.SessionFactsRebuilder); ok {
				frontend.RegisterSessionFactsRebuilder(rb)
				log.Info("Registered session facts rebuilder from BgWorker %s", bgWorkerSetting.Name)
			}

			go bgWorker.Run()
		}
	}
	log.Info("InitializeBgWorkers Done")
	return workers
}

// ShutdownBgWorkers calls Shutdown on each background worker, giving each
// a chance to close connections and release resources.
func ShutdownBgWorkers(workers []bgworker.BgWorker) {
	for _, w := range workers {
		w.Shutdown()
	}
}

// watchlistAdapter bridges bgworker.WatchlistDataSource (shared with the
// plugin) to frontend.WatchlistProvider (used by the RPC layer). This runs
// in the host process, so it correctly accesses the host's frontend state.
type watchlistAdapter struct {
	src bgworker.WatchlistDataSource
}

func (a *watchlistAdapter) ListNames() []string {
	return a.src.ListWatchlistNames()
}

func (a *watchlistAdapter) GetRanking(name string) []frontend.WatchlistRankingEntry {
	ranking := a.src.GetWatchlistRanking(name)
	return convertBgRanking(ranking)
}

func (a *watchlistAdapter) AllRankings() map[string][]frontend.WatchlistRankingEntry {
	all := a.src.AllWatchlistRankings()
	result := make(map[string][]frontend.WatchlistRankingEntry, len(all))
	for name, ranking := range all {
		result[name] = convertBgRanking(ranking)
	}
	return result
}

func (a *watchlistAdapter) Rankings(q frontend.WatchlistQuery) (frontend.WatchlistResult, error) {
	res, err := a.src.Rankings(bgworker.WatchlistQuery{Names: q.Names, Session: q.Session, AsOf: q.AsOf})
	if err != nil {
		// Translate the plugin's error class into the frontend's, keeping
		// the message.
		switch {
		case errors.Is(err, bgworker.ErrWatchlistNotFound):
			return frontend.WatchlistResult{}, fmt.Errorf("%w: %v", frontend.ErrWatchlistNotFound, err)
		case errors.Is(err, bgworker.ErrWatchlistInvalid):
			return frontend.WatchlistResult{}, fmt.Errorf("%w: %v", frontend.ErrWatchlistInvalid, err)
		}
		return frontend.WatchlistResult{}, err
	}
	out := frontend.WatchlistResult{Lists: make([]frontend.WatchlistList, len(res.Lists))}
	for i, l := range res.Lists {
		out.Lists[i] = frontend.WatchlistList{
			Name: l.Name, Basis: l.Basis, Session: l.Session, TradingDate: l.TradingDate,
			WindowStart: l.WindowStart, WindowEnd: l.WindowEnd, Complete: l.Complete,
			Entries: convertBgRanking(l.Entries),
		}
	}
	return out, nil
}

// convertBgRanking translates the bgworker's typed ranking entries to the
// frontend's parallel type. Both shapes are identical by construction; this
// indirection exists only because the two packages cannot import each
// other (the frontend cannot depend on a plugin interface package, and
// the plugin package cannot depend on the frontend).
func convertBgRanking(ranking []bgworker.WatchlistRankingEntry) []frontend.WatchlistRankingEntry {
	entries := make([]frontend.WatchlistRankingEntry, len(ranking))
	for i, r := range ranking {
		fields := make([]frontend.WatchlistRankingField, len(r.Fields))
		for j, f := range r.Fields {
			fields[j] = frontend.WatchlistRankingField{Key: f.Key, Value: f.Value}
		}
		entries[i] = frontend.WatchlistRankingEntry{
			Symbol: r.Symbol,
			Rank:   r.Rank,
			Fields: fields,
			Sector: r.Sector,
		}
	}
	return entries
}

// subscriptionAdapter bridges bgworker.SubscriptionController (shared with the
// plugin) to frontend.SubscriptionController (used by the RPC layer). This runs
// in the host process. The two interfaces are identical by construction; the
// indirection exists only because the frontend and plugin packages cannot
// import each other (same rationale as watchlistAdapter).
type subscriptionAdapter struct {
	src bgworker.SubscriptionController
}

func (a *subscriptionAdapter) Subscribe(symbol string, dataTypes []string) error {
	return a.src.Subscribe(symbol, dataTypes)
}

func (a *subscriptionAdapter) Unsubscribe(symbol string, dataTypes []string) error {
	return a.src.Unsubscribe(symbol, dataTypes)
}

func (a *subscriptionAdapter) ActiveSubscriptions() map[string][]string {
	return a.src.ActiveSubscriptions()
}

func NewBgWorker(s *utils.BgWorkerSetting) bgworker.BgWorker {
	loader, err := plugins.NewSymbolLoader(s.Module)
	if err != nil {
		log.Error("Unable to open plugin for bgworker in %s: %v", s.Module, err)
		return nil
	}
	bgWorker, err := bgworker.Load(loader, s.Config)
	if err != nil {
		log.Error("Failed to create bgworker: %v", err)
	}
	return bgWorker
}
