package start

import (
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
