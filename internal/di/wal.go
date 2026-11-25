package di

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/executor/wal"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func (c *Container) GetInitWALFile() *executor.WALFileType {
	if !c.mktsConfig.InitWALCache {
		return nil
	}
	if c.wal != nil {
		return c.wal
	}

	// initialize TransactionPipe
	txnPipe := executor.NewTransactionPipe()

	walWG := &sync.WaitGroup{}

	// initialize WAL File
	walfile, err := executor.NewWALFile(c.GetAbsRootDir(), c.GetInitInstanceID(), &executor.NopReplicationSender{},
		c.mktsConfig.WALBypass, walWG, c.GetStartTriggerPluginDispatcher(), txnPipe,
	)
	if err != nil {
		log.Error("Unable to create WAL. err=" + err.Error())
		panic(fmt.Sprintf("unable to create WAL: %v", err))
	}

	if ml := c.GetMasterLog(); ml != nil && !walfile.WALBypass {
		walfile.SetReplicationLog(ml)
	}

	if !walfile.WALBypass {
		// ignoreFile := filepath.Base(ThisInstance.WALFile.FilePtr.Name())
		ignoreFile := walfile.FilePtr.Name()
		myInstanceID := walfile.OwningInstanceID

		finder := wal.NewFinder(os.ReadDir)
		walFileAbsPaths, err := finder.Find(filepath.Clean(c.GetAbsRootDir()))
		if err != nil {
			walFileAbsPaths = []string{}
			log.Error("failed to find wal files under %s: %w", filepath.Clean(c.GetAbsRootDir()), err)
		}

		cleaner := executor.NewWALCleaner(ignoreFile, myInstanceID)
		if ml := c.GetMasterLog(); ml != nil {
			cleaner.SetReplicationLog(ml)
		}
		err = cleaner.CleanupOldWALFiles(walFileAbsPaths)
		if err != nil {
			log.Error("Unable to startup Cache and WAL:" + err.Error())
			panic(fmt.Sprintf("unable to startup Cache and WAL:%v", err))
		}
	}
	if c.mktsConfig.BackgroundSync {
		// Startup the WAL and Primary cache flushers
		const (
			defaultWalSyncInterval            = 500 * time.Millisecond
			defaultPrimaryDiskRefreshInterval = 5 * time.Minute
		)
		// Every tick costs an fsync on the WAL, so this knob trades crash-loss
		// window against CPU and IOPS directly. Zero means the config never set
		// it (e.g. a MktsConfig built by hand in a test), so fall back.
		walSyncInterval := c.mktsConfig.WALSyncInterval
		if walSyncInterval <= 0 {
			walSyncInterval = defaultWalSyncInterval
		}
		go walfile.SyncWAL(walSyncInterval, defaultPrimaryDiskRefreshInterval, c.mktsConfig.WALRotateInterval)
		walfile.IncrementWaitGroup()
	}

	c.wal = walfile
	return c.wal
}

const (
	replicationLogSyncInterval   = 100 * time.Millisecond
	replicationLogRetainInterval = 10 * time.Second
	defaultReplicationLogMaxSize = 20 << 30
	defaultReplicationLogMaxAge  = 48 * time.Hour

	replicationLedgerSnapshotInterval = 10 * time.Minute
	defaultReplicationLedgerMaxAge    = 30 * 24 * time.Hour
)

func (c *Container) ledgerSnapshotPath() string {
	return filepath.Join(c.GetAbsRootDir(), "replication_ledger.snap")
}

// GetMasterLog is the master's replication log plus change ledger, as handed
// to the WAL, or nil when this instance is not a replication master. The
// ledger is rebuilt (snapshot plus the log after it) before WAL replay, so
// groups that replay logs are recorded in it too.
func (c *Container) GetMasterLog() *replication.MasterLog {
	rl := c.GetReplicationLog()
	if rl == nil {
		return nil
	}
	if c.masterLog != nil {
		return c.masterLog
	}
	lg := replication.RebuildLedger(c.ledgerSnapshotPath(), rl, 0)
	log.Info("replication ledger: history from sequence %d, up to %d", lg.HistoryStart(), lg.LastSeq())
	go replication.RunLedger(context.Background(), lg, c.ledgerSnapshotPath(),
		replicationLedgerSnapshotInterval, defaultReplicationLedgerMaxAge)
	c.replicationLedger = lg
	c.masterLog = replication.NewMasterLog(rl, lg)
	return c.masterLog
}

// GetReplicationLog opens the master's replication log, or returns nil when
// this instance is not a replication master. The log is opened before WAL
// replay so replayed transaction groups can be logged too.
func (c *Container) GetReplicationLog() *replog.Log {
	if !c.mktsConfig.Replication.Enabled {
		return nil
	}
	if c.replicationLog != nil {
		return c.replicationLog
	}
	l, err := replog.Open(filepath.Join(c.GetAbsRootDir(), "replication_log"), replog.Options{
		MaxBytes: defaultReplicationLogMaxSize,
		MaxAge:   defaultReplicationLogMaxAge,
	})
	if err != nil {
		panic(fmt.Sprintf("unable to open the replication log: %v", err))
	}
	log.Info("replication log: sequences %d-%d, %d bytes", l.FirstSeq(), l.LastSeq(), l.Bytes())
	if err := replication.RegisterLogMetrics(prometheus.DefaultRegisterer, l, nil); err != nil {
		log.Error("replication log metrics: %v", err)
	}
	go l.Run(context.Background(), replicationLogSyncInterval, replicationLogRetainInterval)
	c.replicationLog = l
	return l
}

// CloseReplicationLog makes the replication log durable and closes it. Call it
// after the final WAL flush.
func (c *Container) CloseReplicationLog() {
	if c.replicationLog == nil {
		return
	}
	if c.replicationLedger != nil {
		replication.SaveLedger(c.replicationLedger, c.ledgerSnapshotPath(), defaultReplicationLedgerMaxAge)
	}
	if err := c.replicationLog.Close(); err != nil {
		log.Error("closing the replication log: %v", err)
	}
}
