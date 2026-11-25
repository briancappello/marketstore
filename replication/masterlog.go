package replication

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/alpacahq/marketstore/v4/replication/ledger"
	"github.com/alpacahq/marketstore/v4/replication/replog"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// MasterLog is the master's executor.ReplicationLog: it appends every
// committed transaction group to the replication log and records what it
// changed in the change ledger.
type MasterLog struct {
	log    *replog.Log
	ledger *ledger.Ledger // may be nil
}

func NewMasterLog(l *replog.Log, lg *ledger.Ledger) *MasterLog {
	return &MasterLog{log: l, ledger: lg}
}

// LogTransactionGroup appends one transaction group. A failed append leaves
// the log broken (replog logs it and keeps the sequence), and the write path
// carries on: replicas are told about the hole once the log recovers. The
// ledger records the group either way, so a replica can still heal the hole.
func (m *MasterLog) LogTransactionGroup(tgID int64, tgSerialized []byte) {
	seq, _ := m.log.Append(tgID, tgSerialized)
	m.record(seq, tgSerialized)
}

// LogReplayedTransactionGroup appends a transaction group re-applied by WAL
// replay unless the log already holds it. Transaction-group IDs grow
// monotonically (a restarted master seeds them from the clock), so any group
// newer than the log's last entry never reached the log before the crash.
func (m *MasterLog) LogReplayedTransactionGroup(tgID int64, tgSerialized []byte) {
	if tgID <= m.log.LastTGID() {
		return
	}
	seq, _ := m.log.Append(tgID, tgSerialized)
	m.record(seq, tgSerialized)
}

func (m *MasterLog) record(seq uint64, tgSerialized []byte) {
	if m.ledger != nil && seq != 0 {
		m.ledger.Record(seq, time.Now().UnixNano(), tgSerialized)
	}
}

// Log is the underlying replication log.
func (m *MasterLog) Log() *replog.Log { return m.log }

// RebuildLedger loads the ledger snapshot at path and records the log entries
// logged after it. Whatever neither covers (entries deleted by retention, or
// never logged) is marked unknown, so ChangesSince reports it incomplete
// rather than silently leaving it out.
func RebuildLedger(path string, l *replog.Log, maxRanges int) *ledger.Ledger {
	lg, err := ledger.LoadSnapshot(path, maxRanges)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Error("replication ledger: %v; rebuilding from the log alone", err)
		}
		lg = ledger.New(maxRanges)
	}
	from := lg.LastSeq() + 1
	if first := l.FirstSeq(); first > from {
		// Retention deleted entries the snapshot does not cover.
		lg.Forget(first - 1)
		from = first
	}
	last := l.LastSeq()
	for from <= last {
		es, err := l.Read(from, 1024, 64<<20)
		var gap *replog.GapError
		switch {
		case errors.As(err, &gap):
			lg.Forget(gap.Next - 1) // a hole: those groups were never logged
			from = gap.Next
			continue
		case err != nil:
			log.Error("replication ledger: read log at %d: %v; history before it is unknown", from, err)
			lg.Forget(last)
			return lg
		case len(es) == 0:
			// The log broke at from and has not recovered: what came after
			// is not on disk.
			lg.Forget(last)
			return lg
		}
		for _, e := range es {
			lg.Record(e.Seq, e.Wallclock, e.Data)
		}
		from = es[len(es)-1].Seq + 1
	}
	return lg
}

// RunLedger expires ranges older than maxAge and saves a snapshot to path
// every interval, until ctx is done.
func RunLedger(ctx context.Context, lg *ledger.Ledger, path string, interval, maxAge time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			SaveLedger(lg, path, maxAge)
		}
	}
}

// SaveLedger expires ranges older than maxAge and writes a snapshot.
func SaveLedger(lg *ledger.Ledger, path string, maxAge time.Duration) {
	if maxAge > 0 {
		lg.Expire(time.Now().Add(-maxAge).UnixNano())
	}
	if err := lg.WriteSnapshot(path); err != nil {
		log.Error("replication ledger: %v", err)
	}
}
