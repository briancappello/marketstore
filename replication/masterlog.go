package replication

import (
	"github.com/alpacahq/marketstore/v4/replication/replog"
)

// MasterLog is the master's executor.ReplicationLog: it appends every
// committed transaction group to the replication log.
type MasterLog struct {
	log *replog.Log
}

func NewMasterLog(l *replog.Log) *MasterLog {
	return &MasterLog{log: l}
}

// LogTransactionGroup appends one transaction group. A failed append leaves
// the log broken (replog logs it and keeps the sequence), and the write path
// carries on: replicas are told about the hole once the log recovers.
func (m *MasterLog) LogTransactionGroup(tgID int64, tgSerialized []byte) {
	_, _ = m.log.Append(tgID, tgSerialized)
}

// Log is the underlying replication log.
func (m *MasterLog) Log() *replog.Log { return m.log }
