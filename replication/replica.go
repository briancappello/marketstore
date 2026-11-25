package replication

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

var (
	replicaGaps = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "replication_replica_gaps_total",
		Help: "Gap notices received from the master: sequences it could no longer stream.",
	})
	replicaUnhealedGaps = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "replication_replica_unhealed_gaps_total",
		Help: "Gaps skipped without a heal, because no change ledger heal is configured.",
	})
)

// ReplayApplier applies each transaction group through a Replayer, one at a
// time, and keeps the cursor in memory only.
type ReplayApplier struct {
	replayer Replayer
	mu       sync.Mutex
	cursor   uint64
	ok       bool
}

// Replayer applies one serialized transaction group.
type Replayer interface {
	Replay(transactionGroup []byte) error
}

func NewReplayApplier(r Replayer) *ReplayApplier { return &ReplayApplier{replayer: r} }

func (a *ReplayApplier) Cursor() (uint64, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cursor, a.ok, nil
}

func (a *ReplayApplier) Apply(_ context.Context, seq uint64, tg []byte) error {
	if err := a.replayer.Replay(tg); err != nil {
		return err
	}
	if seq != 0 {
		return a.SetCursor(context.Background(), seq)
	}
	return nil
}

func (a *ReplayApplier) SetCursor(_ context.Context, seq uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cursor, a.ok = seq, true
	return nil
}

// UnhealedGaps is the GapHealer of a replica with no way to re-pull data (no
// master_query_host): it reports the gap and lets the stream continue.
type UnhealedGaps struct{}

func (UnhealedGaps) HealGap(_ context.Context, cursor, firstAvailable uint64) error {
	replicaUnhealedGaps.Inc()
	log.Error("replication: sequences %d-%d were not received and cannot be healed without master_query_host;"+
		" a full resync is required to recover them", cursor+1, firstAvailable-1)
	return nil
}
