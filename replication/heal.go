package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var (
	unrecoverableTickGaps = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "replication_unrecoverable_tick_gaps_total",
		Help: "Variable-length (tick) bucket ranges missed beyond log retention, which cannot be re-pulled.",
	})
	fullResyncRequired = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "replication_full_resync_required",
		Help: "1 once the replica missed changes older than the master's change ledger.",
	})
	healedRanges = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Subsystem: metricsSubsystem,
		Name: "replication_ledger_healed_ranges_total",
		Help: "Fixed bucket ranges re-pulled from the master to heal a gap in the stream.",
	})
)

// RangeHealer re-pulls one fixed bucket range from the master and writes what
// differs. It returns the rows written.
type RangeHealer func(ctx context.Context, tbk string, from, to int64) (int, error)

// LedgerHealer is the GapHealer of a replica that can query the master. It
// asks the master's change ledger what changed after the replica's cursor and
// re-pulls each fixed range written in the gap. Tick ranges cannot be
// re-pulled without duplicating rows, so they are reported instead.
type LedgerHealer struct {
	client pb.ReplicationClient
	heal   RangeHealer
}

func NewLedgerHealer(client pb.ReplicationClient, heal RangeHealer) *LedgerHealer {
	return &LedgerHealer{client: client, heal: heal}
}

func (h *LedgerHealer) HealGap(ctx context.Context, cursor, firstAvailable uint64) error {
	stream, err := h.client.ChangesSince(ctx, &pb.ChangesSinceRequest{SinceSeq: cursor})
	if err != nil {
		return fmt.Errorf("ask the master's change ledger: %w", err)
	}
	var (
		incomplete   bool
		historyStart uint64
		ranges, rows int
		tickRanges   int
	)
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read the master's change ledger: %w", err)
		}
		if resp.GetHistoryIncomplete() {
			incomplete, historyStart = true, resp.GetHistoryStartSeq()
		}
		for _, b := range resp.GetBuckets() {
			for _, r := range b.GetRanges() {
				// A range first written at or after firstAvailable comes down
				// the stream anyway.
				if r.GetFirstSeq() >= firstAvailable {
					continue
				}
				if b.GetVariable() {
					tickRanges++
					unrecoverableTickGaps.Inc()
					log.Error("replication: unrecoverable tick gap: %s epochs %d-%d (sequences %d-%d) were"+
						" missed beyond the master's log retention and cannot be re-pulled without duplicating rows",
						b.GetTbk(), r.GetEpochFrom(), r.GetEpochTo(), r.GetFirstSeq(), r.GetLastSeq())
					continue
				}
				n, err := h.heal(ctx, b.GetTbk(), r.GetEpochFrom(), r.GetEpochTo())
				if err != nil {
					return fmt.Errorf("heal %s epochs %d-%d: %w", b.GetTbk(), r.GetEpochFrom(), r.GetEpochTo(), err)
				}
				ranges++
				rows += n
				healedRanges.Inc()
			}
		}
	}
	if incomplete {
		fullResyncRequired.Set(1)
		log.Error("replication: a full resync is required: this replica's cursor %d is older than the master's"+
			" change ledger (history from %d), so changes before that are unknown. Streaming continues;"+
			" no local data is deleted.", cursor, historyStart)
	}
	log.Info("replication: healed gap %d-%d from the change ledger: %d fixed ranges (%d rows written),"+
		" %d tick ranges unrecoverable", cursor+1, firstAvailable-1, ranges, rows, tickRanges)
	return nil
}

// MasterLastSeq asks a master for the newest sequence in its replication log,
// through its change ledger.
func MasterLastSeq(ctx context.Context, client pb.ReplicationClient) (uint64, error) {
	stream, err := client.ChangesSince(ctx, &pb.ChangesSinceRequest{SinceSeq: math.MaxUint64})
	if err != nil {
		return 0, fmt.Errorf("ask the master for its last sequence: %w", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("ask the master for its last sequence: %w", err)
	}
	return resp.GetAsOfSeq(), nil
}
