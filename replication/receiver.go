package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// ErrNeedsDeepHeal, wrapped into a retryable error, says the replica may have
// missed transaction groups it cannot name, so a deep backfill pass is due.
// That is only the case on a master that does not sequence its stream: a
// sequenced stream resumes exactly, and gaps are healed from the ledger.
var ErrNeedsDeepHeal = errors.New("replica may have missed unsequenced transaction groups")

// GRPCClient is an interface to abstract GRPCReplicationClient.
type GRPCClient interface {
	Connect(ctx context.Context, startSeq uint64) error
	Recv() (*pb.GetWALStreamResponse, error)
}

// Applier applies received transaction groups in sequence order and keeps the
// replica's cursor: the sequence of the last transaction group applied.
type Applier interface {
	// Cursor is the durable cursor, with ok=false when there is none yet.
	Cursor() (seq uint64, ok bool, err error)
	// Apply applies one received transaction group.
	Apply(ctx context.Context, e Received) error
	// SetCursor records that everything up to seq is accounted for, without
	// data: the gap below it was healed some other way.
	SetCursor(ctx context.Context, seq uint64) error
}

// Received is one transaction group from the master's stream.
type Received struct {
	// Seq is its sequence in the master's log; 0 from a master that does not
	// sequence its stream, and such an entry moves no cursor.
	Seq uint64
	// Wallclock is when the master logged it (unix ns); 0 when unknown.
	Wallclock int64
	TG        []byte
}

// GapHealer fills in what a replica missed when the master can no longer
// stream it: the sequences after cursor and before firstAvailable.
type GapHealer interface {
	HealGap(ctx context.Context, cursor, firstAvailable uint64) error
}

type Receiver struct {
	gRPCClient GRPCClient
	applier    Applier
	healer     GapHealer

	// next is the sequence to request on (re)connect: one past the last entry
	// handed to the applier. 0 until the first sequenced entry, then the
	// durable cursor is only consulted at startup.
	next           uint64
	legacyLogged   sync.Once
	legacyStreamed bool
}

func NewReceiver(grpcClient GRPCClient, applier Applier, healer GapHealer) *Receiver {
	return &Receiver{gRPCClient: grpcClient, applier: applier, healer: healer}
}

// Run streams from the master until the stream fails, applying entries in
// order. Retryable failures are wrapped in ErrRetryable, plus ErrMadeProgress
// when this attempt applied something, so the retry backoff resets.
func (r *Receiver) Run(ctx context.Context) error {
	start := r.next
	if start == 0 {
		cursor, ok, err := r.applier.Cursor()
		if err != nil {
			return fmt.Errorf("read replication cursor: %w", err)
		}
		if ok {
			start = cursor + 1
		}
	}
	if err := r.gRPCClient.Connect(ctx, start); err != nil {
		return fmt.Errorf("err: %s: %w", err.Error(), ErrRetryable)
	}
	log.Info("connected to the master instance, requesting sequence %d (0: the tail)", start)

	progress := false
	retry := func(cause error) error {
		errs := []error{cause, ErrRetryable}
		if progress {
			errs = append(errs, ErrMadeProgress)
		}
		if r.legacyStreamed {
			errs = append(errs, ErrNeedsDeepHeal)
		}
		return errors.Join(errs...)
	}

	for {
		resp, err := r.gRPCClient.Recv()
		if errors.Is(err, io.EOF) {
			return retry(errors.New("received EOF from master server"))
		}
		if err != nil {
			log.Error("an error occurred while receiving a replication message"+
				" from master instance. Will be retried soon...:%s", err.Error())
			return retry(err)
		}

		if gap := resp.GetGap(); gap != nil {
			return r.handleGap(ctx, gap)
		}

		tg := resp.GetTransactionGroup()
		seq := resp.GetSeq()
		if seq == 0 {
			r.legacyLogged.Do(func() {
				log.Warn("the replication master does not sequence its stream: resume and gap healing" +
					" are unavailable until it is upgraded")
			})
			r.legacyStreamed = true
			if err := r.applier.Apply(ctx, Received{TG: tg}); err != nil {
				return applyError(err)
			}
			progress = true
			continue
		}
		if r.next != 0 && seq != r.next {
			// Never apply out of order: reconnect from where we are.
			log.Warn("replication stream jumped from %d to %d; reconnecting at %d", r.next-1, seq, r.next)
			return retry(fmt.Errorf("sequence %d received, %d expected", seq, r.next))
		}
		if err := r.applier.Apply(ctx, Received{Seq: seq, Wallclock: resp.GetWallclock(), TG: tg}); err != nil {
			return applyError(err)
		}
		r.next = seq + 1
		progress = true
	}
}

func applyError(err error) error {
	// this might be a bug in the replay logic. We won't retry it.
	return fmt.Errorf("an error occurred while replaying. "+
		"There will be data inconsistency between master and replica:%w", err)
}

// handleGap heals the sequences the master can no longer stream, moves the
// cursor to just before the first one it can, and ends this attempt so the
// next one reconnects there.
func (r *Receiver) handleGap(ctx context.Context, gap *pb.Gap) error {
	cursor := gap.GetRequested() - 1
	first := gap.GetFirstAvailable()
	replicaGaps.Inc()
	log.Warn("replication master cannot serve sequences %d-%d (%s); healing from the change ledger",
		cursor+1, first-1, gap.GetReason())
	if err := r.healer.HealGap(ctx, cursor, first); err != nil {
		return errors.Join(fmt.Errorf("heal replication gap %d-%d: %w", cursor+1, first-1, err), ErrRetryable)
	}
	if err := r.applier.SetCursor(ctx, first-1); err != nil {
		return fmt.Errorf("move replication cursor past healed gap: %w", err)
	}
	r.next = first
	return errors.Join(fmt.Errorf("healed gap %d-%d", cursor+1, first-1), ErrRetryable, ErrMadeProgress)
}
