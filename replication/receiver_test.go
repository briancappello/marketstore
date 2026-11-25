package replication_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/replog"
)

// fakeApplier records what it applied. Its cursor stands in for the durable
// one, so a new Receiver over the same applier models a restart.
type fakeApplier struct {
	mu      sync.Mutex
	cursor  uint64
	ok      bool
	applied []uint64 // sequences, 0 for unsequenced
	data    [][]byte
	delay   time.Duration
	setTo   []uint64
}

func (a *fakeApplier) Cursor() (uint64, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cursor, a.ok, nil
}

func (a *fakeApplier) Apply(_ context.Context, seq uint64, tg []byte) error {
	if a.delay > 0 {
		time.Sleep(a.delay)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.applied = append(a.applied, seq)
	a.data = append(a.data, tg)
	if seq != 0 {
		a.cursor, a.ok = seq, true
	}
	return nil
}

func (a *fakeApplier) SetCursor(_ context.Context, seq uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cursor, a.ok = seq, true
	a.setTo = append(a.setTo, seq)
	return nil
}

func (a *fakeApplier) seqs() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uint64(nil), a.applied...)
}

func (a *fakeApplier) waitFor(t *testing.T, n int) []uint64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if s := a.seqs(); len(s) >= n {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("applied %d entries, want %d", len(a.seqs()), n)
	return nil
}

type healCall struct{ cursor, first uint64 }

type fakeHealer struct {
	mu    sync.Mutex
	calls []healCall
}

func (h *fakeHealer) HealGap(_ context.Context, cursor, first uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = append(h.calls, healCall{cursor, first})
	return nil
}

// runReceiver runs a receiver under a Retryer, like the replica does, until
// the returned cancel is called.
func runReceiver(h *streamHarness, a replication.Applier, healer replication.GapHealer,
) (cancel func(), done chan error) {
	ctx, cancelFn := context.WithCancel(context.Background())
	recv := replication.NewReceiver(replication.NewGRPCReplicationClient(h.client), a, healer)
	done = make(chan error, 1)
	go func() {
		done <- replication.NewRetryer(recv.Run, time.Millisecond, 1, nil).Run(ctx)
	}()
	return cancelFn, done
}

func TestReceiverResumesAfterRestartFromItsCursor(t *testing.T) {
	h := newStreamHarness(t, replog.Options{})
	h.appendN(t, 1, 5200, 16)
	a := &fakeApplier{cursor: 5000, ok: true}

	cancel, _ := runReceiver(h, a, &fakeHealer{})
	defer cancel()
	h.appendN(t, 5201, 10, 16) // live entries after the catch-up
	requireConsecutive(t, a.waitFor(t, 210), 5001)
}

func TestReceiverWithoutCursorStartsAtTheTail(t *testing.T) {
	h := newStreamHarness(t, replog.Options{})
	h.appendN(t, 1, 50, 16)
	a := &fakeApplier{}
	cancel, _ := runReceiver(h, a, &fakeHealer{})
	defer cancel()

	// Keep appending until the stream is up; from then on nothing is skipped.
	next := 51
	for len(a.seqs()) == 0 {
		h.appendN(t, next, 1, 16)
		next++
		time.Sleep(2 * time.Millisecond)
	}
	h.appendN(t, next, 20, 16)
	got := a.waitFor(t, int(uint64(next+19)-a.seqs()[0]+1))
	require.Greater(t, got[0], uint64(50))
	requireConsecutive(t, got, got[0])
}

// scriptedClient plays back responses, one script per connection, and records
// the sequence each connection asked for.
type scriptedClient struct {
	mu        sync.Mutex
	scripts   [][]*pb.GetWALStreamResponse
	requested []uint64
	current   []*pb.GetWALStreamResponse
}

func (c *scriptedClient) Connect(_ context.Context, startSeq uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requested = append(c.requested, startSeq)
	if len(c.scripts) == 0 {
		return fmt.Errorf("no more connections: %w", replication.ErrRetryable)
	}
	c.current, c.scripts = c.scripts[0], c.scripts[1:]
	return nil
}

func (c *scriptedClient) Recv() (*pb.GetWALStreamResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.current) == 0 {
		return nil, io.EOF
	}
	r := c.current[0]
	c.current = c.current[1:]
	return r, nil
}

func tgResp(seq uint64) *pb.GetWALStreamResponse {
	return &pb.GetWALStreamResponse{Seq: seq, Body: &pb.GetWALStreamResponse_TransactionGroup{
		TransactionGroup: []byte(fmt.Sprintf("tg-%d", seq)),
	}}
}

// runScripted runs one receiver attempt per script, as the Retryer would.
func runScripted(t *testing.T, c *scriptedClient, a replication.Applier, healer replication.GapHealer) []error {
	t.Helper()
	recv := replication.NewReceiver(c, a, healer)
	var errs []error
	for {
		c.mu.Lock()
		left := len(c.scripts)
		c.mu.Unlock()
		if left == 0 {
			return errs
		}
		err := recv.Run(context.Background())
		require.ErrorIs(t, err, replication.ErrRetryable)
		errs = append(errs, err)
	}
}

func TestReceiverReconnectsOnASequenceJumpWithoutApplyingIt(t *testing.T) {
	c := &scriptedClient{scripts: [][]*pb.GetWALStreamResponse{
		{tgResp(101), tgResp(103)},              // 102 went missing
		{tgResp(102), tgResp(103), tgResp(104)}, // reconnected at 102
	}}
	a := &fakeApplier{cursor: 100, ok: true}
	errs := runScripted(t, c, a, &fakeHealer{})
	require.Equal(t, []uint64{101, 102, 103, 104}, a.seqs())
	require.Equal(t, []uint64{101, 102}, c.requested)
	require.ErrorIs(t, errs[0], replication.ErrMadeProgress, "101 was applied")
	require.NotErrorIs(t, errs[0], replication.ErrNeedsDeepHeal)
}

func TestReceiverResumesAfterDisconnectAtTheNextSequence(t *testing.T) {
	c := &scriptedClient{scripts: [][]*pb.GetWALStreamResponse{
		{tgResp(6999), tgResp(7000)}, // then the connection drops (EOF)
		{tgResp(7001)},
	}}
	a := &fakeApplier{cursor: 6998, ok: true}
	runScripted(t, c, a, &fakeHealer{})
	require.Equal(t, []uint64{6999, 7000, 7001}, a.seqs())
	require.Equal(t, []uint64{6999, 7001}, c.requested)
}

// A cursor older than the master's log: heal through the ledger, move the
// cursor past the gap, and stream from where the master resumes.
func TestReceiverHealsAGapPastRetention(t *testing.T) {
	h := newStreamHarness(t, replog.Options{SegmentBytes: 400, MaxBytes: 800})
	h.appendN(t, 1, 200, 16)
	require.NoError(t, h.log.Sync())
	h.log.EnforceRetention()
	first := h.log.FirstSeq()
	require.Greater(t, first, uint64(101))

	a := &fakeApplier{cursor: 100, ok: true}
	healer := &fakeHealer{}
	cancel, _ := runReceiver(h, a, healer)
	defer cancel()

	requireConsecutive(t, a.waitFor(t, int(200-first+1)), first)
	healer.mu.Lock()
	require.Equal(t, []healCall{{cursor: 100, first: first}}, healer.calls)
	healer.mu.Unlock()
	require.Equal(t, []uint64{first - 1}, a.setTo)
}

// A master built before sequencing sends seq 0: apply as before, and ask for a
// deep heal on reconnect, since unsequenced entries can be lost unnoticed.
func TestReceiverOnALegacyMaster(t *testing.T) {
	legacy := func(b string) *pb.GetWALStreamResponse {
		return &pb.GetWALStreamResponse{Body: &pb.GetWALStreamResponse_TransactionGroup{TransactionGroup: []byte(b)}}
	}
	c := &scriptedClient{scripts: [][]*pb.GetWALStreamResponse{{legacy("a"), legacy("b")}}}
	a := &fakeApplier{}
	errs := runScripted(t, c, a, &fakeHealer{})
	require.Equal(t, []uint64{0, 0}, a.seqs())
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, a.data)
	require.False(t, a.ok, "unsequenced entries move no cursor")
	require.ErrorIs(t, errs[0], replication.ErrNeedsDeepHeal)
}

// Ticks written in a burst that the replica drains slowly all arrive, once.
func TestReceiverSlowReplicaGetsEveryEntryOnce(t *testing.T) {
	h := newStreamHarness(t, replog.Options{HotTailBytes: 16 << 10, SegmentBytes: 1 << 20})
	a := &fakeApplier{cursor: 0, ok: true, delay: 20 * time.Microsecond}
	cancel, _ := runReceiver(h, a, &fakeHealer{})
	defer cancel()
	h.appendN(t, 1, 3000, 64)
	got := a.waitFor(t, 3000)
	requireConsecutive(t, got, 1)
	require.Len(t, got, 3000)
	require.Greater(t, h.log.DiskReads(), int64(0), "the replica fell behind the memory tail")
}

func TestReceiverStopsOnApplyError(t *testing.T) {
	c := &scriptedClient{scripts: [][]*pb.GetWALStreamResponse{{tgResp(1)}}}
	recv := replication.NewReceiver(c, failingApplier{}, &fakeHealer{})
	err := recv.Run(context.Background())
	require.Error(t, err)
	require.False(t, errors.Is(err, replication.ErrRetryable), "a replay bug is not retried")
}

type failingApplier struct{}

func (failingApplier) Cursor() (uint64, bool, error)               { return 0, true, nil }
func (failingApplier) Apply(context.Context, uint64, []byte) error { return errors.New("boom") }
func (failingApplier) SetCursor(context.Context, uint64) error     { return nil }
