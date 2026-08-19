package replication_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/replication/mock"
)

func TestSendReplicationMessageIsNonBlockingAndRaceFree(t *testing.T) {
	rs := replication.NewGRPCReplicationServer()

	// A registered-but-undrained replica (simulates a stalled replica).
	slow := rs.Register("slow")
	for i := 0; i < cap(slow); i++ {
		slow <- []byte("fill")
	}

	// Concurrent connect/disconnect churn through the real (guarded) code path
	// to exercise the map guard under -race.
	churned := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			addr := fmt.Sprintf("churn-%d", i)
			rs.Register(addr)
			rs.Unregister(addr)
		}
		close(churned)
	}()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			rs.SendReplicationMessage([]byte("x")) // must not block even though `slow` is full
		}
		close(done)
	}()

	for _, ch := range []chan struct{}{done, churned} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("SendReplicationMessage blocked on a full replica channel")
		}
	}
}

// listen messages -> wait 500ms -> put a test message to a channel -> wait 100ms -> the message should be sent.
func TestGRPCReplicationServer_GetWALStream_success(t *testing.T) {
	// --- given ---
	t.Parallel()
	replServer := replication.NewGRPCReplicationServer()
	testTGMessage := []byte{1, 2, 3}

	stream := &mock.WALStreamServer{
		SendFunc: func(resp *proto.GetWALStreamResponse) error {
			// test message should be sent
			if !cmp.Equal(resp.TransactionGroup, testTGMessage) {
				t.Errorf("got: %v, want: %v", resp.TransactionGroup, testTGMessage)
			}
			return nil
		},
	}

	// --- when ---
	// start to listen
	go func() {
		_ = replServer.GetWALStream(nil, stream)
	}()
	time.Sleep(500 * time.Millisecond)

	replServer.SendReplicationMessage(testTGMessage)
	time.Sleep(100 * time.Millisecond)

	// --- then ---
	// assertion is done in SendFunc
}

func TestGRPCReplicationServer_GetWALStream_error(t *testing.T) {
	// --- given ---
	t.Parallel()
	replServer := replication.NewGRPCReplicationServer()
	testTGMessage := []byte{1, 2, 3}

	stream := &mock.ErrorWALStreamServer{}

	// --- when ---

	// start to listen, but Send function should return error and GetWALStream should return nil
	go func() {
		err := replServer.GetWALStream(nil, stream)
		if err != nil {
			t.Errorf("GetWALStream should return nil when Send failed")
		}
	}()
	time.Sleep(500 * time.Millisecond)

	// send a message to the channel
	replServer.SendReplicationMessage(testTGMessage)
	time.Sleep(100 * time.Millisecond)

	// --- then ---
	// assertion is done in the go func
}

func TestGRPCReplicationServer_GetWALStream_getClientAddr_error(t *testing.T) {
	// --- given ---
	t.Parallel()
	replServer := replication.NewGRPCReplicationServer()
	stream := &mock.GetClientAddrErrorWALStreamServer{}

	// --- when ---
	// getClientAddr fails and an error should be returned
	err := replServer.GetWALStream(nil, stream)

	// --- then ---
	if err == nil {
		t.Errorf("getClientAddr should fail")
	}
}
