package replication_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/replication"
)

type MockReplicationService struct {
	LastSentMessage []byte
}

func (ms *MockReplicationService) SendReplicationMessage(transactionGroup []byte) {
	ms.LastSentMessage = transactionGroup
}

func TestNewSender(t *testing.T) {
	t.Parallel()
	// --- given ---
	mockService := MockReplicationService{}
	// --- when ---
	got := replication.NewSender(&mockService)
	// --- then ---
	if got == nil {
		t.Error("Sender is not initialized")
	}
}

func TestSender_Run_Sender(t *testing.T) {
	t.Parallel()

	// --- given ---
	mockService := MockReplicationService{}
	message := []byte{1, 2, 3}
	SUT := replication.NewSender(&mockService)

	// --- when ---
	// run the sender goroutine
	SUT.Run(context.Background())
	// send the message to sender
	SUT.Send(message)
	time.Sleep(300 * time.Millisecond)

	// --- then ---
	// message should be sent to ReplicationService
	if !bytes.Equal(mockService.LastSentMessage, message) {
		t.Errorf("message is not sent")
	}
}

func TestSenderSendDoesNotBlockWhenBufferFull(t *testing.T) {
	t.Parallel()

	// A Sender whose Run loop is never started will never drain its channel.
	s := replication.NewSender(&MockReplicationService{})

	// defaultSenderChannelSize is 500; push far more than that. If Send blocks
	// when full, this test hangs (and the suite times out) instead of passing.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 5000; i++ {
			s.Send([]byte("x"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked when the buffer was full")
	}
}

func TestSender_Run_Context_Done(t *testing.T) {
	t.Parallel()

	// --- given ---
	mockService := MockReplicationService{}
	message := []byte{1, 2, 3}
	SUT := replication.NewSender(&mockService)
	ctx, cancelFunc := context.WithCancel(context.Background())

	// --- when ---
	// run the sender goroutine
	SUT.Run(ctx)

	// finish the goroutine by cancel
	cancelFunc()
	time.Sleep(200 * time.Millisecond)

	// send the message to sender
	SUT.Send(message)
	time.Sleep(100 * time.Millisecond)

	// --- then ---
	// message should not be sent because the goroutine is already finished
	if bytes.Equal(mockService.LastSentMessage, message) {
		t.Errorf("sender goroutine is not finished")
	}
}
