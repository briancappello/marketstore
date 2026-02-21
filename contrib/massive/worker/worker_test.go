package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewWorkerPool(t *testing.T) {
	ctx := context.Background()
	wp := NewWorkerPool(ctx, 2)
	assert.NotNil(t, wp)
	assert.NotNil(t, wp.input)

	wp.CloseAndWait()
}

func TestWorkerPoolDo(t *testing.T) {
	ctx := context.Background()
	wp := NewWorkerPool(ctx, 2)

	var counter int32
	for i := 0; i < 10; i++ {
		wp.Do(func() {
			atomic.AddInt32(&counter, 1)
		})
	}

	wp.CloseAndWait()
	assert.Equal(t, int32(10), atomic.LoadInt32(&counter))
}

func TestWorkerPoolContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wp := NewWorkerPool(ctx, 2)

	var counter int32

	// Submit a few tasks
	for i := 0; i < 3; i++ {
		wp.Do(func() {
			atomic.AddInt32(&counter, 1)
		})
	}

	// Cancel the context
	cancel()

	// Trying to submit more tasks should return false
	result := wp.Do(func() {
		atomic.AddInt32(&counter, 1)
	})

	// The Do should return false because context is cancelled
	assert.False(t, result)

	wp.CloseAndWait()

	// At least the first 3 tasks should have run
	assert.GreaterOrEqual(t, atomic.LoadInt32(&counter), int32(3))
}

func TestWorkerPoolCloseAndWait(t *testing.T) {
	ctx := context.Background()
	wp := NewWorkerPool(ctx, 2)

	var counter int32
	for i := 0; i < 5; i++ {
		wp.Do(func() {
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&counter, 1)
		})
	}

	wp.CloseAndWait()

	// All tasks should have completed
	assert.Equal(t, int32(5), atomic.LoadInt32(&counter))
}

func TestWorkerPoolDoAfterClose(t *testing.T) {
	ctx := context.Background()
	wp := NewWorkerPool(ctx, 2)

	wp.CloseAndWait()

	// Submitting after close should return false
	result := wp.Do(func() {})
	assert.False(t, result)
}
