package worker

import (
	"context"
	"sync"
)

// Pool is a simple goroutine worker pool that executes submitted functions
// concurrently up to a fixed parallelism limit. It supports context cancellation
// for graceful shutdown.
type Pool struct {
	ctx       context.Context
	input     chan func()
	waitGroup sync.WaitGroup
	closed    bool
	mu        sync.Mutex
}

// NewWorkerPool creates a Pool with the given number of goroutines.
// The pool will stop accepting new work when the context is cancelled.
func NewWorkerPool(ctx context.Context, poolSize int) *Pool {
	wp := Pool{
		ctx:   ctx,
		input: make(chan func()),
	}

	for i := 0; i < poolSize; i++ {
		wp.waitGroup.Add(1)
		go wp.work()
	}

	return &wp
}

// Do submits a function to the pool for execution.
// Returns false if the pool is closed or context is cancelled.
func (p *Pool) Do(fn func()) bool {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false
	}
	p.mu.Unlock()

	select {
	case <-p.ctx.Done():
		return false
	case p.input <- fn:
		return true
	}
}

// CloseAndWait signals that no more work will be submitted, then waits
// for all in-progress work to complete.
func (p *Pool) CloseAndWait() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.input)
	}
	p.mu.Unlock()
	p.waitGroup.Wait()
}

func (p *Pool) work() {
	defer p.waitGroup.Done()
	for {
		select {
		case <-p.ctx.Done():
			// Drain remaining work from channel before exiting.
			for fn := range p.input {
				fn()
			}
			return
		case fn, ok := <-p.input:
			if !ok {
				return
			}
			fn()
		}
	}
}
