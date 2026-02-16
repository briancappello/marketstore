package worker

import (
	"sync"
)

// Pool is a simple goroutine worker pool that executes submitted functions
// concurrently up to a fixed parallelism limit.
type Pool struct {
	input     chan func()
	waitGroup sync.WaitGroup
}

// NewWorkerPool creates a Pool with the given number of goroutines.
func NewWorkerPool(poolSize int) *Pool {
	wp := Pool{
		input: make(chan func()),
	}

	for i := 0; i < poolSize; i++ {
		wp.waitGroup.Add(1)
		go work(wp.input, &wp.waitGroup)
	}

	return &wp
}

// Do submits a function to the pool for execution.
func (p *Pool) Do(fn func()) {
	p.input <- fn
}

// CloseAndWait signals that no more work will be submitted, then waits
// for all in-progress work to complete.
func (p *Pool) CloseAndWait() {
	close(p.input)
	p.waitGroup.Wait()
}

func work(input chan func(), waitGroup *sync.WaitGroup) {
	for fn := range input {
		fn()
	}

	waitGroup.Done()
}
