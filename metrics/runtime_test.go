package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/metrics"
)

func TestStartRuntimeMonitor(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run with a tight interval so the test stays fast.
	go metrics.StartRuntimeMonitor(ctx, 5*time.Millisecond)

	// Allow at least one publish cycle to complete.
	time.Sleep(50 * time.Millisecond)

	// Both gauges must be set to a positive value: there is always at
	// least one goroutine (this test) and at least one OS thread.
	assert.Greater(t, testutil.ToFloat64(metrics.Goroutines), 0.0,
		"Goroutines gauge should reflect runtime.NumGoroutine()")
	assert.Greater(t, testutil.ToFloat64(metrics.OSThreads), 0.0,
		"OSThreads gauge should reflect /sched/threads/total:threads")
}
