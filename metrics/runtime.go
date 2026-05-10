package metrics

import (
	"context"
	"runtime"
	"runtime/metrics"
	"time"
)

// runtimeThreadsMetric is the runtime/metrics name for the count of OS
// threads owned by the Go runtime. Available since Go 1.21.
const runtimeThreadsMetric = "/sched/threads/total:threads"

// StartRuntimeMonitor periodically samples runtime-level counters
// (live goroutine count and OS thread count) and publishes them to the
// Goroutines and OSThreads Prometheus gauges.
//
// The sampler runs until ctx is cancelled. Sampling cost is negligible
// (a single runtime call plus a runtime/metrics.Read for one sample),
// so a fairly tight interval (e.g. 15-30s) is safe.
func StartRuntimeMonitor(ctx context.Context, interval time.Duration) {
	sample := []metrics.Sample{{Name: runtimeThreadsMetric}}

	publish := func() {
		Goroutines.Set(float64(runtime.NumGoroutine()))

		metrics.Read(sample)
		// Defensive: KindBad means the metric is unsupported by this
		// runtime (shouldn't happen on Go >= 1.21 but don't crash).
		if sample[0].Value.Kind() == metrics.KindUint64 {
			OSThreads.Set(float64(sample[0].Value.Uint64()))
		}
	}

	publish() // emit an initial sample immediately

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			publish()
		}
	}
}
