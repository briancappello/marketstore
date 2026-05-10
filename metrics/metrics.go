package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	namespace = "alpaca"
	subsystem = "marketstore"
)

var (
	// StartupTime stores how long the startup took (in seconds).
	StartupTime = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "startup_seconds",
			Help:      "Seconds taken by the startup",
		},
	)

	// RPCTotalRequestDuration stores the processing time for every request.
	RPCTotalRequestDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "rpc_total_request_duration_seconds",
		Help:      "RPC request processing time for every request",
	})

	// RPCSuccessfulRequestDuration stores the processing time for successful
	// requests partitioned by method.
	RPCSuccessfulRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "rpc_successful_request_duration_seconds",
		Help:      "RPC request processing time for successful requests partitioned by method",
	}, []string{"method"})

	// WSConnections keeps track of the number of currently established WS connections.
	WSConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "ws_connections",
			Help:      "Current number of ws connections established with Marketstore",
		},
	)

	// WriteCSMDuration stores the WriteCSM call durations for writes
	// that didn't result in an error.
	WriteCSMDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "write_csm_duration_seconds",
		Help:      "WriteCSM call duration",
		Buckets:   []float64{.0001, .001, .005, .01, .05, .1, .25, .5, 1},
	})

	// TotalDiskUsageBytes stores the total size of DB files managed by Marketstore.
	TotalDiskUsageBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_disk_usage_bytes",
			Help:      "Total disk usage [bytes] of the Marketstore data files",
		})

	// Goroutines exposes runtime.NumGoroutine() for monitoring goroutine
	// growth or leaks.
	Goroutines = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "goroutines",
			Help:      "Current number of live goroutines (runtime.NumGoroutine).",
		})

	// OSThreads exposes the count of OS threads owned by the Go runtime
	// (runtime/metrics /sched/threads/total:threads). This is the metric
	// to watch for the unbounded thread accumulation pattern documented in
	// plans/os-thread-accumulation.md: Go never releases OS threads once
	// created, so this gauge only ever grows. Sustained growth past a
	// few hundred is a signal that something (typically blocking syscalls
	// in CGo or backfill code paths) is forcing the scheduler to spawn
	// new M's faster than they can be reused.
	OSThreads = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "os_threads",
			Help:      "Current count of OS threads owned by the Go runtime (/sched/threads/total).",
		})
)
