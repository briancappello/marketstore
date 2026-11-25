package replication

import "github.com/prometheus/client_golang/prometheus"

// Test-only access to package metrics.
func UnrecoverableTickGapsMetric() prometheus.Collector { return unrecoverableTickGaps }
func FullResyncRequiredMetric() prometheus.Collector    { return fullResyncRequired }
