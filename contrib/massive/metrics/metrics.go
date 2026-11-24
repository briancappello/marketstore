package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// MassiveStreamLastUpdate stores the Unix time when the given (bar, quote, trade) stream is updated.
var MassiveStreamLastUpdate = promauto.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "alpaca",
		Subsystem: "marketstore",
		Name:      "massive_last_update_time",
		Help:      "Last update time of Massive streams, partitioned by type",
	},
	[]string{
		"type",
	},
)

// MassiveSplitsLastSync stores the Unix time of the last successful split sync.
var MassiveSplitsLastSync = promauto.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "alpaca",
		Subsystem: "marketstore",
		Name:      "massive_splits_last_sync_time",
		Help:      "Last time split events were synced from Massive",
	},
)
