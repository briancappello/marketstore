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
