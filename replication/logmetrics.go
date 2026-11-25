package replication

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/alpacahq/marketstore/v4/replication/replog"
)

const (
	metricsNamespace = "alpaca"
	metricsSubsystem = "marketstore"
)

// RegisterLogMetrics exposes the master's replication log state on reg: its
// first and last sequence, size, oldest-entry age, broken flag and how often
// readers had to go to disk. Values are read when scraped.
func RegisterLogMetrics(reg prometheus.Registerer, l *replog.Log, now func() time.Time) error {
	if now == nil {
		now = time.Now
	}
	gauge := func(name, help string, fn func() float64) prometheus.Collector {
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem, Name: name, Help: help,
		}, fn)
	}
	collectors := []prometheus.Collector{
		gauge("replication_log_first_seq", "Oldest sequence held by the replication log (0: empty).",
			func() float64 { return float64(l.FirstSeq()) }),
		gauge("replication_log_last_seq", "Newest sequence in the replication log.",
			func() float64 { return float64(l.LastSeq()) }),
		gauge("replication_log_bytes", "Size of the replication log on disk.",
			func() float64 { return float64(l.Bytes()) }),
		gauge("replication_log_oldest_entry_age_seconds", "Age of the oldest replication log entry.",
			func() float64 {
				oldest := l.OldestWallclock()
				if oldest == 0 {
					return 0
				}
				return now().Sub(time.Unix(0, oldest)).Seconds()
			}),
		gauge("replication_log_broken", "1 while replication log appends are failing.",
			func() float64 {
				if broken, _ := l.Broken(); broken {
					return 1
				}
				return 0
			}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: metricsNamespace, Subsystem: metricsSubsystem,
			Name: "replication_log_disk_reads_total",
			Help: "Replication log reads served from disk rather than the memory tail.",
		}, func() float64 { return float64(l.DiskReads()) }),
	}
	for _, c := range collectors {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}
