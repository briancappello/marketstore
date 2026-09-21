package frontend

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

var Queryable uint32 // treated as bool

// replicationBroken is set when this replica's live replication stream has
// stopped for good (a non-retryable replay failure). Treated as bool.
//
// It is deliberately separate from Queryable. The data already on disk is
// still readable and a caller may legitimately want it, so queries keep
// working; what must change is what the server SAYS about itself. Once this
// is set, the health endpoints report unhealthy so an orchestrator or load
// balancer can take the node out of rotation, instead of the node continuing
// to advertise itself as good while its data silently falls behind the master.
var replicationBroken uint32

// SetReplicationBroken marks the live replication stream as permanently
// stopped. It is one-way: recovery requires a restart, because the stream is
// not re-established after a non-retryable failure.
func SetReplicationBroken() {
	atomic.StoreUint32(&replicationBroken, 1)
}

// ReplicationBroken reports whether the live replication stream has stopped
// for good.
func ReplicationBroken() bool {
	return atomic.LoadUint32(&replicationBroken) != 0
}

type HeartbeatMessage struct {
	Status  string `json:"status"`
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	Uptime  string `json:"uptime"`
}

func NewUtilityAPIHandlers(startTime time.Time) *UtilityAPIHandlers {
	return &UtilityAPIHandlers{startTime: startTime}
}

type UtilityAPIHandlers struct {
	startTime time.Time
}

func (uah *UtilityAPIHandlers) Handle(url string) error {
	// heartbeat
	http.HandleFunc("/heartbeat", uah.heartbeat)

	// profiling
	http.HandleFunc("/pprof/", pprof.Index)
	http.HandleFunc("/pprof/cmdline", pprof.Cmdline)
	http.HandleFunc("/pprof/profile", pprof.Profile)
	http.HandleFunc("/pprof/symbol", pprof.Symbol)
	http.HandleFunc("/pprof/trace", pprof.Trace)
	http.Handle("/pprof/heap", pprof.Handler("heap"))
	http.Handle("/pprof/goroutine", pprof.Handler("goroutine"))
	http.Handle("/pprof/threadcreate", pprof.Handler("threadcreate"))
	http.Handle("/pprof/block", pprof.Handler("block"))

	return http.ListenAndServe(url, nil)
}

func (uah *UtilityAPIHandlers) heartbeat(rw http.ResponseWriter, _ *http.Request) {
	uptime := time.Since(uah.startTime).String()
	queryable := atomic.LoadUint32(&Queryable)
	switch {
	case queryable > 0 && ReplicationBroken():
		// Queryable, but the data is no longer advancing. Report unhealthy so
		// this node is pulled from rotation; serving stale data while claiming
		// to be healthy is the failure we are guarding against.
		rw.WriteHeader(http.StatusServiceUnavailable)
		err := json.NewEncoder(rw).Encode(HeartbeatMessage{
			Status:  "replication stopped",
			Version: utils.Tag,
			GitHash: utils.GitHash,
			Uptime:  uptime,
		})
		if err != nil {
			log.Error("Failed to write heartbeat message - Error: %v", err)
		}
	case queryable > 0:
		// queryable
		rw.WriteHeader(http.StatusOK)
		err := json.NewEncoder(rw).Encode(HeartbeatMessage{
			Status:  "queryable",
			Version: utils.Tag,
			GitHash: utils.GitHash,
			Uptime:  uptime,
		})
		if err != nil {
			log.Error("Failed to write heartbeat message - Error: %v", err)
		}
	default:
		// not queryable
		rw.WriteHeader(http.StatusServiceUnavailable)
		err := json.NewEncoder(rw).Encode(HeartbeatMessage{
			Status:  "not queryable",
			Version: utils.Tag,
			GitHash: utils.GitHash,
			Uptime:  uptime,
		})
		if err != nil {
			log.Error("Failed to write heartbeat message - Error: %v", err)
		}
	}
}
