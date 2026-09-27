package frontend

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/alpacahq/marketstore/v4/utils"
)

// SessionFactsRebuilder queues a rebuild of the derived session facts
// (<SYMBOL>/1D/SESSIONS). The watchlist bgworker registers one at startup.
type SessionFactsRebuilder interface {
	QueueSessionFactsRebuild(symbols []string, from, to time.Time) (queued int, err error)
}

var (
	sessionFactsRebuilderMu sync.RWMutex
	sessionFactsRebuilder   SessionFactsRebuilder
)

// RegisterSessionFactsRebuilder registers the rebuilder used by the
// RebuildSessionFacts RPC.
func RegisterSessionFactsRebuilder(r SessionFactsRebuilder) {
	sessionFactsRebuilderMu.Lock()
	defer sessionFactsRebuilderMu.Unlock()
	sessionFactsRebuilder = r
}

func getSessionFactsRebuilder() SessionFactsRebuilder {
	sessionFactsRebuilderMu.RLock()
	defer sessionFactsRebuilderMu.RUnlock()
	return sessionFactsRebuilder
}

// ErrReplicaRebuild rejects a session facts rebuild on a replica, which
// receives session facts from the leader through replication.
var ErrReplicaRebuild = errors.New("session facts can only be rebuilt on the leader; this server is a replica")

// errNoSessionFacts means no loaded plugin maintains session facts.
var errNoSessionFacts = errors.New("session facts are not available: the watchlist bgworker is not loaded")

// RebuildSessionFactsRequest asks the leader to recompute session facts for
// every trading day From..To (YYYY-MM-DD) and Symbols (all with 1Min data
// when empty).
type RebuildSessionFactsRequest struct {
	From    string   `msgpack:"from" json:"from"`
	To      string   `msgpack:"to" json:"to"`
	Symbols []string `msgpack:"symbols" json:"symbols"`
}

// RebuildSessionFactsResponse reports how many (symbol, day) entries were
// queued. The watchlist bgworker works through the queue in the background.
type RebuildSessionFactsResponse struct {
	Queued int `msgpack:"queued" json:"queued"`
}

// RebuildSessionFacts queues a session facts rebuild on the leader.
func (s *DataService) RebuildSessionFacts(
	_ *http.Request, req *RebuildSessionFactsRequest, resp *RebuildSessionFactsResponse,
) error {
	if utils.InstanceConfig.Replication.IsReplica() {
		return ErrReplicaRebuild
	}
	rb := getSessionFactsRebuilder()
	if rb == nil {
		return errNoSessionFacts
	}
	from, err := time.Parse("2006-01-02", req.From)
	if err != nil {
		return fmt.Errorf("invalid from %q: want YYYY-MM-DD", req.From)
	}
	to, err := time.Parse("2006-01-02", req.To)
	if err != nil {
		return fmt.Errorf("invalid to %q: want YYYY-MM-DD", req.To)
	}
	if to.Before(from) {
		return errors.New("to is before from")
	}
	// The dates are calendar dates; pass them as noon UTC so they read as the
	// same date in America/New_York.
	queued, err := rb.QueueSessionFactsRebuild(req.Symbols, from.Add(12*time.Hour), to.Add(12*time.Hour))
	if err != nil {
		return err
	}
	resp.Queued = queued
	return nil
}
