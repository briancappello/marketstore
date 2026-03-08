// Package replayworker implements a BgWorker that serves historical data
// replay over a WebSocket endpoint at /ws/replay. Clients connect, send a
// subscribe message specifying a time range and step interval, and the
// server streams matching bars from disk at the requested pace.
package replayworker

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"

	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Config holds the plugin configuration parsed from marketstore.yml.
type Config struct {
	// Endpoint is the HTTP path to register the WebSocket handler on.
	// Defaults to "/ws/replay".
	Endpoint string `json:"endpoint"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// ReplayWorker implements bgworker.BgWorker.
type ReplayWorker struct {
	endpoint string
}

var _ bgworker.BgWorker = &ReplayWorker{}

func recast(config map[string]interface{}) (*Config, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("[streamreplay] marshal config: %w", err)
	}
	ret := Config{}
	if err = json.Unmarshal(data, &ret); err != nil {
		return nil, fmt.Errorf("[streamreplay] unmarshal config: %w", err)
	}
	return &ret, nil
}

// NewBgWorker creates a new ReplayWorker and registers the HTTP handler.
// This is called during server startup before http.ListenAndServe, so
// registering on http.DefaultServeMux is safe.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	config, err := recast(conf)
	if err != nil {
		return nil, fmt.Errorf("[streamreplay] recast config: %w", err)
	}

	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = "/ws/replay"
	}

	w := &ReplayWorker{endpoint: endpoint}

	// Register the handler on the default mux before the server starts.
	http.HandleFunc(endpoint, w.handleReplay)
	log.Info("[streamreplay] registered replay endpoint at %s", endpoint)

	return w, nil
}

// Run blocks forever. The actual work is done in per-connection goroutines
// spawned by the HTTP handler.
func (w *ReplayWorker) Run() {
	select {}
}

// handleReplay upgrades an HTTP connection to WebSocket and starts a
// replay session.
func (w *ReplayWorker) handleReplay(wr http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(wr, r, nil)
	if err != nil {
		log.Error("[streamreplay] failed to upgrade websocket (%v)", err)
		return
	}

	log.Info("[streamreplay] new replay connection: %v", ws.RemoteAddr().String())

	sess := NewSession(ws)
	go sess.Run()
}
