package main

import (
	"github.com/alpacahq/marketstore/v4/contrib/streamreplay/replayworker"
	"github.com/alpacahq/marketstore/v4/plugins/bgworker"
)

// NewBgWorker returns a new background worker that serves
// historical data replay over a WebSocket endpoint.
// nolint:deadcode // called by plugin using reflection
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	return replayworker.NewBgWorker(conf)
}

func main() {}
