// REST API for MarketStore.
//
// This is a read-only surface returning row-oriented JSON, intended for
// browsers and ordinary tooling. It shares its query implementation with the
// RPC surface via DataService.queryColumnSeries and never touches the
// NumpyDataset wire format.
//
// Authentication, TLS, and response compression are deliberately absent:
// they belong to the reverse proxy that fronts this server.
package frontend

import (
	"net/http"
	"sync/atomic"

	"github.com/alpacahq/marketstore/v4/utils/log"
	"github.com/alpacahq/marketstore/v4/utils/wscodec"
)

type errorResponse struct {
	Error string `json:"error"`
}

// writeJSON renders v as JSON with the given status code.
//
// Marshalling goes through wscodec.JSONCodec rather than encoding/json
// directly because encoding/json rejects NaN and ±Inf, which OHLCV columns
// carry where a bar has no value. The codec renders them as null.
func writeJSON(w http.ResponseWriter, status int, v any) {
	buf, err := wscodec.JSONCodec.Marshal(v)
	if err != nil {
		log.Error("rest: failed to encode response: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"failed to encode response"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(buf); err != nil {
		log.Error("rest: failed to write response: %v", err)
	}
}

// writeError renders a JSON error envelope. The HTTP status carries the
// category, so the body needs only a message.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}

// requireQueryable reports whether the server has finished startup, writing
// a 503 and returning false when it has not. Querying before Queryable is
// set would read an incomplete catalog.
func requireQueryable(w http.ResponseWriter) bool {
	if atomic.LoadUint32(&Queryable) == 0 {
		writeError(w, http.StatusServiceUnavailable, "server is not queryable")
		return false
	}
	return true
}

// corsMiddleware emits CORS headers for configured origins only. With an
// empty allow-list nothing is emitted and the API is same-origin, so
// exposing it to browsers stays a deliberate configuration act.
func corsMiddleware(allowed []string, next http.Handler) http.Handler {
	allowSet := make(map[string]struct{}, len(allowed))
	for _, o := range allowed {
		allowSet[o] = struct{}{}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			_, ok := allowSet[origin]
			if !ok {
				_, ok = allowSet["*"]
			}
			if ok {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			}
		}

		// Preflight is answered here so it never reaches a GET-only route.
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// RegisterRESTRoutes registers the read-only REST API on mux under /v1/.
//
// Routes use the Go 1.22+ ServeMux pattern syntax, so no router dependency
// is required. The whole subtree is mounted behind the CORS middleware so
// that preflight requests and unknown paths are handled consistently.
func (s *DataService) RegisterRESTRoutes(mux *http.ServeMux, allowedOrigins []string) {
	api := http.NewServeMux()

	api.HandleFunc("GET /v1/symbols", s.handleRESTSymbols)
	api.HandleFunc("GET /v1/bars/{symbol}", s.handleRESTBars)
	api.HandleFunc("GET /v1/quotes", s.handleRESTQuotes)
	api.HandleFunc("GET /v1/watchlists", s.handleRESTWatchlists)
	api.HandleFunc("GET /v1/watchlists/{name}", s.handleRESTWatchlist)

	// Catch-all so an unknown path returns a JSON envelope rather than
	// net/http's default HTML 404.
	api.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "no such endpoint: "+r.URL.Path)
	})

	mux.Handle("/v1/", corsMiddleware(allowedOrigins, api))
}
