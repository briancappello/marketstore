package frontend_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/frontend"
)

// newRESTServer builds an httptest server with the REST routes registered
// against the supplied DataService and CORS allow-list.
func newRESTServer(t *testing.T, svc *frontend.DataService, origins []string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	svc.RegisterRESTRoutes(mux, origins)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestRESTSymbols(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/symbols")
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var body map[string]any
	assert.Nil(t, json.NewDecoder(resp.Body).Decode(&body))
	// Lowercase json tag, not the Go field name.
	assert.Contains(t, body, "results")
	assert.NotContains(t, body, "Results")
}

func TestRESTNotQueryableReturns503(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 0)
	t.Cleanup(func() { atomic.StoreUint32(&frontend.Queryable, 1) })
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/symbols")
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	var body map[string]string
	assert.Nil(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.NotEmpty(t, body["error"])
}

func TestRESTUnknownRouteReturnsJSON404(t *testing.T) {
	// A 404 must be a JSON envelope, not net/http's default HTML.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/nope")
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var body map[string]string
	assert.Nil(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.NotEmpty(t, body["error"])
}

func TestRESTCORSDisabledByDefault(t *testing.T) {
	// Empty allow-list means no CORS headers, so browser exposure stays
	// a deliberate configuration act.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/symbols", nil)
	req.Header.Set("Origin", "https://example.com")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"))
}

func TestRESTCORSAllowedOrigin(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, []string{"https://example.com"})

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/symbols", nil)
	req.Header.Set("Origin", "https://example.com")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "Origin", resp.Header.Get("Vary"))

	// A non-allowed origin gets nothing.
	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/symbols", nil)
	req2.Header.Set("Origin", "https://evil.com")
	resp2, err := http.DefaultClient.Do(req2)
	assert.Nil(t, err)
	defer resp2.Body.Close()
	assert.Empty(t, resp2.Header.Get("Access-Control-Allow-Origin"))
}

func TestRESTCORSPreflight(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, []string{"https://example.com"})

	req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/v1/symbols", nil)
	req.Header.Set("Origin", "https://example.com")
	resp, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "https://example.com", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Contains(t, resp.Header.Get("Access-Control-Allow-Methods"), "GET")
}

func TestRESTBarsRejectsMultiSymbolAndWildcard(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	for _, path := range []string{"/v1/bars/AAPL,MSFT", "/v1/bars/*"} {
		resp, err := http.Get(srv.URL + path)
		assert.Nil(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, path)
		resp.Body.Close()
	}
}

func TestRESTBarsRejectsOversizeLimit(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	// 10000 is allowed; 10001 is a 400, not a clamp.
	resp, err := http.Get(srv.URL + "/v1/bars/AAPL?limit=10001")
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]string
	assert.Nil(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Contains(t, body["error"], "10000")
}

func TestRESTBarsRejectsBadTimeBound(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/bars/AAPL?start=nonsense")
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRESTBarsUnknownSymbolReturns404(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/bars/NOSUCHSYMBOL")
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

func TestRESTQuotesRejectsWildcardInSymbols(t *testing.T) {
	// The catalog-wide case is expressed by omitting ?symbols= entirely.
	// A literal "*" must not reach queryColumnSeries, which would expand
	// it a second time.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/quotes?symbols=*")
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRESTQuotesEmptyCatalogReturnsEmptyList(t *testing.T) {
	// No data must yield an empty list, never a 404 and never a null.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/quotes?symbols=NOSUCHSYMBOL")
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Quotes []map[string]any `json:"quotes"`
	}
	assert.Nil(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.NotNil(t, body.Quotes)
	assert.Empty(t, body.Quotes)
}

func TestRESTQuotesNotQueryable(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 0)
	t.Cleanup(func() { atomic.StoreUint32(&frontend.Queryable, 1) })
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/quotes")
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestRESTWatchlistsNoProviderReturnsEmptyList(t *testing.T) {
	// The watchlist plugin is optional. With no provider registered the
	// endpoint must return an empty list, matching the RPC behaviour,
	// rather than erroring.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	frontend.RegisterWatchlistProvider(nil)
	t.Cleanup(func() { frontend.RegisterWatchlistProvider(nil) })
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/watchlists")
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Watchlists []map[string]any `json:"watchlists"`
	}
	assert.Nil(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.NotNil(t, body.Watchlists)
	assert.Empty(t, body.Watchlists)
}

func TestRESTWatchlistsJSONFieldNames(t *testing.T) {
	// The response types carry only msgpack tags today, so without json
	// tags they would serialize as Watchlists/Name/Entries/Symbol/Rank.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	frontend.RegisterWatchlistProvider(nil)
	t.Cleanup(func() { frontend.RegisterWatchlistProvider(nil) })
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/watchlists")
	assert.Nil(t, err)
	defer resp.Body.Close()

	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	assert.Contains(t, string(buf[:n]), `"watchlists"`)
	assert.NotContains(t, string(buf[:n]), `"Watchlists"`)
}

func TestRESTWatchlistByNameNotFound(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	frontend.RegisterWatchlistProvider(nil)
	t.Cleanup(func() { frontend.RegisterWatchlistProvider(nil) })
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/watchlists/NOSUCHLIST")
	assert.Nil(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}

func TestRESTBarsOpenRangeIsNotCached(t *testing.T) {
	// With no end bound the newest bar still mutates, so the response
	// must not be cacheable.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/bars/NOSUCHSYMBOL")
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
}

func TestRESTBarsPastRangeIsCacheable(t *testing.T) {
	// A range that ended in the past can never change.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/bars/NOSUCHSYMBOL?end=2020-01-01T00:00:00Z")
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Contains(t, resp.Header.Get("Cache-Control"), "max-age=")
	assert.NotEqual(t, "no-cache", resp.Header.Get("Cache-Control"))
}

func TestRESTQuotesShortCache(t *testing.T) {
	// Quotes are always "now", so the past-end rule would make them
	// permanently uncacheable. They get a short TTL instead, which is what
	// lets a fronting proxy collapse a catalog-wide stampede.
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	resp, err := http.Get(srv.URL + "/v1/quotes?symbols=NOSUCHSYMBOL")
	assert.Nil(t, err)
	defer resp.Body.Close()
	assert.Contains(t, resp.Header.Get("Cache-Control"), "max-age=2")
}

func TestRESTETagReturns304(t *testing.T) {
	svc := setupListSymbols(t)
	atomic.StoreUint32(&frontend.Queryable, 1)
	srv := newRESTServer(t, svc, nil)

	url := srv.URL + "/v1/quotes?symbols=NOSUCHSYMBOL"
	resp, err := http.Get(url)
	assert.Nil(t, err)
	etag := resp.Header.Get("ETag")
	resp.Body.Close()
	assert.NotEmpty(t, etag)

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req)
	assert.Nil(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusNotModified, resp2.StatusCode)
}
