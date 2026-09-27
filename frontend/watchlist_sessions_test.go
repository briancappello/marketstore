package frontend_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/proto"
)

// Scenarios from the rankings-api spec, against a provider that behaves
// like the watchlist plugin (see mockProvider.Rankings).

func sessionProvider(t *testing.T) *mockProvider {
	t.Helper()
	p := &mockProvider{rankings: map[string][]frontend.WatchlistRankingEntry{
		"PCT_CHANGE_UP":             {{Symbol: "AAPL", Rank: 1}},
		"PCT_CHANGE_UP_TRADITIONAL": {{Symbol: "MSFT", Rank: 1}},
		"GAP_UP_TRADITIONAL":        {},
	}}
	setupWatchlistTest(t, p)
	return p
}

func getJSON(t *testing.T, url string, out interface{}) int {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	if out != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	}
	return resp.StatusCode
}

func TestRESTWatchlistRewind(t *testing.T) {
	p := sessionProvider(t)
	srv := newRESTServer(t, setupListSymbols(t), nil)
	atomic.StoreUint32(&frontend.Queryable, 1)

	var body map[string]interface{}
	code := getJSON(t, srv.URL+"/v1/watchlists/PCT_CHANGE_UP?session=regular&as_of=2026-09-21T11:15", &body)
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, frontend.WatchlistQuery{Names: []string{"PCT_CHANGE_UP"}, Session: "regular",
		AsOf: "2026-09-21T11:15"}, p.lastQuery, "session and as_of reach the provider")
	assert.Equal(t, "session", body["basis"])
	assert.Equal(t, "regular", body["session"])
	assert.Equal(t, "2026-09-21", body["trading_date"])
	assert.Equal(t, false, body["complete"])
	assert.Contains(t, body, "window_start")
	assert.Contains(t, body, "window_end")

	var all frontend.ListWatchlistsResponse
	code = getJSON(t, srv.URL+"/v1/watchlists?session=regular", &all)
	assert.Equal(t, http.StatusOK, code)
	require.Len(t, all.Watchlists, 3)
	assert.Equal(t, "GAP_UP_TRADITIONAL", all.Watchlists[0].Name, "sorted by name")
	assert.Equal(t, "traditional", all.Watchlists[0].Basis)
	assert.NotNil(t, all.Watchlists[0].Entries, "a known empty list is an empty array")
}

func TestRESTWatchlistErrors(t *testing.T) {
	sessionProvider(t)
	srv := newRESTServer(t, setupListSymbols(t), nil)
	atomic.StoreUint32(&frontend.Queryable, 1)

	var e map[string]interface{}
	assert.Equal(t, http.StatusBadRequest, getJSON(t, srv.URL+"/v1/watchlists?as_of=2026-09-26T10:00", &e))
	assert.Contains(t, e["error"], "not a trading day")

	assert.Equal(t, http.StatusBadRequest, getJSON(t, srv.URL+"/v1/watchlists?session=overnight", nil))
	assert.Equal(t, http.StatusBadRequest,
		getJSON(t, srv.URL+"/v1/watchlists/GAP_UP_TRADITIONAL?session=afterhours", nil))
	assert.Equal(t, http.StatusNotFound, getJSON(t, srv.URL+"/v1/watchlists/GAP_UP", nil),
		"the renamed list no longer exists")

	// Listing all lists in afterhours leaves out the traditional ones.
	var all frontend.ListWatchlistsResponse
	assert.Equal(t, http.StatusOK, getJSON(t, srv.URL+"/v1/watchlists?session=afterhours", &all))
	require.Len(t, all.Watchlists, 1)
	assert.Equal(t, "PCT_CHANGE_UP", all.Watchlists[0].Name)
}

func TestGRPCWatchlistRewindAndErrors(t *testing.T) {
	p := sessionProvider(t)
	svc := frontend.GRPCService{}

	resp, err := svc.ListWatchlists(context.Background(),
		&proto.ListWatchlistsRequest{Session: "premarket", AsOf: "2026-09-21"})
	require.NoError(t, err)
	assert.Equal(t, frontend.WatchlistQuery{Session: "premarket", AsOf: "2026-09-21"}, p.lastQuery)
	require.Len(t, resp.Watchlists, 1, "only lists available in premarket")
	w := resp.Watchlists[0]
	assert.Equal(t, "PCT_CHANGE_UP", w.Name)
	assert.Equal(t, "session", w.Basis)
	assert.Equal(t, "premarket", w.Session)
	assert.Equal(t, "2026-09-21", w.TradingDate)
	assert.NotZero(t, w.WindowStart)
	assert.Greater(t, w.WindowEnd, w.WindowStart)

	_, err = svc.ListWatchlists(context.Background(),
		&proto.ListWatchlistsRequest{Name: "GAP_UP_TRADITIONAL", Session: "afterhours"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.ListWatchlists(context.Background(), &proto.ListWatchlistsRequest{Name: "GAP_UP"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestJSONRPCWatchlistRewindAndErrors(t *testing.T) {
	p := sessionProvider(t)
	service := &frontend.DataService{}

	var resp frontend.ListWatchlistsResponse
	require.NoError(t, service.ListWatchlists(nil,
		&frontend.ListWatchlistsRequest{Name: "PCT_CHANGE_UP_TRADITIONAL", Session: "regular", AsOf: "2026-09-21"},
		&resp))
	assert.Equal(t, "2026-09-21", p.lastQuery.AsOf)
	require.Len(t, resp.Watchlists, 1)
	assert.Equal(t, "traditional", resp.Watchlists[0].Basis)

	err := service.ListWatchlists(nil, &frontend.ListWatchlistsRequest{AsOf: "2026-09-26"}, &resp)
	assert.ErrorIs(t, err, frontend.ErrWatchlistInvalid)
	assert.Contains(t, err.Error(), "not a trading day", "the message states the cause")
}
