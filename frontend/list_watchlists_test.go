package frontend_test

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/frontend"
)

// mockProvider implements frontend.WatchlistProvider for testing.
type mockProvider struct {
	rankings  map[string][]frontend.WatchlistRankingEntry
	lastQuery frontend.WatchlistQuery
}

func (m *mockProvider) ListNames() []string {
	names := make([]string, 0, len(m.rankings))
	for name := range m.rankings {
		names = append(names, name)
	}
	return names
}

func (m *mockProvider) GetRanking(name string) []frontend.WatchlistRankingEntry {
	return m.rankings[name]
}

func (m *mockProvider) AllRankings() map[string][]frontend.WatchlistRankingEntry {
	return m.rankings
}

// Rankings imitates the watchlist plugin: unknown names are not found, a
// _TRADITIONAL list outside the regular session and a weekend as_of are
// invalid, and every list carries the resolved window.
func (m *mockProvider) Rankings(q frontend.WatchlistQuery) (frontend.WatchlistResult, error) {
	m.lastQuery = q
	sess := q.Session
	if sess == "" {
		sess = "regular"
	}
	if sess != "premarket" && sess != "regular" && sess != "afterhours" {
		return frontend.WatchlistResult{}, fmt.Errorf("%w: invalid session %q", frontend.ErrWatchlistInvalid, sess)
	}
	if strings.HasPrefix(q.AsOf, "2026-09-26") {
		return frontend.WatchlistResult{}, fmt.Errorf("%w: 2026-09-26: not a trading day", frontend.ErrWatchlistInvalid)
	}
	names := q.Names
	if len(names) == 0 {
		for n := range m.rankings {
			names = append(names, n)
		}
	}
	start := time.Date(2026, 9, 21, 9, 30, 0, 0, time.UTC)
	var res frontend.WatchlistResult
	for _, n := range names {
		entries, ok := m.rankings[n]
		if !ok {
			return frontend.WatchlistResult{}, fmt.Errorf("%w: %s", frontend.ErrWatchlistNotFound, n)
		}
		basis := "session"
		if strings.HasSuffix(n, "_TRADITIONAL") {
			basis = "traditional"
			if sess != "regular" {
				if len(q.Names) > 0 {
					return frontend.WatchlistResult{}, fmt.Errorf(
						"%w: %s is only available in the regular session", frontend.ErrWatchlistInvalid, n)
				}
				continue
			}
		}
		res.Lists = append(res.Lists, frontend.WatchlistList{
			Name: n, Basis: basis, Session: sess, TradingDate: "2026-09-21",
			WindowStart: start, WindowEnd: start.Add(105 * time.Minute), Complete: false,
			Entries: entries,
		})
	}
	return res, nil
}

func setupWatchlistTest(t *testing.T, provider frontend.WatchlistProvider) {
	t.Helper()
	atomic.StoreUint32(&frontend.Queryable, 1)
	if provider != nil {
		frontend.RegisterWatchlistProvider(provider)
	}
	t.Cleanup(func() {
		frontend.RegisterWatchlistProvider(nil)
	})
}

func TestListWatchlists_NoProvider(t *testing.T) {
	atomic.StoreUint32(&frontend.Queryable, 1)
	frontend.RegisterWatchlistProvider(nil)
	defer frontend.RegisterWatchlistProvider(nil)

	service := &frontend.DataService{}
	var resp frontend.ListWatchlistsResponse
	err := service.ListWatchlists(nil, nil, &resp)

	assert.Nil(t, err)
	assert.Empty(t, resp.Watchlists)
}

func TestListWatchlists_AllWatchlists(t *testing.T) {
	provider := &mockProvider{
		rankings: map[string][]frontend.WatchlistRankingEntry{
			"PCT_CHANGE_UP": {
				{Symbol: "AAPL", Rank: 1, Fields: []frontend.WatchlistRankingField{{Key: "pct_change", Value: 5.2}}},
				{Symbol: "NVDA", Rank: 2, Fields: []frontend.WatchlistRankingField{{Key: "pct_change", Value: 4.1}}},
			},
			"VOLUME_UP": {
				{Symbol: "TSLA", Rank: 1, Fields: []frontend.WatchlistRankingField{{Key: "volume", Value: 1000000}}},
			},
		},
	}
	setupWatchlistTest(t, provider)

	service := &frontend.DataService{}
	var resp frontend.ListWatchlistsResponse
	err := service.ListWatchlists(nil, nil, &resp)

	assert.Nil(t, err)
	assert.Len(t, resp.Watchlists, 2)

	// Results are sorted by name
	assert.Equal(t, "PCT_CHANGE_UP", resp.Watchlists[0].Name)
	assert.Len(t, resp.Watchlists[0].Entries, 2)
	assert.Equal(t, "AAPL", resp.Watchlists[0].Entries[0].Symbol)
	assert.Equal(t, 1, resp.Watchlists[0].Entries[0].Rank)
	assert.Equal(t, 5.2, resp.Watchlists[0].Entries[0].Fields["pct_change"])

	assert.Equal(t, "VOLUME_UP", resp.Watchlists[1].Name)
	assert.Len(t, resp.Watchlists[1].Entries, 1)
}

func TestListWatchlists_SingleWatchlist(t *testing.T) {
	provider := &mockProvider{
		rankings: map[string][]frontend.WatchlistRankingEntry{
			"PCT_CHANGE_UP": {
				{Symbol: "AAPL", Rank: 1, Fields: []frontend.WatchlistRankingField{{Key: "pct_change", Value: 5.2}}},
			},
			"VOLUME_UP": {
				{Symbol: "TSLA", Rank: 1, Fields: []frontend.WatchlistRankingField{{Key: "volume", Value: 999}}},
			},
		},
	}
	setupWatchlistTest(t, provider)

	service := &frontend.DataService{}
	req := &frontend.ListWatchlistsRequest{Name: "PCT_CHANGE_UP"}
	var resp frontend.ListWatchlistsResponse
	err := service.ListWatchlists(nil, req, &resp)

	assert.Nil(t, err)
	assert.Len(t, resp.Watchlists, 1)
	assert.Equal(t, "PCT_CHANGE_UP", resp.Watchlists[0].Name)
	assert.Len(t, resp.Watchlists[0].Entries, 1)
	assert.Equal(t, "AAPL", resp.Watchlists[0].Entries[0].Symbol)
}

func TestListWatchlists_UnknownWatchlist(t *testing.T) {
	provider := &mockProvider{
		rankings: map[string][]frontend.WatchlistRankingEntry{},
	}
	setupWatchlistTest(t, provider)

	service := &frontend.DataService{}
	req := &frontend.ListWatchlistsRequest{Name: "NONEXISTENT"}
	var resp frontend.ListWatchlistsResponse
	err := service.ListWatchlists(nil, req, &resp)

	// An unknown name is an error that says so, not an empty list.
	assert.ErrorIs(t, err, frontend.ErrWatchlistNotFound)
	assert.Contains(t, err.Error(), "NONEXISTENT")
}

func TestListWatchlists_NotQueryable(t *testing.T) {
	atomic.StoreUint32(&frontend.Queryable, 0)
	defer atomic.StoreUint32(&frontend.Queryable, 1)

	service := &frontend.DataService{}
	var resp frontend.ListWatchlistsResponse
	err := service.ListWatchlists(nil, nil, &resp)

	assert.NotNil(t, err)
}
