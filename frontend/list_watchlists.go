package frontend

import (
	"net/http"
	"sort"
	"sync/atomic"
)

// ListWatchlistsRequest is the HTTP RPC request for listing watchlists.
type ListWatchlistsRequest struct {
	// Name optionally filters to a specific watchlist. If empty, all
	// watchlists are returned.
	Name string `msgpack:"name,omitempty"`
}

// ListWatchlistsResponse is the HTTP RPC response for listing watchlists.
type ListWatchlistsResponse struct {
	Watchlists []WatchlistRankingResponse `msgpack:"watchlists"`
}

// WatchlistRankingResponse is a single watchlist with its ranked entries.
type WatchlistRankingResponse struct {
	Name    string                          `msgpack:"name"`
	Entries []WatchlistRankingEntryResponse `msgpack:"entries"`
}

// WatchlistRankingEntryResponse is a single ranked symbol in a watchlist.
type WatchlistRankingEntryResponse struct {
	Symbol string                 `msgpack:"symbol"`
	Rank   int                    `msgpack:"rank"`
	Fields map[string]interface{} `msgpack:"fields"`
}

// ListWatchlists returns the current rankings for one or all watchlists.
// If the watchlist plugin is not loaded, an empty response is returned.
func (s *DataService) ListWatchlists(
	r *http.Request,
	req *ListWatchlistsRequest,
	response *ListWatchlistsResponse,
) error {
	if atomic.LoadUint32(&Queryable) == 0 {
		return errNotQueryable
	}

	provider := GetWatchlistProvider()
	if provider == nil {
		response.Watchlists = []WatchlistRankingResponse{}
		return nil
	}

	if req != nil && req.Name != "" {
		// Single watchlist
		ranking := provider.GetRanking(req.Name)
		response.Watchlists = []WatchlistRankingResponse{
			convertRanking(req.Name, ranking),
		}
		return nil
	}

	// All watchlists
	all := provider.AllRankings()
	names := make([]string, 0, len(all))
	for name := range all {
		names = append(names, name)
	}
	sort.Strings(names)

	response.Watchlists = make([]WatchlistRankingResponse, len(names))
	for i, name := range names {
		response.Watchlists[i] = convertRanking(name, all[name])
	}
	return nil
}

func convertRanking(name string, entries []WatchlistRankingEntry) WatchlistRankingResponse {
	resp := WatchlistRankingResponse{
		Name:    name,
		Entries: make([]WatchlistRankingEntryResponse, len(entries)),
	}
	for i, e := range entries {
		resp.Entries[i] = WatchlistRankingEntryResponse{
			Symbol: e.Symbol,
			Rank:   e.Rank,
			Fields: e.Fields,
		}
	}
	return resp
}
