package frontend

import (
	"net/http"
	"sort"
	"sync/atomic"
)

// ListWatchlistsRequest is the HTTP RPC request for listing watchlists.
type ListWatchlistsRequest struct {
	// Name optionally filters to a specific watchlist. If empty, all
	// watchlists available in the resolved session are returned.
	Name string `msgpack:"name,omitempty"`
	// Session is "premarket", "regular" or "afterhours". Optional.
	Session string `msgpack:"session,omitempty"`
	// AsOf is an ISO-8601 date or date-time. Optional. Without Session and
	// AsOf, the live rankings are returned.
	AsOf string `msgpack:"as_of,omitempty"`
}

// ListWatchlistsResponse is the HTTP RPC response for listing watchlists.
type ListWatchlistsResponse struct {
	Watchlists []WatchlistRankingResponse `msgpack:"watchlists" json:"watchlists"`
}

// WatchlistRankingResponse is a single watchlist with its ranked entries and
// the ranking window it covers.
type WatchlistRankingResponse struct {
	Name    string                          `msgpack:"name" json:"name"`
	Entries []WatchlistRankingEntryResponse `msgpack:"entries" json:"entries"`
	// Basis is "session" or "traditional": what prior_close and pct_change
	// mean in this list.
	Basis string `msgpack:"basis" json:"basis"`
	// Session is "premarket", "regular" or "afterhours".
	Session string `msgpack:"session" json:"session"`
	// TradingDate is the session's date, YYYY-MM-DD in America/New_York.
	TradingDate string `msgpack:"trading_date" json:"trading_date"`
	// WindowStart and WindowEnd bound the ranking window (Unix seconds).
	WindowStart int64 `msgpack:"window_start" json:"window_start"`
	WindowEnd   int64 `msgpack:"window_end" json:"window_end"`
	// Complete is true when the window covers the whole session.
	Complete bool `msgpack:"complete" json:"complete"`
}

// WatchlistRankingEntryResponse is a single ranked symbol in a watchlist.
//
// Fields is serialized as a map<string,float64> on the wire to preserve
// JSON/msgpack compatibility with existing clients. It is built per-response
// from the typed framework.Field slice.
type WatchlistRankingEntryResponse struct {
	Symbol string             `msgpack:"symbol" json:"symbol"`
	Rank   int                `msgpack:"rank" json:"rank"`
	Fields map[string]float64 `msgpack:"fields" json:"fields"`
	// Sector is included only when non-empty (aggregate strategies).
	Sector string `msgpack:"sector,omitempty" json:"sector,omitempty"`
}

// queryWatchlists runs q against the registered provider. With no provider
// (the watchlist plugin is not loaded) it returns no lists and no error.
func queryWatchlists(q WatchlistQuery) ([]WatchlistList, error) {
	provider := GetWatchlistProvider()
	if provider == nil {
		return nil, nil
	}
	res, err := provider.Rankings(q)
	if err != nil {
		return nil, err
	}
	sort.Slice(res.Lists, func(i, j int) bool { return res.Lists[i].Name < res.Lists[j].Name })
	return res.Lists, nil
}

// ListWatchlists returns rankings for one or all watchlists, live or for a
// requested session and as_of. If the watchlist plugin is not loaded, an
// empty response is returned. Errors state their cause: an invalid or
// unavailable request wraps ErrWatchlistInvalid, an unknown name
// ErrWatchlistNotFound.
func (s *DataService) ListWatchlists(
	r *http.Request,
	req *ListWatchlistsRequest,
	response *ListWatchlistsResponse,
) error {
	if atomic.LoadUint32(&Queryable) == 0 {
		return errNotQueryable
	}
	q := WatchlistQuery{}
	if req != nil {
		q.Session, q.AsOf = req.Session, req.AsOf
		if req.Name != "" {
			q.Names = []string{req.Name}
		}
	}
	lists, err := queryWatchlists(q)
	if err != nil {
		return err
	}
	response.Watchlists = make([]WatchlistRankingResponse, len(lists))
	for i, l := range lists {
		response.Watchlists[i] = convertList(l)
	}
	return nil
}

func convertList(l WatchlistList) WatchlistRankingResponse {
	resp := convertRanking(l.Name, l.Entries)
	resp.Basis = l.Basis
	resp.Session = l.Session
	resp.TradingDate = l.TradingDate
	resp.WindowStart = l.WindowStart.Unix()
	resp.WindowEnd = l.WindowEnd.Unix()
	resp.Complete = l.Complete
	return resp
}

func convertRanking(name string, entries []WatchlistRankingEntry) WatchlistRankingResponse {
	resp := WatchlistRankingResponse{
		Name:    name,
		Entries: make([]WatchlistRankingEntryResponse, len(entries)),
	}
	for i, e := range entries {
		fields := make(map[string]float64, len(e.Fields))
		for _, f := range e.Fields {
			fields[f.Key] = f.Value
		}
		resp.Entries[i] = WatchlistRankingEntryResponse{
			Symbol: e.Symbol,
			Rank:   e.Rank,
			Fields: fields,
			Sector: e.Sector,
		}
	}
	return resp
}
