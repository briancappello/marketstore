package frontend

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
)

// handleRESTHealth serves GET /v1/health.
//
// A cheap liveness/readiness probe for clients (e.g. a status indicator). It
// touches no catalog data: 200 {"status":"ok"} once the server is queryable,
// 503 while it is still starting — consistent with every other REST route.
//
// A replica whose live replication stream has stopped for good also reports
// 503. Its data is still readable, so queries keep working, but it is no
// longer tracking the master and must not advertise itself as healthy.
func (s *DataService) handleRESTHealth(w http.ResponseWriter, r *http.Request) {
	if !requireQueryable(w) {
		return
	}
	if ReplicationBroken() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "replication stopped",
			"detail": "the live replication stream has stopped; served data is no longer advancing",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleRESTSymbols serves GET /v1/symbols.
//
// Query parameters mirror ListSymbolsRequest: format ("symbol" or "tbk"),
// timeframe, and date.
func (s *DataService) handleRESTSymbols(w http.ResponseWriter, r *http.Request) {
	if !requireQueryable(w) {
		return
	}

	q := r.URL.Query()

	if q.Get("format") == "tbk" {
		writeJSON(w, http.StatusOK, ListSymbolsResponse{
			Results: catalog.ListTimeBucketKeyNames(s.catalogDir),
		})
		return
	}

	timeframe := q.Get("timeframe")
	var date *time.Time
	if raw := q.Get("date"); raw != "" {
		t, err := parseDate(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid date "+raw)
			return
		}
		date = &t
	}

	if timeframe != "" || date != nil {
		symbols, err := listSymbolsForDate(s.catalogDir, timeframe, date)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "list symbols: "+err.Error())
			return
		}
		writeJSON(w, http.StatusOK, ListSymbolsResponse{Results: symbols})
		return
	}

	ret, err := s.catalogDir.GatherCategoriesAndItems()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "gather catalog items: "+err.Error())
		return
	}
	symbols := make([]string, 0, len(ret["Symbol"]))
	for symbol := range ret["Symbol"] {
		symbols = append(symbols, symbol)
	}
	writeJSON(w, http.StatusOK, ListSymbolsResponse{Results: symbols})
}

// barsResponse is the row-oriented payload for GET /v1/bars/{symbol}.
type barsResponse struct {
	Symbol    string           `json:"symbol"`
	Timeframe string           `json:"timeframe"`
	Bars      []map[string]any `json:"bars"`
}

// handleRESTBars serves GET /v1/bars/{symbol}.
//
// Exactly one symbol is accepted. With neither start nor end supplied the
// response is the most recent `limit` bars, which needs no calendar
// arithmetic: LimitRecordCount with LimitFromStart false already means
// "the newest N records".
func (s *DataService) handleRESTBars(w http.ResponseWriter, r *http.Request) {
	if !requireQueryable(w) {
		return
	}

	symbol := strings.ToUpper(r.PathValue("symbol"))
	if err := validateSymbol(symbol); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	q := r.URL.Query()
	timeframe := resolveTimeframe(q.Get("timeframe"))

	limit, err := parseLimit(q.Get("limit"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	start, err := parseTimeBound(q.Get("start"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	end, err := parseTimeBound(q.Get("end"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Set before the query so every exit path (200, no-data 404) is cached
	// under the same rule: an absent symbol must not be cacheable when a
	// present one is not.
	setBarCacheHeaders(w, end, time.Now())

	req := &QueryRequest{
		Destination:      symbol + "/" + timeframe + "/" + attributeGroup,
		LimitRecordCount: &limit,
	}
	if !start.IsZero() {
		secs := start.Unix()
		req.EpochStart = &secs
	}
	if !end.IsZero() {
		secs := end.Unix()
		req.EpochEnd = &secs
	}

	csm, err := s.queryColumnSeries(req)
	if err != nil {
		// A missing symbol/timeframe surfaces as a "no results" query error,
		// which is a 404, not a client error. Everything else is a 400.
		if isNoDataErr(err) {
			writeError(w, http.StatusNotFound,
				"no data for "+symbol+"/"+timeframe+"/"+attributeGroup)
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The route is single-symbol, so there is at most one series.
	for _, cs := range csm {
		rows, rErr := columnSeriesToRows(cs)
		if rErr != nil {
			writeError(w, http.StatusInternalServerError, rErr.Error())
			return
		}
		if len(rows) == 0 {
			break
		}
		writeJSONCached(w, r, http.StatusOK, barsResponse{
			Symbol:    symbol,
			Timeframe: timeframe,
			Bars:      rows,
		})
		return
	}

	writeError(w, http.StatusNotFound,
		"no data for "+symbol+"/"+timeframe+"/"+attributeGroup)
}

// quotesResponse is the payload for GET /v1/quotes.
type quotesResponse struct {
	Quotes []map[string]any `json:"quotes"`
}

// quoteBarCount is how many bars a quote needs: the latest, plus the one
// before it to supply prev_close.
const quoteBarCount = 2

// handleRESTQuotes serves GET /v1/quotes.
//
// With no symbols parameter the request covers every symbol in the catalog.
// That is bounded because the per-symbol record count is fixed at two and
// cannot be raised by the caller.
func (s *DataService) handleRESTQuotes(w http.ResponseWriter, r *http.Request) {
	if !requireQueryable(w) {
		return
	}

	q := r.URL.Query()
	timeframe := resolveTimeframe(q.Get("timeframe"))

	symbolSpec := "*"
	if raw := q.Get("symbols"); raw != "" {
		parts := strings.Split(raw, ",")
		cleaned := make([]string, 0, len(parts))
		for _, p := range parts {
			p = strings.ToUpper(strings.TrimSpace(p))
			if p == "" {
				continue
			}
			// "*" is expressed by omitting the parameter; accepting it here
			// too would give two spellings for one behaviour.
			if strings.ContainsAny(p, "*/") {
				writeError(w, http.StatusBadRequest,
					"symbols must not contain '*' or '/'; omit the parameter for all symbols")
				return
			}
			cleaned = append(cleaned, p)
		}
		if len(cleaned) == 0 {
			writeError(w, http.StatusBadRequest, "symbols parameter is empty")
			return
		}
		symbolSpec = strings.Join(cleaned, ",")
	}

	limit := quoteBarCount
	req := &QueryRequest{
		Destination:      symbolSpec + "/" + timeframe + "/" + attributeGroup,
		LimitRecordCount: &limit,
	}

	csm, err := s.queryColumnSeries(req)
	if err != nil {
		// No data for the requested symbols is a normal empty result, not a
		// client error: return an empty list. Everything else is a 400.
		if isNoDataErr(err) {
			w.Header().Set("Cache-Control",
				fmt.Sprintf("public, max-age=%d", quotesCacheSeconds))
			writeJSONCached(w, r, http.StatusOK, quotesResponse{Quotes: []map[string]any{}})
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Always a list, never null, so clients need no empty-case branch.
	quotes := make([]map[string]any, 0, len(csm))
	for tbk, cs := range csm {
		rows, rErr := columnSeriesToRows(cs)
		if rErr != nil {
			writeError(w, http.StatusInternalServerError, rErr.Error())
			return
		}
		symbol := tbk.GetItemInCategory("Symbol")
		if quote := quoteFromRows(symbol, rows); quote != nil {
			quotes = append(quotes, quote)
		}
	}

	sort.Slice(quotes, func(i, j int) bool {
		si, _ := quotes[i]["symbol"].(string)
		sj, _ := quotes[j]["symbol"].(string)
		return si < sj
	})

	w.Header().Set("Cache-Control",
		fmt.Sprintf("public, max-age=%d", quotesCacheSeconds))
	writeJSONCached(w, r, http.StatusOK, quotesResponse{Quotes: quotes})
}

// watchlistQueryFrom reads the optional session and as_of query parameters.
func watchlistQueryFrom(r *http.Request) WatchlistQuery {
	q := r.URL.Query()
	return WatchlistQuery{Session: q.Get("session"), AsOf: q.Get("as_of")}
}

// writeWatchlistError maps a watchlist query error to its HTTP status: 400
// for an invalid or unavailable request, 404 for an unknown watchlist.
func writeWatchlistError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrWatchlistNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrWatchlistInvalid):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

// handleRESTWatchlists serves GET /v1/watchlists[?session=&as_of=].
//
// The watchlist plugin is optional; with no provider registered this returns
// an empty list rather than an error, matching DataService.ListWatchlists.
// Only the lists available in the resolved session are returned.
func (s *DataService) handleRESTWatchlists(w http.ResponseWriter, r *http.Request) {
	if !requireQueryable(w) {
		return
	}
	lists, err := queryWatchlists(watchlistQueryFrom(r))
	if err != nil {
		writeWatchlistError(w, err)
		return
	}
	out := make([]WatchlistRankingResponse, len(lists))
	for i, l := range lists {
		out[i] = convertList(l)
	}
	writeJSON(w, http.StatusOK, ListWatchlistsResponse{Watchlists: out})
}

// handleRESTWatchlist serves GET /v1/watchlists/{name}[?session=&as_of=].
//
// Unlike the collection endpoint, an unknown name is a 404: the caller named
// a specific resource that does not exist. A known list with no entries is
// a 200 with an empty list.
func (s *DataService) handleRESTWatchlist(w http.ResponseWriter, r *http.Request) {
	if !requireQueryable(w) {
		return
	}

	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, "watchlist name is required")
		return
	}
	if GetWatchlistProvider() == nil {
		writeError(w, http.StatusNotFound, "no such watchlist: "+name)
		return
	}

	q := watchlistQueryFrom(r)
	q.Names = []string{name}
	lists, err := queryWatchlists(q)
	if err != nil {
		writeWatchlistError(w, err)
		return
	}
	if len(lists) == 0 {
		writeError(w, http.StatusNotFound, "no such watchlist: "+name)
		return
	}
	writeJSON(w, http.StatusOK, convertList(lists[0]))
}
