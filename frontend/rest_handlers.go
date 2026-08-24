package frontend

import (
	"net/http"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/catalog"
)

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
		writeJSON(w, http.StatusOK, barsResponse{
			Symbol:    symbol,
			Timeframe: timeframe,
			Bars:      rows,
		})
		return
	}

	writeError(w, http.StatusNotFound,
		"no data for "+symbol+"/"+timeframe+"/"+attributeGroup)
}
