package frontend

import (
	"net/http"
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
