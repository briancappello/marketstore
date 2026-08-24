package frontend

import (
	"reflect"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// columnSeriesToRows converts a column-oriented ColumnSeries into
// row-oriented maps suitable for JSON.
//
// The Epoch column renders as an RFC3339 string under the key "time"; every
// other column renders under its lowercased name. Rendering is dynamic
// rather than a fixed struct because the column set comes from the store.
func columnSeriesToRows(cs *io.ColumnSeries) ([]map[string]any, error) {
	n := cs.Len()
	if n == 0 {
		return []map[string]any{}, nil
	}

	times, err := cs.GetTime()
	if err != nil {
		return nil, err
	}

	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"time": times[i].UTC().Format(time.RFC3339),
		}
	}

	for _, name := range cs.GetColumnNames() {
		// Both are already represented by "time".
		if name == "Epoch" || name == "Nanoseconds" {
			continue
		}
		col := reflect.ValueOf(cs.GetColumn(name))
		if col.Kind() != reflect.Slice {
			continue
		}
		key := strings.ToLower(name)
		for i := 0; i < n && i < col.Len(); i++ {
			rows[i][key] = col.Index(i).Interface()
		}
	}

	return rows, nil
}

// quoteFromRows reduces a symbol's bars to a flat quote object: the latest
// bar's fields, plus the symbol and the previous close.
//
// Returns nil when there are no bars, so the caller can omit the symbol
// rather than failing the whole batch. A symbol with no data is a normal
// condition across a catalog-wide request, not an error.
//
// A plain map is returned rather than a struct with a custom MarshalJSON.
// The OHLCV column set is dynamic so a fixed struct will not do, and a
// json.Marshaler implementation would be passed through the wscodec
// sanitizer untouched, silently reintroducing the NaN failure that
// sanitizer exists to prevent.
func quoteFromRows(symbol string, rows []map[string]any) map[string]any {
	if len(rows) == 0 {
		return nil
	}

	last := rows[len(rows)-1]

	quote := make(map[string]any, len(last)+2)
	for k, v := range last {
		quote[k] = v
	}
	quote["symbol"] = symbol

	// Always present, so clients need no key-existence check. Stays nil
	// (encoding as null) when only one bar is available.
	quote["prev_close"] = nil
	if len(rows) >= 2 {
		if c, ok := rows[len(rows)-2]["close"]; ok {
			quote["prev_close"] = c
		}
	}

	return quote
}
