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
