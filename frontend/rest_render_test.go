package frontend

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

func newTestCS(t *testing.T) *io.ColumnSeries {
	t.Helper()
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{0, 60})
	cs.AddColumn("Open", []float32{1.5, float32(math.NaN())})
	cs.AddColumn("Volume", []int64{100, 200})
	return cs
}

func TestColumnSeriesToRows(t *testing.T) {
	rows, err := columnSeriesToRows(newTestCS(t))
	assert.Nil(t, err)
	assert.Len(t, rows, 2)

	// Epoch renders as RFC3339 under "time"; it must not also appear raw.
	assert.Equal(t, "1970-01-01T00:00:00Z", rows[0]["time"])
	assert.NotContains(t, rows[0], "Epoch")
	assert.NotContains(t, rows[0], "epoch")

	// Other columns render lowercased.
	assert.Equal(t, float32(1.5), rows[0]["open"])
	assert.Equal(t, int64(100), rows[0]["volume"])
	assert.NotContains(t, rows[0], "Open")
}

func TestColumnSeriesToRowsEmpty(t *testing.T) {
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{})
	cs.AddColumn("Open", []float32{})
	rows, err := columnSeriesToRows(cs)
	assert.Nil(t, err)
	assert.Empty(t, rows)
}

func TestParseLimit(t *testing.T) {
	n, err := parseLimit("")
	assert.Nil(t, err)
	assert.Equal(t, 1500, n)

	n, err = parseLimit("42")
	assert.Nil(t, err)
	assert.Equal(t, 42, n)

	n, err = parseLimit("10000")
	assert.Nil(t, err)
	assert.Equal(t, 10000, n)

	// Above the ceiling is an error, not a clamp.
	_, err = parseLimit("10001")
	assert.NotNil(t, err)

	_, err = parseLimit("0")
	assert.NotNil(t, err)

	_, err = parseLimit("abc")
	assert.NotNil(t, err)
}

func TestParseTimeBound(t *testing.T) {
	zero, err := parseTimeBound("")
	assert.Nil(t, err)
	assert.True(t, zero.IsZero())

	// All digits means unix epoch seconds.
	ts, err := parseTimeBound("1705276800")
	assert.Nil(t, err)
	assert.Equal(t, int64(1705276800), ts.Unix())

	// Anything else is RFC3339.
	ts, err = parseTimeBound("2024-01-15T00:00:00Z")
	assert.Nil(t, err)
	assert.Equal(t, int64(1705276800), ts.Unix())

	_, err = parseTimeBound("2024-01-15")
	assert.NotNil(t, err)

	_, err = parseTimeBound("not-a-time")
	assert.NotNil(t, err)
}

func TestValidateSymbol(t *testing.T) {
	assert.Nil(t, validateSymbol("AAPL"))

	// Commas expand to multi-symbol queries and '*' to the whole catalog;
	// both would multiply the record limit by an unbounded symbol count.
	assert.NotNil(t, validateSymbol("AAPL,MSFT"))
	assert.NotNil(t, validateSymbol("*"))
	assert.NotNil(t, validateSymbol("AAPL/1Min/OHLCV"))
	assert.NotNil(t, validateSymbol(""))
}
