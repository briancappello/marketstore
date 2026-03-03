package io

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAttrGroupConfig implements AttrGroupTypeConfig for testing.
type mockAttrGroupConfig struct {
	columns    map[string]string
	recordType string
}

func (m *mockAttrGroupConfig) GetColumns() map[string]string {
	return m.columns
}

func (m *mockAttrGroupConfig) GetRecordType() string {
	return m.recordType
}

func TestGetAttrGroupSchema(t *testing.T) {
	t.Parallel()

	configTypes := map[string]AttrGroupTypeConfig{
		"OHLCV": &mockAttrGroupConfig{
			columns: map[string]string{
				"Open":   "float32",
				"High":   "float32",
				"Low":    "float32",
				"Close":  "float32",
				"Volume": "int64",
			},
			recordType: "fixed",
		},
		"TICK": &mockAttrGroupConfig{
			columns: map[string]string{
				"Price": "float64",
				"Size":  "uint64",
			},
			recordType: "variable",
		},
	}

	t.Run("found OHLCV", func(t *testing.T) {
		schema := GetAttrGroupSchema("OHLCV", configTypes)
		require.NotNil(t, schema)
		assert.Len(t, schema.DataShapes, 5)
		assert.Equal(t, FIXED, schema.RecordType)

		// Check that columns are present (order may vary due to map iteration)
		names := make(map[string]EnumElementType)
		for _, ds := range schema.DataShapes {
			names[ds.Name] = ds.Type
		}
		assert.Equal(t, FLOAT32, names["Open"])
		assert.Equal(t, INT64, names["Volume"])
	})

	t.Run("found TICK variable", func(t *testing.T) {
		schema := GetAttrGroupSchema("TICK", configTypes)
		require.NotNil(t, schema)
		assert.Len(t, schema.DataShapes, 2)
		assert.Equal(t, VARIABLE, schema.RecordType)
	})

	t.Run("not found", func(t *testing.T) {
		schema := GetAttrGroupSchema("UNKNOWN", configTypes)
		assert.Nil(t, schema)
	})
}

func TestMergeSchemaWithInput_NoConfig(t *testing.T) {
	t.Parallel()

	inputShapes := []DataShape{
		{Name: "Epoch", Type: INT64},
		{Name: "Open", Type: FLOAT64},
		{Name: "Close", Type: FLOAT64},
	}

	merged, coercions, err := MergeSchemaWithInput(nil, inputShapes)
	require.NoError(t, err)
	assert.Equal(t, inputShapes, merged)
	assert.Nil(t, coercions)
}

func TestMergeSchemaWithInput_ExactMatch(t *testing.T) {
	t.Parallel()

	configSchema := &AttrGroupSchema{
		DataShapes: []DataShape{
			{Name: "Open", Type: FLOAT32},
			{Name: "Close", Type: FLOAT32},
		},
		RecordType: FIXED,
	}

	inputShapes := []DataShape{
		{Name: "Epoch", Type: INT64},
		{Name: "Open", Type: FLOAT32},
		{Name: "Close", Type: FLOAT32},
	}

	merged, coercions, err := MergeSchemaWithInput(configSchema, inputShapes)
	require.NoError(t, err)
	assert.Len(t, merged, 3)
	assert.Empty(t, coercions)
}

func TestMergeSchemaWithInput_TypeCoercion(t *testing.T) {
	t.Parallel()

	configSchema := &AttrGroupSchema{
		DataShapes: []DataShape{
			{Name: "Price", Type: FLOAT32}, // config wants float32
		},
		RecordType: FIXED,
	}

	inputShapes := []DataShape{
		{Name: "Epoch", Type: INT64},
		{Name: "Price", Type: FLOAT64}, // input has float64
	}

	merged, coercions, err := MergeSchemaWithInput(configSchema, inputShapes)
	require.NoError(t, err)
	assert.Len(t, merged, 2)

	// Check that Price was coerced to config type
	var priceDS *DataShape
	for i := range merged {
		if merged[i].Name == "Price" {
			priceDS = &merged[i]
			break
		}
	}
	require.NotNil(t, priceDS)
	assert.Equal(t, FLOAT32, priceDS.Type)

	// Check coercion was recorded
	assert.Contains(t, coercions, "Price")
	assert.Equal(t, FLOAT64, coercions["Price"][0]) // from
	assert.Equal(t, FLOAT32, coercions["Price"][1]) // to
}

func TestMergeSchemaWithInput_ExtraColumns(t *testing.T) {
	t.Parallel()

	configSchema := &AttrGroupSchema{
		DataShapes: []DataShape{
			{Name: "Open", Type: FLOAT32},
		},
		RecordType: FIXED,
	}

	inputShapes := []DataShape{
		{Name: "Epoch", Type: INT64},
		{Name: "Open", Type: FLOAT32},
		{Name: "ExtraCol", Type: INT32}, // not in config
	}

	merged, coercions, err := MergeSchemaWithInput(configSchema, inputShapes)
	require.NoError(t, err)
	assert.Len(t, merged, 3)
	assert.Empty(t, coercions)

	// ExtraCol should be kept with inferred type
	var extraDS *DataShape
	for i := range merged {
		if merged[i].Name == "ExtraCol" {
			extraDS = &merged[i]
			break
		}
	}
	require.NotNil(t, extraDS)
	assert.Equal(t, INT32, extraDS.Type)
}

func TestMergeSchemaWithInput_MissingRequired(t *testing.T) {
	t.Parallel()

	configSchema := &AttrGroupSchema{
		DataShapes: []DataShape{
			{Name: "Open", Type: FLOAT32},
			{Name: "Close", Type: FLOAT32},
		},
		RecordType: FIXED,
	}

	inputShapes := []DataShape{
		{Name: "Epoch", Type: INT64},
		{Name: "Open", Type: FLOAT32},
		// Missing "Close"
	}

	_, _, err := MergeSchemaWithInput(configSchema, inputShapes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing required column")
	assert.Contains(t, err.Error(), "Close")
}

func TestMergeSchemaWithInput_IncompatibleTypes(t *testing.T) {
	t.Parallel()

	configSchema := &AttrGroupSchema{
		DataShapes: []DataShape{
			{Name: "Price", Type: INT64}, // config wants int
		},
		RecordType: FIXED,
	}

	inputShapes := []DataShape{
		{Name: "Epoch", Type: INT64},
		{Name: "Price", Type: FLOAT64}, // input has float - can't convert to int
	}

	_, _, err := MergeSchemaWithInput(configSchema, inputShapes)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot coerce")
}

func TestCanCoerce(t *testing.T) {
	t.Parallel()

	tests := []struct {
		from     EnumElementType
		to       EnumElementType
		expected bool
	}{
		// Same type
		{FLOAT32, FLOAT32, true},
		{INT64, INT64, true},

		// Float to float
		{FLOAT64, FLOAT32, true},
		{FLOAT32, FLOAT64, true},

		// Int to int
		{INT64, INT32, true},
		{INT32, INT64, true},
		{INT16, INT32, true},

		// Uint to uint
		{UINT64, UINT32, true},
		{UINT32, UINT64, true},

		// Int to float (safe)
		{INT32, FLOAT32, true},
		{INT64, FLOAT64, true},

		// Uint to float (safe)
		{UINT32, FLOAT32, true},
		{UINT64, FLOAT64, true},

		// Int to uint (allowed with warning potential)
		{INT32, UINT32, true},
		{INT64, UINT64, true},

		// Float to int (not allowed)
		{FLOAT32, INT32, false},
		{FLOAT64, INT64, false},

		// Float to uint (not allowed)
		{FLOAT32, UINT32, false},
		{FLOAT64, UINT64, false},
	}

	for _, tt := range tests {
		t.Run(tt.from.String()+"->"+tt.to.String(), func(t *testing.T) {
			result := canCoerce(tt.from, tt.to)
			assert.Equal(t, tt.expected, result)
		})
	}
}
