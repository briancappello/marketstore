package io_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

func shapes(pairs ...any) []io.DataShape {
	out := make([]io.DataShape, 0, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		name, ok := pairs[i].(string)
		if !ok {
			panic("shapes: name must be a string")
		}
		typ, ok := pairs[i+1].(io.EnumElementType)
		if !ok {
			panic("shapes: type must be an EnumElementType")
		}
		out = append(out, io.DataShape{Name: name, Type: typ})
	}
	return out
}

func names(dsv []io.DataShape) []string {
	out := make([]string, 0, len(dsv))
	for _, ds := range dsv {
		out = append(out, ds.Name)
	}
	return out
}

func recordTypePtr(rt io.EnumRecordType) *io.EnumRecordType { return &rt }

func TestResolveCreateSchema(t *testing.T) {
	t.Parallel()

	variableQuoteConfig := &io.AttrGroupSchema{
		DataShapes: shapes("AskPrice", io.FLOAT64, "BidPrice", io.FLOAT64),
		RecordType: io.VARIABLE,
	}
	// A FIXED config that legitimately declares Nanoseconds as a real column.
	fixedTickConfig := &io.AttrGroupSchema{
		DataShapes: shapes("Price", io.FLOAT64, "Nanoseconds", io.INT32),
		RecordType: io.FIXED,
	}

	tests := []struct {
		name           string
		config         *io.AttrGroupSchema
		request        []io.DataShape
		requested      *io.EnumRecordType
		override       bool
		wantNames      []string
		wantRecordType io.EnumRecordType
		wantErr        error
	}{
		{
			name:           "request only, no config",
			request:        shapes("Epoch", io.INT64, "Price", io.FLOAT64),
			requested:      recordTypePtr(io.VARIABLE),
			wantNames:      []string{"Epoch", "Price"},
			wantRecordType: io.VARIABLE,
		},
		{
			name:           "config only dictates record type",
			config:         variableQuoteConfig,
			wantNames:      []string{"AskPrice", "BidPrice"},
			wantRecordType: io.VARIABLE,
		},
		{
			name:           "override ignores config entirely",
			config:         variableQuoteConfig,
			request:        shapes("Epoch", io.INT64, "Only", io.FLOAT32),
			override:       true,
			wantNames:      []string{"Epoch", "Only"},
			wantRecordType: io.FIXED,
		},
		{
			// No caller preference, so a VARIABLE config promotes the bucket.
			name:           "unspecified record type defers to config",
			config:         variableQuoteConfig,
			request:        shapes("Epoch", io.INT64, "AskPrice", io.FLOAT64, "BidPrice", io.FLOAT64),
			requested:      nil,
			wantNames:      []string{"Epoch", "AskPrice", "BidPrice"},
			wantRecordType: io.VARIABLE,
		},
		{
			// THE REGRESSION. A FIXED config declaring Nanoseconds, merged for a
			// caller that asked for VARIABLE. Nanoseconds must be dropped from
			// the merge, otherwise MergeSchemaWithInput reports it as a missing
			// required column and bucket creation fails.
			name:           "variable request drops Nanoseconds from a fixed config merge",
			config:         fixedTickConfig,
			request:        shapes("Epoch", io.INT64, "Price", io.FLOAT64),
			requested:      recordTypePtr(io.VARIABLE),
			wantNames:      []string{"Epoch", "Price"},
			wantRecordType: io.VARIABLE,
		},
		{
			// The opposite direction: a FIXED bucket keeps Nanoseconds.
			name:           "fixed request keeps Nanoseconds from config merge",
			config:         fixedTickConfig,
			request:        shapes("Epoch", io.INT64, "Price", io.FLOAT64, "Nanoseconds", io.INT32),
			requested:      nil,
			wantNames:      []string{"Epoch", "Price", "Nanoseconds"},
			wantRecordType: io.FIXED,
		},
		{
			name:    "no schema from either source",
			wantErr: io.ErrNoSchema,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dsv, rt, _, err := io.ResolveCreateSchema(tt.config, tt.request, tt.requested, tt.override)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tt.wantErr), "got %v, want %v", err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantNames, names(dsv))
			assert.Equal(t, tt.wantRecordType, rt)
		})
	}
}

// TestResolveCreateSchema_NeverYieldsVariableWithNanoseconds is the property
// that the incident turned on: whatever combination of configured schema,
// caller schema and requested record type comes in, a VARIABLE result must
// never carry a Nanoseconds column into bucket creation.
func TestResolveCreateSchema_NeverYieldsVariableWithNanoseconds(t *testing.T) {
	t.Parallel()

	configs := map[string]*io.AttrGroupSchema{
		"nil": nil,
		"variable config": {
			DataShapes: shapes("Price", io.FLOAT64),
			RecordType: io.VARIABLE,
		},
		"fixed config with Nanoseconds": {
			DataShapes: shapes("Price", io.FLOAT64, "Nanoseconds", io.INT32),
			RecordType: io.FIXED,
		},
		"variable config with Nanoseconds": {
			// Config validation rejects this at load, but the resolver must
			// not depend on that to stay correct.
			DataShapes: shapes("Price", io.FLOAT64, "Nanoseconds", io.INT32),
			RecordType: io.VARIABLE,
		},
	}
	requests := map[string][]io.DataShape{
		"empty":            nil,
		"without":          shapes("Epoch", io.INT64, "Price", io.FLOAT64),
		"with Nanoseconds": shapes("Epoch", io.INT64, "Price", io.FLOAT64, "Nanoseconds", io.INT32),
	}
	requestedTypes := map[string]*io.EnumRecordType{
		"unspecified": nil,
		"fixed":       recordTypePtr(io.FIXED),
		"variable":    recordTypePtr(io.VARIABLE),
	}

	for cName, cfg := range configs {
		for rName, req := range requests {
			for tName, rt := range requestedTypes {
				for _, override := range []bool{false, true} {
					dsv, got, _, err := io.ResolveCreateSchema(cfg, req, rt, override)
					if err != nil {
						continue // ErrNoSchema or a merge failure: nothing is created
					}
					if got != io.VARIABLE {
						continue
					}
					for _, ds := range dsv {
						assert.NotEqualf(t, "Nanoseconds", ds.Name,
							"config=%s request=%s requested=%s override=%v resolved to a VARIABLE "+
								"bucket still carrying a Nanoseconds column",
							cName, rName, tName, override)
					}
				}
			}
		}
	}
}
