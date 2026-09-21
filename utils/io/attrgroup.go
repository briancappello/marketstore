package io

import (
	"errors"
	"fmt"
	"strings"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

// AttrGroupSchema represents the schema configuration for an attribute group.
type AttrGroupSchema struct {
	DataShapes []DataShape
	RecordType EnumRecordType
}

// AttrGroupTypeConfig is an interface that matches utils.AttrGroupConfig
// to avoid import cycles.
type AttrGroupTypeConfig interface {
	GetColumns() map[string]string
	GetRecordType() string
}

// GetAttrGroupSchema looks up an attrgroup name in the provided config map
// and returns the corresponding schema. Returns nil if not found.
func GetAttrGroupSchema(attrGroupName string, configTypes map[string]AttrGroupTypeConfig) *AttrGroupSchema {
	cfg, ok := configTypes[attrGroupName]
	if !ok {
		return nil
	}

	columns := cfg.GetColumns()
	dataShapes := make([]DataShape, 0, len(columns))
	for name, typeName := range columns {
		elemType := EnumElementTypeFromName(typeName)
		if elemType == NONE {
			// This should not happen if config was validated, but handle gracefully
			log.Warn("attrgroup %s: invalid type %q for column %s, skipping", attrGroupName, typeName, name)
			continue
		}
		dataShapes = append(dataShapes, DataShape{Name: name, Type: elemType})
	}

	recordType := EnumRecordTypeByName(cfg.GetRecordType())

	return &AttrGroupSchema{
		DataShapes: dataShapes,
		RecordType: recordType,
	}
}

// SchemaWithoutNanoseconds returns a copy of schema with any "Nanoseconds"
// column removed.
//
// Variable-length records store the sub-second offset inside the variable
// record index rather than as a standalone column, and writers strip a
// "Nanoseconds" column from incoming data. A configured schema that lists it
// must therefore not be treated as requiring it during a merge, or bucket
// creation fails with a spurious "missing required column".
func SchemaWithoutNanoseconds(schema *AttrGroupSchema) *AttrGroupSchema {
	if schema == nil {
		return nil
	}
	filtered := make([]DataShape, 0, len(schema.DataShapes))
	for _, ds := range schema.DataShapes {
		if strings.EqualFold(ds.Name, "Nanoseconds") {
			continue
		}
		filtered = append(filtered, ds)
	}
	return &AttrGroupSchema{DataShapes: filtered, RecordType: schema.RecordType}
}

// ResolveCreateSchema decides the column layout and record type for a bucket
// that is about to be created, given an optional configured attrgroup schema
// and an optional caller-supplied schema.
//
// This exists because bucket creation has three entry points -- implicit
// create-on-write in the executor, the JSON-RPC Create API, and the gRPC
// Create API -- which each used to carry their own copy of this decision. The
// copies drifted: only one of them stripped "Nanoseconds" for variable-length
// records, so the other two could persist a bucket header that no write could
// ever satisfy. Resolving in one place is what stops the next schema rule from
// drifting the same way.
//
// requestedRecordType is a tri-state on purpose. nil means the caller
// expressed no preference, in which case a configured record type wins. A
// non-nil value means the caller asked explicitly and is honoured. Callers
// that only have a boolean "is variable" should pass nil for false, which
// preserves the historical behaviour of letting config decide.
func ResolveCreateSchema(
	configSchema *AttrGroupSchema,
	requestShapes []DataShape,
	requestedRecordType *EnumRecordType,
	overrideSchema bool,
) (dsv []DataShape, recordType EnumRecordType, coercions map[string][2]EnumElementType, err error) {
	dsv, recordType, coercions, err = resolveShapes(
		configSchema, requestShapes, requestedRecordType, overrideSchema)
	if err != nil {
		return nil, recordType, nil, err
	}

	// Final, unconditional enforcement of the variable-length invariant,
	// applied to whichever branch produced the layout. The merge branch also
	// strips before merging, for a different reason (so config does not demand
	// a column the caller cannot supply); this catches every other source,
	// including a caller that passed Nanoseconds explicitly.
	//
	// NewTimeBucketInfo strips again as a last line of defence. Both layers are
	// deliberately independent: relying on a single downstream check is what
	// let the three creation paths drift apart in the first place.
	if recordType == VARIABLE {
		filtered := make([]DataShape, 0, len(dsv))
		for _, ds := range dsv {
			if strings.EqualFold(ds.Name, "Nanoseconds") {
				continue
			}
			filtered = append(filtered, ds)
		}
		dsv = filtered
	}

	return dsv, recordType, coercions, nil
}

func resolveShapes(
	configSchema *AttrGroupSchema,
	requestShapes []DataShape,
	requestedRecordType *EnumRecordType,
	overrideSchema bool,
) (dsv []DataShape, recordType EnumRecordType, coercions map[string][2]EnumElementType, err error) {
	recordType = FIXED
	if requestedRecordType != nil {
		recordType = *requestedRecordType
	}

	hasRequestSchema := len(requestShapes) > 0
	hasConfigSchema := configSchema != nil

	switch {
	case hasRequestSchema && (!hasConfigSchema || overrideSchema):
		// Caller's schema wins: either there is no config, or an override was
		// explicitly requested.
		return requestShapes, recordType, nil, nil

	case hasConfigSchema && !hasRequestSchema:
		// Config is the only source, so it dictates the record type too.
		return configSchema.DataShapes, configSchema.RecordType, nil, nil

	case hasConfigSchema && hasRequestSchema:
		// Merge: config types take precedence for columns it defines, the
		// caller may contribute extra columns.
		if requestedRecordType == nil {
			recordType = configSchema.RecordType
		} else if *requestedRecordType == FIXED && configSchema.RecordType == VARIABLE {
			recordType = VARIABLE
		}

		mergeSchema := configSchema
		if recordType == VARIABLE {
			mergeSchema = SchemaWithoutNanoseconds(configSchema)
		}
		if requestedRecordType != nil && *requestedRecordType == VARIABLE &&
			configSchema.RecordType == FIXED {
			log.Warn("attrgroup config specifies a fixed record type, but the caller requested variable")
		}

		merged, coerced, mergeErr := MergeSchemaWithInput(mergeSchema, requestShapes)
		if mergeErr != nil {
			return nil, recordType, nil, mergeErr
		}
		return merged, recordType, coerced, nil

	default:
		return nil, recordType, nil, ErrNoSchema
	}
}

// ErrNoSchema is returned by ResolveCreateSchema when neither the request nor
// the configuration supplies a column layout.
var ErrNoSchema = errors.New("no schema provided and no attrgroup config found")

// MergeSchemaWithInput takes a configured schema and input data shapes, returning
// a merged schema that uses configured types for known columns and inferred types
// for extra columns. It also validates type compatibility and returns coercion info.
//
// Returns:
// - mergedShapes: the final schema to use for bucket creation
// - coercions: map of column name -> (from type, to type) for logging
// - error: if types are incompatible
func MergeSchemaWithInput(
	configSchema *AttrGroupSchema,
	inputShapes []DataShape,
) (mergedShapes []DataShape, coercions map[string][2]EnumElementType, err error) {
	if configSchema == nil {
		// No config, use input as-is
		return inputShapes, nil, nil
	}

	// Build a map of configured columns
	configCols := make(map[string]EnumElementType)
	for _, ds := range configSchema.DataShapes {
		configCols[ds.Name] = ds.Type
	}

	// Build a map of input columns
	inputCols := make(map[string]EnumElementType)
	for _, ds := range inputShapes {
		inputCols[ds.Name] = ds.Type
	}

	mergedShapes = make([]DataShape, 0, len(inputShapes))
	coercions = make(map[string][2]EnumElementType)

	// Process input columns
	for _, inputDS := range inputShapes {
		if inputDS.Name == "Epoch" {
			// Epoch is always int64, skip
			mergedShapes = append(mergedShapes, inputDS)
			continue
		}

		configType, inConfig := configCols[inputDS.Name]
		if !inConfig {
			// Extra column not in config - use inferred type
			log.Debug("column %q not in attrgroup config, using inferred type %s", inputDS.Name, inputDS.Type.String())
			mergedShapes = append(mergedShapes, inputDS)
			continue
		}

		if inputDS.Type == configType {
			// Types match, use as-is
			mergedShapes = append(mergedShapes, inputDS)
			continue
		}

		// Types differ - check if coercion is possible
		if canCoerce(inputDS.Type, configType) {
			coercions[inputDS.Name] = [2]EnumElementType{inputDS.Type, configType}
			mergedShapes = append(mergedShapes, DataShape{Name: inputDS.Name, Type: configType})
		} else {
			return nil, nil, fmt.Errorf(
				"column %q: cannot coerce type %s to configured type %s",
				inputDS.Name, inputDS.Type.String(), configType.String(),
			)
		}
	}

	// Check for missing required columns from config
	for _, configDS := range configSchema.DataShapes {
		if _, found := inputCols[configDS.Name]; !found {
			return nil, nil, fmt.Errorf("missing required column %q from config", configDS.Name)
		}
	}

	return mergedShapes, coercions, nil
}

// canCoerce returns true if fromType can be safely coerced to toType.
func canCoerce(fromType, toType EnumElementType) bool {
	// Same type - trivially ok
	if fromType == toType {
		return true
	}

	// Float to float conversions (with possible precision loss)
	if isFloatType(fromType) && isFloatType(toType) {
		return true
	}

	// Int to int conversions (with possible overflow for narrowing)
	if isIntType(fromType) && isIntType(toType) {
		return true
	}

	// Uint to uint conversions
	if isUintType(fromType) && isUintType(toType) {
		return true
	}

	// Int to float is safe
	if isIntType(fromType) && isFloatType(toType) {
		return true
	}

	// Uint to float is safe
	if isUintType(fromType) && isFloatType(toType) {
		return true
	}

	// Int to uint or vice versa - allow with warning potential
	if (isIntType(fromType) && isUintType(toType)) || (isUintType(fromType) && isIntType(toType)) {
		return true
	}

	// Float to int is not allowed (would lose data)
	if isFloatType(fromType) && (isIntType(toType) || isUintType(toType)) {
		return false
	}

	return false
}

func isFloatType(t EnumElementType) bool {
	return t == FLOAT32 || t == FLOAT64
}

func isIntType(t EnumElementType) bool {
	return t == INT16 || t == INT32 || t == INT64 || t == BYTE
}

func isUintType(t EnumElementType) bool {
	return t == UINT8 || t == UINT16 || t == UINT32 || t == UINT64
}
