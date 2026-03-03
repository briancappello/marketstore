package io

import (
	"fmt"

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
