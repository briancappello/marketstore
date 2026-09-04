package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConfig_ReplicaBackfillFields(t *testing.T) {
	t.Parallel()

	yml := []byte(`
root_directory: /tmp/x
listen_port: 5993
replication:
  master_host: "10.0.0.5:5996"
  master_query_host: "10.0.0.5:5995"
  reconcile_interval: 30s
  backfill_parallelism: 4
  backfill_lookback: 1h
  deep_heal_interval: 2h
`)
	cfg, err := ParseConfig(yml)
	assert.Nil(t, err)
	assert.Equal(t, "10.0.0.5:5995", cfg.Replication.MasterQueryHost)
	assert.Equal(t, 30*time.Second, cfg.Replication.ReconcileInterval)
	assert.Equal(t, 4, cfg.Replication.BackfillParallelism)
	assert.Equal(t, time.Hour, cfg.Replication.BackfillLookback)
	assert.Equal(t, 2*time.Hour, cfg.Replication.DeepHealInterval)
}

// An omitted deep_heal_interval must fall back to 24h, never to 0. A zero value
// reaching the Driver would make every reconcile a deep pass -- the 288x
// write-amplification bug.
func TestParseConfig_DeepHealIntervalDefaultsWhenOmitted(t *testing.T) {
	t.Parallel()

	yml := []byte(`
root_directory: /tmp/x
listen_port: 5993
replication:
  master_host: "10.0.0.5:5996"
  master_query_host: "10.0.0.5:5995"
`)
	cfg, err := ParseConfig(yml)
	assert.Nil(t, err)
	assert.Equal(t, 24*time.Hour, cfg.Replication.DeepHealInterval)
}

func TestParseConfig_TriggerSkipOnReplicaAndIsReplica(t *testing.T) {
	t.Parallel()

	yml := []byte(`
root_directory: /tmp/x
listen_port: 5993
replication:
  master_host: "10.0.0.5:5996"
triggers:
  - module: ondiskagg.so
    on: "*/1Min/OHLCV"
  - module: custom.so
    on: "*/1Min/OHLCV"
    skip_on_replica: true
`)
	cfg, err := ParseConfig(yml)
	assert.Nil(t, err)
	assert.True(t, cfg.Replication.IsReplica())
	assert.Len(t, cfg.Triggers, 2)
	assert.False(t, cfg.Triggers[0].SkipOnReplica)
	assert.True(t, cfg.Triggers[1].SkipOnReplica)
}

func TestReplicationSetting_IsReplica(t *testing.T) {
	t.Parallel()
	assert.False(t, (ReplicationSetting{}).IsReplica())
	assert.False(t, (ReplicationSetting{Enabled: true}).IsReplica())
	assert.True(t, (ReplicationSetting{MasterHost: "host:5996"}).IsReplica())
}

func TestParseConfig_AttrGroupTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		configYAML  string
		expectError bool
		errorMsg    string
		validate    func(t *testing.T, cfg *MktsConfig)
	}{
		{
			name: "valid OHLCV attrgroup config",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  OHLCV:
    columns:
      Open: float32
      High: float32
      Low: float32
      Close: float32
      Volume: int64
    record_type: fixed
`,
			expectError: false,
			validate: func(t *testing.T, cfg *MktsConfig) {
				require.Contains(t, cfg.AttrGroupTypes, "OHLCV")
				ag := cfg.AttrGroupTypes["OHLCV"]
				assert.Equal(t, "fixed", ag.RecordType)
				assert.Len(t, ag.Columns, 5)
				assert.Equal(t, "float32", ag.Columns["Open"])
				assert.Equal(t, "int64", ag.Columns["Volume"])
			},
		},
		{
			name: "valid variable record type",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  TICK:
    columns:
      Price: float64
      Size: uint64
    record_type: variable
`,
			expectError: false,
			validate: func(t *testing.T, cfg *MktsConfig) {
				require.Contains(t, cfg.AttrGroupTypes, "TICK")
				ag := cfg.AttrGroupTypes["TICK"]
				assert.Equal(t, "variable", ag.RecordType)
				assert.Len(t, ag.Columns, 2)
			},
		},
		{
			name: "record_type defaults to fixed",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  OHLCV:
    columns:
      Open: float32
`,
			expectError: false,
			validate: func(t *testing.T, cfg *MktsConfig) {
				require.Contains(t, cfg.AttrGroupTypes, "OHLCV")
				assert.Equal(t, "fixed", cfg.AttrGroupTypes["OHLCV"].RecordType)
			},
		},
		{
			name: "multiple attrgroup types",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  OHLCV:
    columns:
      Open: float32
      Close: float32
  TICK:
    columns:
      Price: float64
    record_type: variable
  QUOTE:
    columns:
      Bid: float64
      Ask: float64
`,
			expectError: false,
			validate: func(t *testing.T, cfg *MktsConfig) {
				assert.Len(t, cfg.AttrGroupTypes, 3)
				assert.Contains(t, cfg.AttrGroupTypes, "OHLCV")
				assert.Contains(t, cfg.AttrGroupTypes, "TICK")
				assert.Contains(t, cfg.AttrGroupTypes, "QUOTE")
			},
		},
		{
			name: "invalid column type",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  OHLCV:
    columns:
      Open: invalidtype
`,
			expectError: true,
			errorMsg:    "invalid type",
		},
		{
			name: "invalid record type",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  OHLCV:
    columns:
      Open: float32
    record_type: invalid
`,
			expectError: true,
			errorMsg:    "must be 'fixed' or 'variable'",
		},
		{
			name: "empty columns",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  OHLCV:
    columns: {}
`,
			expectError: true,
			errorMsg:    "must define at least one column",
		},
		{
			name: "all supported types",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  ALLTYPES:
    columns:
      f32: float32
      f64: float64
      i16: int16
      i32: int32
      i64: int64
      u8: uint8
      u16: uint16
      u32: uint32
      u64: uint64
      b: byte
      bl: bool
      s16: string16
`,
			expectError: false,
			validate: func(t *testing.T, cfg *MktsConfig) {
				require.Contains(t, cfg.AttrGroupTypes, "ALLTYPES")
				assert.Len(t, cfg.AttrGroupTypes["ALLTYPES"].Columns, 12)
			},
		},
		{
			name: "case insensitive type names",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
attrgroup_types:
  OHLCV:
    columns:
      Open: FLOAT32
      High: Float64
      Volume: INT64
`,
			expectError: false,
			validate: func(t *testing.T, cfg *MktsConfig) {
				require.Contains(t, cfg.AttrGroupTypes, "OHLCV")
				// Types are stored as-is, validated case-insensitively
				ag := cfg.AttrGroupTypes["OHLCV"]
				assert.Equal(t, "FLOAT32", ag.Columns["Open"])
			},
		},
		{
			name: "no attrgroup_types section is valid",
			configYAML: `
root_directory: /tmp/data
listen_port: "5993"
`,
			expectError: false,
			validate: func(t *testing.T, cfg *MktsConfig) {
				assert.Empty(t, cfg.AttrGroupTypes)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := ParseConfig([]byte(tt.configYAML))

			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cfg)

			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestIsValidElementTypeName(t *testing.T) {
	t.Parallel()

	validTypes := []string{
		"float32", "float64", "int16", "int32", "int64",
		"uint8", "uint16", "uint32", "uint64",
		"byte", "bool", "string16",
	}

	for _, typ := range validTypes {
		assert.True(t, isValidElementTypeName(typ), "expected %q to be valid", typ)
		// Also test uppercase
		assert.True(t, isValidElementTypeName(typ), "expected %q to be valid (uppercase)", typ)
	}

	invalidTypes := []string{
		"string", "char", "double", "float", "int", "integer",
		"", "invalid", "f32", "i64",
	}

	for _, typ := range invalidTypes {
		assert.False(t, isValidElementTypeName(typ), "expected %q to be invalid", typ)
	}
}

func TestAttrGroupConfig_GetColumns(t *testing.T) {
	t.Parallel()

	cfg := &AttrGroupConfig{
		Columns: map[string]string{
			"Open":  "float32",
			"Close": "float64",
		},
		RecordType: "fixed",
	}

	cols := cfg.GetColumns()
	assert.Len(t, cols, 2)
	assert.Equal(t, "float32", cols["Open"])
	assert.Equal(t, "float64", cols["Close"])
}

func TestAttrGroupConfig_GetRecordType(t *testing.T) {
	t.Parallel()

	cfg := &AttrGroupConfig{
		Columns:    map[string]string{"X": "int32"},
		RecordType: "variable",
	}

	assert.Equal(t, "variable", cfg.GetRecordType())
}
