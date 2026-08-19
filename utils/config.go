package utils

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/alpacahq/marketstore/v4/utils/log"
)

var InstanceConfig MktsConfig

func init() {
	InstanceConfig.Timezone = time.UTC
}

type ReplicationSetting struct {
	Enabled           bool
	TLSEnabled        bool
	CertFile          string
	KeyFile           string
	ListenPort        int
	MasterHost        string
	RetryInterval     time.Duration
	RetryBackoffCoeff int
	// MasterQueryHost is the master's MAIN gRPC address (e.g. "10.0.0.5:5995")
	// used by the replica's backfill client. Distinct from MasterHost (the
	// replication stream port). Empty disables backfill (live-only).
	MasterQueryHost string
	// ReconcileInterval is how often the replica re-pulls [watermark, now] for
	// every bucket to heal any gaps the best-effort live stream missed.
	ReconcileInterval time.Duration
	// BackfillParallelism bounds concurrent per-bucket backfill queries.
	BackfillParallelism int
	// BackfillLookback is the trailing window re-pulled on every reconcile,
	// healing master-side corrections to recent epochs that were missed while
	// disconnected. Re-pulling held data is harmless (idempotent by epoch).
	BackfillLookback time.Duration
}

type TriggerSetting struct {
	Module string
	On     string
	Config map[string]interface{}
}

type BgWorkerSetting struct {
	Module string
	Name   string
	Config map[string]interface{}
}

// AttrGroupConfig defines the schema template for an attribute group.
// When a bucket is created with a matching attrgroup name, this schema
// is used as the default. Columns defined here will use the configured types,
// while extra columns from feeders will use their inferred types.
type AttrGroupConfig struct {
	Columns    map[string]string // column name -> type string (e.g., "float32", "int64")
	RecordType string            // "fixed" or "variable", defaults to "fixed"
}

// GetColumns returns the column name to type mapping.
func (c *AttrGroupConfig) GetColumns() map[string]string {
	return c.Columns
}

// GetRecordType returns the record type string.
func (c *AttrGroupConfig) GetRecordType() string {
	return c.RecordType
}

type MktsConfig struct {
	// RootDirectory is the absolute path to the data directory
	RootDirectory              string
	ListenURL                  string
	GRPCListenURL              string
	GRPCMaxSendMsgSize         int // in bytes
	GRPCMaxRecvMsgSize         int // in bytes
	UtilitiesURL               string
	Timezone                   *time.Location
	StopGracePeriod            time.Duration
	WALRotateInterval          int
	DisableVariableCompression bool
	InitCatalog                bool
	InitWALCache               bool
	BackgroundSync             bool
	WALBypass                  bool
	// NoBackfill disables automatic backfill on startup for background workers
	// like massive. Set via --no-backfill CLI flag.
	NoBackfill     bool
	StartTime      time.Time
	Replication    ReplicationSetting
	Triggers       []*TriggerSetting
	BgWorkers      []*BgWorkerSetting
	AttrGroupTypes map[string]*AttrGroupConfig // attrgroup name -> schema config
}

const (
	// 2^20 = 1048576.
	megabyteToByte                     = 1 << 20
	defaultReplicationMasterListenPort = 5996
	defaultWALRotateInterval           = 5 // * DiskRefreshInterval
)

func NewDefaultConfig(rootDir string) *MktsConfig {
	return &MktsConfig{
		RootDirectory:              rootDir,
		ListenURL:                  "",
		GRPCListenURL:              "",
		GRPCMaxSendMsgSize:         1024 * megabyteToByte, // 1024MB
		GRPCMaxRecvMsgSize:         1024 * megabyteToByte, // 1024MB
		UtilitiesURL:               "",
		Timezone:                   time.UTC,
		StopGracePeriod:            5 * time.Second,
		WALRotateInterval:          defaultWALRotateInterval,
		DisableVariableCompression: false,
		InitCatalog:                true,
		InitWALCache:               true,
		BackgroundSync:             true,
		WALBypass:                  false,
		StartTime:                  time.Now(),
		Replication: ReplicationSetting{
			Enabled:    false,
			TLSEnabled: false,
			CertFile:   "",
			KeyFile:    "",
			ListenPort: defaultReplicationMasterListenPort,
			MasterHost: "",
			// default retry intervals are 10s -> 20s -> 40s -> ...
			RetryInterval:       10 * time.Second,
			RetryBackoffCoeff:   2,
			ReconcileInterval:   5 * time.Minute,
			BackfillParallelism: 8,
			BackfillLookback:    24 * time.Hour,
		},
		Triggers:       nil,
		BgWorkers:      nil,
		AttrGroupTypes: make(map[string]*AttrGroupConfig),
	}
}

type aux struct {
	// RootDirectory can be either a relative or absolute path
	RootDirectory              string `yaml:"root_directory"`
	ListenHost                 string `yaml:"listen_host"`
	ListenPort                 string `yaml:"listen_port"`
	GRPCListenPort             string `yaml:"grpc_listen_port"`
	GRPCMaxSendMsgSize         int    `yaml:"grpc_max_send_msg_size"` // in MB
	GRPCMaxRecvMsgSize         int    `yaml:"grpc_max_recv_msg_size"` // in MB
	UtilitiesURL               string `yaml:"utilities_url"`
	Timezone                   string `yaml:"timezone"`
	LogLevel                   string `yaml:"log_level"`
	StopGracePeriod            int    `yaml:"stop_grace_period"`
	WALRotateInterval          int    `yaml:"wal_rotate_interval"`
	DisableVariableCompression string `yaml:"disable_variable_compression"`
	InitCatalog                string `yaml:"init_catalog"`
	InitWALCache               string `yaml:"init_wal_cache"`
	BackgroundSync             string `yaml:"background_sync"`
	WALBypass                  string `yaml:"wal_bypass"`
	Replication                struct {
		Enabled    bool   `yaml:"enabled"`
		TLSEnabled bool   `yaml:"tls_enabled"`
		CertFile   string `yaml:"cert_file"`
		KeyFile    string `yaml:"key_file"`
		// ListenPort is used for the replication protocol by the master instance
		ListenPort          int           `yaml:"listen_port"`
		MasterHost          string        `yaml:"master_host"`
		RetryInterval       time.Duration `yaml:"retry_interval"`
		RetryBackoffCoeff   int           `yaml:"retry_backoff_coeff"`
		MasterQueryHost     string        `yaml:"master_query_host"`
		ReconcileInterval   time.Duration `yaml:"reconcile_interval"`
		BackfillParallelism int           `yaml:"backfill_parallelism"`
		BackfillLookback    time.Duration `yaml:"backfill_lookback"`
	} `yaml:"replication"`
	Triggers []struct {
		Module string                 `yaml:"module"`
		On     string                 `yaml:"on"`
		Config map[string]interface{} `yaml:"config"`
	} `yaml:"triggers"`
	BgWorkers []struct {
		Module string                 `yaml:"module"`
		Name   string                 `yaml:"name"`
		Config map[string]interface{} `yaml:"config"`
	} `yaml:"bgworkers"`
	AttrGroupTypes map[string]struct {
		Columns    map[string]string `yaml:"columns"`
		RecordType string            `yaml:"record_type"`
	} `yaml:"attrgroup_types"`
}

func ParseConfig(data []byte) (*MktsConfig, error) {
	var a aux
	if err := yaml.Unmarshal(data, &a); err != nil {
		return nil, err
	}

	absoluteRootDir, err := filepath.Abs(filepath.Clean(a.RootDirectory))
	if a.RootDirectory == "" || err != nil {
		return nil, fmt.Errorf("invalid root directory. rootDir=%s: %w", a.RootDirectory, err)
	}
	m := NewDefaultConfig(absoluteRootDir)

	if a.ListenPort == "" {
		return nil, errors.New("invalid listen port. Listen port can't be empty")
	}

	// GRPC is optional for now
	// if aux.GRPCListenPort == "" {
	// 	log.Error("Invalid GRPC listen port.")
	// 	return errors.New("Invalid GRPC listen port.")
	// }
	const (
		recommendedMinGRPCSendMsgSize = 64
		recommendedMinGRPCRecvMsgSize = 64
	)
	if a.GRPCMaxSendMsgSize != 0 {
		m.GRPCMaxSendMsgSize = a.GRPCMaxSendMsgSize * megabyteToByte
		if a.GRPCMaxSendMsgSize < recommendedMinGRPCSendMsgSize {
			log.Warn("WARNING: Low grpc_max_send_msg_size: %dMB (recommend at least 64MB)", a.GRPCMaxSendMsgSize)
		}
	}

	if a.GRPCMaxRecvMsgSize != 0 {
		m.GRPCMaxRecvMsgSize = a.GRPCMaxRecvMsgSize * megabyteToByte
		if a.GRPCMaxRecvMsgSize < recommendedMinGRPCRecvMsgSize {
			log.Warn("WARNING: Low grpc_max_recv_msg_size: %dMB (recommend at least 64MB)", a.GRPCMaxRecvMsgSize)
		}
	}

	// Giving "" to LoadLocation will be UTC anyway, which is our default too.
	m.Timezone, err = time.LoadLocation(a.Timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone:%s", a.Timezone)
	}

	if a.WALRotateInterval != 0 {
		m.WALRotateInterval = a.WALRotateInterval
	}

	if a.LogLevel != "" {
		switch strings.ToLower(a.LogLevel) {
		case "fatal":
			log.SetLevel(log.FATAL)
		case "error":
			log.SetLevel(log.ERROR)
		case "warning":
			log.SetLevel(log.WARNING)
		case "debug":
			log.SetLevel(log.DEBUG)
		default: // case "info":
			log.SetLevel(log.INFO)
		}
	}

	if a.StopGracePeriod > 0 {
		m.StopGracePeriod = time.Duration(a.StopGracePeriod) * time.Second
	}

	if a.DisableVariableCompression != "" {
		m.DisableVariableCompression, err = strconv.ParseBool(a.DisableVariableCompression)
		if err != nil {
			return nil, fmt.Errorf("invalid value for DisableVariableCompression: %w", err)
		}
	}

	if a.InitCatalog != "" {
		m.InitCatalog, err = strconv.ParseBool(a.InitCatalog)
		if err != nil {
			return nil, fmt.Errorf("invalid value for InitCatalog: %w", err)
		}
	}

	if a.InitWALCache != "" {
		m.InitWALCache, err = strconv.ParseBool(a.InitWALCache)
		if err != nil {
			return nil, fmt.Errorf("invalid value for InitWALCache: %w", err)
		}
	}

	if a.BackgroundSync != "" {
		m.BackgroundSync, err = strconv.ParseBool(a.BackgroundSync)
		if err != nil {
			return nil, fmt.Errorf("invalid value for BackgroundSync: %w", err)
		}
	}

	if a.WALBypass != "" {
		m.WALBypass, err = strconv.ParseBool(a.WALBypass)
		if err != nil {
			return nil, fmt.Errorf("invalid value for WALBypass: %w", err)
		}
	}

	if a.Replication.ListenPort != 0 {
		m.Replication.ListenPort = a.Replication.ListenPort
	}

	m.Replication.Enabled = a.Replication.Enabled
	m.Replication.TLSEnabled = a.Replication.TLSEnabled
	m.Replication.CertFile = a.Replication.CertFile
	m.Replication.KeyFile = a.Replication.KeyFile
	m.Replication.MasterHost = a.Replication.MasterHost
	if a.Replication.RetryInterval != 0 {
		m.Replication.RetryInterval = a.Replication.RetryInterval
	}

	if a.Replication.RetryBackoffCoeff != 0 {
		m.Replication.RetryBackoffCoeff = a.Replication.RetryBackoffCoeff
	}

	m.Replication.MasterQueryHost = a.Replication.MasterQueryHost
	if a.Replication.ReconcileInterval != 0 {
		m.Replication.ReconcileInterval = a.Replication.ReconcileInterval
	}
	if a.Replication.BackfillParallelism != 0 {
		m.Replication.BackfillParallelism = a.Replication.BackfillParallelism
	}
	if a.Replication.BackfillLookback != 0 {
		m.Replication.BackfillLookback = a.Replication.BackfillLookback
	}

	m.ListenURL = fmt.Sprintf("%v:%v", a.ListenHost, a.ListenPort)
	if a.GRPCListenPort != "" {
		m.GRPCListenURL = fmt.Sprintf("%v:%v", a.ListenHost, a.GRPCListenPort)
	}
	m.UtilitiesURL = a.UtilitiesURL

	for _, trig := range a.Triggers {
		triggerSetting := &TriggerSetting{
			Module: trig.Module,
			On:     trig.On,
			Config: trig.Config,
		}
		m.Triggers = append(m.Triggers, triggerSetting)
	}

	for _, bg := range a.BgWorkers {
		bgWorkerSetting := &BgWorkerSetting{
			Module: bg.Module,
			Name:   bg.Name,
			Config: bg.Config,
		}
		m.BgWorkers = append(m.BgWorkers, bgWorkerSetting)
	}

	// Parse and validate attrgroup_types
	for name, agCfg := range a.AttrGroupTypes {
		if len(agCfg.Columns) == 0 {
			return nil, fmt.Errorf("attrgroup_types[%s]: must define at least one column", name)
		}

		// Validate column types
		for colName, colType := range agCfg.Columns {
			if !isValidElementTypeName(colType) {
				return nil, fmt.Errorf("attrgroup_types[%s].columns[%s]: invalid type %q", name, colName, colType)
			}
		}

		// Validate record type
		recordType := agCfg.RecordType
		if recordType == "" {
			recordType = "fixed" // default
		}
		recordType = strings.ToLower(recordType)
		if recordType != "fixed" && recordType != "variable" {
			return nil, fmt.Errorf("attrgroup_types[%s].record_type: must be 'fixed' or 'variable', got %q", name, agCfg.RecordType)
		}

		m.AttrGroupTypes[name] = &AttrGroupConfig{
			Columns:    agCfg.Columns,
			RecordType: recordType,
		}
	}

	return m, nil
}

// validElementTypeNames contains all valid type names for column definitions.
// This must match the names in utils/io/datatypes.go attributeMap.
var validElementTypeNames = map[string]bool{
	"float32":  true,
	"int32":    true,
	"float64":  true,
	"int64":    true,
	"byte":     true,
	"bool":     true,
	"int16":    true,
	"uint8":    true,
	"uint16":   true,
	"uint32":   true,
	"uint64":   true,
	"string16": true,
}

// isValidElementTypeName checks if a type name is valid for column definitions.
func isValidElementTypeName(name string) bool {
	return validElementTypeNames[strings.ToLower(name)]
}
