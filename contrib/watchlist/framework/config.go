package framework

// Use jsoniter because gopkg.in/yaml.v2 decodes nested YAML maps as
// map[interface{}]interface{}, which encoding/json cannot marshal.
// jsoniter handles this transparently by converting interface{} keys
// to their string representations.
import jsoniter "github.com/json-iterator/go"

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// TriggerConfig is the config block for the watchlist trigger in mkts.yml.
type TriggerConfig struct {
	Curation   CurationConfig    `json:"curation"`
	Watchlists []WatchlistConfig `json:"watchlists"`
}

// CurationConfig defines the criteria for symbol curation.
// These are the default fields available to the Curator; custom Curator
// implementations may use additional fields from the raw config map.
type CurationConfig struct {
	MinPrice         float64 `json:"min_price"`
	MinDollarVolRate float64 `json:"min_dollar_vol_rate"`
	LookbackSecs     int     `json:"lookback_secs"`
}

// WatchlistConfig is the per-watchlist config block.
type WatchlistConfig struct {
	Name    string                 `json:"name"`
	SortBy  string                 `json:"sort_by"`
	SortDir string                 `json:"sort_dir"`
	Limit   int                    `json:"limit"`
	Filters map[string]float64     `json:"filters"`
	Extra   map[string]interface{} `json:"extra"`
}

// WorkerConfig is the config block for the watchlist bgworker in mkts.yml.
type WorkerConfig struct {
	BaselineLookbackDays int    `json:"baseline_lookback_days"`
	MedianWindow         int    `json:"median_window"`
	RankingIntervalMs    int    `json:"ranking_interval_ms"`
	RefreshInterval      string `json:"refresh_interval"`
}

// ParseTriggerConfig parses a raw config map into a TriggerConfig.
func ParseTriggerConfig(raw map[string]interface{}) (*TriggerConfig, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var cfg TriggerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ParseWorkerConfig parses a raw config map into a WorkerConfig.
func ParseWorkerConfig(raw map[string]interface{}) (*WorkerConfig, error) {
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var cfg WorkerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	// Apply defaults
	if cfg.BaselineLookbackDays == 0 {
		cfg.BaselineLookbackDays = 60
	}
	if cfg.MedianWindow == 0 {
		cfg.MedianWindow = 50
	}
	if cfg.RankingIntervalMs == 0 {
		cfg.RankingIntervalMs = 1000
	}
	if cfg.RefreshInterval == "" {
		cfg.RefreshInterval = "24h"
	}
	return &cfg, nil
}
