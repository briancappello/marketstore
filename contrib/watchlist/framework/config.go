package framework

import (
	"encoding/json"
	"fmt"
)

// normalizeMapKeys recursively converts map[interface{}]interface{} (produced by
// gopkg.in/yaml.v2 for nested YAML maps) into map[string]interface{} so the
// stdlib encoding/json can marshal it. We deliberately avoid jsoniter/reflect2:
// they are unmaintained and their unsafe reflect internals crash under Go 1.26.
func normalizeMapKeys(v interface{}) interface{} {
	switch m := v.(type) {
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(m))
		for k, val := range m {
			out[fmt.Sprint(k)] = normalizeMapKeys(val)
		}
		return out
	case map[string]interface{}:
		for k, val := range m {
			m[k] = normalizeMapKeys(val)
		}
		return m
	case []interface{}:
		for i, val := range m {
			m[i] = normalizeMapKeys(val)
		}
		return m
	default:
		return v
	}
}

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

	// StrategyConfig is an optional map of strategy-name to config that is
	// passed to each WatchlistStrategy factory at creation time. This allows
	// bgworker-level config (e.g., database DSNs) to reach strategies that
	// need it.
	StrategyConfig map[string]map[string]interface{} `json:"strategy_config"`
}

// ParseTriggerConfig parses a raw config map into a TriggerConfig.
func ParseTriggerConfig(raw map[string]interface{}) (*TriggerConfig, error) {
	data, err := json.Marshal(normalizeMapKeys(raw))
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
	data, err := json.Marshal(normalizeMapKeys(raw))
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
