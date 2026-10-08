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
//
// The trigger has no settings of its own. Watchlists and curation belong to
// the bgworker, which owns the curator and the strategies: see WorkerConfig
// and ignoredTriggerKeys.
type TriggerConfig struct{}

// ignoredTriggerKeys returns the keys of a raw trigger config that look
// meaningful but are not read, with where each belongs instead, so NewTrigger
// can warn rather than let them silently do nothing.
//
// "watchlists" once listed the watchlists to run, with per-list limits. Nothing
// ever read it: the set of watchlists is the set the plugin registers.
//
// "curation" set the curator's thresholds, but the curator is built by the
// bgworker, so only lookback_secs ever took effect. There are also two
// trigger instances (1Sec and 1Min), which could disagree.
func ignoredTriggerKeys(raw map[string]interface{}) map[string]string {
	keys := map[string]string{}
	if _, ok := raw["watchlists"]; ok {
		keys["watchlists"] = "every watchlist the plugin registers runs; " +
			"configure strategies in the bgworker's strategy_config"
	}
	if _, ok := raw["curation"]; ok {
		keys["curation"] = "move it to the watchlist bgworker's config"
	}
	return keys
}

// WorkerConfig is the config block for the watchlist bgworker in mkts.yml.
type WorkerConfig struct {
	BaselineLookbackDays int    `json:"baseline_lookback_days"`
	MedianWindow         int    `json:"median_window"`
	RankingIntervalMs    int    `json:"ranking_interval_ms"`
	RefreshInterval      string `json:"refresh_interval"`

	// SessionFactsGrace is how long after afterhours ends the daily session
	// facts job waits for end-of-day fills (a Go duration, default "30m").
	SessionFactsGrace string `json:"session_facts_grace"`

	// StrategyConfig is an optional map of strategy-name to config that is
	// passed to each WatchlistStrategy factory at creation time. This allows
	// bgworker-level config (e.g., database DSNs) to reach strategies that
	// need it.
	StrategyConfig map[string]map[string]interface{} `json:"strategy_config"`

	// Curation is passed as-is to the registered curator's factory, for the
	// live curator and the rewind's own. Its keys are the curator's (the
	// default liquidity curator reads min_price, min_dollar_vol_rate and
	// min_median_volume). Numbers arrive as float64 whatever their YAML form.
	//
	// The framework itself reads one key, lookback_secs: the window in
	// seconds of SymbolState.DollarVolumeRate (default 300).
	Curation map[string]interface{} `json:"curation"`
}

// lookbackSecs returns curation.lookback_secs, and false when it is not set.
func (c *WorkerConfig) lookbackSecs() (int64, bool, error) {
	v, ok := c.Curation["lookback_secs"]
	if !ok {
		return 0, false, nil
	}
	f, isNum := v.(float64)
	if !isNum || f <= 0 || f != float64(int64(f)) {
		return 0, false, fmt.Errorf("curation.lookback_secs must be a positive whole number of seconds, got %v", v)
	}
	return int64(f), true, nil
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
	if _, _, err := cfg.lookbackSecs(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
