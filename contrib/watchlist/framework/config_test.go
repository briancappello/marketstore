package framework

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A trigger config that still carries the old "watchlists" or "curation"
// blocks must load, and report both as ignored.
func TestTriggerConfigLegacyKeysAreIgnored(t *testing.T) {
	raw := map[string]interface{}{
		"curation": map[interface{}]interface{}{"lookback_secs": 300},
		"watchlists": []interface{}{
			map[interface{}]interface{}{"name": "VOLUME_UP", "limit": 100},
		},
	}

	_, err := ParseTriggerConfig(raw)
	require.NoError(t, err)
	ignored := ignoredTriggerKeys(raw)
	assert.Len(t, ignored, 2)
	assert.Contains(t, ignored, "watchlists")
	assert.Contains(t, ignored, "curation")
}

// A trigger entry with no config: block at all reaches NewTrigger as nil.
func TestTriggerWithoutConfigLoads(t *testing.T) {
	_, err := NewTrigger(nil)
	require.NoError(t, err)
	assert.Empty(t, ignoredTriggerKeys(nil))
}

// stubCurator curates everything.
type stubCurator struct{}

func (stubCurator) Init(map[string]*SymbolState)       {}
func (stubCurator) Evaluate(string, *SymbolState) bool { return true }

// withDollarVolLookback restores the package-wide lookback after a test.
func withDollarVolLookback(t *testing.T) {
	t.Helper()
	prev := dollarVolLookback.Load()
	t.Cleanup(func() { dollarVolLookback.Store(prev) })
}

// The bgworker's curation block reaches the curator factory, with YAML
// integers as float64 so a curator's float64 assertions match.
func TestWorkerCurationConfigReachesCurator(t *testing.T) {
	ResetRegistry()
	t.Cleanup(ResetRegistry)
	withDollarVolLookback(t)

	var got map[string]interface{}
	RegisterCurator(func(config map[string]interface{}) (Curator, error) {
		got = config
		return stubCurator{}, nil
	})

	w, err := NewBgWorker(map[string]interface{}{
		"curation": map[interface{}]interface{}{
			"min_price":           5,
			"min_dollar_vol_rate": 2500.5,
			"lookback_secs":       120,
		},
	})
	require.NoError(t, err)
	ww := w.(*WatchlistWorker)

	c, err := newCurator(ww.config.Curation)
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.Equal(t, 5.0, got["min_price"])
	assert.Equal(t, 2500.5, got["min_dollar_vol_rate"])
	assert.Equal(t, int64(120), dollarVolLookback.Load())

	// The rewind builds its own curator from the same config.
	got = nil
	r := newRewinder(5, nil, ww.config.Curation)
	require.NoError(t, r.build())
	assert.Equal(t, 5.0, got["min_price"])
}

// Without a curation block the factory gets an empty map, not nil, and the
// lookback keeps its default.
func TestWorkerWithoutCurationConfig(t *testing.T) {
	ResetRegistry()
	t.Cleanup(ResetRegistry)
	withDollarVolLookback(t)
	dollarVolLookback.Store(defaultDollarVolLookback)

	var got map[string]interface{}
	RegisterCurator(func(config map[string]interface{}) (Curator, error) {
		got = config
		return stubCurator{}, nil
	})

	w, err := NewBgWorker(map[string]interface{}{})
	require.NoError(t, err)
	_, err = newCurator(w.(*WatchlistWorker).config.Curation)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
	assert.Equal(t, int64(defaultDollarVolLookback), dollarVolLookback.Load())
}

func TestWorkerRejectsBadLookback(t *testing.T) {
	for _, v := range []interface{}{0, -5, 1.5, "300"} {
		_, err := ParseWorkerConfig(map[string]interface{}{
			"curation": map[string]interface{}{"lookback_secs": v},
		})
		assert.Error(t, err, "lookback_secs=%v", v)
	}
}
