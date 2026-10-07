package framework

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A trigger config that still carries the old "watchlists" block must load,
// keep its curation settings, and report the block as ignored.
func TestTriggerConfigWatchlistsKeyIsIgnored(t *testing.T) {
	raw := map[string]interface{}{
		"curation": map[interface{}]interface{}{"lookback_secs": 300},
		"watchlists": []interface{}{
			map[interface{}]interface{}{"name": "VOLUME_UP", "limit": 100},
		},
	}

	cfg, err := ParseTriggerConfig(raw)
	require.NoError(t, err)
	assert.Equal(t, 300, cfg.Curation.LookbackSecs)
	assert.Equal(t, []string{"watchlists"}, ignoredTriggerKeys(raw))
}

func TestTriggerConfigWithoutWatchlistsKeyHasNothingIgnored(t *testing.T) {
	raw := map[string]interface{}{
		"curation": map[string]interface{}{"lookback_secs": 60},
	}

	_, err := ParseTriggerConfig(raw)
	require.NoError(t, err)
	assert.Empty(t, ignoredTriggerKeys(raw))
}
