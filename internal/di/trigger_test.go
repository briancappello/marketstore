package di

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils"
)

func modules(ts []*utils.TriggerSetting) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.Module
	}
	return out
}

func TestFilterReplicaTriggers(t *testing.T) {
	triggers := []*utils.TriggerSetting{
		{Module: "ondiskagg.so"},                   // aggregation: always skipped
		{Module: "stream.so"},                      // websocket streaming: kept
		{Module: "custom.so", SkipOnReplica: true}, // explicit opt-out: skipped
		{Module: "OnDiskAgg.so"},                   // case-insensitive match: skipped
		{Module: "othertrigger.so"},                // kept
	}

	kept := filterReplicaTriggers(triggers)

	assert.Equal(t, []string{"stream.so", "othertrigger.so"}, modules(kept))
}

func TestUnsafeTriggerOnReplica(t *testing.T) {
	assert.True(t, unsafeTriggerOnReplica(&utils.TriggerSetting{Module: "ondiskagg.so"}))
	assert.True(t, unsafeTriggerOnReplica(&utils.TriggerSetting{Module: "x.so", SkipOnReplica: true}))
	assert.False(t, unsafeTriggerOnReplica(&utils.TriggerSetting{Module: "stream.so"}))
}
