package di

import (
	"strings"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

func (c *Container) InjectTriggerMatchers(ms []*trigger.Matcher) {
	c.triggerMatchers = ms
}

func (c *Container) GetTriggerMatchers() []*trigger.Matcher {
	if c.triggerMatchers != nil {
		return c.triggerMatchers
	}
	triggers := c.mktsConfig.Triggers
	if c.mktsConfig.Replication.IsReplica() {
		triggers = filterReplicaTriggers(triggers)
	}
	c.triggerMatchers = trigger.NewTriggerMatchers(triggers)
	return c.triggerMatchers
}

// filterReplicaTriggers drops triggers that must not run on a replica and keeps
// the rest (e.g. websocket streaming is fine on a replica). A replica already
// receives the leader's trigger output via replication, so re-running a
// write-generating trigger locally is redundant and can diverge.
func filterReplicaTriggers(triggers []*utils.TriggerSetting) []*utils.TriggerSetting {
	kept := make([]*utils.TriggerSetting, 0, len(triggers))
	for _, ts := range triggers {
		if unsafeTriggerOnReplica(ts) {
			log.Info("[replication] skipping trigger %q on replica "+
				"(redundant/unsafe; the leader replicates its output)", ts.Module)
			continue
		}
		kept = append(kept, ts)
	}
	return kept
}

// unsafeTriggerOnReplica reports whether a trigger should be skipped on a
// replica: the on-disk aggregation trigger (always), or any trigger the
// operator marked with skip_on_replica.
func unsafeTriggerOnReplica(ts *utils.TriggerSetting) bool {
	if ts.SkipOnReplica {
		return true
	}
	// The on-disk aggregation trigger recomputes higher timeframes from base
	// writes; on a replica those aggregates are already replicated from the
	// leader, so running it is redundant and races the replicated bars.
	return strings.Contains(strings.ToLower(ts.Module), "ondiskagg")
}

func (c *Container) GetStartTriggerPluginDispatcher() *executor.TriggerPluginDispatcher {
	if c.tpd != nil {
		return c.tpd
	}

	c.tpd = executor.StartNewTriggerPluginDispatcher(c.GetTriggerMatchers())
	return c.tpd
}
