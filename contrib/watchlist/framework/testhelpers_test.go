package framework

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/plugins/trigger"
	"github.com/alpacahq/marketstore/v4/utils"
)

// captureTrigger receives what the real WAL dispatcher hands to triggers.
type captureTrigger struct {
	mu    sync.Mutex
	fires map[string][][]trigger.Record
	cond  *sync.Cond
}

func newCaptureTrigger() *captureTrigger {
	c := &captureTrigger{fires: map[string][][]trigger.Record{}}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *captureTrigger) Fire(key string, recs []trigger.Record) {
	c.mu.Lock()
	c.fires[key] = append(c.fires[key], recs)
	c.mu.Unlock()
	c.cond.Broadcast()
}

// next blocks until the n-th fire (0-based) for key has arrived.
func (c *captureTrigger) next(t testing.TB, key string, n int) []trigger.Record {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.fires[key]) <= n {
		if time.Now().After(deadline) {
			t.Fatalf("no fire #%d for %s", n, key)
		}
		c.mu.Unlock()
		time.Sleep(time.Millisecond)
		c.mu.Lock()
	}
	return c.fires[key][n]
}

// setupCapturingInstance starts a MarketStore instance whose WAL dispatches
// every written bucket to the returned captureTrigger.
func setupCapturingInstance(t testing.TB) *captureTrigger {
	t.Helper()
	cfg := utils.NewDefaultConfig(t.TempDir())
	cfg.BackgroundSync = false
	cfg.Timezone, _ = time.LoadLocation("America/New_York")
	utils.InstanceConfig = *cfg
	c := di.NewContainer(cfg)

	capture := newCaptureTrigger()
	tpd := executor.StartNewTriggerPluginDispatcherWithWorkers(
		[]*trigger.Matcher{trigger.NewMatcher(capture, "*")}, 1)
	walFile, err := executor.NewWALFile(c.GetAbsRootDir(), c.GetInitInstanceID(),
		&executor.NopReplicationSender{}, false, &sync.WaitGroup{}, tpd, executor.NewTransactionPipe())
	require.NoError(t, err)
	executor.NewInstanceSetup(c.GetCatalogDir(), walFile)
	return capture
}
