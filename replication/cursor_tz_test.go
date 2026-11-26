package replication_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/executor"
	"github.com/alpacahq/marketstore/v4/internal/di"
	"github.com/alpacahq/marketstore/v4/replication"
	"github.com/alpacahq/marketstore/v4/utils"
)

// Bucket indexes and epochs depend on the instance's configured timezone
// (utils.InstanceConfig.Timezone). The cursor must round-trip under any
// timezone, and when written under one timezone and read under another:
// set-cursor ran with UTC while p1's server runs America/New_York.
func TestCursorRoundTripsAcrossTimezones(t *testing.T) {
	zones := []string{"UTC", "America/New_York", "Pacific/Kiritimati", "Pacific/Pago_Pago", "Asia/Kolkata"}
	prev := utils.InstanceConfig.Timezone
	t.Cleanup(func() { utils.InstanceConfig.Timezone = prev })
	for _, write := range zones {
		for _, read := range zones {
			t.Run(write+"->"+read, func(t *testing.T) {
				wloc, err := time.LoadLocation(write)
				require.NoError(t, err)
				rloc, err := time.LoadLocation(read)
				require.NoError(t, err)

				root := t.TempDir()
				utils.InstanceConfig.Timezone = wloc
				cfg := utils.NewDefaultConfig(root)
				cfg.BackgroundSync = false
				c := di.NewContainer(cfg)
				md := ownInstance(c.GetCatalogDir(), c.GetInitWALFile())
				bw, err := executor.NewBatchWriter(md.CatalogDir, md.WALFile)
				require.NoError(t, err)
				require.NoError(t, bw.QueueCSM(replication.CursorCSM(4242), false))
				require.NoError(t, bw.Commit())

				utils.InstanceConfig.Timezone = rloc
				c2 := di.NewContainer(cfg) // a fresh catalog, as after a restart
				seq, ok, err := replication.ReadCursor(c2.GetCatalogDir())
				require.NoError(t, err)
				require.True(t, ok, "cursor written under %s unreadable under %s", write, read)
				require.Equal(t, uint64(4242), seq)
			})
		}
	}
}
