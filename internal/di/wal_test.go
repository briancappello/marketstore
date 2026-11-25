package di

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/utils"
)

func TestReplicationLogOpensOnlyOnAMaster(t *testing.T) {
	root := t.TempDir()
	cfg := utils.NewDefaultConfig(root)
	require.Nil(t, NewContainer(cfg).GetReplicationLog())

	cfg.Replication.Enabled = true
	c := NewContainer(cfg)
	l := c.GetReplicationLog()
	require.NotNil(t, l)
	require.Same(t, l, c.GetReplicationLog())
	_, err := os.Stat(filepath.Join(root, "replication_log"))
	require.NoError(t, err)
	c.CloseReplicationLog()
}
