package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The example replication configs must parse, so their documented keys are
// real.
func TestReplicationExampleConfigsParse(t *testing.T) {
	for _, name := range []string{"mkts-master.yml", "mkts-replica.yml"} {
		b, err := os.ReadFile(filepath.Join("..", "examples", "replication", name))
		require.NoError(t, err)
		_, err = ParseConfig(b)
		require.NoError(t, err, name)
	}
}
