package backfill_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
)

// Compile-time check that GRPCClient satisfies MasterAPI.
func TestGRPCClientImplementsMasterAPI(t *testing.T) {
	var _ backfill.MasterAPI = (*backfill.GRPCClient)(nil)
	assert.True(t, true)
}
