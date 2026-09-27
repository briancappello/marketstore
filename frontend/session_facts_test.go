package frontend_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/frontend"
	"github.com/alpacahq/marketstore/v4/utils"
)

type fakeRebuilder struct {
	symbols  []string
	from, to time.Time
	calls    int
}

func (f *fakeRebuilder) QueueSessionFactsRebuild(symbols []string, from, to time.Time) (int, error) {
	f.calls++
	f.symbols, f.from, f.to = symbols, from, to
	return 42, nil
}

func withReplication(t *testing.T, masterHost string) {
	t.Helper()
	prev := utils.InstanceConfig.Replication
	utils.InstanceConfig.Replication.MasterHost = masterHost
	t.Cleanup(func() { utils.InstanceConfig.Replication = prev })
}

func withRebuilder(t *testing.T, r frontend.SessionFactsRebuilder) {
	t.Helper()
	frontend.RegisterSessionFactsRebuilder(r)
	t.Cleanup(func() { frontend.RegisterSessionFactsRebuilder(nil) })
}

func TestRebuildSessionFactsQueuesOnLeader(t *testing.T) {
	withReplication(t, "")
	fr := &fakeRebuilder{}
	withRebuilder(t, fr)

	var ds frontend.DataService
	var resp frontend.RebuildSessionFactsResponse
	err := ds.RebuildSessionFacts(nil, &frontend.RebuildSessionFactsRequest{
		From: "2026-09-21", To: "2026-09-25", Symbols: []string{"AAPL"},
	}, &resp)
	require.NoError(t, err)
	assert.Equal(t, 42, resp.Queued)
	assert.Equal(t, []string{"AAPL"}, fr.symbols)
	ny, _ := time.LoadLocation("America/New_York")
	assert.Equal(t, "2026-09-21", fr.from.In(ny).Format("2006-01-02"), "the date survives the timezone")
	assert.Equal(t, "2026-09-25", fr.to.In(ny).Format("2006-01-02"))
}

func TestRebuildSessionFactsRejectedOnReplica(t *testing.T) {
	withReplication(t, "taichi:5996")
	fr := &fakeRebuilder{}
	withRebuilder(t, fr)

	var ds frontend.DataService
	var resp frontend.RebuildSessionFactsResponse
	err := ds.RebuildSessionFacts(nil, &frontend.RebuildSessionFactsRequest{From: "2026-09-21", To: "2026-09-25"}, &resp)
	assert.True(t, errors.Is(err, frontend.ErrReplicaRebuild), "err = %v", err)
	assert.Equal(t, 0, fr.calls, "nothing is queued on a replica")
}

func TestRebuildSessionFactsValidates(t *testing.T) {
	withReplication(t, "")
	withRebuilder(t, &fakeRebuilder{})
	var ds frontend.DataService
	var resp frontend.RebuildSessionFactsResponse
	for _, req := range []frontend.RebuildSessionFactsRequest{
		{From: "yesterday", To: "2026-09-25"},
		{From: "2026-09-25", To: "2026-09-21"},
	} {
		req := req
		assert.Error(t, ds.RebuildSessionFacts(nil, &req, &resp), "%+v", req)
	}

	frontend.RegisterSessionFactsRebuilder(nil)
	assert.Error(t, ds.RebuildSessionFacts(nil,
		&frontend.RebuildSessionFactsRequest{From: "2026-09-21", To: "2026-09-25"}, &resp),
		"no plugin loaded")
}
