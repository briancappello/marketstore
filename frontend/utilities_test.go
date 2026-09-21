package frontend

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alpacahq/marketstore/v4/utils"
)

func TestHandler(t *testing.T) {
	startTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	utils.Tag = "dev"
	TestValues := map[string]struct {
		Recorder        *httptest.ResponseRecorder
		Version         string
		ExpectedVersion string
	}{
		"Failure": {
			httptest.NewRecorder(),
			"dev",
			"dev",
		},
		"Success": {
			httptest.NewRecorder(),
			"dev",
			"dev",
		},
	}
	for key, val := range TestValues {
		switch key {
		case "Success":
			atomic.StoreUint32(&Queryable, uint32(1))
			NewUtilityAPIHandlers(startTime).heartbeat(val.Recorder, nil)
			hm := HeartbeatMessage{}
			err := json.NewDecoder(val.Recorder.Body).Decode(&hm)
			if err != nil {
				t.Fatal(err)
			}
			if hm.Version != val.ExpectedVersion {
				t.Error("Wrong version - Expected:", val.ExpectedVersion, "Got:", hm.Version)
			}
			assert.Equal(t, hm.Status, "queryable")
			assert.Equal(t, val.Recorder.Code, http.StatusOK)
		case "Failure":
			atomic.StoreUint32(&Queryable, uint32(0))
			NewUtilityAPIHandlers(startTime).heartbeat(val.Recorder, nil)
			hm := HeartbeatMessage{}
			err := json.NewDecoder(val.Recorder.Body).Decode(&hm)
			if err != nil {
				t.Fatal(err)
			}
			if hm.Version != val.ExpectedVersion {
				t.Error("Wrong version - Expected:", val.ExpectedVersion, "Got:", hm.Version)
			}
			assert.Equal(t, hm.Status, "not queryable")
			assert.Equal(t, val.Recorder.Code, http.StatusServiceUnavailable)
		}
	}
}

// TestHeartbeat_ReplicationBroken is the regression test for a replica that
// kept advertising itself as healthy after its live replication stream died.
//
// The retryer does not redial after a non-retryable replay failure, and the
// listeners stay up, so the node would go on answering queries from data that
// silently stops advancing. Queries are intentionally still served -- the data
// on disk remains readable -- but health must report unhealthy so an
// orchestrator pulls the node from rotation.
func TestHeartbeat_ReplicationBroken(t *testing.T) {
	startTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	utils.Tag = "dev"

	// Queryable, replication healthy: the node is genuinely good.
	atomic.StoreUint32(&Queryable, 1)
	atomic.StoreUint32(&replicationBroken, 0)
	t.Cleanup(func() { atomic.StoreUint32(&replicationBroken, 0) })

	rec := httptest.NewRecorder()
	NewUtilityAPIHandlers(startTime).heartbeat(rec, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Same node, stream now permanently stopped.
	SetReplicationBroken()
	assert.True(t, ReplicationBroken())

	rec = httptest.NewRecorder()
	NewUtilityAPIHandlers(startTime).heartbeat(rec, nil)

	hm := HeartbeatMessage{}
	if err := json.NewDecoder(rec.Body).Decode(&hm); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code,
		"a replica whose replication stream has stopped must not report healthy")
	assert.Equal(t, "replication stopped", hm.Status)

	// Queryable is untouched: queries keep working on purpose.
	assert.Equal(t, uint32(1), atomic.LoadUint32(&Queryable))
}

// TestRESTHealth_ReplicationBroken covers the same contract on the REST probe,
// which is the endpoint most load balancers are pointed at.
func TestRESTHealth_ReplicationBroken(t *testing.T) {
	atomic.StoreUint32(&Queryable, 1)
	atomic.StoreUint32(&replicationBroken, 0)
	t.Cleanup(func() { atomic.StoreUint32(&replicationBroken, 0) })

	s := &DataService{}

	rec := httptest.NewRecorder()
	s.handleRESTHealth(rec, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	SetReplicationBroken()

	rec = httptest.NewRecorder()
	s.handleRESTHealth(rec, nil)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "replication stopped", body["status"])
}
