# Logical Replication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make MarketStore master→replica replication actually usable for a LAN dev/analytics mirror by (a) making the existing live gRPC feed unable to stall the master, and (b) adding a pull-backfill reconciler that bootstraps pre-existing data and heals any gaps.

**Architecture:** Reuse the existing gRPC WAL live feed (`replication/`) unchanged except for isolation fixes. Add a replica-side backfill that reads historical ranges from the master's existing `Query`/`ListSymbols` gRPC API, writes them locally (idempotent by epoch), and tracks a per-bucket watermark. The live stream is best-effort for freshness; the backfill reconciler guarantees completeness (bootstrap at startup, periodic reconcile, reconnect-gap).

**Tech Stack:** Go 1.18, gRPC (`google.golang.org/grpc`), Prometheus (`promauto`), testify. Design doc: `docs/design/logical_replication_design.md`.

## Global Constraints

- Module path: `github.com/alpacahq/marketstore/v4`. Copied verbatim in imports.
- A broken/slow/disconnected replica must NEVER block or panic the master. This is the acceptance bar for Phase 0.
- All writes are idempotent by epoch (`WriteCSM` overwrites) — rely on this; never add dedup logic.
- Deletes are out of scope. Do not implement delete propagation.
- Error wrapping: prefer `fmt.Errorf("...: %w", err)`. Logging: `utils/log` printf-style.
- Tests: `testing` + `github.com/stretchr/testify/assert` (and `require`); black-box `_test` packages where practical; `t.TempDir()` for scratch dirs.
- The replication package's existing `receiver.go`, `replay.go`, `retry.go`, `grpc_client.go` are REUSED UNCHANGED.

---

## Phase 0 — Server isolation (independently shippable)

### Task 1: Non-blocking write-path handoff (F1)

Make `Sender.Send` (called from the WAL flush at `executor/wal.go:319-320`) never block, so replication can never back-pressure the master's write path.

**Files:**
- Modify: `metrics/metrics.go` (add a counter)
- Modify: `replication/sender.go`
- Test: `replication/sender_test.go` (existing file, add a test)

**Interfaces:**
- Produces: `metrics.ReplicationDroppedMessages prometheus.Counter`; `Sender.Send([]byte)` now non-blocking (drops + counts when full).

- [ ] **Step 1: Add the metric**

In `metrics/metrics.go`, add inside the existing `var (...)` block (mirror the `WSConnections` pattern):

```go
	// ReplicationDroppedMessages counts transaction groups dropped by the
	// master because a replication buffer was full. Non-zero means a replica
	// was too slow; the gap is healed by the replica's backfill reconciler.
	ReplicationDroppedMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "marketstore_replication_dropped_messages_total",
		Help: "Transaction groups dropped due to a full replication buffer.",
	})
```

- [ ] **Step 2: Write the failing test**

In `replication/sender_test.go` add:

```go
func TestSenderSendDoesNotBlockWhenBufferFull(t *testing.T) {
	// A Sender whose Run loop is never started will never drain its channel.
	s := replication.NewSender(&mockService{})

	// defaultSenderChannelSize is 500; push far more than that. If Send blocks
	// when full, this test hangs (and the suite times out) instead of passing.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 5000; i++ {
			s.Send([]byte("x"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked when the buffer was full")
	}
}
```

Ensure `mockService` exists in the test file (the existing `TestNewSender` uses one); if it only implements `SendReplicationMessage`, it is sufficient. Add `"time"` to imports.

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./replication/ -run TestSenderSendDoesNotBlockWhenBufferFull -v`
Expected: FAIL (hangs → times out) because `Send` currently does a blocking `s.channel <- transactionGroup`.

- [ ] **Step 4: Make Send non-blocking**

In `replication/sender.go`, replace the body of `Send`:

```go
func (s *Sender) Send(transactionGroup []byte) {
	select {
	case s.channel <- transactionGroup:
	default:
		// Buffer full: drop rather than block the caller (the WAL flush).
		// The replica heals the gap via its backfill reconciler.
		metrics.ReplicationDroppedMessages.Inc()
		log.Debug("replication sender buffer full; dropped a transaction group")
	}
}
```

Add imports: `"github.com/alpacahq/marketstore/v4/metrics"` and (if not present) `"github.com/alpacahq/marketstore/v4/utils/log"`.

- [ ] **Step 5: Run test to verify it passes**

Run: `go test ./replication/ -run TestSenderSendDoesNotBlockWhenBufferFull -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add metrics/metrics.go replication/sender.go replication/sender_test.go
git commit -m "fix(replication): make Sender.Send non-blocking so a slow replica cannot stall the master"
```

---

### Task 2: Isolated, race-free fan-out (F2 + F3)

Encapsulate the `StreamChannels` map behind mutex-guarded `Register`/`Unregister` methods and make per-replica delivery non-blocking, so a slow replica neither panics the master (map race, send-on-closed-channel) nor blocks the sender goroutine or its peers. The map has no consumers outside `grpc_server.go`, so unexporting it is safe.

**Files:**
- Modify: `replication/grpc_server.go`
- Test: `replication/grpc_server_test.go` (existing file, add tests)

**Interfaces:**
- Consumes: `metrics.ReplicationDroppedMessages` (Task 1).
- Produces: `GRPCReplicationServer.Register(addr string) chan []byte` and `.Unregister(addr string)` (mutex-guarded); `SendReplicationMessage` non-blocking per replica; `streamChannels` unexported.

- [ ] **Step 1: Write the failing test**

In `replication/grpc_server_test.go` add (the existing tests never touch the map directly, so unexporting it breaks nothing):

```go
func TestSendReplicationMessageIsNonBlockingAndRaceFree(t *testing.T) {
	rs := replication.NewGRPCReplicationServer()

	// A registered-but-undrained replica (simulates a stalled replica).
	slow := rs.Register("slow")
	for i := 0; i < cap(slow); i++ {
		slow <- []byte("fill")
	}

	// Concurrent connect/disconnect churn through the real (guarded) code path
	// to exercise the map guard under -race.
	churned := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			addr := fmt.Sprintf("churn-%d", i)
			rs.Register(addr)
			rs.Unregister(addr)
		}
		close(churned)
	}()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			rs.SendReplicationMessage([]byte("x")) // must not block even though `slow` is full
		}
		close(done)
	}()

	for _, ch := range []chan struct{}{done, churned} {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatal("SendReplicationMessage blocked on a full replica channel")
		}
	}
}
```

Add imports `"fmt"` and `"time"` if missing. (Run this task's verification with `-race`, scoped to `./replication/` — the repo-wide race run is disabled because of an unrelated contrib test.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./replication/ -run TestSendReplicationMessageIsNonBlockingAndRaceFree -race -v`
Expected: FAIL — compile error (`Register`/`Unregister` undefined). After a naive unguarded implementation it would fail with a data race or a hang on the full `slow` channel; the guarded implementation in Step 3 is what makes it pass.

- [ ] **Step 3: Implement guarded registration + non-blocking fan-out**

In `replication/grpc_server.go`, unexport the map, add the mutex and the two methods, and rewrite `SendReplicationMessage`. Import `"sync"`:

```go
type GRPCReplicationServer struct {
	pb.UnimplementedReplicationServer
	CertFile    string
	CertKeyFile string
	mu          sync.Mutex // guards streamChannels
	// Key: IPAddr (e.g. "192.125.18.1:25"), Value: channel for messages sent to each gRPC stream
	streamChannels map[string]chan []byte
}

func NewGRPCReplicationServer() *GRPCReplicationServer {
	return &GRPCReplicationServer{
		streamChannels: map[string]chan []byte{},
	}
}

// Register creates and tracks the outbound buffer for one replica stream.
func (rs *GRPCReplicationServer) Register(addr string) chan []byte {
	ch := make(chan []byte, defaultReplicationStreamChannelSize)
	rs.mu.Lock()
	rs.streamChannels[addr] = ch
	rs.mu.Unlock()
	return ch
}

// Unregister stops tracking a replica stream. The channel is deliberately NOT
// closed: SendReplicationMessage may hold a snapshot referencing it, and a
// send on a closed channel panics. An untracked channel is simply GC'd.
func (rs *GRPCReplicationServer) Unregister(addr string) {
	rs.mu.Lock()
	delete(rs.streamChannels, addr)
	rs.mu.Unlock()
}
```

In `GetWALStream`, replace the direct map writes with the methods:

```go
	streamChannel := rs.Register(clientAddr)
```

and near the end, replacing the `delete(...)` and `close(streamChannel)` lines:

```go
	rs.Unregister(clientAddr)
```

(The old `close(streamChannel)` is removed on purpose — see the `Unregister` comment. The existing `if transactionGroup == nil { break }` guard in the read loop only fired on close and is now harmless; leave it.)

Replace `SendReplicationMessage`:

```go
func (rs *GRPCReplicationServer) SendReplicationMessage(transactionGroup []byte) {
	// Snapshot under lock, deliver outside the lock so a slow replica never
	// holds the map or blocks peers.
	rs.mu.Lock()
	targets := make(map[string]chan []byte, len(rs.streamChannels))
	for ip, ch := range rs.streamChannels {
		targets[ip] = ch
	}
	rs.mu.Unlock()

	for ip, channel := range targets {
		select {
		case channel <- transactionGroup:
		default:
			// Replica too slow: drop for this replica only. Its gap is healed
			// by its backfill reconciler. Never block the master or peers.
			metrics.ReplicationDroppedMessages.Inc()
			log.Debug("replication stream buffer full for %s; dropped a transaction group", ip)
		}
	}
}
```

Add import `"github.com/alpacahq/marketstore/v4/metrics"` (log is already imported).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./replication/ -run TestSendReplicationMessageIsNonBlockingAndRaceFree -race -v`
Expected: PASS

- [ ] **Step 5: Run the whole replication package under race**

Run: `go test ./replication/... -race`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add replication/grpc_server.go replication/grpc_server_test.go
git commit -m "fix(replication): encapsulate stream registry and make fan-out non-blocking per replica"
```

---

## Phase 1 — Replica backfill core

### Task 3: Config for the backfill client

Add the replica-side settings the backfill needs: the master's MAIN gRPC address (distinct from the replication stream port), the reconcile cadence, backfill parallelism, and the correction-healing lookback window.

**Files:**
- Modify: `utils/config.go`
- Test: `utils/config_test.go` (existing; add a case)

**Interfaces:**
- Produces: `ReplicationSetting.MasterQueryHost string`, `.ReconcileInterval time.Duration`, `.BackfillParallelism int`, `.BackfillLookback time.Duration`.

- [ ] **Step 1: Extend the structs**

In `utils/config.go`, add to `ReplicationSetting`:

```go
	// MasterQueryHost is the master's MAIN gRPC address (e.g. "10.0.0.5:5995")
	// used by the replica's backfill client. Distinct from MasterHost (the
	// replication stream port). Empty disables backfill (live-only).
	MasterQueryHost string
	// ReconcileInterval is how often the replica re-pulls [watermark, now] for
	// every bucket to heal any gaps the best-effort live stream missed.
	ReconcileInterval time.Duration
	// BackfillParallelism bounds concurrent per-bucket backfill queries.
	BackfillParallelism int
	// BackfillLookback is the trailing window re-pulled on every reconcile,
	// healing master-side corrections to recent epochs that were missed while
	// disconnected. Re-pulling held data is harmless (idempotent by epoch).
	BackfillLookback time.Duration
```

Add the matching YAML fields to the `aux.Replication` struct:

```go
		MasterQueryHost     string        `yaml:"master_query_host"`
		ReconcileInterval   time.Duration `yaml:"reconcile_interval"`
		BackfillParallelism int           `yaml:"backfill_parallelism"`
		BackfillLookback    time.Duration `yaml:"backfill_lookback"`
```

In `NewDefaultConfig`, set defaults inside the `Replication:` literal:

```go
			ReconcileInterval:   5 * time.Minute,
			BackfillParallelism: 8,
			BackfillLookback:    24 * time.Hour,
```

In the config-merge section (near the existing `m.Replication.MasterHost = a.Replication.MasterHost`), add:

```go
	m.Replication.MasterQueryHost = a.Replication.MasterQueryHost
	if a.Replication.ReconcileInterval != 0 {
		m.Replication.ReconcileInterval = a.Replication.ReconcileInterval
	}
	if a.Replication.BackfillParallelism != 0 {
		m.Replication.BackfillParallelism = a.Replication.BackfillParallelism
	}
	if a.Replication.BackfillLookback != 0 {
		m.Replication.BackfillLookback = a.Replication.BackfillLookback
	}
```

- [ ] **Step 2: Write the failing test**

In `utils/config_test.go`, add a test that parses a YAML snippet with the new fields and asserts they land on the config:

```go
func TestParseConfig_ReplicaBackfillFields(t *testing.T) {
	yml := []byte(`
root_directory: /tmp/x
replication:
  master_host: "10.0.0.5:5996"
  master_query_host: "10.0.0.5:5995"
  reconcile_interval: 30s
  backfill_parallelism: 4
  backfill_lookback: 1h
`)
	cfg, err := utils.ParseConfig(yml)
	assert.Nil(t, err)
	assert.Equal(t, "10.0.0.5:5995", cfg.Replication.MasterQueryHost)
	assert.Equal(t, 30*time.Second, cfg.Replication.ReconcileInterval)
	assert.Equal(t, 4, cfg.Replication.BackfillParallelism)
	assert.Equal(t, time.Hour, cfg.Replication.BackfillLookback)
}
```

- [ ] **Step 3: Run test to verify it fails, then passes after Step 1**

Run: `go test ./utils/ -run TestParseConfig_ReplicaBackfillFields -v`
Expected: FAIL before Step 1 is applied (unknown fields / zero values), PASS after.

- [ ] **Step 4: Commit**

```bash
git add utils/config.go utils/config_test.go
git commit -m "feat(replication): add replica backfill config (master_query_host, reconcile_interval, backfill_parallelism)"
```

---

### Task 4: Watermark store

Persist the per-bucket last-synced epoch so restarts do not re-copy everything (and losing it is still safe — it just widens the idempotent backfill).

**Files:**
- Create: `replication/backfill/watermark.go`
- Test: `replication/backfill/watermark_test.go`

**Interfaces:**
- Produces:
  - `type Watermarks struct { ... }`
  - `func NewWatermarks(path string) (*Watermarks, error)` — loads existing JSON or starts empty.
  - `func (w *Watermarks) Get(tbk string) int64` — last-synced epoch (0 if none).
  - `func (w *Watermarks) Set(tbk string, epoch int64) error` — updates and persists (only advances; ignores lower values).

- [ ] **Step 1: Write the failing test**

Create `replication/backfill/watermark_test.go`:

```go
package backfill_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
)

func TestWatermarksPersistAndAdvanceOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wm.json")

	w, err := backfill.NewWatermarks(path)
	require.Nil(t, err)
	assert.Equal(t, int64(0), w.Get("AAPL/1Min/OHLCV"))

	require.Nil(t, w.Set("AAPL/1Min/OHLCV", 100))
	assert.Equal(t, int64(100), w.Get("AAPL/1Min/OHLCV"))

	// Lower values never regress the watermark.
	require.Nil(t, w.Set("AAPL/1Min/OHLCV", 50))
	assert.Equal(t, int64(100), w.Get("AAPL/1Min/OHLCV"))

	// Reload from disk: value survived.
	w2, err := backfill.NewWatermarks(path)
	require.Nil(t, err)
	assert.Equal(t, int64(100), w2.Get("AAPL/1Min/OHLCV"))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./replication/backfill/ -run TestWatermarksPersistAndAdvanceOnly -v`
Expected: FAIL (package/symbols do not exist).

- [ ] **Step 3: Implement**

Create `replication/backfill/watermark.go`:

```go
// Package backfill implements the replica-side pull backfill: it enumerates the
// master's buckets, reads historical ranges via the master's Query API, writes
// them locally (idempotent), and tracks a per-bucket watermark so it only
// fetches what is outstanding.
package backfill

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Watermarks stores the last-synced epoch per TimeBucketKey, persisted as JSON.
type Watermarks struct {
	mu   sync.Mutex
	path string
	m    map[string]int64
}

// NewWatermarks loads watermarks from path, or starts empty if it does not exist.
func NewWatermarks(path string) (*Watermarks, error) {
	w := &Watermarks{path: path, m: map[string]int64{}}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return w, nil
		}
		return nil, fmt.Errorf("read watermarks %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &w.m); err != nil {
		return nil, fmt.Errorf("parse watermarks %s: %w", path, err)
	}
	return w, nil
}

// Get returns the last-synced epoch for tbk, or 0 if none is recorded.
func (w *Watermarks) Get(tbk string) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.m[tbk]
}

// Set advances the watermark for tbk to epoch (never regresses) and persists.
func (w *Watermarks) Set(tbk string, epoch int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if epoch <= w.m[tbk] {
		return nil
	}
	w.m[tbk] = epoch
	return w.persistLocked()
}

func (w *Watermarks) persistLocked() error {
	data, err := json.Marshal(w.m)
	if err != nil {
		return fmt.Errorf("marshal watermarks: %w", err)
	}
	tmp := w.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write watermarks: %w", err)
	}
	return os.Rename(tmp, w.path) // atomic replace
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./replication/backfill/ -run TestWatermarksPersistAndAdvanceOnly -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add replication/backfill/watermark.go replication/backfill/watermark_test.go
git commit -m "feat(replication): add per-bucket watermark store for backfill"
```

---

### Task 5: Backfill client (enumerate + query the master)

A gRPC client to the master's MAIN service that lists buckets and reads a range into a `ColumnSeriesMap`.

**Files:**
- Create: `replication/backfill/client.go`
- Test: `replication/backfill/client_test.go`

**Interfaces:**
- Produces:
  - `type MasterAPI interface { ListTBKs(ctx) ([]string, error); QueryRange(ctx, tbk string, startEpoch, endEpoch int64) (io.ColumnSeriesMap, error) }`
  - `type GRPCClient struct { ... }` implementing `MasterAPI`, built by `func NewGRPCClient(cc grpc.ClientConnInterface) *GRPCClient`.

- [ ] **Step 1: Write the failing test (interface shape only)**

Create `replication/backfill/client_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./replication/backfill/ -run TestGRPCClientImplementsMasterAPI -v`
Expected: FAIL (symbols undefined).

- [ ] **Step 3: Implement the client**

Create `replication/backfill/client.go`:

```go
package backfill

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/alpacahq/marketstore/v4/frontend"
	pb "github.com/alpacahq/marketstore/v4/proto"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// MasterAPI is the subset of the master's main gRPC service the backfill needs.
type MasterAPI interface {
	ListTBKs(ctx context.Context) ([]string, error)
	QueryRange(ctx context.Context, tbk string, startEpoch, endEpoch int64) (io.ColumnSeriesMap, error)
}

// GRPCClient reads from the master's main Marketstore gRPC service.
type GRPCClient struct {
	cli pb.MarketstoreClient
}

func NewGRPCClient(cc grpc.ClientConnInterface) *GRPCClient {
	return &GRPCClient{cli: pb.NewMarketstoreClient(cc)}
}

// ListTBKs returns every bucket on the master as "Symbol/Timeframe/AttrGroup".
func (c *GRPCClient) ListTBKs(ctx context.Context) ([]string, error) {
	resp, err := c.cli.ListSymbols(ctx, &pb.ListSymbolsRequest{
		Format: pb.ListSymbolsRequest_TIME_BUCKET_KEY,
	})
	if err != nil {
		return nil, fmt.Errorf("list symbols: %w", err)
	}
	return resp.GetResults(), nil
}

// QueryRange reads [startEpoch, endEpoch] (inclusive of start) for one bucket.
func (c *GRPCClient) QueryRange(ctx context.Context, tbk string, startEpoch, endEpoch int64) (io.ColumnSeriesMap, error) {
	resp, err := c.cli.Query(ctx, &pb.MultiQueryRequest{
		Requests: []*pb.QueryRequest{{
			Destination: tbk,
			EpochStart:  startEpoch,
			EpochEnd:    endEpoch,
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", tbk, err)
	}
	csm := io.NewColumnSeriesMap()
	for _, r := range resp.GetResponses() {
		if r.GetResult() == nil {
			continue
		}
		part, err := frontend.ToNumpyMultiDataSet(r.GetResult()).ToColumnSeriesMap()
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", tbk, err)
		}
		for k, v := range part {
			csm[k] = v
		}
	}
	return csm, nil
}
```

Note: if importing `frontend` from `replication/backfill` produces an import cycle at build time, inline the ~12-line proto→`io.NumpyMultiDataset` conversion from `frontend.ToNumpyMultiDataSet` (`frontend/grpc.go:214-225`) here instead. Verify with `go build ./...`.

- [ ] **Step 4: Run test + build**

Run: `go build ./... && go test ./replication/backfill/ -run TestGRPCClientImplementsMasterAPI -v`
Expected: build OK, test PASS. (If build fails with an import cycle, apply the inline note above.)

- [ ] **Step 5: Commit**

```bash
git add replication/backfill/client.go replication/backfill/client_test.go
git commit -m "feat(replication): add backfill client (ListTBKs + QueryRange over master gRPC)"
```

---

### Task 6: Backfill one bucket (worker unit)

The pure per-bucket step: query `[watermark − lookback, now]`, write locally, return the newest epoch written. The lookback re-pulls a trailing window so master-side corrections to recent epochs missed while disconnected are healed (idempotent, so overlap is free). Unit-tested with a fake `MasterAPI` and a fake write function — no network, no disk.

**Files:**
- Create: `replication/backfill/worker.go`
- Test: `replication/backfill/worker_test.go`

**Interfaces:**
- Consumes: `MasterAPI` (Task 5), `Watermarks` (Task 4).
- Produces:
  - `type WriteFunc func(csm io.ColumnSeriesMap, isVariableLength bool) error`
  - `func BackfillBucket(ctx, api MasterAPI, write WriteFunc, wm *Watermarks, tbk string, now int64, lookback time.Duration, isVariable bool) error`

- [ ] **Step 1: Write the failing test**

Create `replication/backfill/worker_test.go`:

```go
package backfill_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type fakeAPI struct {
	gotStart, gotEnd int64
	ret              io.ColumnSeriesMap
}

func (f *fakeAPI) ListTBKs(_ context.Context) ([]string, error) { return nil, nil }
func (f *fakeAPI) QueryRange(_ context.Context, _ string, s, e int64) (io.ColumnSeriesMap, error) {
	f.gotStart, f.gotEnd = s, e
	return f.ret, nil
}

func TestBackfillBucketQueriesFromWatermarkAndAdvances(t *testing.T) {
	tbk := io.NewTimeBucketKey("AAPL/1Min/OHLCV")
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{100, 200, 300})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tbk, cs)

	api := &fakeAPI{ret: csm}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	require.Nil(t, wm.Set("AAPL/1Min/OHLCV", 100))

	var wrote io.ColumnSeriesMap
	write := func(m io.ColumnSeriesMap, _ bool) error { wrote = m; return nil }

	err = backfill.BackfillBucket(context.Background(), api, write, wm, "AAPL/1Min/OHLCV", 999, 0, false)
	require.Nil(t, err)

	// Queried from just after the watermark to now.
	assert.Equal(t, int64(101), api.gotStart)
	assert.Equal(t, int64(999), api.gotEnd)
	// Wrote the returned data and advanced the watermark to the newest epoch.
	assert.NotNil(t, wrote)
	assert.Equal(t, int64(300), wm.Get("AAPL/1Min/OHLCV"))
}

func TestBackfillBucketLookbackWidensStart(t *testing.T) {
	api := &fakeAPI{ret: io.NewColumnSeriesMap()} // empty; we only check the range
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("AAPL/1Min/OHLCV", 100)

	err := backfill.BackfillBucket(context.Background(), api, nil, wm, "AAPL/1Min/OHLCV", 999, 60*time.Second, false)
	require.Nil(t, err)

	// Start reaches back behind the watermark by the lookback: 100+1-60 = 41.
	assert.Equal(t, int64(41), api.gotStart)
	assert.Equal(t, int64(999), api.gotEnd)

	// Lookback never produces a start below 1.
	_ = wm.Set("X/1Min/OHLCV", 10)
	err = backfill.BackfillBucket(context.Background(), api, nil, wm, "X/1Min/OHLCV", 999, time.Hour, false)
	require.Nil(t, err)
	assert.Equal(t, int64(1), api.gotStart)
}

func TestBackfillBucketNoDataLeavesWatermark(t *testing.T) {
	api := &fakeAPI{ret: io.NewColumnSeriesMap()} // empty
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	_ = wm.Set("X/1Min/OHLCV", 500)
	called := false
	write := func(io.ColumnSeriesMap, bool) error { called = true; return nil }

	err := backfill.BackfillBucket(context.Background(), api, write, wm, "X/1Min/OHLCV", 999, 0, false)
	require.Nil(t, err)
	assert.False(t, called, "must not write when there is no data")
	assert.Equal(t, int64(500), wm.Get("X/1Min/OHLCV"))
}
```

Add `"time"` to the test imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./replication/backfill/ -run TestBackfillBucket -v`
Expected: FAIL (symbols undefined).

- [ ] **Step 3: Implement**

Create `replication/backfill/worker.go`:

```go
package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/utils/io"
)

// WriteFunc writes a ColumnSeriesMap locally. Mirrors the replayer's write seam
// (executor.WriteCSM / GetDefaultWriter().WriteCSM).
type WriteFunc func(csm io.ColumnSeriesMap, isVariableLength bool) error

// BackfillBucket queries [watermark+1−lookback, now] for one bucket, writes
// what it gets, and advances the watermark to the newest epoch written. The
// lookback re-pulls a trailing window to heal master-side corrections to
// recent epochs that were missed while disconnected; corrections older than
// the lookback require a deep resync (delete the watermark file). A no-data
// result is a no-op. Writes are idempotent (WriteCSM overwrites by epoch), so
// overlap and re-running are always safe.
func BackfillBucket(
	ctx context.Context, api MasterAPI, write WriteFunc, wm *Watermarks,
	tbk string, now int64, lookback time.Duration, isVariable bool,
) error {
	start := wm.Get(tbk) + 1 - int64(lookback.Seconds())
	if start < 1 {
		start = 1
	}
	if start > now {
		return nil
	}
	csm, err := api.QueryRange(ctx, tbk, start, now)
	if err != nil {
		return err
	}
	if len(csm) == 0 {
		return nil
	}

	newest := int64(0)
	for _, cs := range csm {
		epochs := cs.GetEpoch()
		if len(epochs) == 0 {
			continue
		}
		if last := epochs[len(epochs)-1]; last > newest {
			newest = last
		}
	}
	if newest == 0 {
		return nil // rows present but no Epoch column — treat as no-op
	}

	if err := write(csm, isVariable); err != nil {
		return fmt.Errorf("write %s: %w", tbk, err)
	}
	return wm.Set(tbk, newest)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./replication/backfill/ -run TestBackfillBucket -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add replication/backfill/worker.go replication/backfill/worker_test.go
git commit -m "feat(replication): backfill a single bucket from watermark with idempotent write"
```

---

### Task 7: Driver — bootstrap all buckets

Enumerate every bucket and backfill each concurrently with a bounded worker pool. Phase 1 targets FIXED-record buckets (OHLCV bars); variable-length ticks are Task 11.

**Files:**
- Create: `replication/backfill/driver.go`
- Test: `replication/backfill/driver_test.go`

**Interfaces:**
- Consumes: `MasterAPI`, `Watermarks`, `WriteFunc`, `worker.BackfillBucket`.
- Produces:
  - `type Driver struct { ... }`
  - `func NewDriver(api MasterAPI, write WriteFunc, wm *Watermarks, parallelism int, lookback time.Duration, isVariable func(tbk string) bool) *Driver`
  - `func (d *Driver) Reconcile(ctx context.Context, now int64) error` — enumerate + backfill every bucket once.

- [ ] **Step 1: Write the failing test**

Create `replication/backfill/driver_test.go`:

```go
package backfill_test

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alpacahq/marketstore/v4/replication/backfill"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

type listAPI struct {
	tbks    []string
	mu      sync.Mutex
	queried []string
}

func (l *listAPI) ListTBKs(_ context.Context) ([]string, error) { return l.tbks, nil }
func (l *listAPI) QueryRange(_ context.Context, tbk string, _, _ int64) (io.ColumnSeriesMap, error) {
	l.mu.Lock()
	l.queried = append(l.queried, tbk)
	l.mu.Unlock()
	tk := io.NewTimeBucketKey(tbk)
	cs := io.NewColumnSeries()
	cs.AddColumn("Epoch", []int64{10})
	csm := io.NewColumnSeriesMap()
	csm.AddColumnSeries(*tk, cs)
	return csm, nil
}

func TestDriverReconcileBackfillsEveryBucket(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV", "MSFT/1D/OHLCV"}}
	wm, err := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	require.Nil(t, err)
	write := func(io.ColumnSeriesMap, bool) error { return nil }

	d := backfill.NewDriver(api, write, wm, 4, 0, func(string) bool { return false })
	require.Nil(t, d.Reconcile(context.Background(), 1000))

	sort.Strings(api.queried)
	assert.Equal(t, []string{"AAPL/1Min/OHLCV", "MSFT/1D/OHLCV"}, api.queried)
	assert.Equal(t, int64(10), wm.Get("AAPL/1Min/OHLCV"))
	assert.Equal(t, int64(10), wm.Get("MSFT/1D/OHLCV"))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./replication/backfill/ -run TestDriverReconcile -v`
Expected: FAIL (symbols undefined).

- [ ] **Step 3: Implement**

Create `replication/backfill/driver.go`:

```go
package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/massive/worker"
	"github.com/alpacahq/marketstore/v4/utils/log"
)

// Driver reconciles the whole bucket set against the master.
type Driver struct {
	api         MasterAPI
	write       WriteFunc
	wm          *Watermarks
	parallelism int
	lookback    time.Duration
	isVariable  func(tbk string) bool
}

func NewDriver(api MasterAPI, write WriteFunc, wm *Watermarks, parallelism int, lookback time.Duration, isVariable func(tbk string) bool) *Driver {
	if parallelism <= 0 {
		parallelism = 8
	}
	return &Driver{api: api, write: write, wm: wm, parallelism: parallelism, lookback: lookback, isVariable: isVariable}
}

// Reconcile enumerates every bucket on the master and backfills each from its
// watermark up to now, concurrently. Per-bucket errors are logged, not fatal:
// a transient failure is retried on the next reconcile.
func (d *Driver) Reconcile(ctx context.Context, now int64) error {
	tbks, err := d.api.ListTBKs(ctx)
	if err != nil {
		return fmt.Errorf("enumerate buckets: %w", err)
	}
	wp := worker.NewWorkerPool(ctx, d.parallelism)
	for _, tbk := range tbks {
		tbk := tbk
		wp.Do(func() {
			if err := BackfillBucket(ctx, d.api, d.write, d.wm, tbk, now, d.lookback, d.isVariable(tbk)); err != nil {
				log.Warn("[replication-backfill] %s: %v", tbk, err)
			}
		})
	}
	wp.CloseAndWait()
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./replication/backfill/ -run TestDriverReconcile -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add replication/backfill/driver.go replication/backfill/driver_test.go
git commit -m "feat(replication): backfill driver reconciles every bucket concurrently"
```

---

## Phase 2 — Catch-up loop + variable-length support

### Task 8: Periodic reconcile loop

Run `Reconcile` at startup (bootstrap) and then on a ticker forever. This single loop covers bootstrap, in-connection drops, and reconnect gaps (a disconnect's gap is healed at the next tick).

**Files:**
- Modify: `replication/backfill/driver.go`
- Test: `replication/backfill/driver_test.go`

**Interfaces:**
- Produces: `func (d *Driver) Run(ctx context.Context, interval time.Duration, nowFn func() int64)` — runs one immediate reconcile, then one per interval until ctx is cancelled.

- [ ] **Step 1: Write the failing test**

Add to `replication/backfill/driver_test.go`:

```go
func TestDriverRunReconcilesImmediatelyThenStops(t *testing.T) {
	api := &listAPI{tbks: []string{"AAPL/1Min/OHLCV"}}
	wm, _ := backfill.NewWatermarks(t.TempDir() + "/wm.json")
	d := backfill.NewDriver(api, func(io.ColumnSeriesMap, bool) error { return nil }, wm, 2, 0, func(string) bool { return false })

	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx, time.Hour, func() int64 { return 1000 }) // long interval: only the immediate pass runs
	// Give the immediate reconcile time to happen, then stop.
	assert.Eventually(t, func() bool { return wm.Get("AAPL/1Min/OHLCV") == 10 }, 2*time.Second, 10*time.Millisecond)
	cancel()
}
```

Add `"time"` to the test imports.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./replication/backfill/ -run TestDriverRun -v`
Expected: FAIL (`Run` undefined).

- [ ] **Step 3: Implement**

Append to `replication/backfill/driver.go` (`"time"` is already imported from Task 7):

```go
// Run performs an immediate reconcile (bootstrap), then reconciles once per
// interval until ctx is cancelled. This one loop guarantees eventual
// completeness regardless of live-stream drops or disconnects.
func (d *Driver) Run(ctx context.Context, interval time.Duration, nowFn func() int64) {
	if err := d.Reconcile(ctx, nowFn()); err != nil && ctx.Err() == nil {
		log.Warn("[replication-backfill] bootstrap reconcile: %v", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.Reconcile(ctx, nowFn()); err != nil && ctx.Err() == nil {
				log.Warn("[replication-backfill] periodic reconcile: %v", err)
			}
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./replication/backfill/ -run TestDriverRun -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add replication/backfill/driver.go replication/backfill/driver_test.go
git commit -m "feat(replication): periodic reconcile loop (bootstrap + gap healing)"
```

---

### Task 9: Variable-length (tick) record-type resolution + fidelity test

Ticks (TRADE/QUOTE) are variable-length; writing them requires `isVariableLength=true` and a lossless round-trip. Resolve the flag from the local catalog and prove a tick round-trips.

**Files:**
- Modify: `replication/backfill/driver.go` wiring is unaffected; add resolver `replication/backfill/recordtype.go`
- Test: `replication/backfill/recordtype_test.go`

**Interfaces:**
- Produces: `func IsVariableTBK(catDir *catalog.Directory, tbk string) bool` — reports whether the local bucket for tbk is variable-length.

- [ ] **Step 1: Write the failing test**

Create `replication/backfill/recordtype_test.go` using a temp catalog with one fixed and one variable bucket. Model it on `frontend/list_symbols_test.go`'s `setupListSymbols` (which builds a `catalog.Directory` under `t.TempDir()`); create `AAPL/1Min/OHLCV` (fixed) and `AAPL/1Sec/TRADE` (variable), then:

```go
func TestIsVariableTBK(t *testing.T) {
	catDir := setupCatalogWithFixedAndVariable(t) // helper: see frontend/list_symbols_test.go pattern
	assert.False(t, backfill.IsVariableTBK(catDir, "AAPL/1Min/OHLCV"))
	assert.True(t, backfill.IsVariableTBK(catDir, "AAPL/1Sec/TRADE"))
}
```

(Write `setupCatalogWithFixedAndVariable` in the test file, following the existing catalog-construction test helpers; use `io.VARIABLE`/`io.FIXED` record types when creating the buckets.)

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./replication/backfill/ -run TestIsVariableTBK -v`
Expected: FAIL (`IsVariableTBK` undefined).

- [ ] **Step 3: Implement**

Create `replication/backfill/recordtype.go`:

```go
package backfill

import (
	"github.com/alpacahq/marketstore/v4/catalog"
	"github.com/alpacahq/marketstore/v4/utils/io"
)

// IsVariableTBK reports whether the local bucket for tbk stores variable-length
// records. Returns false if the bucket is unknown locally (a fixed OHLCV bucket
// will be created on first write from the CSM's shape).
func IsVariableTBK(catDir *catalog.Directory, tbk string) bool {
	tk := io.NewTimeBucketKey(tbk)
	tbi, err := catDir.GetLatestTimeBucketInfoFromKey(tk)
	if err != nil {
		return false
	}
	return tbi.GetRecordType() == io.VARIABLE
}
```

Verify the exact catalog accessor name against `catalog/catalog.go` (e.g. `GetLatestTimeBucketInfoFromKey`); adjust to the real method if it differs, keeping the same behavior.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./replication/backfill/ -run TestIsVariableTBK -v`
Expected: PASS

- [ ] **Step 5: Add an end-to-end variable-length fidelity test**

Add `replication/backfill/tick_fidelity_test.go` that: builds a local catalog + writer, writes a variable-length TRADE bar directly, reads it back via a real `executor.NewReader` range query into a CSM, then writes that CSM back through the writer with `isVariableLength=true`, reads again, and asserts the rows (Epoch, Nanoseconds, price/size columns) are identical. This proves the query→write round-trip the backfill relies on is lossless for ticks. Model reader/writer setup on `contrib/streamreplay/replayworker/session.go:defaultQueryRange` and existing executor writer tests.

- [ ] **Step 6: Run + commit**

Run: `go test ./replication/backfill/ -v`
Expected: PASS

```bash
git add replication/backfill/recordtype.go replication/backfill/recordtype_test.go replication/backfill/tick_fidelity_test.go
git commit -m "feat(replication): resolve variable-length record type + prove tick round-trip fidelity"
```

---

## Phase 3 — Wiring and integration

### Task 10: Wire the driver into the replica (DI + startup)

Construct the backfill client, watermarks, and driver when the instance is a replica with `master_query_host` set, and run it alongside the existing live receiver.

**Files:**
- Modify: `internal/di/replication.go`
- Modify: `internal/di/container.go` (add a field)
- Modify: `cmd/start/main.go` (start the driver)

**Interfaces:**
- Consumes: `backfill.NewGRPCClient`, `backfill.NewWatermarks`, `backfill.NewDriver`, `backfill.IsVariableTBK`; `c.GetDefaultWriter().WriteCSM`; `c.GetCatalogDir()`; `c.GetAbsRootDir()`.
- Produces: `func (c *Container) GetReplicationBackfillDriver() *backfill.Driver` (nil when not a replica or `master_query_host` empty).

- [ ] **Step 1: Add container field**

In `internal/di/container.go`, add:

```go
	replicationBackfill *backfill.Driver
```

and import `"github.com/alpacahq/marketstore/v4/replication/backfill"`.

- [ ] **Step 2: Add the getter**

In `internal/di/replication.go`:

```go
func (c *Container) GetReplicationBackfillDriver() *backfill.Driver {
	if c.mktsConfig.Replication.MasterQueryHost == "" {
		return nil // live-only or master; no backfill
	}
	if c.replicationBackfill != nil {
		return c.replicationBackfill
	}

	conn, err := grpc.Dial(c.mktsConfig.Replication.MasterQueryHost,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("replication backfill: dial %s: %v", c.mktsConfig.Replication.MasterQueryHost, err))
	}
	api := backfill.NewGRPCClient(conn)

	wmPath := filepath.Join(c.GetAbsRootDir(), "replication_watermarks.json")
	wm, err := backfill.NewWatermarks(wmPath)
	if err != nil {
		panic(fmt.Sprintf("replication backfill: watermarks: %v", err))
	}

	writer := c.GetDefaultWriter()
	write := func(csm io.ColumnSeriesMap, isVar bool) error { return writer.WriteCSM(csm, isVar) }
	catDir := c.GetCatalogDir()
	isVar := func(tbk string) bool { return backfill.IsVariableTBK(catDir, tbk) }

	c.replicationBackfill = backfill.NewDriver(api, write, wm,
		c.mktsConfig.Replication.BackfillParallelism,
		c.mktsConfig.Replication.BackfillLookback, isVar)
	return c.replicationBackfill
}
```

Add imports: `"path/filepath"`, `"github.com/alpacahq/marketstore/v4/replication/backfill"`, `"github.com/alpacahq/marketstore/v4/utils/io"`, `"google.golang.org/grpc/credentials/insecure"` (already imported in this file).

- [ ] **Step 3: Start it in cmd/start**

In `cmd/start/main.go`, after the existing replication client goroutine (around `c.GetReplicationClientWithRetry().Run(globalCtx)`), add:

```go
	// Start the replication backfill reconciler (bootstrap + periodic catch-up).
	if driver := c.GetReplicationBackfillDriver(); driver != nil {
		go driver.Run(globalCtx, config.Replication.ReconcileInterval, func() int64 { return time.Now().Unix() })
	}
```

- [ ] **Step 4: Build + full unit suite**

Run: `go build ./... && go test ./replication/... ./internal/di/... ./utils/...`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/di/replication.go internal/di/container.go cmd/start/main.go
git commit -m "feat(replication): wire backfill reconciler into replica startup"
```

---

### Task 11: Integration test — bootstrap of pre-existing data

Prove the thing the built-in feature cannot do: data written to the master BEFORE the replica connects still appears on the replica.

**Files:**
- Modify: `tests/replication/config/mkts-replica.yml` (add `master_query_host` + a short `reconcile_interval`)
- Create: `tests/replication/tests/test_bootstrap.py`

- [ ] **Step 1: Update replica config**

In `tests/replication/config/mkts-replica.yml`, under `replication:` add:

```yaml
  master_query_host: "replication_tests_mstore_master:5997"  # master's grpc_listen_port
  reconcile_interval: 2s
  backfill_parallelism: 4
```

(5997 is the master's `grpc_listen_port` in `mkts-master.yml`.)

- [ ] **Step 2: Write the test**

Create `tests/replication/tests/test_bootstrap.py`:

```python
import os
import time
import numpy as np
import pandas as pd
import pymarketstore as pymkts

master = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5996)}/rpc")
replica = pymkts.Client(f"http://127.0.0.1:{os.getenv('REPLICA_PORT',5999)}/rpc")


def test_bootstrap_of_preexisting_data():
    # Written to master; replica must acquire it via backfill, not the live feed.
    data = np.array([(pd.Timestamp('2018-01-01 00:00').value / 10**9, 1.0, 2.0)],
                    dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')])
    master.write(data, 'BOOT/1Min/OHLCV')

    # Wait for a reconcile cycle (reconcile_interval=2s) to bootstrap it.
    deadline = time.time() + 20
    got = None
    while time.time() < deadline:
        resp = replica.query(pymkts.Params('BOOT', '1Min', 'OHLCV'))
        try:
            got = resp.first().df()
            if len(got) > 0:
                break
        except Exception:
            pass
        time.sleep(1)

    assert got is not None and len(got) == 1
    assert got['High'].iloc[0] == 1.0

    master.destroy('BOOT/1Min/OHLCV')
    replica.destroy('BOOT/1Min/OHLCV')
```

Confirm the replica's JSON-RPC port env var name against the existing harness (`tests/replication/Makefile` / `test_write.py` use `MARKETSTORE_PORT=5996` for master and `5999` for the replica); align `REPLICA_PORT` accordingly or hardcode 5999 as `test_write.py` does.

- [ ] **Step 3: Run the replication integration suite**

Run: `make replication-test`
Expected: existing tests plus `test_bootstrap_of_preexisting_data` PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/replication/config/mkts-replica.yml tests/replication/tests/test_bootstrap.py
git commit -m "test(replication): integration test for bootstrap of pre-existing data"
```

---

### Task 12: Integration test — master survives a frozen replica

Prove the isolation guarantee end-to-end: a replica that stops reading does not stall the master.

**Files:**
- Create: `tests/replication/tests/test_master_isolation.py`

- [ ] **Step 1: Write the test**

Create `tests/replication/tests/test_master_isolation.py`. Strategy: hold a raw replication stream open to the master but never read from it (fill its 500-buffer), then hammer the master with writes and assert every write still succeeds quickly and the master keeps answering queries. Because a pure black-box freeze is awkward over the JSON-RPC client, assert the observable guarantee: with the replica container paused, master writes/queries continue and the master process stays up.

```python
import os, time, subprocess
import numpy as np, pandas as pd
import pymarketstore as pymkts

master = pymkts.Client(f"http://127.0.0.1:{os.getenv('MARKETSTORE_PORT',5996)}/rpc")


def test_master_writes_survive_frozen_replica():
    # Freeze the replica container so it stops draining its replication stream.
    subprocess.run(["docker", "pause", "replication_tests_mstore_replica"], check=False)
    try:
        for i in range(200):
            data = np.array([((pd.Timestamp('2019-01-01 00:00').value / 10**9) + i, 1.0, 2.0)],
                            dtype=[('Epoch', 'i8'), ('High', 'f4'), ('Low', 'f4')])
            t0 = time.time()
            master.write(data, 'ISO/1Min/OHLCV')
            assert time.time() - t0 < 2.0, "master write stalled while replica was frozen"
    finally:
        subprocess.run(["docker", "unpause", "replication_tests_mstore_replica"], check=False)
    master.destroy('ISO/1Min/OHLCV')
```

Confirm the replica container name against `tests/replication/Makefile` / docker-compose service names; adjust the `docker pause` target to the real name.

- [ ] **Step 2: Run**

Run: `make replication-test`
Expected: PASS; no master stall while the replica is paused.

- [ ] **Step 3: Commit**

```bash
git add tests/replication/tests/test_master_isolation.py
git commit -m "test(replication): master writes survive a frozen replica (isolation guarantee)"
```

---

## Self-review notes (traceability to spec)

- Spec §3 (isolation F1/F2/F3) → Tasks 1, 2; verified by Tasks 12 and the `-race` run in Task 2. F3 also removes the send-on-closed-channel panic: `Unregister` deletes without closing, since fan-out may hold a snapshot reference.
- Spec §2 / §4 (pull backfill, no new read endpoints) → Tasks 4–7 (watermark, client, worker, driver).
- Spec §2.1 correctness model (bootstrap / periodic reconcile / reconnect gap) → Task 8. Reconnect-gap is covered by periodic reconcile (a disconnect's gap is healed at the next tick); a dedicated reconnect-triggered reconcile is the noted future optimization, not required for correctness.
- Spec tick-fidelity risk → Task 9 (record-type resolution + lossless round-trip test).
- Spec §6 (config) → Task 3.
- Spec §2.1/§6 correction healing (`backfill_lookback`) → Tasks 3, 6 (start = watermark+1−lookback, clamped to 1); corrections older than the window are the documented §9 divergence, healed only by deep resync (delete the watermark file).
- Spec §7 (testing: bootstrap, isolation, catch-up) → Tasks 11, 12 (catch-up is exercised by the reconcile loop in Task 8's unit test and the bootstrap timing in Task 11).
- Spec §9 non-goals (deletes, WS endpoint) → not implemented, by design.
```
