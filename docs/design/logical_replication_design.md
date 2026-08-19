# Logical Replication Design

Status: draft
Date: 2026-08-18
Author: brian

## 1. Problem

MarketStore ships a WAL-based master→replica replication feature
(`replication/`, `internal/di/replication.go`). Its live-write feed works and is
covered by CI (`tests/replication`), but it is unusable as a real replica
because it has **no bootstrap and no catch-up**: a replica only receives writes
that occur *after* it connects, pre-existing data is never transferred, and any
write missed during a disconnect is lost forever. It also has an isolation
defect: a slow or broken replica can stall the master's write path (see §3).

We want a replica for a **dev / analytics mirror** of a production instance on
the same LAN:

- **Freshness target:** seconds-stale is fine; minutes is not.
- **Hard constraint:** a broken, slow, or disconnected replica must **never**
  degrade the master's availability or write throughput. Replica failure is
  acceptable; master failure is not.
- **Scope:** deletes are **out of scope** (accepted divergence). Point-in-time
  consistency is not required; per-bucket eventual consistency is sufficient.

## 2. Approach

Replication is two independent problems: **bootstrap/catch-up** (copy existing
and missed data) and **live tail** (apply new writes). We use the right tool for
each and deliberately keep them decoupled:

- **Live tail = the existing gRPC WAL feed (reused).** The tap
  (`executor/wal.go`), transaction-group serialization (`replication/sender.go`,
  `grpc_server.go`), and the replica-side decode+apply — including
  variable-length tick reconstruction (`replication/replay.go`) — already exist
  and are tested. We reuse them as-is except for the isolation fixes in §3. We do
  **not** build a new WebSocket endpoint: replication is server-to-server on the
  LAN and never traverses the app proxy, so gRPC's separate port costs nothing,
  and re-implementing a tested path would add effort and risk for no benefit.

- **Bootstrap + catch-up = pull backfill (new).** The replica pulls historical
  ranges from the master's **existing** query API. No new server read endpoints
  are required:
  - enumerate every bucket with `ListSymbols(format=TBK)` →
    `catalog.ListTimeBucketKeyNames`;
  - for each bucket, `Query` the range `[watermark, now]`;
  - write locally via the normal write path (idempotent: `WriteCSM` overwrites
    by epoch);
  - advance a per-bucket watermark.

### 2.1 Correctness model

The live stream is treated as **best-effort** (it may drop under load or
disconnect — see §3). Completeness is **guaranteed by the backfill reconciler**,
not the stream. The reconciler runs in three situations:

1. **Bootstrap** — at startup, backfill every bucket from the beginning.
2. **Reconnect gap** — after any live-stream disconnect, backfill
   `[watermark, now]` for all buckets before/while resuming the stream.
3. **Periodic reconcile** — on a slow timer, re-pull
   `[watermark − backfill_lookback, now]` for all buckets to heal any messages
   dropped while connected and any master-side corrections to recent epochs
   missed while disconnected.

Master-side **corrections** (writes to older epochs) propagate through the live
stream like any other write — the WAL tap fires on every flush regardless of
epoch. The lookback window exists only for corrections that coincide with a
drop or disconnect: without it, an old-epoch write sits *behind* the watermark
and would never be re-pulled.

Because every write is idempotent by epoch, the live stream and the backfill may
overlap freely; applying the same bar twice is harmless. The system therefore
degrades gracefully: if the live stream fails entirely, the replica still
converges at the reconcile cadence, just less fresh.

This inverts the built-in feature's fatal flaw: a dropped connection is no longer
data loss, only a scheduled backfill.

## 3. Isolation: the master must never stall (required)

The current live path can back-pressure the master. On each WAL flush,
`Sender.Send` does a blocking send into a bounded channel (`sender.go`, size
500); `SendReplicationMessage` (`grpc_server.go`) then does a blocking send into
each replica's bounded stream channel (size 500). A replica that stops draining
fills its stream channel, which blocks the sender goroutine, which stops draining
the sender channel, which blocks `Send`, which blocks the **WAL flush** — i.e.
the master's write path. A separate defect: `StreamChannels` is an unguarded map
mutated from `GetWALStream` while `SendReplicationMessage` ranges over it, which
can panic. Related: `GetWALStream` closes a replica's channel on disconnect
while fan-out may still hold a reference to it — a send on a closed channel
panics the master.

Fixes (all in `replication/`):

- **F1 — non-blocking handoff from the write path.** `Sender.Send` becomes a
  non-blocking send: if the buffer is full, drop the transaction group and
  increment a dropped-message counter/metric. The write path never blocks on
  replication, regardless of replica health.
- **F2 — non-blocking, isolated fan-out.** `SendReplicationMessage` sends to each
  replica's channel non-blocking. A replica whose buffer is full does not block
  the sender or any peer. On overflow for a given replica, drop for that replica
  (its gap is healed by backfill). Optionally disconnect the overflowing replica
  to force an immediate reconnect+catch-up.
- **F3 — guard the subscriber map.** Unexport `StreamChannels` behind
  mutex-guarded `Register`/`Unregister` methods so connect/disconnect and
  fan-out cannot race. On disconnect the channel is deliberately **not
  closed** — fan-out may hold a reference and a send on a closed channel
  panics; an untracked channel is simply garbage-collected.

After F1–F3, no replica state — full buffer, slow socket, or crash — can affect
the master. Drops are surfaced via metrics and healed by the reconciler.

## 4. Components

### Server (small, mostly fixes)

- `replication/sender.go` — F1 (non-blocking `Send` + dropped counter).
- `replication/grpc_server.go` — F2 (non-blocking fan-out, overflow policy) and
  F3 (map guard).
- No new read endpoints: backfill reuses the existing `Query` and `ListSymbols`
  gRPC service.

### Replica (the real new work)

A replica is a normal MarketStore instance started with `master_host` set. New
components, wired in `internal/di` when replication is configured as a replica:

- **Backfill client** — a gRPC client to the master's **main** service
  (`Query`, `ListSymbols`) for reading historical ranges. This is distinct from
  the replication-stream client, and needs the master's main gRPC address (see
  §6 config).
- **Watermark store** — persists the per-bucket last-synced epoch across
  restarts. A simple local checkpoint file keyed by TBK is sufficient (mirrors
  the existing massive checkpoint pattern). Lost watermarks are safe: they just
  cause a wider (still idempotent) backfill.
- **Backfill worker** — enumerate TBKs → for each, `Query [watermark, now]` →
  write locally → advance watermark. Runs concurrently across buckets with a
  bounded worker pool.
- **Replication driver** — orchestrates the lifecycle: start the live receiver,
  run bootstrap, schedule periodic reconcile, and on receiver reconnect run a
  gap backfill. Owns the "best-effort stream + guaranteed reconciler" policy.

Reused unchanged: `replication/receiver.go`, `replication/replay.go`,
`replication/retry.go`.

## 5. Data flow

Replica startup:

1. Connect the live replication stream and begin applying transaction groups
   immediately (idempotent).
2. Concurrently run bootstrap backfill for all buckets `[begin, now]`.
3. Enter steady state: live stream provides seconds-freshness; periodic reconcile
   and reconnect-gap backfill guarantee completeness.

Steady state per write on master: WAL flush → `Sender.Send` (non-blocking) →
fan-out (non-blocking) → replica receiver → `replay` → local `WriteCSM`.

## 6. Configuration

Existing (`utils/config.go` `ReplicationSetting`): `Enabled`, `MasterHost`
(replication stream, default port 5996), TLS, retry.

New fields required on the replica for backfill:

- `master_query_host` — the master's **main** gRPC address (e.g. `host:5995`)
  used by the backfill client. Distinct from `master_host` (the stream port).
- `reconcile_interval` — periodic reconcile cadence (default e.g. 5m).
- `backfill_lookback` — trailing window re-pulled on every reconcile (default
  24h). Heals master-side corrections to epochs within the window that were
  missed while disconnected or dropped. Re-pulling already-held data is
  harmless (idempotent by epoch); the cost is only query volume.
- Optional: `backfill_parallelism`, backfill start bound.

TLS is optional and off by default (LAN, trusted).

## 7. Testing

- **Server isolation (F1–F3):**
  - `Send` never blocks when its buffer is full (drop path taken).
  - A stalled replica channel does not block `SendReplicationMessage` or other
    replicas.
  - Race test (`-race`) for concurrent connect/disconnect vs fan-out on
    `StreamChannels`.
- **Replica backfill:** enumerate → query → write → advance watermark; idempotent
  re-apply of the same epoch; reconnect triggers gap backfill; watermark persists
  across restart.
- **Integration (extends `tests/replication`):**
  - **Bootstrap:** write to the master *before* the replica connects; start the
    replica; assert the pre-existing data appears. (This is exactly what the
    built-in feature cannot do.)
  - **Master isolation:** freeze the replica; assert the master's writes/queries
    continue unaffected and drop metrics increment.
  - **Catch-up:** disconnect the replica during writes, reconnect, assert
    convergence.

## 8. Phasing

- **Phase 0 — server isolation (F1–F3).** Independently shippable; makes the
  existing feature safe to run at all. Small.
- **Phase 1 — replica bootstrap.** Backfill client, watermark store, backfill
  worker, driver wiring; enable live+bootstrap on startup. The core new
  capability.
- **Phase 2 — catch-up.** Reconnect-gap backfill and periodic reconcile.
- **Phase 3 — integration tests and hardening** (bootstrap, isolation, catch-up).

## 9. Non-goals / deferred

- **Deletes** — not propagated; accepted divergence for the dev-mirror use case.
- **Old corrections** — a master-side write to an epoch older than
  `backfill_lookback`, made while the replica was disconnected (or dropped from
  the stream), is not healed automatically. Recovery ("deep resync"): delete
  `replication_watermarks.json` on the replica — lost watermarks are safe (§4)
  and force a full idempotent re-bootstrap.
- **Point-in-time consistency** — only per-bucket eventual consistency.
- **New WebSocket replication endpoint** — unnecessary for LAN server-to-server;
  the gRPC feed is reused.
- **Transaction-group sequence-gap detection** — a future optimization that could
  replace the periodic reconcile with precise, gap-triggered backfill using the
  existing `tgID`. Out of scope initially.
