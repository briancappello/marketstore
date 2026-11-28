# Replicas and the master's Postgres

A replica mirrors the master's market data through replication
(see [logical_replication_design.md](../design/logical_replication_design.md)).
Some reference data does not live in MarketStore, though. It lives in the
master's Postgres database (`fa_techan`), which the `fin` project manages.
This page explains which data that is, who owns it, and how a replica reads
it from the master.

## What a replica reads from Postgres

| Reader | Data | On a replica |
|---|---|---|
| `SECTOR_AGGREGATE` watchlist | each active equity's sector | reads the **master's** Postgres |
| `INDUSTRY_AGGREGATE` watchlist | each active equity's industry and sector | reads the **master's** Postgres |
| massive bgworker (`symbols_dsn`, `sync_queries`) | the symbol universe and backfill coverage | **not used**: ingest runs on the master only, and a replica receives its output through replication |

A replica never writes to Postgres. It reads two queries, and only through
the watchlist plugin.

## One source of truth

The master's Postgres is the only copy of the reference data. On the master,
`fin sync` maintains it:

- **The symbol universe.** It contains every symbol Massive lists and Alpaca
  can trade. Its active assets are exactly what the master's massive
  bgworker ingests: `SELECT id, ticker, listed FROM asset WHERE is_active AND
  delisted IS NULL`.
- **Sector and industry,** in Yahoo's scheme. Where Yahoo has no answer, the
  sector is inferred from the SEC SIC code or the Nasdaq screener industry.
  `equity.classification_source` records which source applied.
- **Index memberships.**

Do not point a replica's watchlists at a Postgres of its own. Two databases
classify differently: they use different taxonomies, cover different
symbols, and were refreshed at different times. That makes the replica's
sector and industry lists differ from the master's for reasons that have
nothing to do with replication.

## What the master must provide

The network layer (addresses, names, routing, firewall policy) belongs to
the network design, not to MarketStore. The requirements below say *what*
must be true. *How* it is done is up to that design.

1. **A stable name for the master's database host.** The replica's DSN uses
   it. Do not hard-code IP addresses: a host on DHCP changes address, and an
   address in a DSN, `pg_hba.conf` or a firewall rule then silently stops
   matching.
2. **A read-only role** that can read exactly the tables the queries use.
   Run this as a Postgres superuser on the master:

   ```sql
   CREATE ROLE sector_reader LOGIN PASSWORD '...';
   GRANT CONNECT ON DATABASE fa_techan TO sector_reader;
   GRANT USAGE ON SCHEMA public TO sector_reader;
   GRANT SELECT ON asset, equity, sector, industry TO sector_reader;
   ```

   Never give a replica the database owner's credentials. Those can write
   the reference data the master ingests from.
3. **Postgres reachable from the replicas.** `listen_addresses` must
   include an interface the replicas can reach (changing it needs a
   Postgres restart). `pg_hba.conf` needs a `host` (or `hostssl`) entry for
   `sector_reader` from the replicas' network, using `scram-sha-256`. The
   firewall must allow 5432/tcp from the replicas only.
4. **Optionally, TLS.** The data is not sensitive, and `scram-sha-256`
   never sends the password. But without TLS the query results cross the
   network in plain text. Use `hostssl` and `sslmode=verify-full` when that
   matters, or reach Postgres through an encrypted tunnel or overlay network.

Restarting the master's Postgres is safe for replicas. See "Behaviour"
below.

## Replica configuration

In the replica's `mkts.yml`, point both aggregates at the master's database.
Use the read-only role and the master's stable name, written here as
`MASTER_DB_HOST`. The queries are the same as on the master.

```yaml
bgworkers:
  - module: /path/to/marketstore-watchlists/watchlist.so
    name: WatchlistBaselines
    config:
      strategy_config:
        SECTOR_AGGREGATE:
          dsn: "postgres://sector_reader:PASSWORD@MASTER_DB_HOST:5432/fa_techan?sslmode=require"
          refresh: "1h"   # optional reload interval (Go duration), default 1h
          query: >
            SELECT a.ticker, s.name AS sector
            FROM equity e
            JOIN asset a ON a.id = e.id
            LEFT JOIN sector s ON s.id = e.sector_id
            WHERE a.is_active = TRUE AND a.delisted IS NULL
        INDUSTRY_AGGREGATE:
          dsn: "postgres://sector_reader:PASSWORD@MASTER_DB_HOST:5432/fa_techan?sslmode=require"
          refresh: "1h"
          query: >
            SELECT a.ticker, i.name AS industry, s.name AS sector
            FROM equity e
            JOIN asset a ON a.id = e.id
            LEFT JOIN industry i ON i.id = e.industry_id
            LEFT JOIN sector s ON s.id = e.sector_id
            WHERE a.is_active = TRUE AND a.delisted IS NULL
```

- **`sslmode`.** Use `require` (or `verify-full`) when the master offers
  TLS, and `disable` when the connection runs inside an encrypted tunnel or
  overlay.
- **The password.** `mkts.yml` now holds a credential. Keep it out of version
  control, as the master's `mkts.yml` already is, and readable only by the
  user that runs MarketStore.
- **The other keys.** A replica needs no `symbols_dsn`. It runs no massive
  bgworker.

## Behaviour

- **Startup.** The mappings load in the background, so the server starts
  even when Postgres is unreachable. Until the first load succeeds, both
  lists are empty. A failed load is retried after 30 s, then 1 min, doubling
  up to the refresh interval. The log shows each failure as
  `[sector_aggregate] load failed, retrying in ...`.
- **Steady state.** Every `refresh` the mappings reload. A reclassification
  on the master (`fin sync`) reaches replicas within that interval, without a
  restart. When a reload fails, the last good mapping stays in use.
- **Log line on success:** `[sector_aggregate] loaded N mappings; next
  reload in 1h0m0s` (and the same for `industry_aggregate`).

## Verifying a replica

1. Both log lines show the same `N` on the master and on the replica.
2. At a quiet time (a weekend, or after 20:00 ET), the sector and industry
   lists match:

   ```bash
   for h in MASTER REPLICA; do curl -s "http://$h:5993/v1/watchlists/SECTOR_AGGREGATE" > "/tmp/sector-$h.json"; done
   cmp /tmp/sector-MASTER.json /tmp/sector-REPLICA.json && echo identical
   ```

   During a session the lists can differ by a few milliseconds of tick
   timing. Compare at a quiet time.
3. To check the role on the master, run `psql` as `sector_reader` and try an
   `UPDATE`: it must fail with `permission denied`.
