## Why

The symbol universe, sector and industry data, and backfill coverage live in Postgres. Today no one owns them clearly:
- **Two writers, one owner.** The `fin` Python library declares and migrates the tables (`asset`, `equity`, `asset_data_vendor`, ...). marketstore reads and writes them through free-form SQL strings in `mkts.yml` (`symbols_query`, `sync_queries`, the watchlist `query`).
- **The universe is decided outside the process that uses it.** `fin sync` decides which symbols marketstore ingests, but marketstore reads that decision only at startup. A daily sync therefore needs a restart, or a reload hack, before it takes effect.
- **Securities have no identity beyond the ticker.** A recycled ticker revives the old company's row, its classification and its coverage records. In marketstore, the new company's bars join the old company's series.
- **The Python stack wants its own data too.** Users manage curated watchlists and portfolios that point at securities. Those tables have no clean place to live: the library cannot reference the web server's `user` table.

Rewriting `fin sync` (2026-11-28) made all of this visible. This change fixes the ownership before more is built on the current layout.

## What Changes

- **Three schemas in one Postgres database, each with one migration owner:**
  - `market`: owned by marketstore.
  - `fin`: owned by the `fin` library.
  - `web`: owned by the `fa-techan` server.

  References run one way only: `web` → `fin` → `market`.
- **marketstore owns the reference data.** That means securities, their ticker history, exchanges and markets, sectors, industries and classifications, index memberships, vendor coverage, broker tradability and an audit of sync runs. marketstore migrates the `market` schema itself, with SQL migrations embedded in the binary and applied on the master only.
- **Securities get a stable identity.** A security is identified by its composite FIGI (and CIK where there is one). The ticker becomes a listing with a validity period, so a reused ticker produces a new security and never revives an old one.
- **The symbol sync moves into marketstore, in Go.** It covers the universe (Massive ∩ tradable on Alpaca), delisting and deactivation, classification (Yahoo scheme, with SIC and Nasdaq-industry inference) and index memberships. It runs daily before premarket, and the master swaps its in-process universe afterwards, with no restart.
- **The percentage delisting cap is replaced** by a check on the vendor responses themselves: abort when a vendor list is far smaller than the current universe.
- **Postgres becomes a first-class marketstore dependency.** One core `sql:` configuration and a `refdata` package give typed access to plugins.
  - **BREAKING:** `symbols_dsn`, `symbols_query`, `sync_queries` and the watchlist aggregates' `dsn`/`query` keys are removed from `mkts.yml`. Queries become code.
  - Postgres stays optional for deployments that enable no feature needing it.
- **Database roles:**
  - marketstore's role owns `market`.
  - The Python role owns `fin` and `web`, and has read-only access to `market`.
  - `market_reader` serves replicas and other read-only consumers. It replaces the documented `sector_reader`.
- **Python side (`fin` and `fa-techan` repositories):**
  - `fin` keeps the domain models and is usable headless.
  - `user` (identity only) moves into `fin`; authentication stays in `fa-techan`.
  - `fa-techan` extends `fin` only by adding tables that reference it.
  - Two alembic branches (`fin`, `web`) with explicit `depends_on`.
  - Market tables are mapped read-only in `fin` and checked by a contract test against marketstore's migrations.
- **Removals:**
  - The `IntradayBar`, `DataItem`, `DataItemVendor` and `Stats` models and their admin views.
  - `fin sync`, once the Go sync is in production.

## Capabilities

### New Capabilities
- `refdata/schemas`: the three schemas, their owners, migration ownership, database roles, and marketstore's core `sql:` configuration (master and replica modes).
- `refdata/securities`: security identity (FIGI/CIK), ticker listings with validity periods, and how a reused ticker is told apart from a returning security.
- `refdata/universe-sync`: the daily Go sync of the universe from Massive and Alpaca: adding, delisting, deactivating and reactivating; the vendor-response check; the schedule; live universe reload on the master.
- `refdata/classification`: sector and industry in Yahoo's scheme, with SIC and Nasdaq-industry inference, provenance, and the Yahoo-failure guard.
- `refdata/index-membership`: index memberships (Dow Jones averages, NASDAQ-100, S&P 500) with membership periods.

### Modified Capabilities
- `massive/split-ingestion`: the tracked symbol set comes from the refdata universe instead of the static `symbols` list or a configured `symbols_query`.

## Impact

- **marketstore:**
  - A new core `refdata/` package with embedded migrations and the Go sync (Massive, Alpaca, Yahoo, Wikipedia and Nasdaq screener clients).
  - `contrib/massive` (universe, coverage) and `contrib/watchlist` and the `marketstore-watchlists` plugin (sector and industry mappings) switch to `refdata`.
  - `utils` gains the `sql:` config; `internal/di` wires it.
  - New dependency: a Postgres driver in core (`pgx`, already used by `contrib/massive`).
- **Config (BREAKING):** taichi's and p1's `mkts.yml` lose the SQL keys and gain `sql:`. Replica docs (`docs/replication/replica-postgres.md`) are updated for `market_reader`.
- **Data:** existing tables move into `market` with `ALTER TABLE ... SET SCHEMA`. Ids are preserved, because backfill coverage is keyed by them. Ticker reuse needs a one-off backfill of FIGI/CIK from Massive.
- **`fin` repository:** the models are split by schema, `user` moves in, a read-only `market` mapping is added, the alembic branch is labelled `fin`, junk models are deleted, and `fin sync` is retired.
- **`fa-techan` repository:** auth tables move to `web` referencing `fin.user`, the alembic branch is labelled `web` with `depends_on`, and the admin views for market tables become read-only.
