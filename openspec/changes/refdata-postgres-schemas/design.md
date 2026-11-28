## Context

See proposal.md (Why). The current state that shapes the approach:

- **One database, `fa_techan`, all tables in `public`.** `fin`'s alembic history creates them (6 revisions). `fa-techan`'s history (1 revision: `user`, `access_token`) is stitched in by `migrations/cli.py`, which concatenates both `version_locations` into one `alembic_version` table with two heads.
- **marketstore touches Postgres in three places, all from plugin config:**
  - `contrib/massive` reads the universe once in `NewBgWorker` (`FetchSymbolsFromDB`, `symbols_query`).
  - `contrib/massive` reads and writes backfill coverage in `asset_data_vendor` through `sync_queries` SQL strings in `mkts.yml`. They are keyed by `asset.id`.
  - The `marketstore-watchlists` plugin loads sector and industry mappings (`dsn`/`query` per strategy) with a background hourly reload.
- **`pgx/v5` is already a module dependency** (`go.mod`), used by `contrib/massive`.
- **Assets use joined-table inheritance** (`asset` plus `equity`/`etf`/`adr`/`bond`, discriminated by `type`). Sector and industry are columns on `equity`.
- **Python domain tables reference securities by ticker string** (`position.ticker`, `order.ticker`, `trading_signal.ticker`, `research_hit.symbol`), except `watchlist_asset.asset_id`.
- **fa-techan authenticates with fastapi-users**, which expects one `User` model carrying `email`, `hashed_password`, `is_active`, `is_superuser` and `is_verified`.
- **Plugins are Go `.so` files built against the marketstore module.** A package they import from marketstore shares its state with the host process.
- **The Python sync being ported** is `fin/cli/sync.py` (`fin` `b9455e2`). Its behaviour, verified on an empty database and on a copy of taichi's, is the reference for the Go port.

## Goals / Non-Goals

**Goals:**
- Every table has exactly one writer, and the database's roles enforce it, not just a convention.
- A fresh database and taichi's existing one converge to the same schema through the same migrations.
- Ids are preserved throughout, so backfill coverage and `watchlist_asset` survive.
- `fin` stays usable headless: `fin`'s migrations and models work without `fa-techan` installed.

**Non-Goals:**
- **Separating marketstore bar series on ticker reuse or ticker renames.** This change records listing periods, which makes a later change possible. Bars stay keyed by ticker here.
- **Moving bar data into Postgres.** marketstore remains the time-series store.
- **A public HTTP API for reference data.** Consumers read Postgres directly.
- **High availability for Postgres.** One master database, as today.

## Decisions

### D1. Three schemas in one database, owned by roles

| Schema | Owner role | Migrations | Readers |
|---|---|---|---|
| `market` | `marketstore` | marketstore (Go) | `fin_app` (SELECT), `market_reader` (SELECT) |
| `fin` | `fin_app` | `fin` (alembic) | `fin_app` |
| `web` | `fin_app` | `fa-techan` (alembic) | `fin_app` |

Foreign keys run `web → fin → market` and never back. Ownership is enforced by grants: `fin_app` has no `INSERT`/`UPDATE`/`DELETE` on `market`, so a Python write to a market table fails in Postgres, whatever the ORM allows.

*Alternatives:*
- **Separate databases.** This loses cross-schema foreign keys, the property that lets a watchlist entry point at a security.
- **One schema with a naming convention** (the status quo). Ownership would stay unenforced.

### D2. marketstore migrates `market` with embedded SQL and goose

- **Format.** Migrations are plain `.sql` files in `refdata/migrations/`, embedded with `go:embed` and applied with `pressly/goose/v3`. goose takes an `embed.FS`, a custom version table (`market.goose_db_version`), and a Postgres advisory lock, so two starting processes cannot race.
- **When they run.** On the master at startup, before any plugin loads, and through a new `marketstore tool refdata migrate|status` command for CI and manual use.
- **Replicas** never migrate. They check that the schema version is at least the one they were built for, and log an error if it is not.

*Alternatives:*
- **golang-migrate.** It is equivalent, but has a heavier driver/source abstraction.
- **A hand-written runner.** It is small, but means reimplementing locking and version bookkeeping.
- **Keeping alembic for `market`.** That puts the schema in a different repo, and a different language, from its only writer.

### D3. Baseline that adopts existing tables

Migration `00001_adopt` creates schema `market`. Then, per table:
- **If the table exists in `public`** (taichi, p1): `ALTER TABLE public.<t> SET SCHEMA market`.
- **Otherwise** (a fresh database): it creates the table.

Ids and sequences move with the tables. Later migrations (D4) reshape `market` identically on both paths.

On the Python side, `fin`'s existing six revisions are replaced by a new baseline for schema `fin` that adopts its tables the same way. The old `public.alembic_version` is dropped after the stamp.

### D4. The `market` data model

- **`security`.** This is the renamed `asset`, ids preserved, single-table: `kind` (equity, etf, adr, bond), `name`, `composite_figi`, `cik`, `share_class_figi`, `in_universe`, `universe_changed_at`. The subtype tables fold in; the few kind-specific columns become nullable columns.
  - *Why:* joined inheritance across a schema boundary makes every reader join four tables for one row.
- **`security_listing`.** Fields: `security_id`, `ticker`, `exchange_id`, `valid_from`, `valid_to`. A partial unique index on `ticker WHERE valid_to IS NULL` lets at most one security hold a ticker at a time.
  - **Delisting** closes the listing.
  - **A ticker rename** closes one listing and opens another for the same security.
- **The universe** is the open listings of securities with `in_universe`: what marketstore ingests. `in_universe = false` is the reversible "deactivated" state.
- **`classification`.** Fields: `security_id` (PK), `sector_id`, `industry_id`, `source` (yahoo, inferred_sic, inferred_nasdaq), `sic_code`, `classified_at`. Plus `sector` and `industry`.
- **`yahoo_profile_cache`.** Fields: `symbol`, `fetched_at`, `status`, `sector`, `industry`. It replaces `~/.fin/yahoo-info.jsonl`, so a resumed or repeated run reuses answers, and the cache lives with the data.
- **`index`, `index_membership`.** Fields: `index_id`, `security_id`, `valid_from`, `valid_to`.
- **`security_vendor_coverage`.** This is the renamed `asset_data_vendor`, unchanged in content.
- **`broker_tradability`.** This is today's `asset_broker`.
- **`sync_run`.** Fields: `started_at`, `finished_at`, `dry_run`, vendor list sizes, counts per action, `error`.
- **Kept:** `exchange`, `market`, `country`, `currency`, `data_vendor`.

### D5. Identity resolution

The sync matches each Massive ticker to a security in this order:
1. **The composite FIGI matches a security.** It is the same security.
   - Its open listing for that ticker continues.
   - If the security holds a different open ticker, that is a **rename**: close the old listing and open the new one.
2. **No FIGI match, but the ticker's open listing belongs to a security without a FIGI** (legacy rows before the backfill). It is the same security, and the FIGI is recorded.
3. **Otherwise** it is a **new security.** If another security still holds the ticker's open listing, that listing is closed first. That is ticker reuse: the old security keeps its history and stops matching the ticker.

A Massive ticker with no composite FIGI (some new listings and ETFs) falls back to rule 2 by ticker, and the sync run counts it as `identity_unverified`.

*Alternative:* match by CIK. CIK identifies the issuer, not the security. One company has several share classes and warrants (PSNY, PSNYW), so CIK alone would merge them.

### D6. The sync is a core service on the master

- **Location.** `refdata/sync` lives in the marketstore binary, not in a plugin.
  - *Why:* both the massive plugin and the watchlist plugin consume its output, and a plugin's lifetime and config are its own.
  - *How it runs:* it runs when `sql.sync.enabled` is true and the instance is the master.
- **Vendor clients.** They are ported from `fin`: Massive reference tickers and ticker details (SIC), Alpaca assets (raw JSON, so an exchange that is new to the vendor cannot fail the sync), the Nasdaq screener, Yahoo `quoteSummary` (`assetProfile`, with the cookie-and-crumb handshake that yfinance performs), and the Wikipedia index tables (`goquery`).
  - *Where they live:* under `refdata/vendors/`, separate from `contrib/massive/api`, so the sync does not depend on a plugin package.
- **Schedule.** Daily at `sql.sync.time` (America/New_York, default `01:30`). It also runs at startup when the last successful `sync_run` is older than 24 h. A run takes about 30 minutes, most of it Yahoo, and finishes well before the 03:00 overnight backfill and the 04:00 premarket.
- **Phases and transactions:**
  1. **Universe and identity:** one transaction.
  2. **Classification:** its own transaction.
  3. **Indexes:** their own transaction.

  A failed later phase never rolls back an earlier one, as in `fin sync`. Each run writes a `sync_run` row.
- **Dry run.** `marketstore tool refdata sync --dry-run` runs the same plan against the configured database, prints it, and writes nothing (not even `sync_run`). That is the review step before a first real run.

### D7. The vendor-response check replaces the percentage cap

Before any universe change, the sync checks every vendor list:
- **Abort the universe phase** when a list is empty, or has fewer than `sql.sync.min_vendor_fraction` (default 0.9) times the current universe size. The run records the reason in `sync_run.error`.
- **Then apply all changes**, whatever their count.

The failure the old cap guarded against is a truncated vendor response. Delisting already needs both vendors to agree, and a truncated list is visible directly in its size. A backlog of genuine delistings, such as the 351 on taichi after four months without a sync, is never blocked.

The classification phase keeps its own guard: it skips, writing nothing, when Yahoo fails for more than 5% of equities after a retry.

### D8. One `sql:` section and a `refdata` package in core

```yaml
sql:
  dsn: "postgres://marketstore:...@localhost:5432/fa_techan"
  mode: master            # master | reader (replicas)
  sync:
    enabled: true
    time: "01:30"
    min_vendor_fraction: 0.9
    massive_api_key: ...
    alpaca_api_key: ...
    alpaca_api_secret: ...
```

- **Typed API.** `refdata` exposes `Universe()` (a snapshot), `SubscribeUniverse()` (a channel of add/remove deltas), `Classifications()`, and `Coverage` (read and write for the massive plugin). Plugins import `refdata` and get the process-wide instance from `internal/di`.
- **No SQL in config.** The removed keys (proposal, BREAKING) fail startup with an error that names the key and its replacement, the same style as `backfill_adjusted` in `massive/raw-bars`.
- **Without `sql:`,** `refdata` is disabled. The massive plugin then uses its static `symbols` list, and the aggregates are empty, as today without a DSN.

### D9. Live universe changes on the master

The massive plugin subscribes to universe deltas:
- **Added tickers:** `AddTickers` on the live WebSocket connection, and a backfill request from `query_start`.
- **Removed tickers:** `RemoveTickers`. Their stored bars are kept.

No restart or reconnect is needed, and the 03:58 reconnect continues as before. Split ingestion (`massive/split-ingestion`) tracks the same set.

### D10. Replicas read `market` as `market_reader`

- **What runs.** A replica runs `refdata` in `mode: reader`, against the master's database: no migrations and no sync.
- **What it reads.** Classifications, for the aggregates, with the existing hourly reload and backoff.
- **`docs/replication/replica-postgres.md`** changes: `sector_reader` becomes `market_reader`, with `GRANT SELECT ON ALL TABLES IN SCHEMA market` plus default privileges so new tables are covered, and the config becomes one `sql:` section.

### D11. Python ORM: one registry, read-only market mapping, contract test

- **One registry.** `fin.db` exports one SQLAlchemy registry (`Base`, one `MetaData`). Every model sets `__table_args__ = {"schema": ...}`. With a single `MetaData`, string foreign keys across schemas (`ForeignKey("market.security.id")`) resolve, and relationships work.
- **`fin.db.market`** maps the `market` tables read-only:
  - A `before_flush` listener raises on any pending market object, so the mistake fails in tests with a clear message.
  - The grants (D1) are the real guard.
- **Autogenerate.** It never sees `market`: each alembic env's `include_object` keeps only its own schema.
- **Contract test** in `fin`'s CI: start Postgres, run `marketstore tool refdata migrate` (from a pinned marketstore release, or a container image), reflect `market`, and compare it with `fin.db.market`'s metadata (tables, columns, types, nullability). A marketstore schema change then fails `fin`'s CI, not production.
- **Domain tables get `security_id`.** `position`, `order`, `trading_signal` and `research_hit` gain `security_id → market.security`. Their ticker columns stay, as the symbol recorded at the time. `watchlist_asset` becomes `watchlist_security`.

*Alternative:* expose `market` to Python as views. That would give a stable API that marketstore can refactor behind, but it adds a layer to maintain. It is deferred until marketstore actually needs to refactor; the contract test makes the need visible.

### D12. Two alembic environments with their own version tables

- **`fin`** has its own env: version table `fin.alembic_version`, history in `fin/migrations/versions`. A headless deployment runs fin's `db upgrade` alone.
- **`fa-techan`** has its own env: version table `web.alembic_version`. Its `db upgrade` upgrades `fin` first, then `web`.
- **Cross-schema dependencies.** A `web` revision that needs a `fin` table declares it in a module-level `requires_fin = "<rev>"`. `fa-techan`'s `env.py` refuses to run that revision unless `fin.alembic_version` contains it or a descendant.

This replaces the current stitched setup, and corrects the shared-table, branch-label approach discussed earlier. Alembic resolves every revision in its version table against its script directories. With one shared table, `fin`'s env would fail on `web`'s revisions, so `fin` could not migrate headless.

*Alternative:* one shared history with branch labels and `depends_on`. It is simpler to order, but loses headless `fin` migrations, which is the reason the library/server split exists.

### D13. `user` identity in `fin`, authentication in `web`

- **`fin.user`:** `id` (UUID, as fastapi-users uses today), `email` (unique), `display_name`, `created_at`.
- **`web.user_auth`:** `user_id` (PK, FK `fin.user`), `hashed_password`, `is_active`, `is_superuser`, `is_verified`.
- **One model for fastapi-users.** `fa-techan`'s `User` subclasses `fin`'s `User` with joined-table inheritance over `web.user_auth`, so fastapi-users still sees one model with every field it expects.
- **Elsewhere in `web`:** `access_token` and `oauth_account` reference `fin.user`.
- **Ownership.** Watchlists, portfolios and positions reference `fin.user`, so a headless script can create a user's curated watchlist, and the webapp adds only the ability to log in as that user.

*Alternative:* keep `user` in `web` and use an unconstrained `owner_id` in `fin`. That loses referential integrity on every user-owned row.

## Risks / Trade-offs

- **[Yahoo's crumb handshake changes, and there is no yfinance to absorb it in Go]** → The handshake lives in one small client with a recorded-response test. A Yahoo outage only skips classification (D7), and the previous classification stays in place.
- **[Porting the sync to Go loses `fin sync`'s verified behaviour]** → Parity test: run both against copies of the same database snapshot, and require identical universes and classifications before `fin sync` is retired.
- **[FIGI coverage is incomplete, so identity falls back to tickers]** → `identity_unverified` is counted per run. Phase 1 measures the FIGI coverage of the current universe and reports it before the identity rules are switched on.
- **[A schema move breaks a running reader]** → The move runs with marketstore stopped and fa-techan stopped, on a weekend. Every reader is in this change's scope, and they switch together.
- **[Ticker-keyed bars mix two securities after reuse]** → A non-goal here. Listing periods (D4) record when it happened, so a later change can split the series. Until then, `sync_run` counts reuse events so they are visible.
- **[Python cannot write reference data any more, e.g. a manual fix in the admin]** → The intended effect. Fixes go through marketstore (a `tool refdata` command) so that the next sync does not undo them.
- **[A core Postgres dependency in a fork of upstream marketstore]** → `refdata` is disabled without `sql:`, and core does not import it outside `internal/di` and the startup path. Upstream-style deployments are unaffected.

## Migration Plan

1. **Measure:** FIGI/CIK coverage of taichi's universe from Massive (read-only).
2. **marketstore `refdata` baseline:**
   - `00001_adopt` and the reshaping migrations (D3, D4).
   - The `tool refdata migrate|status` command.
   - Test both paths (fresh, and taichi's dump restored) on p1 scratch databases.
3. **marketstore readers switch to `refdata`:** universe, coverage and classifications. Removed config keys fail startup.
4. **`fin` and `fa-techan`:** schema-aware models, a new `fin` baseline, separate alembic envs, `user` split, junk models deleted, and the contract test.
5. **Cut-over on taichi** (weekend, approved by the user):
   1. Back up (`pg_dump -Fc`).
   2. Stop marketstore and fa-techan.
   3. Create the roles.
   4. Run `marketstore tool refdata migrate`.
   5. Run the `fin`/`fa-techan` upgrades.
   6. Update `mkts.yml` and fa-techan's `.env`.
   7. Start both.
   8. Compare the universe with the pre-cut-over `asset WHERE is_active AND delisted IS NULL`.
6. **Go sync:**
   - Port it with the parity test.
   - Dry run on taichi for review.
   - Enable the daily schedule.
   - Retire `fin sync`.
7. **p1 (replica):** `mode: reader` with `market_reader`, once the user's network design is in place.

**Rollback:**
- Before step 5 nothing on taichi has changed.
- During or after step 5, restore the dump into a fresh database and start the previous builds (the marketstore commit and the `fin` branch head are recorded in `notes.md` before the cut-over).

## Open Questions

- Should `market_reader`'s password come from `mkts.yml` or from a `PGPASSFILE`? Both work with pgx. This is decided when the replica is set up.
- Should `yahoo_profile_cache` keep only the latest answer per symbol or a history? The latest only is enough for the sync. A history would show classification drift.
