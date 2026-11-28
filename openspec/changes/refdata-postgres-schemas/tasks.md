Tasks marked **[fin]** or **[fa-techan]** are done in those repositories. Each is committed there and recorded here by commit.

## 0. Baseline and measurement

- [ ] 0.1 Record the test baseline in `notes.md`: `go test ./contrib/massive/... ./contrib/watchlist/... ./utils/... ./internal/di/... ./cmd/...` per package, `fin`'s `uv run pytest` (known: `tests/test_cache.py` import error, `test_top_10_at_checkpoints` failing), and `fa-techan`'s test suite.
- [ ] 0.2 Measure identity coverage of taichi's current universe from Massive's reference tickers (read-only): the share with a composite FIGI, the share with a CIK, and FIGI collisions between tickers. Verify by recording the numbers in `notes.md`. If FIGI coverage is under 95%, revisit D5 with the user before section 3.
- [ ] 0.3 Take a fresh `pg_dump -Fc` of taichi's `fa_techan` for scratch restores on p1. Verify that it restores into a `TEMPLATE template0` database with 0 errors.

## 1. `market` schema and migrations (marketstore)

- [ ] 1.1 Add `refdata/` with goose, embedded SQL migrations, the `market.goose_db_version` table and the advisory lock. Add `marketstore tool refdata migrate|status`. Verify with a test against a throwaway Postgres: two concurrent `migrate` runs apply each version once, and `status` lists applied and pending versions.
- [ ] 1.2 Migration `00001_adopt` (D3): create `market` and move the existing reference tables from `public`, or create them on a fresh database. Verify by running it against both an empty database and taichi's restored dump. A schema-only `pg_dump -n market` of the two must be identical, and row counts and max ids on the dump are unchanged.
- [ ] 1.3 Reshaping migrations (D4):
  - fold `equity`/`etf`/`adr`/`bond` into `security`;
  - create `security_listing` from tickers, with `delisted` mapped to `valid_to` and the partial unique index;
  - move classification columns into `classification`;
  - rename `asset_data_vendor` and `asset_broker`;
  - add `yahoo_profile_cache`, `index_membership` periods and `sync_run`;
  - drop `intraday_bar`, `data_item`, `data_item_vendor`, `stats`.

  Verify on taichi's dump: the universe query returns the same 11,794 tickers as `asset WHERE is_active AND delisted IS NULL` did, every security id is preserved, and inserting a second open listing for a ticker fails.
- [ ] 1.4 Roles (D1): a migration or `tool refdata grant` creates `market_reader` and grants `SELECT` on `market` to it and to the Python role, with default privileges. Verify with a test: the Python role's `UPDATE market.security` fails with `permission denied`, and `market_reader` can read a table created by a later migration.

## 2. `sql:` configuration and the `refdata` API (marketstore)

- [ ] 2.1 Parse the `sql:` section (`dsn`, `mode`, `sync.*`) in `utils`, and wire a process-wide `refdata` instance through `internal/di`. A master migrates before plugins start; a reader checks the version only. Verify with tests: without `sql:` startup succeeds with refdata disabled; a reader against an older schema logs both versions and does not migrate.
- [ ] 2.2 `Universe()`, `SubscribeUniverse()`, `Classifications()` and `Coverage`, with tests against a throwaway Postgres. Verify that a universe change is delivered to subscribers as an add/remove delta.
- [ ] 2.3 Startup errors for the removed keys (`symbols_dsn`, `symbols_query`, `sync_queries`; aggregate `dsn`/`query`). Verify with one config test per key, asserting the error names the key and `sql:`.

## 3. Readers switch to `refdata` (marketstore, marketstore-watchlists)

- [ ] 3.1 `contrib/massive` takes its universe from `refdata` and records coverage through `Coverage`, replacing `FetchSymbolsFromDB` and `sync_queries`. Verify with the existing massive tests (adapted), plus a test that coverage reads and writes hit `market.security_vendor_coverage` for the right security id.
- [ ] 3.2 Live universe (D9): on a delta, call `AddTickers`/`RemoveTickers` on the live connection and queue a `query_start` backfill for added tickers. Verify with a test using a fake WebSocket server: an added ticker is subscribed without a reconnect and gets a backfill request; a removed one is unsubscribed and its buckets remain.
- [ ] 3.3 Split ingestion covers the current universe (`specs/massive/split-ingestion`). Verify with a test: a ticker added after the last split sync is covered by the next one without a restart.
- [ ] 3.4 The `marketstore-watchlists` sector and industry aggregates read `refdata.Classifications()` instead of their own DSN and query, keeping the background reload and backoff. Verify with the plugin's tests, plus a check that the plugin still builds against the host (`make build` rebuilds `watchlist.so`). Update its README.

## 4. Python side **[fin]**, **[fa-techan]**

- [ ] 4.1 **[fin]** One registry, schema-tagged models (D11): `fin.db.market` maps `market` read-only with the `before_flush` guard, and domain models move to schema `fin`. Delete `IntradayBar`, `DataItem`, `DataItemVendor`, `Stats`. Verify with a test that flushing a market object raises, and that the suite stays at its baseline.
- [ ] 4.2 **[fin]** `fin.user` (D13). Add `security_id` to `position`, `order`, `trading_signal` and `research_hit`; rename `watchlist_asset` to `watchlist_security`; add the `watchlist.user_id` FK. Verify with model tests that a watchlist and its securities round-trip with a user.
- [ ] 4.3 **[fin]** A new alembic baseline for schema `fin` that adopts the existing tables, with `fin.alembic_version` and `include_object` limited to `fin` (D12). Verify on taichi's dump, after task 1.3: `upgrade` adopts and stamps; autogenerate then reports no changes; it never emits DDL for `market`; a fresh database reaches the same schema.
- [ ] 4.4 **[fin]** Contract test: CI starts Postgres, runs `marketstore tool refdata migrate` from a pinned marketstore build, reflects `market`, and compares it with `fin.db.market`. Verify that it passes, then fails when a column is renamed in a scratch migration.
- [ ] 4.5 **[fa-techan]** Its own alembic env with `web.alembic_version`, the `requires_fin` check, and `db upgrade` running `fin` first. Move `access_token` and `oauth_account` to `web`, and make `User` a joined-table subclass over `web.user_auth`. Verify with tests: register, log in and token auth work through fastapi-users; a `web` revision whose `requires_fin` is missing refuses to run.
- [ ] 4.6 **[fa-techan]** Admin: delete the views for the removed models, and make the market-table views (sector, industry, exchange, market, index, country, currency, equity) read-only. Verify that the admin loads and that the market views offer no create, edit or delete.

## 5. Cut-over on taichi (needs the user's approval)

- [ ] 5.1 Rehearse the full cut-over on a p1 scratch database restored from task 0.3. Run the marketstore migrations, the `fin` and `fa-techan` upgrades, then start a scratch marketstore against it. Verify that the universe equals the pre-migration query and that the fa-techan app logs in an existing user.
- [ ] 5.2 Record in `notes.md`: taichi's marketstore commit, the `fin` and `fa-techan` heads, and the backup path. Then cut over on a weekend (design: Migration Plan step 5). Verify the universe count, the aggregates' mapping counts in the log, a fa-techan login, and the next 03:00 overnight backfill and 06:00 parity checks.

## 6. The sync in Go (marketstore)

- [ ] 6.1 Vendor clients under `refdata/vendors/`: Massive reference tickers (with FIGI and CIK) and ticker details (SIC); Alpaca assets as raw JSON; the Nasdaq screener. Verify with recorded-response tests, including an Alpaca `TXSE` asset and a Massive ticker without a primary exchange.
- [ ] 6.2 Universe planning and identity (D5, D7), as a pure function over vendor lists and the current state. Verify with tests for every scenario in `specs/refdata/universe-sync` and `specs/refdata/securities`: both vendors gone, one vendor only, back on both, empty database, truncated list, 351-row backlog, recycled ticker, returning security, rename, and no FIGI.
- [ ] 6.3 Yahoo `quoteSummary` client with the cookie-and-crumb handshake, pacing, one retry pass, and `yahoo_profile_cache` reuse under 20 h. Verify with recorded-response tests for found, unclassified, not found, throttled and crumb expiry, and a test that a rerun fetches only uncached symbols.
- [ ] 6.4 Classification (`specs/refdata/classification`): Yahoo first, then the SIC and Nasdaq-industry inference ported from `fin/classification.py`, the 5% guard, and removal of unused sectors and industries. Verify with the ported `fin` classification tests, plus the outage scenario.
- [ ] 6.5 Index memberships (`specs/refdata/index-membership`): the five sources with throttling retries, periods, and per-index failure isolation. Verify with recorded pages, including a page with no ticker table.
- [ ] 6.6 Sync orchestration (D6): the phases and their transactions, `sync_run` rows, the daily schedule with the stale-at-startup run, master only, and `tool refdata sync --dry-run`. Verify with tests: a classification failure keeps the universe changes; a reader never syncs; a dry run leaves the database byte-identical (`pg_dump` before and after).
- [ ] 6.7 Parity with `fin sync`: run both against two restores of the same taichi dump, then compare the universes, the classifications (sector, industry, source) and the index memberships. Verify that the only differences are the intended ones (identity rules, no percentage cap), each listed in `notes.md`.
- [ ] 6.8 On taichi (approved by the user): show the user `tool refdata sync --dry-run`, run the first real sync, then enable the 01:30 schedule. Verify the next day: the `sync_run` row, new tickers streaming before 04:00 without a restart, and their backfill.
- [ ] 6.9 **[fin]** Retire `fin sync` and its vendor code; the README points to marketstore. Verify that `fin --help` no longer lists `sync` and the suite stays at its baseline.

## 7. Replicas and documentation

- [ ] 7.1 Update `docs/replication/replica-postgres.md` and `examples/replication/mkts-replica.yml` for `market_reader` and `sql: {mode: reader}`, and document the `sql:` section in the main config docs. Verify that the example parses (YAML test) and that a scratch reader instance loads classifications from a scratch master database.
- [ ] 7.2 p1 as a reader of taichi's database, once the user's network design is in place. Verify that the aggregates' mapping counts in p1's log equal taichi's, and that the sector list matches at a quiet time (the doc's verification steps).
