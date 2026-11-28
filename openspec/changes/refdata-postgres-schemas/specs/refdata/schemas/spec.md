## Purpose

Defines who owns which reference and application data in the shared Postgres database, how each part is migrated, which database roles may read or write it, and how marketstore is configured to use it.

## ADDED Requirements

### Requirement: Three schemas, one owner each
The database SHALL hold reference data in schema `market`, the `fin` library's domain data in schema `fin`, and the `fa-techan` server's data in schema `web`. Each schema SHALL be created and migrated by exactly one project: `market` by marketstore, `fin` by `fin`, `web` by `fa-techan`. A foreign key SHALL only point from `web` to `fin` or `market`, or from `fin` to `market`.

#### Scenario: A fin table references a security
- **WHEN** a `fin` table stores which security a row is about
- **THEN** it holds a foreign key to `market.security`

#### Scenario: No reverse reference
- **WHEN** the `market` schema is inspected
- **THEN** no `market` table has a foreign key into `fin` or `web`

### Requirement: Roles enforce ownership
The role marketstore connects as SHALL own `market`. The role the Python stack connects as SHALL own `fin` and `web` and SHALL have only `SELECT` on `market`. A role `market_reader` SHALL have only `SELECT` on `market`, including on tables created by later migrations.

#### Scenario: Python cannot write reference data
- **WHEN** the Python role runs `UPDATE market.security SET name = 'x'`
- **THEN** Postgres rejects it with `permission denied`

#### Scenario: A reader sees a new table
- **WHEN** a marketstore migration adds a table to `market`
- **THEN** `market_reader` can `SELECT` from it without a new grant

### Requirement: marketstore migrates `market` on the master
A marketstore master configured with `sql:` SHALL apply pending `market` migrations at startup, before any plugin starts, holding a database lock so concurrent starts cannot apply a migration twice. The same migrations SHALL be available as `marketstore tool refdata migrate`, and `marketstore tool refdata status` SHALL print the applied and pending versions. A marketstore in `reader` mode SHALL NOT migrate. It SHALL log an error naming both versions when the database's schema version is older than the one it requires.

#### Scenario: Fresh database
- **WHEN** a master starts against a database with no `market` schema and no reference tables
- **THEN** it creates `market` at the latest version before plugins start

#### Scenario: Existing tables are adopted
- **WHEN** a master starts against a database whose reference tables are in `public`
- **THEN** the tables are moved into `market` with their rows and ids unchanged, and the result has the same schema as the fresh path

#### Scenario: Replica against an older schema
- **WHEN** a `reader` instance starts and the database's `market` version is older than it requires
- **THEN** it logs an error naming both versions and does not change the schema

### Requirement: One `sql:` configuration
marketstore SHALL take its Postgres connection from one top-level `sql:` section with a `dsn` and a `mode` (`master` or `reader`). Plugins SHALL obtain reference data through marketstore, not through their own DSNs or configured SQL. Startup SHALL fail, with an error naming the key and its replacement, when the configuration contains `symbols_dsn`, `symbols_query` or `sync_queries` in the massive plugin, or `dsn` or `query` in a watchlist aggregate's `strategy_config`. Without `sql:`, marketstore SHALL start with reference data disabled.

#### Scenario: Removed key
- **WHEN** `mkts.yml` contains `symbols_query` in the massive plugin's config
- **THEN** startup fails with an error that names `symbols_query` and the `sql:` section

#### Scenario: No Postgres configured
- **WHEN** `mkts.yml` has no `sql:` section
- **THEN** marketstore starts, the massive plugin uses its static `symbols`, and the sector and industry aggregates are empty
