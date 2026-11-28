## MODIFIED Requirements

### Requirement: Sync scope
A sync SHALL cover the tracked symbols: the configured symbol set plus every symbol that already has data in this MarketStore instance. The configured symbol set SHALL be the reference-data universe when `sql:` is configured, and the static `symbols` list otherwise. When the universe changes between syncs, the next sync SHALL cover the new universe. When the configured set is `["*"]`, every symbol that Massive returns SHALL be tracked. For each tracked symbol, a sync SHALL write that symbol's full split history and SHALL neutralize stored splits that Massive no longer returns.

#### Scenario: Symbol added only through dynamic ticks
- **WHEN** TSLA is not in the configured symbols, but has a `TSLA/TRADE` bucket from a dynamic tick subscription
- **THEN** the next sync writes TSLA's splits

#### Scenario: Untracked symbol
- **WHEN** Massive returns a split for a symbol that is not configured and has no data in this instance, and the configured set is not `["*"]`
- **THEN** no bucket is created for that symbol

#### Scenario: Symbol added to the universe
- **WHEN** the reference-data sync adds a ticker to the universe after the last split sync
- **THEN** the next split sync writes that ticker's splits without a restart
