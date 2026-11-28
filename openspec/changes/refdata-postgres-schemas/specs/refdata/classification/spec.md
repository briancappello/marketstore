## Purpose

Classifies every equity in the universe by sector and industry in one consistent scheme, records where each classification came from, and keeps the previous classification when the source is unavailable.

## ADDED Requirements

### Requirement: Yahoo's scheme with inferred fallbacks
Every equity in the universe SHALL be classified in Yahoo's sector and industry scheme. When Yahoo has no sector for an equity, the sector SHALL be inferred from its SEC SIC code; failing that, from its Nasdaq screener industry; failing that, it SHALL be unclassified. Each classification SHALL record its source (`yahoo`, `inferred_sic`, `inferred_nasdaq`) and the SIC code when known.

#### Scenario: Yahoo answers
- **WHEN** Yahoo reports a sector and industry for an equity
- **THEN** the equity has that sector and industry, with source `yahoo`

#### Scenario: Inferred from SIC
- **WHEN** Yahoo has no sector for an equity and its SIC code maps to a sector
- **THEN** the equity has the inferred sector, no industry, and source `inferred_sic`

### Requirement: Classification survives a Yahoo outage
Failed Yahoo requests SHALL be retried once after a pause. If more than 5% of equities still fail, the sync SHALL skip classification for that run, write no classification, and record the reason. The universe changes of the run SHALL still apply.

#### Scenario: Yahoo throttles the run
- **WHEN** Yahoo fails for 20% of equities after the retry
- **THEN** every equity keeps its previous classification, and the run records that classification was skipped

### Requirement: Answers are reused within a day
A Yahoo answer younger than 20 hours SHALL be reused instead of fetched again, so an interrupted or repeated run completes in minutes. The stored answers SHALL be in the database.

#### Scenario: Rerun after an interruption
- **WHEN** a sync is interrupted during classification and run again an hour later
- **THEN** it fetches only the equities it had not reached

### Requirement: Unused sectors and industries are removed
After classification, sectors and industries that no classification refers to SHALL be removed. Delisted equities whose classification predates the scheme SHALL have it cleared.

#### Scenario: An industry nobody uses any more
- **WHEN** the last equity in an industry is reclassified
- **THEN** that industry no longer exists after the run
