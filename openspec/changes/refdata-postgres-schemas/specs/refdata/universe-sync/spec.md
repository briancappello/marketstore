## Purpose

Keeps the universe of tradable securities current from Massive and Alpaca every day before premarket, on the master, and makes the master ingest the new universe without a restart.

## ADDED Requirements

### Requirement: The universe is Massive and tradable on Alpaca
The sync SHALL put in the universe every security Massive lists as active with a supported type and Alpaca reports as tradable. A security that leaves the universe SHALL be:
- **delisted** (its listing ends) when neither Massive nor Alpaca lists its ticker;
- **deactivated** (out of the universe, listing still open) otherwise.

A deactivated security SHALL return to the universe when it is again on both. An empty database SHALL be populated by the same sync.

#### Scenario: Gone from both vendors
- **WHEN** a security in the universe is listed by neither Massive nor Alpaca
- **THEN** its listing ends with the sync date

#### Scenario: On one vendor only
- **WHEN** a security in the universe is listed by Massive but not by Alpaca
- **THEN** it is deactivated, and its listing stays open

#### Scenario: Back on both
- **WHEN** a deactivated security is listed by Massive and tradable on Alpaca again
- **THEN** it is back in the universe

#### Scenario: Empty database
- **WHEN** the sync runs against a database with no securities
- **THEN** afterwards the universe equals Massive's active tickers that are tradable on Alpaca

### Requirement: A truncated vendor response changes nothing
Before changing the universe, the sync SHALL check each vendor's list. If either list is empty, or smaller than the configured fraction (default 0.9) of the current universe, the sync SHALL make no universe change in that run and SHALL record the reason. Otherwise it SHALL apply every change, regardless of how many securities are delisted or deactivated.

#### Scenario: Truncated Massive list
- **WHEN** the universe has 12,000 securities and Massive returns 3,000 tickers
- **THEN** no security is added, delisted, deactivated or reactivated, and the run records that Massive's list was too small

#### Scenario: A large genuine backlog
- **WHEN** both vendor lists are complete and 351 securities are gone from both
- **THEN** all 351 are delisted

### Requirement: Vendor quirks do not fail the sync
An asset with an exchange code the sync does not know SHALL still be read. A security with no exchange from Massive SHALL take its exchange from Alpaca.

#### Scenario: A new exchange
- **WHEN** Alpaca returns assets on `TXSE`
- **THEN** the sync completes, and universe securities listed only on TXSE are added with that exchange

### Requirement: Daily schedule on the master
The sync SHALL run only on a master with sync enabled. It SHALL run daily at a configured time (America/New_York, default 01:30), and at startup when the last successful run is more than 24 hours old. Each run SHALL record when it started and finished, the vendor list sizes, the count of each kind of change, and any error. A failure in classification or index membership SHALL NOT undo the universe changes of the same run.

#### Scenario: Missed run
- **WHEN** the master starts and the last successful sync finished 30 hours ago
- **THEN** a sync starts without waiting for the scheduled time

#### Scenario: Replica
- **WHEN** an instance in `reader` mode reaches the sync time
- **THEN** no sync runs

### Requirement: Dry run
`marketstore tool refdata sync --dry-run` SHALL compute the same plan as a real run and print every count and the affected tickers. It SHALL write nothing to the database.

#### Scenario: Review before the first run
- **WHEN** the dry run is run against taichi's database
- **THEN** it prints the additions, delistings, deactivations, reactivations, renames and reuses, and the database is unchanged

### Requirement: The master ingests the new universe without a restart
After a sync changes the universe, the master's ingest SHALL subscribe to added tickers and unsubscribe from removed ones on its live connection. It SHALL schedule a history backfill for added tickers. A removed ticker's stored data SHALL be kept.

#### Scenario: New listing
- **WHEN** the 01:30 sync adds a ticker
- **THEN** the master streams it before 04:00 the same day without a restart, and its history is backfilled
