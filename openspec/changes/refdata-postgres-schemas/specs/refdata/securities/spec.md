## Purpose

Gives every security a stable identity independent of its ticker, so that ticker reuse, ticker renames and delistings are recorded faithfully and references to a security never silently move to another one.

## ADDED Requirements

### Requirement: Securities have a stable identity
Each security SHALL have a permanent id and SHALL record its composite FIGI and CIK when the vendor provides them. A security's id SHALL never change and SHALL never be reassigned to another security. Ids existing before this change SHALL be preserved.

#### Scenario: Existing ids survive the migration
- **WHEN** taichi's database is migrated
- **THEN** every former `asset.id` identifies the same security, and backfill coverage rows keyed by it are unchanged

### Requirement: Tickers are listings with a validity period
A security's tickers SHALL be recorded as listings, each with a start and an optional end. At most one security SHALL hold an open listing for a given ticker at any time. A delisting SHALL end the security's open listing; it SHALL NOT delete the security or its earlier listings.

#### Scenario: Delisting keeps history
- **WHEN** a security is delisted
- **THEN** its listing has an end date, and the security and its classification history remain readable

#### Scenario: Two securities cannot hold one ticker
- **WHEN** a write would open a second listing for a ticker that already has an open listing
- **THEN** the database rejects it

### Requirement: Identity decides reuse, renames and returns
When the sync sees a ticker, it SHALL identify the security by composite FIGI first:
- A FIGI that matches a security means that security, even if it now trades under a different ticker. That is a rename: the old listing ends and a new one starts for the same security.
- A FIGI that matches no security means a new security. If another security holds that ticker's open listing, that listing SHALL end first.
- A ticker without a FIGI SHALL match the security holding its open listing only when that security has no FIGI either, and the run SHALL count it as identity-unverified.

#### Scenario: A recycled ticker
- **WHEN** PSNYW was delisted for security A (FIGI `F1`), and Massive now lists PSNYW with FIGI `F2`
- **THEN** a new security B is created with an open PSNYW listing, and A is unchanged

#### Scenario: A returning security
- **WHEN** PSNYW was delisted for security A (FIGI `BBG017H4W5K7`), and Massive lists PSNYW again with FIGI `BBG017H4W5K7`
- **THEN** A gets a new open PSNYW listing, and no new security is created

#### Scenario: A rename
- **WHEN** security A holds an open listing for `FB`, and Massive lists A's FIGI under `META`
- **THEN** A's `FB` listing ends and A gets an open `META` listing

### Requirement: The universe
The universe SHALL be the open-listed tickers of securities marked in the universe. marketstore SHALL ingest exactly the universe, and the universe SHALL be readable by every role with access to `market`.

#### Scenario: A deactivated security is not ingested
- **WHEN** a security is marked out of the universe but its listing is open
- **THEN** it is not in the universe and marketstore does not subscribe to it
