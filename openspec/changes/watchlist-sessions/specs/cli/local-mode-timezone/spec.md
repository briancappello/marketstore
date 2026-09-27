## Purpose

Makes the CLI in local mode (`marketstore connect --dir`) show and parse bar times in the database's configured timezone, the same as the server.

## ADDED Requirements

### Requirement: Local mode uses the configured timezone
When `marketstore connect` opens a database directory in local mode, it SHALL decode and show bar timestamps using the timezone that the database was written with. That is the `timezone` from the server configuration. The same bar SHALL show the same timestamp in local mode and in remote mode.

#### Scenario: Regular open shown correctly
- **WHEN** a database written with timezone America/New_York is opened in local mode and the 09:30 ET bar of an EDT trading day is shown
- **THEN** the bar's timestamp is 13:30 UTC, not 08:30 UTC

#### Scenario: Local and remote agree
- **WHEN** the same bucket and range are shown once in local mode and once in remote mode against the same data
- **THEN** both show identical timestamps

### Requirement: Timezone can be stated explicitly
Local mode SHALL accept a way to name the configuration file or the timezone. Without either, it SHALL use the configuration file in the working directory when one exists. If none exists, it SHALL use UTC and print a warning that times may be shifted.

#### Scenario: No configuration available
- **WHEN** local mode starts with no configuration file and no timezone given
- **THEN** it prints a warning that times are decoded as UTC
