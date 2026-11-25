## Purpose

Stores corporate events (this change: splits) per symbol, in one provider-neutral format. Any feeder can write events and any query function can read them. New event kinds can be added without a schema migration.

## ADDED Requirements

### Requirement: One bucket per symbol and event kind
The system SHALL store the events of each kind for a symbol in the fixed-length bucket `<SYMBOL>/1D/<KIND>`. This change defines one kind, `SPLITS`. A new kind SHALL use a new attribute group name and SHALL NOT change the buckets of existing kinds. The buckets SHALL be readable and writable through the normal query and write APIs.

#### Scenario: Split stored for a symbol
- **WHEN** a 2-for-1 split for AAPL that takes effect on 2020-08-31 is written
- **THEN** `AAPL/1D/SPLITS` holds one row for 2020-08-31, and a normal query of that bucket returns it

#### Scenario: Kinds do not collide
- **WHEN** a later change adds a dividend kind, and a dividend and a split take effect for the same symbol on the same date
- **THEN** each event is stored in its own bucket, and neither overwrites the other

### Requirement: Common event columns
Every event kind SHALL use these columns:
- `Epoch`: the start of the effective trading date, 00:00 America/New_York. Data with a timestamp before this instant is on the old basis.
- `PriceFactor` (float64): the number that pre-event prices are divided by to put them on the post-event basis.
- `VolumeFactor` (float64): the number that pre-event sizes and volumes are multiplied by.
- `SplitFrom`, `SplitTo` (float64): the ratio as published. Kinds without a ratio set both to 0.
- `Subtype` (byte): the variant within the kind. For `SPLITS`: forward split, reverse split, stock dividend.

For `SPLITS`, `PriceFactor` and `VolumeFactor` SHALL both equal `SplitTo / SplitFrom`. A factor of exactly 1.0 SHALL mean "no effect".

#### Scenario: Forward split factors
- **WHEN** a split with `SplitFrom` = 1 and `SplitTo` = 4 is stored
- **THEN** its `PriceFactor` and `VolumeFactor` are both 4.0

#### Scenario: Reverse split factors
- **WHEN** a split with `SplitFrom` = 10 and `SplitTo` = 1 is stored
- **THEN** its `PriceFactor` and `VolumeFactor` are both 0.1, and its `Subtype` is reverse split

### Requirement: Idempotent writes keyed by effective date
Writing an event SHALL replace any stored event of the same kind, symbol and effective date. Writing the same set of events again SHALL leave the bucket unchanged. If one write batch holds two events of the same kind for the same symbol and effective date, they SHALL be stored as one row whose factors are the product of the two.

#### Scenario: Re-sync is a no-op
- **WHEN** the same split history for a symbol is written twice
- **THEN** the bucket holds exactly one row per effective date, with the same values as after the first write

#### Scenario: Corrected ratio
- **WHEN** a stored split for 2024-06-10 with ratio 1:4 is written again with ratio 1:10
- **THEN** the bucket holds one row for 2024-06-10 with factor 10.0

#### Scenario: Two events on the same date
- **WHEN** a batch holds a 1:2 forward split and a 1:1.5 stock dividend for the same symbol, both effective 2023-03-01
- **THEN** the stored row for 2023-03-01 has `PriceFactor` = 3.0

### Requirement: Withdrawn events are neutralized
A writer that syncs a symbol's full event history SHALL neutralize every stored event of that kind that is no longer in the provider's history. To neutralize an event, the writer SHALL overwrite its row so that both factors are 1.0. A neutralized event SHALL have no effect on adjusted data.

#### Scenario: Provider deletes a split
- **WHEN** a stored AAPL split for 2021-01-04 is no longer returned by the provider, and AAPL's split history is synced
- **THEN** the 2021-01-04 row has `PriceFactor` = 1.0 and `VolumeFactor` = 1.0

### Requirement: Read events by symbol and kind
The system SHALL provide one read that returns a symbol's events for a given set of kinds, sorted by `Epoch` ascending, with neutralized events left out. A symbol with no bucket SHALL return an empty result, not an error.

#### Scenario: Symbol without events
- **WHEN** events are read for a symbol that has no `SPLITS` bucket
- **THEN** the read returns no events and no error
