## Purpose

Defines the meaning of `prior_close`, `pct_change`, `open`, volume and relative volume in watchlist rankings under the session basis and the traditional basis. Also defines which watchlists exist in which session.

## ADDED Requirements

### Requirement: Session-scoped running values
For every session, the system SHALL compute each symbol's `open`, last price, high, low and `volume` only from that session's 1Min bars inside the resolved window:
- `open` is the open of the first bar in the session.
- `volume` is the sum of the volume of the bars in the window.

The same bar SHALL never be counted twice, even when it is rewritten or delivered more than once.

#### Scenario: Premarket volume excluded from regular volume
- **WHEN** a symbol trades 100k shares in premarket and 2M shares in the regular session
- **THEN** its regular-session `volume` is 2M, and its premarket-session `volume` is 100k

#### Scenario: Regular open is the 09:30 bar
- **WHEN** a symbol's first premarket bar opens at 50.00 and its 09:30 bar opens at 52.00
- **THEN** `open` is 50.00 in the premarket session and 52.00 in the regular session

#### Scenario: Rewritten bar
- **WHEN** the 10:15 bar is written three times with volumes 1,000, 3,000 and 5,000
- **THEN** that minute adds 5,000 to the session's volume

### Requirement: Premarket volume is reported
The system SHALL report `premarket_volume` as the symbol's premarket-session volume for the trading date:
- during premarket: the volume so far in the window
- in the regular and afterhours sessions: the whole premarket session's volume

#### Scenario: Premarket volume carried into regular session
- **WHEN** rankings for the regular session are produced for a symbol that traded 100k shares in premarket
- **THEN** each entry for that symbol has `premarket_volume` equal to 100k

### Requirement: Official close
The system SHALL treat the official close of a trading date as the close of that date's 1D bar. When that bar does not exist yet, it SHALL use the close of the last regular-session 1Min bar of that date.

#### Scenario: Daily bar present
- **WHEN** the 1D bar for 2026-09-22 has close 339.75 and the last regular 1Min bar has close 339.73
- **THEN** the official close of 2026-09-22 is 339.75

#### Scenario: Daily bar not yet written
- **WHEN** at 16:05 the 1D bar for today does not exist and the 15:59 bar closed at 101.20
- **THEN** today's official close is 101.20

### Requirement: Session basis
Bare watchlists (names without a `_TRADITIONAL` suffix) SHALL use the session basis:
- `prior_close` is the closing price of the session immediately before:
  - premarket: the last afterhours 1Min close of the previous trading date
  - regular: the last premarket 1Min close of the same trading date
  - afterhours: the official close of the same trading date
- `pct_change` is `(last price - prior_close) / prior_close * 100`.

Bare watchlists SHALL be available in all three sessions.

#### Scenario: Regular-session baseline is the premarket close
- **WHEN** a symbol's last premarket bar closed at 10.00 and its last price at 10:30 is 11.00
- **THEN** in the bare regular-session rankings, its `prior_close` is 10.00 and its `pct_change` is 10.0

#### Scenario: Afterhours baseline is the official close
- **WHEN** a symbol's official close today is 20.00 and its last afterhours price is 19.00
- **THEN** in the bare afterhours rankings, its `prior_close` is 20.00 and its `pct_change` is -5.0

#### Scenario: Premarket baseline is the prior afterhours close
- **WHEN** a symbol's last afterhours bar on the previous trading date closed at 30.00 and its premarket price is 33.00
- **THEN** in the bare premarket rankings, its `prior_close` is 30.00 and its `pct_change` is 10.0

### Requirement: Traditional basis
Watchlists named with a `_TRADITIONAL` suffix SHALL use the traditional basis:
- `prior_close` is the official close of the previous trading date.
- `pct_change` is `(last price - prior_close) / prior_close * 100`.

Traditional watchlists SHALL exist only in the regular session. A request for a traditional watchlist in the premarket or afterhours session SHALL fail with a "not available in this session" error.

#### Scenario: Traditional regular-session change
- **WHEN** a symbol's previous official close is 100.00 and its last regular-session price is 104.00
- **THEN** in `PCT_CHANGE_UP_TRADITIONAL`, its `prior_close` is 100.00 and its `pct_change` is 4.0

#### Scenario: Traditional list outside regular session
- **WHEN** `PCT_CHANGE_UP_TRADITIONAL` is requested for the afterhours session
- **THEN** the request fails with a "not available in this session" error

### Requirement: Which watchlists exist in which session
Every watchlist whose ranking or output depends on `prior_close` or `pct_change` SHALL have a `_TRADITIONAL` variant in the regular session. Watchlists that are traditional by definition SHALL exist only under their `_TRADITIONAL` name:
- `GAP_UP_TRADITIONAL` and `GAP_DOWN_TRADITIONAL` rank the 09:30 open against the previous official close. They replace `GAP_UP` and `GAP_DOWN`, which SHALL no longer exist.
- `SMA_CROSS_UP_TRADITIONAL` and `SMA_CROSS_DOWN_TRADITIONAL` compare the last price with a moving average of daily closes. They replace `SMA_CROSS_UP` and `SMA_CROSS_DOWN`.

`VOLUME_UP` and `VOLUME_DOWN` SHALL be available. They rank by session `volume` among gainers and among losers, respectively, by `pct_change`. Like other lists that depend on `pct_change`, they SHALL have `_TRADITIONAL` variants in the regular session.

#### Scenario: Watchlist inventory in the regular session
- **WHEN** the watchlists for the regular session are listed
- **THEN** the list includes both `PCT_CHANGE_UP` and `PCT_CHANGE_UP_TRADITIONAL`, and `GAP_UP_TRADITIONAL`, and does not include `GAP_UP`

#### Scenario: Volume lists available
- **WHEN** the watchlists for the premarket session are listed
- **THEN** the list includes `VOLUME_UP` and `VOLUME_DOWN`

#### Scenario: Watchlist inventory in premarket
- **WHEN** the watchlists for the premarket session are listed
- **THEN** no `_TRADITIONAL` watchlist is included

### Requirement: Symbols without baseline data drop out
When the data that a basis needs for a symbol does not exist, the system SHALL leave that symbol out of every ranking that uses that data for that session. It SHALL NOT fall back to an earlier session or trading date. The data in question includes the prior session's close, the previous official close and the session volume median.

#### Scenario: No afterhours prints on the previous day
- **WHEN** a symbol had no afterhours bars on the previous trading date
- **THEN** it does not appear in any bare premarket ranking that uses `prior_close` or `pct_change`

#### Scenario: No premarket prints today
- **WHEN** a symbol had no premarket bars today
- **THEN** it does not appear in any bare regular-session ranking that uses `prior_close` or `pct_change`, and it can still appear in `PCT_CHANGE_UP_TRADITIONAL`

### Requirement: Per-session relative volume
The system SHALL compute a symbol's relative volume in a session as the session `volume` in the window divided by the median of that same session's full volume over the previous N trading dates. N is the configured median window, 50 by default. Session volumes for this median SHALL come from 1Min bars, so the numerator and the denominator measure the same thing. Relative volume SHALL be available in all three sessions. A symbol without any past volume for the session SHALL be left out of rankings that use relative volume.

#### Scenario: Normal regular session
- **WHEN** a symbol's regular-session volume at the close equals its 50-day median regular-session volume
- **THEN** its relative volume is 1.0

#### Scenario: Premarket relative volume
- **WHEN** a symbol's premarket volume in the window is 300k and its 50-day median premarket volume is 100k
- **THEN** its premarket relative volume is 3.0

#### Scenario: Median uses only earlier dates
- **WHEN** rankings for trading date D are computed, live or rewound
- **THEN** the median uses only session volumes from trading dates before D
