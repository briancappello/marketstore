## Purpose

Defines how clients get watchlist rankings, live and for a past session and time. Covers the JSON-RPC, gRPC and REST APIs and the WebSocket push, including the metadata that says which session and basis a ranking uses.

## ADDED Requirements

### Requirement: Session and as_of parameters on every query API
The JSON-RPC `ListWatchlists` method, the gRPC `ListWatchlists` RPC, and the REST endpoints `GET /v1/watchlists` and `GET /v1/watchlists/{name}` SHALL accept two optional parameters:
- `session`: one of `premarket`, `regular` or `afterhours`
- `as_of`: an ISO-8601 date or date-time

The parameters SHALL resolve as defined by the trading-sessions capability. A request without either parameter SHALL return the live rankings.

#### Scenario: REST rewind
- **WHEN** a client calls `GET /v1/watchlists/PCT_CHANGE_UP?session=regular&as_of=2026-09-21T11:15`
- **THEN** the response holds the PCT_CHANGE_UP ranking over 2026-09-21 09:30–11:15

#### Scenario: gRPC rewind
- **WHEN** a client calls gRPC `ListWatchlists` with `session` `premarket` and `as_of` `2026-09-21`
- **THEN** the response holds every watchlist available in premarket, each computed over the whole premarket session of 2026-09-21

#### Scenario: Existing clients unchanged in shape
- **WHEN** a client calls `ListWatchlists` without `session` or `as_of`
- **THEN** the response has the same structure as before, with the metadata fields added

### Requirement: Ranking metadata
Every returned watchlist SHALL carry:
- `basis`: `session` or `traditional`
- `session`: the resolved session
- `trading_date`: the trading date of the session (YYYY-MM-DD, America/New_York)
- `window_start` and `window_end`: the resolved window
- `complete`: true when the window covers the whole session

#### Scenario: Metadata for a partial window
- **WHEN** rankings are requested for session `regular` with `as_of` 2026-09-21T11:15
- **THEN** each returned watchlist has `session` `regular`, `trading_date` `2026-09-21`, `window_end` 11:15 and `complete` false

#### Scenario: Metadata for a traditional list
- **WHEN** `PCT_CHANGE_UP_TRADITIONAL` is returned
- **THEN** its `basis` is `traditional`

### Requirement: Error responses
Parameter and availability errors SHALL be client errors with a message saying what is wrong:
- REST SHALL return 400 for an invalid `session` or `as_of`, a non-trading date, a session not started, or a watchlist that is not available in the requested session.
- REST SHALL return 404 for an unknown watchlist name.
- gRPC SHALL use `InvalidArgument` for the 400 cases and `NotFound` for the 404 case.
- JSON-RPC SHALL return an error whose message states the cause.

#### Scenario: Weekend over REST
- **WHEN** a client calls `GET /v1/watchlists?as_of=2026-09-26T10:00`
- **THEN** the response status is 400 and the message says 2026-09-26 is not a trading day

#### Scenario: Traditional list in afterhours over gRPC
- **WHEN** a client requests `GAP_UP_TRADITIONAL` for session `afterhours` over gRPC
- **THEN** the call fails with `InvalidArgument`

#### Scenario: Renamed list
- **WHEN** a client calls `GET /v1/watchlists/GAP_UP`
- **THEN** the response status is 404

### Requirement: Listing returns only lists available in the session
A request that returns all watchlists SHALL include only the watchlists that are available in the resolved session.

#### Scenario: All watchlists in afterhours
- **WHEN** all watchlists are requested for an afterhours session
- **THEN** the response contains only bare watchlists

### Requirement: Live push carries session metadata
Each `watchlist_update` push on the `WATCHLISTS/<timeframe>/<name>` stream SHALL carry `basis`, `session`, `trading_date` and `as_of`. Traditional watchlists SHALL be pushed only during the regular session. Existing payload fields SHALL keep their names and positions in the message.

#### Scenario: Push during premarket
- **WHEN** the ranking loop runs during premarket
- **THEN** only bare watchlists are pushed, each with `session` `premarket` and `basis` `session`

#### Scenario: Push during regular session
- **WHEN** the ranking loop runs during the regular session
- **THEN** both bare and `_TRADITIONAL` watchlists are pushed, and each push has the `basis` that matches it

### Requirement: Session rollover in live rankings
Live rankings SHALL switch to the next session at each session boundary. Between the end of afterhours and the next premarket start, and on non-trading days, live rankings SHALL show the most recently completed session.

#### Scenario: Rollover at the open
- **WHEN** the clock passes 09:30 on a trading day
- **THEN** the next live rankings are for the regular session, with volumes counted from 09:30

#### Scenario: Weekend
- **WHEN** live rankings are requested on a Saturday
- **THEN** they show Friday's afterhours session with `complete` true

### Requirement: Rewound and live rankings agree
For the same bars and the same window, a rewound ranking SHALL equal the live ranking computed over that window. When nothing has changed on disk, repeated rewind requests for a completed session SHALL return identical results.

#### Scenario: Rewind of a completed session matches its final live state
- **WHEN** the regular session of trading date D has ended, no bars of D have been rewritten, and the regular session of D is requested with `as_of` at its end
- **THEN** the result equals the last live regular-session ranking of D

### Requirement: Rewind does not disturb live rankings
Serving a rewind request SHALL NOT change live rankings, live state, or the timing of the live ranking loop.

#### Scenario: Rewind during market hours
- **WHEN** a rewind request is served during the regular session
- **THEN** the live rankings and the pushes are the same as they would be without that request
