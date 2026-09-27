## Purpose

Defines the three trading sessions of a US equities trading day. Also defines how a requested session and `as_of` time resolve to the time window that watchlist rankings are computed over.

## ADDED Requirements

### Requirement: Session boundaries follow the exchange calendar
The system SHALL divide every trading day into three consecutive sessions. It SHALL take the boundaries from the exchange calendar, in America/New_York local time:
- `premarket`: 04:00 up to the regular open (09:30).
- `regular`: 09:30 up to the regular close. The regular close is 16:00, or the early-close time on early-close days.
- `afterhours`: the regular close up to the regular close plus 4 hours.

Each session SHALL include its start and exclude its end. Weekends and exchange holidays SHALL have no sessions.

#### Scenario: Normal trading day
- **WHEN** the sessions for a normal trading day are resolved
- **THEN** premarket is 04:00–09:30, regular is 09:30–16:00 and afterhours is 16:00–20:00, all America/New_York

#### Scenario: Early-close day
- **WHEN** the sessions for an early-close day with a 13:00 close are resolved
- **THEN** regular is 09:30–13:00 and afterhours is 13:00–17:00

#### Scenario: Daylight saving time does not shift sessions
- **WHEN** sessions are resolved for a trading day in EST (winter) and for one in EDT (summer)
- **THEN** both use the same local wall-clock boundaries, and no bar between 19:00 and 20:00 local time is assigned to the next trading day

#### Scenario: Bar on a session boundary
- **WHEN** a 1Min bar starts at exactly 09:30
- **THEN** it belongs to the regular session, not to premarket

### Requirement: Window resolution from session and as_of
Given a requested `session` and an `as_of` date or date-time, the system SHALL resolve a ranking window `[start, end]` on the trading date of `as_of`:
- `as_of` falls inside the session: the window is session start to `as_of`.
- `as_of` is at or after the session end: the window is the whole session.
- `as_of` is before the session start, on a trading date before the current one: the window is the whole session.
- `as_of` is a date with no time, on a past trading date: the window is the whole session.

An `as_of` with no timezone SHALL be read as America/New_York.

#### Scenario: as_of inside the session
- **WHEN** rankings are requested for session `regular` with `as_of` 2026-09-21 11:15
- **THEN** the window is 2026-09-21 09:30 to 11:15

#### Scenario: as_of after the session
- **WHEN** rankings are requested for session `premarket` with `as_of` 2026-09-21 10:00
- **THEN** the window is the whole premarket session, 2026-09-21 04:00 to 09:30

#### Scenario: as_of before the session on a past date
- **WHEN** rankings are requested with `as_of` 2026-09-21 08:00 (a past trading date) for each session
- **THEN** premarket covers 04:00–08:00, regular covers 09:30–16:00 and afterhours covers 16:00–20:00, all on 2026-09-21

#### Scenario: Date-only as_of
- **WHEN** rankings are requested for session `afterhours` with `as_of` 2026-09-21 (no time)
- **THEN** the window is the whole afterhours session of 2026-09-21

### Requirement: Non-trading dates are rejected
The system SHALL reject a request whose `as_of` falls on a weekend or an exchange holiday. The error SHALL say that the date is not a trading day.

#### Scenario: Weekend as_of
- **WHEN** rankings are requested with `as_of` 2026-09-26 (a Saturday)
- **THEN** the request fails with a "not a trading day" error

#### Scenario: Holiday as_of
- **WHEN** rankings are requested with `as_of` on an exchange holiday
- **THEN** the request fails with a "not a trading day" error

### Requirement: Sessions that have not started or lie in the future are rejected
The system SHALL reject a request for a session that has not started yet at the time of the request. This includes any `as_of` later than the current time.

#### Scenario: Today's session not started yet
- **WHEN** at 08:00 on a trading day, rankings are requested for session `regular` of that day
- **THEN** the request fails with a "session not started" error

#### Scenario: Future as_of
- **WHEN** rankings are requested with an `as_of` later than the current time
- **THEN** the request fails with a "session not started" error

### Requirement: Defaults when session or as_of is omitted
When parameters are omitted, the system SHALL resolve them as follows:
- No `session` and no `as_of`: the live session. That is the session in progress now, or, when no session is in progress, the most recently completed session.
- `session` without `as_of`: the most recent occurrence of that session that has started. If it is in progress, the window ends now.
- `as_of` date-time without `session`: the session that contains `as_of`. When no session contains it, the most recent session that ended at or before `as_of`.
- A date-only `as_of` without `session`: the regular session of that trading date.

#### Scenario: No parameters during regular hours
- **WHEN** at 11:00 on a trading day rankings are requested with no parameters
- **THEN** the live regular session of that day is used, with the window 09:30 to now

#### Scenario: No parameters overnight
- **WHEN** at 22:00 on a trading day rankings are requested with no parameters
- **THEN** the completed afterhours session of that day is used

#### Scenario: Session only
- **WHEN** at 11:00 on a trading day rankings are requested for session `afterhours` with no `as_of`
- **THEN** the afterhours session of the previous trading day is used, because today's has not started

#### Scenario: Date-only as_of without session
- **WHEN** rankings are requested with `as_of` 2026-09-21 (a past trading date) and no `session`
- **THEN** the whole regular session of 2026-09-21 is used
