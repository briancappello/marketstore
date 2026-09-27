## Why

Watchlist rankings treat a trading day as one undivided 04:00–20:00 window. After-hours prints move the live rankings. The next day's baseline is the last after-hours print instead of the official close. Premarket volume is never tracked. Clients cannot ask for rankings at any point in the past. Traders think in three separate sessions (premarket, regular, after-hours). They need rankings that match each session, both live and for a past date and time.

## What Changes

- Each trading day is split into three **sessions**, taken from the exchange calendar in America/New_York:
  - premarket: 04:00–09:30
  - regular: 09:30 to the regular close, which is earlier on early-close days
  - after-hours: the regular close to the close plus 4h
- Each session has its own running state: open, last price, high, low and volume.
- **Session basis (the default for every list).**
  - `prior_close` is the close of the session immediately before.
  - For premarket, that is the previous trading day's after-hours session.
  - For regular, it is the same day's premarket session.
  - For after-hours, it is the same day's official close.
  - `pct_change` is measured against that `prior_close`.
- **Traditional basis (regular session only).**
  - `*_TRADITIONAL` lists use the previous trading day's official regular-session close (the 1D bar close) as `prior_close`.
  - Traditional lists have no premarket or after-hours form.
- **BREAKING:** Bare list names (e.g. `PCT_CHANGE_UP`) switch to the session basis. The fields `prior_close`, `pct_change` and `open` keep their names, but their meaning now depends on the list.
- **BREAKING:** `GAP_UP`/`GAP_DOWN` are renamed to `GAP_UP_TRADITIONAL`/`GAP_DOWN_TRADITIONAL`. Gaps are measured from the 09:30 open against the previous official close, not from the 04:00 premarket open.
- Each ranking response and each pushed `watchlist_update` carries `basis`, `session`, `trading_date` and `as_of`.
- **Rewind:** callers can ask for rankings with a `session` and an `as_of` date or time.
  - An `as_of` inside the session covers the session open to `as_of`.
  - Any other time on a past trading day covers the whole session.
  - A weekend or holiday is an error.
- **Relative volume** is computed per session. It compares against a 50-day median of that session's volume, built from 1Min bars. This fixes the ~0.7x offset that comes from comparing 1Min volume against consolidated 1D volume. It also lets MOMENTUM and RELATIVE_VOLUME rank in every session.
- A new derived bucket, `SYM/1D/SESSIONS`, stores one row per symbol per trading day. Each row holds each session's volume and closing price. It follows explicit derived-data rules: store facts, not rolling stats; rebuildable from bars; versioned; late writes invalidate rows; one rebuild command; a defined fallback; only the leader writes.
- `premarket_volume` gets filled in. Today it is always zero.
- `VOLUME_UP`/`VOLUME_DOWN` are registered. They are configured today but never produced.
- A date-only `as_of` without a `session` means the regular session.
- A symbol that lacks the baseline data a session needs drops out of that session's rankings. There is no walking back to earlier sessions.
- Fix, as a separate commit: `marketstore connect -d` (local mode) ignores the configured timezone. Every bar time shows up shifted by 5h.

## Capabilities

### New Capabilities
- `watchlist/trading-sessions`: how sessions are defined, and how `session` + `as_of` resolve to a time window.
- `watchlist/ranking-basis`: what the session and traditional bases mean, the field meanings for each basis, which lists are available in which session, and the rule that symbols without data drop out.
- `watchlist/rankings-api`: live and rewound rankings over JSON-RPC, gRPC, REST and the WebSocket push, including the response metadata.
- `watchlist/session-facts`: the `SYM/1D/SESSIONS` derived bucket and its derived-data requirements. Per-session volume medians are built from it.
- `cli/local-mode-timezone`: local-mode CLI sessions decode bar times using the configured timezone.

### Modified Capabilities
<!-- None: no specs exist yet in openspec/specs/. -->

## Impact

- **Code (this repo):**
  - `contrib/watchlist/framework`: `day_state.go`, `baseline.go`, `trigger.go`, `worker.go`, `rankings.go`, `push.go`, `state.go`, plus new session and derivation packages
  - `contrib/calendar`: session boundaries
  - `frontend`: `list_watchlists.go`, `grpc.go`, `rest_handlers.go`, `watchlist_provider.go`
  - `plugins/bgworker`: the `WatchlistDataSource` interface
  - `cmd/start/plugins.go`
  - `proto/marketstore.proto`, plus the regenerated Go code
  - `cmd/connect`
- **Plugin ABI:** `WatchlistDataSource` gains a method, so the host binary and `watchlist.so` must be rebuilt together. `deploy.sh` already does this.
- **Other repos (tracked as tasks, edited in their own repos):**
  - `marketstore-watchlists`: the renamed GAP lists; which basis each strategy supports; SMA_CROSS's daily-close cache must use the `as_of` date instead of the current date.
  - `ta-droid`: list names and labels; the meaning of `prior_close` and `open`; showing `basis`/`session`.
- **Data:** a new bucket per symbol (`1D/SESSIONS`), plus a one-time backfill that reads existing 1Min history (~11.8k symbols).
- **Replication:** only the leader writes `1D/SESSIONS`. Replicas (e.g. p1) receive it through replication.
