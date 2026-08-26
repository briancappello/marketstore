# REST API

A read-only HTTP surface returning row-oriented JSON, intended for browsers
and ordinary tooling. It shares its query implementation with the JSON-RPC
surface and returns plain JSON rather than the binary column format used by
`/rpc`.

Authentication, TLS, and response compression are not provided. Put a reverse
proxy in front of MarketStore if you need them.

## Endpoints

### `GET /v1/health`

A cheap liveness/readiness probe that touches no catalog data. Returns
`200 {"status":"ok"}` once the server is queryable and `503` while it is still
starting, matching the startup behaviour of every other endpoint. Intended for
status indicators and uptime checks.

```console
$ curl 'http://localhost:5993/v1/health'
{"status":"ok"}
```

### `GET /v1/bars/{symbol}`

Exactly one symbol. A comma or `*` is rejected with `400`.

| Parameter | Default | Notes |
|---|---|---|
| `timeframe` | `1D` | Any timeframe present in the store. |
| `limit` | `1500` | Maximum `10000`; above that is a `400`. |
| `start` | unset | RFC3339 or unix epoch seconds. |
| `end` | unset | RFC3339 or unix epoch seconds. |

With neither `start` nor `end`, the most recent `limit` bars are returned.
`limit` always applies, so it is an upper bound on the response size even
when a range is given.

```console
$ curl 'http://localhost:5993/v1/bars/AAPL?timeframe=1D&limit=2'
{"symbol":"AAPL","timeframe":"1D","bars":[
  {"time":"2024-01-12T00:00:00Z","open":1.0,"high":2.0,"low":0.5,"close":1.5,"volume":1000},
  {"time":"2024-01-15T00:00:00Z","open":1.5,"high":2.5,"low":1.0,"close":2.0,"volume":1200}]}
```

At the default limit, `1D` covers roughly six years, `60Min` about ten
months, `5Min` about nineteen trading days, and `1Min` about four.

### `GET /v1/quotes`

Latest bar plus previous close per symbol. Omit `symbols` for the whole
catalog.

| Parameter | Default | Notes |
|---|---|---|
| `symbols` | all | Comma-separated. `*` and `/` are rejected. |
| `timeframe` | `1D` | |

Symbols with no data are omitted rather than failing the request.
`prev_close` is `null` when only one bar exists.

### `GET /v1/symbols`

| Parameter | Default | Notes |
|---|---|---|
| `format` | `symbol` | `tbk` returns full TimeBucketKey names. |
| `timeframe` | unset | Filter to symbols holding this timeframe. |
| `date` | unset | Filter to symbols holding data on this date. |

### `GET /v1/watchlists`, `GET /v1/watchlists/{name}`

Current rankings from the watchlist plugin. With the plugin not loaded, the
collection endpoint returns an empty list and the single endpoint returns
`404`.

## Conventions

Timestamps are RFC3339 under the key `time`. All other column names are
lowercased. Values that are `NaN` or infinite render as `null`, because JSON
cannot represent them.

Errors are `{"error": "message"}`, with the status carrying the category:
`400` for a malformed parameter, `404` for an unknown symbol or watchlist,
`503` while the server is still starting.

## Caching

Bar responses bounded by an `end` in the past are immutable and are served
with a long `max-age` and an `ETag`. Any other bar request is `no-cache`,
because the newest bar in an open period still changes. Quotes carry a short
`max-age` so a fronting proxy can collapse concurrent requests.

`If-None-Match` is honoured and returns `304`.

## CORS

No CORS headers are emitted unless `rest_allowed_origins` is configured in
`mkts.yml`, so the API is same-origin by default:

```yaml
rest_allowed_origins:
  - https://app.example.com
```

The literal `*` allows any origin. Because MarketStore has no
authentication, enabling this exposes all stored data to any page on the
listed origins.
