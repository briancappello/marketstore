# Stream Replay Plugin

A background worker plugin that provides historical data replay over WebSocket.
Clients connect to `/ws/replay`, send a subscribe message with a time range and
step interval, and the server streams matching bars from disk at the requested pace.

This is designed for backtesting scenarios where a client needs to simulate
receiving historical market data as if it were arriving in real time.

## Configuration

Add to `marketstore.yml`:

```yaml
bgworkers:
  - module: streamreplay.so
    name: StreamReplay
    config:
      endpoint: /ws/replay   # optional, defaults to /ws/replay
```

## Build

```bash
make -C contrib/streamreplay
```

## Protocol

All messages are encoded as [MessagePack](https://msgpack.org/) over WebSocket
binary frames.

### Subscribe (client → server)

```json
{
  "action": "subscribe",
  "tbks":   ["AAPL/1Min/OHLCV", "MSFT/1Min/OHLCV"],
  "start":  "2024-01-01 09:30:00+00:00",
  "end":    "2024-01-01 16:00:00+00:00",
  "step":   500
}
```

| Field    | Type       | Description                                                                                                                       |
|----------|------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `action` | `string`   | Must be `"subscribe"`                                                                                                             |
| `tbks`   | `[]string` | One or more fully-qualified TimeBucketKeys (`Symbol/Timeframe/AttributeGroup`)                                                    |
| `start`  | `string`   | Start of the replay window (inclusive). Accepts multiple formats (see below)                                                      |
| `end`    | `string`   | End of the replay window (inclusive). Must be after `start`                                                                       |
| `step`   | `int`      | Milliseconds between each epoch batch. `0` = no delay (all bars sent immediately). Non-zero values below 10ms are clamped to 10ms |

#### Accepted time formats

- `2024-01-01 09:30:00+00:00`
- `2024-01-01T09:30:00+00:00`
- `2024-01-01T09:30:00Z`
- `2024-01-01 09:30:00` (interpreted as UTC)
- `2024-01-01` (midnight UTC)

### Data (server → client)

```json
{
  "key":  "AAPL/1Min/OHLCV",
  "data": {
    "Epoch": 1704100200,
    "Open":  185.50,
    "High":  185.75,
    "Low":   185.25,
    "Close": 185.60,
    "Volume": 1234
  }
}
```

The payload format matches the live streaming plugin (`contrib/stream`), so
clients can use the same deserialization logic for both live and replay data.

### Multi-TBK ordering

When multiple TBKs are subscribed, bars are interleaved by epoch:

1. At each timestamp, bars for **all** subscribed TBKs at that timestamp are
   sent before advancing.
2. The `step` delay is applied **between** timestamps, not between individual
   TBK payloads within the same timestamp.
3. TBKs within the same timestamp are sent in subscription order.

### End (server → client)

```json
{"action": "end"}
```

Sent after all bars have been delivered. The server then closes the WebSocket
connection with a normal close frame.

### Error (server → client)

```json
{"error": "no data found for AAPL/1Min/OHLCV in range [...]"}
```

Sent when validation fails or no data exists for the requested range. The
connection stays open so the client can retry with a corrected subscribe message.

## Differences from live streaming (`/ws`)

| Feature                | Live (`/ws`)                             | Replay (`/ws/replay`)                                                           |
|------------------------|------------------------------------------|---------------------------------------------------------------------------------|
| Endpoint               | `/ws`                                    | `/ws/replay`                                                                    |
| Data source            | Real-time writes via WAL trigger         | Historical data read from disk                                                  |
| Subscribe format       | `{"action": "subscribe", "tbks": [...]}` | `{"action": "subscribe", "tbks": [...], "start": ..., "end": ..., "step": ...}` |
| Wildcards              | Supported (`*/*/*`)                      | Not supported (fully qualified TBKs only)                                       |
| Connection lifecycle   | Long-lived                               | Closes after replay ends                                                        |
| Multiple subscriptions | Replace-in-place                         | One replay per connection                                                       |
