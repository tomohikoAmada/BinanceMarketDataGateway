# BMDG — official Binance constraints

Reference record, not a runtime dependency. Retrieved 2026-08-23 from the
official developer documentation; hashes and exact URLs were recorded in the
historical acquisition manifest. Refresh this file before changing protocol
behavior; do not treat old values as permanent.

## Spot WebSocket

- Raw: `wss://stream.binance.com:9443/ws/<streamName>`; combined:
  `/stream?streams=...`. Stream symbols are lowercase.
- Connection lifetime: 24 hours. Server ping every 20 seconds; pong must copy
  the payload within one minute. Client connection attempts: 300 per 5 minutes
  per IP. At most 1024 streams per connection.
- Local order book bootstrap: buffer events from the first `U`, fetch
  `GET /api/v3/depth` (`lastUpdateId = L`), reacquire if `L < first U`, discard
  buffered `u <= L`; first retained event should contain `L` in `[U,u]`.
- Apply-time continuity: `U > local_update_id + 1` is a gap; a normal next event
  has `U = previous_event.u + 1`. Projection owns this classification; BMDG
  must not add a second one.
- Snapshot maximum: 5000 levels per side. Levels outside the snapshot are
  unknown until changed; zero quantity removes a level.

## Spot REST

- `GET /api/v3/depth` weight by limit: 1-100 = 5, 101-500 = 25,
  501-1000 = 50, 1001-5000 = 250.
- `GET /api/v3/exchangeInfo` weight is 20.
- Requests are IP-rate-limited; each route has a weight; used-weight headers
  are returned. HTTP 429 means rate-limit violation; repeated violations can
  become 418. `Retry-After` gives seconds. IP bans scale from minutes to days.

## USD-M perpetual WebSocket

- Base: `wss://fstream.binance.com`; high-frequency public depth uses
  `/public`, regular market data `/market`, private data `/private`.
  USD-M public combined route: `/public/stream?streams=<symbol>@depth@100ms/<symbol>@bookTicker`.
  USD-M market combined route: `/market/stream?streams=<symbol>@aggTrade/<symbol>@markPrice@1s/<symbol>@forceOrder`.
  Single-symbol depth remains available at `/public/ws/<symbol>@depth@100ms` for the
  depth-only diagnostic profile.
- Connection lifetime: 24 hours. Server ping every 3 minutes; disconnect if no
  pong within 10 minutes. At most 1024 streams per connection.
- `st == 1` and `ps` pair identity are present on relevant single-symbol
  streams; validate them when present.
- Bootstrap: `GET /fapi/v1/depth?symbol=BTCUSDT&limit=1000`. Discard
  `u < lastUpdateId`; first processed event must satisfy
  `U <= lastUpdateId <= u`; subsequent events must satisfy
  `pu == previous_event.u`, otherwise restart from the snapshot.
- Quantities are absolute; zero removes a level. Deleting an absent level is
  normal.

## Operational implications

- Spot ping every 20 s means a 90 s idle timeout is safe for Spot. USD-M ping
  every 3 min means the same 90 s timeout can reconnect quiet USD-M symbols;
  use a per-market timeout or client keepalive.
- A full 300 s quiet interval is required after restarting a process because
  BMDG's shared-IP budget is in-memory while Binance's counters persist.
- Shared-IP deployment must account for every consumer; BMDG's fixed shares
  are 3000 Spot / 600 USD-M weight per minute and 60 WS attempts per 5 minutes
  per market.
