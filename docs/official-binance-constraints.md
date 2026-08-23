# Official Binance constraints acquisition record

This is a checked-in evidence record, not a runtime dependency. Exact text-only HTTPS response
bytes were retrieved on **2026-08-23T07:22:34.921794Z UTC** through an allowlisted updater that
validated credential-free HTTPS redirects, bounded the response size, wrote only to
`/private/tmp/gateway-binance-docs-20260823`, and marked `remote_content_executed=false` and
`llms_full_loaded=false`. The ephemeral local acquisition manifest is
`/private/tmp/gateway-binance-docs-20260823/manifest.json`; it is not a remote dependency or build
input. No downloaded body was executed or committed.

The portal's old, extensionless catalog URLs returned an HTTP 202 WAF challenge in an earlier
probe. The current `.md` URLs listed below are the working official Agent Native paths and were
validated against the retrieved `llms.txt` index. Hashes are of the exact response bytes; page
body is intentionally not copied into this repository.

## Retrieved sources

| Coverage | Official URL | Bytes | Response SHA-256 |
|---|---|---:|---|
| Agent Native selection index | `https://developers.binance.com/en/docs/llms.txt` | 167471 | `c5decbaaef780287cec46d394b34451e1bc56ffe6681ca74e2f38819fda4a12a` |
| Spot WebSocket streams | `https://developers.binance.com/en/docs/products/spot/web-socket-streams.md` | 10370 | `193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f` |
| Spot REST API general/rate-limit page | `https://developers.binance.com/en/docs/products/spot/rest-api.md` | 33310 | `3bfe5526b745c976ae2db7c6bffdee14f10663d5fe326d8aa54c8b5f12968775` |
| USD-M WebSocket connect/routing | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect.md` | 2613 | `912f2dad9da21b5c1801d73f052473b6a1d7136a43b2ff3e7a1c2cdc54abdde2` |
| USD-M local order book | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly.md` | 1114 | `d6a94d17fb32450c67ad598c0f923bf9df12ecdc43ced4928798a9fa56d62622` |
| CM/UM integration notice | `https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice.md` | 12109 | `7b2f3c49aec8d1ab2d9ca2d1295ae1c0c8a54a958ee01e8aa23ecd03568eaa98` |

The index itself lists the current Spot `GET /api/v3/depth` and USD-M `GET /fapi/v1/depth`
order-book endpoints. The source selection's expected terms were checked before the bytes were
written. The response `Content-Type` was `application/octet-stream` for the `.md` pages, but the
validated bodies were UTF-8 Markdown and were never interpreted as executable content.

## Constraints relevant to the future Gateway

These are concise conclusions derived from the retrieved pages; they are not implemented by G0.

### Spot WebSocket and local order-book bootstrap

- Raw streams use `wss://stream.binance.com:9443/ws/<streamName>`; combined streams use
  `/stream?streams=...`; stream symbols are lowercase.
- A connection is valid for 24 hours. The server sends a ping every 20 seconds; the client must
  return a pong copying the ping payload within one minute. A connection permits 5 incoming
  messages/second, at most 1024 streams, and 300 connection attempts per 5 minutes per IP.
- Keep the documented Spot bootstrap acquisition rule separate from apply-time continuity. The
  Host snapshot/buffer procedure obtains `GET /api/v3/depth`, calls its `lastUpdateId` value `L`,
  discards buffered events with `u <= L`, and requires the first retained event to contain `L`
  within `[U,u]`. This is the official snapshot reacquisition and buffering instruction.
- At apply time, after a local update id `C` has been established, the official gap condition is
  `U > local_update_id + 1`; a normal next update has `U = previous_event.u + 1`. Projection
  ADR-0008/Core classifies this with the overflow-safe relation `U <= C + 1 <= u`, where an
  exact-next event is continuous. Gateway must not add a second classifier or overwrite this
  accepted Projection semantic merely because the first retained range after the Host's `u <= L`
  filter does not contain `L`. The future Host owns snapshot reacquisition, buffering, and feeding
  order to Projection; it does not reinterpret apply-time continuity.
- The documented snapshot has a maximum of 5000 price levels on each side; levels outside the
  snapshot are not known until changed. Zero quantity removes a level.

### Spot REST

- The current index identifies `GET /api/v3/depth` as the Spot order-book endpoint.
- The retrieved REST general page states that requests are IP-rate-limited, each route has a
  request weight, used-weight headers are returned, HTTP 429 indicates a rate-limit violation,
  and repeated violations can lead to a ban. Public endpoints with security type `NONE` do not
  require credentials.

### USD-M WebSocket and routing

- The base is `wss://fstream.binance.com`; high-frequency public depth uses `/public`, regular
  market data uses `/market`, and private user data uses `/private`.
- An unrouted connection receives only Public streams; Market/Private streams require the routed
  path. Symbols are lowercase.
- A connection is valid for 24 hours. The server sends a ping every 3 minutes and disconnects if a
  pong is not received within 10 minutes. The limit is 10 incoming messages/second and 1024 streams
  per connection.

### CM/UM integration notice

- The current notice records `st == 1` on the relevant USD-M streams and `ps` pair identity on
  single-symbol depth/bookTicker streams. Future transport must retain and validate these fields;
  G0 does not parse or implement this rule.

### USD-M local order book and REST depth

- The documented bootstrap uses
  `wss://fstream.binance.com/public/stream?streams=btcusdt@depth` and
  `https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000`.
- Discard events with `u < lastUpdateId`; the first processed event must satisfy
  `U <= lastUpdateId <= u`. Subsequent events must have `pu == previous_event.u`, otherwise the
  process must restart from the snapshot step.
- Quantities are absolute. Zero removes a level, and deleting a level absent from the local book is
  documented as normal.

## G0 disposition

No Phase A code consumes these values. The typed configuration only records a future market/symbol
identity and an unbound endpoint; it does not open a WebSocket, call REST, allocate a queue, or
implement either product's sequence policy. Before Phase B transport work, refresh this record and
repeat the hash/term validation so protocol drift is reviewed explicitly.
