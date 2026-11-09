# BMDG — Binance Market Data Gateway

**BMDG** is a C++20 Binance market-data gateway. One process serves 1..20 exact
Spot / USD-M perpetual products, maintains a private Projection order book per
product, and publishes order-book, event and status data over loopback gRPC.
BMDG does not store history and does not depend on Recorder.

## Current status

- Production entry point: `bmdg --config PATH`.
- Historical G0-G11 / G12-A/B/C work is complete and archived.
- v0.4 G3-a (20-key offline routing/startup/recovery) and G3-b (shared-IP API
  budget) are complete, independently reviewed and merged.
- **G3-c is open**: shared VPS, real-network, Recorder co-load and resource
  budgets are not yet accepted. Twenty products are a resource bound, not a
  measured production capacity.
- Performance budgets are not established. The dominant measured cost is the
  pinned Projection full-book transaction copy, not BMDG queues.

See [current state](docs/CURRENT_STATE.md), [milestones](docs/MILESTONES.md),
[evidence](docs/EVIDENCE.md) and [architecture](ARCHITECTURE.md).

## Source layout

```text
include/binance_market_data/gateway/v1/   public Foundation API
src/foundation/                            config/lifecycle
src/protocol/                              Spot/USD-M JSON protocol
src/transport/                             REST/WebSocket transport, API budget
src/runtime/                               MarketRuntime, MarketKey, clock
src/recovery/                              reconnect/resync and rotation
src/publication/                           order-book/event subscriber channels
src/grpc/                                  gRPC service/server
src/status/                                GetGatewayStatus
src/composition/                           ProductRuntime, configured set, registry
src/production/                            daemon config/metadata/startup/signals
src/performance/                           opt-in trace instrumentation
app/bmdg/                                  production daemon
app/acceptance/                            historical live/acceptance tools
tests/                                     unit/integration/production tests
```

Historical `src/g1`, `src/g2`, ... directories no longer exist. See
[docs/CODE_MAP.md](docs/CODE_MAP.md) for the old-to-new mapping.

## Configuration

```json
{
  "grpc_listen": "127.0.0.1:50051",
  "spot_symbols": ["BTCUSDT", "ETHUSDT"],
  "usdm_symbols": ["BTCUSDT", "ETHUSDT"]
}
```

- 1..20 products total; the same symbol in Spot and USD-M is two products.
- Symbols are exact Binance uppercase identifiers; no case folding.
- Unknown config fields are rejected. Changes require restart; no hot reload.
- All configured products must reach Live/Synchronized before gRPC binds.
  Any startup failure or exception rolls the whole set back.
- Insecure gRPC binds numeric loopback only (`127/8` or `::1`).
- Each product has its own transport, owner thread, private Projection and
  recovery coordinator. There is one process-wide 48 streaming-context cap,
  not 48 per product.

## Build

Foundation only (no Conan, no network/threads):

```sh
cmake -S . -B build/foundation -DCMAKE_BUILD_TYPE=Debug
cmake --build build/foundation --parallel
ctest --test-dir build/foundation --output-on-failure
```

Production daemon plus domain tests (requires the pinned Conan graph):

```sh
cmake -S . -B build/production \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=/absolute/path/to/conan_toolchain.cmake \
  -DBMDG_BUILD_PRODUCTION_DAEMON=ON \
  -DBMDG_BUILD_TESTS=ON
cmake --build build/production --parallel 4
ctest --test-dir build/production --output-on-failure
```

Options:

| Option | Default | Purpose |
| --- | --- | --- |
| `BMDG_BUILD_TESTS` | `BUILD_TESTING` | Foundation tests; domain tests when the domain graph is enabled |
| `BMDG_BUILD_DOMAIN_TESTS` | `OFF` | Build domain tests without the daemon |
| `BMDG_BUILD_PRODUCTION_DAEMON` | `OFF` | Build `bmdg` and the production library |
| `BMDG_BUILD_ACCEPTANCE_TOOLS` | `OFF` | Build historical live apps, synthetic host and acceptance client |
| `BMDG_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION` | `OFF` | Opt-in trace/delivery measurement; adds about 110 MiB RSS for 20 products |
| `BMDG_BUILD_UPSTREAM_LINK_SMOKE` | `OFF` | Frozen Contracts/Projection consumer link smoke |
| `BMDG_ENABLE_ASAN` / `UBSAN` / `TSAN` | `OFF` | Sanitizers |

Dependencies are pinned by `conan.lock` and the `profiles/` files. Do not use
floating FetchContent. The historical G1 proof scripts are in
`scripts/archive/`; the current upstream smoke remains an explicit opt-in.

### Naming compatibility

- New CMake options use `BMDG_*`. The old `BMD_GATEWAY_*` names are accepted
  as deprecated aliases for one transition period.
- New CMake library targets use `bmdg_*`; old `bmd_gateway_*` library names
  are aliases.
- The executable is `bmdg`. A `bmd-gatewayd` symlink/custom target is kept for
  old scripts and service units during the transition.
- Internal namespace and include path remain
  `binance_market_data::gateway` to avoid breaking Contracts/Projection
  integration.

## Run

```sh
build/production/bmdg --config examples/gateway.json
# production example: /etc/bmdg/gateway.json
```

A systemd example is provided at `deployment/bmdg.service`.

SIGINT/SIGTERM stop admission, drain/cancel gRPC handlers, stop transports and
join owners. For systemd, allow a full 300 s quiet interval before restarting
after a crash or replacement because the shared-IP API budget is in-memory
while Binance's server-side counters persist.

## gRPC surface

- `SubscribeOrderBook`: Projection-Applied order book; accepted + initial
  snapshot, then contiguous depth updates.
- `SubscribeEvents`: exactly one V1 selector. Spot supports DIFF_DEPTH,
  AGG_TRADE, BOOK_TICKER; USD-M supports DIFF_DEPTH. Depth events are
  pre-Projection normalized events.
- `GetGatewayStatus`: dynamic product list, state, active subscriptions,
  connection generation and last Applied Depth receive time.
- `SubscribeMarketState`: not implemented.

Per-product active subscriptions are at most 8 order-book and 8 event; ordinary
queues are 64. Slow consumers are terminalized. The process-wide streaming
context cap is 48.

`GetGatewayStatus.markets[].last_event_utc_ns` is the receive UTC of the last
Projection-Applied Depth in the current valid generation; it is absent when the
book/source is not valid. It is not an age/SLA promise. Clients must treat a
wall-clock regression as unknown age.

## Validation

Current baseline after the domain-layout refactor and BMDG rename:

- Release production graph: **27/27** tests pass.
- Release with `BMDG_WARNINGS_AS_ERRORS=ON`: **27/27** tests pass.
- Debug ASan+UBSan production graph: **27/27** tests pass.
- Debug TSan production graph: **27/27** tests pass.
- Release with performance instrumentation: **28/28** tests pass.
- Foundation-only build: **1/1** test passes.

Sanitizer and real-network/VPS acceptance remain milestone/release activities;
see [milestones](docs/MILESTONES.md). Historical full evidence is in
[docs/archive/](docs/archive/) and summarized in [docs/EVIDENCE.md](docs/EVIDENCE.md).

## Documentation

- [Architecture](ARCHITECTURE.md)
- [Current state](docs/CURRENT_STATE.md)
- [Milestones](docs/MILESTONES.md)
- [Evidence summary](docs/EVIDENCE.md)
- [Code map](docs/CODE_MAP.md)
- [Binance constraints](docs/official-binance-constraints.md)
- [Performance instrumentation](docs/PERFORMANCE_BASELINE_INSTRUMENTATION.md)
- [Data parity with BMDR](docs/DATA_PARITY.md)
- [Archived historical docs](docs/archive/README.md)
