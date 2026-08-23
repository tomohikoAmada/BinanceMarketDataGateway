# Binance Market Data Gateway

This repository is the C++20 Gateway foundation for Issue #1 / G0. Phase A proves a small typed
configuration surface, a synchronous lifecycle seam, an offline unit-test target, and a minimal
Linux CI/sanitizer foundation. It deliberately does not connect to Binance or implement a market-
data pipeline.

## Build and test

```sh
cmake --preset gcc-debug
cmake --build --preset gcc-debug
ctest --preset gcc-debug
scripts/format-check.sh
```

The equivalent Clang preset is `clang-debug`. The daemon lifecycle smoke is offline:

```sh
build/gcc-debug/bmd-gatewayd \
  --venue binance \
  --market spot \
  --symbol BTCUSDT \
  --grpc-listen 127.0.0.1:50051 \
  --queue-capacity 1024
```

It validates the configuration, transitions `constructed -> running -> stopped`, and exits. It
does not bind the endpoint, start a thread, open a WebSocket, or start a gRPC server.

## Upstream link smoke

`BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE` is `OFF` by default. When explicitly enabled, the build
requires an exact staged package prefix and discovers, without copying schemas:

- `BinanceMarketDataContracts::Protobuf`;
- `BinanceMarketDataContracts::Grpc` from `BinanceMarketDataContractsGrpc`; and
- `BinanceMarketDataProjection::Core` and `::ProtoAdapter`.

The single smoke executable links all four upstream targets and includes their public surfaces.
Contracts package publication/revision and the final Projection candidate gate remain pending;
therefore no RREV, SHA, or floating source is recorded here.

## Scope and ownership

The dependency and ownership decisions are recorded in [ARCHITECTURE.md](ARCHITECTURE.md),
[docs/CURRENT_STATE.md](docs/CURRENT_STATE.md), and [docs/adr/ADR-0001-g0-foundation-boundaries.md](docs/adr/ADR-0001-g0-foundation-boundaries.md).
Official Binance source acquisition status is recorded in
[docs/official-binance-constraints.md](docs/official-binance-constraints.md).
