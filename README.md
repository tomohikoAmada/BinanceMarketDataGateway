# Binance Market Data Gateway

This repository contains the C++20 Binance Market Data Gateway. G0, G1,
GW-PREQ-002, and G2 through G11 are complete, and post-G11 runtime
productization is complete.

The ordinary `bmd-gatewayd` is the long-running production host for exactly
these two products:

- `BINANCE / SPOT / BTCUSDT`;
- `BINANCE / USD_M_PERPETUAL / BTCUSDT`.

Projection owns fixed-point numeric semantics, deterministic order-book state,
sequence/gap classification, and reset/resync semantics. Gateway owns Binance
transport and metadata acquisition, recovery and rotation orchestration,
bounded publication, subscriber sessions, and gRPC. The ordinary production
daemon does not yet expose arbitrary multi-symbol serving.

The accepted current state is `POST_G11_PERFORMANCE_BASELINE=COMPLETE`.
Recovery observability and the bounded recovery-observation campaign are
complete. The accepted baseline is descriptive evidence, not a hard SLA,
capacity guarantee, or Production Qualification. It describes the fixed
two-product G11 daemon and is not G12 multi-product capacity evidence.
Production qualification and optimization are not authorized. The G12 campaign
is in progress: G12-A exact single-product parameterization and G12-B
configured runtime serving are complete, and G12-C is next. Current production
remains the fixed G11 two-product daemon; G12-A's ETH support is reusable
offline path capability, not current production composition.

G12-B provides a finite stable configured owner set and immutable dynamic
registry for reusable/internal serving: exact `MarketKey` membership drives
dynamic routing, status, observations, diagnostics, and shutdown aggregation.
The ordinary `bmd-gatewayd` still serves only the two BTC products above.
`--config PATH`, configured metadata acquisition, and arbitrary-N production
startup remain G12-C responsibilities.

G12 targets a startup-configured finite set of Binance Spot and USD-M
perpetual products, identified by exact `MarketKey` values, with one
independent transport per product and a maximum of eight configured products.
Its target configuration authority is `bmd-gatewayd --config PATH`; the current
G11 daemon and its `--grpc-listen` seam remain unchanged until G12-C production
configuration and startup composition are implemented. Contracts and
Projection production changes are not required.

## Build and test

```sh
cmake --preset gcc-debug
cmake --build --preset gcc-debug
ctest --preset gcc-debug
scripts/format-check.sh
```

The equivalent Clang preset is `clang-debug`. The full production graph is an
explicit CMake opt-in through `BMD_GATEWAY_BUILD_PRODUCTION_DAEMON=ON` and
requires the configured Contracts/Projection dependencies. The production
daemon requires `--grpc-listen HOST:PORT` and also supports `--help`:

```sh
bmd-gatewayd --grpc-listen HOST:PORT
```

It waits for both fixed products to reach initial Live/Synchronized before
serving, handles SIGINT/SIGTERM, rolls back startup failures, isolates a later
single-market failure, and shuts down server handlers before destroying the
product graph.

## Project authority

- [Current state](docs/CURRENT_STATE.md)
- [Milestones](docs/MILESTONES.md)
- [Architecture](ARCHITECTURE.md)
- [Performance-baseline instrumentation](docs/PERFORMANCE_BASELINE_INSTRUMENTATION.md)

Historical evidence is retained in [docs/HANDOFF_2026-08-23.md](docs/HANDOFF_2026-08-23.md).
