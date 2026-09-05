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
bounded publication, subscriber sessions, and gRPC. There is no arbitrary
multi-symbol runtime.

The accepted current state is `POST_G11_PERFORMANCE_BASELINE=COMPLETE`.
Recovery observability and the bounded recovery-observation campaign are
complete. The accepted baseline is descriptive evidence, not a hard SLA,
capacity guarantee, or Production Qualification. Production qualification and
optimization are not authorized, and no further numbered Gateway milestone is
frozen.

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
