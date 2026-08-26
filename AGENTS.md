# Gateway contribution rules

This repository implements Issue #1, G0 only. Keep Phase A small and independently buildable.

## Boundaries

- Gateway depends on the Contracts-owned message-only artifact and the separate
  `BinanceMarketDataContractsGrpc` artifact.
- Gateway consumes Projection through the existing `ProtoAdapter`/`Core` surfaces.
- Gateway has no Recorder dependency.
- This phase has no network clients, gRPC business flow, Projection business logic, order-book
  implementation, sequence classifier, storage, threads, clocks, event bus, DI, plugins, or
  generic runtime framework.
- Do not copy Contracts `.proto` files or introduce floating FetchContent dependencies.

## Phase A implementation rules

- Public configuration is the typed, finite surface in `include/binance_market_data/gateway/v1`.
- Configuration validation performs no I/O. The lifecycle seam is synchronous and owns no runtime
  resources.
- The upstream link-smoke is opt-in and must remain off until exact upstream package publication
  pins are available from Contracts #14 and Projection #45.
- Never claim an upstream smoke or official protocol fact was verified when the dependency or
  source was unavailable.

## Validation

Use CMake 3.24 or newer, build the offline tests, run CTest, run sanitizer configurations, and
run `scripts/format-check.sh`. Keep all build/cache output under ignored directories.
