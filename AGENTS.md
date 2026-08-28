# Gateway contribution rules

Read in this order before making Gateway changes:

1. [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md)
2. [docs/MILESTONES.md](docs/MILESTONES.md)
3. [ARCHITECTURE.md](ARCHITECTURE.md)
4. milestone-specific evidence as needed

G0 and G1 are complete; the current Gateway runtime is not implemented. Keep
Phase A small and independently buildable.

This repository contains the G0 foundation and frozen G1 proof; future runtime
work follows the milestone authority. Keep Phase A small and independently
buildable.

## Boundaries

- The normal G2–G6 runtime lane depends on the Contracts-owned message-only/
  Protobuf artifact and Projection `ProtoAdapter`/`Core` surfaces.
- The separate `BinanceMarketDataContractsGrpc` artifact remains explicit and
  opt-in for the frozen G1 proof and enters the normal runtime graph at G7.
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
- The G1 upstream link-smoke is explicit and opt-in; do not continuously repin
  its frozen candidate proof merely because upstream `main` changes.
- Never claim an upstream smoke or official protocol fact was verified when the dependency or
  source was unavailable.

## Validation

Use CMake 3.24 or newer, build the offline tests, run CTest, run sanitizer configurations, and
run `scripts/format-check.sh`. Keep all build/cache output under ignored directories.
