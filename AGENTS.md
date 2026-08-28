# Gateway contribution rules

Read in this order before making Gateway changes:

1. [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md)
2. [docs/MILESTONES.md](docs/MILESTONES.md)
3. [ARCHITECTURE.md](ARCHITECTURE.md)
4. milestone-specific evidence as needed

G0, G1, GW-PREQ-002, G2, G3, and G4 are complete. The deterministic G2
synthetic host, serialized G3 `MarketRuntime`, and real G4 Binance Spot BTCUSDT
transport/bootstrap are implemented. Keep Phase A small and independently
buildable.

This repository contains the G0 foundation, frozen G1 proof, deterministic G2
synthetic host, serialized G3 runtime, and real G4 Spot transport; future runtime
work follows the milestone authority. Keep Phase A small and independently
buildable.

## Boundaries

- The normal G2–G6 runtime lane depends on the Contracts-owned message-only/
  Protobuf artifact and Projection `ProtoAdapter`/`Core` surfaces.
- The separate `BinanceMarketDataContractsGrpc` artifact remains explicit and
  opt-in for the frozen G1 proof and enters the normal runtime graph at G7.
- Gateway consumes Projection through the existing `ProtoAdapter`/`Core` surfaces.
- Gateway has no Recorder dependency.
- G3 intentionally has one owner thread, bounded ingress/bootstrap queues, and
  an injected clock. It still has no network clients, gRPC business flow,
  Gateway-owned Projection business logic, order-book implementation, sequence
  classifier, storage, event bus, DI, plugins, or generic runtime framework.
- G4 is exactly Binance Spot BTCUSDT, has one networking I/O thread and one
  connection generation, and drives Projection only through G3's bounded owner
  boundary. It has no reconnect, automatic recovery, planned rotation, gRPC,
  publication, or subscriptions.
- Do not copy Contracts `.proto` files or introduce floating FetchContent dependencies.

## Phase A implementation rules

- Public configuration is the typed, finite surface in `include/binance_market_data/gateway/v1`.
- Configuration validation performs no I/O. The lifecycle seam is synchronous and owns no runtime
  resources.
- The G1 upstream link-smoke is explicit and opt-in; do not continuously repin
  its frozen candidate proof merely because upstream `main` changes.
- The G3 runtime target is explicit and opt-in. Its owned `BookProjection` must
  remain accessible only to its serialized owner after start; callers submit
  complete events and receive owning copied observations/snapshots.
- Never claim an upstream smoke or official protocol fact was verified when the dependency or
  source was unavailable.

## Validation

Use CMake 3.24 or newer, build the offline tests, run CTest, run sanitizer configurations, and
run `scripts/format-check.sh`. Keep all build/cache output under ignored directories.
