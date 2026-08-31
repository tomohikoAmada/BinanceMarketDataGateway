# Gateway contribution rules

Read in this order before making Gateway changes:

1. [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md)
2. [docs/MILESTONES.md](docs/MILESTONES.md)
3. [ARCHITECTURE.md](ARCHITECTURE.md)
4. milestone-specific evidence as needed

G0, G1, GW-PREQ-002, G2, G3, G4, G5, G6, G7, G8, G9, G10, and G11 are complete. The deterministic G2
synthetic host, serialized G3 `MarketRuntime`, real G4 Binance Spot BTCUSDT
transport/bootstrap, bounded G5 reconnect/resync recovery, and break-before-make
G6 planned connection rotation are implemented. G7 adds bounded owner-domain
order-book publication and the first synchronous `SubscribeOrderBook` gRPC flow.
G8 closes the Projection M6 real-Gateway order-book integration acceptance using
the existing G3-G7 production architecture and adds no second order book,
sequence classifier, recovery coordinator, or production runtime abstraction.
G9 adds focused bounded synchronous `SubscribeEvents` for Spot BTCUSDT
`DIFF_DEPTH`, `AGG_TRADE`, and `BOOK_TICKER`.
G10 adds minimal synchronous `GetGatewayStatus` for one Spot BTCUSDT market,
assembled from existing runtime, recovery, and publication observations.
G11 adds the fixed two-product USD-M and multi-market runtime boundary.
`NEXT=POST_G11_PLANNING`; no G12 or further numbered Gateway milestone is
currently frozen.
Keep Phase A small and independently buildable.

This repository contains the G0 foundation, frozen G1 proof, deterministic G2
synthetic host, serialized G3 runtime, real G4 Spot transport, G5 recovery, and
G6 rotation, G7 publication/gRPC, G8 integration acceptance, G9
`SubscribeEvents`, G10 `GetGatewayStatus`, and the G11 fixed two-product
USD-M/multi-market runtime; future work follows the milestone authority.
`NEXT=POST_G11_PLANNING`; no G12 or further numbered Gateway milestone is
currently frozen.
Keep Phase A small and independently buildable.

## Boundaries

- The normal G2–G6 and G7-disabled runtime lane depends on the Contracts-owned
  message-only/Protobuf artifact and Projection `ProtoAdapter`/`Core` surfaces.
- The separate `BinanceMarketDataContractsGrpc` artifact remains explicit and
  opt-in for the frozen G1 proof; G7 enables it conditionally in the normal
  runtime graph.
- Gateway consumes Projection through the existing `ProtoAdapter`/`Core` surfaces.
- Gateway has no Recorder dependency.
- The G3 baseline has one owner thread, bounded ingress/bootstrap queues, and an
  injected clock. G7 extends that same owner with bounded publication; it does
  not add Gateway-owned Projection business logic, an order-book implementation,
  a sequence classifier, storage, event bus, DI, plugins, or a generic runtime
  framework.
- G4 is exactly Binance Spot BTCUSDT, has one networking I/O thread and one
  connection generation, and drives Projection only through G3's bounded owner
  boundary. As an independently usable milestone it remains one-shot.
- G5 retains one MarketRuntime/Projection owner, allows at most one active Spot
  transport, quiesces the old network thread before owner-domain reset, and uses
  bounded interruptible rate-limit-aware recovery. It has no second sequence
  classifier, planned rotation, gRPC, publication, or subscriptions.
- G6 integrates the 23h50m monotonic planned-rotation policy into the G5
  coordinator. It remains break-before-make, uses the distinct owner-domain
  healthy reset only after source quiescence and a Live/Synchronized barrier,
  and has no source stitching, gRPC, publication, or subscriptions.
- G7 implements only synchronous `SubscribeOrderBook`. Publication and registry
  mutation stay on the G3 owner; each accepted RPC handler is its sole writer.
  Existing sessions terminate before G5 recovery or G6 planned reset and never
  cross a full Projection rebootstrap.
- G9 implements synchronous `SubscribeEvents` with exactly one V1 selector.
  `DIFF_DEPTH` publication is PRE_PROJECTION_NORMALIZED; G7 OrderBook remains
  Projection-Applied-only. G9 adds no second classifier or generic event bus,
  and Event sessions terminate at actual source-generation replacement rather
  than stitching across it.
- G10 implements a one-shot read-only `GetGatewayStatus` for one Spot BTCUSDT
  market. It has no health/metrics/telemetry framework; last-event freshness is
  the normalized WebSocket receive observation, generation is optional only
  while uniquely applicable, and the status count is G7 resident plus G9
  active Event subscriptions. Pending G7 admissions are excluded. The unary
  status RPC does not enter the 24-context streaming TryCancel tracker, and
  expensive status collection is limited to one concurrent RPC.
- G11 implements exactly two products: Binance Spot BTCUSDT and Binance USD-M
  perpetual BTCUSDT. Each has one `MarketRuntime`, private `BookProjection`,
  serialized owner, and independent `RecoveryCoordinator` instance. A fixed
  non-owning two-entry registry routes one shared synchronous gRPC service.
  Projection exclusively owns USD-M `pu` continuity through
  `DepthUpdate.previous_final_update_id`; Gateway adds no classifier. G7 routes
  both products, G9 exposes only USD-M `DIFF_DEPTH`, status has two rows, and
  the G11-enabled streaming bound is 48 (the G11-off legacy bound is 24).
  There is no generic multi-market, event, plugin, or runtime framework.
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
