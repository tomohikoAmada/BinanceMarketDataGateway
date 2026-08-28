# Gateway architecture

The detailed, ordered development authority is
[docs/MILESTONES.md](docs/MILESTONES.md). This document records only the
responsibility split and the current foundation/G4 boundary.

## Dependency direction

```text
Binance public APIs
        |
        v
BinanceMarketDataGateway
        +--> Contracts message-only package
        +--> Projection::ProtoAdapter --> Projection::Core
        +--> Contracts separate gRPC package (G7 publication)
```

There is no Gateway-to-Recorder dependency. Contracts owns Protobuf messages,
service contracts, wire compatibility, and both C++ message and separate gRPC
service/stub packages. Projection owns fixed-point semantics, deterministic order
book state, Spot/USD-M sequence and gap policy, lifecycle, reset/resync,
ProtoAdapter, and snapshot construction. Gateway owns transport acquisition,
metadata, timestamps, connection and recovery orchestration, serialized
Projection scheduling, bounded queues and subscriptions, slow-consumer
isolation, and the Gateway gRPC runtime.

## Phase A components

`bmd_gateway_foundation` is a small library with two synchronous seams:

- `config.hpp` defines `Venue`, `Market`, `ListenEndpoint`, `GatewayConfig`, and typed validation
  errors. The only configuration fields are venue, market, symbol, a future gRPC listen endpoint,
  and a nonzero queue-capacity value. The listen endpoint performs only shallow, no-I/O validation
  of a non-empty safe ASCII host token and port; it does not claim full DNS/IP syntax validation.
  The symbol is an opaque, exact, non-empty strict UTF-8
  scalar identity without ASCII C0, DEL, or ASCII-whitespace bytes. Validation preserves the
  original bytes and does not normalize, case-fold, impose a length/alphabet rule, or assert
  exchange membership. Later transport code must map the configured identity to the official
  lowercase stream name and use exchangeInfo for existence/market membership.
- `lifecycle.hpp` defines `Foundation` and the explicit states `Constructed`, `Running`, and
  `Stopped`. `Foundation` owns only validated configuration and state; it creates no threads,
  sockets, queues, clocks, callbacks, or asynchronous work.

`bmd-gatewayd` is an offline lifecycle demonstration. It parses the five flags, validates them,
starts the foundation, reports `running`, stops it, reports `stopped`, and exits. The endpoint is
not bound and no transport is attempted. The separate `bmd-gateway-g2-synthetic`
executable drives one deterministic in-memory Spot BTCUSDT scenario through the
direct Projection APIs; it is not a production transport runtime.

The opt-in `bmd_gateway_g3_runtime` target is the first concurrent runtime
boundary. It implements exactly one Binance Spot BTCUSDT `MarketRuntime`, which
owns one private `BookProjection` and one dedicated serialized owner thread. Its
complete-frame ingress FIFO and distinct owner-local bootstrap buffer have
independent finite capacities. The owner performs all adaptation, Projection
mutation/state reads, and consumer snapshot capture. External callers receive
only copied observations or owning protobuf snapshots. An injected clock supplies
snapshot-generation timestamps. Shutdown admission closure and wakeup are
out-of-band from the bounded ingress; graceful stop drains accepted work in FIFO
order unless a terminal fault forbids mutation, then joins the owner.

G3 accepts only already-parsed synthetic Contracts messages and explicit
transport/snapshot fault events. It has no sockets, HTTP, WebSocket, JSON,
reconnect, retry, publication, subscription, or gRPC behavior.

The opt-in G4 targets add exactly one real Binance Spot BTCUSDT transport. Before
runtime construction, verified HTTPS exchangeInfo validates `TRADING` Spot
membership and derives Projection NumericSpec from `PRICE_FILTER.tickSize` and
`LOT_SIZE.stepSize`. A separate networking I/O thread owns verified TLS DNS/TCP,
the raw `btcusdt@depth@100ms` WebSocket, and asynchronous HTTPS depth snapshot.
It timestamps each complete frame before strict JSON parsing, preserves receive
order, and submits only complete Contracts messages through G3's existing
bounded ingress. Projection remains private to the G3 owner. G4 assigns one
stable connection ID and generation 1, handles server ping/pong and
`serverShutdown`, fails closed without retry/reconnect, and stops/join cleanly.

## MarketRuntime Projection boundary

The G3 `MarketRuntime` and G4 transport use, and future Gateway runtime work must
continue to use, the existing Projection APIs directly:
construct one `BookProjection` per `venue + market + symbol`, adapt Contracts
messages with `ProtoAdapter`, feed updates in source receive order, and follow
Projection's returned classification. It must not add a
`GatewayProjectionHost`, second sequence classifier, second order book, second
Projection lifecycle, generic event bus, DI framework, plugin framework, or
generic runtime framework.

The foundation and frozen G1 link proof remain runtime-free. The completed G2
synthetic host remains the deterministic direct-Projection proof. G3 establishes
serialized concurrency and bounded runtime ownership independently of transport.
G4 is the first real network/bootstrap implementation. Recovery, planned
rotation, publication, and gRPC remain G5, G6, and G7 work respectively.
