# Gateway architecture

The detailed, ordered development authority is
[docs/MILESTONES.md](docs/MILESTONES.md). This document records only the
responsibility split and the current foundation/G2 boundary.

## Dependency direction

```text
Binance public APIs (future Gateway runtime)
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

## Future Host boundary

The future Gateway runtime must reuse the existing Projection APIs directly:
construct one `BookProjection` per `venue + market + symbol`, adapt Contracts
messages with `ProtoAdapter`, feed updates in source receive order, and follow
Projection's returned classification. It must not add a
`GatewayProjectionHost`, second sequence classifier, second order book, second
Projection lifecycle, generic event bus, DI framework, plugin framework, or
generic runtime framework.

The current foundation and frozen G1 link proof remain runtime-free. The completed G2
synthetic host implements only the deterministic in-memory scenario described in the
milestone authority; real network, concurrency, recovery, publication, and gRPC
behavior remain future milestones.
