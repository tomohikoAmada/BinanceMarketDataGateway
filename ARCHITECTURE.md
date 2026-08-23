# G0 architecture

## Dependency direction

```text
Binance public APIs (future Phase B)
        |
        v
BinanceMarketDataGateway
        +--> Contracts message-only package
        +--> Contracts separate gRPC package
        +--> Projection::ProtoAdapter --> Projection::Core
```

There is no Gateway-to-Recorder dependency. Contracts owns schemas and service bindings. Projection
owns numeric semantics, sequence policy, deterministic state, and single-writer mutation. Gateway
will own transport orchestration, bounded queues, timestamps, subscriptions, and gRPC runtime only
in later phases.

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
not bound and no transport is attempted.

## Future Host boundary

The later Gateway Host must reuse the existing Projection M3/M4 APIs directly: construct one
`BookProjection` per `venue + market + symbol`, adapt Contracts messages with `ProtoAdapter`, feed
updates in source receive order, and follow Projection's returned classification. It must not add a
`GatewayProjectionHost`, facade, event bus, second sequence classifier, or second order book.

The Phase A link smoke only proves package discovery and one final link across message, service,
adapter, and Core surfaces. It implements none of that future runtime behavior.
