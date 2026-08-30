# Gateway architecture

The detailed, ordered development authority is
[docs/MILESTONES.md](docs/MILESTONES.md). This document records only the
responsibility split and the current foundation/G8 boundary.

## Dependency direction

```text
Binance public APIs
        |
        v
BinanceMarketDataGateway
        +--> Contracts message-only package
        +--> Projection::ProtoAdapter --> Projection::Core
        +--> Contracts separate gRPC package (conditionally for G7)
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

The opt-in G5 target adds one concrete recovery coordinator. It retains the same
G3 runtime, private Projection, and owner thread while replacing G4 transport
attempts one at a time. The old network thread joins before the runtime owner
barrier/reset and before a higher, never-reused connection generation is
created. Projection reset is an owner-domain control, runtime state transitions
are observed with condition-variable waits, and stop interrupts deterministic
bounded backoff. HTTP 429/418 honor strict `Retry-After`; terminal internal and
HTTP 4xx classifications fail closed. Recovery always returns through fresh
WebSocket buffering and REST depth bootstrap. G5 adds no continuity predicate or
second sequence classifier.

The opt-in G6 target adds the 23h50m project planned-rotation policy to that same
coordinator. A generation's injected monotonic birth timestamp is captured
before its transport start. While qualified Live, one timed condition-variable
wait ends on recovery, the planned deadline, or stop; there is no polling loop.
On a clean deadline cut, the old transport stops and joins before the owner FIFO
barrier and before a distinct owner-thread healthy reset. Only then may the next
generation start and conservatively re-enter G4 bootstrap. A clean rotation has
no recovery backoff or budget cost. A real transport fault or Projection
`NeedsResync` observed at the cut wins and follows G5 recovery. G6 remains
break-before-make, permits at most one active transport, and adds no sequence
classifier, gRPC, publication, or subscriptions. The reviewed Boost 1.91
reactor/scheduler shutdown barrier is unchanged and requires re-proof on upgrade.

The opt-in G7 targets add the first normal gRPC/publication flow, exactly
`SubscribeOrderBook` for Binance Spot BTCUSDT. Source snapshot and update inputs
carry immutable optional connection-generation provenance from G4 into G3. The
existing `MarketRuntime` Projection owner exclusively owns the bounded pending
admission mailbox, target-ticket subscription cut, subscriber registry, and
fanout after Projection returns `Applied`; stale and duplicate inputs are not
published. Each accepted channel has 64 fixed ordinary slots plus one separate
terminal descriptor slot, with at most eight resident accepted channels and
eight pending admissions.

One synchronous RPC handler is the sole writer for each accepted stream. It
peeks without removing the front record, performs `ServerWriter::Write` off the
Projection owner, and acknowledges the same record only after success. Ordinary
overflow reserves one `SLOW_CONSUMER`/`RESUBSCRIBE` terminal notice without
blocking the owner. Projection gap, other recovery, and planned rotation
terminalize existing sessions before reset; sessions never cross a full
rebootstrap. Service shutdown closes admission, executes a reserved owner
publication-shutdown control, cancels a bounded snapshot of active contexts,
then shuts down and waits for the synchronous server before G5/G6/runtime stop.

G8 composes the existing G3-G7 production components at the accepted
cross-repository boundary. Its deterministic and real acceptance paths prove
consumer-visible behavior across Projection `NeedsResync`, G5 recovery and
rebootstrap, G6 planned rotation and rebootstrap, fresh resubscription,
generation provenance, per-session sequencing, and final shutdown. G8 adds no
production runtime layer, second order book, sequence classifier, recovery
coordinator, or other runtime abstraction.

## MarketRuntime Projection boundary

The G3 `MarketRuntime`, G4 transport, and G5/G6 lifecycle coordinator use, and
future Gateway runtime work must continue to use, the existing Projection APIs
directly:
construct one `BookProjection` per `venue + market + symbol`, adapt Contracts
messages with `ProtoAdapter`, feed updates in source receive order, and follow
Projection's returned classification. It must not add a
`GatewayProjectionHost`, second sequence classifier, second order book, second
Projection lifecycle, generic event bus, DI framework, plugin framework, or
generic runtime framework.

The foundation and frozen G1 link proof remain runtime-free. The completed G2
synthetic host remains the deterministic direct-Projection proof. G3 establishes
serialized concurrency and bounded runtime ownership independently of transport.
G4 is the first real network/bootstrap implementation. G5 adds bounded automatic
recovery and G6 adds planned break-before-make rotation without changing
Projection continuity ownership. G7 adds bounded publication and the first
synchronous gRPC business flow without adding a second classifier or order book.
G8 closes the Projection M6 real-Gateway order-book integration acceptance.
Later work is G9 `SubscribeEvents`, G10 minimal `GetGatewayStatus`, and G11
USD-M / multi-market runtime.
