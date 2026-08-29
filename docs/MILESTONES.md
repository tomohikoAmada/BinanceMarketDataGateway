# Gateway milestones

This is the authoritative development plan for the Gateway. The G3
implementation baseline is Gateway `main` at
`d99ab4512c251316cc71d4ba1a5ec390e4224d48`; the related upstream authority
checked for this checkpoint is Contracts
`518880bdfa60948c3b65b6b3525d024526995166` and Projection
`01a66aa80c764d2600da2cc309c0fd69655b55c`.

The current implementation is G0, G1, GW-PREQ-002, G2, G3, G4, G5, and G6
complete.
The deterministic synthetic host, serialized `MarketRuntime`, first real Binance
Spot BTCUSDT network/bootstrap runtime, bounded reconnect/resync recovery, and
planned connection rotation are implemented.
Historical G2/G3 attempts, including the deleted `feat/g2-deterministic-synthetic-host`
branch and its recovery bundle, are not implementation authority. Historical PR #5 is
retained only as a closed, not-merged, abandoned implementation attempt.

## Responsibility split

Contracts owns:

- Protobuf messages;
- service contracts;
- public wire compatibility;
- the C++ message package;
- the separate C++ gRPC service/stub package.

Projection owns:

- fixed-point numeric semantics;
- deterministic `OrderBook`;
- Spot sequence policy;
- USD-M sequence policy;
- stale/duplicate/gap classification;
- bootstrap bridge classification;
- `BookProjection` lifecycle;
- reset/resync semantics;
- `ProtoAdapter`;
- `LocalOrderBookSnapshot` construction.

Gateway owns:

- Binance WebSocket and REST transport;
- transport JSON/wire acquisition;
- authoritative symbol metadata acquisition;
- receive timestamps;
- connection lifecycle;
- bootstrap buffering;
- reconnect/resync orchestration;
- planned connection rotation;
- serialized Projection scheduling;
- bounded queues;
- subscription admission;
- slow-consumer isolation;
- Gateway gRPC runtime;
- `gateway_instance_id`, `connection_id`, `connection_generation`,
  `subscription_id`, `session_sequence`, and status.

Gateway must not create a second order book, Spot classifier, USD-M classifier, gap
classifier, Projection lifecycle, or `GatewayProjectionHost` abstraction. It must
also not introduce a generic event bus, DI framework, plugin framework, or generic
runtime framework.

## Current implementation status

- `G0=COMPLETE`.
- `G1=COMPLETE`.
- `G2=COMPLETE`.
- `G3=COMPLETE`.
- `G4=COMPLETE`.
- `G5=COMPLETE`.
- `G6=COMPLETE`.
- `G2_SYNTHETIC_HOST_IMPLEMENTED=YES`.
- `G3_SERIALIZED_MARKET_RUNTIME_IMPLEMENTED=YES`.
- `CURRENT_GATEWAY_RUNTIME_IMPLEMENTED=YES`.
- `REAL_GATEWAY_NETWORK_RUNTIME_IMPLEMENTED=YES`.
- `GATEWAY_NETWORK=SPOT_BTCUSDT_IMPLEMENTED`.
- `RECONNECT=IMPLEMENTED`.
- `AUTOMATIC_RECOVERY=IMPLEMENTED`.
- `PLANNED_ROTATION=IMPLEMENTED`.
- `NEXT=G7`.

Gateway `main` currently has typed configuration, synchronous Foundation
lifecycle, a daemon CLI that immediately starts and stops Foundation, Foundation
tests, build/CI/sanitizers, the explicit G1 dependency proof, and the deterministic
in-memory G2 synthetic Spot BTCUSDT host. G3 adds one fixed Spot BTCUSDT
`MarketRuntime` with one private `BookProjection`, one owner thread, bounded
ingress and bootstrap buffers, injected clock/input/faults, copied owner-domain
observation and snapshot capture, and deterministic joined shutdown. G4 adds
verified TLS Binance exchangeInfo/depth REST, raw Spot diff-depth WebSocket,
strict transport JSON decoding, real receive timestamps, connection generation
1 identity, authoritative NumericSpec derivation, and bounded real bootstrap
through that runtime. Reconnect/recovery is implemented in G5 and planned
rotation is implemented in G6; gRPC, publication, and subscriptions are not
implemented.

## G0 — Repository Foundation

**STATUS=COMPLETE**

Purpose: establish the minimal C++20 Gateway repository and synchronous lifecycle
seam.

Includes C++20, CMake, typed configuration, synchronous Foundation, CLI,
deterministic offline tests, GCC/Clang, sanitizers, and format CI. It adds no
runtime behavior.

## G1 — Exact Candidate Dependency Integration

**STATUS=COMPLETE**

Purpose: provide historical explicit proof that one process can consume
`BinanceMarketDataContracts::Protobuf`, `BinanceMarketDataContracts::Grpc`,
`BinanceMarketDataProjection::Core`, and
`BinanceMarketDataProjection::ProtoAdapter` with one coherent Contracts message
lineage.

G1 is a frozen historical development proof. Do not continuously repin it merely
because current upstream `main` changes. It remains explicit and opt-in; it is
not the normal G2–G6 runtime dependency lane.

## GW-PREQ-002 — Runtime Dependency Lane Decoupling

**STATUS=COMPLETE**

`GW-PREQ-002=COMPLETE`.

This is a prerequisite, not a new G-numbered product milestone. It establishes
the normal current Gateway runtime development dependency lane.

For G2–G6, the normal Gateway graph consumes the Contracts message/Protobuf
artifact, Projection Core, and Projection ProtoAdapter. It must not require
`BinanceMarketDataContracts::Grpc`, and G2–G6 development must not cold-build the
complete gRPC dependency graph merely to prove runtime development.

The historical G1 four-target proof remains explicit and opt-in. Contracts gRPC
enters the normal runtime/server dependency graph when G7 implements gRPC
publication. No runtime behavior is added in this prerequisite.

The normal root recipe is `conanfile.py`. The frozen historical proof recipe is
`conanfile_g1.py`, and `scripts/g1-candidate-proof.sh` selects it explicitly.
The normal graph is verified by
`scripts/gw-preq-002-verify-graph.py`; its invariant is one Contracts message
lineage plus Projection, with no Contracts gRPC package and no `grpc` package.

`NEXT=G7`.

## G2 — Deterministic Synthetic Host

**STATUS=COMPLETE**

`FIRST_RUNNABLE=G2`.

Scope is Binance Spot BTCUSDT only, with no real network, gRPC, owner thread, or
runtime ingress queue. Use a synthetic/in-memory REST snapshot, synthetic
pre-snapshot WebSocket diff events, deterministic receive order, and Projection
ProtoAdapter. Install the baseline, replay buffered updates, let Projection own
stale/duplicate/bridge/gap classification, reach `Synchronized`, and create a
`LocalOrderBookSnapshot`.

G2 is implemented as a single-threaded, deterministic in-memory host with a concrete
development executable and focused acceptance tests. It adds no real Binance network,
gRPC, concurrency, bounded runtime queue, or recovery behavior.

Use explicit deterministic `NumericSpec`, timestamps, request IDs, connection
IDs, and ambient inputs. Do not depend on the system clock or random IDs for
deterministic acceptance.

Required coverage includes normal bootstrap, stale prefix, duplicate prefix,
valid bridge, no bridge, sequence gap, post-LIVE update, deterministic final
snapshot, and reset/rebootstrap. Do not add a second sequence classifier.

## G3 — Serialized MarketRuntime and Bounded Runtime

**STATUS=COMPLETE**

For one `(venue, market, symbol)`, own one `MarketRuntime`, one `BookProjection`,
and one serialized mutation owner. Add bounded ingress, bounded bootstrap buffer,
injectable clock, injectable transport/snapshot events, fault injection, and
deterministic shutdown.

Only the owner may install the baseline, apply an update, reset Projection, or
read/capture publication snapshot state. Network callbacks never mutate
Projection.

Complete-frame upstream receive order must be preserved through the owner
boundary. Parallel parse/adaptation completion must not reorder source events. For
V1, prefer simple serialization over parallel parsing plus a reorder buffer.
Prefer standard synchronization and simple bounded structures. Do not require
lock-free queues, custom allocators, busy-spin, CPU affinity, or generic
executors before measurement.

G3 is implemented as an opt-in internal target for exactly Binance Spot
BTCUSDT. Producer admission is nonblocking and preserves complete-frame FIFO
order. Ingress and the distinct owner-local bootstrap buffer are independently
bounded with configurable nonzero capacities and defaults of 64. Overflow,
adapter errors, transport failure, snapshot failure, and Projection gap state
fail closed without recovery. Synchronous observation and snapshot requests are
serialized behind earlier admitted work and return owning copies; the owned
Projection has no escape hatch. Graceful stop drains finite admitted work in
FIFO order unless a terminal fault forbids further mutation, signals shutdown
outside the bounded data queue, and joins the owner.

G3's ordinary standalone behavior remains fail-closed. G5 adds one blocking
owner-domain rebootstrap control and stop-token-aware state waits; Projection
reset still executes only on the original owner thread after the caller has
quiesced the previous source generation. Lifetime ticket counters remain
monotonic.

## G4 — Real Spot Transport and Bootstrap

**STATUS=COMPLETE**

`FIRST_REAL_NETWORK=G4`.

G4 implements Binance Spot BTCUSDT diff-depth WebSocket and REST depth snapshot,
receive timestamps, `connection_id`, and `connection_generation` starting at 1
for the first real applicable source. It uses bounded pre-snapshot handoff, real
bootstrap into Projection, and the synchronized LIVE state.

### NumericSpec authority

Before constructing `BookProjection`, G4:

- acquire authoritative Binance Spot symbol metadata;
- validate configured BTCUSDT market membership;
- establish the explicit price/quantity `NumericSpec` from authoritative symbol
  metadata under Projection's accepted fixed-point semantics;
- validate transport-name mapping at the Host boundary.

The official Binance Spot documentation was refreshed at implementation on
2026-08-28. The frozen G4 mapping is `PRICE_FILTER.tickSize` to price storage
scale and `LOT_SIZE.stepSize` to quantity storage scale after strict positive
plain-decimal quantum normalization. Observed values, precision fields, and
`MARKET_LOT_SIZE` do not determine Projection NumericSpec.

The implementation uses Boost.Asio/Beast, OpenSSL peer and hostname
verification with SNI, and nlohmann_json. One networking I/O thread maintains
the WebSocket read while the REST snapshot is in flight; complete frames are
timestamped before serialized parsing and nonblocking admission. Beast processes
server ping frames and returns the required payload pong. `serverShutdown`,
close, TLS/socket errors, malformed payload, REST failure, and bounded handoff
failure are terminal. G4 has no retry, reconnect, rotation, or gRPC.

The opt-in `bmd-gateway-g4-spot-live` acceptance reached synchronized LIVE,
applied a later real update, captured an owner-domain local-book snapshot, and
stopped cleanly in the required local run.

## G5 — Reconnect / Resync / Recovery

**STATUS=COMPLETE**

G5 implements one concrete Spot BTCUSDT lifecycle coordinator around one
persistent `MarketRuntime`, private `BookProjection`, and owner thread. At most
one `SpotTransport` is active. Every recoverable incident stops and joins the old
transport, establishes the runtime owner barrier, waits interruptibly, resets
through the owner domain, increments the nonzero connection generation, and
re-enters the G4 WebSocket-buffer/REST-snapshot/Projection bootstrap path.

Recoverable causes include disconnect/read and connection failures, idle
timeouts, `serverShutdown`, REST snapshot failures and timeouts, transient 5xx,
malformed transport payloads, ingress/bootstrap overflow, and Projection
`NeedsResync`. Adapter, Projection-rejection, clock, internal invariant, HTTP
403, and other HTTP 4xx failures are terminal. Gateway reacts to Projection
status and runtime faults and has no second sequence classifier.

Each incident permits six recovery attempts with deterministic delays of 1, 2,
4, 8, 16, and 30 seconds and no jitter. A successful return to Live resets the
incident counter. HTTP 429 and 418 require a strictly parsed nonnegative whole
second `Retry-After`; the wait is the maximum of ordinary backoff and that value,
and malformed or absent rate-limit guidance fails closed. Stop interrupts all
backoff, including long IP-ban waits. The official Binance Spot authority was
refreshed on 2026-08-29.

The opt-in real G5 acceptance proved generation 1 Live, a controlled normal
recovery cut, distinct generation 2 transport identity, fresh verified TLS
WebSocket and REST bootstrap, Synchronized/Live Projection, a later real update,
and owner-domain snapshot capture.

`NEXT=G7`.

## G6 — Planned Connection Rotation

**STATUS=COMPLETE**

G6 integrates an optional planned-rotation policy into the single G5 lifecycle
coordinator. The production project policy is 23 hours 50 minutes, a ten-minute
safety margin before Binance's documented 24-hour Spot WebSocket lifetime; it is
not a Binance-prescribed value. Each transport attempt captures its generation
birth from the injected monotonic clock before `start()`, receives a fresh
deadline, and waits without fixed-interval polling for runtime recovery, the
deadline, or stop.

A clean deadline cut stops and joins generation N, establishes the owner FIFO
barrier, requires stopped/no-terminal-error transport plus
Live/Synchronized/no-fault runtime, and invokes the distinct healthy planned
reset command on the existing `MarketRuntime` owner thread. Generation N+1 then
immediately re-enters the G4 WebSocket-buffer/REST-snapshot/Projection bootstrap
path. Lifetime ticket counters remain monotonic, planned rotations consume no G5
backoff or recovery budget, and at most one transport is active. Genuine
transport failure or `NeedsResync` at the cut wins and uses ordinary bounded G5
recovery.

G6 remains break-before-make with no cross-generation source stitching, second
sequence classifier, gRPC, publication, or subscriptions. The accepted Boost
1.91 network-domain shutdown barrier is unchanged and must be re-proved before
any Boost/backend upgrade.

The explicit real G6 acceptance used a five-second acceptance-only rotation age:
generation 1 reached Live and applied a later update, the planned clean cut
completed, and distinct generation 2 freshly bootstrapped, reached Live, applied
a later update, and produced an owner-domain snapshot with zero planned-rotation
recovery attempts.

`NEXT=G7`.

## G7 — Bounded Publication + SubscribeOrderBook + gRPC

**STATUS=NOT_STARTED**

`FIRST_GRPC=G7`.

Implement the Contracts-owned gRPC service for `SubscribeOrderBook`.

### Publication cut

The serialized Projection owner establishes the publication cut:

```text
Projection has accepted through update C
  -> capture LocalOrderBookSnapshot(last_update_id=C)
  -> establish subscription publication cut
  -> admit mandatory initial output
  -> only subsequently applicable accepted updates may follow
```

Do not modify Projection to add publication callbacks or mutex APIs.

Normal order-book fanout follows Projection `ApplyDisposition::Applied` only.
`IgnoredStale` and `IgnoredDuplicate` are not republished as accepted book
mutations. `GapDetected` enters recovery/gap handling.

### Fully bounded subscriptions

The runtime must have a finite active-subscription limit. A bounded queue per
subscriber is insufficient if subscription count is unbounded. Requests above
the configured or defined active-subscription limit are rejected before
`SubscriptionAccepted`.

Each subscriber has a bounded ordinary FIFO plus one guaranteed bounded
terminal-control admission path, or a semantically equivalent bounded design.
Subscription acceptance is serialized with the publication owner and occurs only
if the mandatory initial `SubscriptionAccepted` and `LocalOrderBookSnapshot`
outputs can be admitted under the bounded design.

### Slow-consumer overflow invariant

When ordinary enqueue first fails:

- the failed ordinary item consumes no `session_sequence`;
- atomically mark the subscription terminal;
- stop later normal data admission;
- guarantee exactly one `ConsumerGapNotice` through the reserved terminal path;
- assign that emitted notice the next `session_sequence`;
- emit prior admitted items in order, then the notice, then close the stream;
- have the consumer recover by resubscribing according to Contracts semantics.

gRPC writes must not execute on or block the Projection owner. A slow consumer
must not stall Binance ingress, Projection mutation, or other subscribers.

### Identity and sequencing

For every actually emitted item, `session_sequence` is exactly `1, 2, 3, ...`.
It is distinct from Binance `U/u/pu`, Projection `last_update_id`, and
`connection_generation`. Control items that are actually emitted consume sequence
values.

Populate `connection_generation` only when one unique applicable upstream source
generation exists, preserving Contracts optional-presence semantics.

## G8 — Projection M6 Integration Acceptance

**STATUS=NOT_STARTED**

`PROJECTION_M6_START_GATE=G8`.

End-to-end acceptance is Binance Spot BTCUSDT → real Gateway → ProtoAdapter →
Projection → Gateway publication → `SubscribeOrderBook` → real consumer.
Acceptance covers bootstrap, the publication snapshot/update cut, contiguous
post-snapshot updates, reconnect, resync, single writer, and bounded
slow-consumer isolation.

Projection M6 implementation and acceptance begin here, not during G0–G7.

## G9 — SubscribeEvents

**STATUS=NOT_STARTED**

Add contiguous event publication for `DepthUpdate`, `AggTrade`, and `BookTicker`,
with no silent loss.

Initial V1 acceptance supports exactly one selector per accepted
`EventSubscriptionRequest`. Requests with more than one selector are rejected
until a deliberate deterministic cross-selector merge-ordering contract exists.
Do not create a generic event bus merely because the schema permits repeated
selectors.

## G10 — Minimal GetGatewayStatus

**STATUS=NOT_STARTED**

Implement only the existing useful operational status surface:
`gateway_instance_id`, uptime, market runtime state, last event time,
`connection_generation` when uniquely applicable, and active subscription count.
Do not add a large metrics or telemetry framework here.

## G11 — USD-M and Multi-Market Runtime

**STATUS=NOT_STARTED**

`FIRST_MULTI_MARKET=G11`.

Add USD-M BTCUSDT, correct USD-M transport routing, isolated Spot and USD-M
`MarketRuntime` instances, USD-M inputs passed through Projection's existing
USD-M sequence policy, and runtime failure isolation. Gateway must not implement
its own `pu` classifier.

After the Spot/USD-M two-product model is proven, multi-symbol expansion may
reuse the `MarketRuntime` registry/design.

## Deferred product surface

The critical path explicitly defers `SubscribeMarketState`, mark price, index
price, funding rate, open interest, Recorder integration, persistence, a generic
multi-exchange framework, Kafka, plugin architecture, Kubernetes, and speculative
lock-free optimization.

## Performance policy

Gateway is latency-sensitive, but no arbitrary latency target is set before
hardware and workload measurement. Once real transport exists, preserve
measurement points equivalent to:

```text
T0 = complete WS frame received
T1 = parse/adaptation complete
T2 = owner dequeues item
T3 = Projection apply complete
T4 = subscriber admission/enqueue complete
T5 = gRPC write/delivery completion observation
```

Measure at minimum T0→T3, T3→T4, T0→T4, and T4→T5 separately, with p50, p95,
p99, max, throughput, and queue occupancy. T0→T4 is primarily internal
Gateway latency. T4→T5 is subscriber/network/gRPC-flow-control sensitive and
must not be represented as Projection hot-path latency.

Performance evidence records build identity, hardware identity, workload, depth,
subscriber count, and warm-up/sample methodology. Do not rewrite Projection
merely because its current implementation uses `std::map` or internal
transactional copying. Optimize only after measurement identifies a concrete
bottleneck.

## Milestone labels

```text
FIRST_RUNNABLE=G2
FIRST_REAL_NETWORK=G4
FIRST_GRPC=G7
PROJECTION_M6_START_GATE=G8
FIRST_MULTI_MARKET=G11
```
