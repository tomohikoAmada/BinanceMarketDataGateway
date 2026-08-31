# Gateway milestones

This is the authoritative development plan for the Gateway. The current
implementation authority is Gateway `main` at
`04505b69529e33db8a94b48cd7a216678721cf33`; the related upstream authorities
for the current productization closure are Contracts
`d194b663827185feb773515aa63467290780c670` and Projection
`8621499cbeba0e42c409572ee3f209c32691698b`.

The current implementation is G0, G1, GW-PREQ-002, G2, G3, G4, G5, G6, G7, G8,
G9, G10, and G11 complete.
The deterministic synthetic host, serialized `MarketRuntime`, first real Binance
Spot BTCUSDT network/bootstrap runtime, bounded reconnect/resync recovery, and
planned connection rotation are implemented. G7 adds bounded order-book
publication and the first synchronous `SubscribeOrderBook` gRPC flow. G8 adds
the Projection M6 integration acceptance composition. G9 adds bounded synchronous
`SubscribeEvents` for Spot BTCUSDT `DIFF_DEPTH`, `AGG_TRADE`, and `BOOK_TICKER`.
G10 adds minimal synchronous `GetGatewayStatus` for one Binance Spot BTCUSDT
market. G11 adds the fixed two-product USD-M and multi-market runtime for Spot
BTCUSDT and USD-M perpetual BTCUSDT.
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
- `G7=COMPLETE`.
- `G8=COMPLETE`.
- `G9=COMPLETE`.
- `G10=COMPLETE`.
- `G11=COMPLETE`.
- `G2_SYNTHETIC_HOST_IMPLEMENTED=YES`.
- `G3_SERIALIZED_MARKET_RUNTIME_IMPLEMENTED=YES`.
- `CURRENT_GATEWAY_RUNTIME_IMPLEMENTED=YES`.
- `REAL_GATEWAY_NETWORK_RUNTIME_IMPLEMENTED=YES`.
- `GATEWAY_NETWORK=SPOT_BTCUSDT_AND_USD_M_PERPETUAL_BTCUSDT_IMPLEMENTED`.
- `USD_M_BTCUSDT=IMPLEMENTED`.
- `MULTI_MARKET_RUNTIME=IMPLEMENTED`.
- `G11_PRODUCT_COUNT=2`.
- `RECONNECT=IMPLEMENTED`.
- `AUTOMATIC_RECOVERY=IMPLEMENTED`.
- `PLANNED_ROTATION=IMPLEMENTED`.
- `SUBSCRIBE_ORDER_BOOK=IMPLEMENTED`.
- `SUBSCRIBE_EVENTS=IMPLEMENTED`.
- `GET_GATEWAY_STATUS=IMPLEMENTED`.
- `BOUNDED_PUBLICATION=IMPLEMENTED`.
- `GRPC=IMPLEMENTED`.
- `STATUS_MARKET_COUNT=2`.
- `MAX_CONCURRENT_STATUS_RPCS=1`.
- `STATUS_USES_STREAM_CONTEXT_TRACKER=NO`.
- `STREAM_CONTEXT_LIMIT=48`.
- `STREAM_CONTEXT_LIMIT_G11_OFF=24`.
- `G7_ACTIVE_LIMIT_PER_MARKET=8`.
- `G7_PENDING_LIMIT_PER_MARKET=8`.
- `G9_ACTIVE_LIMIT_PER_MARKET=8`.
- `MAX_G7_ACTIVE_TOTAL=16`.
- `MAX_G7_PENDING_TOTAL=16`.
- `MAX_G9_ACTIVE_TOTAL=16`.
- `MAX_ACTIVE_TRANSPORTS_PER_MARKET=1`.
- `MAX_ACTIVE_TRANSPORTS_TOTAL=2`.
- `FIRST_MULTI_MARKET=G11`.
- `POST_G11_RUNTIME_PRODUCTIZATION=COMPLETE`.
- `PRODUCTION_DAEMON=bmd-gatewayd`.
- `PRODUCTION_PRODUCT_COUNT=2`.
- `PRODUCTION_GRPC_CONFIGURABLE=YES`.
- `PRODUCTION_REQUIRES_BOTH_INITIAL_LIVE=YES`.
- `PRODUCTION_SERVES_BEFORE_BOTH_INITIAL_LIVE=NO`.
- `SIGINT_SUPPORTED=YES`.
- `SIGTERM_SUPPORTED=YES`.
- `STARTUP_ROLLBACK=IMPLEMENTED`.
- `POST_START_SINGLE_MARKET_FAILURE_ISOLATION=IMPLEMENTED`.
- `INSTALLABLE_PRODUCTION_DAEMON=YES`.
- `NEXT=POST_G11_PERFORMANCE_BASELINE`.

Gateway `main` currently has typed configuration, synchronous Foundation
lifecycle, the historical/minimal Foundation CLI seam, Foundation tests,
build/CI/sanitizers, the explicit G1 dependency proof, and the deterministic
in-memory G2 synthetic Spot BTCUSDT host. G3 adds one fixed Spot BTCUSDT
`MarketRuntime` with one private `BookProjection`, one owner thread, bounded
ingress and bootstrap buffers, injected clock/input/faults, copied owner-domain
observation and snapshot capture, and deterministic joined shutdown. G4 adds
verified TLS Binance exchangeInfo/depth REST, raw Spot diff-depth WebSocket,
strict transport JSON decoding, real receive timestamps, connection generation
1 identity, authoritative NumericSpec derivation, and bounded real bootstrap
through that runtime. Reconnect/recovery is implemented in G5 and planned
rotation is implemented in G6. G7 implements bounded order-book publication and
`SubscribeOrderBook`; G8 integration acceptance is implemented as a focused
acceptance/test composition and opt-in CMake wiring. G9 implements synchronous
`SubscribeEvents`. G10 implements synchronous `GetGatewayStatus` for one Spot
BTCUSDT market using existing runtime, recovery, and publication observations.
G11 implements exactly two isolated products: Binance Spot BTCUSDT and Binance
USD-M perpetual BTCUSDT, with one `MarketRuntime`/`BookProjection`/serialized
owner and one independent `RecoveryCoordinator` per product. It adds USD-M
REST/WS transport, routes G7 by exact market, exposes only USD-M `DIFF_DEPTH`
through G9, and returns two deterministic status rows. Projection remains the
sole USD-M `pu` continuity authority.

The ordinary `bmd-gatewayd` now acquires authoritative metadata, constructs the
two accepted product runtimes, waits for both initial Live/Synchronized, starts
the configured synchronous gRPC listener only after that readiness cut, and
serves until SIGINT/SIGTERM. Startup failure rolls back the partial graph;
after startup, one market may fail without globally stopping the other market
or the gRPC server. Shutdown drains/cancels server handlers before product
owner destruction. This productization preserves the accepted G11 semantics
and adds no new market-data semantics.

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

For G2–G6 and G7-disabled builds, the normal Gateway graph consumes the Contracts
message/Protobuf artifact, Projection Core, and Projection ProtoAdapter. It must not require
`BinanceMarketDataContracts::Grpc`, and G2–G6 development must not cold-build the
complete gRPC dependency graph merely to prove runtime development.

The historical G1 four-target proof remains explicit and opt-in. G7 conditionally
adds the current Contracts gRPC artifact and `grpc` to the normal runtime/server
dependency graph while preserving one Contracts message lineage. No runtime
behavior is added in this prerequisite.

The normal root recipe is `conanfile.py`. The frozen historical proof recipe is
`conanfile_g1.py`, and `scripts/g1-candidate-proof.sh` selects it explicitly.
The normal graph is verified by
`scripts/gw-preq-002-verify-graph.py`; its invariant is one Contracts message
lineage plus Projection, with no Contracts gRPC package and no `grpc` package.

`NEXT=G8`.

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

`NEXT=G8`.

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

`NEXT=G8`.

## G7 — Bounded Publication + SubscribeOrderBook + gRPC

**STATUS=COMPLETE**

`FIRST_GRPC=G7`.

G7 implements the Contracts-owned synchronous C++ gRPC service for
`SubscribeOrderBook`. The first normal G7 dependency graph conditionally adds
the current Contracts gRPC artifact and `grpc`; the G7-disabled G3–G6 graph
remains message/Projection-only. Both graphs retain exactly one current
Contracts message lineage.

### Publication cut

The serialized Projection owner establishes the implemented publication cut:

```text
Projection has accepted through update C
  -> capture LocalOrderBookSnapshot(last_update_id=C)
  -> establish subscription publication cut
  -> admit mandatory initial output
  -> only subsequently applicable accepted updates may follow
```

Projection is unchanged and remains the only order-book and sequence
classification authority.

Normal order-book fanout follows Projection `ApplyDisposition::Applied` only.
`IgnoredStale` and `IgnoredDuplicate` are not republished as accepted book
mutations. `GapDetected` enters recovery/gap handling.

### Fully bounded subscriptions

Production V1 permits eight resident accepted channels, 64 ordinary records per
channel, one separate terminal descriptor per channel, and eight pending owner
admissions. Closed channels are owner-swept; `TerminalGap` channels remain
resident until writer closure. The mandatory `SubscriptionAccepted` sequence 1
and `LocalOrderBookSnapshot` sequence 2 are staged privately and commit together
at the owner target-ticket cut. A process-local owner counter assigns `ob-1`,
`ob-2`, and so on only on successful commit.

### Slow-consumer overflow invariant

When ordinary enqueue first fails:

- the failed ordinary item consumes no `session_sequence`;
- atomically mark the subscription terminal;
- stop later normal data admission;
- reserve exactly one server-side `ConsumerGapNotice` through the terminal path;
- assign that emitted notice the next `session_sequence`;
- emit prior admitted items in order, then the notice, then close the stream;
- have the consumer recover by resubscribing according to Contracts semantics.

The reserved terminal record yields one terminal Write attempt if the writer
reaches it; remote application receipt is not guaranteed. gRPC writes execute
only on the subscriber's single synchronous RPC handler and never on the
Projection owner. Peek/ack retains an in-flight record in its ring slot until a
successful Write, so a slow consumer cannot expand memory or stall Binance
ingress, Projection mutation, or other subscribers.

### Identity and sequencing

For every actually emitted item, `session_sequence` is exactly `1, 2, 3, ...`.
It is distinct from Binance `U/u/pu`, Projection `last_update_id`, and
`connection_generation`. Control items that are actually emitted consume sequence
values.

G4 carries immutable optional source generation with each snapshot/update into
G3. Writers never query mutable G5/G6 lifecycle state. Accepted metadata has no
generation; synchronized snapshots and Applied updates use their frozen unique
generation; slow-consumer gaps omit it. Projection gap, other continuity-losing
faults, and planned rotation terminalize current sessions before full reset with
the required gap reason/action. Existing sessions never cross G5 recovery or G6
planned rebootstrap.

Service shutdown closes its gate, synchronously completes the reserved owner
publication-shutdown control, snapshots and cancels at most 16 tracked contexts,
then shuts down and waits for the synchronous server before acquisition/runtime
shutdown. The real G7 acceptance proved generation 1 Live, exact sequences 1/2/3
for Accepted/Snapshot/first post-snapshot real update, frozen generation 1 on the
snapshot and update, client disconnect cleanup, subscriber removal, and joined
gRPC/Gateway shutdown.

`NEXT=G8`.

## G8 — Projection M6 Integration Acceptance

**STATUS=COMPLETE**

`PROJECTION_M6_START_GATE=G8`.

G7 already proves the first real Binance Spot BTCUSDT gRPC happy path. G8 owns
the broader Projection M6 integration acceptance through reconnect, resync, and
planned rotation as observed by a real consumer, plus the complete integration
acceptance boundary; it does not move those behaviors into G7 or add a
production G8 runtime layer.

The accepted implementation is PR #14 at reviewed head
`d272b59c74d813483137619e9640f9726700b32c`, merged by
`4c46b8667a183dd9b30e04fa8e97363cc8ca2254`. Exact-head automatic PR CI run
`33303448578` completed successfully. Independent technical review found
`P0=0`, `P1=0`, and `G8_TECHNICAL_ACCEPTANCE=PASS`.

G8 acceptance covers:

1. deterministic real-gRPC Projection-owned `NeedsResync`;
2. an `UPSTREAM_SEQUENCE_GAP` / `REQUEST_NEW_SNAPSHOT` terminal boundary;
3. owner-domain reset/rebootstrap and a new subscription;
4. real Binance G5 controlled recovery observed by a continuously draining
   consumer, with `RESUME_NOT_AVAILABLE` / `REQUEST_NEW_SNAPSHOT`;
5. real Binance G6 planned rotation observed by a continuously draining
   consumer, with `CONNECTION_GENERATION_CHANGED` / `RESUBSCRIBE`;
6. no old subscription crossing a full rebootstrap;
7. exact contiguous per-session `session_sequence`;
8. at most one active transport; and
9. clean transport, gRPC, subscriber, and runtime shutdown.

G8 added acceptance/test composition and opt-in CMake wiring. It did not modify
G3-G7 production source and did not implement G9, G10, or G11.

`NEXT=G10`.

## G9 — SubscribeEvents

**STATUS=COMPLETE**

Add contiguous event publication for `DepthUpdate`, `AggTrade`, and `BookTicker`,
with no silent loss.

Initial V1 acceptance supports exactly one selector per accepted
`EventSubscriptionRequest`. Requests with more than one selector are rejected
until a deliberate deterministic cross-selector merge-ordering contract exists.
Do not create a generic event bus merely because the schema permits repeated
selectors.

The accepted implementation is semantic commit
`aa821c5f76e0014c64a2f447dfe4bda07d9765e3` with format-only child
`ba13d5a823d8cc1c7e122bf4465cbf1146e10d9e`, merged by PR #16 as
`90a1e4019f57369a420d263fd298c5ad51ce8bdd`. Exact-head PR CI run
`33312746318` completed successfully. Independent review found `P0=0`,
`P1=0`, `G9_TECHNICAL_ACCEPTANCE=PASS`, and
`FALSE_PASS_PATH_FOUND=NO`.

G9's accepted boundary is one V1 selector for `DIFF_DEPTH`, `AGG_TRADE`, or
`BOOK_TICKER` on Binance Spot BTCUSDT, carried by one combined Spot WebSocket.
Depth events are `PRE_PROJECTION_NORMALIZED`; G7 OrderBook remains
Projection-Applied-only, with no second sequence classifier. Event publication
is bounded, sessions are generation-bounded, and recovery/rotation terminalize
with `CONNECTION_GENERATION_CHANGED` / `RESUBSCRIBE`; permanent failures do not
fabricate generation changes. G7 and G9 share the synchronous generated service
and server with a mechanical tracked-context bound of 24. G9 changes neither
Contracts nor Projection.

## G10 — Minimal GetGatewayStatus

**STATUS=COMPLETE**

Implement only the existing useful operational status surface:
`gateway_instance_id`, uptime, market runtime state, last event time,
`connection_generation` when uniquely applicable, and active subscription count.
Do not add a large metrics or telemetry framework here.

Accepted implementation record: commit
`14ebb9e335e0ff4e733671a3e0e120dcdefd1b52`, merged by PR #18 as
`c57e689380e3b4e24a41863eb4f80a133daa2cb8`. Exact-head automatic PR CI run
`33319504110` completed successfully. Independent technical review found
`P0=0`, `P1=0`, `P2=1` nonblocking, and
`G10_TECHNICAL_ACCEPTANCE=PASS`.

G10's accepted boundary is the existing Contracts `GetGatewayStatus` wire
surface: one-shot synchronous read-only status for BINANCE/SPOT/BTCUSDT, with
the G3 runtime state mapped to `StreamLifecycleState`, the last successfully
normalized WebSocket market-event receive time carried across recovery and
rotation, and `connection_generation` present only with one active source.
Active subscriptions combine G7 resident channels and G9 Event subscriptions;
pending G7 admissions are excluded. At most one expensive status collection is
allowed, with overload returning `RESOURCE_EXHAUSTED`. The unary status call
does not enter the streaming TryCancel tracker, whose bound remains 24. G10
adds no health/metrics/telemetry subsystem, Contracts code change, Projection
code change, or real Binance acceptance requirement.

## G11 — USD-M and Multi-Market Runtime

**STATUS=COMPLETE**

G11 is deliberately limited to exactly two products:

1. `BINANCE / SPOT / BTCUSDT`;
2. `BINANCE / USD_M_PERPETUAL / BTCUSDT`.

It is not arbitrary multi-symbol support or generic dynamic market registration.
The accepted architecture has two isolated single-product `MarketRuntime`
instances, two private `BookProjection` instances, two serialized Projection
owner threads, two independent mutable `RecoveryCoordinator` instances, one
fixed non-owning two-entry registry, and one shared synchronous Gateway gRPC
service. At most one transport is active per market, and terminal failure of
one market does not stop the other product or the gRPC server.

Gateway parses and forwards USD-M `pu` through
`DepthUpdate.previous_final_update_id`. Projection's
`SequencePolicyKind::UsdMPerpetual`, through `ProtoAdapter`, remains the sole
authority for bootstrap bridge, stale, duplicate, missing previous-final,
previous-final mismatch, gap, and `NeedsResync`. Gateway adds no second
classifier. USD-M uses `fapi.binance.com` REST and
`wss://fstream.binance.com/public/ws/btcusdt@depth@100ms`; the historical Spot
route authority is unchanged.

G7 routes one exact product per RPC, with eight active and eight pending limits
per market (16 each in total). G7 controlled recovery remains
`RESUME_NOT_AVAILABLE` / `REQUEST_NEW_SNAPSHOT`. Spot G9 supports
`DIFF_DEPTH`, `AGG_TRADE`, and `BOOK_TICKER`; USD-M G9 supports only
`DIFF_DEPTH`. Generation replacement remains
`CONNECTION_GENERATION_CHANGED` / `RESUBSCRIBE`. Status returns two rows in
stable order, Spot then USD-M, and permits one concurrent collection. The G11
streaming bound is 48 contexts; the historical G10/G11-off composition remains
24.

The accepted implementation is semantic reviewed head
`c5889db34099f515cd519db49da1dfe3d36d79f7`, with formatting-only child
`c25f9ee782d7c428278d4e7e4fc7fdec60ccdd0e`, implemented by PR #20 and merged
as `b2c1c140bb378eaf7edef6ffa554427f5c3f5aab`. Exact PR CI run
`33351013834` completed successfully. Independent review found `P0=0`,
`P1=0`, and `G11_TECHNICAL_ACCEPTANCE=PASS`. Accepted local evidence includes
current Contracts main `d194b663827185feb773515aa63467290780c670` and current
Projection main `8621499cbeba0e42c409572ee3f209c32691698b` as the upstream
authorities for this implementation.
G11-enabled regression and ASAN/UBSAN/TSAN passes, plus real simultaneous Spot
and USD-M live/order-book acceptance, USD-M `DIFF_DEPTH`, two-market status,
controlled USD-M recovery isolation, two-transport maximum, and clean joined
shutdown. The first real-run false negative was an acceptance-harness
expectation error for G7 controlled recovery, not a production G11 failure;
production semantics did not change between runs.

`NEXT=POST_G11_PERFORMANCE_BASELINE`. No additional numbered Gateway milestone
is currently frozen. The existing deferred product surface remains deferred.

## POST_G11_RUNTIME_PRODUCTIZATION

**STATUS=COMPLETE**

Purpose: turn the accepted G11 fixed two-product graph into the ordinary
deployable long-running `bmd-gatewayd`.

Exact implementation evidence:

- production semantic head:
  `61fbc8f021e8c7dabde6ef02bfb1b26d90658765`;
- CI-only child:
  `fd69d943a7330abf119eae80154204fdaa24a257`;
- acceptance correction:
  `248faa8277291de01a323c35fed827145eb760f7`;
- implementation PR: #22;
- implementation merge:
  `04505b69529e33db8a94b48cd7a216678721cf33`;
- exact PR CI: `33378772518` SUCCESS.

Final independent review found P0=0, P1=0, and known P2=3.
`TARGETED_P1_CLOSED=YES` and `TECHNICAL_ACCEPTANCE=PASS`.

Accepted semantics are narrowly:

- exactly two products: Spot BTCUSDT and USD-M perpetual BTCUSDT;
- operational gRPC listen configuration only;
- both markets initial-Live before serving;
- SIGINT/SIGTERM process lifecycle;
- complete startup rollback;
- serving-time single-market failure isolation;
- unchanged G11 context and transport bounds; and
- an installable daemon target with no new market-data semantics.

The final real production-daemon acceptance launches and owns the exact
`bmd-gatewayd` child, authenticates its `gateway_instance_id`, proves the child
alive before signaling, sends SIGTERM, waits for normal exit, and requires final
`contexts=0`, `transports=0`, `subscriptions=0`, `owners_joined=yes`. Spot Live,
USD-M Live, both order books, USD-M `DIFF_DEPTH`, two-market status,
status/child-instance matching, SIGTERM lifecycle, and clean final shutdown
all passed.

Acceptance history: the initial reported real production acceptance was not
valid final lifecycle proof because an external-only client could structurally
false-pass. This was an acceptance-harness evidence defect, not a production
source correctness defect. Correction `248faa8277291de01a323c35fed827145eb760f7`
made acceptance own, authenticate, signal, and reap the exact daemon process;
exactly one replacement real acceptance passed.

Known nonblocking findings remain:

- P2-1: the deterministic productionization test matrix is not exhaustive;
  some explicit start-result, exception, and combined-backpressure paths are
  not individually tested, with no corresponding production source defect
  found;
- P2-2: GitHub default sanitizer jobs do not enable the production-daemon
  graph, and local productization-enabled sanitizer evidence was not
  independently authenticated; and
- P2-3: SIGINT/SIGTERM cannot immediately cancel synchronous metadata HTTPS
  acquisition, although network stage deadlines make the delay bounded.

## POST_G11_PERFORMANCE_BASELINE

**STATUS=NOT_STARTED**

Purpose: measure before optimizing the actual merged production daemon.

Scope is high-level only: measure T0 complete WebSocket frame received, T1
parsing/adaptation, T2 owner dequeue, T3 Projection Apply, T4 subscriber
enqueue, and T5 gRPC delivery completion observation; report p50/p95/p99/max,
queue occupancy, CPU, RSS, and Spot versus USD-M contention.

No optimization is authorized merely by adding this future section. After the
baseline, `POST_G11_PRODUCTION_QUALIFICATION` remains future/planned and not
started. No numbered G12 is currently frozen.

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
