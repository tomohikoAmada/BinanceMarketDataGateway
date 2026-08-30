# Current state

This file is the concise current-orientation record. The authoritative
development plan is [docs/MILESTONES.md](MILESTONES.md).

```text
G0=COMPLETE
G1=COMPLETE
G2=COMPLETE
G3=COMPLETE
G4=COMPLETE
G5=COMPLETE
G6=COMPLETE
G7=COMPLETE
G8=COMPLETE
G9=COMPLETE
G2_SYNTHETIC_HOST_IMPLEMENTED=YES
G3_SERIALIZED_MARKET_RUNTIME_IMPLEMENTED=YES
CURRENT_GATEWAY_RUNTIME_IMPLEMENTED=YES
REAL_GATEWAY_NETWORK_RUNTIME_IMPLEMENTED=YES
GATEWAY_NETWORK=SPOT_BTCUSDT_IMPLEMENTED
RECONNECT=IMPLEMENTED
AUTOMATIC_RECOVERY=IMPLEMENTED
PLANNED_ROTATION=IMPLEMENTED
SUBSCRIBE_ORDER_BOOK=IMPLEMENTED
SUBSCRIBE_EVENTS=IMPLEMENTED
BOUNDED_PUBLICATION=IMPLEMENTED
EVENT_PUBLICATION=IMPLEMENTED
GRPC=IMPLEMENTED
MAX_ACTIVE_SUBSCRIPTIONS=8
MAX_ACTIVE_EVENT_SUBSCRIPTIONS=8
ORDINARY_QUEUE_CAPACITY=64
TERMINAL_CONTROL_CAPACITY=1
EVENT_ORDINARY_QUEUE_CAPACITY=64
EVENT_TERMINAL_CONTROL_CAPACITY=1
PENDING_ADMISSION_CAPACITY=8
IDLE_CLIENT_CANCELLATION_CHECK_INTERVAL=250ms
MAKE_BEFORE_BREAK=NO
GATEWAY_GRPC_BUSINESS_FLOW=SUBSCRIBE_ORDER_BOOK_AND_SUBSCRIBE_EVENTS_IMPLEMENTED
RECORDER_DEPENDENCY=NO
GW-PREQ-002=COMPLETE
PROJECTION_M6_GATEWAY_INTEGRATION_ACCEPTANCE=COMPLETE
NEXT=G10
FIRST_RUNNABLE=G2
FIRST_REAL_NETWORK=G4
FIRST_GRPC=G7
PROJECTION_M6_START_GATE=G8
FIRST_MULTI_MARKET=G11
```

## What is implemented

Gateway `main` currently contains:

- typed, finite configuration;
- synchronous Foundation lifecycle;
- a daemon CLI that immediately starts and stops Foundation;
- deterministic Foundation tests;
- build/CI/sanitizer support;
- the explicit, opt-in G1 dependency proof;
- the deterministic in-memory G2 synthetic Spot BTCUSDT host with focused tests
  and a dedicated executable;
- the G3 Spot BTCUSDT `MarketRuntime` with one private `BookProjection`, one
  serialized owner thread, bounded ingress and bootstrap buffers, an injected
  clock, synthetic input/fault admission, copied owner-domain observations, and
  deterministic joined shutdown; and
- the opt-in G4 real Binance Spot BTCUSDT runtime with verified exchangeInfo
  metadata acquisition, `PRICE_FILTER.tickSize`/`LOT_SIZE.stepSize` NumericSpec
  derivation, verified TLS REST and raw diff-depth WebSocket transport, real
  receive timestamps, connection generation 1 identity, bounded bootstrap
  through `MarketRuntime`/Projection, server ping handling, and deterministic
  clean stop; and
- the opt-in G5 Spot BTCUSDT recovery coordinator, which preserves one
  `MarketRuntime`/private Projection owner across break-before-make connection
  generations, resets Projection only through the owner domain after the old
  transport joins, detects `NeedsResync` without polling, and applies bounded,
  interruptible, rate-limit-aware recovery; and
- the opt-in G6 planned-rotation policy integrated into that same coordinator,
  which uses monotonic generation age, rotates at the project-defined 23h50m
  mark, quiesces and joins the old source before an owner-domain healthy reset,
  then conservatively re-enters the existing bootstrap path with a new
  generation; and
- the opt-in G7 bounded order-book publication runtime and synchronous
  `SubscribeOrderBook` service. The existing `MarketRuntime` owner exclusively
  owns subscriber admission, snapshot cuts, registry mutation, Applied-update
  fanout, and recovery/rotation terminalization. Each accepted synchronous RPC
  handler exclusively owns its stream writer; and
- the G8 Projection M6 integration acceptance composition. It proves
  deterministic real-gRPC Projection `NeedsResync`, real consumer-visible G5
  controlled recovery, and real consumer-visible G6 planned rotation using the
  existing G3-G7 architecture. Old subscriptions terminate before full
  rebootstrap, fresh subscriptions restart `session_sequence`, and the
  acceptance adds no production runtime redesign.
- the G9 synchronous `SubscribeEvents` flow. V1 accepts exactly one selector
  for Spot BTCUSDT `DIFF_DEPTH`, `AGG_TRADE`, or `BOOK_TICKER` over one combined
  Binance Spot WebSocket; the legacy depth-only G4-G8 profile remains available.
  `DIFF_DEPTH` is pre-Projection normalized, while G7 `SubscribeOrderBook`
  remains Projection-Applied-only. Event publication is bounded with exact
  per-session `session_sequence`; sessions do not cross source-generation
  replacement and recovery/rotation use
  `CONNECTION_GENERATION_CHANGED`/`RESUBSCRIBE`. There is no second sequence
  classifier, generic event bus, or new publication thread.

There is no make-before-break source stitching. G7 historically implemented only
`SubscribeOrderBook`; the current Gateway also implements G9 `SubscribeEvents`.
`GetGatewayStatus`, USD-M, and multi-market runtime remain unimplemented. G8 is
an acceptance/test composition and opt-in CMake wiring; it does not add a
production runtime layer. G4 remains independently usable as a one-shot transport.
G5 recovers transport, snapshot, malformed-input, bounded-admission, bootstrap
overflow, `serverShutdown`, and Projection `NeedsResync` failures through a new
connection and the same conservative bootstrap path. Internal adapter,
Projection-rejection, clock, and invariant failures remain terminal. Historical
G2/G3 implementation attempts are abandoned and are not authority for the
current implementation.

The completed G2 host remains a separate direct-Projection milestone proof. G3
remains independently testable without transport and accepts deterministic
in-memory Contracts messages and synthetic fault events. G4 is the first real
network milestone and drives G3 only through its bounded serialized boundary.
G5/G6 add no sequence classifier: Projection remains the sole owner of continuity
meaning. Recovery permits at most six consecutive attempts with deterministic
delays of 1, 2, 4, 8, 16, and 30 seconds. HTTP 429/418 additionally require a
strict valid `Retry-After`, which is never capped downward; stop interrupts the
wait. G6 clean rotations consume no recovery attempt or backoff; genuine
transport/runtime failure at the planned cut wins and remains ordinary G5
recovery.

G0 acceptance and the frozen G1 candidate dependency proof do not establish a
formal upstream release. G1 must not be continuously repinned. GW-PREQ-002 is
complete and adds no runtime behavior: the normal G2–G6/G7-disabled lane uses
the root `conanfile.py` with Contracts message/Protobuf and Projection
Core/ProtoAdapter, without Contracts gRPC or `grpc`. G7 conditionally adds the
current Contracts gRPC artifact and its `grpc` dependency while preserving one
Contracts message lineage. The historical four-target proof uses
`conanfile_g1.py` explicitly.

Projection remains the owner of numeric semantics, order-book state, sequence and
gap classification, lifecycle, reset/resync, ProtoAdapter, and snapshot
construction. G7 publishes only Projection `Applied` updates. Its limits are
eight resident accepted channels, 64 ordinary records per channel, one separate
terminal slot per channel, and eight pending admissions. Existing sessions
terminate and resubscribe rather than cross G5/G6 full Projection rebootstrap.
G9 event subscribers are separately bounded at eight active sessions, 64 ordinary
records, and one terminal control slot per session.

The [2026-08-23 handoff](HANDOFF_2026-08-23.md) is historical provenance, not
current project status.
