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
G2_SYNTHETIC_HOST_IMPLEMENTED=YES
G3_SERIALIZED_MARKET_RUNTIME_IMPLEMENTED=YES
CURRENT_GATEWAY_RUNTIME_IMPLEMENTED=YES
REAL_GATEWAY_NETWORK_RUNTIME_IMPLEMENTED=YES
GATEWAY_NETWORK=SPOT_BTCUSDT_IMPLEMENTED
RECONNECT=IMPLEMENTED
AUTOMATIC_RECOVERY=IMPLEMENTED
PLANNED_ROTATION=IMPLEMENTED
MAKE_BEFORE_BREAK=NO
GATEWAY_GRPC_BUSINESS_FLOW=NOT_IMPLEMENTED
RECORDER_DEPENDENCY=NO
GW-PREQ-002=COMPLETE
NEXT=G7
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
  generation.

There is no make-before-break source stitching, gRPC server, publication queue,
or subscription runtime. G4 remains independently usable as a one-shot
transport.
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
complete and adds no runtime behavior: the normal G2–G6 lane uses the root
`conanfile.py` with Contracts message/Protobuf and Projection Core/ProtoAdapter,
without Contracts gRPC or `grpc`. The historical four-target proof uses
`conanfile_g1.py` explicitly.

Projection remains the owner of numeric semantics, order-book state, sequence and
gap classification, lifecycle, reset/resync, ProtoAdapter, and snapshot
construction. Gateway's future bounded publication and gRPC responsibilities
are defined in the milestone authority.

The [2026-08-23 handoff](HANDOFF_2026-08-23.md) is historical provenance, not
current project status.
