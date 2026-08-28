# Current state

This file is the concise current-orientation record. The authoritative
development plan is [docs/MILESTONES.md](MILESTONES.md).

```text
G0=COMPLETE
G1=COMPLETE
G2=COMPLETE
G3=COMPLETE
G4=COMPLETE
G2_SYNTHETIC_HOST_IMPLEMENTED=YES
G3_SERIALIZED_MARKET_RUNTIME_IMPLEMENTED=YES
CURRENT_GATEWAY_RUNTIME_IMPLEMENTED=YES
REAL_GATEWAY_NETWORK_RUNTIME_IMPLEMENTED=YES
GATEWAY_NETWORK=SPOT_BTCUSDT_IMPLEMENTED
RECONNECT=NOT_IMPLEMENTED
AUTOMATIC_RECOVERY=NOT_IMPLEMENTED
PLANNED_ROTATION=NOT_IMPLEMENTED
GATEWAY_GRPC_BUSINESS_FLOW=NOT_IMPLEMENTED
RECORDER_DEPENDENCY=NO
GW-PREQ-002=COMPLETE
NEXT=G5
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
  clean stop.

There is no reconnect/recovery runtime, planned connection rotation, gRPC
server, publication queue, or subscription runtime. G4 makes one startup attempt
and fails closed on transport, parser, snapshot, bounded-admission, or Projection
failure. Historical G2/G3 implementation attempts are abandoned and are not
authority for the current implementation.

The completed G2 host remains a separate direct-Projection milestone proof. G3
remains independently testable without transport and accepts deterministic
in-memory Contracts messages and synthetic fault events. G4 is the first real
network milestone and drives G3 only through its bounded serialized boundary.

G0 acceptance and the frozen G1 candidate dependency proof do not establish a
formal upstream release. G1 must not be continuously repinned. GW-PREQ-002 is
complete and adds no runtime behavior: the normal G2–G6 lane uses the root
`conanfile.py` with Contracts message/Protobuf and Projection Core/ProtoAdapter,
without Contracts gRPC or `grpc`. The historical four-target proof uses
`conanfile_g1.py` explicitly.

Projection remains the owner of numeric semantics, order-book state, sequence and
gap classification, lifecycle, reset/resync, ProtoAdapter, and snapshot
construction. Gateway's future recovery, bounded publication, and gRPC
responsibilities are defined in the milestone authority.

The [2026-08-23 handoff](HANDOFF_2026-08-23.md) is historical provenance, not
current project status.
