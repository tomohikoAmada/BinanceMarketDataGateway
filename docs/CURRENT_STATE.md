# Current state

This file is the concise current-orientation record. The authoritative
development plan is [docs/MILESTONES.md](MILESTONES.md).

```text
G0=COMPLETE
G1=COMPLETE
G2=COMPLETE
G3=COMPLETE
G2_SYNTHETIC_HOST_IMPLEMENTED=YES
G3_SERIALIZED_MARKET_RUNTIME_IMPLEMENTED=YES
CURRENT_GATEWAY_RUNTIME_IMPLEMENTED=YES
REAL_GATEWAY_NETWORK_RUNTIME_IMPLEMENTED=NO
GATEWAY_NETWORK=NOT_IMPLEMENTED
GATEWAY_GRPC_BUSINESS_FLOW=NOT_IMPLEMENTED
RECORDER_DEPENDENCY=NO
GW-PREQ-002=COMPLETE
NEXT=G4
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
  and a dedicated executable; and
- the G3 Spot BTCUSDT `MarketRuntime` with one private `BookProjection`, one
  serialized owner thread, bounded ingress and bootstrap buffers, an injected
  clock, synthetic input/fault admission, copied owner-domain observations, and
  deterministic joined shutdown.

There is no production WebSocket or REST transport, reconnect/recovery runtime,
gRPC server, publication queue, or subscription runtime. Historical G2/G3
implementation attempts are abandoned and are not authority for the current G3
implementation.

The completed G2 host remains a separate direct-Projection milestone proof. G3
also has no real transport: it accepts deterministic in-memory Contracts messages
and synthetic fault events through its bounded runtime boundary. G4 remains the
first real network milestone.

G0 acceptance and the frozen G1 candidate dependency proof do not establish a
formal upstream release. G1 must not be continuously repinned. GW-PREQ-002 is
complete and adds no runtime behavior: the normal G2–G6 lane uses the root
`conanfile.py` with Contracts message/Protobuf and Projection Core/ProtoAdapter,
without Contracts gRPC or `grpc`. The historical four-target proof uses
`conanfile_g1.py` explicitly.

Projection remains the owner of numeric semantics, order-book state, sequence and
gap classification, lifecycle, reset/resync, ProtoAdapter, and snapshot
construction. Gateway's future transport, recovery, bounded
publication, and gRPC responsibilities are defined in the milestone authority.

The [2026-08-23 handoff](HANDOFF_2026-08-23.md) is historical provenance, not
current project status.
