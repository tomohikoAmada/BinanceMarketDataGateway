# Current state

This file is the concise current-orientation record. The authoritative
development plan is [docs/MILESTONES.md](MILESTONES.md).

```text
G0=COMPLETE
G1=COMPLETE
CURRENT_GATEWAY_RUNTIME_IMPLEMENTED=NO
GATEWAY_NETWORK=NOT_IMPLEMENTED
GATEWAY_GRPC_BUSINESS_FLOW=NOT_IMPLEMENTED
RECORDER_DEPENDENCY=NO
NEXT=GW-PREQ-002
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
- build/CI/sanitizer support; and
- the explicit, opt-in G1 dependency proof.

There is no current production WebSocket, REST transport, `MarketRuntime`, owner
thread, runtime ingress queue, bootstrap buffer, reconnect runtime, gRPC server,
or subscription runtime. Historical G2/G3 implementation attempts are abandoned
and absent from `main`.

G0 acceptance and the frozen G1 candidate dependency proof do not establish a
formal upstream release or normal runtime dependency lane. G1 must not be
continuously repinned. GW-PREQ-002 is the next work item and adds no runtime
behavior; the normal G2–G6 lane does not require Contracts gRPC.

Projection remains the owner of numeric semantics, order-book state, sequence and
gap classification, lifecycle, reset/resync, ProtoAdapter, and snapshot
construction. Gateway's future transport, scheduling, recovery, bounded
publication, and gRPC responsibilities are defined in the milestone authority.

The [2026-08-23 handoff](HANDOFF_2026-08-23.md) is historical provenance, not
current project status.
