# Current state

```text
G0_PHASE_A=IMPLEMENTED_PENDING_REVIEW
GATEWAY_NETWORK=NOT_IMPLEMENTED
GATEWAY_GRPC_BUSINESS_FLOW=NOT_IMPLEMENTED
PROJECTION_INTEGRATION=NOT_IMPLEMENTED
UPSTREAM_LINK_SMOKE=OPT_IN_BLOCKED_ON_FORMAL_PACKAGE_PINS
RECORDER_DEPENDENCY=NO
```

## Implemented in this Phase A branch

- CMake 3.24+ C++20 foundation library and `bmd-gatewayd` executable.
- Strict, finite typed configuration for Binance, Spot/USD-M perpetual, opaque non-empty symbol
  (without NUL, control, or ASCII-whitespace bytes), future gRPC endpoint, and nonzero queue
  capacity.
- Synchronous constructed/running/stopped lifecycle with typed transition errors.
- Offline unit tests and CTest; GCC/Clang warning configuration; ASan, UBSan, and TSan options.
- Linux-only CI foundation for independent GCC/Clang builds/tests and sanitizer jobs.
- Default-off upstream link-smoke target that discovers the exact named Contracts and Projection
  package components without copying `.proto` files.
- Checked-in Binance source acquisition status and G0 evidence record.

## Pending gates

- Contracts #14: formal/public package artifact and revision for the message-only and separate gRPC
  packages.
- Projection #45/#47: refreshed candidate package/adapter evidence suitable for the final link
  smoke.
- The current Projection ProtoAdapter still carries an older ASCII-only symbol rule. That is a
  separate upstream compatibility correction; Gateway Phase A deliberately keeps the symbol
  opaque and non-empty rather than copying that restriction.
- A future Phase B implementation must be explicitly authorized before adding transport, REST
  bootstrap, buffering, reconnect/resync, queues, subscriptions, or gRPC service behavior.
- Official Binance constraints are recorded from the current Agent Native `.md` pages with UTC,
  exact response hashes, and a temporary manifest; no downloaded body is committed or executed.

The official Spot and USD-M bootstrap notes are source-acquisition facts only. Phase A does not
open a WebSocket, call REST, allocate a queue, or implement either product's sequence policy;
continuous-update classification remains Projection `Core` behavior under ADR-0008 and must not
be duplicated in Gateway. Refresh the source record and repeat hash/term validation before Phase B.
