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
- Strict, finite typed configuration for Binance, Spot/USD-M perpetual, opaque exact non-empty
  strict UTF-8 scalar symbol (without malformed encodings, Unicode surrogates, ASCII C0, DEL, or
  ASCII whitespace), future gRPC endpoint, and nonzero queue capacity.
- Synchronous constructed/running/stopped lifecycle with typed transition errors.
- Offline unit tests and CTest; GCC/Clang warning configuration; ASan, UBSan, and TSan options.
- Linux-only CI foundation for independent GCC/Clang builds/tests and sanitizer jobs.
- Default-off upstream link-smoke target that discovers the exact named Contracts and Projection
  package components without copying `.proto` files.
- The future upstream smoke input is deliberately an installed prefix restored from a verified,
  platform/linkage-specific exact Conan cache bundle. Bundle verification, cache restoration, and
  `--build=never` graph replay are release-consumer steps; this repository does not claim to
  authenticate them while the formal Contracts/Projection releases are absent.
- Checked-in Binance source acquisition status and G0 evidence record.

## Pending gates

- Contracts #14: formal/public package artifact and revision for the message-only and separate gRPC
  packages.
- As of the 2026-08-23 review snapshot, [Projection PR #45](https://github.com/tomohikoAmada/BinanceMarketDataProjection/pull/45)
  and [issue #48](https://github.com/tomohikoAmada/BinanceMarketDataProjection/issues/48) remain
  OPEN for the stale ProtoAdapter/reference ASCII-only symbol rule; no merge is claimed. Gateway
  Phase A keeps the public symbol identity strict-UTF-8 and opaque rather than copying that rule.
- [Contracts issue #17](https://github.com/tomohikoAmada/BinanceMarketDataContracts/issues/17) and
  [Draft PR #18](https://github.com/tomohikoAmada/BinanceMarketDataContracts/pull/18) remain OPEN
  and unmerged for the matching Domain/schema correction; reconcile its generated outputs with the
  concurrent Contracts #13/#16 work before publication.
- A future Phase B implementation must be explicitly authorized before adding transport, REST
  bootstrap, buffering, reconnect/resync, queues, subscriptions, or gRPC service behavior.
- The remaining G0 integration dependency is publication of the immutable upstream bundle,
  hashes, and package identities. The staged-prefix smoke is intentionally not a substitute for
  that release evidence.
- Official Binance constraints are recorded from the current Agent Native `.md` pages with UTC,
  exact response hashes, and an ephemeral local acquisition manifest; no downloaded body is
  committed or executed, and the manifest is not a remote/build dependency.

The official Spot and USD-M bootstrap notes are source-acquisition facts only. Phase A does not
open a WebSocket, call REST, allocate a queue, or implement either product's sequence policy;
continuous-update classification remains Projection `Core` behavior under ADR-0008 and must not
be duplicated in Gateway. Refresh the source record and repeat hash/term validation before Phase B.
