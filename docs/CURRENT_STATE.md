# Current state

```text
G0_PHASE_A=IMPLEMENTED_ACCEPTED
G1_EXACT_CANDIDATE=COMPLETE
G2_LOCAL_SYNTHETIC_CANDIDATE=IMPLEMENTED
GATEWAY_NETWORK=NOT_IMPLEMENTED
GATEWAY_GRPC_BUSINESS_FLOW=NOT_IMPLEMENTED
PROJECTION_INTEGRATION=G2_SYNTHETIC_ONLY
UPSTREAM_LINK_SMOKE=OPT_IN_NOT_FORMALLY_VERIFIED
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

## G2 local implementation candidate

- One deterministic in-memory Binance Spot `BTCUSDT` host uses the exact G1 Contracts and
  Projection candidate graph.
- Synthetic depth diffs are buffered before a synthetic REST snapshot, then replayed in arrival
  order through `Projection::ProtoAdapter` and `Projection::Core`; later diffs use the same path.
- The host exposes the real `LocalOrderBookSnapshot` contract with fixed synthetic metadata.
- This is not a real network client, gRPC runtime, production service, deployment artifact, or G3+
  runtime. There are no threads, queues, clocks, reconnects, subscriptions, or Recorder links.

## Gate distinction

G0 foundation acceptance is independent of formal upstream package publication. The focused
foundation code, tests, CI evidence, and runtime-free scope are accepted here without claiming
that the upstream link-smoke established any formal package identity.

G1/G2 candidate development may use explicitly identified exact candidate dependencies, but those
dependencies must not be described as a formal release. Formal integration/release/deployment
acceptance remains a separate later gate requiring final Contracts and Projection identities,
immutable supported package artifacts, exact consumer/link proof, and appropriate clean-cache
evidence.

## Later gates

- Contracts #14: formal/public package artifact and revision for the message-only and separate gRPC
  packages.
- Contracts PRs #13 and #18 are merged; issues #15 and #17 are closed. Draft PR #16 remains OPEN
  for the package split and formal release-readiness surface. Its latest local, unpushed release
  candidate still has P1 gaps in exact base package-ID/PREV provenance and full frozen/replayed
  graph-node comparison, so it is not release-ready.
- Projection PR #49 is merged and issue #48 is closed. [Projection PR #45](https://github.com/tomohikoAmada/BinanceMarketDataProjection/pull/45)
  remains OPEN and Draft until it is repinned to the final Contracts release identities and its
  formal package gate passes. Gateway Phase A keeps the same strict-UTF-8 opaque identity.
- A future Phase B implementation must be explicitly authorized before adding transport, REST
  bootstrap, buffering, reconnect/resync, queues, subscriptions, or gRPC service behavior.
- Formal upstream publication of the immutable bundle, hashes, and package identities remains a
  later integration/release/deployment requirement. The staged-prefix smoke is intentionally not
  a substitute for that release evidence and is not a G0 merge blocker.
- Official Binance constraints are recorded from the current Agent Native `.md` pages with UTC,
  exact response hashes, and an ephemeral local acquisition manifest; no downloaded body is
  committed or executed, and the manifest is not a remote/build dependency.

The official Spot and USD-M bootstrap notes are source-acquisition facts only. Phase A does not
open a WebSocket, call REST, allocate a queue, or implement either product's sequence policy;
continuous-update classification remains Projection `Core` behavior under ADR-0008 and must not
be duplicated in Gateway. Refresh the source record and repeat hash/term validation before Phase B.

The cross-repository checkpoint, exact heads, blockers, next-action order, and agent rules are in
[HANDOFF_2026-08-23.md](HANDOFF_2026-08-23.md).
