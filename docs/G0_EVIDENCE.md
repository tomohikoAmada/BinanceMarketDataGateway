# G0 Phase A evidence

## Scope

This branch addresses only Gateway Issue #1. It establishes a reproducible foundation; it does not
claim a production Gateway pipeline or an upstream package integration.

## Evidence map

| Area | Evidence | Status |
|---|---|---|
| Typed configuration | `include/.../config.hpp`, `src/config.cpp`, offline unit tests | Implemented |
| Lifecycle seam | `include/.../lifecycle.hpp`, `src/lifecycle.cpp`, daemon smoke | Implemented |
| Build/test | `CMakeLists.txt`, CTest, `CMakePresets.json` | Implemented; local validation recorded in PR |
| Sanitizers | CMake ASan/UBSan/TSan options and Linux CI matrix | Implemented; CI executes on push/PR |
| Upstream link smoke | `BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE` and `tests/upstream_link_smoke.cpp` | Opt-in; blocked until exact packages are published |
| Binance protocol record | `docs/official-binance-constraints.md` | Exact `.md` hashes/bytes and concise future constraints recorded; no runtime use |

## Explicit non-claims

- The endpoint flag is validated but never bound.
- The queue capacity is a typed future-runtime parameter; no queue is allocated.
- Symbol text is treated as an opaque, exact, non-empty strict UTF-8 scalar identity (ASCII C0,
  space/ASCII whitespace, DEL, malformed encodings, and surrogate code points are rejected);
  original bytes, case, and normalization are preserved. G0 does not assert exchange membership.
  Official lowercase stream mapping and `exchangeInfo` membership checks belong to the later
  transport boundary.
- No `.proto` source is copied or generated in this repository.
- No Contracts/Projection package was fetched, pinned, or linked during the offline default build.
- No network, REST, WebSocket, gRPC business flow, snapshot handoff, reconnect, or sequence policy
  behavior is implemented.

The ephemeral local acquisition manifest for the recorded retrieval was
`/private/tmp/gateway-binance-docs-20260823/manifest.json` with
`remote_content_executed=false` and `llms_full_loaded=false`; it is not a remote dependency, build
input, repository dependency, or committed artifact.
