# Binance Market Data Gateway

This repository is the C++20 Gateway implementation for Issue #1. G1 is complete as an exact
candidate dependency proof. G2 is a local implementation candidate for one Binance Spot BTCUSDT
synthetic host; it uses only deterministic in-memory inputs and makes no production or deployment
claim.

## Build and test

```sh
cmake --preset gcc-debug
cmake --build --preset gcc-debug
ctest --preset gcc-debug
scripts/format-check.sh
```

The equivalent Clang preset is `clang-debug`. The daemon lifecycle smoke is offline:

```sh
build/gcc-debug/bmd-gatewayd \
  --venue binance \
  --market spot \
  --symbol BTCUSDT \
  --grpc-listen 127.0.0.1:50051 \
  --queue-capacity 1024
```

It validates the configuration, transitions `constructed -> running -> stopped`, and exits. It
does not bind the endpoint, start a thread, open a WebSocket, or start a gRPC server.

## G2 synthetic proof

With the exact G1 candidate Conan graph installed, generate a build-local Conan toolchain and
configure the opt-in G2 target:

```sh
conan install . --output-folder=build/g2-conan --build=never \
  -s build_type=Release -s compiler.cppstd=20
cmake -S . -B build/g2-synthetic -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=build/g2-conan/conan_toolchain.cmake \
  -DBMD_GATEWAY_BUILD_TESTS=ON \
  -DBMD_GATEWAY_BUILD_G2_SYNTHETIC=ON
cmake --build build/g2-synthetic
ctest --test-dir build/g2-synthetic --output-on-failure
build/g2-synthetic/bmd_gateway_g2_synthetic_smoke
```

The proof covers pre-snapshot buffering, synthetic REST baseline installation, ordered replay,
Projection-owned Spot sequence handling, LIVE updates, and the real
`LocalOrderBookSnapshot`. It adds no real network, gRPC runtime, owner thread, bounded queue, or
Recorder dependency.

## Upstream link smoke

`BMD_GATEWAY_BUILD_UPSTREAM_LINK_SMOKE` is `OFF` by default. When explicitly enabled, the build
requires Conan-generated package metadata for an exact graph and discovers, without copying schemas:

- `BinanceMarketDataContracts::Protobuf`;
- `BinanceMarketDataContracts::Grpc` from `BinanceMarketDataContractsGrpc`; and
- `BinanceMarketDataProjection::Core` and `::ProtoAdapter`.

The single smoke executable links all four upstream targets and includes their public surfaces.
Contracts package publication/revision and the final Projection candidate gate remain pending;
therefore no RREV, SHA, or floating source is recorded here. This is not a blocker for G0
foundation acceptance; it remains a later integration/release/deployment gate.

When the upstream releases are formally published, the exact platform/linkage-specific Conan graph
must be installed with `--build=never` and consumed through Conan’s generated CMake toolchain and
package metadata. The Gateway smoke validates the exact CMake package versions and exported
targets; it does not authenticate a bundle or invent upstream revisions. Until those release
identities exist, keep this option disabled and do not replace it with a source checkout, a
floating dependency, or a locally invented SHA.

G0 foundation acceptance is separate from formal upstream release acceptance. G0 claims no formal
Contracts or Projection release identity and no formal upstream link proof. Later G1/G2 candidate
development may use explicitly identified exact candidates, but formal integration/release/
deployment acceptance still requires the final identities, immutable supported artifacts, exact
consumer/link proof, and appropriate clean-cache evidence.

The G1 exact candidate proof is documented in [docs/G1_CANDIDATE.md](docs/G1_CANDIDATE.md). Run
`scripts/g1-candidate-proof.sh` only with a Conan cache containing the exact candidate packages;
it performs the one-process four-target compile/link smoke and rejects a second Contracts message
lineage. G1 remains complete dependency proof; G2 is the local synthetic-only runtime candidate.

## Scope and ownership

The dependency and ownership decisions are recorded in [ARCHITECTURE.md](ARCHITECTURE.md),
[docs/CURRENT_STATE.md](docs/CURRENT_STATE.md), and [docs/adr/ADR-0001-g0-foundation-boundaries.md](docs/adr/ADR-0001-g0-foundation-boundaries.md).
Official Binance source acquisition status is recorded in
[docs/official-binance-constraints.md](docs/official-binance-constraints.md).
The current cross-repository completion state and continuation order are recorded in
[docs/HANDOFF_2026-08-23.md](docs/HANDOFF_2026-08-23.md).
