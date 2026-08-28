# Binance Market Data Gateway

This repository is the C++20 Gateway foundation for Issue #1. G0 and G1 are
complete; the current `main` has no production market-data runtime. The
authoritative development plan and ownership boundaries are in
[docs/MILESTONES.md](docs/MILESTONES.md), with current orientation in
[docs/CURRENT_STATE.md](docs/CURRENT_STATE.md).

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

G0 foundation acceptance is separate from formal upstream release acceptance.
The four-target G1 proof is frozen historical evidence and remains explicit and
opt-in; it is not the normal G2–G6 runtime dependency lane. Formal release and
deployment acceptance require final upstream identities and exact consumer/link
evidence.

The G1 exact candidate proof is documented in [docs/G1_CANDIDATE.md](docs/G1_CANDIDATE.md). Run
`scripts/g1-candidate-proof.sh` only with a Conan cache containing the exact candidate packages;
it performs the one-process four-target compile/link smoke and rejects a second Contracts message
lineage. G1 remains dependency proof only and does not add Gateway runtime behavior.

## Scope and ownership

See [ARCHITECTURE.md](ARCHITECTURE.md) for the concise responsibility split.
Historical evidence is retained in
[docs/HANDOFF_2026-08-23.md](docs/HANDOFF_2026-08-23.md).
