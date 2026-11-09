# BMDG contribution rules

Read before changes:

1. [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md)
2. [docs/MILESTONES.md](docs/MILESTONES.md)
3. [ARCHITECTURE.md](ARCHITECTURE.md)
4. [docs/CODE_MAP.md](docs/CODE_MAP.md), [docs/DATA_PARITY.md](docs/DATA_PARITY.md) and relevant evidence

If code, protocol facts or measurements contradict documentation, establish
evidence and update the current documents. User instructions take precedence.
Do not infer permission steps from archived milestone freezes.

## Current status

Historical G0-G11 / G12-A/B/C are complete and archived. Current v0.4 G3-a
(20-key offline lifecycle/routing/P1) and G3-b (shared-IP API budget) are
complete, reviewed and merged. **G3-c is open**: shared VPS, real network,
Recorder co-load and resource budgets are not accepted. Twenty products is a
resource bound, not measured production capacity. Performance budgets are not
established. Keep Foundation independently buildable.

## Boundaries

- BMDG owns Binance transport, metadata, lifecycle, bounded publication and
  gRPC. Contracts owns proto/wire; Projection owns numeric semantics, order book
  and Spot/USD-M sequencing (including `pu`). No Recorder dependency.
- Reuse `ConfiguredProductRuntimeSet` and the immutable registry. Exact
  `MarketKey = (venue, market, exact symbol)`, 1..20 products, one isolated
  ProductRuntime/private Projection/owner/recovery/transport per key. The
  process-global streaming-context cap remains 48, not 48 per product.
- All configured products must initially reach Live/Synchronized. Any startup
  return failure or exception rolls everything back. Later product failure is
  isolated. Shut down and drain handlers before destroying products.
- Use startup JSON `bmdg --config PATH`. No hot reload, runtime
  add/remove, generic event bus, DI/plugins, shared multiplexed transport or
  second classifier.
- Insecure gRPC must bind numeric loopback only; reject hostname, wildcard and
  non-loopback endpoints before starting products or binding.
- Preserve bounded queues, slow-client isolation, cancellation lifetime safety,
  source quiescence before reset, and async-safe signal handling.
- Spot events: DIFF_DEPTH, AGG_TRADE, BOOK_TICKER. USD-M: DIFF_DEPTH only.
  Order-book publication is Projection-Applied; depth events are pre-Projection.
  Sessions do not stitch across a full rebootstrap/source replacement.
- Tests may use existing internal seams; production must not contain an
  acceptance-only hook or expose test controls in configuration.
- Do not copy proto files or add floating dependencies. Preserve the separate,
  explicit frozen upstream link smoke; do not repin it merely for newer
  upstream main.
- Twenty products is a resource bound, not measured capacity. Historical
  fixed-two performance evidence is not multi-product proof. Optimize only an
  evidenced problem; keep changes small and avoid speculative concurrency
  frameworks.
- Research/live parity is a delivery requirement. Any BMDR-only field used by
  a production signal must be provided by BMDG or explicitly marked
  research-only; see [docs/DATA_PARITY.md](docs/DATA_PARITY.md).

## Source layout

Use domain directories and namespaces; do not add new `gN` directories.

```text
src/foundation/  src/protocol/   src/transport/  src/runtime/
src/recovery/    src/publication/ src/grpc/      src/status/
src/composition/ src/production/ src/performance/
```

`src/grpc/` uses namespace `rpc` because `grpc` would collide with the gRPC
library. Historical docs/scripts are in `docs/archive/` and `scripts/archive/`;
they are not current build instructions.

Naming: BMDG is the product/brand, `bmdg` is the binary, CMake options are
`BMDG_*`, and library targets are `bmdg_*`. Old `BMD_GATEWAY_*` options,
`bmd_gateway_*` library targets and `bmd-gatewayd` remain as transition aliases.

## Validation and delivery

Use CMake >=3.24. Foundation-only:

```sh
cmake -S . -B build/foundation -DCMAKE_BUILD_TYPE=Debug
cmake --build build/foundation --parallel
ctest --test-dir build/foundation --output-on-failure
```

Production graph:

```sh
cmake -S . -B build/production -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=/absolute/path/to/conan_toolchain.cmake \
  -DBMDG_BUILD_PRODUCTION_DAEMON=ON \
  -DBMDG_BUILD_TESTS=ON
cmake --build build/production --parallel 4
ctest --test-dir build/production --output-on-failure
```

Use `BMDG_BUILD_ACCEPTANCE_TOOLS=ON` for historical live/acceptance
executables. Use `BMDG_ENABLE_PERFORMANCE_BASELINE_INSTRUMENTATION=ON`
only for measurement (about +110 MiB RSS for 20 products). Use
`BMDG_BUILD_UPSTREAM_LINK_SMOKE=ON` only for the frozen link proof.

For runtime changes explicitly enable the production graph, run affected
offline tests, relevant sanitizers and `scripts/format-check.sh`. Existing
GitHub Foundation checks alone do not verify production changes. Reuse pinned
binary dependencies; do not repeat historical G1 proof or cold-build all
third-party sources for routine fixes. Full offline and necessary sanitizer
checks belong at milestone/release closeout. Keep build/cache output under
ignored directories.

Update current state, milestone status and evidence with each milestone. Push
reviewable commits; merge completed PRs before removing their remote branch.
Never discard unmerged work. Keep main as the only long-lived GitHub branch;
retain merged PR/history records.
