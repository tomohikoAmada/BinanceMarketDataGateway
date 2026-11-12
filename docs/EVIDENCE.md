# BMDG evidence summary

This is the current evidence index. Full historical records are in
[archive/](archive/README.md) and are not current build/design instructions.

## Domain layout and BMDG naming — 2026-11-10

- Source/tests/acceptance tools moved from `gN` directories to domain
  directories; namespaces and CMake targets renamed accordingly.
- Release production graph: **27/27** CTest pass.
- Release with `BMDG_WARNINGS_AS_ERRORS=ON`: **27/27** CTest pass.
- Debug ASan+UBSan production graph: **27/27** CTest pass.
- Debug TSan production graph: **27/27** CTest pass.
- Release with performance instrumentation: **28/28** tests pass.
- Foundation-only build (no Conan): **1/1** CTest pass.
- No wire/protocol, Projection pin or behavior change. The old `G0`-`G11`
  evidence remains archived; see [CODE_MAP](CODE_MAP.md).

## BMDG naming and data parity

- Project/brand: **BMDG** (Binance Market Data Gateway).
- Binary: `bmdg`; `bmd-gatewayd` remains a transition symlink/custom target.
- CMake options: `BMDG_*`; old `BMD_GATEWAY_*` names are deprecated aliases.
- CMake library targets: `bmdg_*`; old `bmd_gateway_*` names are aliases.
- `bmd-gatewayd` build symlink and installed symlink point to `bmdg`.
- Old `BMD_GATEWAY_*` CMake options configure successfully as deprecated
  aliases.
- Data parity: Spot depth/bookTicker/aggTrade aligned; USD-M
  bookTicker/aggTrade/markPrice/forceOrder are Phase 1 gaps; REST side data is
  explicitly research-only for Live Contract v1. See
  [DATA_PARITY](DATA_PARITY.md).

## M-G1.A — protocol boundaries and startup metadata

- Spot decimal validation now rejects `"0.0"` as zero, not as a non-zero value
  containing a decimal point. Rejection tests cover both price and quantity.
- Spot aggTrade rejects `last_trade_id < first_trade_id`; `l == f` remains valid.
- Spot and USD-M exchangeInfo batch parsers decode the document once. Production
  startup groups configured keys by market and resolves all requested symbols
  from that decode; single-symbol parsing uses the same implementation.
- Release production graph: **27/27** CTest pass; Debug ASan+UBSan production
  graph: **27/27** CTest pass; `scripts/format-check.sh` passes.
- A 10 Spot + 10 USD-M startup metadata test resolves all 20 products and
  verifies each symbol's precision from one fetch count per market.

## Plan G1 — Projection M-P2 production pin

Built from Gateway candidate `7016d47bda65ab86ad2114e88321f4dfa487f38f`
(G1 dependency change `62f5d61a64a276cba7db65a22e32ca4e01f5b965`) on GitHub
Actions `ubuntu-24.04`, GCC 13, C++20, Release.

- Projection source `2de9b68071867eb0d35a9b11a06bb1dd8c652d56`; recipe
  revision `00e889f34095258672fd46db83f0826a`; Linux package ID
  `65e04c2c5034ee79c36b29706a827245e945a52a`; PREV
  `21e94d3728186f19e779d5ccce6e408c`.
- `proto_adapter=True`, `shared=False`; Contracts message/gRPC packages remain
  `53713fb82c27cf6ea0395f6b8a853006` /
  `c3661aa735e1eb6450c1a93f635dbc20`.
- Production daemon ON, tests ON, upstream link smoke OFF. CTest **27/27
  passed** (6.26 s), including four/twenty-product, RPC, recovery/rotation and
  production/daemon tests.
- Projection same-commit CI was rerun after Contracts became public (run
  [42979315668](https://github.com/tomohikoAmada/BinanceMarketDataProjection/actions/runs/42979315668)).
  Ubuntu Clang Release, macOS Release, ASan/UBSan, canonical Quality, M4
  static/shared macOS and the Conan package regression pass. Ubuntu GCC Release
  and M4 shared Ubuntu GCC are **513/514**: the only failure is the M5
  allocation-instrumentation test
  `AllocationInstrumentationAdversarial.PointerReuseObservesFreshProvenance`
  (`allocation_instrumentation_test.cpp:361`, pointer-reuse assertion). The
  M-P2 tests (`M3AcceptedApplyMutatesInPlace`,
  `M3EmptyAcceptedBatchDoesNoBookWork`) and production Core/Adapter tests pass.
  This GCC-only test issue is inherited from `2de9b680`, not introduced by the
  Gateway pin; it is not part of the Gateway production graph.
- The final `bmdg` link line references `libbmd_projection_core.a` and
  `libbmd_projection_proto_adapter.a` from the new package; see the artifact's
  `projection-link-inputs.txt` and `bmdg-link.txt`.
- `bmdg` SHA-256
  `d5e03400c21f22d4bf5350337f9adc69ece56429595b958e0d3b8261d63344e3`;
  `ldd` lists only system libraries, with no `not found` and no build-cache
  path.
- GitHub Actions run
  [43143646562](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/43143646562);
  artifact `bmdg-linux-x86_64-7016d47bda65ab86ad2114e88321f4dfa487f38f`.
  Archived tar SHA-256
  `75e000c1632ad10785a2fc8b3f6ace06b93330cb8862cb42686aba07abe6b4ad`.
- Scope: Linux build/package evidence only. No VPS install or service change.
  Plan G2/G3/G4 remain open.

## G3-a — 20-key offline lifecycle

- 20 exact products accepted at config/preparation/registry; 21 rejected.
- 20 private owners/Projections/recoveries/transports; heterogeneous precision
  and exact Book/Event/Status routing verified.
- 40 baseline streams (one Book + one Depth Event per key); 48 contexts
  accepted, 49th `RESOURCE_EXHAUSTED`; cancel/reuse and final zero contexts.
- All-product first-Live and every startup failure position roll back; a late
  Live still obeys the absolute deadline.
- Product-local recovery and two rounds of all-20-product recovery pass; max one
  active transport per key.
- 48 remains the process cap. A 60-stream all-events profile is outside it.
- Full record: [archive/GATEWAY_G3_OFFLINE_EVIDENCE.md](archive/GATEWAY_G3_OFFLINE_EVIDENCE.md).

## G3-b — shared-IP API budget

- BMDG fixed shares: 3000 Spot / 600 USD-M weight per minute; 60 WebSocket
  attempts per 5 minutes per market.
- Rolling windows count failed attempts; reconnect/Live does not reset them.
- 429/418 `Retry-After` installs a shared market gate; metadata fails closed on
  unavailable quota; cancellation drains timers cleanly.
- Five two-process BMDG/Recorder composition scenarios pass offline; no IPC,
  new service or dependency pin.
- Deployment still requires exactly one instance per role, no blue/green
  overlap, and a full 300 s quiet interval after restart/replacement.
- Full record: [archive/GATEWAY_G3B_SHARED_API_EVIDENCE.md](archive/GATEWAY_G3B_SHARED_API_EVIDENCE.md).

## Projection P1 — pre-optimization measured cost

Offline, Apple M3, eight cores, 24 GiB; whole-process CPU/RSS include embedded
fixtures/clients. Not a VPS or SLA result.

| 20-key scenario | CPU mean | RSS | Delivered updates/s |
| --- | ---: | ---: | ---: |
| normal | 0.115 cores | ~27 MiB | ~82 |
| deep | 1.192 cores | ~41 MiB | ~820 |
| batch | 1.242 cores | ~53 MiB | ~820 |

- 127,920 measured updates / 255,840 client deliveries across 18 paired trials.
- Checked apply is 95.2% / 98.3% / 97.6% of owner update wall time
  (normal/deep/batch).
- Checked apply means: ~592 / ~1018 / ~991 µs per update.
- Projection allocations: ~2023 / 161 KB per update at D1000; ~10029 / 1 MB at
  D5000 (existing-price B1/B10/B100).
- P1 attributed the dominant cost to full-book `apply_transaction` copy/rebuild.
  That is pre-optimization evidence: Projection M-P2.A/B/C are now complete,
  accepted apply is in place, and D5000 existing-price updates measured zero
  allocations. Target-VPS costs remain unmeasured.
- Tail variability is retained; the largest observed Book maximum was 102.4 ms
  in one deep run. No latency SLA is claimed.
- Four-product P1 (2026-10-18): 0.057 / 0.925 / 0.983 cores and ~18.5 / ~28 /
  ~33 MiB for D1000/B10/10Hz, D5000/B10/100Hz, D5000/B100/100Hz.
- Projection-side records: `BinanceMarketDataProjection/docs/PROJECTION_P1_RESULT.md`
  and `PROJECTION_P1_TWENTY_RESULT.md`.

## Historical records

Archived for traceability only:

- [2026-10-04 code review](archive/CODE_REVIEW_2026-10-04.md)
- [G0 evidence](archive/G0_EVIDENCE.md), [G1 candidate](archive/G1_CANDIDATE.md)
- [v0.3 G0](archive/GATEWAY_G0_V03_EVIDENCE.md),
  [G1](archive/GATEWAY_G1_V03_EVIDENCE.md),
  [G2](archive/GATEWAY_G2_V03_EVIDENCE.md)
- [G3-a full](archive/GATEWAY_G3_OFFLINE_EVIDENCE.md),
  [G3-b full](archive/GATEWAY_G3B_SHARED_API_EVIDENCE.md)
- [2026-08-23 handoff](archive/HANDOFF_2026-08-23.md)
- [pre-review milestones](archive/MILESTONES_PRE_REVIEW_2026-10-04.md)
- [Projection integration review](archive/PROJECTION_INTEGRATION_REVIEW_2026-10-04.md)
- [ADR-0001](archive/ADR-0001-g0-foundation-boundaries.md)

## Still unproven

- Plan G2: recoverable failures still exhaust after six attempts; long-failure
  recovery, cancellation and product isolation tests are pending.
- Plan G3: low-frequency runtime diagnostics and the attach-only 20-key /
  40-stream consumer are not implemented.
- Plan G4: no VPS installation, systemd/CA validation or 2h co-load run.
- G3-c: shared VPS, real Binance network, Recorder co-load, target hardware,
  resource budgets and production restart behavior.
- No approved multi-product capacity or latency SLA.
- Real-network TLS/CA and DNS/proxy behavior on the target VPS.
