# Evidence summary

This is the current evidence index. Full historical records are in
[archive/](archive/README.md) and are not current build/design instructions.

## Domain-layout refactor — 2026-11-09

- Source/tests/acceptance tools moved from `gN` directories to domain
  directories; namespaces and CMake targets renamed accordingly.
- Release production graph: **27/27** CTest pass.
- Release with `BMD_GATEWAY_WARNINGS_AS_ERRORS=ON`: **27/27** CTest pass.
- Debug ASan+UBSan production graph: **27/27** CTest pass.
- Debug TSan production graph: **27/27** CTest pass.
- Release with performance instrumentation: **28/28** tests pass.
- Foundation-only build (no Conan): **1/1** CTest pass.
- No wire/protocol, Projection pin or behavior change. The old `G0`-`G11`
  evidence remains archived; see [CODE_MAP](CODE_MAP.md).

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

- Gateway fixed shares: 3000 Spot / 600 USD-M weight per minute; 60 WebSocket
  attempts per 5 minutes per market.
- Rolling windows count failed attempts; reconnect/Live does not reset them.
- 429/418 `Retry-After` installs a shared market gate; metadata fails closed on
  unavailable quota; cancellation drains timers cleanly.
- Five two-process Gateway/Recorder composition scenarios pass offline; no IPC,
  new service or dependency pin.
- Deployment still requires exactly one instance per role, no blue/green
  overlap, and a full 300 s quiet interval after restart/replacement.
- Full record: [archive/GATEWAY_G3B_SHARED_API_EVIDENCE.md](archive/GATEWAY_G3B_SHARED_API_EVIDENCE.md).

## Projection P1 — measured cost

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
- Full-book `apply_transaction` copy/rebuild is the dominant hotspot. P2
  remains conditional on a measured shared-host budget.
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

- G3-c: shared VPS, real Binance network, Recorder co-load, target hardware,
  resource budgets and production restart behavior.
- No approved multi-product capacity or latency SLA.
- Real-network TLS/CA and DNS/proxy behavior on the target VPS.
