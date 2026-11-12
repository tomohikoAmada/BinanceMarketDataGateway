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
  Plan G2, G3 and G4 were subsequently completed (see below).

## Plan G2 — persistent recoverable recovery

Gateway candidate `6fdeb47682c51463c13a04cb9a23efc5972b841b`.

- Recoverable failures no longer become terminal after six attempts. The
  coordinator keeps retrying with a 30 s capped backoff; the consecutive count
  remains observational and resets on Live.
- `kRecoveryBackoffStages=6` (`1,2,4,8,16,30`) is decoupled from
  `kRecoveryFailureHistoryCapacity=7`; the failure history stays bounded.
- The wait remains `max(normal backoff, Retry-After)` and uses the existing
  stop-token/condition-variable path. Nonrecoverable internal, adapter and
  terminal HTTP 4xx failures remain terminal.
- Tests cover seven consecutive recoverable failures with recovery on the
  eighth generation, the 30 s backoff cap, a long `Retry-After` interrupted by
  stop, and a single Spot or USD-M product failing persistently while the other
  product stays Live with generation, owner and update state unchanged.
- Local Release CTest **27/27**; ASan+UBSan and TSan recovery/composition tests
  pass.
- Linux GitHub Actions run
  [43152507325](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/43152507325):
  production CTest **27/27** (6.86 s) with the same Projection pin
  (`00e889f3…`, package ID `65e04c2c…`, PREV `21e94d37…`).
- `bmdg` SHA-256
  `094968f76874fdb63de287f565538b8a8abe7fe23d23e83531f1e845b694e516`;
  artifact `bmdg-linux-x86_64-6fdeb47682c51463c13a04cb9a23efc5972b841b`;
  archived tar SHA-256
  `f77cb52c08a9d7318838d8735d3ca40266a5f39bb38c2ce0d466faf5041ab795`.
- Scope: off-VPS implementation and Linux build/package evidence only. Plan
  G3 and G4 were subsequently completed (see below).

## Plan G3 — runtime diagnostics and attach-only consumer

Gateway candidate `282f0fc6773534470bd3bcdf9eaa3096740e0eed`.

- The daemon now waits on SIGINT/SIGTERM with a bounded 30 s interval, emits a
  one-line runtime summary each cycle, and emits per-product diagnostics only
  when state/generation/recovery-count/terminal cause changes. Failure history
  is emitted once per new generation; observation timeouts print
  `gateway_observation=unavailable` without stopping the daemon.
- `tools/consume_gateway.py` is an attach-only 20-key / 40-stream probe. It
  uses one OrderBook subscription per key, one Spot AggTrade and one Spot
  BookTicker selector per symbol, validates Accepted/session sequence/canonical
  metadata, maintains only the delivered local book, invalidates on gaps/EOF/RPC
  errors, resubscribes with `1,2,4,8,16,30 s` backoff, and writes the summary
  atomically every 30 s. It never starts/stops the gateway or contacts Binance.
- The client uses the fixed Contracts Python revision
  `518880bdfa60948c3b65b6b3525d024526995166`, with `grpcio==1.81.1` and
  `protobuf==6.33.5`. `deployment/bmdg-consumer.service` is the thin loopback
  unit; it deliberately has no `Requires`/`BindsTo` on `bmdg.service`.
- Tests: C++ diagnostics dedup/change and timed-signal tests pass; Python fake
  gRPC tests cover normal attach, sequence break, legacy metadata, EOF/reconnect
  with a new snapshot, missing data, 40-stream construction and no overlapping
  old calls.
- Local Release CTest **27/27**; ASan+UBSan and TSan production tests pass.
- Linux GitHub Actions run
  [43157004231](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/43157004231):
  consumer probe tests pass; production CTest **27/27** (6.83 s). The release
  package now also contains `tools/consume_gateway.py`, pinned client
  requirements, `contracts-src`, and `deployment/bmdg-consumer.service`.
- `bmdg` SHA-256
  `041dade119200c634d48306840749be355a68f19cfc2331af8de02563cf6259e`;
  artifact `bmdg-linux-x86_64-282f0fc6773534470bd3bcdf9eaa3096740e0eed`;
  archived tar SHA-256
  `b88c945b9b3b62ff3f62e2ff1bf96a38d72f8c052ab3270c017272f0664dd74d`.
- Scope: off-VPS implementation and Linux build/package evidence only.
  G4 was subsequently completed (see below).

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

## Plan G4 — co-located VPS 2h acceptance

Gateway release `5e47c59c0769de8b16b67f1aabdb095c3bacdf78`; binary SHA-256
`f57c1a13670f18a59f34c882c98532934f242d596e9e138852a35a78599c105f`.

- Deployed from successful Linux CI run
  [43195397457](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/43195397457)
  (CTest 27/27) to isolated `/opt/bmdg` and `/etc/bmdg`, alongside the existing
  Recorder under `/opt/sbmdr` and `/srv/recorder-data`. The Recorder was not
  restarted; its PIDs remained 442874/442895.
- A 20-key / 40-stream consumer ran for 2 h (240 samples, 30 s interval):
  40/40 accepted and valid, 20/20 order books with snapshots, 1,239,203 updates,
  3,306,529 Spot events, 20/20 status keys, 0 identity/protocol/RPC/status
  errors, 20/20 products Live, and no service restarts.
- 11 `SLOW_CONSUMER` gaps occurred on BTC/ETH/DOGE BookTicker during host
  load/archive peaks (BTC 5, ETH 5, DOGE 1). Each invalidated the stream and
  resubscribed; the maximum below-40-valid interval was one 30 s sample, and
  every stream finished valid. No order-book gap or silent generation splice
  occurred.
- The 64-deep event queue was raised to the still-bounded 1024 after a live
  BookTicker burst overflowed 64; order-book queues remain 64. The consumer
  uses per-symbol Spot event shards and an 8 MiB gRPC receive lookahead.
- Resources met the suggested lines but with thin CPU headroom: 10-minute CPU
  mean max **297.9%** of one core (mean 184.9%), MemAvailable min **3.39 GiB**,
  root free min **26.87 GiB**, no OOM or swap growth. Load peaks correlate with
  successful `sbmdr-archive.service` runs.
- Recorder C5 `verify-only` remains PASS; archive timer active with successful
  runs and no blocked/error journal entries.
- Verdict: **G4 functional acceptance PASS with observations**. The monitor's
  strict sample-level `valid == 40` rule reports FAIL because two samples were
  38/40 during gap recovery; the plan's session-correctness clause treats that
  as the expected recovered-gap path. CPU headroom and the latency tail remain
  open for G3-c/approved budgets.
- Full record: [archive/G4_VPS_2H_EVIDENCE.md](archive/G4_VPS_2H_EVIDENCE.md);
  raw monitor verdict [G4_VPS_2H_FINAL.json](archive/G4_VPS_2H_FINAL.json).
- **M4 / Phase 5 24h follow-up:** same process, consumer start
  `2026-11-12T09:57:33Z`, target `2026-11-13T09:57:33Z`; running, not yet
  accepted. At 4h08m it remained 40/40 valid with zero protocol/RPC/identity
  errors, but gaps had risen to 45 and two USD-M order books had resubscribed
  (snapshots 22); load1 was about 21. The 24h endpoint, planned rotations and
  resources still need review.

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

- G3-c: approved resource and latency budgets; G4 recorded the co-load data
  (11 recovered slow-consumer gaps, 10-minute CPU max 297.9%) but did not
  establish a new capacity/SLA budget.
- No approved multi-product capacity or latency SLA.
