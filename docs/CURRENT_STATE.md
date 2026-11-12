# Current BMDG state

Updated: 2026-11-12. Source tree reorganized by domain and renamed BMDG;
historical G0-G11 / G12-A/B/C records archived. v0.4 G3-a, G3-b and M-G1.A are
complete; plans G1 (Projection M-P2 production pin), G2 (persistent recoverable
recovery) and G3 (runtime diagnostics + attach-only consumer) have Linux
Release artifacts; plan G4 completed the Tokyo VPS 2h co-load acceptance with
observations, while G3-c resource/latency budgets remain open.

```text
HISTORICAL_G0_G11=COMPLETE_ARCHIVED
V04_G3A=COMPLETE_OFFLINE_VERIFIED_REVIEWED_CI_PASSED
V04_G3B=COMPLETE_OFFLINE_VERIFIED_REVIEWED_CI_PASSED_MERGED
V04_M_G1A=COMPLETE_RELEASE_27/27_ASAN_UBSAN_27/27_FORMAT_PASSED
V04_G3C=OPEN
PRODUCTION_DAEMON=bmdg
BINARY_COMPAT=bmd-gatewayd_symlink
CMAKE_OPTIONS=BMDG_*
CMAKE_OPTION_COMPAT=BMD_GATEWAY_*_ALIAS
PRODUCTION_CONFIG=--config PATH
CONFIGURED_PRODUCT_COUNT=1..20
GLOBAL_TRACKED_STREAMING_CONTEXT_LIMIT=48
INSECURE_GRPC_LISTEN=NUMERIC_LOOPBACK_ONLY
TRANSPORT_MODEL=INDEPENDENT_PER_MARKET_KEY
INITIAL_READINESS=ALL_CONFIGURED_PRODUCTS_LIVE_SYNCHRONIZED
LATER_FAILURE=PRODUCT_LOCAL
HOT_RELOAD=NO
SOURCE_LAYOUT=DOMAIN_BASED
TEST_BASELINE=RELEASE_27/27_WERROR_27/27_ASAN_UBSAN_27/27_TSAN_27/27_PERF_28/28_FOUNDATION_1/1
PERFORMANCE_AND_RESOURCE_BUDGETS=NOT_ESTABLISHED
MULTI_PRODUCT_PRODUCTION_CAPACITY=NOT_ESTABLISHED
PROJECTION_M_P2_A_B_C=COMPLETE
PLAN_G1_PROJECTION_PIN=COMPLETE_LINUX_RELEASE_27/27
PLAN_G2=PERSISTENT_RECOVERY_LINUX_RELEASE_27/27
PLAN_G3=DIAGNOSTICS_CONSUMER_LINUX_RELEASE_27/27
PLAN_G4=COMPLETE_VPS_2H_PASS_WITH_OBSERVATIONS
DEPLOYED_GATEWAY_RELEASE=5e47c59c0769de8b16b67f1aabdb095c3bacdf78
DEPLOYED_GATEWAY_BINARY_SHA256=f57c1a13670f18a59f34c882c98532934f242d596e9e138852a35a78599c105f
PLAN_G3C=OPEN_RESOURCE_LATENCY_BUDGET
DATA_PARITY_SPOT=ALIGNED
DATA_PARITY_USDM=GAP_BOOKTICKER_AGGTRADE_MARKPRICE_LIQUIDATION
DATA_PARITY_REST_SIDE_DATA=RESEARCH_ONLY
```

## Implemented

- `bmdg --config PATH`; strict startup JSON, one exchangeInfo fetch and one
  JSON decode per configured market, batch precision extraction for every
  requested symbol, exact `MarketKey` resolution.
- Spot decimal validation rejects zero written with a decimal point (`"0.0"`);
  aggTrade rejects `last_trade_id < first_trade_id`.
- 1..20 products; each product has its own transport, owner thread, private
  Projection, event publication and recovery coordinator. One process-wide
  48 streaming-context cap.
- All configured products must first reach Live/Synchronized. Any startup
  failure or exception rolls back the whole set. Later failures are isolated.
- `SubscribeOrderBook`, `SubscribeEvents`, `GetGatewayStatus`.
  `SubscribeMarketState` is not implemented.
- Spot events: DIFF_DEPTH / AGG_TRADE / BOOK_TICKER. USD-M events: DIFF_DEPTH.
  Order-book data is Projection-Applied; depth events are pre-Projection.
- Per-product 8 order-book + 8 event subscriptions; ordinary order-book queues
  64, event queues 1024, each with a terminal slot; slow consumers are
  terminated per session.
- Insecure gRPC binds numeric loopback only. Planned rotation is 23h50m
  monotonic, break-before-make. SIGINT/SIGTERM uses an async-safe signal model.
- Shared-IP public API budget: 3000 Spot / 600 USD-M weight per minute and 60
  WebSocket attempts per 5 minutes per market; 429/418 `Retry-After` gate.
- Domain layout: `foundation`, `protocol`, `transport`, `runtime`, `recovery`,
  `publication`, `grpc` (namespace `rpc`), `status`, `composition`,
  `production`, `performance`. Historical docs/scripts are archived.

## Verified

- Release production graph, 27/27 tests.
- Release with `BMDG_WARNINGS_AS_ERRORS=ON`, 27/27 tests.
- Debug ASan+UBSan production graph, 27/27 tests.
- Debug TSan production graph, 27/27 tests.
- Release with performance instrumentation, 28/28 tests.
- Foundation-only build, 1/1 test.
- G3-a offline: 20 keys accepted / 21 rejected; 40 baseline streams; 48
  contexts accepted, 49th `RESOURCE_EXHAUSTED`, cancel/reuse; all-product
  startup/rollback, local recovery and two full-set recovery rounds.
- G3-b offline: fixed shares, rolling windows, shared 429/418 gate, bounded
  metadata and clean cancellation; companion Recorder composition scenarios.
- M-G1.A: Release production graph 27/27 and format check pass; `"0.0"` and
  `l < f` rejection cases pass; 10 Spot + 10 USD-M startup metadata batch
  resolves all 20 products from one decode per market.
- Plan G1: Linux Ubuntu 24.04 / GCC 13 / C++20 Release artifact built with
  Projection `2de9b680` (RREV `00e889f34095258672fd46db83f0826a`, package ID
  `65e04c2c5034ee79c36b29706a827245e945a52a`, PREV
  `21e94d3728186f19e779d5ccce6e408c`); CTest 27/27. The final `bmdg` link
  inputs and binary SHA are recorded in [EVIDENCE](EVIDENCE.md). Projection's
  same-commit Ubuntu GCC suite has one inherited M5 allocation-instrumentation
  test failure (513/514); all M-P2 and production Core/Adapter tests pass.
- Plan G2: recoverable failures continue after the sixth attempt with a 30 s
  backoff cap; history remains seven entries. Seven-failure recovery, long
  `Retry-After` cancellation and Spot/USD-M product-isolation tests pass.
  Linux Release CTest 27/27; `bmdg` SHA-256
  `094968f76874fdb63de287f565538b8a8abe7fe23d23e83531f1e845b694e516`.
- Plan G3: 30 s runtime summaries, change-only per-product diagnostics,
  incremental failure history and timed signal waits are implemented.
  `tools/consume_gateway.py` attaches to the live gateway as the 20-key /
  40-stream probe; fake gRPC tests cover normal, sequence, legacy metadata,
  EOF/reconnect and missing-data cases. Linux Release CTest 27/27; `bmdg`
  SHA-256
  `041dade119200c634d48306840749be355a68f19cfc2331af8de02563cf6259e`.
- Plan G4: Gateway release `5e47c59c` / binary `f57c1a13…` deployed to the
  Tokyo VPS alongside the untouched Recorder C5 processes. A 20-key /
  40-stream consumer ran 2 h (240 samples): 40/40 accepted and valid, 20
  snapshots, 1,239,203 updates, 3,306,529 events, 20/20 status keys, 0
  protocol/RPC/status/identity errors, 20/20 Live and no restarts. Eleven
  BookTicker `SLOW_CONSUMER` gaps (BTC 5, ETH 5, DOGE 1) were detected,
  resubscribed and recovered within one 30 s sample; no order-book gaps. The
  event queue was right-sized to a still-bounded 1024. Resources met the
  suggested lines (10-minute CPU max 297.9%, MemAvailable min 3.39 GiB, root
  free min 26.87 GiB) but CPU headroom is thin. `systemctl enable` is set for
  `bmdg` and `bmdg-consumer`; the services remain running.
- P1 (pre-optimization): 20-key normal/deep/batch CPU means 0.115 / 1.192 /
  1.242 cores on the local M3, including fixtures/clients; checked apply was
  the dominant owner cost. Projection M-P2.A/B/C now replace the full-book copy
  with in-place accepted apply and the trusted adapter path; target-VPS costs
  still require G3-c measurement.

## Open

- **G3-c**: approved resource and latency budgets. G4 collected target-VPS
  co-load data (11 recovered slow-consumer gaps, 10-minute CPU max 297.9%,
  p99 publish-to-client bucket 500 ms+ under archive/load peaks) but did not
  establish a new capacity/SLA budget.
- **BMDG/BMDR data parity**: Spot is aligned; USD-M bookTicker/aggTrade/
  markPrice/forceOrder and REST side data are not yet live-provided. See
  [DATA_PARITY](DATA_PARITY.md).
- No approved multi-product performance/SLA budget. Twenty products is a
  resource bound, not measured production capacity.

See [MILESTONES](MILESTONES.md), [EVIDENCE](EVIDENCE.md),
[CODE_MAP](CODE_MAP.md) and [ARCHITECTURE](../ARCHITECTURE.md).
