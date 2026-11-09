# Current Gateway state

Updated: 2026-11-09. Source tree reorganized by domain; historical G0-G11 /
G12-A/B/C records archived. Current v0.4 G3-a and G3-b offline work is complete
and merged; G3-c remains open.

```text
HISTORICAL_G0_G11=COMPLETE_ARCHIVED
V04_G3A=COMPLETE_OFFLINE_VERIFIED_REVIEWED_CI_PASSED
V04_G3B=COMPLETE_OFFLINE_VERIFIED_REVIEWED_CI_PASSED_MERGED
V04_G3C=OPEN
PRODUCTION_DAEMON=bmd-gatewayd
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
PROJECTION_P2=CONDITIONAL
```

## Implemented

- `bmd-gatewayd --config PATH`; strict startup JSON, one exchangeInfo fetch per
  configured market, exact `MarketKey` precision resolution.
- 1..20 products; each product has its own transport, owner thread, private
  Projection, event publication and recovery coordinator. One process-wide
  48 streaming-context cap.
- All configured products must first reach Live/Synchronized. Any startup
  failure or exception rolls back the whole set. Later failures are isolated.
- `SubscribeOrderBook`, `SubscribeEvents`, `GetGatewayStatus`.
  `SubscribeMarketState` is not implemented.
- Spot events: DIFF_DEPTH / AGG_TRADE / BOOK_TICKER. USD-M events: DIFF_DEPTH.
  Order-book data is Projection-Applied; depth events are pre-Projection.
- Per-product 8 order-book + 8 event subscriptions; ordinary queues 64 with a
  terminal slot; slow consumers are terminated per session.
- Insecure gRPC binds numeric loopback only. Planned rotation is 23h50m
  monotonic, break-before-make. SIGINT/SIGTERM uses an async-safe signal model.
- Shared-IP public API budget: 3000 Spot / 600 USD-M weight per minute and 60
  WebSocket attempts per 5 minutes per market; 429/418 `Retry-After` gate.
- Domain layout: `foundation`, `protocol`, `transport`, `runtime`, `recovery`,
  `publication`, `grpc` (namespace `rpc`), `status`, `composition`,
  `production`, `performance`. Historical docs/scripts are archived.

## Verified

- Release production graph, 27/27 tests.
- Release with `BMD_GATEWAY_WARNINGS_AS_ERRORS=ON`, 27/27 tests.
- Foundation-only build, 1/1 test.
- G3-a offline: 20 keys accepted / 21 rejected; 40 baseline streams; 48
  contexts accepted, 49th `RESOURCE_EXHAUSTED`, cancel/reuse; all-product
  startup/rollback, local recovery and two full-set recovery rounds.
- G3-b offline: fixed shares, rolling windows, shared 429/418 gate, bounded
  metadata and clean cancellation; companion Recorder composition scenarios.
- P1: 20-key normal/deep/batch CPU means 0.115 / 1.192 / 1.242 cores on the
  local M3, including fixtures/clients; checked apply is the dominant owner
  cost; full-book Projection copy is the main hotspot.

## Open

- **G3-c**: shared VPS, real Binance network, Recorder co-load, target
  hardware, API budget in production and resource budgets.
- No approved multi-product performance/SLA budget. Twenty products is a
  resource bound, not measured production capacity.
- Projection P2 (partial transactional update instead of full-book copy) stays
  conditional on measured target/shared-host budgets.
- Real-network TLS/CA, DNS/proxy and deployment behavior remain to be verified
  on the target VPS.

See [MILESTONES](MILESTONES.md), [EVIDENCE](EVIDENCE.md),
[CODE_MAP](CODE_MAP.md) and [ARCHITECTURE](../ARCHITECTURE.md).
