# Gateway milestone plan

Updated: 2026-11-09. Current work follows the BinanceMarketData v0.4 overall
and Gateway module plans. Historical G0-G11 / G12-A/B/C remain complete and
archived; the v0.3/v0.4 G0-G3 labels do not reopen that history.

## Current phases

| Phase | Scope | Status |
| --- | --- | --- |
| G0 | Security, protocol compatibility, availability | Complete; archived evidence |
| G1 | Data semantics and minimum freshness | Complete; archived evidence |
| G2 | Four-product production path and minimal CI | Complete; archived evidence |
| G3-a | 20-key offline config/routing/startup/recovery, 48 contexts, P1 | Complete, reviewed, merged |
| G3-b | Shared-IP API budget / BMD-RL-01 offline | Complete, reviewed, merged |
| G3-c | Shared VPS, real network, Recorder co-load, resource budget | **Open** |

## Current design boundaries

- Gateway owns Binance transport, metadata, lifecycle, bounded publication and
  gRPC. Contracts owns wire; Projection is the sole authority for fixed-point
  numbers, order book and Spot/USD-M sequence (including `pu`).
- Reuse `ConfiguredProductRuntimeSet` and the immutable registry. Exact
  `MarketKey = (venue, market, exact symbol)`; 1..20 products; one isolated
  ProductRuntime/private Projection/owner/recovery/transport per key.
- The process-global streaming-context cap is 48, not 48 per product.
- `bmd-gatewayd --config PATH`; no hot reload, runtime add/remove, generic event
  bus, DI/plugins, shared multiplexed transport or second classifier.
- All products must first reach Live/Synchronized; startup failure/exception
  rolls back. Later failure is product-local. Drain gRPC handlers before
  destroying products.
- Bounded queues, slow-client isolation, cancellation lifetime safety, source
  quiescence and async-safe signals are required.
- Spot events: DIFF_DEPTH / AGG_TRADE / BOOK_TICKER; USD-M: DIFF_DEPTH.
  Order-book publication is Projection-Applied; depth events are pre-Projection.
- Insecure gRPC binds numeric loopback only. `SubscribeMarketState` remains
  unimplemented.
- Do not label future capacity/performance acceptance complete because
  configurable code exists. Keep Foundation independently buildable.

## Validation policy

- Daily work: one production Release build plus focused offline tests using the
  pinned Conan binary cache.
- Current baseline: Release 27/27; `WARNINGS_AS_ERRORS=ON` 27/27;
  ASan+UBSan 27/27; TSan 27/27; performance instrumentation 28/28;
  Foundation 1/1.
- Concurrency/lifecycle/memory/numeric changes: relevant sanitizer and focused
  regressions. Milestone/release: full offline graph and necessary sanitizers.
- Do not repeat historical G1 proof or cold-build all third-party sources for
  routine changes. The frozen upstream link smoke remains opt-in.
- Historical proof scripts are in `scripts/archive/`; do not use them as
  current build instructions.

## Remaining quality work

- Define target hardware, product set, depth/batch/rate, CPU/RSS/thread/FD,
  p99/max latency and recovery-time budgets for G3-c.
- Measure Recorder + Gateway on the same VPS with the shared API budget.
- Verify real-network TLS/CA, DNS/proxy, restart quiet window and 429/418
  behavior.
- Projection full-book `apply_transaction` remains the dominant measured
  hotspot. P2 (partial transactional update) is conditional on the target
  budget; until then depth limit and product count are the main mitigations.
- Consider Gateway hot-path cleanups only with before/after measurements; do
  not add speculative concurrency/allocator/affinity frameworks.

See [CURRENT_STATE](CURRENT_STATE.md), [EVIDENCE](EVIDENCE.md) and
[CODE_MAP](CODE_MAP.md).
