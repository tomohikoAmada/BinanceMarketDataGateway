# Gateway contribution rules

Read before changes:

1. [docs/CURRENT_STATE.md](docs/CURRENT_STATE.md)
2. [docs/MILESTONES.md](docs/MILESTONES.md)
3. [ARCHITECTURE.md](ARCHITECTURE.md)
4. relevant implementation and evidence

The current plan replaces stale historical stage/authorization prose. If code,
protocol facts or measurements contradict documentation, establish evidence and
update the current documents. User instructions take precedence. Do not add
permission steps inferred from old milestone freezes.

Historical G0-G11 and G12-A/B/C are complete. The 2026-10-12 BinanceMarketData
v0.3 overall and Gateway module plans now define G0-G3; these new labels do not
reopen historical dependency proof. G0's independent review and reset-observation
fix are complete. The user authorized v0.3 G1 on 2026-10-15; implementation,
offline validation and independent review completed on 2026-10-16. G2/G3 are
not started. High performance, low latency, low CPU/RSS
and safe stable behavior are delivery requirements; no multi-product performance
budget has yet been established. Do not label future acceptance complete
because configurable code exists. Keep Foundation independently buildable.

## Boundaries

- Gateway owns Binance transport, metadata, lifecycle, bounded publication and
  gRPC. Contracts owns proto/wire; Projection owns numeric semantics, order
  book and Spot/USD-M sequencing (including `pu`). No Recorder dependency.
- Reuse `ConfiguredProductRuntimeSet` and the immutable registry. Exact
  `MarketKey = (venue, market, exact symbol)`, 1..8 configured products, one
  isolated ProductRuntime/private Projection/owner/recovery/transport per key.
  The process-global streaming context cap remains 48, not 48 per product.
- All configured products must initially reach Live/Synchronized. Any startup
  return failure or exception rolls everything back. Later product failure is
  isolated. Shut down and drain handlers before destroying products.
- Use startup JSON `bmd-gatewayd --config PATH`. No hot reload, runtime add/remove,
  generic event bus, DI/plugins, shared multiplexed transport or second classifier.
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
  explicit frozen G1 upstream smoke; do not repin it merely for newer upstream main.
- Eight products is a resource bound, not measured capacity. Historical fixed-two
  performance evidence is not multi-product proof. Optimize only an evidenced
  problem; keep changes small and avoid speculative concurrency frameworks.

## Validation and delivery

Use CMake >=3.24. For runtime changes explicitly enable the production graph,
build/run the affected offline CTest groups, relevant sanitizers and
`scripts/format-check.sh`. Existing GitHub Foundation checks alone do not verify
production changes. Reuse pinned binary dependencies; do not repeat historical
G1 proof or cold-build all third-party sources for routine fixes. Full offline
and necessary sanitizer checks belong at milestone/release closeout. Keep
build/cache output under ignored directories.

Update current state, milestone status and evidence with each milestone. Push
reviewable commits; merge completed PRs before removing their remote branch.
Never discard unmerged work. Keep main as the only long-lived GitHub branch;
retain merged PR/history records.
