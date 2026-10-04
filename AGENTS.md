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

G0-G11, G12-A and G12-B are complete. This merge completes G12-C configured
production composition and the review fixes. M3 / G12-D four-product offline
production acceptance and production CI is next; M4 / G12-E bounded live
acceptance and delivery follows it. Do not label future acceptance complete
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
build/run its offline CTest groups, ASan/UBSan/TSan configurations and
`scripts/format-check.sh`. Existing GitHub Foundation checks alone do not verify
production changes. Keep build/cache output under ignored directories.

Update current state, milestone status and evidence with each milestone. Push
reviewable commits; merge completed PRs before removing their remote branch.
Never discard unmerged work. Keep main as the only long-lived GitHub branch;
retain merged PR/history records.
