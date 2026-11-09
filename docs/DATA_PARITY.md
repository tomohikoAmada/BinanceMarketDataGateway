# BMDG ↔ BMDR data parity

Updated: 2026-11-10. This document defines the research/live data contract
between **BMDG** (Binance Market Data Gateway) and **BMDR** (Binance Market
Data Recorder, Python) / **S-BMDR** (C++ rewrite).

## Roles

```text
Binance public streams/REST
  ├─ BMDR / S-BMDR: raw archive + offline normalize/replay/candles/research
  └─ BMDG: normalized live order book + events + status over gRPC
```

- **BMDR / S-BMDR** preserve exact raw payloads, REST bodies, provenance,
  receive time, sequence fields and capture flags. C++ S-BMDR is the
  production collector; the frozen Python baseline is the offline reader,
  normalizer, replay and K-line tool.
- **BMDG** owns live transport, timestamps, recovery, Projection book
  maintenance, bounded publication and gRPC. It does not persist history.
- **Projection** is the shared order-book engine. It owns fixed-point numbers,
  L2 book state, Spot/USD-M sequence policy, gap detection and quality flags.
- **Contracts** owns the protobuf/wire schema.

## Current coverage

| Data | BMDR / S-BMDR | BMDG | Parity |
|---|---|---|---|
| Spot depth@100ms | recorded | order book + depth event | aligned |
| Spot bookTicker | recorded | event | aligned |
| Spot aggTrade | recorded | event | aligned |
| USD-M depth@100ms | recorded | order book + depth event | aligned |
| USD-M bookTicker | recorded | not provided | **gap** |
| USD-M aggTrade | recorded | not provided | **gap** |
| USD-M markPrice@1s | recorded | not provided | **gap** |
| USD-M forceOrder | recorded | not provided | **gap** |
| exchangeInfo | raw REST recorded | internal startup use, not published | research-only |
| depth snapshot | raw REST recorded | internal bootstrap use, not published | research-only |
| OI / long-short ratios / taker volume | recorded | not provided | research-only for v1 |
| raw payload bytes | preserved | normalized protobuf only | research-only |
| gap/capture flags | `capture_flags`, `sequence_gap`, chunk incomplete | Projection state, ConsumerGap, new generation | semantic mapping needed |

Current BMDG is therefore **not yet a live superset of BMDR**. Spot is
aligned; USD-M is depth-only; REST side data is absent. Until the phases below
close, fields BMDG does not publish are **research-only** and must not drive a
live strategy signal.

## Contract parity

| Semantic | BMDR | BMDG |
|---|---|---|
| market | `spot` / `um_perpetual` | `MARKET_SPOT` / `MARKET_USD_M_PERPETUAL` |
| symbol | exact uppercase | exact `MarketKey.symbol` |
| receive UTC | `receive_time_utc_ns` | `receive_time_utc_ns` |
| receive monotonic | `receive_monotonic_ns` | `receive_monotonic_ns` |
| exchange event time | `exchange_event_time` (ms value) | `exchange_event_time_ms` |
| USD-M transaction time | `exchange_transaction_time` | `exchange_transaction_time_ms` |
| trade time | `exchange_trade_time` | `exchange_trade_time_ms` |
| depth sequence | `source_sequence.U/u/pu` | `first_update_id` / `final_update_id` / `previous_final_update_id` |
| raw payload | `raw_payload` + encoding | not published |
| connection identity | `connection_id`, `collector_instance_id` | `connection_id`, `connection_generation`, `gateway_instance_id` |
| session identity | none | `subscription_id`, `session_sequence`, `publish_time_*` |

Fields are semantically aligned for overlapping streams, but names, units and
session metadata differ. `publish_time_utc_ns` is not a receive time and must
not be compared with BMDR `receive_time_utc_ns`.

## Projection-computable data

Projection currently computes:

- exact decimal parse/format and `NumericSpec`;
- L2 book state, best bid/ask, quantity-at-price, top-N/all levels;
- Spot `U/u` and USD-M `U/u/pu` sequence/gap classification;
- projection lifecycle (`AwaitingBaseline`, `AwaitingBridge`,
  `Synchronized`, `NeedsResync`);
- `last_update_id`, `last_gap`, `GapInfo`;
- `LocalOrderBookSnapshot` levels, `synchronized`, `quality_flags`,
  depth limit and gap descriptor.

Projection does **not** currently compute mid, spread, microprice, mark price,
index price, funding, open interest, VWAP or imbalance. M2 explicitly excludes
derived prices; M4 defers `MarketStateSnapshot`.

BMDG exposes `LocalOrderBookSnapshot` through `SubscribeOrderBook` (including
`depth_limit`, `synchronized` and `quality_flags`). It does not expose an
explicit best-bid/ask, mid/spread/microprice, `last_gap` details in
`GetGatewayStatus`, or `data_freshness_ms`. `SubscribeMarketState` remains
unimplemented.

## Live contract target

Define **BMDG Live Contract v1** as the only live source for strategies:

1. `SubscribeOrderBook`: Projection-Applied book, snapshot + diffs, depth
   limit, `last_update_id`, quality, gap.
2. `SubscribeEvents`: add USD-M `BOOK_TICKER`, `AGG_TRADE`, `MARK_PRICE`,
   `LIQUIDATION` (requires Contracts messages/enums and two USD-M WS routes).
3. `SubscribeMarketState`: implement `MarketStateSnapshot` for:
   - best bid/ask + quantities;
   - top-N bids/asks;
   - mid/spread/microprice with an explicit exact-arithmetic/rounding policy;
   - `source_book_update_id`, `source_trade_id`;
   - `book_synchronized`, quality, gap, `data_freshness_ms`.
4. `GetGatewayStatus`: add gap/quality/freshness details needed by monitoring.
5. REST reference data (OI, long/short, taker): **research-only for Live
   Contract v1**. Do not add it to a live strategy until BMDG publishes a
   bounded polling path with explicit freshness and staleness semantics.

Projection Core should stay pure. Put mid/spread/microprice and
`MarketStateSnapshot` composition in a dedicated builder above Core (Adapter or
BMDG), with exact fixed-point/rational semantics. Mark price, index price,
funding and OI require exchange data, not book derivation.

## Parity test

1. Record a window with BMDR/S-BMDR.
2. Replay it through the offline normalizer.
3. Run BMDG against the same symbols/window and record gRPC output.
4. Align by `(market, symbol, stream, U/u/pu)` or `lastUpdateId`.
5. Compare event counts/order, timestamps, book hash at the same update ID,
   gap/quality mapping and freshness.
6. Fail if any BMDR live-required field is missing from BMDG.

## Delivery phases

- **Phase 1**: Contracts + BMDG USD-M five-stream parity; parity test for
  depth/bookTicker/aggTrade/markPrice/forceOrder.
- **Phase 2**: `MarketStateSnapshot` / `SubscribeMarketState` for
  Projection-computable fields; exact mid/spread/microprice policy.
- **Phase 3**: promote REST reference data only if live strategies require
  it; add bounded polling publication with freshness/staleness semantics.
- **Phase 4**: Full BMDR ↔ BMDG replay/live parity campaign on the target VPS.

Until these phases close, research must not use a BMDR-only live-required
field for production signals. In particular, USD-M bookTicker/aggTrade/
markPrice/forceOrder and all REST side data are research-only until BMDG
publishes them.
