# BMDG ↔ BMDR data parity

Updated: 2026-11-13. This document defines the research/live data contract
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
| USD-M bookTicker | recorded | event | implemented; fixture/wire parity pending R4 |
| USD-M aggTrade | recorded | event with optional `nq` | implemented; fixture/wire parity pending R4 |
| USD-M markPrice@1s | recorded | event with next funding time and optional `ap` | implemented; fixture/wire parity pending R4 |
| USD-M forceOrder | recorded | typed liquidation event | implemented; fixture/wire parity pending R4 |
| exchangeInfo | raw REST recorded | typed instrument definition (24 h refresh) | implemented; fixture/wire parity pending R4 |
| depth snapshot | raw REST recorded | typed depth-baseline reference observation | implemented; fixture/wire parity pending R4 |
| OI / long-short ratios / taker volume | recorded | typed ReferenceObservation, 300 s polling | implemented; fixture/wire parity pending R4 |
| raw payload bytes | preserved | normalized protobuf only | research-only |
| gap/capture flags | `capture_flags`, `sequence_gap`, chunk incomplete | Projection state, ConsumerGap, new generation | semantic mapping needed |

BMDG now implements the five USD-M WS families plus the five periodic REST
sources, exchangeInfo and depth-baseline observations. Raw payload bytes remain
a Recorder-only capability; BMDG publishes normalized messages. G7 must still
run the joint replay/live comparison before claiming full parity for every
field.

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
2. `SubscribeEvents`: USD-M `BOOK_TICKER`, `AGG_TRADE`, `MARK_PRICE`,
   `LIQUIDATION` and typed `REFERENCE_DATA` are implemented through two
   product-local USD-M WS connections.
3. REST reference data (OI, long/short, taker, exchangeInfo, depth baseline)
   is implemented as bounded `REFERENCE_DATA` observations; clients retain
   source/receive times and apply the 600 s/48 h staleness rules.
4. `SubscribeMarketState` remains unimplemented; mid/spread/microprice and
   `data_freshness_ms` are not part of this delivery.
5. `GetGatewayStatus` keeps its existing fields; per-source reference health is
   carried by typed observation status rather than a new status framework.

Projection Core should stay pure. Put mid/spread/microprice and
`MarketStateSnapshot` composition in a dedicated builder above Core (Adapter or
BMDG), with exact fixed-point/rational semantics. Mark price, index price,
funding and OI require exchange data, not book derivation.

## Parity test

Deterministic C++ decode entrypoints for cross-repo `R4 vs G7` same-input
fixture comparison:

- Spot WS: `protocol::parse_spot_depth_frame`, `protocol::parse_spot_combined_frame`
  (`src/protocol/spot_protocol.hpp`)
- USD-M WS: `protocol::parse_usdm_depth_frame`, `protocol::parse_usdm_public_frame`,
  `protocol::parse_usdm_market_frame`, `protocol::parse_usdm_depth_snapshot`
  (`src/protocol/usdm_protocol.hpp`)
- USD-M Reference REST: `transport::parse_usdm_reference_payload`,
  `transport::parse_usdm_instrument_definitions_batch`,
  `transport::parse_usdm_instrument_definition` (`src/transport/usdm_reference.hpp`)

Full parity steps:

1. Record a window with BMDR/S-BMDR.
2. Replay it through the offline normalizer (and compare normalized fields on
   identical WS/REST fixtures using the deterministic entrypoints above).
3. Run BMDG against the same symbols/window and record gRPC output.
4. Align by `(market, symbol, stream, U/u/pu)` or `lastUpdateId`.
5. Compare event counts/order, timestamps, book hash at the same update ID,
   gap/quality mapping and freshness.
6. Fail if any BMDR live-required field is missing from BMDG.

## Delivery phases

- **Phase 1 (implemented)**: Contracts C2 and BMDG USD-M five-stream support.
- **Phase 2 (implemented)**: BMDG periodic REST reference observations,
  exchangeInfo refresh and depth baseline publication with explicit status.
- **Phase 3**: G7 joint BMDR ↔ BMDG replay/live parity campaign on the target
  VPS, including staleness and field-level comparison.
- **Phase 4**: only if required, add `MarketStateSnapshot` derived fields with
  an explicit exact-arithmetic/rounding policy.

Research must not treat a warming, EMPTY, ERROR or stale reference observation
as fresh, and must not use raw payload bytes as if BMDG published them.
