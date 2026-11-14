#!/usr/bin/env python3
"""Minimal standalone async gRPC client example for BMDG.

Connects to an already-running ``bmdg`` daemon, prints ``GetGatewayStatus``,
maintains a local L2 order book for one symbol, consumes its event/reference
streams, invalidates local state on ``consumer_gap`` / ``CLOSED`` / EOF, and
tracks typed ``REFERENCE_DATA`` coverage.
"""

from __future__ import annotations

import argparse
import asyncio
from decimal import Decimal
import time
from typing import Any, Iterable

import grpc

from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import (
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)

MARKET_MAP = {
    "spot": enums_pb2.MARKET_SPOT,
    "usdm": enums_pb2.MARKET_USD_M_PERPETUAL,
}
EVENT_STREAM_MAP = {
    "agg_trade": (enums_pb2.STREAM_AGG_TRADE, "agg-trade.v1", "agg_trade"),
    "book_ticker": (enums_pb2.STREAM_BOOK_TICKER, "book-ticker.v1", "book_ticker"),
    "mark_price": (enums_pb2.STREAM_MARK_PRICE, "mark-price.v1", "mark_price"),
    "liquidation": (enums_pb2.STREAM_LIQUIDATION, "liquidation.v1", "liquidation"),
    "reference_data": (
        enums_pb2.STREAM_REFERENCE_DATA,
        "reference-observation.v1",
        "reference_observation",
    ),
}
PERIODIC_MAX_RECEIVE_AGE_NS = 600 * 1_000_000_000
PERIODIC_MAX_SOURCE_AGE_MS = 1200 * 1_000
INSTRUMENT_MAX_RECEIVE_AGE_NS = 48 * 3600 * 1_000_000_000


def apply_levels(book_side: dict[Decimal, str], levels: Iterable[Any]) -> None:
    """Apply snapshot or delta levels keyed by exact numeric Decimal price."""
    for level in levels:
        # Snapshot and depth_update format trailing zeros differently (e.g.
        # "67362.8" vs "67362.80"), so key levels by Decimal rather than raw str.
        price = Decimal(level.price)
        if Decimal(level.quantity) == 0:
            book_side.pop(price, None)
        else:
            book_side[price] = level.quantity


def build_order_book_request(market: int, symbol: str, request_id: str) -> Any:
    req = gateway_messages_pb2.OrderBookSubscriptionRequest()
    req.request_id = request_id
    req.schema_version = "order-book-subscription-request.v1"
    req.venue = enums_pb2.VENUE_BINANCE
    req.market = market
    req.symbol = symbol
    req.initial_snapshot_mode = enums_pb2.INITIAL_SNAPSHOT_MODE_REQUIRED
    req.supported_snapshot_schema_versions.append("local-order-book-snapshot.v1")
    req.supported_update_schema_versions.append("depth-update.v1")
    return req


def build_event_request(
    market: int, symbol: str, stream_name: str, request_id: str
) -> Any:
    stream_enum, payload_schema, _ = EVENT_STREAM_MAP[stream_name]
    req = gateway_messages_pb2.EventSubscriptionRequest()
    req.request_id = request_id
    req.schema_version = "event-subscription-request.v1"
    sel = req.selectors.add()
    sel.venue = enums_pb2.VENUE_BINANCE
    sel.market = market
    sel.symbol = symbol
    sel.stream = stream_enum
    req.delivery_mode = enums_pb2.DELIVERY_MODE_CONTIGUOUS_EVENTS
    req.supported_payload_schema_versions.append(payload_schema)
    return req


async def consume_order_book(
    stub: Any, market: int, symbol: str, stop: asyncio.Event, stats: dict[str, Any]
) -> None:
    attempt = 0
    while not stop.is_set():
        attempt += 1
        bids: dict[Decimal, str] = {}
        asks: dict[Decimal, str] = {}
        valid = False
        resubscribe = False
        req = build_order_book_request(market, symbol, f"ex-ob-{symbol}-{attempt}")
        try:
            call = stub.SubscribeOrderBook(req)
            async for item in call:
                payload = item.WhichOneof("payload")
                if payload == "consumer_gap" or (
                    payload == "stream_status"
                    and item.stream_status.state
                    == enums_pb2.STREAM_LIFECYCLE_STATE_CLOSED
                ):
                    resubscribe = True
                    stats["book_resubscribes"] += 1
                    break
                if payload == "snapshot":
                    bids.clear()
                    asks.clear()
                    apply_levels(bids, item.snapshot.bids)
                    apply_levels(asks, item.snapshot.asks)
                    valid = item.snapshot.synchronized
                    stats["book_snapshots"] += 1
                elif payload == "depth_update" and valid:
                    apply_levels(bids, item.depth_update.bids)
                    apply_levels(asks, item.depth_update.asks)
                    stats["book_updates"] += 1
                if bids and asks:
                    stats["best_bid"] = str(max(bids))
                    stats["best_ask"] = str(min(asks))
                if stop.is_set():
                    break
        except grpc.aio.AioRpcError as exc:
            stats["rpc_errors"] += 1
            print(f"[order_book] rpc error {exc.code().name}: {exc.details()}")
        if not stop.is_set() and not resubscribe:
            await asyncio.sleep(1.0)


async def consume_events(
    stub: Any,
    market: int,
    symbol: str,
    stream_name: str,
    stop: asyncio.Event,
    stats: dict[str, Any],
) -> None:
    attempt = 0
    _, _, expected_payload = EVENT_STREAM_MAP[stream_name]
    while not stop.is_set():
        attempt += 1
        req = build_event_request(
            market, symbol, stream_name, f"ex-{stream_name}-{symbol}-{attempt}"
        )
        resubscribe = False
        try:
            async for item in stub.SubscribeEvents(req):
                payload = item.WhichOneof("payload")
                if payload == "subscription_accepted":
                    stats["accepted_streams"].add(stream_name)
                elif payload == "consumer_gap" or (
                    payload == "stream_status"
                    and item.stream_status.state
                    == enums_pb2.STREAM_LIFECYCLE_STATE_CLOSED
                ):
                    resubscribe = True
                    stats["event_resubscribes"] += 1
                    break
                elif payload == "reference_observation":
                    ref = item.reference_observation
                    kind = enums_pb2.ReferenceSourceKind.Name(ref.source_kind)
                    if ref.status == enums_pb2.REFERENCE_OBSERVATION_STATUS_ERROR:
                        # ERROR never overwrites cached VALID values/timestamps.
                        stats["reference_errors"][kind] = ref.error_code
                    else:
                        stats["reference_last_ok"][kind] = {
                            "status": enums_pb2.ReferenceObservationStatus.Name(
                                ref.status
                            ),
                            "receive_time_utc_ns": ref.receive_time_utc_ns,
                            "source_time_ms": (
                                ref.source_time_ms
                                if ref.HasField("source_time_ms")
                                else None
                            ),
                            "stale": _is_reference_stale(ref, time.time_ns()),
                        }
                    stats["events"][stream_name] = (
                        stats["events"].get(stream_name, 0) + 1
                    )
                elif payload == expected_payload:
                    stats["events"][stream_name] = (
                        stats["events"].get(stream_name, 0) + 1
                    )
                if stop.is_set():
                    break
        except grpc.aio.AioRpcError as exc:
            stats["rpc_errors"] += 1
            print(f"[{stream_name}] rpc error {exc.code().name}: {exc.details()}")
        if not stop.is_set() and not resubscribe:
            await asyncio.sleep(1.0)


def _is_reference_stale(ref: Any, now_ns: int) -> bool:
    if ref.receive_time_utc_ns <= 0:
        return True
    if ref.source_kind == enums_pb2.REFERENCE_SOURCE_KIND_DEPTH_BASELINE:
        return False
    if ref.source_kind == enums_pb2.REFERENCE_SOURCE_KIND_INSTRUMENT:
        return (now_ns - ref.receive_time_utc_ns) > INSTRUMENT_MAX_RECEIVE_AGE_NS
    if (now_ns - ref.receive_time_utc_ns) > PERIODIC_MAX_RECEIVE_AGE_NS:
        return True
    if ref.HasField("source_time_ms"):
        now_ms = now_ns // 1_000_000
        return (now_ms - ref.source_time_ms) > PERIODIC_MAX_SOURCE_AGE_MS
    return False


async def run_client(
    target: str, market_name: str, symbol: str, duration_seconds: float
) -> dict[str, Any]:
    market = MARKET_MAP[market_name]
    streams = ["agg_trade", "book_ticker"]
    if market == enums_pb2.MARKET_USD_M_PERPETUAL:
        streams.extend(["mark_price", "liquidation", "reference_data"])

    stop = asyncio.Event()
    stats: dict[str, Any] = {
        "accepted_streams": set(),
        "book_snapshots": 0,
        "book_updates": 0,
        "book_resubscribes": 0,
        "event_resubscribes": 0,
        "rpc_errors": 0,
        "best_bid": None,
        "best_ask": None,
        "events": {},
        "reference_last_ok": {},
        "reference_errors": {},
    }
    async with grpc.aio.insecure_channel(target) as channel:
        stub = gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceStub(channel)
        status_req = gateway_messages_pb2.GatewayStatusRequest(
            request_id="example-status",
            schema_version="gateway-status-request.v1",
        )
        status = await stub.GetGatewayStatus(status_req, timeout=2.0)
        print(
            f"Connected gateway={status.gateway_instance_id} "
            f"active_subs={status.total_active_subscriptions} "
            f"markets={len(status.markets)}"
        )
        tasks = [
            asyncio.create_task(consume_order_book(stub, market, symbol, stop, stats)),
            *(
                asyncio.create_task(
                    consume_events(stub, market, symbol, name, stop, stats)
                )
                for name in streams
            ),
        ]
        try:
            await asyncio.sleep(duration_seconds)
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default="127.0.0.1:50051")
    parser.add_argument("--market", choices=tuple(MARKET_MAP), default="usdm")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration-seconds", type=float, default=15.0)
    args = parser.parse_args()
    stats = asyncio.run(
        run_client(args.target, args.market, args.symbol, args.duration_seconds)
    )
    print(
        f"Summary {args.market}:{args.symbol} "
        f"book_snapshots={stats['book_snapshots']} updates={stats['book_updates']} "
        f"best_bid={stats['best_bid']} best_ask={stats['best_ask']} "
        f"events={stats['events']} refs_ok={sorted(stats['reference_last_ok'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
