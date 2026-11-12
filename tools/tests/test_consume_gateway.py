from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any

import grpc
import pytest
from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import (
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)

TOOLS = Path(__file__).resolve().parents[1]
SCRIPT = TOOLS / "consume_gateway.py"
SPEC = importlib.util.spec_from_file_location("consume_gateway", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
consumer_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = consumer_module
SPEC.loader.exec_module(consumer_module)

GATEWAY_INSTANCE_ID = "gw-test"
PROTOCOL_VERSION = "bmdg-v1"
SPOT = enums_pb2.MARKET_SPOT
USDM = enums_pb2.MARKET_USD_M_PERPETUAL


def _delivery(item: Any, subscription_id: str, sequence: int) -> None:
    delivery = item.delivery_metadata
    delivery.protocol_version = PROTOCOL_VERSION
    delivery.gateway_instance_id = GATEWAY_INSTANCE_ID
    delivery.subscription_id = subscription_id
    delivery.session_sequence = sequence
    delivery.publish_time_utc_ns = time.time_ns()


def _accepted_order_book(
    request: gateway_messages_pb2.OrderBookSubscriptionRequest,
    subscription_id: str,
) -> gateway_messages_pb2.OrderBookStreamItem:
    item = gateway_messages_pb2.OrderBookStreamItem()
    _delivery(item, subscription_id, 1)
    accepted = item.subscription_accepted
    accepted.request_id = request.request_id
    accepted.subscription_id = subscription_id
    accepted.schema_version = "subscription-accepted.v1"
    accepted.gateway_instance_id = GATEWAY_INSTANCE_ID
    accepted.accepted_time_utc_ns = time.time_ns()
    accepted.negotiated_payload_schema_versions.extend(
        [consumer_module.SNAPSHOT_SCHEMA, consumer_module.DEPTH_SCHEMA]
    )
    return item


def _accepted_event(
    request: gateway_messages_pb2.EventSubscriptionRequest,
    subscription_id: str,
) -> gateway_messages_pb2.GatewayEventEnvelope:
    item = gateway_messages_pb2.GatewayEventEnvelope()
    _delivery(item, subscription_id, 1)
    accepted = item.subscription_accepted
    accepted.request_id = request.request_id
    accepted.subscription_id = subscription_id
    accepted.schema_version = "subscription-accepted.v1"
    accepted.gateway_instance_id = GATEWAY_INSTANCE_ID
    accepted.accepted_time_utc_ns = time.time_ns()
    selector = request.selectors[0]
    schema = (
        consumer_module.AGG_TRADE_SCHEMA
        if selector.stream == enums_pb2.STREAM_AGG_TRADE
        else consumer_module.BOOK_TICKER_SCHEMA
    )
    accepted.negotiated_payload_schema_versions.append(schema)
    return item


def _snapshot(
    request: gateway_messages_pb2.OrderBookSubscriptionRequest,
    subscription_id: str,
    sequence: int = 2,
) -> gateway_messages_pb2.OrderBookStreamItem:
    item = gateway_messages_pb2.OrderBookStreamItem()
    _delivery(item, subscription_id, sequence)
    snapshot = item.snapshot
    snapshot.venue = enums_pb2.VENUE_BINANCE
    snapshot.market = request.market
    snapshot.symbol = request.symbol
    snapshot.schema_version = consumer_module.SNAPSHOT_SCHEMA
    snapshot.producer = "fake-gateway"
    snapshot.producer_version = "test"
    snapshot.source = enums_pb2.SNAPSHOT_SOURCE_GATEWAY_LIVE
    snapshot.last_update_id = 100
    snapshot.generated_time_utc_ns = time.time_ns()
    snapshot.synchronized = True
    bid = snapshot.bids.add()
    bid.price = "100.00"
    bid.quantity = "1.000"
    ask = snapshot.asks.add()
    ask.price = "101.00"
    ask.quantity = "2.000"
    return item


def _depth(
    request: gateway_messages_pb2.OrderBookSubscriptionRequest,
    subscription_id: str,
    sequence: int,
    first_update_id: int,
    final_update_id: int,
) -> gateway_messages_pb2.OrderBookStreamItem:
    item = gateway_messages_pb2.OrderBookStreamItem()
    _delivery(item, subscription_id, sequence)
    update = item.depth_update
    update.metadata.venue = enums_pb2.VENUE_BINANCE
    update.metadata.market = request.market
    update.metadata.symbol = request.symbol
    update.metadata.producer = "fake-gateway"
    update.metadata.producer_version = "test"
    update.metadata.connection_id = subscription_id
    update.metadata.stream = enums_pb2.STREAM_DIFF_DEPTH
    update.metadata.schema_version = consumer_module.DEPTH_SCHEMA
    update.metadata.exchange_event_time_ms = int(time.time() * 1000)
    update.metadata.receive_time_utc_ns = time.time_ns()
    update.metadata.receive_monotonic_ns = time.monotonic_ns()
    update.first_update_id = first_update_id
    update.final_update_id = final_update_id
    bid = update.bids.add()
    bid.price = "100.00"
    bid.quantity = "3.000"
    return item


def _event(
    request: gateway_messages_pb2.EventSubscriptionRequest,
    subscription_id: str,
    sequence: int = 2,
) -> gateway_messages_pb2.GatewayEventEnvelope:
    selector = request.selectors[0]
    item = gateway_messages_pb2.GatewayEventEnvelope()
    _delivery(item, subscription_id, sequence)
    if selector.stream == enums_pb2.STREAM_AGG_TRADE:
        event = item.agg_trade
        event.metadata.venue = enums_pb2.VENUE_BINANCE
        event.metadata.market = selector.market
        event.metadata.symbol = selector.symbol
        event.metadata.producer = "fake-gateway"
        event.metadata.producer_version = "test"
        event.metadata.connection_id = subscription_id
        event.metadata.stream = enums_pb2.STREAM_AGG_TRADE
        event.metadata.schema_version = consumer_module.AGG_TRADE_SCHEMA
        event.metadata.exchange_event_time_ms = int(time.time() * 1000)
        event.metadata.receive_time_utc_ns = time.time_ns()
        event.metadata.receive_monotonic_ns = time.monotonic_ns()
        event.aggregate_trade_id = 1
        event.price = "100.00"
        event.quantity = "1.000"
        event.first_trade_id = 1
        event.last_trade_id = 1
        event.trade_time_ms = int(time.time() * 1000)
    else:
        event = item.book_ticker
        event.metadata.venue = enums_pb2.VENUE_BINANCE
        event.metadata.market = selector.market
        event.metadata.symbol = selector.symbol
        event.metadata.producer = "fake-gateway"
        event.metadata.producer_version = "test"
        event.metadata.connection_id = subscription_id
        event.metadata.stream = enums_pb2.STREAM_BOOK_TICKER
        event.metadata.schema_version = consumer_module.BOOK_TICKER_SCHEMA
        event.metadata.exchange_event_time_ms = int(time.time() * 1000)
        event.metadata.receive_time_utc_ns = time.time_ns()
        event.metadata.receive_monotonic_ns = time.monotonic_ns()
        event.update_id = 1
        event.best_bid_price = "100.00"
        event.best_bid_quantity = "1.000"
        event.best_ask_price = "101.00"
        event.best_ask_quantity = "2.000"
    return item


class FakeGateway(
    gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceServicer
):
    def __init__(self, mode: str, products: list[tuple[int, str]]) -> None:
        self.mode = mode
        self.products = products
        self.calls: dict[tuple[str, int, str], int] = {}
        self.active: dict[tuple[str, int, str], int] = {}
        self.max_active: dict[tuple[str, int, str], int] = {}

    def _begin(self, key: tuple[str, int, str]) -> int:
        self.calls[key] = self.calls.get(key, 0) + 1
        self.active[key] = self.active.get(key, 0) + 1
        self.max_active[key] = max(self.max_active.get(key, 0), self.active[key])
        return self.calls[key]

    def _end(self, key: tuple[str, int, str]) -> None:
        self.active[key] -= 1

    async def _hold(self) -> None:
        await asyncio.sleep(10)

    async def SubscribeOrderBook(self, request, context):  # noqa: N802
        key = ("order_book", request.market, request.symbol)
        call_number = self._begin(key)
        subscription_id = f"{key[0]}-{key[2]}-{call_number}"
        try:
            yield _accepted_order_book(request, subscription_id)
            if self.mode == "accepted_only":
                await self._hold()
                return
            if self.mode == "eof_once" and call_number == 1:
                yield _snapshot(request, subscription_id)
                yield _depth(request, subscription_id, 3, 101, 101)
                return
            if self.mode == "sequence_break" and call_number == 1:
                yield _snapshot(request, subscription_id)
                yield _depth(request, subscription_id, 3, 101, 101)
                yield _depth(request, subscription_id, 5, 102, 102)
                return
            if self.mode == "legacy_metadata" and call_number == 1:
                yield _snapshot(request, subscription_id)
                legacy = _depth(request, subscription_id, 3, 101, 101)
                legacy.envelope_metadata.protocol_version = "legacy"
                yield legacy
                return
            yield _snapshot(request, subscription_id)
            yield _depth(request, subscription_id, 3, 101, 101)
            await self._hold()
        finally:
            self._end(key)

    async def SubscribeEvents(self, request, context):  # noqa: N802
        selector = request.selectors[0]
        stream = (
            "agg_trade"
            if selector.stream == enums_pb2.STREAM_AGG_TRADE
            else "book_ticker"
        )
        key = (stream, selector.market, selector.symbol)
        call_number = self._begin(key)
        subscription_id = f"{stream}-{key[2]}-{call_number}"
        try:
            yield _accepted_event(request, subscription_id)
            if self.mode == "accepted_only" and stream == "agg_trade":
                await self._hold()
                return
            yield _event(request, subscription_id)
            await self._hold()
        finally:
            self._end(key)

    async def GetGatewayStatus(self, request, context):  # noqa: N802
        del request, context
        response = gateway_messages_pb2.GatewayStatusSnapshot()
        response.schema_version = "gateway-status-snapshot.v1"
        response.gateway_instance_id = GATEWAY_INSTANCE_ID
        response.observed_time_utc_ns = time.time_ns()
        response.uptime_seconds = 1
        for market, symbol in self.products:
            entry = response.markets.add()
            entry.venue = enums_pb2.VENUE_BINANCE
            entry.market = market
            entry.symbol = symbol
            entry.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
            entry.last_event_utc_ns = time.time_ns()
            entry.connection_generation = 1
            entry.active_subscription_count = 1
        response.total_active_subscriptions = len(self.products)
        return response


async def _run_case(mode: str, tmp_path: Path, duration: float = 0.5):
    products = [(SPOT, "BTCUSDT"), (USDM, "BTCUSDT")]
    server = grpc.aio.server()
    service = FakeGateway(mode, products)
    gateway_service_pb2_grpc.add_BinanceMarketDataGatewayServiceServicer_to_server(
        service, server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    config = {"spot_symbols": ["BTCUSDT"], "usdm_symbols": ["BTCUSDT"]}
    streams = consumer_module.build_streams(config, "test")
    output = tmp_path / "summary.json"
    consumer = consumer_module.Consumer(
        target=f"127.0.0.1:{port}",
        streams=streams,
        output_path=output,
        profile="test",
        duration_seconds=duration,
        summary_interval_seconds=0.1,
        status_interval_seconds=0.05,
        retry_delays=(0.01, 0.02, 0.04),
        install_signals=False,
    )
    try:
        code = await consumer.run()
    finally:
        await server.stop(0)
    return code, json.loads(output.read_text(encoding="utf-8")), service


def test_normal_attach_consumer(tmp_path: Path) -> None:
    code, summary, service = asyncio.run(_run_case("normal", tmp_path))
    assert code == 0
    assert summary["ok"] is True
    assert summary["totals"]["accepted"] == 4
    assert summary["totals"]["snapshots"] == 2
    assert summary["totals"]["updates"] == 2
    assert summary["totals"]["events"] == 2
    assert summary["totals"]["protocol_errors"] == 0
    assert summary["totals"]["resubscriptions"] == 0
    assert len(summary["status"]["seen_keys"]) == 2
    assert summary["status"]["missing_keys"] == []
    assert all(value <= 1 for value in service.max_active.values())


def test_sequence_break_invalidates_and_resubscribes(tmp_path: Path) -> None:
    code, summary, _ = asyncio.run(_run_case("sequence_break", tmp_path))
    assert code == 1
    assert summary["ok"] is False
    assert summary["totals"]["protocol_errors"] >= 1
    assert summary["totals"]["resubscriptions"] >= 1
    assert summary["totals"]["snapshots"] >= 2


def test_legacy_metadata_is_rejected(tmp_path: Path) -> None:
    code, summary, _ = asyncio.run(_run_case("legacy_metadata", tmp_path))
    assert code == 1
    assert summary["ok"] is False
    assert summary["totals"]["protocol_errors"] >= 1
    assert summary["totals"]["resubscriptions"] >= 1


def test_eof_invalidates_old_book_and_waits_for_new_snapshot(
    tmp_path: Path,
) -> None:
    code, summary, service = asyncio.run(_run_case("eof_once", tmp_path))
    assert code == 0
    assert summary["ok"] is True
    assert summary["totals"]["resubscriptions"] >= 1
    assert summary["totals"]["snapshots"] >= 2
    assert all(value <= 1 for value in service.max_active.values())


def test_accepted_without_data_does_not_pass(tmp_path: Path) -> None:
    code, summary, _ = asyncio.run(_run_case("accepted_only", tmp_path))
    assert code == 1
    assert summary["ok"] is False
    order_books = [
        stream
        for stream in summary["streams"]
        if stream["stream"] == "order_book"
    ]
    assert order_books
    assert all(stream["snapshot_count"] == 0 for stream in order_books)


def test_live40_requires_exactly_ten_spot_and_ten_usdm() -> None:
    with pytest.raises(consumer_module.ConfigError):
        consumer_module.build_streams(
            {"spot_symbols": ["BTCUSDT"], "usdm_symbols": ["BTCUSDT"]},
            "live40",
        )


def test_live40_builds_twenty_books_and_twenty_spot_events() -> None:
    spot = [f"SPOT{i}USDT" for i in range(10)]
    usdm = [f"USD{i}USDT" for i in range(10)]
    streams = consumer_module.build_streams(
        {"spot_symbols": spot, "usdm_symbols": usdm}, "live40"
    )
    assert len(streams) == 40
    assert sum(1 for stream in streams if stream.stream == "order_book") == 20
    assert sum(1 for stream in streams if stream.stream == "agg_trade") == 10
    assert sum(1 for stream in streams if stream.stream == "book_ticker") == 10


def test_stream_state_eof_clears_book_and_validity() -> None:
    state = consumer_module.StreamState(
        consumer_module.StreamSpec(
            enums_pb2.VENUE_BINANCE, SPOT, "BTCUSDT", "order_book"
        )
    )
    state.current_valid = True
    state.snapshot_seen = True
    state.book["100.00"] = "1.000"
    state.on_eof()
    assert state.current_valid is False
    assert state.snapshot_seen is False
    assert state.book == {}
    assert state.eof_count == 1
