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


def test_configured_profile_uses_five_pairs_without_hardcoded_ten() -> None:
    spot = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    usdm = list(spot)
    streams = consumer_module.build_streams(
        {"spot_symbols": spot, "usdm_symbols": usdm}, "configured"
    )
    assert len(streams) == 45
    assert sum(1 for stream in streams if stream.stream == "order_book") == 10
    assert sum(1 for stream in streams if stream.stream == "agg_trade") == 10
    assert sum(1 for stream in streams if stream.stream == "book_ticker") == 10
    assert sum(1 for stream in streams if stream.stream == "mark_price") == 5
    assert sum(1 for stream in streams if stream.stream == "liquidation") == 5
    assert sum(1 for stream in streams if stream.stream == "reference_data") == 5


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


def test_filter_streams_partitions_live40() -> None:
    spot = [f"SPOT{i}USDT" for i in range(10)]
    usdm = [f"USD{i}USDT" for i in range(10)]
    streams = consumer_module.build_streams(
        {"spot_symbols": spot, "usdm_symbols": usdm}, "live40"
    )
    partitions = {
        stream_filter: consumer_module.filter_streams(streams, stream_filter)
        for stream_filter in ("order_book", "agg_trade", "book_ticker")
    }
    assert len(partitions["order_book"]) == 20
    assert len(partitions["agg_trade"]) == 10
    assert len(partitions["book_ticker"]) == 10
    keys = [stream.key for shard in partitions.values() for stream in shard]
    assert sorted(keys) == sorted(stream.key for stream in streams)
    assert len(set(keys)) == 40
    assert consumer_module.filter_streams(streams, "all") == streams
    events = consumer_module.filter_streams(streams, "spot_events")
    assert len(events) == 20
    one = consumer_module.filter_streams(streams, "spot_events", spot[0])
    assert len(one) == 2
    assert {stream.stream for stream in one} == {"agg_trade", "book_ticker"}


def test_supervisor_merge_sums_shards_and_sorts() -> None:
    supervisor = object.__new__(consumer_module.Supervisor)
    streams, totals = supervisor._merge(
        [
            {
                "streams": [
                    {"key": "b", "stream": "agg_trade", "snapshot_count": 0},
                    {"key": "a", "stream": "agg_trade", "snapshot_count": 0},
                ],
                "totals": {
                    "streams": 2,
                    "accepted": 2,
                    "snapshots": 0,
                    "updates": 0,
                    "events": 5,
                },
            },
            {
                "streams": [
                    {"key": "c", "stream": "order_book", "snapshot_count": 3}
                ],
                "totals": {
                    "streams": 1,
                    "accepted": 1,
                    "snapshots": 3,
                    "updates": 4,
                    "events": 0,
                },
            },
        ]
    )
    assert [stream["key"] for stream in streams] == ["a", "b", "c"]
    assert totals["streams"] == 3
    assert totals["accepted"] == 3
    assert totals["snapshots"] == 3
    assert totals["updates"] == 4
    assert totals["events"] == 5
    assert totals["protocol_errors"] == 0


async def _run_supervisor_case(tmp_path: Path, duration: float = 3.0):
    spot = [f"SPOT{i}USDT" for i in range(10)]
    usdm = [f"USD{i}USDT" for i in range(10)]
    products = [(SPOT, symbol) for symbol in spot] + [
        (USDM, symbol) for symbol in usdm
    ]
    server = grpc.aio.server()
    service = FakeGateway("normal", products)
    gateway_service_pb2_grpc.add_BinanceMarketDataGatewayServiceServicer_to_server(
        service, server
    )
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({"spot_symbols": spot, "usdm_symbols": usdm}),
        encoding="utf-8",
    )
    config = json.loads(config_path.read_text(encoding="utf-8"))
    streams = consumer_module.build_streams(config, "live40")
    output = tmp_path / "summary.json"
    supervisor = consumer_module.Supervisor(
        target=f"127.0.0.1:{port}",
        config_path=config_path,
        profile="live40",
        streams=streams,
        output_path=output,
        duration_seconds=duration,
        summary_interval_seconds=0.2,
        status_interval_seconds=0.1,
        retry_delays=(0.01, 0.02, 0.04),
        install_signals=False,
    )
    try:
        code = await supervisor.run()
    finally:
        await server.stop(0)
    return code, json.loads(output.read_text(encoding="utf-8"))


def test_supervisor_shards_end_to_end(tmp_path: Path) -> None:
    code, summary = asyncio.run(_run_supervisor_case(tmp_path))
    assert code == 0
    assert summary["ok"] is True
    assert summary["totals"]["streams"] == 40
    assert summary["totals"]["accepted"] == 40
    assert summary["totals"]["valid"] == 40
    assert summary["totals"]["snapshots"] == 20
    assert summary["totals"]["updates"] == 20
    assert summary["totals"]["events"] == 20
    assert summary["totals"]["protocol_errors"] == 0
    assert summary["totals"]["gaps"] == 0
    assert len(summary["status"]["seen_keys"]) == 20
    assert summary["status"]["missing_keys"] == []
    assert summary["exit_reason"] == "duration"


def test_quantity_fast_path_rejects_invalid_and_detects_zero() -> None:
    assert consumer_module._quantity_is_zero("0")
    assert consumer_module._quantity_is_zero("0.0000")
    assert not consumer_module._quantity_is_zero("1.0000")
    assert not consumer_module._quantity_is_zero("0.0001")
    for invalid in ("", "-1", "+1", "1e-3", "1.2.3", "."):
        with pytest.raises(consumer_module.ProtocolViolation):
            consumer_module._quantity_is_zero(invalid)


def test_stream_state_eof_clears_book_and_validity() -> None:
    state = consumer_module.StreamState(
        consumer_module.StreamSpec(
            enums_pb2.VENUE_BINANCE, SPOT, "BTCUSDT", "order_book"
        )
    )
    state.current_valid = True
    state.snapshot_seen = True
    state.bids["100.00"] = "1.000"
    state.asks["101.00"] = "2.000"
    state.on_eof()
    assert state.current_valid is False
    assert state.snapshot_seen is False
    assert state.bids == {}
    assert state.asks == {}
    assert state.eof_count == 1


def test_live_stream_status_does_not_clear_ask_side() -> None:
    state = consumer_module.StreamState(
        consumer_module.StreamSpec(
            enums_pb2.VENUE_BINANCE, SPOT, "BTCUSDT", "order_book"
        )
    )
    state.subscription_id = "ev-test"
    state.gateway_instance_id = GATEWAY_INSTANCE_ID
    state.session_accepted = True
    state.current_valid = True
    state.last_session_sequence = 1
    state.bids["100.00"] = "1.000"
    state.asks["101.00"] = "2.000"

    live = gateway_messages_pb2.GatewayEventEnvelope()
    _delivery(live, "ev-test", 2)
    live.stream_status.state = enums_pb2.STREAM_LIFECYCLE_STATE_LIVE
    assert state.consume(live) == "continue"
    assert state.asks == {"101.00": "2.000"}

    closed = gateway_messages_pb2.GatewayEventEnvelope()
    _delivery(closed, "ev-test", 3)
    closed.stream_status.state = enums_pb2.STREAM_LIFECYCLE_STATE_CLOSED
    assert state.consume(closed) == "resubscribe"
    assert state.bids == {}
    assert state.asks == {}


def test_bid_and_ask_sides_do_not_delete_each_other() -> None:
    state = consumer_module.StreamState(
        consumer_module.StreamSpec(
            enums_pb2.VENUE_BINANCE, SPOT, "BTCUSDT", "order_book"
        )
    )
    world = type("Level", (), {"price": "", "quantity": ""})

    def level(price: str, quantity: str):
        value = world()
        value.price = price
        value.quantity = quantity
        return value

    state.bids["100.00"] = "1.000"
    state.asks["101.00"] = "2.000"
    consumer_module._apply_levels(state.bids, [level("101.00", "3.000")])
    consumer_module._apply_levels(
        state.asks,
        [level("101.00", "0"), level("102.00", "4.000")],
    )
    assert state.bids == {"100.00": "1.000", "101.00": "3.000"}
    assert state.asks == {"102.00": "4.000"}
    assert state.book_levels == 3


def test_retry_backoff_resets_after_healthy_session_and_caps() -> None:
    consumer = object.__new__(consumer_module.Consumer)
    consumer.retry_delays = (1.0, 2.0, 4.0)
    assert consumer._next_failure_count(7, True) == 1
    assert consumer._next_failure_count(7, False) == 8
    assert consumer._retry_delay(1) == 1.0
    assert consumer._retry_delay(2) == 2.0
    assert consumer._retry_delay(9) == 4.0


@pytest.mark.asyncio
async def test_healthy_resubscribe_does_not_sleep_retry_delay(tmp_path: Path) -> None:
    stream = consumer_module.StreamSpec(
        enums_pb2.VENUE_BINANCE,
        consumer_module.MARKET_USD_M_PERPETUAL,
        "BTCUSDT",
        "book_ticker",
    )
    state = consumer_module.StreamState(stream)

    class _FakeStub:
        def __init__(self, c: consumer_module.Consumer) -> None:
            self.calls = 0
            self.consumer = c

        def SubscribeEvents(self, request):  # noqa: N802
            self.calls += 1
            attempt = self.calls
            sub_id = f"sub-{attempt}"

            async def _gen():
                yield _accepted_event(request, sub_id)
                yield _event(request, sub_id, 2)

                if attempt == 1:
                    gap = gateway_messages_pb2.GatewayEventEnvelope()
                    _delivery(gap, sub_id, 3)
                    gap.consumer_gap.reason = enums_pb2.CONSUMER_GAP_REASON_SLOW_CONSUMER
                    yield gap
                else:
                    self.consumer.stop_event.set()

            class _Call:
                def __aiter__(self):
                    return _gen()

                def cancel(self):
                    pass

            return _Call()

    sleeps: list[float] = []
    consumer = consumer_module.Consumer(
        target="127.0.0.1:1",
        streams=[stream],
        output_path=tmp_path / "summary.json",
        profile="test",
        retry_delays=(5.0, 10.0),
        install_signals=False,
        enable_status=False,
    )
    consumer._stub = _FakeStub(consumer)

    async def _record_sleep(delay: float) -> None:
        sleeps.append(delay)

    consumer._sleep_or_stop = _record_sleep  # type: ignore[method-assign]
    await consumer._stream_loop(state)
    assert consumer._stub.calls == 2
    assert sleeps == [0.0]


def test_configured_profile_partitions_usdm_side_and_reference() -> None:
    spot = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    usdm = list(spot)
    streams = consumer_module.build_streams(
        {"spot_symbols": spot, "usdm_symbols": usdm}, "configured"
    )
    assert len(streams) == 45
    usdm_events = consumer_module.filter_streams(streams, "usdm_events")
    reference = consumer_module.filter_streams(streams, "reference_data")
    assert len(usdm_events) == 25
    assert len(reference) == 5
    assert {stream.stream for stream in usdm_events} == {
        "agg_trade",
        "book_ticker",
        "mark_price",
        "liquidation",
        "reference_data",
    }
    one_side = consumer_module.filter_streams(
        streams, "usdm_events", "BTCUSDT"
    )
    one_reference = consumer_module.filter_streams(
        streams, "reference_data", "BTCUSDT"
    )
    assert len(one_side) == 5
    assert len(one_reference) == 1
    assert one_reference[0].expected_schema == "reference-observation.v1"

    supervisor = object.__new__(consumer_module.Supervisor)
    supervisor.streams = streams
    shards = supervisor._shards()
    assert len(shards) == 11
    assert not any(name.startswith("reference-") for name, _, _ in shards)
    total_routed = sum(
        len(consumer_module.filter_streams(streams, s_filter, symbol))
        for _, s_filter, symbol in shards
    )
    assert total_routed == 45


def _make_reference_envelope(
    subscription_id: str,
    sequence: int,
    source_kind: int,
    status: int,
    *,
    receive_ns: int,
    source_time_ms: int | None = None,
    error_code: str = "",
    payload_override: str | None = None,
) -> gateway_messages_pb2.GatewayEventEnvelope:
    item = gateway_messages_pb2.GatewayEventEnvelope()
    _delivery(item, subscription_id, sequence)
    obs = item.reference_observation
    obs.schema_version = consumer_module.REFERENCE_OBSERVATION_SCHEMA
    obs.venue = enums_pb2.VENUE_BINANCE
    obs.market = USDM
    obs.symbol = "BTCUSDT"
    obs.request_id = f"req-{sequence}"
    obs.source_kind = source_kind
    obs.status = status
    obs.receive_time_utc_ns = receive_ns
    if source_time_ms is not None:
        obs.source_time_ms = source_time_ms
    if error_code:
        obs.error_code = error_code
    if status == enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID:
        payload_name = (
            payload_override
            or consumer_module.REFERENCE_SOURCE_PAYLOADS[source_kind]
        )
        if payload_name == "open_interest":
            obs.open_interest.open_interest = "10.0"
        elif payload_name == "top_long_short_account_ratio":
            obs.top_long_short_account_ratio.long_short_ratio = "1.1"
        elif payload_name == "top_long_short_position_ratio":
            obs.top_long_short_position_ratio.long_short_ratio = "1.2"
        elif payload_name == "global_long_short_account_ratio":
            obs.global_long_short_account_ratio.long_short_ratio = "1.3"
        elif payload_name == "taker_buy_sell_volume":
            obs.taker_buy_sell_volume.buy_sell_ratio = "0.9"
        elif payload_name == "instrument":
            obs.instrument.symbol_present = True
            obs.instrument.trading_status = "TRADING"
        elif payload_name == "depth_baseline":
            obs.depth_baseline.symbol = "BTCUSDT"
            obs.depth_baseline.last_update_id = 100
    return item


def _new_reference_state() -> consumer_module.StreamState:
    spec = consumer_module.StreamSpec(
        consumer_module.VENUE_BINANCE, USDM, "BTCUSDT", "reference_data"
    )
    state = consumer_module.StreamState(spec)
    state.gateway_instance_id = GATEWAY_INSTANCE_ID
    state.subscription_id = "ref-sub"
    state.session_accepted = True
    state.accepted_count = 1
    state.last_session_sequence = 1
    return state


def test_reference_source_kind_payload_mismatch_raises() -> None:
    state = _new_reference_state()
    bad = _make_reference_envelope(
        "ref-sub",
        2,
        enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
        enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID,
        receive_ns=1_000_000_000,
        payload_override="instrument",
    )
    with pytest.raises(consumer_module.ProtocolViolation):
        state.consume(bad)


def test_reference_coverage_error_empty_and_staleness(tmp_path: Path) -> None:
    now_ns = 1_700_000_000 * 1_000_000_000
    now_ms = now_ns // 1_000_000

    # ERROR-only -> _compute_ok False in both Consumer and Supervisor
    state = _new_reference_state()
    state.consume(
        _make_reference_envelope(
            "ref-sub",
            2,
            enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
            enums_pb2.REFERENCE_OBSERVATION_STATUS_ERROR,
            receive_ns=now_ns,
            error_code="http-429",
        )
    )
    consumer = consumer_module.Consumer(
        target="127.0.0.1:1",
        streams=[state.spec],
        output_path=tmp_path / "summary.json",
        profile="test",
        install_signals=False,
        enable_status=False,
    )
    consumer.states = [state]
    assert consumer._compute_ok(current_ns=now_ns) is False

    # Feed VALID for OI, INSTRUMENT, DEPTH_BASELINE, and EMPTY for the 4 ratio/taker stats
    seq = 3
    for kind in (
        enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
        enums_pb2.REFERENCE_SOURCE_KIND_INSTRUMENT,
        enums_pb2.REFERENCE_SOURCE_KIND_DEPTH_BASELINE,
    ):
        rx = (
            now_ns - 3_600 * 1_000_000_000
            if kind == enums_pb2.REFERENCE_SOURCE_KIND_DEPTH_BASELINE
            else now_ns
        )
        state.consume(
            _make_reference_envelope(
                "ref-sub",
                seq,
                kind,
                enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID,
                receive_ns=rx,
                source_time_ms=now_ms,
            )
        )
        seq += 1
    for kind in consumer_module.EMPTY_ALLOWED_REFERENCE_SOURCES:
        state.consume(
            _make_reference_envelope(
                "ref-sub",
                seq,
                kind,
                enums_pb2.REFERENCE_OBSERVATION_STATUS_EMPTY,
                receive_ns=now_ns,
            )
        )
        seq += 1
    assert consumer._compute_ok(current_ns=now_ns) is True

    # Supervisor agrees with ok=True and checks liquidation 0-events allowed
    supervisor = object.__new__(consumer_module.Supervisor)
    supervisor.fatal_error = None
    supervisor.status_consumer = consumer
    supervisor.streams = [state.spec]
    consumer._status_seen_keys = set(consumer.expected_product_keys)
    consumer._status_identity_mismatches = 0
    summary = state.summary(current_ns=now_ns)
    totals = {"streams": 1, "protocol_errors": 0}
    assert supervisor._compute_ok([summary], totals, {}, current_ns=now_ns) is True

    # Disallowed EMPTY on OPEN_INTEREST fails ok
    state.consume(
        _make_reference_envelope(
            "ref-sub",
            seq,
            enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
            enums_pb2.REFERENCE_OBSERVATION_STATUS_EMPTY,
            receive_ns=now_ns,
        )
    )
    seq += 1
    assert consumer._compute_ok(current_ns=now_ns) is False
    bad_summary = state.summary(current_ns=now_ns)
    assert (
        supervisor._compute_ok([bad_summary], totals, {}, current_ns=now_ns)
        is False
    )

    # Restore fresh OI, then validate 5m settled bucket within source age limit
    # (850s <= 1200s) stays ok while receive age is fresh
    state.consume(
        _make_reference_envelope(
            "ref-sub",
            seq,
            enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
            enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID,
            receive_ns=now_ns,
            source_time_ms=now_ms,
        )
    )
    seq += 1
    state.consume(
        _make_reference_envelope(
            "ref-sub",
            seq,
            enums_pb2.REFERENCE_SOURCE_KIND_TAKER_BUY_SELL_VOLUME,
            enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID,
            receive_ns=now_ns,
            source_time_ms=now_ms - 850_000,
        )
    )
    seq += 1
    assert consumer._compute_ok(current_ns=now_ns) is True

    # Restore VALID OI with genuinely stale source bucket (> 1200s) -> fails ok
    state.consume(
        _make_reference_envelope(
            "ref-sub",
            seq,
            enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
            enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID,
            receive_ns=now_ns,
            source_time_ms=now_ms - 1_205_000,
        )
    )
    seq += 1
    assert consumer._compute_ok(current_ns=now_ns) is False

    # Restore fresh OI, advance now_ns by 605s -> periodic stale fails ok
    state.consume(
        _make_reference_envelope(
            "ref-sub",
            seq,
            enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
            enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID,
            receive_ns=now_ns,
            source_time_ms=now_ms,
        )
    )
    seq += 1
    assert consumer._compute_ok(current_ns=now_ns) is True
    assert (
        consumer._compute_ok(current_ns=now_ns + 605 * 1_000_000_000) is False
    )

    # INSTRUMENT within 48h stays ok when periodic is fresh, fails > 48h
    instrument_old_ns = now_ns - 49 * 3600 * 1_000_000_000
    state.consume(
        _make_reference_envelope(
            "ref-sub",
            seq,
            enums_pb2.REFERENCE_SOURCE_KIND_INSTRUMENT,
            enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID,
            receive_ns=instrument_old_ns,
            source_time_ms=now_ms,
        )
    )
    assert consumer._compute_ok(current_ns=now_ns) is False


def test_zero_force_orders_does_not_fail_liquidation_stream(tmp_path: Path) -> None:
    spec = consumer_module.StreamSpec(
        consumer_module.VENUE_BINANCE,
        consumer_module.MARKET_USD_M_PERPETUAL,
        "BTCUSDT",
        "liquidation",
    )
    state = consumer_module.StreamState(spec)
    state.accepted_count = 1
    state.current_valid = True
    consumer = consumer_module.Consumer(
        target="127.0.0.1:1",
        streams=[spec],
        output_path=tmp_path / "summary.json",
        profile="test",
        install_signals=False,
        enable_status=False,
    )
    consumer.states = [state]
    assert consumer._compute_ok() is True

    supervisor = object.__new__(consumer_module.Supervisor)
    supervisor.fatal_error = None
    supervisor.status_consumer = consumer
    supervisor.streams = [spec]
    consumer._status_seen_keys = set(consumer.expected_product_keys)
    consumer._status_identity_mismatches = 0
    assert (
        supervisor._compute_ok(
            [state.summary()], {"streams": 1, "protocol_errors": 0}, {}
        )
        is True
    )
