from __future__ import annotations

import asyncio
from decimal import Decimal
import importlib.util
import sys
import time
from pathlib import Path

from binance_market_data.common.v1 import enums_pb2, metadata_pb2
from binance_market_data.gateway.v1 import gateway_messages_pb2
from binance_market_data.market.v1 import reference_data_pb2

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_PATH = REPO_ROOT / "examples" / "consumer" / "minimal_client.py"
SPEC = importlib.util.spec_from_file_location("minimal_client", EXAMPLE_PATH)
assert SPEC is not None and SPEC.loader is not None
example_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = example_module
SPEC.loader.exec_module(example_module)


def test_request_builders_and_level_application() -> None:
    ob_req = example_module.build_order_book_request(
        enums_pb2.MARKET_USD_M_PERPETUAL, "BTCUSDT", "req-ob"
    )
    assert ob_req.symbol == "BTCUSDT"
    assert ob_req.market == enums_pb2.MARKET_USD_M_PERPETUAL
    assert list(ob_req.supported_snapshot_schema_versions) == [
        "local-order-book-snapshot.v1"
    ]
    assert list(ob_req.supported_update_schema_versions) == ["depth-update.v1"]

    ev_req = example_module.build_event_request(
        enums_pb2.MARKET_USD_M_PERPETUAL,
        "BTCUSDT",
        "reference_data",
        "req-ref",
    )
    assert ev_req.selectors[0].stream == enums_pb2.STREAM_REFERENCE_DATA
    assert list(ev_req.supported_payload_schema_versions) == [
        "reference-observation.v1"
    ]

    side: dict[Decimal, str] = {Decimal("90000.0"): "1.5"}
    lvl_remove = metadata_pb2.PriceLevel(price="90000.00", quantity="0.000")
    lvl_add = metadata_pb2.PriceLevel(price="90010.0", quantity="2.25")
    example_module.apply_levels(side, [lvl_remove, lvl_add])
    assert side == {Decimal("90010.0"): "2.25"}


def test_reference_staleness_rules() -> None:
    now_ns = 2_000_000_000_000_000_000
    now_ms = now_ns // 1_000_000

    fresh_oi = reference_data_pb2.ReferenceObservation(
        source_kind=enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
        receive_time_utc_ns=now_ns - 100 * 1_000_000_000,
        source_time_ms=now_ms - 850 * 1_000,
    )
    assert example_module._is_reference_stale(fresh_oi, now_ns) is False

    stale_oi = reference_data_pb2.ReferenceObservation(
        source_kind=enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
        receive_time_utc_ns=now_ns - 100 * 1_000_000_000,
        source_time_ms=now_ms - 1_250 * 1_000,
    )
    assert example_module._is_reference_stale(stale_oi, now_ns) is True

    instrument = reference_data_pb2.ReferenceObservation(
        source_kind=enums_pb2.REFERENCE_SOURCE_KIND_INSTRUMENT,
        receive_time_utc_ns=now_ns - 12 * 3600 * 1_000_000_000,
    )
    assert example_module._is_reference_stale(instrument, now_ns) is False

    baseline = reference_data_pb2.ReferenceObservation(
        source_kind=enums_pb2.REFERENCE_SOURCE_KIND_DEPTH_BASELINE,
        receive_time_utc_ns=1,
    )
    assert example_module._is_reference_stale(baseline, now_ns) is False


def test_consume_order_book_and_events_handles_gap_and_reference_error() -> None:
    async def _exercise() -> None:
        now_ns = time.time_ns()

        class FakeStub:
            def __init__(self) -> None:
                self.ob_calls = 0

            def SubscribeOrderBook(self, _req):
                self.ob_calls += 1

                async def _gen():
                    if self.ob_calls == 1:
                        gap = gateway_messages_pb2.OrderBookStreamItem()
                        gap.consumer_gap.reason = (
                            enums_pb2.CONSUMER_GAP_REASON_SLOW_CONSUMER
                        )
                        yield gap
                        return
                    snap = gateway_messages_pb2.OrderBookStreamItem()
                    snap.snapshot.synchronized = True
                    snap.snapshot.bids.add(price="100.0", quantity="1.0")
                    snap.snapshot.asks.add(price="101.0", quantity="2.0")
                    yield snap
                    upd = gateway_messages_pb2.OrderBookStreamItem()
                    upd.depth_update.bids.add(price="100.5", quantity="3.0")
                    yield upd
                    await asyncio.sleep(5.0)

                return _gen()

            def SubscribeEvents(self, req):
                stream = req.selectors[0].stream

                async def _gen():
                    acc = gateway_messages_pb2.GatewayEventEnvelope()
                    acc.subscription_accepted.subscription_id = "sub-1"
                    yield acc
                    if stream == enums_pb2.STREAM_REFERENCE_DATA:
                        ok_ref = gateway_messages_pb2.GatewayEventEnvelope()
                        ok_ref.reference_observation.source_kind = (
                            enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST
                        )
                        ok_ref.reference_observation.status = (
                            enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID
                        )
                        ok_ref.reference_observation.receive_time_utc_ns = now_ns
                        ok_ref.reference_observation.source_time_ms = (
                            now_ns // 1_000_000
                        )
                        yield ok_ref
                        err_ref = gateway_messages_pb2.GatewayEventEnvelope()
                        err_ref.reference_observation.source_kind = (
                            enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST
                        )
                        err_ref.reference_observation.status = (
                            enums_pb2.REFERENCE_OBSERVATION_STATUS_ERROR
                        )
                        err_ref.reference_observation.error_code = "http_500"
                        yield err_ref
                    await asyncio.sleep(5.0)

                return _gen()

        stub = FakeStub()
        stop = asyncio.Event()
        stats = {
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
        ob_task = asyncio.create_task(
            example_module.consume_order_book(
                stub, enums_pb2.MARKET_USD_M_PERPETUAL, "BTCUSDT", stop, stats
            )
        )
        ev_task = asyncio.create_task(
            example_module.consume_events(
                stub,
                enums_pb2.MARKET_USD_M_PERPETUAL,
                "BTCUSDT",
                "reference_data",
                stop,
                stats,
            )
        )
        await asyncio.sleep(0.15)
        stop.set()
        ob_task.cancel()
        ev_task.cancel()
        await asyncio.gather(ob_task, ev_task, return_exceptions=True)

        assert stats["book_resubscribes"] == 1
        assert stats["book_snapshots"] == 1
        assert stats["book_updates"] == 1
        assert stats["best_bid"] == "100.5"
        assert stats["best_ask"] == "101.0"
        assert "reference_data" in stats["accepted_streams"]
        assert "REFERENCE_SOURCE_KIND_OPEN_INTEREST" in stats["reference_last_ok"]
        assert (
            stats["reference_errors"]["REFERENCE_SOURCE_KIND_OPEN_INTEREST"]
            == "http_500"
        )

    asyncio.run(_exercise())
