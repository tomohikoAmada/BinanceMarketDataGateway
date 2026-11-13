#!/usr/bin/env python3
"""Attach-only BMDG consumer probe.

The probe connects to an already running ``bmdg`` process on a loopback gRPC
address.  It never starts/stops the daemon, never connects to Binance, and
never persists market data.  It maintains a bounded in-memory view of each
order book only to prove the subscription/snapshot/update lifecycle.

The summary is written atomically every ``--summary-interval-seconds`` and at
shutdown.  A finite run exits 0 only when all configured streams received their
required data and no protocol/session violation was observed.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import datetime as dt
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Iterable, Optional

import grpc

from binance_market_data.common.v1 import enums_pb2
from binance_market_data.gateway.v1 import (
    gateway_messages_pb2,
    gateway_service_pb2_grpc,
)

VENUE_BINANCE = enums_pb2.VENUE_BINANCE
MARKET_SPOT = enums_pb2.MARKET_SPOT
MARKET_USD_M_PERPETUAL = enums_pb2.MARKET_USD_M_PERPETUAL
STREAM_AGG_TRADE = enums_pb2.STREAM_AGG_TRADE
STREAM_BOOK_TICKER = enums_pb2.STREAM_BOOK_TICKER
STREAM_DIFF_DEPTH = enums_pb2.STREAM_DIFF_DEPTH
STREAM_LIQUIDATION = enums_pb2.STREAM_LIQUIDATION
STREAM_MARK_PRICE = enums_pb2.STREAM_MARK_PRICE
STREAM_REFERENCE_DATA = enums_pb2.STREAM_REFERENCE_DATA
DELIVERY_MODE_CONTIGUOUS_EVENTS = enums_pb2.DELIVERY_MODE_CONTIGUOUS_EVENTS
INITIAL_SNAPSHOT_MODE_REQUIRED = enums_pb2.INITIAL_SNAPSHOT_MODE_REQUIRED

ORDER_BOOK_REQUEST_SCHEMA = "order-book-subscription-request.v1"
EVENT_REQUEST_SCHEMA = "event-subscription-request.v1"
STATUS_REQUEST_SCHEMA = "gateway-status-request.v1"
SNAPSHOT_SCHEMA = "local-order-book-snapshot.v1"
DEPTH_SCHEMA = "depth-update.v1"
AGG_TRADE_SCHEMA = "agg-trade.v1"
BOOK_TICKER_SCHEMA = "book-ticker.v1"
MARK_PRICE_SCHEMA = "mark-price.v1"
LIQUIDATION_SCHEMA = "liquidation.v1"
REFERENCE_OBSERVATION_SCHEMA = "reference-observation.v1"

REFERENCE_SOURCE_PAYLOADS: dict[int, str] = {
    enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST: "open_interest",
    enums_pb2.REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_ACCOUNT_RATIO: (
        "top_long_short_account_ratio"
    ),
    enums_pb2.REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_POSITION_RATIO: (
        "top_long_short_position_ratio"
    ),
    enums_pb2.REFERENCE_SOURCE_KIND_GLOBAL_LONG_SHORT_ACCOUNT_RATIO: (
        "global_long_short_account_ratio"
    ),
    enums_pb2.REFERENCE_SOURCE_KIND_TAKER_BUY_SELL_VOLUME: (
        "taker_buy_sell_volume"
    ),
    enums_pb2.REFERENCE_SOURCE_KIND_INSTRUMENT: "instrument",
    enums_pb2.REFERENCE_SOURCE_KIND_DEPTH_BASELINE: "depth_baseline",
}
REQUIRED_REFERENCE_SOURCES: tuple[int, ...] = tuple(
    REFERENCE_SOURCE_PAYLOADS.keys()
)
PERIODIC_REFERENCE_SOURCES: frozenset[int] = frozenset(
    {
        enums_pb2.REFERENCE_SOURCE_KIND_OPEN_INTEREST,
        enums_pb2.REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_ACCOUNT_RATIO,
        enums_pb2.REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_POSITION_RATIO,
        enums_pb2.REFERENCE_SOURCE_KIND_GLOBAL_LONG_SHORT_ACCOUNT_RATIO,
        enums_pb2.REFERENCE_SOURCE_KIND_TAKER_BUY_SELL_VOLUME,
    }
)
EMPTY_ALLOWED_REFERENCE_SOURCES: frozenset[int] = frozenset(
    {
        enums_pb2.REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_ACCOUNT_RATIO,
        enums_pb2.REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_POSITION_RATIO,
        enums_pb2.REFERENCE_SOURCE_KIND_GLOBAL_LONG_SHORT_ACCOUNT_RATIO,
        enums_pb2.REFERENCE_SOURCE_KIND_TAKER_BUY_SELL_VOLUME,
    }
)
PERIODIC_REFERENCE_MAX_AGE_SECONDS = 600.0
INSTRUMENT_REFERENCE_MAX_RECEIVE_AGE_SECONDS = 48.0 * 3600.0

RETRY_DELAYS = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
FATAL_RPC_CODES = {
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.FAILED_PRECONDITION,
    grpc.StatusCode.UNIMPLEMENTED,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.UNAUTHENTICATED,
}
CLIENT_CHANNEL_OPTIONS = (
    ("grpc.max_receive_message_length", 16 * 1024 * 1024),
    # Keep the HTTP/2 receive window open while Python handles a burst. This
    # does not change the Gateway's bounded publication queue.
    ("grpc.http2.lookahead_bytes", 8 * 1024 * 1024),
    ("grpc.optimization_target", "throughput"),
)
LATENCY_BUCKETS = (
    ("negative", None, 0.0),
    ("0-1ms", 0.0, 1.0),
    ("1-5ms", 1.0, 5.0),
    ("5-10ms", 5.0, 10.0),
    ("10-50ms", 10.0, 50.0),
    ("50-100ms", 50.0, 100.0),
    ("100-500ms", 100.0, 500.0),
    ("500ms+", 500.0, None),
)


class ConfigError(RuntimeError):
    """Invalid probe configuration; exit without retrying."""


class ProtocolViolation(RuntimeError):
    """The gateway violated the v1 session or payload contract."""


class SequenceViolation(ProtocolViolation):
    """The per-subscription session sequence did not advance by one."""


def now_utc_ns() -> int:
    return time.time_ns()


def iso_utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def iso_utc_from_ns(value_ns: int) -> str:
    if value_ns <= 0:
        return ""
    return dt.datetime.fromtimestamp(
        value_ns / 1_000_000_000.0, tz=dt.timezone.utc
    ).isoformat()


def market_name(value: int) -> str:
    return enums_pb2.Market.Name(value)


def stream_name(value: str) -> str:
    return value.replace("_", "-")


def stream_key(spec: "StreamSpec") -> str:
    return f"{market_name(spec.market)}:{spec.symbol}:{spec.stream}"


@dataclasses.dataclass(frozen=True)
class StreamSpec:
    venue: int
    market: int
    symbol: str
    stream: str

    @property
    def key(self) -> str:
        return stream_key(self)

    @property
    def is_order_book(self) -> bool:
        return self.stream == "order_book"

    @property
    def expected_payload(self) -> str:
        return {
            "agg_trade": "agg_trade",
            "book_ticker": "book_ticker",
            "mark_price": "mark_price",
            "liquidation": "liquidation",
            "reference_data": "reference_observation",
        }[self.stream]

    @property
    def expected_schema(self) -> str:
        return {
            "agg_trade": AGG_TRADE_SCHEMA,
            "book_ticker": BOOK_TICKER_SCHEMA,
            "mark_price": MARK_PRICE_SCHEMA,
            "liquidation": LIQUIDATION_SCHEMA,
            "reference_data": REFERENCE_OBSERVATION_SCHEMA,
        }[self.stream]


@dataclasses.dataclass
class LatencyStats:
    count: int = 0
    minimum_ms: Optional[float] = None
    maximum_ms: Optional[float] = None
    total_ms: float = 0.0
    buckets: dict[str, int] = dataclasses.field(
        default_factory=lambda: {label: 0 for label, _, _ in LATENCY_BUCKETS}
    )

    def record(self, value_ms: float) -> None:
        self.count += 1
        self.total_ms += value_ms
        if self.minimum_ms is None or value_ms < self.minimum_ms:
            self.minimum_ms = value_ms
        if self.maximum_ms is None or value_ms > self.maximum_ms:
            self.maximum_ms = value_ms
        for label, low, high in LATENCY_BUCKETS:
            if low is None and value_ms < 0.0:
                self.buckets[label] += 1
                return
            if low is not None and high is not None and low <= value_ms < high:
                self.buckets[label] += 1
                return
            if low is not None and high is None and value_ms >= low:
                self.buckets[label] += 1
                return

    def to_dict(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "min_ms": self.minimum_ms,
            "max_ms": self.maximum_ms,
            "avg_ms": (self.total_ms / self.count) if self.count else None,
            "buckets": dict(self.buckets),
        }


def _quantity_is_zero(quantity: str) -> bool:
    if not quantity:
        raise ProtocolViolation("empty quantity")
    seen_dot = False
    digits = False
    nonzero = False
    for char in quantity:
        if char == ".":
            if seen_dot:
                raise ProtocolViolation(f"invalid quantity {quantity!r}")
            seen_dot = True
        elif "0" <= char <= "9":
            digits = True
            if char != "0":
                nonzero = True
        else:
            raise ProtocolViolation(f"invalid quantity {quantity!r}")
    if not digits:
        raise ProtocolViolation(f"invalid quantity {quantity!r}")
    return not nonzero


def _apply_levels(book: dict[str, str], levels: Iterable[Any]) -> None:
    for level in levels:
        price = level.price
        if not price:
            raise ProtocolViolation("empty price")
        if _quantity_is_zero(level.quantity):
            book.pop(price, None)
        else:
            book[price] = level.quantity


@dataclasses.dataclass
class StreamState:
    spec: StreamSpec
    request_id: str = ""
    subscription_id: str = ""
    gateway_instance_id: str = ""
    accepted_count: int = 0
    session_accepted: bool = False
    snapshot_count: int = 0
    update_count: int = 0
    event_count: int = 0
    resubscriptions: int = 0
    gap_count: int = 0
    rpc_errors: int = 0
    eof_count: int = 0
    protocol_errors: int = 0
    current_valid: bool = False
    snapshot_seen: bool = False
    last_session_sequence: int = 0
    last_payload: str = ""
    last_gap_reason: str = ""
    last_stream_status: str = ""
    last_error: str = ""
    first_snapshot_utc: str = ""
    last_event_utc_ns: int = 0
    bids: dict[str, str] = dataclasses.field(default_factory=dict)
    asks: dict[str, str] = dataclasses.field(default_factory=dict)
    receive_to_publish: LatencyStats = dataclasses.field(
        default_factory=LatencyStats
    )
    publish_to_client: LatencyStats = dataclasses.field(
        default_factory=LatencyStats
    )
    reference_sources: set[int] = dataclasses.field(default_factory=set)
    reference_source_states: dict[str, dict[str, Any]] = dataclasses.field(
        default_factory=dict
    )

    @property
    def book_levels(self) -> int:
        return len(self.bids) + len(self.asks)

    def reset_session(self) -> None:
        self.subscription_id = ""
        self.session_accepted = False
        self.last_session_sequence = 0
        self.current_valid = False
        self.snapshot_seen = False
        self.last_payload = ""
        self.bids.clear()
        self.asks.clear()

    def _record_publish_latency(
        self, delivery: Any, source_receive_ns: int
    ) -> None:
        publish_ns = delivery.publish_time_utc_ns
        if publish_ns <= 0:
            return
        if source_receive_ns > 0:
            self.receive_to_publish.record(
                (publish_ns - source_receive_ns) / 1_000_000.0
            )
        self.publish_to_client.record(
            (now_utc_ns() - publish_ns) / 1_000_000.0
        )

    def _validate_identity(self, metadata: Any) -> None:
        if metadata.venue != self.spec.venue:
            raise ProtocolViolation("payload venue mismatch")
        if metadata.market != self.spec.market:
            raise ProtocolViolation("payload market mismatch")
        if metadata.symbol != self.spec.symbol:
            raise ProtocolViolation("payload symbol mismatch")

    def _accept(self, item: Any, delivery: Any) -> None:
        if self.session_accepted:
            raise ProtocolViolation("duplicate subscription_accepted")
        accepted = item.subscription_accepted
        if accepted.request_id != self.request_id:
            raise ProtocolViolation("accepted request_id mismatch")
        if accepted.schema_version != "subscription-accepted.v1":
            raise ProtocolViolation("accepted schema version mismatch")
        if not accepted.gateway_instance_id or not accepted.subscription_id:
            raise ProtocolViolation("accepted identity is empty")
        if delivery.session_sequence != 1:
            raise ProtocolViolation("accepted sequence must be 1")
        if delivery.gateway_instance_id != accepted.gateway_instance_id:
            raise ProtocolViolation("accepted gateway instance mismatch")
        if delivery.subscription_id != accepted.subscription_id:
            raise ProtocolViolation("accepted subscription id mismatch")
        expected = (
            (SNAPSHOT_SCHEMA, DEPTH_SCHEMA)
            if self.spec.is_order_book
            else (self.spec.expected_schema,)
        )
        negotiated = set(accepted.negotiated_payload_schema_versions)
        missing = [schema for schema in expected if schema not in negotiated]
        if missing:
            raise ProtocolViolation(
                "accepted did not negotiate " + ",".join(missing)
            )
        self.gateway_instance_id = accepted.gateway_instance_id
        self.subscription_id = accepted.subscription_id
        self.session_accepted = True
        self.accepted_count += 1
        self.last_session_sequence = 1
        self.last_payload = "subscription_accepted"
        self.current_valid = not self.spec.is_order_book

    def _snapshot(self, snapshot: Any, delivery: Any) -> None:
        self._validate_identity(snapshot)
        if snapshot.schema_version != SNAPSHOT_SCHEMA:
            raise ProtocolViolation("snapshot schema version mismatch")
        if not snapshot.synchronized:
            raise ProtocolViolation("snapshot is not synchronized")
        if snapshot.last_update_id == 0:
            raise ProtocolViolation("snapshot last_update_id is zero")
        self.bids.clear()
        self.asks.clear()
        _apply_levels(self.bids, snapshot.bids)
        _apply_levels(self.asks, snapshot.asks)
        self.snapshot_count += 1
        self.snapshot_seen = True
        self.current_valid = True
        if not self.first_snapshot_utc:
            self.first_snapshot_utc = iso_utc_now()
        self.last_event_utc_ns = now_utc_ns()
        self._record_publish_latency(delivery, snapshot.generated_time_utc_ns)

    def _depth(self, depth: Any, delivery: Any) -> None:
        self._validate_identity(depth.metadata)
        if depth.metadata.stream != STREAM_DIFF_DEPTH:
            raise ProtocolViolation("depth stream mismatch")
        if not self.snapshot_seen:
            raise ProtocolViolation("depth update before snapshot")
        if not self.current_valid:
            raise ProtocolViolation("depth update while book is invalid")
        if depth.final_update_id == 0:
            raise ProtocolViolation("depth final_update_id is zero")
        _apply_levels(self.bids, depth.bids)
        _apply_levels(self.asks, depth.asks)
        self.update_count += 1
        self.last_event_utc_ns = now_utc_ns()
        self._record_publish_latency(
            delivery, depth.metadata.receive_time_utc_ns
        )

    def _event(self, event: Any, delivery: Any) -> None:
        expected_stream = {
            "agg_trade": STREAM_AGG_TRADE,
            "book_ticker": STREAM_BOOK_TICKER,
            "mark_price": STREAM_MARK_PRICE,
            "liquidation": STREAM_LIQUIDATION,
        }.get(self.spec.stream)
        if expected_stream is None:
            raise ProtocolViolation("unexpected metadata event stream")
        self._validate_identity(event.metadata)
        if event.metadata.stream != expected_stream:
            raise ProtocolViolation("event stream mismatch")
        if self.spec.stream == "mark_price":
            if event.metadata.HasField("exchange_transaction_time_ms"):
                raise ProtocolViolation(
                    "markPrice T must not be transaction time"
                )
            if not event.next_funding_time_ms:
                raise ProtocolViolation("markPrice next funding time is absent")
        if self.spec.stream == "liquidation" and not event.order_trade_time_ms:
            raise ProtocolViolation("liquidation order trade time is absent")
        self.event_count += 1
        self.current_valid = True
        self.last_event_utc_ns = now_utc_ns()
        self._record_publish_latency(
            delivery, event.metadata.receive_time_utc_ns
        )

    def _reference(self, reference: Any, delivery: Any) -> None:
        if reference.venue != self.spec.venue:
            raise ProtocolViolation("reference venue mismatch")
        if reference.market != self.spec.market:
            raise ProtocolViolation("reference market mismatch")
        if reference.symbol != self.spec.symbol:
            raise ProtocolViolation("reference symbol mismatch")
        if reference.schema_version != REFERENCE_OBSERVATION_SCHEMA:
            raise ProtocolViolation("reference schema version mismatch")
        expected_payload = REFERENCE_SOURCE_PAYLOADS.get(reference.source_kind)
        if expected_payload is None:
            raise ProtocolViolation("reference source kind is unspecified")
        kind_name = enums_pb2.ReferenceSourceKind.Name(reference.source_kind)
        payload_name = reference.WhichOneof("payload")
        now_ns = now_utc_ns()
        receive_ns = int(reference.receive_time_utc_ns or 0)
        source_time_ms = (
            int(reference.source_time_ms)
            if reference.HasField("source_time_ms")
            else None
        )
        entry = self.reference_source_states.setdefault(
            kind_name,
            {
                "status": "missing",
                "last_success_receive_utc_ns": None,
                "last_success_source_time_ms": None,
                "last_error": "",
                "last_observation_utc_ns": None,
                "observations": 0,
                "valid_count": 0,
                "empty_count": 0,
                "error_count": 0,
            },
        )
        entry["observations"] += 1
        entry["last_observation_utc_ns"] = receive_ns or now_ns
        status = reference.status
        if status == enums_pb2.REFERENCE_OBSERVATION_STATUS_VALID:
            if payload_name is None:
                raise ProtocolViolation("VALID reference has no payload")
            if payload_name != expected_payload:
                raise ProtocolViolation(
                    f"reference source_kind {kind_name} mismatch payload "
                    f"{payload_name}"
                )
            entry["status"] = "VALID"
            entry["valid_count"] += 1
            entry["last_success_receive_utc_ns"] = receive_ns or now_ns
            entry["last_success_source_time_ms"] = source_time_ms
            entry["last_error"] = ""
            self.reference_sources.add(reference.source_kind)
            self.current_valid = True
        elif status == enums_pb2.REFERENCE_OBSERVATION_STATUS_EMPTY:
            if payload_name is not None:
                raise ProtocolViolation(
                    "EMPTY/ERROR reference carries a payload"
                )
            entry["status"] = "EMPTY"
            entry["empty_count"] += 1
            if reference.source_kind in EMPTY_ALLOWED_REFERENCE_SOURCES:
                entry["last_success_receive_utc_ns"] = receive_ns or now_ns
                entry["last_success_source_time_ms"] = source_time_ms
                entry["last_error"] = ""
                self.reference_sources.add(reference.source_kind)
                self.current_valid = True
            else:
                entry["last_error"] = "disallowed_empty"
                self.reference_sources.discard(reference.source_kind)
        elif status == enums_pb2.REFERENCE_OBSERVATION_STATUS_ERROR:
            if payload_name is not None:
                raise ProtocolViolation(
                    "EMPTY/ERROR reference carries a payload"
                )
            entry["status"] = "ERROR"
            entry["error_count"] += 1
            entry["last_error"] = reference.error_code or "error"
            self.reference_sources.discard(reference.source_kind)
        else:
            raise ProtocolViolation("reference status is unspecified")
        self.event_count += 1
        self.last_event_utc_ns = now_ns
        self._record_publish_latency(delivery, reference.receive_time_utc_ns)

    def consume(self, item: Any) -> str:
        if item.HasField("envelope_metadata"):
            raise ProtocolViolation("legacy envelope_metadata is present")
        if not item.HasField("delivery_metadata"):
            raise ProtocolViolation("delivery_metadata is absent")
        delivery = item.delivery_metadata
        if not delivery.gateway_instance_id or not delivery.subscription_id:
            raise ProtocolViolation("delivery metadata identity is empty")
        if delivery.session_sequence == 0:
            raise ProtocolViolation("session_sequence is zero")
        payload = item.WhichOneof("payload")
        if payload is None:
            raise ProtocolViolation("payload oneof is absent")
        if payload == "subscription_accepted":
            self._accept(item, delivery)
            return "continue"
        if not self.session_accepted:
            raise ProtocolViolation("stream item before subscription_accepted")
        if delivery.subscription_id != self.subscription_id:
            raise ProtocolViolation("delivery subscription id mismatch")
        if delivery.gateway_instance_id != self.gateway_instance_id:
            raise ProtocolViolation("delivery gateway instance mismatch")
        expected_sequence = self.last_session_sequence + 1
        if delivery.session_sequence != expected_sequence:
            raise SequenceViolation(
                f"session_sequence {delivery.session_sequence} != "
                f"{expected_sequence}"
            )
        if payload == "consumer_gap":
            self.gap_count += 1
            self.last_gap_reason = enums_pb2.ConsumerGapReason.Name(
                item.consumer_gap.reason
            )
            self.last_error = f"consumer_gap:{self.last_gap_reason}"
            self.current_valid = False
            self.snapshot_seen = False
            self.bids.clear()
            self.asks.clear()
            self.last_session_sequence = delivery.session_sequence
            self.last_payload = payload
            return "resubscribe"
        if payload == "stream_status":
            status = item.stream_status
            self.last_stream_status = enums_pb2.StreamLifecycleState.Name(
                status.state
            )
            if status.state in {
                enums_pb2.STREAM_LIFECYCLE_STATE_CLOSED,
                enums_pb2.STREAM_LIFECYCLE_STATE_CLOSING,
                enums_pb2.STREAM_LIFECYCLE_STATE_DEGRADED,
            }:
                self.current_valid = False
                self.snapshot_seen = False
                self.bids.clear()
                self.asks.clear()
            self.last_session_sequence = delivery.session_sequence
            self.last_payload = payload
            return (
                "resubscribe"
                if status.state == enums_pb2.STREAM_LIFECYCLE_STATE_CLOSED
                else "continue"
            )
        if self.spec.is_order_book:
            if payload == "snapshot":
                self._snapshot(item.snapshot, delivery)
            elif payload == "depth_update":
                self._depth(item.depth_update, delivery)
            else:
                raise ProtocolViolation(f"unexpected order-book payload {payload}")
        else:
            if payload != self.spec.expected_payload:
                raise ProtocolViolation(
                    f"unexpected event payload {payload}; expected "
                    f"{self.spec.expected_payload}"
                )
            if payload == "reference_observation":
                self._reference(item.reference_observation, delivery)
            else:
                self._event(getattr(item, payload), delivery)
        self.last_session_sequence = delivery.session_sequence
        self.last_payload = payload
        return "continue"

    def on_gap_state(self) -> None:
        self.current_valid = False
        self.snapshot_seen = False
        self.bids.clear()
        self.asks.clear()

    def on_eof(self) -> None:
        self.eof_count += 1
        self.last_error = "eof"
        self.on_gap_state()

    def on_rpc_error(self, message: str) -> None:
        self.rpc_errors += 1
        self.last_error = message
        self.on_gap_state()

    def on_protocol_error(self, message: str) -> None:
        self.protocol_errors += 1
        self.last_error = message
        self.on_gap_state()

    def summary(self, current_ns: Optional[int] = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": self.spec.key,
            "venue": enums_pb2.Venue.Name(self.spec.venue),
            "market": market_name(self.spec.market),
            "symbol": self.spec.symbol,
            "stream": self.spec.stream,
            "accepted": self.accepted_count > 0,
            "accepted_count": self.accepted_count,
            "subscription_id": self.subscription_id,
            "gateway_instance_id": self.gateway_instance_id,
            "snapshot_count": self.snapshot_count,
            "update_count": self.update_count,
            "event_count": self.event_count,
            "reference_sources": sorted(
                enums_pb2.ReferenceSourceKind.Name(value)
                for value in self.reference_sources
            ),
            "book_levels": self.book_levels,
            "resubscriptions": self.resubscriptions,
            "gap_count": self.gap_count,
            "last_gap_reason": self.last_gap_reason,
            "stream_status": self.last_stream_status,
            "rpc_errors": self.rpc_errors,
            "eof_count": self.eof_count,
            "protocol_errors": self.protocol_errors,
            "current_valid": self.current_valid,
            "last_session_sequence": self.last_session_sequence,
            "last_payload": self.last_payload,
            "last_error": self.last_error,
            "first_snapshot_utc": self.first_snapshot_utc,
            "last_event_utc": iso_utc_from_ns(self.last_event_utc_ns),
            "latency_receive_to_publish_ms": self.receive_to_publish.to_dict(),
            "latency_publish_to_client_ms": self.publish_to_client.to_dict(),
        }
        if self.spec.stream == "reference_data":
            now_ns = now_utc_ns() if current_ns is None else current_ns
            details, issues = evaluate_reference_source_states(
                self.reference_source_states, now_ns
            )
            payload["reference_source_states"] = details
            payload["reference_issues"] = issues
            payload["reference_ok"] = not issues
        return payload


def evaluate_reference_source_states(
    states: dict[str, dict[str, Any]], now_ns: Optional[int] = None
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    current_ns = now_utc_ns() if now_ns is None else now_ns
    details: dict[str, dict[str, Any]] = {}
    issues: list[str] = []
    for kind in REQUIRED_REFERENCE_SOURCES:
        kind_name = enums_pb2.ReferenceSourceKind.Name(kind)
        raw = states.get(kind_name, {})
        status = raw.get("status", "missing")
        rx_ns = raw.get("last_success_receive_utc_ns")
        src_ms = raw.get("last_success_source_time_ms")
        rx_age = (
            round((current_ns - rx_ns) / 1_000_000_000.0, 3)
            if isinstance(rx_ns, int) and rx_ns > 0
            else None
        )
        src_age = (
            round((current_ns // 1_000_000 - src_ms) / 1000.0, 3)
            if isinstance(src_ms, int)
            else None
        )
        covered_status = status == "VALID" or (
            status == "EMPTY" and kind in EMPTY_ALLOWED_REFERENCE_SOURCES
        )
        source_issue: Optional[str] = None
        if not covered_status:
            source_issue = f"{kind_name}:status={status}"
        elif rx_ns is None or rx_ns <= 0:
            source_issue = f"{kind_name}:missing_receive_time"
        elif kind in PERIODIC_REFERENCE_SOURCES:
            if rx_age is not None and rx_age > PERIODIC_REFERENCE_MAX_AGE_SECONDS:
                source_issue = f"{kind_name}:stale_receive_age={rx_age:.1f}s"
            elif (
                status == "VALID"
                and src_age is not None
                and src_age > PERIODIC_REFERENCE_MAX_AGE_SECONDS
            ):
                source_issue = f"{kind_name}:stale_source_age={src_age:.1f}s"
        elif kind == enums_pb2.REFERENCE_SOURCE_KIND_INSTRUMENT:
            if (
                rx_age is not None
                and rx_age > INSTRUMENT_REFERENCE_MAX_RECEIVE_AGE_SECONDS
            ):
                source_issue = f"{kind_name}:stale_receive_age={rx_age:.1f}s"
        if source_issue is not None:
            issues.append(source_issue)
        details[kind_name] = {
            "status": status,
            "covered": source_issue is None,
            "last_success_receive_utc_ns": rx_ns,
            "last_success_source_time_ms": src_ms,
            "receive_age_seconds": rx_age,
            "source_age_seconds": src_age,
            "last_error": raw.get("last_error", ""),
            "observations": raw.get("observations", 0),
            "valid_count": raw.get("valid_count", 0),
            "empty_count": raw.get("empty_count", 0),
            "error_count": raw.get("error_count", 0),
        }
    return details, issues


def reference_stream_issues(
    stream_summary: dict[str, Any], now_ns: Optional[int] = None
) -> list[str]:
    states = stream_summary.get("reference_source_states")
    if isinstance(states, dict):
        _, issues = evaluate_reference_source_states(states, now_ns)
        return issues
    return ["missing_reference_source_states"]


def build_streams(config: dict[str, Any], profile: str) -> list[StreamSpec]:
    spot_symbols = config.get("spot_symbols")
    usdm_symbols = config.get("usdm_symbols")
    if not isinstance(spot_symbols, list) or not isinstance(usdm_symbols, list):
        raise ConfigError("config must contain spot_symbols and usdm_symbols lists")
    if not all(isinstance(symbol, str) and symbol for symbol in spot_symbols):
        raise ConfigError("spot_symbols must contain non-empty strings")
    if not all(isinstance(symbol, str) and symbol for symbol in usdm_symbols):
        raise ConfigError("usdm_symbols must contain non-empty strings")
    if len(set(spot_symbols)) != len(spot_symbols):
        raise ConfigError("spot_symbols contains duplicates")
    if len(set(usdm_symbols)) != len(usdm_symbols):
        raise ConfigError("usdm_symbols contains duplicates")
    if profile == "live40" and (
        len(spot_symbols) != 10 or len(usdm_symbols) != 10
    ):
        raise ConfigError(
            "live40 requires exactly 10 Spot and 10 USD-M symbols"
        )
    if not spot_symbols and not usdm_symbols:
        raise ConfigError("config has no symbols")
    streams: list[StreamSpec] = []
    for symbol in spot_symbols:
        streams.append(
            StreamSpec(VENUE_BINANCE, MARKET_SPOT, symbol, "order_book")
        )
    for symbol in usdm_symbols:
        streams.append(
            StreamSpec(
                VENUE_BINANCE,
                MARKET_USD_M_PERPETUAL,
                symbol,
                "order_book",
            )
        )
    for symbol in spot_symbols:
        streams.append(
            StreamSpec(VENUE_BINANCE, MARKET_SPOT, symbol, "agg_trade")
        )
    for symbol in spot_symbols:
        streams.append(
            StreamSpec(VENUE_BINANCE, MARKET_SPOT, symbol, "book_ticker")
        )
    if profile == "configured":
        for symbol in usdm_symbols:
            for kind in (
                "agg_trade",
                "book_ticker",
                "mark_price",
                "liquidation",
            ):
                streams.append(
                    StreamSpec(
                        VENUE_BINANCE,
                        MARKET_USD_M_PERPETUAL,
                        symbol,
                        kind,
                    )
                )
        for symbol in usdm_symbols:
            streams.append(
                StreamSpec(
                    VENUE_BINANCE,
                    MARKET_USD_M_PERPETUAL,
                    symbol,
                    "reference_data",
                )
            )
    return streams


def filter_streams(
    streams: list[StreamSpec], stream_filter: str, symbol: Optional[str] = None
) -> list[StreamSpec]:
    if symbol is not None:
        streams = [stream for stream in streams if stream.symbol == symbol]
    if stream_filter == "all":
        return list(streams)
    if stream_filter == "spot_events":
        return [
            stream
            for stream in streams
            if stream.market == MARKET_SPOT
            and stream.stream in ("agg_trade", "book_ticker")
        ]
    if stream_filter == "usdm_events":
        return [
            stream
            for stream in streams
            if stream.market == MARKET_USD_M_PERPETUAL
            and stream.stream
            in (
                "agg_trade",
                "book_ticker",
                "mark_price",
                "liquidation",
                "reference_data",
            )
        ]
    if stream_filter == "reference_data":
        return [stream for stream in streams if stream.stream == stream_filter]
    return [stream for stream in streams if stream.stream == stream_filter]


def load_config(path: str) -> dict[str, Any]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"cannot read config: {error}") from error
    except json.JSONDecodeError as error:
        raise ConfigError(f"invalid config JSON: {error}") from error
    if not isinstance(data, dict):
        raise ConfigError("config root must be an object")
    return data


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


class Consumer:
    def __init__(
        self,
        *,
        target: str,
        streams: list[StreamSpec],
        output_path: Path,
        profile: str,
        duration_seconds: float = 0.0,
        summary_interval_seconds: float = 30.0,
        status_interval_seconds: float = 10.0,
        retry_delays: Iterable[float] = RETRY_DELAYS,
        install_signals: bool = True,
        enable_status: bool = True,
    ) -> None:
        self.target = target
        self.streams = streams
        self.output_path = output_path
        self.profile = profile
        self.duration_seconds = duration_seconds
        self.summary_interval_seconds = summary_interval_seconds
        self.status_interval_seconds = status_interval_seconds
        self.retry_delays = tuple(retry_delays)
        self.install_signals = install_signals
        self.enable_status = enable_status
        self.started_at_utc = iso_utc_now()
        self.started_monotonic = time.monotonic()
        self.stop_event = asyncio.Event()
        self.exit_reason = "running"
        self.fatal_error: Optional[str] = None
        self.states = [StreamState(spec) for spec in streams]
        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[
            gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceStub
        ] = None
        self._request_counter = 0
        self._status_errors = 0
        self._status_timeouts = 0
        self._status_identity_mismatches = 0
        self._status_last_error = ""
        self._status_last_success_utc = ""
        self._status_seen_keys: set[tuple[int, int, str]] = set()
        self._status_markets: dict[str, Any] = {}
        self._status_total_active_subscriptions = 0
        self._status_gateway_instance_id = ""
        self._last_summary: Optional[dict[str, Any]] = None

    @property
    def expected_product_keys(self) -> set[tuple[int, int, str]]:
        return {
            (spec.venue, spec.market, spec.symbol)
            for spec in self.streams
            if spec.is_order_book
        }

    def _next_request_id(self, spec: StreamSpec, stream: str) -> str:
        self._request_counter += 1
        return (
            f"live40-{market_name(spec.market)}-{spec.symbol}-{stream}-"
            f"{self._request_counter}"
        )

    def _build_request(self, state: StreamState, attempt: int) -> Any:
        spec = state.spec
        if spec.is_order_book:
            request = gateway_messages_pb2.OrderBookSubscriptionRequest()
            request.request_id = self._next_request_id(
                spec, f"order-book-{attempt}"
            )
            request.schema_version = ORDER_BOOK_REQUEST_SCHEMA
            request.venue = spec.venue
            request.market = spec.market
            request.symbol = spec.symbol
            request.initial_snapshot_mode = INITIAL_SNAPSHOT_MODE_REQUIRED
            request.supported_snapshot_schema_versions.append(SNAPSHOT_SCHEMA)
            request.supported_update_schema_versions.append(DEPTH_SCHEMA)
            return request
        request = gateway_messages_pb2.EventSubscriptionRequest()
        request.request_id = self._next_request_id(
            spec, f"{spec.stream}-{attempt}"
        )
        request.schema_version = EVENT_REQUEST_SCHEMA
        selector = request.selectors.add()
        selector.venue = spec.venue
        selector.market = spec.market
        selector.symbol = spec.symbol
        selector.stream = {
            "agg_trade": STREAM_AGG_TRADE,
            "book_ticker": STREAM_BOOK_TICKER,
            "mark_price": STREAM_MARK_PRICE,
            "liquidation": STREAM_LIQUIDATION,
            "reference_data": STREAM_REFERENCE_DATA,
        }[spec.stream]
        request.delivery_mode = DELIVERY_MODE_CONTIGUOUS_EVENTS
        request.supported_payload_schema_versions.append(spec.expected_schema)
        return request

    async def _sleep_or_stop(self, delay: float) -> None:
        if delay <= 0:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    async def _stream_loop(self, state: StreamState) -> None:
        attempt = 0
        consecutive_failures = 0
        while not self.stop_event.is_set():
            attempt += 1
            healthy_session = False
            state.reset_session()
            if attempt > 1:
                state.resubscriptions += 1
            request = self._build_request(state, attempt)
            state.request_id = request.request_id
            call = None
            resubscribe_requested = False
            completed = True
            try:
                if state.spec.is_order_book:
                    call = self._stub.SubscribeOrderBook(request)
                else:
                    call = self._stub.SubscribeEvents(request)
                async for item in call:
                    if self.stop_event.is_set():
                        break
                    action = state.consume(item)
                    if action == "continue" and state.current_valid:
                        healthy_session = True
                    if action == "resubscribe":
                        resubscribe_requested = True
                        break
            except ProtocolViolation as error:
                state.on_protocol_error(str(error))
                completed = False
            except grpc.aio.AioRpcError as error:
                completed = False
                if self.stop_event.is_set():
                    break
                if error.code() in FATAL_RPC_CODES:
                    message = f"rpc {error.code().name}: {error.details()}"
                    state.on_protocol_error(message)
                    self.fatal_error = f"{state.spec.key}: {message}"
                    self.exit_reason = "fatal"
                    self.stop_event.set()
                    break
                state.on_rpc_error(
                    f"rpc {error.code().name}: {error.details()}"
                )
            except asyncio.CancelledError:
                completed = False
                if self.stop_event.is_set():
                    break
                raise
            except Exception as error:  # noqa: BLE001 - keep the probe alive
                completed = False
                state.on_rpc_error(f"{type(error).__name__}: {error}")
            finally:
                if call is not None:
                    call.cancel()
            if self.stop_event.is_set():
                break
            if completed and not resubscribe_requested:
                state.on_eof()
            consecutive_failures = self._next_failure_count(
                consecutive_failures, healthy_session
            )
            await self._sleep_or_stop(
                self._retry_delay(consecutive_failures)
            )

    def _next_failure_count(
        self, consecutive_failures: int, healthy_session: bool
    ) -> int:
        # A session that delivered valid data resets the backoff; the event
        # that ended it is the first failure of the new sequence.
        if healthy_session:
            return 1
        return consecutive_failures + 1

    def _retry_delay(self, consecutive_failures: int) -> float:
        index = min(
            max(consecutive_failures - 1, 0), len(self.retry_delays) - 1
        )
        return self.retry_delays[index]

    async def _status_loop(self) -> None:
        while not self.stop_event.is_set():
            self._request_counter += 1
            request = gateway_messages_pb2.GatewayStatusRequest()
            request.request_id = f"status-{self._request_counter}"
            request.schema_version = STATUS_REQUEST_SCHEMA
            try:
                response = await self._stub.GetGatewayStatus(
                    request, timeout=1.0
                )
                self._consume_status(response)
            except grpc.aio.AioRpcError as error:
                self._status_errors += 1
                if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    self._status_timeouts += 1
                self._status_last_error = (
                    f"rpc {error.code().name}: {error.details()}"
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - status is advisory
                self._status_errors += 1
                self._status_last_error = f"{type(error).__name__}: {error}"
            await self._sleep_or_stop(self.status_interval_seconds)

    def _consume_status(self, response: Any) -> None:
        if response.gateway_instance_id:
            if not self._status_gateway_instance_id:
                self._status_gateway_instance_id = response.gateway_instance_id
            elif response.gateway_instance_id != self._status_gateway_instance_id:
                self._status_identity_mismatches += 1
        expected = self.expected_product_keys
        now_ns = now_utc_ns()
        for market in response.markets:
            key = (market.venue, market.market, market.symbol)
            if key not in expected:
                self._status_identity_mismatches += 1
                continue
            self._status_seen_keys.add(key)
            last_event = (
                market.last_event_utc_ns
                if market.HasField("last_event_utc_ns")
                else 0
            )
            age_ms: Optional[float] = None
            if last_event > 0:
                candidate = (now_ns - last_event) / 1_000_000.0
                if candidate >= 0:
                    age_ms = candidate
            self._status_markets[
                f"{enums_pb2.Market.Name(market.market)}:{market.symbol}"
            ] = {
                "state": enums_pb2.StreamLifecycleState.Name(market.state),
                "last_event_utc_ns": last_event if last_event > 0 else None,
                "last_event_age_ms": age_ms,
                "connection_generation": (
                    market.connection_generation
                    if market.HasField("connection_generation")
                    else None
                ),
                "active_subscription_count": market.active_subscription_count,
            }
        self._status_total_active_subscriptions = (
            response.total_active_subscriptions
        )
        self._status_last_success_utc = iso_utc_now()
        self._status_last_error = ""

    def _totals(self) -> dict[str, int]:
        return {
            "streams": len(self.states),
            "order_books": sum(
                1 for state in self.states if state.spec.is_order_book
            ),
            "accepted": sum(
                1 for state in self.states if state.accepted_count > 0
            ),
            "valid": sum(1 for state in self.states if state.current_valid),
            "snapshots": sum(state.snapshot_count for state in self.states),
            "updates": sum(state.update_count for state in self.states),
            "events": sum(state.event_count for state in self.states),
            "resubscriptions": sum(
                state.resubscriptions for state in self.states
            ),
            "gaps": sum(state.gap_count for state in self.states),
            "protocol_errors": sum(
                state.protocol_errors for state in self.states
            ),
            "rpc_errors": sum(state.rpc_errors for state in self.states),
            "eofs": sum(state.eof_count for state in self.states),
        }

    def _compute_ok(self, current_ns: Optional[int] = None) -> bool:
        if self.fatal_error:
            return False
        books = [state for state in self.states if state.spec.is_order_book]
        events = [state for state in self.states if not state.spec.is_order_book]
        required_event_data = [
            state for state in events if state.spec.stream != "liquidation"
        ]
        reference_streams = [
            state for state in events if state.spec.stream == "reference_data"
        ]
        now_ns = now_utc_ns() if current_ns is None else current_ns
        reference_ok = all(
            not evaluate_reference_source_states(
                state.reference_source_states, now_ns
            )[1]
            for state in reference_streams
        )
        status_ok = True
        if self.enable_status:
            status_ok = (
                self.expected_product_keys <= self._status_seen_keys
                and self._status_identity_mismatches == 0
            )
        return (
            all(state.accepted_count > 0 for state in self.states)
            and all(state.current_valid for state in self.states)
            and all(
                state.snapshot_count > 0 and state.update_count > 0
                for state in books
            )
            and all(state.event_count > 0 for state in required_event_data)
            and reference_ok
            and sum(state.protocol_errors for state in self.states) == 0
            and status_ok
        )

    def _status_summary(self) -> dict[str, Any]:
        return {
            "errors": self._status_errors,
            "timeouts": self._status_timeouts,
            "identity_mismatches": self._status_identity_mismatches,
            "last_error": self._status_last_error,
            "last_success_utc": self._status_last_success_utc,
            "seen_keys": sorted(
                f"{enums_pb2.Market.Name(market)}:{symbol}"
                for _, market, symbol in self._status_seen_keys
            ),
            "missing_keys": sorted(
                f"{enums_pb2.Market.Name(market)}:{symbol}"
                for _, market, symbol in (
                    self.expected_product_keys - self._status_seen_keys
                )
            ),
            "total_active_subscriptions": (
                self._status_total_active_subscriptions
            ),
            "markets": self._status_markets,
        }

    def _summary_payload(self) -> dict[str, Any]:
        totals = self._totals()
        ok = self._compute_ok()
        return {
            "schema_version": "bmdg-consumer-summary.v1",
            "profile": self.profile,
            "target": self.target,
            "started_at_utc": self.started_at_utc,
            "updated_at_utc": iso_utc_now(),
            "elapsed_seconds": time.monotonic() - self.started_monotonic,
            "duration_seconds": self.duration_seconds,
            "exit_reason": self.exit_reason,
            "ok": ok,
            "fatal_error": self.fatal_error,
            "gateway_instance_id": self._status_gateway_instance_id,
            "totals": totals,
            "status": self._status_summary(),
            "streams": [state.summary() for state in self.states],
            "latency_basis": (
                "UTC wall clock; receive→publish uses event/snapshot "
                "timestamps; publish→client includes client scheduling; "
                "negative values are not clamped"
            ),
        }

    def _write_summary(self) -> None:
        payload = self._summary_payload()
        try:
            atomic_write_json(self.output_path, payload)
        except OSError as error:
            self.fatal_error = f"cannot write summary: {error}"
            self.exit_reason = "fatal"
            self.stop_event.set()
            return
        self._last_summary = payload
        totals = payload["totals"]
        print(
            "consumer_summary "
            f"elapsed={payload['elapsed_seconds']:.0f}s "
            f"streams={totals['streams']} accepted={totals['accepted']} "
            f"valid={totals['valid']} snapshots={totals['snapshots']} "
            f"updates={totals['updates']} events={totals['events']} "
            f"resubscriptions={totals['resubscriptions']} "
            f"gaps={totals['gaps']} "
            f"protocol_errors={totals['protocol_errors']} "
            f"rpc_errors={totals['rpc_errors']} "
            f"status_errors={self._status_errors} "
            f"ok={'yes' if payload['ok'] else 'no'}",
            flush=True,
        )

    async def _summary_loop(self) -> None:
        self._write_summary()
        while not self.stop_event.is_set():
            await self._sleep_or_stop(self.summary_interval_seconds)
            if self.stop_event.is_set():
                break
            self._write_summary()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, self._request_stop)
            except NotImplementedError:
                return

    def _request_stop(self) -> None:
        if not self.stop_event.is_set():
            if self.exit_reason == "running":
                self.exit_reason = "signal"
            self.stop_event.set()

    async def run(self) -> int:
        if self.install_signals:
            self._install_signal_handlers()
        self._channel = grpc.aio.insecure_channel(
            self.target,
            options=list(CLIENT_CHANNEL_OPTIONS),
        )
        self._stub = gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceStub(
            self._channel
        )
        tasks = [
            asyncio.create_task(self._stream_loop(state))
            for state in self.states
        ]
        if self.enable_status:
            tasks.append(asyncio.create_task(self._status_loop()))
        tasks.append(asyncio.create_task(self._summary_loop()))
        try:
            if self.duration_seconds > 0:
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(),
                        timeout=self.duration_seconds,
                    )
                except asyncio.TimeoutError:
                    if self.exit_reason == "running":
                        self.exit_reason = "duration"
            else:
                await self.stop_event.wait()
                if self.exit_reason == "running":
                    self.exit_reason = "signal"
        finally:
            self.stop_event.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._write_summary()
            if self._channel is not None:
                try:
                    await asyncio.wait_for(self._channel.close(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
        if self.fatal_error:
            return 2
        return 0 if (self._last_summary or {}).get("ok", False) else 1


class Supervisor:
    """Run independent stream shards so one high-rate stream cannot starve another."""

    def _shards(self) -> list[tuple[str, str, Optional[str]]]:
        shards: list[tuple[str, str, Optional[str]]] = [
            ("order-book", "order_book", None)
        ]
        seen: set[str] = set()
        for stream in self.streams:
            if (
                stream.market != MARKET_SPOT
                or stream.stream not in ("agg_trade", "book_ticker")
                or stream.symbol in seen
            ):
                continue
            seen.add(stream.symbol)
            shards.append(
                (f"events-{stream.symbol}", "spot_events", stream.symbol)
            )
        usdm_seen: set[str] = set()
        for stream in self.streams:
            if stream.market != MARKET_USD_M_PERPETUAL:
                continue
            if (
                stream.stream
                in (
                    "agg_trade",
                    "book_ticker",
                    "mark_price",
                    "liquidation",
                    "reference_data",
                )
                and stream.symbol not in usdm_seen
            ):
                usdm_seen.add(stream.symbol)
                shards.append(
                    (
                        f"usdm-events-{stream.symbol}",
                        "usdm_events",
                        stream.symbol,
                    )
                )
        return shards

    def __init__(
        self,
        *,
        target: str,
        config_path: Path,
        profile: str,
        streams: list[StreamSpec],
        output_path: Path,
        duration_seconds: float = 0.0,
        summary_interval_seconds: float = 30.0,
        status_interval_seconds: float = 10.0,
        retry_delays: Iterable[float] = RETRY_DELAYS,
        install_signals: bool = True,
    ) -> None:
        self.target = target
        self.config_path = config_path
        self.profile = profile
        self.streams = streams
        self.output_path = output_path
        self.duration_seconds = duration_seconds
        self.summary_interval_seconds = summary_interval_seconds
        self.status_interval_seconds = status_interval_seconds
        self.retry_delays = tuple(retry_delays)
        self.install_signals = install_signals
        self.started_at_utc = iso_utc_now()
        self.started_monotonic = time.monotonic()
        self.stop_event = asyncio.Event()
        self.exit_reason = "running"
        self.fatal_error: Optional[str] = None
        self.shard_paths: list[Path] = []
        self.processes: list[tuple[str, asyncio.subprocess.Process]] = []
        self._last_summary: Optional[dict[str, Any]] = None
        self.status_consumer = Consumer(
            target=target,
            streams=streams,
            output_path=output_path,
            profile=profile,
            duration_seconds=0.0,
            summary_interval_seconds=summary_interval_seconds,
            status_interval_seconds=status_interval_seconds,
            retry_delays=retry_delays,
            install_signals=False,
            enable_status=True,
        )
        self.status_consumer.stop_event = self.stop_event

    def _request_stop(self) -> None:
        if not self.stop_event.is_set():
            if self.exit_reason == "running":
                self.exit_reason = "signal"
            self.stop_event.set()

    async def _wait_children(self) -> None:
        pending = {
            asyncio.create_task(process.wait()): (name, process)
            for name, process in self.processes
        }
        while pending:
            done, _ = await asyncio.wait(
                pending.keys(), return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                name, _process = pending.pop(task)
                code = task.result()
                if not self.stop_event.is_set():
                    self.fatal_error = f"shard {name} exited {code}"
                    self.exit_reason = "fatal"
                    self.stop_event.set()

    async def _summary_loop(self) -> None:
        self._write_summary()
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=self.summary_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass
            if self.stop_event.is_set():
                break
            self._write_summary()

    def _read_shards(self) -> list[dict[str, Any]]:
        shards: list[dict[str, Any]] = []
        for path in self.shard_paths:
            try:
                shards.append(json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):
                continue
        return shards

    def _merge(
        self, shards: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], dict[str, int]]:
        streams: list[dict[str, Any]] = []
        totals = {
            "streams": 0,
            "order_books": 0,
            "accepted": 0,
            "valid": 0,
            "snapshots": 0,
            "updates": 0,
            "events": 0,
            "resubscriptions": 0,
            "gaps": 0,
            "protocol_errors": 0,
            "rpc_errors": 0,
            "eofs": 0,
        }
        for shard in shards:
            streams.extend(shard.get("streams", []))
            shard_totals = shard.get("totals", {})
            for key in totals:
                totals[key] += int(shard_totals.get(key, 0))
        streams.sort(key=lambda stream: stream.get("key", ""))
        return streams, totals

    def _summary_payload(self) -> dict[str, Any]:
        streams, totals = self._merge(self._read_shards())
        status = self.status_consumer._status_summary()
        ok = self._compute_ok(streams, totals, status)
        return {
            "schema_version": "bmdg-consumer-summary.v1",
            "profile": self.profile,
            "target": self.target,
            "started_at_utc": self.started_at_utc,
            "updated_at_utc": iso_utc_now(),
            "elapsed_seconds": time.monotonic() - self.started_monotonic,
            "duration_seconds": self.duration_seconds,
            "exit_reason": self.exit_reason,
            "ok": ok,
            "fatal_error": self.fatal_error,
            "gateway_instance_id": self.status_consumer._status_gateway_instance_id,
            "totals": totals,
            "status": status,
            "streams": streams,
            "latency_basis": (
                "UTC wall clock; receive→publish uses event/snapshot "
                "timestamps; publish→client includes client scheduling; "
                "negative values are not clamped"
            ),
        }

    def _compute_ok(
        self,
        streams: list[dict[str, Any]],
        totals: dict[str, int],
        status: dict[str, Any],
        current_ns: Optional[int] = None,
    ) -> bool:
        if self.fatal_error:
            return False
        books = [stream for stream in streams if stream["stream"] == "order_book"]
        events = [stream for stream in streams if stream["stream"] != "order_book"]
        required_event_data = [
            stream for stream in events if stream["stream"] != "liquidation"
        ]
        reference_streams = [
            stream for stream in events if stream["stream"] == "reference_data"
        ]
        now_ns = now_utc_ns() if current_ns is None else current_ns
        reference_ok = all(
            not reference_stream_issues(stream, now_ns)
            for stream in reference_streams
        )
        expected_keys = {stream.key for stream in self.streams}
        actual_keys = {stream["key"] for stream in streams}
        status_ok = (
            self.status_consumer.expected_product_keys
            <= self.status_consumer._status_seen_keys
            and self.status_consumer._status_identity_mismatches == 0
        )
        return (
            bool(streams)
            and totals["streams"] == len(self.streams)
            and actual_keys == expected_keys
            and all(stream["accepted_count"] > 0 for stream in streams)
            and all(stream["current_valid"] for stream in streams)
            and all(
                stream["snapshot_count"] > 0 and stream["update_count"] > 0
                for stream in books
            )
            and all(
                stream["event_count"] > 0 for stream in required_event_data
            )
            and reference_ok
            and totals["protocol_errors"] == 0
            and status_ok
        )

    def _write_summary(self) -> None:
        payload = self._summary_payload()
        try:
            atomic_write_json(self.output_path, payload)
        except OSError as error:
            self.fatal_error = f"cannot write summary: {error}"
            self.exit_reason = "fatal"
            self.stop_event.set()
            return
        self._last_summary = payload
        totals = payload["totals"]
        print(
            "consumer_summary "
            f"supervisor=yes elapsed={payload['elapsed_seconds']:.0f}s "
            f"streams={totals['streams']} accepted={totals['accepted']} "
            f"valid={totals['valid']} snapshots={totals['snapshots']} "
            f"updates={totals['updates']} events={totals['events']} "
            f"resubscriptions={totals['resubscriptions']} "
            f"gaps={totals['gaps']} "
            f"protocol_errors={totals['protocol_errors']} "
            f"rpc_errors={totals['rpc_errors']} "
            f"status_errors={payload['status']['errors']} "
            f"ok={'yes' if payload['ok'] else 'no'}",
            flush=True,
        )

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        if self.install_signals:
            for signum in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(signum, self._request_stop)
                except NotImplementedError:
                    break
        status_task: Optional[asyncio.Task[Any]] = None
        summary_task: Optional[asyncio.Task[Any]] = None
        child_task: Optional[asyncio.Task[Any]] = None
        try:
            script = Path(__file__).resolve()
            child_summary_interval = min(
                10.0, self.summary_interval_seconds
            )
            for name, stream_filter, symbol in self._shards():
                shard_path = (
                    self.output_path.parent
                    / f".{self.output_path.stem}.{name}.json"
                ).resolve()
                self.shard_paths.append(shard_path)
                shard_path.unlink(missing_ok=True)
                command = [
                    sys.executable,
                    str(script),
                    "--target",
                    self.target,
                    "--config",
                    str(self.config_path),
                    "--profile",
                    self.profile,
                    "--streams",
                    stream_filter,
                    "--no-status",
                    "--duration-seconds",
                    "0",
                    "--output",
                    str(shard_path),
                    "--summary-interval-seconds",
                    str(child_summary_interval),
                ]
                if symbol is not None:
                    command.extend(["--symbol", symbol])
                process = await asyncio.create_subprocess_exec(*command)
                self.processes.append((name, process))
            # Spawn shard children before this process initializes gRPC, so
            # they do not inherit an active channel's completion-queue fds.
            self.status_consumer._channel = grpc.aio.insecure_channel(
                self.target,
                options=list(CLIENT_CHANNEL_OPTIONS),
            )
            self.status_consumer._stub = (
                gateway_service_pb2_grpc.BinanceMarketDataGatewayServiceStub(
                    self.status_consumer._channel
                )
            )
            status_task = asyncio.create_task(
                self.status_consumer._status_loop()
            )
            summary_task = asyncio.create_task(self._summary_loop())
            child_task = asyncio.create_task(self._wait_children())
            if self.duration_seconds > 0:
                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(),
                        timeout=self.duration_seconds,
                    )
                except asyncio.TimeoutError:
                    if self.exit_reason == "running":
                        self.exit_reason = "duration"
            else:
                await self.stop_event.wait()
                if self.exit_reason == "running":
                    self.exit_reason = "signal"
        finally:
            self.stop_event.set()
            for _name, process in self.processes:
                if process.returncode is None:
                    process.terminate()
            deadline = loop.time() + 15.0
            for _name, process in self.processes:
                remaining = max(0.0, deadline - loop.time())
                try:
                    await asyncio.wait_for(process.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
            for task in (status_task, summary_task, child_task):
                if task is not None:
                    task.cancel()
            await asyncio.gather(
                *(
                    task
                    for task in (status_task, summary_task, child_task)
                    if task is not None
                ),
                return_exceptions=True,
            )
            self._write_summary()
            if self.status_consumer._channel is not None:
                try:
                    await asyncio.wait_for(
                        self.status_consumer._channel.close(), timeout=5.0
                    )
                except asyncio.TimeoutError:
                    pass
        if self.fatal_error:
            return 2
        return 0 if (self._last_summary or {}).get("ok", False) else 1


def _validate_target(target: str) -> None:
    if ":" not in target:
        raise ConfigError("target must be HOST:PORT")
    host = target.rsplit(":", 1)[0].strip("[]")
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ConfigError("target must be a loopback address")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Attach-only BMDG consumer probe; never starts or stops the "
            "gateway and never connects to Binance."
        )
    )
    parser.add_argument("--target", default="127.0.0.1:50051")
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--profile", default="configured", choices=("configured", "live40")
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0.0,
        help="0 runs until SIGTERM; a positive value is a finite run",
    )
    parser.add_argument(
        "--output", default="gateway-consumer-summary.json"
    )
    parser.add_argument(
        "--summary-interval-seconds", type=float, default=30.0
    )
    parser.add_argument(
        "--status-interval-seconds", type=float, default=10.0
    )
    parser.add_argument(
        "--supervisor",
        action="store_true",
        help="run independent order-book, aggTrade and bookTicker shards",
    )
    parser.add_argument(
        "--streams",
        choices=(
            "all",
            "order_book",
            "agg_trade",
            "book_ticker",
            "mark_price",
            "liquidation",
            "reference_data",
            "spot_events",
            "usdm_events",
        ),
        default="all",
        help="internal shard filter",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="internal single-symbol shard filter",
    )
    parser.add_argument(
        "--no-status",
        action="store_true",
        help="internal shard mode without GetGatewayStatus",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    try:
        _validate_target(args.target)
        config = load_config(args.config)
        streams = filter_streams(
            build_streams(config, args.profile), args.streams, args.symbol
        )
    except ConfigError as error:
        print(f"consumer_config_error={error}", file=sys.stderr)
        return 2
    mode = "supervisor" if args.supervisor else "single"
    print(
        "consumer_start "
        f"target={args.target} profile={args.profile} mode={mode} "
        f"streams={len(streams)} output={args.output}",
        flush=True,
    )
    try:
        if args.supervisor:
            supervisor = Supervisor(
                target=args.target,
                config_path=Path(args.config),
                profile=args.profile,
                streams=streams,
                output_path=Path(args.output),
                duration_seconds=args.duration_seconds,
                summary_interval_seconds=args.summary_interval_seconds,
                status_interval_seconds=args.status_interval_seconds,
            )
            return asyncio.run(supervisor.run())
        consumer = Consumer(
            target=args.target,
            streams=streams,
            output_path=Path(args.output),
            profile=args.profile,
            duration_seconds=args.duration_seconds,
            summary_interval_seconds=args.summary_interval_seconds,
            status_interval_seconds=args.status_interval_seconds,
            enable_status=not args.no_status,
        )
        return asyncio.run(consumer.run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
