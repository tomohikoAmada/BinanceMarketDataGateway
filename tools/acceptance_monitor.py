#!/usr/bin/env python3
"""Sample the G4 co-located acceptance run and write a final verdict."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CONSUMER_SUMMARY = Path("/var/lib/bmdg-consumer/summary.json")
RECORDER_STATUS = {
    "sbmdr-recorder@spot": Path("/var/log/sbmdr/spot.status.jsonl"),
    "sbmdr-recorder@usdm": Path("/var/log/sbmdr/usdm.status.jsonl"),
}
ARCHIVE_UNIT = "sbmdr-archive.service"
ARCHIVE_TIMER = "sbmdr-archive.timer"
ARCHIVE_LOG = Path("/var/log/sbmdr/archive.jsonl")
SBMDR_ROOTS = Path("/etc/sbmdr/roots.json")
SBMDR_METADATA_ROOTS = Path("/etc/sbmdr/metadata-roots.json")
RECORDER_UNITS = ("sbmdr-recorder@spot", "sbmdr-recorder@usdm")
GIB = 1024 * 1024 * 1024
PERIODIC_REFERENCE_SOURCE_NAMES = frozenset(
    {
        "REFERENCE_SOURCE_KIND_OPEN_INTEREST",
        "REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_ACCOUNT_RATIO",
        "REFERENCE_SOURCE_KIND_TOP_LONG_SHORT_POSITION_RATIO",
        "REFERENCE_SOURCE_KIND_GLOBAL_LONG_SHORT_ACCOUNT_RATIO",
        "REFERENCE_SOURCE_KIND_TAKER_BUY_SELL_VOLUME",
    }
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def show(unit: str, properties: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for _ in range(2):
        try:
            result = subprocess.run(
                ["systemctl", "show", unit, f"--property={properties}"],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        for line in result.stdout.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
        if values:
            return values
    return values


def read_summary() -> dict[str, Any]:
    try:
        return json.loads(CONSUMER_SUMMARY.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def read_last_jsonl(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - 64 * 1024))
            lines = handle.read().decode("utf-8", "replace").splitlines()
    except OSError:
        return {}
    for line in reversed(lines):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return {}


def read_meminfo() -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        with open("/proc/meminfo", encoding="utf-8") as handle:
            for line in handle:
                key, _, rest = line.partition(":")
                parts = rest.split()
                if parts:
                    values[key] = int(parts[0]) * 1024
    except OSError:
        pass
    return values


def read_cpu() -> dict[str, int]:
    with open("/proc/stat", encoding="utf-8") as handle:
        fields = handle.readline().split()
    values = [int(field) for field in fields[1:]]
    return {
        "total": sum(values),
        "idle": values[3] + (values[4] if len(values) > 4 else 0),
        "steal": values[7] if len(values) > 7 else 0,
    }


def children(pid: int) -> list[int]:
    if pid <= 0:
        return []
    try:
        result = subprocess.run(
            ["pgrep", "-P", str(pid)],
            check=False,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    return sorted(int(line) for line in result.stdout.split() if line.isdigit())


def state_counts(summary: dict[str, Any]) -> tuple[int, int, int]:
    markets = summary.get("status", {}).get("markets", {})
    live = 0
    non_live = 0
    unknown = 0
    for entry in markets.values():
        state = entry.get("state", "")
        if state == "STREAM_LIFECYCLE_STATE_LIVE":
            live += 1
        elif state:
            non_live += 1
        else:
            unknown += 1
    return live, non_live, unknown


def summarize_reference_streams(summary: dict[str, Any]) -> dict[str, Any]:
    streams = summary.get("streams", [])
    reference_streams = [
        stream for stream in streams if stream.get("stream") == "reference_data"
    ]
    unhealthy: dict[str, Any] = {}
    ok_streams = 0
    error_sources = 0
    missing_sources = 0
    max_periodic_age: float | None = None
    for stream in reference_streams:
        issues = stream.get("reference_issues", [])
        if stream.get("reference_ok") is True and not issues:
            ok_streams += 1
        else:
            unhealthy[stream.get("key", "")] = {
                "issues": issues,
                "valid": stream.get("current_valid", False),
                "events": stream.get("event_count", 0),
            }
        source_states = stream.get("reference_source_states", {})
        if isinstance(source_states, dict):
            for name, source in source_states.items():
                if not isinstance(source, dict):
                    continue
                status = source.get("status")
                if status == "ERROR":
                    error_sources += 1
                elif status == "missing":
                    missing_sources += 1
                if name in PERIODIC_REFERENCE_SOURCE_NAMES:
                    age = source.get("receive_age_seconds")
                    if isinstance(age, (int, float)):
                        max_periodic_age = (
                            float(age)
                            if max_periodic_age is None
                            else max(max_periodic_age, float(age))
                        )
    return {
        "reference_streams": len(reference_streams),
        "reference_ok_streams": ok_streams,
        "reference_error_sources": error_sources,
        "reference_missing_sources": missing_sources,
        "reference_max_periodic_receive_age_seconds": max_periodic_age,
        "reference_unhealthy": unhealthy,
    }


def _read_json_dict(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size if path.is_file() else 0
    except OSError:
        return 0


def read_r3_backlog(
    *,
    archive_log: Path = ARCHIVE_LOG,
    roots_path: Path = SBMDR_ROOTS,
    metadata_roots_path: Path = SBMDR_METADATA_ROOTS,
    now_seconds: float | None = None,
) -> dict[str, Any]:
    now_ts = time.time() if now_seconds is None else now_seconds
    last_archive = read_last_jsonl(archive_log)
    archive_finish = last_archive.get("finish_utc_seconds")
    archive_failed = last_archive.get("failed")
    migration_failures = 0
    markets = last_archive.get("markets", {})
    if isinstance(markets, dict):
        for entry in markets.values():
            if isinstance(entry, dict):
                exit_code = entry.get("migration_exit")
                if isinstance(exit_code, int) and exit_code != 0:
                    migration_failures += 1

    roots = _read_json_dict(roots_path)
    cold_roots = _read_json_dict(metadata_roots_path)
    pending_migrate = 0
    oldest_pending_age: float | None = None
    hot_metadata_bytes = 0
    cold_metadata_bytes = 0
    for market, raw_root in roots.items():
        if not isinstance(raw_root, str) or not raw_root:
            continue
        partitions_dir = Path(raw_root) / "partitions"
        if not partitions_dir.is_dir():
            continue
        cold_raw = cold_roots.get(market)
        cold_partitions: Path | None = None
        if isinstance(cold_raw, str) and cold_raw:
            cold_base = Path(cold_raw)
            nested_cold = cold_base / "partitions"
            cold_partitions = nested_cold if nested_cold.is_dir() else cold_base
        try:
            entries = sorted(partitions_dir.iterdir())
        except OSError:
            continue
        for partition_dir in entries:
            if not partition_dir.is_dir():
                continue
            hot_metadata_bytes += _file_size(
                partition_dir / "state" / "catalog.sqlite"
            )
            hot_metadata_bytes += _file_size(partition_dir / "partition.json")
            meta = _read_json_dict(partition_dir / "partition.json")
            if meta.get("state") == "closed" and cold_partitions is not None:
                cold_dir = cold_partitions / partition_dir.name
                if not (cold_dir / "partition.json").is_file():
                    pending_migrate += 1
                    closed_ts = meta.get("closed_utc_seconds")
                    if not isinstance(closed_ts, (int, float)):
                        try:
                            closed_ts = (
                                partition_dir / "partition.json"
                            ).stat().st_mtime
                        except OSError:
                            closed_ts = None
                    if isinstance(closed_ts, (int, float)):
                        age = max(0.0, float(now_ts) - float(closed_ts))
                        oldest_pending_age = (
                            age
                            if oldest_pending_age is None
                            else max(oldest_pending_age, age)
                        )
        if cold_partitions is not None and cold_partitions.is_dir():
            try:
                for partition_dir in cold_partitions.iterdir():
                    if partition_dir.is_dir():
                        cold_metadata_bytes += _file_size(
                            partition_dir / "state" / "catalog.sqlite"
                        )
            except OSError:
                pass
    return {
        "last_archive_finish_utc_seconds": archive_finish,
        "last_archive_failed": archive_failed,
        "migration_failures": migration_failures,
        "pending_migrate_partitions": pending_migrate,
        "oldest_pending_age_seconds": (
            round(oldest_pending_age, 1)
            if oldest_pending_age is not None
            else None
        ),
        "hot_metadata_bytes": hot_metadata_bytes,
        "cold_metadata_bytes": cold_metadata_bytes,
    }


def collect(
    index: int,
    started: float,
    previous_cpu: dict[str, int] | None,
) -> tuple[dict[str, Any], dict[str, int]]:
    now = time.monotonic()
    wall = utc_now()
    cpu = read_cpu()
    nproc = os.cpu_count() or 1
    cpu_percent = None
    steal_percent = None
    if previous_cpu is not None:
        total_delta = cpu["total"] - previous_cpu["total"]
        idle_delta = cpu["idle"] - previous_cpu["idle"]
        steal_delta = cpu["steal"] - previous_cpu["steal"]
        if total_delta > 0:
            cpu_percent = (
                (total_delta - idle_delta - steal_delta)
                / total_delta
                * nproc
                * 100.0
            )
            steal_percent = steal_delta / total_delta * nproc * 100.0
    memory = read_meminfo()
    root_disk = shutil.disk_usage("/")
    data_disk = shutil.disk_usage("/srv/recorder-data")
    summary = read_summary()
    totals = summary.get("totals", {})
    status = summary.get("status", {})
    live, non_live, unknown = state_counts(summary)
    reference_summary = summarize_reference_streams(summary)
    r3_backlog = read_r3_backlog()
    streams = summary.get("streams", [])
    order_books = [
        stream for stream in streams if stream.get("stream") == "order_book"
    ]
    books_without_snapshot = sum(
        1 for stream in order_books if stream.get("snapshot_count", 0) == 0
    )
    gap_streams = {
        stream.get("key", ""): {
            "gaps": stream.get("gap_count", 0),
            "resubscriptions": stream.get("resubscriptions", 0),
            "reason": stream.get("last_gap_reason", ""),
            "valid": stream.get("current_valid", False),
            "snapshots": stream.get("snapshot_count", 0),
            "updates": stream.get("update_count", 0),
            "events": stream.get("event_count", 0),
        }
        for stream in streams
        if stream.get("gap_count", 0) or stream.get("resubscriptions", 0)
    }
    consumer = show(
        "bmdg-consumer",
        "ActiveState,SubState,MainPID,NRestarts,MemoryCurrent,"
        "MemoryPeak,CPUUsageNSec",
    )
    gateway = show(
        "bmdg",
        "ActiveState,SubState,MainPID,NRestarts,MemoryCurrent,"
        "MemoryPeak,CPUUsageNSec",
    )
    consumer_pid = int(consumer.get("MainPID", "0") or 0)
    recorder: dict[str, dict[str, Any]] = {}
    for unit in RECORDER_UNITS:
        values = show(unit, "ActiveState,SubState,MainPID,NRestarts")
        recorder_status = read_last_jsonl(RECORDER_STATUS[unit])
        recorder[unit] = {
            "active": values.get("ActiveState", ""),
            "sub_state": values.get("SubState", ""),
            "main_pid": int(values.get("MainPID", "0") or 0),
            "n_restarts": int(values.get("NRestarts", "0") or 0),
            "records": recorder_status.get("records"),
            "dropped_total": recorder_status.get("dropped_total"),
            "queued_records": recorder_status.get("queued_records"),
            "queued_payload_bytes": recorder_status.get("queued_payload_bytes"),
            "seal_queue_depth": recorder_status.get("seal_queue_depth"),
            "max_durability_ns": recorder_status.get("max_durability_ns"),
            "rss_bytes": recorder_status.get("rss_bytes"),
            "cpu_percent": recorder_status.get("cpu_percent"),
            "elapsed_seconds": recorder_status.get("elapsed_seconds"),
        }
    archive = show(
        ARCHIVE_UNIT,
        "ActiveState,SubState,Result,ExecMainStatus,NRestarts",
    )
    archive_timer = show(ARCHIVE_TIMER, "ActiveState,SubState,LastTriggerUSec")
    sample = {
        "sample": index,
        "utc": wall,
        "elapsed_seconds": now - started,
        "consumer": {
            "active": consumer.get("ActiveState", ""),
            "sub_state": consumer.get("SubState", ""),
            "main_pid": consumer_pid,
            "n_restarts": int(consumer.get("NRestarts", "0") or 0),
            "memory_bytes": int(consumer.get("MemoryCurrent", "0") or 0),
            "memory_peak_bytes": int(consumer.get("MemoryPeak", "0") or 0),
            "child_pids": children(consumer_pid),
            "elapsed": summary.get("elapsed_seconds"),
            "ok": summary.get("ok"),
            "exit_reason": summary.get("exit_reason"),
            "fatal_error": summary.get("fatal_error", ""),
            "gateway_instance_id": summary.get("gateway_instance_id", ""),
            "streams": totals.get("streams"),
            "accepted": totals.get("accepted"),
            "valid": totals.get("valid"),
            "order_books": totals.get("order_books"),
            "books_without_snapshot": books_without_snapshot,
            "snapshots": totals.get("snapshots"),
            "updates": totals.get("updates"),
            "events": totals.get("events"),
            "gaps": totals.get("gaps"),
            "resubscriptions": totals.get("resubscriptions"),
            "protocol_errors": totals.get("protocol_errors"),
            "rpc_errors": totals.get("rpc_errors"),
            "eofs": totals.get("eofs"),
            "status_errors": status.get("errors"),
            "status_timeouts": status.get("timeouts"),
            "identity_mismatches": status.get("identity_mismatches"),
            "seen_keys": len(status.get("seen_keys", [])),
            "missing_keys": len(status.get("missing_keys", [])),
            "active_subscriptions": status.get(
                "total_active_subscriptions"
            ),
            "markets_live": live,
            "markets_non_live": non_live,
            "markets_unknown": unknown,
            "gap_streams": gap_streams,
            **reference_summary,
        },
        "gateway": {
            "active": gateway.get("ActiveState", ""),
            "sub_state": gateway.get("SubState", ""),
            "main_pid": int(gateway.get("MainPID", "0") or 0),
            "n_restarts": int(gateway.get("NRestarts", "0") or 0),
            "memory_bytes": int(gateway.get("MemoryCurrent", "0") or 0),
            "memory_peak_bytes": int(gateway.get("MemoryPeak", "0") or 0),
        },
        "recorder": recorder,
        "archive": {
            "active": archive.get("ActiveState", ""),
            "sub_state": archive.get("SubState", ""),
            "result": archive.get("Result", ""),
            "exec_main_status": archive.get("ExecMainStatus", ""),
            "timer_active": archive_timer.get("ActiveState", ""),
            "timer_sub_state": archive_timer.get("SubState", ""),
            "last_trigger_utc": archive_timer.get("LastTriggerUSec", ""),
            **r3_backlog,
        },
        "resources": {
            "nproc": nproc,
            "load1": os.getloadavg()[0],
            "cpu_busy_percent_1core": cpu_percent,
            "cpu_steal_percent_1core": steal_percent,
            "mem_available_bytes": memory.get("MemAvailable", 0),
            "swap_free_bytes": memory.get("SwapFree", 0),
            "root_free_bytes": root_disk.free,
            "data_free_bytes": data_disk.free,
        },
    }
    return sample, cpu


def rolling_max(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return max(
        statistics.fmean(values[index : index + window])
        for index in range(len(values) - window + 1)
    )


def load_expected(config_path: Path) -> dict[str, int] | None:
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    spot = config.get("spot_symbols")
    usdm = config.get("usdm_symbols")
    if not isinstance(spot, list) or not isinstance(usdm, list):
        return None
    products = len(spot) + len(usdm)
    return {
        "spot": len(spot),
        "usdm": len(usdm),
        "products": products,
        "books": products,
        # Five subscriptions per USD-M product (four WS events plus typed
        # reference data), two Spot events per Spot product, and one book.
        "streams": products + 2 * len(spot) + 5 * len(usdm),
    }


def final_verdict(
    samples: list[dict[str, Any]], expected: dict[str, int] | None = None
) -> dict[str, Any]:
    if not samples:
        return {"status": "FAIL", "reasons": ["no samples"]}
    last = samples[-1]
    consumer = last["consumer"]
    reasons: list[str] = []
    warnings: list[str] = []
    if expected is None:
        warnings.append("config expected counts unavailable")
    expected_streams = expected["streams"] if expected else None
    expected_books = expected["books"] if expected else None
    expected_products = expected["products"] if expected else None

    if len(samples) < 2:
        reasons.append("insufficient samples")
    if last["gateway"]["active"] != "active":
        reasons.append("gateway is not active")
    if last["consumer"]["active"] != "active":
        reasons.append("consumer is not active")
    gateway_pids = {
        sample["gateway"]["main_pid"]
        for sample in samples
        if sample["gateway"].get("main_pid")
    }
    if len(gateway_pids) != 1 or not last["gateway"].get("main_pid"):
        reasons.append("gateway main PID changed")
    elif any(not sample["gateway"].get("main_pid") for sample in samples):
        warnings.append("transient systemctl timeout reading gateway main_pid")
    consumer_pids = {
        sample["consumer"]["main_pid"]
        for sample in samples
        if sample["consumer"].get("main_pid")
    }
    if len(consumer_pids) != 1 or not last["consumer"].get("main_pid"):
        reasons.append("consumer main PID changed")
    elif any(not sample["consumer"].get("main_pid") for sample in samples):
        warnings.append("transient systemctl timeout reading consumer main_pid")
    if any(sample["gateway"]["n_restarts"] for sample in samples):
        reasons.append("gateway restarted")
    if any(sample["consumer"]["n_restarts"] for sample in samples):
        reasons.append("consumer restarted")
    for unit in RECORDER_UNITS:
        pids = {
            sample["recorder"][unit]["main_pid"]
            for sample in samples
            if sample["recorder"][unit].get("main_pid")
        }
        if len(pids) != 1 or not last["recorder"][unit].get("main_pid"):
            reasons.append(f"{unit} PID changed or is zero")
        elif any(not sample["recorder"][unit].get("main_pid") for sample in samples):
            warnings.append(f"transient systemctl timeout reading {unit} main_pid")
        if any(sample["recorder"][unit]["n_restarts"] for sample in samples):
            reasons.append(f"{unit} restarted")
        if any(
            sample["recorder"][unit]["active"] != "active"
            for sample in samples
        ):
            reasons.append(f"{unit} was not active")
    recorder_drop_deltas: dict[str, int | None] = {}
    first = samples[0]
    for unit in RECORDER_UNITS:
        start_drops = first["recorder"][unit].get("dropped_total")
        end_drops = last["recorder"][unit].get("dropped_total")
        if isinstance(start_drops, int) and isinstance(end_drops, int):
            delta = end_drops - start_drops
            recorder_drop_deltas[unit] = delta
            if delta > 0:
                reasons.append(
                    f"{unit} dropped {delta} records during the run"
                )
        else:
            recorder_drop_deltas[unit] = None
            warnings.append(f"{unit} drop counter unavailable")
    if expected_streams is not None and consumer.get("streams") != expected_streams:
        reasons.append(
            f"streams={consumer.get('streams')} != {expected_streams}"
        )
    if expected_streams is not None and consumer.get("accepted") != expected_streams:
        reasons.append(
            f"accepted={consumer.get('accepted')} != {expected_streams}"
        )
    if expected_streams is not None and consumer.get("valid") != expected_streams:
        reasons.append(f"valid={consumer.get('valid')} != {expected_streams}")
    if expected_books is not None and consumer.get("order_books") != expected_books:
        reasons.append(
            f"order_books={consumer.get('order_books')} != {expected_books}"
        )
    if consumer.get("books_without_snapshot"):
        reasons.append(
            f"{consumer.get('books_without_snapshot')} order books have no snapshot"
        )
    if not consumer.get("updates"):
        reasons.append("no order-book updates")
    if not consumer.get("events"):
        reasons.append("no event deliveries")
    if consumer.get("protocol_errors"):
        reasons.append("protocol errors present")
    if consumer.get("rpc_errors"):
        reasons.append("RPC errors present")
    if consumer.get("identity_mismatches"):
        reasons.append("status identity mismatches present")
    if consumer.get("missing_keys"):
        reasons.append(f"status missing {consumer.get('missing_keys')} keys")
    if expected_products is not None and consumer.get("seen_keys") != expected_products:
        reasons.append(
            f"status seen_keys={consumer.get('seen_keys')} != {expected_products}"
        )
    if (
        expected_products is not None
        and consumer.get("markets_live") != expected_products
    ):
        reasons.append(
            f"markets_live={consumer.get('markets_live')} != {expected_products}"
        )
    if consumer.get("markets_non_live"):
        reasons.append(
            f"{consumer.get('markets_non_live')} status markets not Live"
        )
    if (
        expected is not None
        and consumer.get("reference_streams") is not None
        and consumer.get("reference_streams") != expected.get("usdm")
    ):
        reasons.append(
            f"reference_streams={consumer.get('reference_streams')} != {expected.get('usdm')}"
        )
    if (
        consumer.get("reference_streams")
        and consumer.get("reference_ok_streams")
        != consumer.get("reference_streams")
    ):
        reasons.append(
            f"reference_ok_streams={consumer.get('reference_ok_streams')} != "
            f"{consumer.get('reference_streams')}"
        )
    if last.get("archive", {}).get("migration_failures"):
        warnings.append(
            f"archive metadata migration failures={last['archive']['migration_failures']}"
        )
    if consumer.get("exit_reason") not in (None, "running"):
        reasons.append(f"consumer exit_reason={consumer.get('exit_reason')}")

    post_warmup = samples[2:]
    if expected_streams is not None and any(
        sample["consumer"]["accepted"] != expected_streams for sample in post_warmup
    ):
        reasons.append("accepted fell below expected count after warmup")
    if expected_streams is not None and any(
        sample["consumer"]["valid"] != expected_streams for sample in post_warmup
    ):
        reasons.append("valid fell below expected count after warmup")
    if any(sample["consumer"]["missing_keys"] for sample in post_warmup):
        reasons.append("status keys missing after warmup")
    if any(sample["consumer"]["identity_mismatches"] for sample in post_warmup):
        reasons.append("status mismatch after warmup")
    gaps = [sample["consumer"]["gaps"] or 0 for sample in samples]
    post_warmup_gaps = (
        max(gaps[2:]) - gaps[2] if len(gaps) > 2 else 0
    )
    if post_warmup_gaps > 0:
        warnings.append(f"{post_warmup_gaps} gap(s) after warmup")

    memory = [sample["resources"]["mem_available_bytes"] for sample in samples]
    if min(memory) < 1 * GIB:
        reasons.append("MemAvailable below 1 GiB")
    root_free = [sample["resources"]["root_free_bytes"] for sample in samples]
    if min(root_free) < 15 * GIB:
        reasons.append("root free space below 15 GiB")
    cpu_values = [
        sample["resources"]["cpu_busy_percent_1core"]
        for sample in samples
        if sample["resources"]["cpu_busy_percent_1core"] is not None
    ]
    cpu_10m = rolling_max(cpu_values, 20)
    if cpu_10m is not None and cpu_10m > 300.0:
        reasons.append(f"10-minute CPU mean {cpu_10m:.1f}% > 300%")
    if cpu_10m is None:
        warnings.append("CPU window shorter than 10 minutes; no mean computed")

    archive_success = any(
        sample["archive"]["result"] == "success" for sample in samples
    )
    if not archive_success:
        warnings.append("no successful archive service observation")
    if any(
        sample["archive"]["timer_active"] != "active" for sample in samples
    ):
        warnings.append("archive timer inactive in at least one sample")

    return {
        "status": "PASS" if not reasons else "FAIL",
        "reasons": reasons,
        "warnings": warnings,
        "samples": len(samples),
        "start_utc": samples[0]["utc"],
        "end_utc": samples[-1]["utc"],
        "duration_seconds": samples[-1]["elapsed_seconds"],
        "final_summary": last,
        "post_warmup_gaps": post_warmup_gaps,
        "min_mem_available_bytes": min(memory),
        "min_root_free_bytes": min(root_free),
        "max_cpu_10m_percent_1core": cpu_10m,
        "archive_success_observed": archive_success,
        "recorder_drop_deltas": recorder_drop_deltas,
        "expected": expected,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=7200.0)
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--config", default="/etc/bmdg/gateway.json")
    parser.add_argument("--samples", default="/var/lib/bmdg-consumer/g4-2h.jsonl")
    parser.add_argument(
        "--final", default="/var/lib/bmdg-consumer/g4-2h-final.json"
    )
    args = parser.parse_args()
    expected = load_expected(Path(args.config))
    samples_path = Path(args.samples)
    samples_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    started_utc = utc_now()
    samples: list[dict[str, Any]] = []
    previous_cpu = None
    index = 0
    with samples_path.open("w", encoding="utf-8") as handle:
        while True:
            sample, previous_cpu = collect(index, started, previous_cpu)
            samples.append(sample)
            handle.write(json.dumps(sample, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
            consumer = sample["consumer"]
            cpu_values_for_log = [
                s["resources"]["cpu_busy_percent_1core"] or 0 for s in samples
            ]
            cpu10m = rolling_max(cpu_values_for_log, 20)
            print(
                f"g4_sample={index} elapsed={sample['elapsed_seconds']:.0f}s "
                f"ok={consumer['ok']} accepted={consumer['accepted']} "
                f"valid={consumer['valid']} gaps={consumer['gaps']} "
                f"status_errors={consumer['status_errors']} "
                f"mem_avail={sample['resources']['mem_available_bytes']} "
                f"cpu10m={cpu10m}",
                flush=True,
            )
            index += 1
            if sample["elapsed_seconds"] >= args.duration:
                break
            time.sleep(max(0.0, args.interval))
    verdict = final_verdict(samples, expected)
    verdict["started_utc"] = started_utc
    verdict["samples_file"] = str(samples_path)
    Path(args.final).write_text(
        json.dumps(verdict, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        f"g4_final status={verdict['status']} samples={verdict['samples']} "
        f"reasons={verdict['reasons']} warnings={verdict['warnings']}",
        flush=True,
    )
    return 0 if verdict["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
