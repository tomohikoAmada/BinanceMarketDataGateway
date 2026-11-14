from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

TOOLS = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "acceptance_monitor", TOOLS / "acceptance_monitor.py"
)
assert SPEC is not None and SPEC.loader is not None
monitor = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = monitor
SPEC.loader.exec_module(monitor)


def test_rolling_max_requires_a_full_window() -> None:
    assert monitor.rolling_max([], 20) is None
    assert monitor.rolling_max([1.0] * 19, 20) is None
    assert monitor.rolling_max([1.0] * 20, 20) == 1.0
    assert monitor.rolling_max([1.0] * 19 + [21.0], 20) == 2.0


def test_read_last_jsonl_ignores_partial_tail(tmp_path: Path) -> None:
    path = tmp_path / "status.jsonl"
    path.write_text(
        '{"dropped_total": 1, "records": 10}\n'
        '{"dropped_total": 2, "records": 20}\n'
        '{"dropped_total": 3, "records": ',
        encoding="utf-8",
    )
    assert monitor.read_last_jsonl(path) == {
        "dropped_total": 2,
        "records": 20,
    }


def test_read_last_jsonl_missing_file_is_unknown(tmp_path: Path) -> None:
    assert monitor.read_last_jsonl(tmp_path / "missing.jsonl") == {}


def test_expected_counts_follow_configured_five_pairs(tmp_path: Path) -> None:
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    path = tmp_path / "gateway.json"
    path.write_text(
        json.dumps({"spot_symbols": symbols, "usdm_symbols": symbols}),
        encoding="utf-8",
    )
    assert monitor.load_expected(path) == {
        "spot": 5,
        "usdm": 5,
        "products": 10,
        "books": 10,
        "streams": 45,
    }


def test_summarize_reference_streams_and_verdict_checks() -> None:
    summary = {
        "streams": [
            {
                "key": "binance:usdm:BTCUSDT:reference_data",
                "stream": "reference_data",
                "reference_ok": False,
                "reference_issues": ["OPEN_INTEREST:status=ERROR"],
                "reference_source_states": {
                    "REFERENCE_SOURCE_KIND_OPEN_INTEREST": {
                        "status": "ERROR",
                        "receive_age_seconds": 650.0,
                    },
                    "REFERENCE_SOURCE_KIND_INSTRUMENT": {
                        "status": "missing",
                        "receive_age_seconds": None,
                    },
                },
            }
        ]
    }
    ref = monitor.summarize_reference_streams(summary)
    assert ref["reference_streams"] == 1
    assert ref["reference_ok_streams"] == 0
    assert ref["reference_error_sources"] == 1
    assert ref["reference_missing_sources"] == 1
    assert ref["reference_max_periodic_receive_age_seconds"] == 650.0

    sample = {
        "utc": "2026-11-13T00:00:00Z",
        "elapsed_seconds": 30.0,
        "gateway": {"active": "active", "main_pid": 10, "n_restarts": 0},
        "recorder": {
            "sbmdr-recorder@spot": {"active": "active", "main_pid": 11, "n_restarts": 0, "dropped_total": 0},
            "sbmdr-recorder@usdm": {"active": "active", "main_pid": 12, "n_restarts": 0, "dropped_total": 0},
        },
        "consumer": {
            "active": "active",
            "main_pid": 13,
            "n_restarts": 0,
            "streams": 45,
            "accepted": 45,
            "valid": 45,
            "order_books": 10,
            "books_without_snapshot": 0,
            "updates": 10,
            "events": 10,
            "protocol_errors": 0,
            "rpc_errors": 0,
            "identity_mismatches": 0,
            "missing_keys": 0,
            "seen_keys": 10,
            "markets_live": 10,
            "markets_non_live": 0,
            "gaps": 0,
            "exit_reason": "running",
            **ref,
        },
        "resources": {
            "mem_available_bytes": 4 * monitor.GIB,
            "root_free_bytes": 30 * monitor.GIB,
            "cpu_busy_percent_1core": 10.0,
        },
        "archive": {"result": "success", "timer_active": "active", "migration_failures": 0},
    }
    verdict = monitor.final_verdict([sample, sample], expected={"streams": 45, "books": 10, "products": 10, "usdm": 5})
    assert verdict["status"] == "FAIL"
    assert any("reference_streams=1 != 5" in r for r in verdict["reasons"])
    assert any("reference_ok_streams=0 != 1" in r for r in verdict["reasons"])


def test_read_r3_backlog_counts_pending_partitions(tmp_path: Path) -> None:
    hot = tmp_path / "hot_spot"
    cold = tmp_path / "cold_spot"
    p_pending = hot / "partitions" / "2026-11-12"
    (p_pending / "state").mkdir(parents=True)
    (p_pending / "state" / "catalog.sqlite").write_bytes(b"abcde")
    (p_pending / "partition.json").write_text(
        json.dumps({"state": "closed", "closed_utc_seconds": 1000.0}),
        encoding="utf-8",
    )
    p_migrated = hot / "partitions" / "2026-11-11"
    (p_migrated / "state").mkdir(parents=True)
    (p_migrated / "partition.json").write_text(
        json.dumps({"state": "closed", "closed_utc_seconds": 500.0}),
        encoding="utf-8",
    )
    (cold / "partitions" / "2026-11-11" / "state").mkdir(parents=True)
    (cold / "partitions" / "2026-11-11" / "partition.json").write_text("{}", encoding="utf-8")
    (cold / "partitions" / "2026-11-11" / "state" / "catalog.sqlite").write_bytes(b"123456")

    archive_log = tmp_path / "archive.jsonl"
    archive_log.write_text(
        json.dumps(
            {
                "finish_utc_seconds": 1200.0,
                "failed": False,
                "markets": {"spot": {"migration_exit": 0}},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    roots = tmp_path / "roots.json"
    roots.write_text(json.dumps({"spot": str(hot)}), encoding="utf-8")
    meta_roots = tmp_path / "meta.json"
    meta_roots.write_text(json.dumps({"spot": str(cold)}), encoding="utf-8")

    backlog = monitor.read_r3_backlog(
        archive_log=archive_log,
        roots_path=roots,
        metadata_roots_path=meta_roots,
        now_seconds=1300.0,
    )
    assert backlog["pending_migrate_partitions"] == 1
    assert backlog["oldest_pending_age_seconds"] == 300.0
    assert backlog["migration_failures"] == 0
    assert backlog["cold_metadata_bytes"] == 6
    assert backlog["hot_metadata_bytes"] > 5

    # Production migrate_metadata layout places <partition_id> directly under cold_root
    direct_cold = tmp_path / "direct_cold_spot"
    (direct_cold / "2026-11-11" / "state").mkdir(parents=True)
    (direct_cold / "2026-11-11" / "partition.json").write_text("{}", encoding="utf-8")
    (direct_cold / "2026-11-11" / "state" / "catalog.sqlite").write_bytes(b"1234567")
    meta_roots.write_text(json.dumps({"spot": str(direct_cold)}), encoding="utf-8")
    direct_backlog = monitor.read_r3_backlog(
        archive_log=archive_log,
        roots_path=roots,
        metadata_roots_path=meta_roots,
        now_seconds=1300.0,
    )
    assert direct_backlog["pending_migrate_partitions"] == 1
    assert direct_backlog["cold_metadata_bytes"] == 7


def test_collect_preserves_gateway_status_and_recorder_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        monitor,
        "read_cpu",
        lambda: {"total": 200, "idle": 100, "steal": 10},
    )
    monkeypatch.setattr(monitor, "read_meminfo", lambda: {"MemAvailable": 2048, "SwapFree": 1024})
    monkeypatch.setattr(
        monitor.shutil,
        "disk_usage",
        lambda _path: type("Disk", (), {"free": 100_000})(),
    )
    monkeypatch.setattr(
        monitor,
        "read_summary",
        lambda: {
            "elapsed_seconds": 60.0,
            "ok": True,
            "exit_reason": "running",
            "gateway_instance_id": "gw-1",
            "totals": {"streams": 45, "accepted": 45, "valid": 45},
            "status": {
                "errors": 0,
                "timeouts": 0,
                "identity_mismatches": 0,
                "seen_keys": [f"k{i}" for i in range(10)],
                "missing_keys": [],
                "total_active_subscriptions": 45,
                "markets": {"k0": {"state": "STREAM_LIFECYCLE_STATE_LIVE"}},
            },
            "streams": [],
        },
    )
    monkeypatch.setattr(
        monitor,
        "read_r3_backlog",
        lambda: {
            "pending_migrate_partitions": 0,
            "oldest_pending_age_seconds": None,
            "migration_failures": 0,
            "last_archive_finish_utc_seconds": 100.0,
            "last_archive_failed": False,
            "hot_metadata_bytes": 10,
            "cold_metadata_bytes": 0,
        },
    )
    monkeypatch.setattr(
        monitor,
        "show",
        lambda unit, _props: (
            {
                "ActiveState": "active",
                "SubState": "running",
                "MainPID": "123",
                "NRestarts": "0",
                "MemoryCurrent": "1024",
                "MemoryPeak": "2048",
                "CPUUsageNSec": "100",
                "Result": "success",
                "ExecMainStatus": "0",
                "LastTriggerUSec": "now",
            }
            if unit
            else {}
        ),
    )
    monkeypatch.setattr(monitor, "children", lambda _pid: [])
    monkeypatch.setattr(
        monitor,
        "read_last_jsonl",
        lambda path: {
            "records": 999 if "spot" in str(path) else 1999,
            "dropped_total": 0,
            "queued_records": 0,
            "queued_payload_bytes": 0,
            "seal_queue_depth": 0,
            "max_durability_ns": 1000,
            "rss_bytes": 4096,
            "cpu_percent": 5.0,
            "elapsed_seconds": 60.0,
        },
    )
    sample, _cpu = monitor.collect(
        1, 100.0, {"total": 100, "idle": 50, "steal": 0}
    )
    assert sample["consumer"]["seen_keys"] == 10
    assert sample["consumer"]["missing_keys"] == 0
    assert sample["consumer"]["status_errors"] == 0
    assert sample["consumer"]["status_timeouts"] == 0
    assert sample["consumer"]["identity_mismatches"] == 0
    assert sample["consumer"]["active_subscriptions"] == 45
    assert sample["recorder"]["sbmdr-recorder@spot"]["records"] == 999
    assert sample["recorder"]["sbmdr-recorder@usdm"]["records"] == 1999
    assert sample["recorder"]["sbmdr-recorder@spot"]["dropped_total"] == 0
