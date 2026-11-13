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
