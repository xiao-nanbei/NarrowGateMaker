from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from models import cache_tier_lru
from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_b0_mechanics_adapter_v1 as b0_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_offline_day_input_cache_v1 as cache_mod,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("ascii")).hexdigest()


@dataclasses.dataclass(frozen=True)
class _ReplayInputs:
    utc_day: str
    continuation_day: str
    trades: pd.DataFrame
    var_ts_ms: np.ndarray
    var_ssq: np.ndarray
    var_ti: np.ndarray
    var_retsq: np.ndarray
    bbo_data: HistoricalBBOData
    l2_data: HistoricalL2Data
    ml_data: tuple[Any, ...]
    params: dict[str, Any]
    market_window_identity_sha256: str
    model_overlay_identity_sha256: str
    latency_identity_sha256: str
    queue_random_identity_sha256: str
    replay_input_receipt_sha256: str


@pytest.fixture
def governed_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    hot = tmp_path / "hot"
    cold = tmp_path / "cold"
    hot.mkdir()
    cold.mkdir()
    monkeypatch.setenv("NARROWGATE_CACHE_HOT_ROOT", str(hot))
    monkeypatch.setenv("NARROWGATE_CACHE_COLD_ROOT", str(cold))
    monkeypatch.setenv(
        "NARROWGATE_CACHE_LEDGER_PATH",
        str(hot / ".cache_tier_lru" / "access_ledger.sqlite3"),
    )
    root = cold / "replay_dag" / cache_mod.CACHE_IDENTITY
    root.parent.mkdir()
    return root


def _source_replay() -> _ReplayInputs:
    trades = pd.DataFrame(
        {
            "trade_id": np.arange(4, dtype=np.int64),
            "price": np.array([100.0, 100.1, 100.0, 99.9], dtype=np.float64),
            "quantity": np.full(4, 0.001, dtype=np.float32),
            "transact_time": np.array(
                [1_786_224_000_100, 1_786_224_001_100, 1_786_310_400_100, 1_786_310_401_100],
                dtype=np.int64,
            ),
            "is_buyer_maker": np.array([True, False, True, False], dtype=np.bool_),
        }
    )
    bbo_ts = np.arange(1_786_137_600_000, 1_786_137_606_000, 1_000, dtype=np.int64)
    bbo = HistoricalBBOData(
        ts_ms=bbo_ts,
        best_bid=np.linspace(99.8, 100.3, len(bbo_ts), dtype=np.float64),
        best_ask=np.linspace(100.0, 100.5, len(bbo_ts), dtype=np.float64),
        bid_qty=np.full(len(bbo_ts), 1.0, dtype=np.float64),
        ask_qty=np.full(len(bbo_ts), 1.1, dtype=np.float64),
        source="fixture_D_minus_1_D_D_plus_1",
    )
    levels = 2
    l2 = HistoricalL2Data(
        ts_ms=bbo_ts,
        bid_px=np.column_stack((bbo.best_bid, bbo.best_bid - 0.1)),
        bid_qty=np.full((len(bbo_ts), levels), 2.0, dtype=np.float64),
        ask_px=np.column_stack((bbo.best_ask, bbo.best_ask + 0.1)),
        ask_qty=np.full((len(bbo_ts), levels), 2.1, dtype=np.float64),
        source="fixture_D_minus_1_D_D_plus_1",
    )
    ready = np.array(
        [1_786_224_010_000, 1_786_224_020_000, 1_786_310_410_000, 1_786_310_420_000],
        dtype=np.int64,
    )
    ml_data = (
        ready,
        np.array([0.1, 0.2, 0.3, 0.4], dtype=np.float64),
        {
            "feature_a": np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float32),
            "feature_b": np.array([4.0, 3.0, 2.0, 1.0], dtype=np.float32),
        },
    )
    var_ts = np.array([1_786_137_601_000, 1_786_137_602_000, 1_786_137_603_000])
    return _ReplayInputs(
        utc_day="2026-08-08",
        continuation_day="2026-08-09",
        trades=trades,
        var_ts_ms=var_ts,
        var_ssq=np.array([0.1, 0.2, 0.3], dtype=np.float64),
        var_ti=np.array([1.0, 1.1, 1.2], dtype=np.float64),
        var_retsq=np.array([0.0, 0.01, 0.02], dtype=np.float64),
        bbo_data=bbo,
        l2_data=l2,
        ml_data=ml_data,
        params={"replay_event_clock": "merged", "rng_seed": 42},
        market_window_identity_sha256=_sha("market"),
        model_overlay_identity_sha256=_sha("overlay"),
        latency_identity_sha256=_sha("latency"),
        queue_random_identity_sha256=_sha("queue"),
        replay_input_receipt_sha256=_sha("replay"),
    )


def _context() -> tuple[
    cache_mod.DayInputCacheIdentity,
    cache_mod.ReplayDayInputSchema,
    cache_mod.ReplayDayInputArrays,
]:
    return cache_mod.target_day_context_from_replay_inputs(
        _source_replay(),
        source_receipts={component: _sha(component) for component in cache_mod.SOURCE_COMPONENTS},
        clock_identity="exchange_time_merged_100ms_D_minus_1_D_D_plus_1",
        clock_identity_sha256=_sha("clock"),
        engine_identity="python_authoritative_modeled_queue",
        engine_identity_sha256=_sha("engine"),
        ml_main_array_count=2,
    )


def test_atomic_admission_opens_read_only_mmaps_and_rebuilds_replay_inputs(
    governed_root: Path,
) -> None:
    identity, schema, inputs = _context()
    binding = cache_mod.admit_replay_day_inputs(
        governed_root,
        identity=identity,
        schema=schema,
        inputs=inputs,
    )
    assert binding.estimated_size_bytes > 0
    entry = governed_root / "entries" / binding.cache_identity_sha256
    manifest_path = entry / "manifest.json"
    manifest_before = manifest_path.read_bytes()
    manifest = json.loads(manifest_before)
    assert manifest["estimated_size_bytes"] == binding.estimated_size_bytes
    assert manifest["request"]["continuation_day"] == "2026-08-09"
    assert manifest["economic_outcomes_read"] is False

    with cache_mod.open_replay_day_inputs(
        governed_root,
        identity=identity,
        schema=schema,
        expected=binding,
    ) as opened:
        for component in cache_mod.COMPONENTS:
            for array in opened.component(component).values():
                assert isinstance(array, np.memmap)
                assert array.flags.writeable is False
        with pytest.raises(ValueError):
            opened.component("l2")["bid_px"][0, 0] = 0.0
        rebuilt = opened.to_replay_inputs(_ReplayInputs)
        authoritative = opened.to_replay_inputs(b0_adapter._ReplayInputs)
        assert list(rebuilt.trades.columns) == list(cache_mod.TRADES_COLUMNS)
        assert all(pd.api.types.is_numeric_dtype(dtype) for dtype in rebuilt.trades.dtypes)
        assert isinstance(rebuilt.bbo_data, HistoricalBBOData)
        assert isinstance(rebuilt.l2_data, HistoricalL2Data)
        assert rebuilt.bbo_data.ts_ms.flags.writeable is False
        assert rebuilt.l2_data.bid_px.flags.writeable is False
        assert rebuilt.ml_data[0].flags.writeable is False
        assert rebuilt.utc_day == "2026-08-08"
        assert rebuilt.continuation_day == "2026-08-09"
        assert rebuilt.market_window_identity_sha256 == _sha("market")
        assert authoritative.replay_input_receipt_sha256 == _sha("replay")

    with cache_mod.open_replay_day_inputs(
        governed_root,
        identity=identity,
        schema=schema,
        expected=binding,
    ):
        pass
    assert manifest_path.read_bytes() == manifest_before
    assert not (governed_root / "lru_access").exists()
    config = cache_tier_lru.CacheTierConfig.from_environment()
    records = cache_tier_lru.list_artifacts(config)
    matching = [row for row in records if row.identity_sha256 == binding.cache_identity_sha256]
    assert len(matching) == 1
    assert matching[0].tier == "cold"
    assert matching[0].relative_path.startswith("replay_dag/")
    assert matching[0].access_count >= 3


def test_missing_column_and_shape_drift_fail_before_admission(governed_root: Path) -> None:
    identity, schema, inputs = _context()
    missing_bbo = dataclasses.replace(
        inputs,
        bbo={name: value for name, value in inputs.bbo.items() if name != "ask_qty"},
    )
    with pytest.raises(cache_mod.OfflineDayInputCacheError, match="missing=.*ask_qty"):
        cache_mod.admit_replay_day_inputs(
            governed_root,
            identity=identity,
            schema=schema,
            inputs=missing_bbo,
        )
    bad_l2 = dict(inputs.l2)
    bad_l2["ask_qty"] = bad_l2["ask_qty"][:, :1]
    with pytest.raises(cache_mod.OfflineDayInputCacheError, match="matrix shapes differ"):
        cache_mod.admit_replay_day_inputs(
            governed_root,
            identity=identity,
            schema=schema,
            inputs=dataclasses.replace(inputs, l2=bad_l2),
        )
    assert not (governed_root / "admissions").exists()


def test_content_corruption_and_source_receipt_drift_fail_closed(governed_root: Path) -> None:
    identity, schema, inputs = _context()
    binding = cache_mod.admit_replay_day_inputs(
        governed_root,
        identity=identity,
        schema=schema,
        inputs=inputs,
    )
    entry = governed_root / "entries" / binding.cache_identity_sha256
    manifest = json.loads((entry / "manifest.json").read_text(encoding="ascii"))
    l2_row = next(
        row for row in manifest["arrays"] if row["component"] == "l2" and row["name"] == "bid_px"
    )
    path = entry / l2_row["file"]
    with path.open("r+b") as handle:
        handle.seek(-1, os.SEEK_END)
        original = handle.read(1)
        handle.seek(-1, os.SEEK_END)
        handle.write(bytes([original[0] ^ 1]))
    with pytest.raises(cache_mod.OfflineDayInputCacheError, match="file hash drifted"):
        cache_mod.open_replay_day_inputs(
            governed_root,
            identity=identity,
            schema=schema,
            expected=binding,
        )

    drifted_receipts = dict(identity.source_receipts)
    drifted_receipts["trades"] = _sha("different trades receipt")
    drifted = dataclasses.replace(
        identity,
        source_receipts=tuple(drifted_receipts.items()),
    )
    with pytest.raises(cache_mod.OfflineDayInputCacheError, match="not atomically admitted"):
        cache_mod.open_replay_day_inputs(
            governed_root,
            identity=drifted,
            schema=schema,
        )


def test_concurrent_build_uses_one_writer(
    governed_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    identity, schema, inputs = _context()
    original = cache_mod.ReplayDayInputCache._write_entry
    writes = 0
    writes_lock = threading.Lock()

    def counted_write(self: cache_mod.ReplayDayInputCache, **kwargs: Any) -> tuple[Path, str]:
        nonlocal writes
        with writes_lock:
            writes += 1
        return original(self, **kwargs)

    monkeypatch.setattr(cache_mod.ReplayDayInputCache, "_write_entry", counted_write)

    def build() -> cache_mod.DayInputCacheBinding:
        return cache_mod.admit_replay_day_inputs(
            governed_root,
            identity=identity,
            schema=schema,
            inputs=inputs,
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        bindings = list(executor.map(lambda _: build(), range(4)))
    assert len(set(bindings)) == 1
    assert writes == 1
    assert len(list((governed_root / "entries").iterdir())) == 1
    assert len(list((governed_root / "admissions").iterdir())) == 1
    assert not list(governed_root.rglob("*.partial"))


def test_same_immutable_request_rejects_different_content(governed_root: Path) -> None:
    identity, schema, inputs = _context()
    cache_mod.admit_replay_day_inputs(
        governed_root,
        identity=identity,
        schema=schema,
        inputs=inputs,
    )
    changed_trades = dict(inputs.trades)
    changed_trades["price"] = changed_trades["price"].copy()
    changed_trades["price"][0] += 0.1
    with pytest.raises(
        cache_mod.OfflineDayInputCacheError,
        match="immutable day input request resolved to different content",
    ):
        cache_mod.admit_replay_day_inputs(
            governed_root,
            identity=identity,
            schema=schema,
            inputs=dataclasses.replace(inputs, trades=changed_trades),
        )


def test_cache_root_must_be_governed_replay_dag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hot = tmp_path / "hot"
    cold = tmp_path / "cold"
    hot.mkdir()
    cold.mkdir()
    monkeypatch.setenv("NARROWGATE_CACHE_HOT_ROOT", str(hot))
    monkeypatch.setenv("NARROWGATE_CACHE_COLD_ROOT", str(cold))
    monkeypatch.setenv(
        "NARROWGATE_CACHE_LEDGER_PATH",
        str(hot / ".cache_tier_lru" / "access_ledger.sqlite3"),
    )
    with pytest.raises(cache_mod.OfflineDayInputCacheError, match="governed replay_dag"):
        cache_mod.ReplayDayInputCache(cold / "unmanaged" / "f05")
