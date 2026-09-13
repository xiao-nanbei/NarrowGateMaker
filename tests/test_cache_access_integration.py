from __future__ import annotations

import json
import pickle
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from models import data_windows
from models import native_exchange_book_cache as native_cache
from models import replay_cache_components as replay_components
from models.audit import content_addressed_cache
from models.tick_data_types import HistoricalExchangeBookEvent
from research.families.f02_empirical_p3_touch.audit import p3_reach_time_cache
from research.families.f02_empirical_p3_touch.audit.p3_reach_time_surface import (
    ReachTimeLabelSurface,
)


def _window() -> data_windows.WindowData:
    return data_windows.WindowData(
        trades=pd.DataFrame({"price": [100.0]}),
        var_ts_ms=np.array([1], dtype=np.int64),
        var_ssq=np.array([0.1], dtype=np.float64),
        var_ti=None,
        var_retsq=None,
        bbo_data=None,
        l2_data=None,
    )


def test_window_pickle_roundtrip_and_invalid_payload(
    tmp_path: Path,
) -> None:

    path = tmp_path / "window_cache" / "window.pkl"
    data_windows._write_cached_window(path, _window())

    restored = data_windows._load_cached_window(path)
    assert isinstance(restored, data_windows.WindowData)
    pd.testing.assert_frame_equal(restored.trades, _window().trades)
    np.testing.assert_array_equal(restored.var_ssq, _window().var_ssq)

    invalid = path.with_name("invalid.pkl")
    with invalid.open("wb") as handle:
        pickle.dump({"not": "a WindowData"}, handle)
    assert data_windows._load_cached_window(invalid) is None

    component_path = tmp_path / "window_cache" / "overlay.pkl"
    component = data_windows.WindowModelOverlay(
        ml_data=(np.array([0.5]),),
        toxicity_horizon_s=10,
    )
    data_windows._write_component(component_path, component)
    restored_component = data_windows._load_component(
        component_path,
        data_windows.WindowModelOverlay,
    )
    assert isinstance(restored_component, data_windows.WindowModelOverlay)

    assert isinstance(data_windows._load_cached_window(path), data_windows.WindowData)


def _market_context_fixture(
    tmp_path: Path,
) -> tuple[dict[str, Any], replay_components.MarketContextPayload]:
    source = tmp_path / "source.parquet"
    source.write_bytes(b"source")
    references = (replay_components.file_reference(source, role="normalized_bbo"),)
    identity = replay_components.market_context_identity(
        symbol="BTCUSDC",
        day="2099-01-02",
        warmup_days=1,
        source_references=references,
        book_source_authority="native_strict",
        book_dataset_version="fixture.v1",
        transform_identity_sha256="a" * 64,
    )
    payload = replay_components.MarketContextPayload(
        trades=pd.DataFrame(
            {
                "transact_time": pd.Series([1], dtype="int64"),
                "price": pd.Series([100.0], dtype="float64"),
                "quantity": pd.Series([0.001], dtype="float64"),
                "is_buyer_maker": pd.Series([True], dtype="bool"),
            }
        ),
        var_ts_ms=np.array([1], dtype=np.int64),
        var_ssq=np.array([0.1], dtype=np.float64),
        var_ti=None,
        var_retsq=None,
        metadata={"execution_trade_source": "trades"},
        source_references=references,
    )
    return identity, payload


def test_replay_component_roundtrip_through_existing_alias(
    tmp_path: Path,
) -> None:
    identity, payload = _market_context_fixture(tmp_path)
    physical = tmp_path / "cold"
    physical.mkdir()
    logical = tmp_path / "hot_alias"
    logical.symlink_to(physical, target_is_directory=True)

    artifact = replay_components.write_market_context(
        cache_root=logical,
        identity=identity,
        payload=payload,
    )
    identity_sha256 = replay_components.canonical_sha256(identity)
    assert artifact.directory.is_relative_to(physical)

    restored = replay_components.load_market_context(cache_root=logical, identity=identity)
    assert restored is not None
    pd.testing.assert_frame_equal(restored.trades, payload.trades)

    missing = dict(identity)
    missing["day"] = "2099-01-03"
    assert replay_components.load_market_context(cache_root=logical, identity=missing) is None

    overlay_identity = replay_components.model_overlay_identity(
        symbol="BTCUSDC",
        day="2099-01-02",
        market_context_identity_sha256=identity_sha256,
        feature_source_identity=(),
        model_bundle_identity=(),
        toxicity_horizon_s=10,
        cross_market_enabled=False,
        run_ml_inference=True,
    )
    replay_components.write_model_overlay(
        cache_root=logical,
        identity=overlay_identity,
        ml_data=(np.array([0.25]),),
    )
    assert replay_components.load_model_overlay(
        cache_root=logical,
        identity=overlay_identity,
    ) is not None


def _native_event(source: Path) -> HistoricalExchangeBookEvent:
    return HistoricalExchangeBookEvent(
        market_id="binance_futures:perpetual:BTCUSDC",
        event_type="snapshot",
        exchange_ts_ns=1_767_322_800_001_000_000,
        exchange_ts_source="transaction",
        local_receive_ts_ns=1_767_322_800_002_000_000,
        event_time_ns=1_767_322_800_000_000_000,
        transaction_time_ns=1_767_322_800_001_000_000,
        last_update_id=100,
        levels=(("bid", 900_000, 1.25), ("ask", 900_002, 2.5)),
        source=str(source),
        source_ordinal=1,
    )


def test_native_book_hit_avoids_parse_and_invalid_manifest_rebuilds(
    tmp_path: Path,
) -> None:
    source = tmp_path / "raw" / "2026-01-02" / "03" / "book.parquet.zst"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"source")
    identity = native_cache.native_book_hour_identity(
        source_path=source,
        symbol="BTCUSDC",
        exchange="binance_futures",
        market_id="binance_futures:perpetual:BTCUSDC",
        tick_size=0.1,
        parser_identity_sha256="b" * 64,
    )
    cache_root = tmp_path / "cache"
    first = native_cache.ensure_native_book_hour_cache(
        cache_root=cache_root,
        identity=identity,
        events_factory=lambda: iter((_native_event(source),)),
    )
    assert not first.cache_hit

    second = native_cache.ensure_native_book_hour_cache(
        cache_root=cache_root,
        identity=identity,
        events_factory=lambda: (_ for _ in ()).throw(AssertionError("unexpected parse")),
    )
    assert second.cache_hit

    manifest = json.loads(second.manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = "invalid"
    second.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid cache must rebuild"):
        native_cache.ensure_native_book_hour_cache(
            cache_root=cache_root,
            identity=identity,
            events_factory=lambda: (_ for _ in ()).throw(
                RuntimeError("invalid cache must rebuild")
            ),
        )


def test_content_addressed_cache_roundtrip_and_miss_through_existing_alias(
    tmp_path: Path,
) -> None:
    physical = tmp_path / "cold"
    physical.mkdir()
    logical = tmp_path / "hot_alias"
    logical.symlink_to(physical, target_is_directory=True)
    cache = content_addressed_cache.ParquetContentAddressedCache(
        logical,
        namespace="request_state",
    )
    identity = {"day": "2026-07-25", "source": "fixture"}

    stored = cache.store(identity, pd.DataFrame({"x": [1, 2]}))
    assert not stored.hit
    pd.testing.assert_frame_equal(cache.load(identity).frame, pd.DataFrame({"x": [1, 2]}))
    assert cache.load({"day": "missing"}) is None

    directory_cache = content_addressed_cache.DirectoryContentAddressedCache(
        logical,
        namespace="sparse_tape",
    )
    directory_identity = {"day": "2026-07-26", "source": "fixture"}

    def build(payload_dir: Path) -> dict[str, int]:
        (payload_dir / "rows.bin").write_bytes(b"rows")
        return {"rows": 1}

    built = directory_cache.get_or_build(directory_identity, build)
    assert not built.hit
    assert directory_cache.load(directory_identity) is not None
    assert directory_cache.load({"day": "missing"}) is None


def test_p3_label_cache_roundtrip_and_invalid_manifest(
    tmp_path: Path,
) -> None:
    cache_key = "c" * 64
    path = tmp_path / "p3_touch_reaches_v1" / "labels.npz"
    surface = ReachTimeLabelSurface(
        time_upper_ms=np.array([100, 200], dtype=np.int32),
        buy_cumulative_reach_ticks=np.array([[0, 1]], dtype=np.int16),
        sell_cumulative_reach_ticks=np.array([[1, 1]], dtype=np.int16),
    )
    p3_reach_time_cache.write_label_cache(
        path,
        origins_ms=np.array([1_000], dtype=np.int64),
        surface=surface,
        cache_key=cache_key,
        identity={"day": "2026-01-01"},
    )

    origins, restored, _ = p3_reach_time_cache.load_label_cache(
        path,
        expected_cache_key=cache_key,
    )
    np.testing.assert_array_equal(origins, np.array([1_000], dtype=np.int64))
    np.testing.assert_array_equal(
        restored.buy_cumulative_reach_ticks,
        surface.buy_cumulative_reach_ticks,
    )

    manifest_path = path.with_suffix(path.suffix + ".manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["rows"] = 2
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical hash mismatch"):
        p3_reach_time_cache.load_label_cache(path, expected_cache_key=cache_key)
