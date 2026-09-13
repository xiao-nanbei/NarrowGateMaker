from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models.replay_cache_components import (
    MARKET_CONTEXT_SCHEMA,
    MarketContextPayload,
    ReplayCacheIntegrityError,
    canonical_sha256,
    file_reference,
    load_market_context,
    load_model_overlay,
    market_context_identity,
    market_context_parity_report,
    model_overlay_identity,
    model_overlay_parity_report,
    references_sha256,
    write_market_context,
    write_model_overlay,
)
from models.replay_cache_dag import REPLAY_WINDOW_CACHE_GRAPH_V2


def test_v2_dag_has_only_three_persistent_boundaries() -> None:
    graph = REPLAY_WINDOW_CACHE_GRAPH_V2
    graph.validate()
    persistent = {node.name for node in graph.nodes if node.materialization == "persistent"}
    assert persistent == {
        "native_book_hour",
        "market_context_day_v2",
        "model_overlay_day",
    }
    by_name = {node.name: node for node in graph.nodes}
    assert by_name["window_data"].materialization == "ephemeral"
    forbidden = by_name["action_dependent_replay_state"]
    assert forbidden.materialization == "forbidden"
    assert forbidden.strategy_dependent is True


def _context_fixture(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "BTCUSDC-bbo-2099-01-02.parquet"
    source.write_bytes(b"authoritative-source-placeholder")
    references = (file_reference(source, role="normalized_bbo"),)
    identity = market_context_identity(
        symbol="BTCUSDC",
        day="2099-01-02",
        warmup_days=1,
        source_references=references,
        book_source_authority="native_strict",
        book_dataset_version="fixture.v1",
        transform_identity_sha256="a" * 64,
    )
    trades = pd.DataFrame(
        {
            "transact_time": pd.Series([1, 2, 3], dtype="int64"),
            "price": pd.Series([100.0, 100.1, 100.2], dtype="float64"),
            "quantity": pd.Series([0.001, 0.002, 0.001], dtype="float64"),
            "is_buyer_maker": pd.Series([True, False, True], dtype="bool"),
        }
    )
    payload = MarketContextPayload(
        trades=trades,
        var_ts_ms=np.array([1, 2, 3], dtype=np.int64),
        var_ssq=np.array([0.1, 0.2, 0.3], dtype=np.float64),
        var_ti=None,
        var_retsq=np.array([0.01, 0.02, 0.03], dtype=np.float64),
        metadata={
            "execution_trade_source": "trades",
            "book_source_authority": "native_strict",
            "book_dataset_version": "fixture.v1",
            "formal_lifecycle_replay_eligible": True,
            "provider_sensitivity_replay_eligible": False,
            "exact_queue_policy_eligible": True,
        },
        source_references=references,
    )
    return identity, payload


def test_market_context_directory_round_trip_and_hash_reuse(tmp_path: Path) -> None:
    identity, payload = _context_fixture(tmp_path)
    cache_root = tmp_path / "cache"
    first = write_market_context(
        cache_root=cache_root,
        identity=identity,
        payload=payload,
    )
    second = write_market_context(
        cache_root=cache_root,
        identity=identity,
        payload=payload,
    )
    assert first.cache_hit is False
    assert second.cache_hit is True
    assert first.directory == second.directory
    assert first.directory.name == canonical_sha256(identity)
    assert sorted(path.name for path in first.directory.iterdir()) == [
        "manifest.json",
        "rolling_arrays.npz",
        "source_references.json",
        "trades.parquet",
    ]
    assert not list(first.directory.glob("*.pkl"))
    manifest = json.loads(first.manifest_path.read_text())
    assert manifest["identity_sha256"] == canonical_sha256(identity)
    assert "bbo_data" not in json.dumps(manifest)
    assert "l2_data" not in json.dumps(manifest)
    assert set(MARKET_CONTEXT_SCHEMA["forbidden_fields"]).isdisjoint(manifest["metadata"])

    restored = load_market_context(cache_root=cache_root, identity=identity)
    assert restored is not None
    pd.testing.assert_frame_equal(restored.trades, payload.trades)
    np.testing.assert_array_equal(restored.var_ts_ms, payload.var_ts_ms)
    np.testing.assert_array_equal(restored.var_ssq, payload.var_ssq)
    assert restored.var_ti is None
    np.testing.assert_array_equal(restored.var_retsq, payload.var_retsq)
    assert market_context_parity_report(payload, restored)["passed"] is True


def test_source_identity_survives_relocation_and_mtime_touch(
    tmp_path: Path,
) -> None:
    identity, payload = _context_fixture(tmp_path / "original")
    cache_root = tmp_path / "cache"
    artifact = write_market_context(
        cache_root=cache_root,
        identity=identity,
        payload=payload,
    )
    original_path = Path(payload.source_references[0]["locator"]["path"])
    relocated_path = tmp_path / "relocated" / original_path.name
    relocated_path.parent.mkdir(parents=True)
    shutil.copy2(original_path, relocated_path)
    original_path.unlink()
    os.utime(relocated_path, ns=(1_900_000_000_000_000_000,) * 2)
    relocated_references = (
        file_reference(
            relocated_path,
            role="normalized_bbo",
            logical_source=payload.source_references[0]["logical_source"],
        ),
    )
    relocated_identity = market_context_identity(
        symbol="BTCUSDC",
        day="2099-01-02",
        warmup_days=1,
        source_references=relocated_references,
        book_source_authority="native_strict",
        book_dataset_version="fixture.v1",
        transform_identity_sha256="a" * 64,
    )
    assert references_sha256(relocated_references) == references_sha256(payload.source_references)
    assert relocated_identity == identity
    assert artifact.directory.name == canonical_sha256(relocated_identity)

    restored = load_market_context(
        cache_root=cache_root,
        identity=relocated_identity,
        source_references=relocated_references,
    )
    assert restored is not None
    assert restored.source_references[0]["locator"]["path"] == str(relocated_path.resolve())
    assert market_context_parity_report(payload, restored)["passed"] is True


def test_source_byte_change_invalidates_component_identity(tmp_path: Path) -> None:
    identity, payload = _context_fixture(tmp_path)
    cache_root = tmp_path / "cache"
    write_market_context(cache_root=cache_root, identity=identity, payload=payload)
    source_path = Path(payload.source_references[0]["locator"]["path"])
    original_size = source_path.stat().st_size
    source_path.write_bytes(b"x" * original_size)
    changed_references = (
        file_reference(
            source_path,
            role="normalized_bbo",
            logical_source=payload.source_references[0]["logical_source"],
        ),
    )
    changed_identity = market_context_identity(
        symbol="BTCUSDC",
        day="2099-01-02",
        warmup_days=1,
        source_references=changed_references,
        book_source_authority="native_strict",
        book_dataset_version="fixture.v1",
        transform_identity_sha256="a" * 64,
    )
    assert changed_identity != identity
    assert (
        load_market_context(
            cache_root=cache_root,
            identity=changed_identity,
            source_references=changed_references,
        )
        is None
    )


def test_market_context_detects_content_tampering(tmp_path: Path) -> None:
    identity, payload = _context_fixture(tmp_path)
    artifact = write_market_context(
        cache_root=tmp_path / "cache",
        identity=identity,
        payload=payload,
    )
    arrays_path = artifact.directory / "rolling_arrays.npz"
    arrays_path.write_bytes(arrays_path.read_bytes() + b"tampered")
    with pytest.raises(ReplayCacheIntegrityError, match="size mismatch"):
        load_market_context(cache_root=tmp_path / "cache", identity=identity)


def test_component_writers_reject_strategy_path_fields(tmp_path: Path) -> None:
    identity, payload = _context_fixture(tmp_path)
    bad_payload = MarketContextPayload(
        trades=payload.trades.assign(inventory_btc=0.001),
        var_ts_ms=payload.var_ts_ms,
        var_ssq=payload.var_ssq,
        var_ti=payload.var_ti,
        var_retsq=payload.var_retsq,
        metadata=payload.metadata,
        source_references=payload.source_references,
    )
    with pytest.raises(ValueError, match="strategy-dependent fields"):
        write_market_context(
            cache_root=tmp_path / "cache",
            identity=identity,
            payload=bad_payload,
        )


def test_model_overlay_round_trip_is_independent_of_market_payload(
    tmp_path: Path,
) -> None:
    feature_source = tmp_path / "features.parquet"
    model_source = tmp_path / "model.txt"
    feature_source.write_bytes(b"feature")
    model_source.write_bytes(b"model")
    identity = model_overlay_identity(
        symbol="BTCUSDC",
        day="2099-01-02",
        market_context_identity_sha256="b" * 64,
        feature_source_identity=(file_reference(feature_source, role="causal_features"),),
        model_bundle_identity=(file_reference(model_source, role="model_bundle"),),
        toxicity_horizon_s=10,
        cross_market_enabled=True,
        run_ml_inference=True,
    )
    overlay = (
        np.array([1, 2], dtype=np.int64),
        np.array([0.4, 0.6], dtype=np.float64),
        {"feature_a": np.array([3.0, 4.0], dtype=np.float32)},
    )
    artifact = write_model_overlay(
        cache_root=tmp_path / "cache",
        identity=identity,
        ml_data=overlay,
    )
    assert artifact.cache_hit is False
    assert sorted(path.name for path in artifact.directory.iterdir()) == [
        "manifest.json",
        "model_overlay.npz",
    ]
    restored = load_model_overlay(cache_root=tmp_path / "cache", identity=identity)
    assert isinstance(restored, tuple)
    np.testing.assert_array_equal(restored[0], overlay[0])
    np.testing.assert_array_equal(restored[1], overlay[1])
    np.testing.assert_array_equal(restored[2]["feature_a"], overlay[2]["feature_a"])
    assert model_overlay_parity_report(overlay, restored)["passed"] is True

    with pytest.raises(ValueError, match="strategy-dependent fields"):
        write_model_overlay(
            cache_root=tmp_path / "bad-cache",
            identity=identity,
            ml_data=(
                np.array([1.0]),
                {"campaign_terminal_pnl": np.array([2.0])},
            ),
        )
