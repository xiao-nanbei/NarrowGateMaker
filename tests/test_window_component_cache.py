from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models import data_windows as dw
from models.data_windows import load_tick_window
from models.native_exchange_book_cache import native_book_parser_identity
from strategy.model_contract import (
    REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
    REQUIRED_FEATURE_DAG_ID,
    REQUIRED_FEATURE_DAG_SHA256,
    REQUIRED_FEATURE_SEMANTICS_VERSION,
)


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transact_time": [1_700_000_000_000 + 1_000 * i for i in range(20)],
            "price": [90_000.0 + 0.1 * i for i in range(20)],
            "qty": [0.001] * 20,
        }
    )


def test_compute_preroll_changes_prediction_cache_not_market_context(tmp_path, monkeypatch):
    monkeypatch.setattr(dw, "_window_source_signature", lambda *args, **kwargs: ())
    monkeypatch.setattr(dw, "_model_artifact_signatures", lambda *args: [])
    day = "2099-01-02"
    base = {"market_context_warmup_days": 1}
    timed = {**base, "runtime_compute_clock": "source_time_assumption"}

    def identities(params):
        return (
            dw._window_model_overlay_cache_path(
                tmp_path, day, params, feature_dir=tmp_path,
                run_ml_inference=False, cross_market_enabled=False,
                market_context_path=tmp_path / "shared.pkl",
            ),
            dw._window_model_overlay_v2_identity(
                day, params, feature_dir=tmp_path, run_ml_inference=False,
                cross_market_enabled=False, market_context_identity_sha256="a" * 64,
            ),
        )

    old, new = identities(base), identities(timed)
    assert old[0] != new[0]
    assert "prediction_context_start_ms" not in old[1]
    start = int(pd.Timestamp(day, tz="UTC").value // 1_000_000)
    assert new[1]["prediction_context_start_ms"] == start
    assert new[1]["prediction_context_end_ms"] == start + 86_400_000 - 1
    assert (
        dw._window_market_context_v2_identity(day, base)
        == dw._window_market_context_v2_identity(day, timed)
    )
    assert identities({**timed, "replay_event_clock_start_ts_ms": start + 1000}) != new
    assert identities({**timed, "replay_event_clock_end_ts_ms": start + 2000}) != new


def test_prediction_cache_tracks_separate_feature_warmup_directory(tmp_path, monkeypatch):
    panel, warmup = tmp_path / "panel", tmp_path / "warmup"
    panel.mkdir()
    warmup.mkdir()
    monkeypatch.delenv("MM_FEATURE_WARMUP_DIR", raising=False)
    days = ["2099-01-01", "2099-01-02"]
    before = dw._feature_source_signatures(panel, days)
    source = warmup / "features_2099-01-01.parquet"
    source.write_bytes(b"synthetic feature bytes")
    monkeypatch.setenv("MM_FEATURE_WARMUP_DIR", str(warmup))
    bound = dw._feature_source_signatures(panel, days)
    assert bound != before
    assert any(row[0] == str(source) for row in bound)
    source.write_bytes(b"changed synthetic feature bytes")
    assert dw._feature_source_signatures(panel, days) != bound


@pytest.mark.parametrize(
    "changed_input", ["dtype", "_read_aggtrade_csv", "_read_individual_trade_csv"]
)
def test_trade_reader_change_invalidates_all_market_caches_not_native(
    tmp_path: Path, monkeypatch, changed_input: str,
) -> None:
    monkeypatch.setattr(dw, "_window_source_signature", lambda *args, **kwargs: ())
    params = {"execution_trade_source": "trades", "market_context_warmup_days": 0}

    def identities():
        return (
            dw._window_cache_path(
                tmp_path, "2099-01-02", params,
                load_ml=False, require_ml=False, run_ml_inference=False,
                feature_dir=tmp_path, require_target_feature_files=False,
                cross_market_enabled=False, with_ml_cache=False,
                require_historical_bbo=False,
            ),
            dw._window_market_context_cache_path(tmp_path, "2099-01-02", params),
            dw._window_market_context_v2_identity("2099-01-02", params)[0],
        )

    before = identities()
    native_before = native_book_parser_identity()
    if changed_input == "dtype":
        current = np.dtype(bt.AGGTRADE_DTYPES["quantity"])
        monkeypatch.setitem(
            bt.AGGTRADE_DTYPES, "quantity",
            np.float32 if current == np.dtype("float64") else np.float64,
        )
    else:
        def changed_reader(path):
            raise AssertionError("identity checks must not read market data")

        monkeypatch.setattr(bt, changed_input, changed_reader)

    assert all(old != new for old, new in zip(before, identities(), strict=True))
    assert native_book_parser_identity() == native_before
    assert not list(tmp_path.iterdir())


def test_new_window_miss_persists_reusable_component_not_monolith(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = {"trades": 0, "bars": 0, "bbo": 0, "l2": 0}

    def load_trades(*args, **kwargs):
        calls["trades"] += 1
        return _trades()

    def load_bars(*args, **kwargs):
        calls["bars"] += 1
        return None

    def load_bbo(*args, **kwargs):
        calls["bbo"] += 1
        return None

    def load_l2(*args, **kwargs):
        calls["l2"] += 1
        return None

    monkeypatch.setattr(bt, "load_execution_trades", load_trades)
    monkeypatch.setattr(bt, "load_1s_bars", load_bars)
    monkeypatch.setattr(bt, "load_bbo_data", load_bbo)
    monkeypatch.setattr(bt, "load_l2_data", load_l2)
    params = {
        "execution_trade_source": "trades",
        "market_context_warmup_days": 0,
        "window_cache_write_enabled": True,
        "ml_enabled": False,
    }

    first = load_tick_window(
        "2099-01-02",
        params,
        load_ml=False,
        require_ml=False,
        require_historical_bbo=False,
        cache_dir=tmp_path,
    )
    assert calls == {"trades": 1, "bars": 1, "bbo": 1, "l2": 1}
    assert len(first.trades) == 20
    assert not list(tmp_path.glob("*_tick_window_v13_*.pkl"))
    components = list(
        (tmp_path / "components_v2" / "market_context_day_v2" / "btcusdc" / "2099-01-02").glob(
            "*/manifest.json"
        )
    )
    assert len(components) == 1
    artifact_dir = components[0].parent
    assert (artifact_dir / "trades.parquet").is_file()
    assert (artifact_dir / "rolling_arrays.npz").is_file()
    assert (artifact_dir / "source_references.json").is_file()
    assert not list(artifact_dir.glob("*.pkl"))

    gate_only_change = {
        **params,
        "execution_trade_source": "individual",
        "require_ml": True,
        "require_target_feature_files": True,
        "_formal_quality_day_manifest_sha256": "different-gate-only-identity",
    }
    second = load_tick_window(
        "2099-01-02",
        gate_only_change,
        load_ml=False,
        require_ml=False,
        require_historical_bbo=False,
        cache_dir=tmp_path,
    )

    pd.testing.assert_frame_equal(second.trades, first.trades)
    assert second.var_ts_ms.tolist() == first.var_ts_ms.tolist()
    assert calls == {"trades": 1, "bars": 1, "bbo": 1, "l2": 1}


def test_model_overlay_reuses_predictions_without_copying_market_context(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls = {"trades": 0, "bars": 0, "bbo": 0, "l2": 0, "ml": 0}

    def count(name, value):
        def loader(*args, **kwargs):
            calls[name] += 1
            return value

        return loader

    monkeypatch.setattr(bt, "load_execution_trades", count("trades", _trades()))
    monkeypatch.setattr(bt, "load_1s_bars", count("bars", None))
    monkeypatch.setattr(bt, "load_bbo_data", count("bbo", None))
    monkeypatch.setattr(bt, "load_l2_data", count("l2", None))

    def load_ml(*args, **kwargs):
        calls["ml"] += 1
        return (
            pd.Series([1, 2], dtype="int64").to_numpy(),
            pd.Series([0.4, 0.6], dtype="float64").to_numpy(),
            {"feature_a": pd.Series([3.0, 4.0]).to_numpy()},
        )

    monkeypatch.setattr(bt, "load_ml_predictions", load_ml)
    feature_dir = tmp_path / "features"
    feature_dir.mkdir()
    (feature_dir / "causal_feature_manifest.json").write_text("{}\n")
    params = {
        "execution_trade_source": "trades",
        "market_context_warmup_days": 0,
        "window_cache_write_enabled": True,
        "ml_enabled": False,
        "toxicity_horizon_s": 10,
    }
    first = load_tick_window(
        "2099-01-02",
        params,
        load_ml=True,
        require_ml=True,
        run_ml_inference=False,
        feature_dir=feature_dir,
        require_historical_bbo=False,
        cache_dir=tmp_path / "cache",
    )
    second = load_tick_window(
        "2099-01-02",
        params,
        load_ml=True,
        require_ml=True,
        run_ml_inference=False,
        feature_dir=feature_dir,
        require_historical_bbo=False,
        cache_dir=tmp_path / "cache",
    )

    assert calls == {"trades": 1, "bars": 1, "bbo": 1, "l2": 1, "ml": 1}
    assert first.ml_data[1].tolist() == second.ml_data[1].tolist()
    overlays = list(
        (
            tmp_path / "cache" / "components_v2" / "model_overlay_day" / "btcusdc" / "2099-01-02"
        ).glob("*/manifest.json")
    )
    assert len(overlays) == 1
    assert not list((tmp_path / "cache").glob("*_tick_window_v13_*.pkl"))


@pytest.mark.parametrize("changed_input", ["manifest", "parquet", "directory"])
def test_actual_inference_panel_changes_only_model_overlay_cache(
    tmp_path, monkeypatch, changed_input,
):
    calls = {"trades": 0, "ml": 0}

    def load_trades(*_args, **_kwargs):
        calls["trades"] += 1
        return _trades()

    def load_ml(*_args, **kwargs):
        calls["ml"] += 1
        panel = Path(kwargs["feature_dir"])
        values = pd.read_parquet(panel / "features_2099-01-02.parquet")["feature_a"]
        return (np.array([1, 2]), values.to_numpy(), {"feature_a": values.to_numpy()})

    monkeypatch.setattr(bt, "load_execution_trades", load_trades)
    monkeypatch.setattr(bt, "load_1s_bars", lambda *_a, **_k: None)
    monkeypatch.setattr(bt, "load_bbo_data", lambda *_a, **_k: None)
    monkeypatch.setattr(bt, "load_l2_data", lambda *_a, **_k: None)
    monkeypatch.setattr(bt, "load_ml_predictions", load_ml)
    panel = tmp_path / "inference-panel"
    panel.mkdir()
    manifest = panel / "causal_feature_manifest.json"
    manifest.write_text('{"panel": "original"}')
    pd.DataFrame({"feature_a": [0.4, 0.6]}).to_parquet(panel / "features_2099-01-02.parquet")
    params = {
        "execution_trade_source": "trades", "market_context_warmup_days": 0,
        "window_cache_write_enabled": True, "ml_enabled": False,
    }

    def load(selected):
        return load_tick_window(
            "2099-01-02", params, load_ml=True, require_ml=True, run_ml_inference=False,
            feature_dir=selected, require_historical_bbo=False, cache_dir=tmp_path / "cache",
        )

    first = load(panel)
    load(panel)
    assert calls == {"trades": 1, "ml": 1}
    if changed_input == "manifest":
        manifest.write_text('{"panel": "new-inference-dates"}')
    else:
        if changed_input == "directory":
            panel = tmp_path / "another-inference-panel"
            panel.mkdir()
            (panel / "causal_feature_manifest.json").write_text(manifest.read_text())
        pd.DataFrame({"feature_a": [0.2, 0.8]}).to_parquet(
            panel / "features_2099-01-02.parquet"
        )
    second = load(panel)
    load(panel)
    assert calls == {"trades": 1, "ml": 2}
    if changed_input == "manifest":
        np.testing.assert_array_equal(first.ml_data[1], second.ml_data[1])
    else:
        np.testing.assert_array_equal(second.ml_data[1], [0.2, 0.8])
        assert not np.array_equal(first.ml_data[1], second.ml_data[1])


@pytest.mark.parametrize("cache_layer", ["whole_window", "overlay_v1", "overlay_v2"])
@pytest.mark.parametrize("compatible", [True, False])
def test_reusing_cached_inference_checks_current_input_abi_once(
    tmp_path, monkeypatch, cache_layer, compatible,
):
    panel = tmp_path / "features"
    panel.mkdir()
    model = tmp_path / "models"
    model.mkdir()
    interface = {
        "schema_version": 3, "symbol": "BTCUSDC",
        "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
        "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
        "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
        "feature_bucket_ms": 10000, "feature_ready_offset_ms": 10000,
        "feature_timestamp_semantics": "left_label_bucket_end",
        "feature_cutoff_semantics": "strict_exclusive_completed_bucket_end",
        "calendar_timestamp_semantics": REQUIRED_CALENDAR_TIMESTAMP_SEMANTICS,
        "microstructure_5s_semantics": (
            "trailing_five_seconds_from_causal_left_labelled_1s_bars"
        ),
        "market_stage": "minimal", "reference_symbol": "BTCUSDT",
    }
    (model / "dir_10s_meta.json").write_text(json.dumps({**interface, "feature_cols": ["a"]}))
    # The cache already contains predictions; the model file is a locator,
    # not something this cache-hit interface check loads for inference.
    (model / "dir_10s.txt").write_text("synthetic previously-cached model\n")
    if not compatible:
        interface["feature_cutoff_semantics"] = "inclusive"
    (panel / "causal_feature_manifest.json").write_text(json.dumps(interface))
    monkeypatch.setattr(bt, "MODEL_DIR", model)
    monkeypatch.setattr(bt, "SYMBOL", "BTCUSDC")
    monkeypatch.delenv("MM_FEATURE_WARMUP_DIR", raising=False)
    monkeypatch.setattr(bt, "load_execution_trades", lambda *_a, **_k: _trades())
    monkeypatch.setattr(bt, "load_1s_bars", lambda *_a, **_k: None)
    monkeypatch.setattr(bt, "load_bbo_data", lambda *_a, **_k: None)
    monkeypatch.setattr(bt, "load_l2_data", lambda *_a, **_k: None)
    calls = {"inference": 0, "validation": 0}

    def old_inference(*_args, **_kwargs):
        # Reproduce a cache emitted before the new input ABI check existed.
        calls["inference"] += 1
        return (np.array([1, 2]), np.array([0.4, 0.6]), {"a": np.array([1.0, 2.0])})

    monkeypatch.setattr(bt, "load_ml_predictions", old_inference)
    params = {
        "execution_trade_source": "trades", "market_context_warmup_days": 0,
        "ml_enabled": True, "toxicity_horizon_s": 5,
        "legacy_monolithic_window_cache_write_enabled": cache_layer == "whole_window",
        "legacy_component_v1_write_enabled": cache_layer == "overlay_v1",
    }
    if cache_layer == "overlay_v1":
        monkeypatch.setattr(dw, "load_model_overlay", lambda **_k: None)

    def load():
        return load_tick_window(
            "2099-01-02", params, feature_dir=panel, require_historical_bbo=False,
            cache_dir=tmp_path / "cache",
        )

    validate = bt._load_ml_inference_metadata

    def counted_validation(path, *, toxicity_horizon_s):
        calls["validation"] += 1
        assert path == panel
        assert toxicity_horizon_s == 5
        return validate(path, toxicity_horizon_s=toxicity_horizon_s)

    monkeypatch.setattr(bt, "_load_ml_inference_metadata", counted_validation)
    load()
    assert calls == {"inference": 1, "validation": 0}
    if compatible:
        result = load()
        assert result.ml_data[1].tolist() == [0.4, 0.6]
    else:
        with pytest.raises(RuntimeError, match="incompatible feature_cutoff_semantics"):
            load()
    assert calls == {"inference": 1, "validation": 1}
