from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from models import data_windows as dw
from models.data_windows import load_tick_window
from models.native_exchange_book_cache import native_book_parser_identity


def _trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transact_time": [1_700_000_000_000 + 1_000 * i for i in range(20)],
            "price": [90_000.0 + 0.1 * i for i in range(20)],
            "qty": [0.001] * 20,
        }
    )


@pytest.mark.parametrize("changed_input", ["dtype", "_read_aggtrade_csv", "_read_individual_trade_csv"])
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
