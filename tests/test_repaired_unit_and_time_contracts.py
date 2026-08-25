from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from features.feature_engineer import (
    _TS_NS,
    _compute_label_triplet,
    _compute_toxicity_pair,
    _microstructure_5s_from_1s,
)
from research.families.f10_live_replay_attribution.audit.metrics import _order_scores
from models.backtest_tick import simulate_tick
from strategy.inventory_manager import InventoryManager
from strategy.maker_engine import _commission_in_quote_asset
from strategy.model_contract import (
    REQUIRED_FEATURE_DAG_ID,
    REQUIRED_FEATURE_DAG_SHA256,
    REQUIRED_FEATURE_SEMANTICS_VERSION,
    REQUIRED_MODEL_HEADS,
    validate_model_bundle,
)
from strategy.order_manager import OrderManager, Side
from strategy.signal import SignalEngine


def test_legacy_bar_backtest_emits_epoch_milliseconds() -> None:
    from models import backtest

    index = pd.date_range("2026-07-26", periods=3, freq="1s", tz="UTC")
    bars = pd.DataFrame(
        {
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
        },
        index=index,
    )

    ts, *_ = backtest.prepare_arrays(bars)

    assert np.diff(ts).tolist() == [1_000, 1_000]
    assert ts[0] == int(index[0].timestamp() * 1_000)


def test_legacy_ml_bar_backtest_aligns_on_epoch_milliseconds(monkeypatch) -> None:
    from models import backtest_ml

    index = pd.date_range("2026-07-26", periods=4, freq="1s", tz="UTC")
    bars = pd.DataFrame(
        {
            "high": [101.0] * 4,
            "low": [99.0] * 4,
            "close": [100.0, 100.1, 100.2, 100.3],
            "trade_count": [1.0] * 4,
        },
        index=index,
    )
    predictions = pd.DataFrame(
        {
            "pred_dir_10s": [0.7],
            "pred_vol_10s": [2.0],
            "pred_ret_10s": [0.001],
        },
        index=pd.DatetimeIndex([index[0]]),
    )
    monkeypatch.setattr(
        backtest_ml,
        "_load_book_imbalance",
        lambda frame, ts: (np.zeros(len(frame)), np.zeros(len(frame))),
    )

    (
        ts,
        _hi,
        _lo,
        _close,
        _variance,
        pred_dir,
        pred_vol,
        pred_ret,
        _book,
        _intensity,
        _depth,
    ) = backtest_ml.build_ml_arrays(bars, predictions)

    assert np.diff(ts).tolist() == [1_000, 1_000, 1_000]
    assert np.all(pred_dir == pytest.approx(0.7))
    assert np.all(pred_vol == pytest.approx(2.0))
    assert np.all(pred_ret == pytest.approx(0.001))


def test_label_horizon_excludes_bar_starting_at_right_endpoint() -> None:
    ts = np.arange(12, dtype=np.int64) * _TS_NS
    close = np.full(12, 100.0)
    high = np.full(12, 100.1)
    low = np.full(12, 99.9)
    # A touch only in the bar [t+10, t+11) must not enter a 10s label.
    low[10] = 98.0
    diff = np.zeros(12)
    diff[10] = 50.0
    quote_time = np.asarray([0], dtype=np.int64)
    start = np.asarray([0], dtype=np.int64)
    bid = np.asarray([99.0])
    ask = np.asarray([101.0])

    ret, direction, variance = _compute_label_triplet(
        ts,
        close,
        high,
        low,
        diff,
        quote_time,
        start,
        bid,
        ask,
        close[:1],
        10 * _TS_NS,
    )
    tox_bid, tox_ask = _compute_toxicity_pair(
        ts,
        close,
        high,
        low,
        quote_time,
        start,
        bid,
        ask,
        10 * _TS_NS,
    )

    assert np.isnan(ret[0])
    assert np.isnan(direction[0])
    assert variance[0] == pytest.approx(0.0)
    assert np.isnan(tox_bid[0])
    assert np.isnan(tox_ask[0])


def test_five_second_microstructure_uses_only_last_five_one_second_bars() -> None:
    index = pd.date_range("2026-01-01", periods=20, freq="1s", tz="UTC")
    frame = pd.DataFrame(
        {
            "close": np.arange(100.0, 120.0),
            "buy_volume": [100.0] * 15 + [1.0] * 5,
            "sell_volume": [0.0] * 15 + [3.0] * 5,
            "trade_count": [100] * 15 + [1, 2, 3, 4, 5],
        },
        index=index,
    )

    result = _microstructure_5s_from_1s(frame).iloc[-1]

    assert result["volume_imbalance_5s"] == pytest.approx(-0.5)
    assert result["trade_intensity_5s"] == pytest.approx(3.0)
    assert result["vpin_5s"] == pytest.approx(0.5)
    assert result["price_change_5s"] == pytest.approx(119.0 / 114.0 - 1.0)


def test_utc_daily_pnl_rolls_marked_equity_without_recounting_open_pnl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = {"now": 86_400.0 - 10.0}
    monkeypatch.setattr("strategy.inventory_manager.time.time", lambda: clock["now"])
    inventory = InventoryManager(max_inventory=1.0)
    inventory.on_fill("BUY", 1.0, 100.0)
    inventory.update_mark_price(90.0)
    assert inventory.daily_pnl == pytest.approx(-10.0)

    clock["now"] = 86_400.0 + 1.0
    assert inventory.daily_pnl == pytest.approx(0.0)
    inventory.update_mark_price(95.0)
    assert inventory.daily_pnl == pytest.approx(5.0)

    # A delayed prior-day event cannot rewind the accounting day.
    inventory.on_fill("BUY", 0.1, 95.0, trade_time_ms=86_390_000)
    assert inventory._daily_utc_day == 1


def test_commission_asset_conversion_never_mixes_currency_units() -> None:
    common = {
        "fill_price": 100.0,
        "base_asset": "BTC",
        "quote_asset": "USDC",
        "settlement_asset": "USDC",
    }
    assert _commission_in_quote_asset(0.2, "USDC", **common) == pytest.approx(0.2)
    assert _commission_in_quote_asset(0.001, "BTC", **common) == pytest.approx(0.1)
    assert _commission_in_quote_asset(0.0, "", **common) == 0.0
    with pytest.raises(ValueError, match="unsupported commission asset"):
        _commission_in_quote_asset(0.1, "BNB", **common)
    with pytest.raises(ValueError, match="missing its asset"):
        _commission_in_quote_asset(0.1, "", **common)


def test_order_manager_preserves_commission_asset_for_fill_callback() -> None:
    seen: list[dict] = []
    manager = OrderManager(on_fill=lambda _order, event: seen.append(dict(event)))
    cid = manager.create_order("BTCUSDC", Side.BUY, 100.0, 0.001)
    manager.confirm_new(cid, 7)
    manager.on_order_update(
        {
            "s": "BTCUSDC",
            "c": cid,
            "S": "BUY",
            "o": "LIMIT",
            "X": "FILLED",
            "i": 7,
            "p": "100.0",
            "q": "0.001",
            "z": "0.001",
            "l": "0.001",
            "L": "100.0",
            "ap": "100.0",
            "n": "0.000001",
            "N": "BTC",
        }
    )
    assert seen[0]["_fill_commission_asset"] == "BTC"


def _write_contract_bundle(root: Path, *, vol_semantics: str) -> None:
    root.mkdir(parents=True)
    for head in REQUIRED_MODEL_HEADS:
        (root / f"{head}.txt").write_text("placeholder", encoding="utf-8")
        metadata = {
            "feature_cols": ["close"],
            "feature_semantics_version": REQUIRED_FEATURE_SEMANTICS_VERSION,
            "feature_dag_id": REQUIRED_FEATURE_DAG_ID,
            "feature_dag_sha256": REQUIRED_FEATURE_DAG_SHA256,
            "calendar_timestamp_semantics": (
                "preserve_datetime_physical_unit_ms_us_ns_before_epoch_conversion"
            ),
            "label_semantics_version": 3,
            "label_window_semantics": "left_closed_right_open_[t,t+h)",
            "feature_manifest_sha256": "feature-manifest-sha",
        }
        if head.startswith("vol_"):
            metadata["label_semantics"] = vol_semantics
        (root / f"{head}_meta.json").write_text(json.dumps(metadata), encoding="utf-8")


def test_model_contract_requires_absolute_price_variance_metadata(tmp_path: Path) -> None:
    valid = tmp_path / "valid"
    _write_contract_bundle(valid, vol_semantics="fixed_forward_h_absolute_price_variance")
    assert set(validate_model_bundle(valid)) == set(REQUIRED_MODEL_HEADS)

    invalid = tmp_path / "invalid"
    _write_contract_bundle(invalid, vol_semantics="log_return_variance")
    with pytest.raises(ValueError, match="label_semantics"):
        validate_model_bundle(invalid)

    mixed = tmp_path / "mixed"
    _write_contract_bundle(mixed, vol_semantics="fixed_forward_h_absolute_price_variance")
    meta_path = mixed / "dir_10s_meta.json"
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    metadata["feature_manifest_sha256"] = "different-feature-manifest"
    meta_path.write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="one feature manifest"):
        validate_model_bundle(mixed)


def test_ml_off_prediction_does_not_reuse_dimensionless_realized_volatility() -> None:
    engine = SignalEngine(enable_ml=False)
    prediction = engine._predict({"volatility_30s": 0.123})
    assert prediction.vol_10s == 0.0


def test_maker_signed_markout_has_same_favorable_direction_for_buy_and_sell() -> None:
    common = {
        "quote_distance_bps": 4.0,
        "near_depth_total": 1.0,
        "l2_book_refresh_ratio": 0.2,
        "l2_book_cancel_ratio": 0.1,
        "l2_quote_flip_rate": 0.0,
        "toxicity": 0.5,
        "campaign_max_abs_qty": 0.0,
        "campaign_age_s": 0.0,
        "campaign_adverse_excursion": 0.0,
    }
    for side in ("BUY", "SELL"):
        favorable = _order_scores({**common, "side": side, "markout_ema": 1.0})
        adverse = _order_scores({**common, "side": side, "markout_ema": -1.0})
        assert favorable["toxic_risk_score"] < adverse["toxic_risk_score"]
        assert favorable["fill_quality_score"] > adverse["fill_quality_score"]


def test_tick_replay_rejects_nonpositive_symbol_filters() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": [0, 1_000],
            "price": [100.0, 100.0],
            "quantity": [0.0, 0.0],
            "is_buyer_maker": [False, False],
        }
    )
    with pytest.raises(ValueError, match="tick_size"):
        simulate_tick(
            trades,
            np.asarray([0], dtype=np.int64),
            np.asarray([1.0]),
            {"tick_size": 0.0, "lot_size": 0.001},
        )
