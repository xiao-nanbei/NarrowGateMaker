from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from live.config import Config, to_backtest_params
from models import backtest_tick as bt
from research.families.f06_placement_fill_cif.audit.fixed_spread_support import (
    audit_execution_trade_inputs,
    select_days,
)
from research.families.f06_placement_fill_cif.audit.paired_fixed_spread_monotonic import (
    _archive_cpp_runtime,
    _freeze_run_identity,
    assert_paired_monotonicity,
)
from models.backtest_config import apply_tick_defaults, build_backtest_base_params
from models.tick_data_types import HistoricalL2Data


def test_python_engine_rejects_retired_or_cpp_only_spread_probes() -> None:
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray([0], dtype=np.int64),
            "price": np.asarray([100.0]),
            "quantity": np.asarray([0.0]),
            "is_buyer_maker": np.asarray([False]),
        }
    )
    empty_i64 = np.empty(0, dtype=np.int64)
    empty_f64 = np.empty(0, dtype=np.float64)

    with pytest.raises(ValueError, match="scalar fixed-spread probe was retired"):
        bt.simulate_tick(
            trades,
            empty_i64,
            empty_f64,
            {"fixed_spread_probe_enabled": True},
        )
    with pytest.raises(NotImplementedError, match=r"C\+\+-only"):
        bt.simulate_tick(
            trades,
            empty_i64,
            empty_f64,
            {"paired_fixed_spread_probe_enabled": True},
        )



def test_live_to_replay_abi_preserves_q90_action_identity() -> None:
    config = Config()
    config.strategy.dynamic_fill_hazard_shadow_enabled = True
    config.strategy.dynamic_fill_hazard_action_enabled = True
    config.strategy.dynamic_fill_hazard_action_policy_path = "q90.json"
    config.strategy.dynamic_fill_hazard_action_policy_sha256 = "abc123"

    live_params = to_backtest_params(config)
    replay_params = build_backtest_base_params(live_params)

    assert replay_params["dynamic_fill_hazard_action_enabled"] is True
    assert replay_params["dynamic_fill_hazard_action_policy_path"] == "q90.json"
    assert replay_params["dynamic_fill_hazard_action_policy_sha256"] == "abc123"


def test_replay_fill_controls_are_not_live_strategy_parameters() -> None:
    live_params = to_backtest_params(Config())

    assert "maker_fill_prob" not in live_params
    assert "direction_aware_fill" not in live_params
    assert "fill_directional_strength" not in live_params

    replay_params = apply_tick_defaults(dict(live_params))
    assert replay_params["maker_fill_prob"] == pytest.approx(1.0)


def test_cpp_runtime_archive_freezes_binary_identity(tmp_path: Path) -> None:
    cpp = pytest.importorskip("narrowgate_cpp")
    if not hasattr(cpp.TickReplayParams(), "paired_fixed_spread_probe_enabled"):
        pytest.skip("installed narrowgate_cpp predates paired fixed-spread ABI")

    identity = _archive_cpp_runtime(tmp_path)
    source = Path(identity["source_path"])
    archived = Path(identity["archived_path"])

    assert archived.is_file()
    assert archived != source
    assert identity["paired_abi"] is True
    assert identity["sha256"] == hashlib.sha256(archived.read_bytes()).hexdigest()
    assert archived.read_bytes() == source.read_bytes()


def test_frozen_run_identity_ignores_only_creation_time() -> None:
    first = _freeze_run_identity(
        {"created_at_utc": "2026-07-26T00:00:00Z", "dataset": "same"}
    )
    second = _freeze_run_identity(
        {"created_at_utc": "2026-07-26T01:00:00Z", "dataset": "same"}
    )
    changed = _freeze_run_identity(
        {"created_at_utc": "2026-07-26T01:00:00Z", "dataset": "different"}
    )

    assert first["run_identity_sha256"] == second["run_identity_sha256"]
    assert first["run_identity_sha256"] != changed["run_identity_sha256"]


def test_select_days_respects_formal_registry() -> None:
    quality = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "rebuilt": [True, True, True],
            "formal_eligible": [False, True, True],
        }
    )
    assert select_days(
        quality,
        panel="descriptive",
        requested_days=None,
        max_days=None,
    ) == ["2026-01-01", "2026-01-02", "2026-01-03"]
    assert select_days(
        quality,
        panel="formal",
        requested_days=None,
        max_days=None,
    ) == ["2026-01-02", "2026-01-03"]
    with pytest.raises(ValueError, match="outside panel=formal"):
        select_days(
            quality,
            panel="formal",
            requested_days=["2026-01-01"],
            max_days=None,
        )


def test_execution_trade_preflight_requires_both_taker_sides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_root = tmp_path / "raw_trades"
    symbol_root = raw_root / "BTCUSDC"
    symbol_root.mkdir(parents=True)
    day = "2026-01-01"
    path = symbol_root / f"BTCUSDC-trades-{day}.csv"
    path.write_text(
        "id,price,qty,quote_qty,time,is_buyer_maker\n"
        "1,100,0.1,10,1,true\n"
        "2,100,0.1,10,2,false\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "research.families.f06_placement_fill_cif.audit.fixed_spread_support.bt.RAW_TRADES_DIR",
        raw_root,
    )

    quality = audit_execution_trade_inputs([day])

    assert quality["has_buyer_maker_true"].all()
    assert quality["has_buyer_maker_false"].all()

    path.write_text(
        "id,price,qty,quote_qty,time,is_buyer_maker\n"
        "1,100,0.1,10,1,true\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="lacks BUY/SELL"):
        audit_execution_trade_inputs([day])


def test_paired_spread_probe_uses_exact_quantity_and_strict_through_fill() -> None:
    cpp = pytest.importorskip("narrowgate_cpp")
    if not hasattr(cpp.TickReplayParams(), "paired_fixed_spread_probe_enabled"):
        pytest.skip("installed narrowgate_cpp predates paired fixed-spread ABI")
    bt.configure_symbol("BTCUSDC")
    ts = np.asarray([0, 500, 1_000, 1_500, 2_000, 2_500], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": [100.0, 99.8, 100.0, 100.2, 100.0, 100.0],
            "quantity": [0.0, 0.001, 0.0, 0.001, 0.0, 0.0],
            "is_buyer_maker": [False, True, False, False, False, False],
            "_is_execution_trade": [False, True, False, True, False, False],
        }
    )
    bid_px = np.tile(np.asarray([99.9, 99.8, 99.7]), (ts.size, 1))
    ask_px = np.tile(np.asarray([100.1, 100.2, 100.3]), (ts.size, 1))
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=bid_px,
        bid_qty=np.zeros_like(bid_px),
        ask_px=ask_px,
        ask_qty=np.zeros_like(ask_px),
    )
    params = {
        "paired_fixed_spread_probe_enabled": True,
        "paired_fixed_spread_probe_ticks": [0.0, 1.0, 2.0],
        "paired_fixed_spread_fail_on_violation": True,
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 2.0,
        "rq_min": 2.0,
        "rq_max": 2.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "maker_fee": 0.0,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "queue_ahead_base_mult": 0.0,
        "queue_deplete_base_mult": 1.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "max_exec_book_age_s": 5.0,
        "dynamic_cap_enabled": False,
        "ml_enabled": False,
        "collect_curves": False,
    }
    result = bt._simulate_tick_cpp(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )
    assert result["paired_fixed_spread_violations"] == []
    rows = {
        (row["side"], int(row["distance_ticks"])): row
        for row in result["paired_fixed_spread_rows"]
    }
    assert [rows[("BUY", d)]["filled_orders"] for d in range(3)] == [1, 1, 0]
    assert [rows[("SELL", d)]["filled_orders"] for d in range(3)] == [1, 1, 0]
    assert rows[("BUY", 0)]["through_touched_orders"] == 1
    assert rows[("BUY", 0)]["through_forced_fill_orders"] == 1
    assert rows[("BUY", 1)]["exact_touched_orders"] == 1
    assert rows[("BUY", 1)]["filled_via_exact_orders"] == 1
    for side in ("BUY", "SELL"):
        assert len(
            {
                rows[(side, distance)]["observed_lifecycle_orders"]
                for distance in range(3)
            }
        ) == 1


def test_paired_spread_probe_exact_touch_only_consumes_print_quantity() -> None:
    cpp = pytest.importorskip("narrowgate_cpp")
    if not hasattr(cpp.TickReplayParams(), "paired_fixed_spread_probe_enabled"):
        pytest.skip("installed narrowgate_cpp predates paired fixed-spread ABI")
    bt.configure_symbol("BTCUSDC")
    ts = np.asarray([0, 500, 2_000, 2_500], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": [100.0, 99.8, 100.0, 100.0],
            "quantity": [0.0, 0.001, 0.0, 0.0],
            "is_buyer_maker": [False, True, False, False],
            "_is_execution_trade": [False, True, False, False],
        }
    )
    bid_px = np.tile(np.asarray([99.9, 99.8]), (ts.size, 1))
    ask_px = np.tile(np.asarray([100.1, 100.2]), (ts.size, 1))
    bid_qty = np.tile(np.asarray([0.0, 0.002]), (ts.size, 1))
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=bid_px,
        bid_qty=bid_qty,
        ask_px=ask_px,
        ask_qty=np.zeros_like(ask_px),
    )
    params = {
        "paired_fixed_spread_probe_enabled": True,
        "paired_fixed_spread_probe_ticks": [0.0, 1.0],
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 2.0,
        "rq_min": 2.0,
        "rq_max": 2.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "maker_fee": 0.0,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "queue_ahead_base_mult": 1.0,
        "queue_deplete_base_mult": 1.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "max_exec_book_age_s": 5.0,
        "dynamic_cap_enabled": False,
        "ml_enabled": False,
        "collect_curves": False,
    }
    result = bt._simulate_tick_cpp(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )
    buy_rows = {
        int(row["distance_ticks"]): row
        for row in result["paired_fixed_spread_rows"]
        if row["side"] == "BUY"
    }
    assert buy_rows[0]["filled_orders"] == 1
    assert buy_rows[0]["filled_via_through_orders"] == 1
    assert buy_rows[1]["exact_touched_orders"] == 1
    assert buy_rows[1]["filled_orders"] == 0


def test_paired_spread_probe_uses_activation_time_book_for_gtx() -> None:
    cpp = pytest.importorskip("narrowgate_cpp")
    if not hasattr(cpp.TickReplayParams(), "paired_fixed_spread_probe_enabled"):
        pytest.skip("installed narrowgate_cpp predates paired fixed-spread ABI")
    bt.configure_symbol("BTCUSDC")
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray([0, 200, 2_000], dtype=np.int64),
            "price": [100.0, 100.0, 100.0],
            "quantity": [0.0, 0.0, 0.0],
            "is_buyer_maker": [False, False, False],
            "_is_execution_trade": [False, False, False],
        }
    )
    l2 = HistoricalL2Data(
        ts_ms=np.asarray([0, 60, 200], dtype=np.int64),
        bid_px=np.asarray([[99.9], [99.8], [99.9]]),
        bid_qty=np.zeros((3, 1)),
        ask_px=np.asarray([[100.1], [99.9], [100.1]]),
        ask_qty=np.zeros((3, 1)),
    )
    params = {
        "paired_fixed_spread_probe_enabled": True,
        "paired_fixed_spread_probe_ticks": [0.0, 1.0],
        "new_order_latency_ms": 50,
        "cancel_order_latency_ms": 0,
        "maker_fee": 0.0,
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "queue_ahead_base_mult": 0.0,
        "queue_deplete_base_mult": 1.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "max_exec_book_age_s": 5.0,
        "dynamic_cap_enabled": False,
        "ml_enabled": False,
        "collect_curves": False,
    }
    result = bt._simulate_tick_cpp(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )
    rows = result["paired_fixed_spread_rows"]
    assert all(row["activation_gtx_rejects"] == 0 for row in rows)
    assert all(row["placed_orders"] == 2 for row in rows)


def test_paired_spread_probe_applies_one_shared_ttl_window() -> None:
    cpp = pytest.importorskip("narrowgate_cpp")
    if not hasattr(cpp.TickReplayParams(), "paired_fixed_spread_probe_enabled"):
        pytest.skip("installed narrowgate_cpp predates paired fixed-spread ABI")
    bt.configure_symbol("BTCUSDC")
    ts = np.asarray([0, 1_000, 2_000], dtype=np.int64)
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": [100.0, 99.9, 100.0],
            "quantity": [0.0, 0.001, 0.0],
            "is_buyer_maker": [False, True, False],
            "_is_execution_trade": [False, True, False],
        }
    )
    bid_px = np.tile(np.asarray([99.9, 99.8]), (ts.size, 1))
    ask_px = np.tile(np.asarray([100.1, 100.2]), (ts.size, 1))
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=bid_px,
        bid_qty=np.full_like(bid_px, 0.001),
        ask_px=ask_px,
        ask_qty=np.full_like(ask_px, 0.001),
    )
    params = {
        "paired_fixed_spread_probe_enabled": True,
        "paired_fixed_spread_probe_ticks": [0.0, 1.0],
        "fragile_order_ttl_s": 0.25,
        "local_extreme_thin_depth_threshold": 1.0,
        "maker_fee": 0.0,
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 2.0,
        "rq_min": 2.0,
        "rq_max": 2.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "queue_ahead_base_mult": 0.0,
        "queue_deplete_base_mult": 1.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "max_exec_book_age_s": 5.0,
        "dynamic_cap_enabled": False,
        "ml_enabled": False,
        "collect_curves": False,
    }
    result = bt._simulate_tick_cpp(
        trades,
        np.asarray([0], dtype=np.int64),
        np.asarray([1.0], dtype=np.float64),
        params,
        l2_data=l2,
    )
    rows = result["paired_fixed_spread_rows"]
    for side in ("BUY", "SELL"):
        side_rows = [row for row in rows if row["side"] == side]
        assert len({row["observed_lifecycle_orders"] for row in side_rows}) == 1
        assert all(row["filled_orders"] == 0 for row in side_rows)
    assert result["fragile_ttl_cancel_count"] >= 2


def test_paired_monotonicity_validator_rejects_deeper_fill_increase() -> None:
    frame = pd.DataFrame(
        {
            "day": ["2026-01-01", "2026-01-01"],
            "side": ["BUY", "BUY"],
            "distance_ticks": [0, 1],
            "submitted_orders": [10, 10],
            "placed_orders": [10, 10],
            "observed_lifecycle_orders": [10, 10],
            "observed_1s_orders": [10, 10],
            "observed_5s_orders": [10, 10],
            "observed_10s_orders": [10, 10],
            "filled_orders": [3, 4],
            "fully_filled_orders": [3, 4],
            "filled_within_1s": [1, 2],
            "filled_within_5s": [2, 3],
            "filled_within_10s": [3, 4],
            "fill_qty_btc": [0.003, 0.004],
        }
    )
    with pytest.raises(ValueError, match="deeper distance increases"):
        assert_paired_monotonicity(frame)
