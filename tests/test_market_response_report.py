"""Synthetic offline probe checks; no real market rows or strategy execution."""
import numpy as np
import pandas as pd
import pytest
import json

from research.families.f08_side_taker_lifecycle.market_response_report import (
    OUTCOMES, SECOND, TrainingTransform, block_interval, validate_panel, load_panel_sources,
    response_curves,
)


def panel(day="2025-08-01"):
    start = pd.Timestamp(day, tz="UTC").value
    row = dict(day=day, panel="visible", now_ns=start + SECOND,
               source_asof_ns=start, inclusion_probability=.5)
    row.update({name: np.nan for name in OUTCOMES})
    return pd.DataFrame([row])


def test_transform_uses_only_supplied_training_support_and_preserves_missing_indicator():
    train = pd.DataFrame({"x": [1., 3., np.nan], "y": [np.nan] * 3})
    fitted = TrainingTransform.fit(train, ("x", "y"), np.ones(3))
    before = fitted.mean.copy()
    transformed = fitted.apply(pd.DataFrame({"x": [1000., np.nan], "y": [2., np.nan]}))
    np.testing.assert_array_equal(fitted.mean, before)
    np.testing.assert_array_equal(transformed[:, 2:], [[0., 0.], [1., 1.]])
    assert np.isfinite(transformed).all()
    with pytest.raises(ValueError, match="infinite"):
        fitted.apply(pd.DataFrame({"x": [np.inf], "y": [0.]}))


def test_partial_panel_cannot_be_formally_fitted():
    with pytest.raises(ValueError, match="incomplete"):
        validate_panel(panel())
    validate_panel(panel(), require_complete=False)


def test_final_never_admitted_even_for_partial_check():
    with pytest.raises(ValueError, match="Final"):
        validate_panel(panel("2026-05-28"), require_complete=False)


@pytest.mark.parametrize("probability", [0., -1., 1.1, np.nan, np.inf])
def test_sampling_probability_is_not_silently_repaired(probability):
    frame = panel(); frame["inclusion_probability"] = probability
    with pytest.raises(ValueError, match="sampling"):
        validate_panel(frame, require_complete=False)


def test_future_source_and_cross_day_outcomes_rejected():
    frame = panel(); frame["source_asof_ns"] = frame.now_ns + 1
    with pytest.raises(ValueError, match="future"):
        validate_panel(frame, require_complete=False)
    frame = panel(); frame["now_ns"] = pd.Timestamp("2025-08-02", tz="UTC").value - SECOND
    frame["mid_move_1000000000"] = 0.
    with pytest.raises(ValueError, match="crosses"):
        validate_panel(frame, require_complete=False)


def test_calendar_block_interval_preserves_constant_paired_difference():
    result = block_interval(["2025-08-01", "2025-08-09"], [2., 2.], [1., 5.], draws=100)
    assert result["low"] == result["high"] == 2.
    assert result["supported_days"] == 2
    assert result["valid_draws"] > 0


def test_duplicate_daily_input_exceeding_sampling_cap_is_rejected():
    with pytest.raises(ValueError, match="cap"):
        validate_panel(pd.concat([panel()] * 3001, ignore_index=True), require_complete=False)


def test_running_scan_cannot_be_accepted_as_a_prefix(tmp_path):
    (tmp_path / "market-status.json").write_text(json.dumps({"status": "running"}))
    with pytest.raises(ValueError, match="active writer"):
        load_panel_sources({"sources": [{"directory": str(tmp_path), "days": ["2025-08-01"],
                                         "acceptance": "previously_verified_closed_prefix"}]})


def test_unknown_pressure_is_not_classified_as_high_pressure():
    from research.families.f03_causal_13_head.time_weighted_evaluation import SPLITS
    rows = []
    for pressure in (0., 1., np.nan):
        row = {"day": SPLITS["T"][0], "panel": "visible", "trade_pressure_1s": pressure,
               "touch_delta_qty": -1., "inclusion_probability": .5, "recovery_start_ns": 0}
        for h in (100_000_000, 500_000_000, SECOND, 5 * SECOND):
            row[f"missing_{h}"] = None
            row[f"recovery_event_{h}"] = "recovered"
        rows.append(row)
    curves, _ = response_curves(pd.DataFrame(rows))
    unknown = [x for x in curves if x["pressure_bin"] == -1]
    assert len(unknown) == 4
    assert all(x["rows"] == 1 for x in unknown)


def test_tie_diagnostic_keeps_ready_boundaries_and_channel_order():
    from decimal import Decimal
    from data.runtime import InputTick
    from data.observation import Observation, TradeContribution
    from data.tardis_input import BookView
    from research.families.f08_side_taker_lifecycle.market_response_report import trade_first_ties
    view = BookView(1, ((Decimal(100), Decimal(1)),), ((Decimal(101), Decimal(1)),),
                    None, None, "test", True)
    trade = TradeContribution("trade", 0, Decimal(100), Decimal(1), "sell", 1, None, "test", 1)
    def observation(name, ready, payload):
        return Observation(name, "test", "test", "test", 0, 0, ready, ready, payload, "observed")
    a, b, c = observation("b1", 1, view), observation("b2", 2, view), observation("t2", 2, trade)
    tick = InputTick(2, (), (a, b, c), (), None, None)
    changed = trade_first_ties(tick)
    assert changed.observations == (a, c, b)
    assert tick.observations == (a, b, c)
    assert changed.now_ns == tick.now_ns


def test_baseline_inventory_does_not_promote_ec_opportunities(tmp_path):
    from research.families.f08_side_taker_lifecycle.market_response_report import audit_baseline_logs
    (tmp_path / "accounting.json").write_text(json.dumps({"economic_complete": True, "all_in_net_pnl": 0.}))
    (tmp_path / "replay.json").write_text(json.dumps({"_risk_selection_opportunities": [{"kind": "C"}]}))
    result = audit_baseline_logs({"accounts": [{"account_id": "development-synthetic", "directory": str(tmp_path)}]}, tmp_path / "out.json")
    row = result["accounts"][0]
    assert row["legacy_opportunity_kinds"] == {"C": 1}
    assert row["new_update_opportunity_stream_verified"] is False
    assert result["strategy_replays"] == 0
