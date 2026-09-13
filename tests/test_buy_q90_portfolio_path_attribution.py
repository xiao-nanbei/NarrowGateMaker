from __future__ import annotations

import copy

import pandas as pd
import pytest

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_portfolio_path_attribution as audit,
)


def _fill(
    side: str,
    ts: int,
    price: float,
    qty: float,
    before: float,
    after: float,
) -> dict[str, object]:
    return {
        "side": side,
        "fill_ts": ts,
        "quote_px": price,
        "fill_qty": qty,
        "fill_fee_usdc": 0.0,
        "inventory_before_fill": before,
        "inventory_after_fill": after,
    }


def test_campaign_reconstruction_closes_accounting_and_classifies_inventory_levels() -> None:
    fills = pd.DataFrame(
        [
            _fill("BUY", 1_000, 100.0, 0.001, 0.0, 0.001),
            _fill("BUY", 2_000, 99.0, 0.001, 0.001, 0.002),
            _fill("SELL", 3_000, 101.0, 0.001, 0.002, 0.001),
            _fill("SELL", 4_000, 102.0, 0.001, 0.001, 0.0),
            _fill("SELL", 5_000, 100.0, 0.001, 0.0, -0.001),
        ]
    )

    campaigns = audit.reconstruct_campaigns(
        fills,
        day="2026-04-17",
        arm="q90_off",
        inventory_unit_btc=0.001,
        terminal_mark_price=99.0,
        expected_campaign_count=2,
        expected_closed_count=1,
    )

    assert campaigns["direction"].tolist() == ["LONG", "SHORT"]
    assert campaigns["inventory_level_bucket"].tolist() == [
        "levels_2_3",
        "single_level",
    ]
    assert campaigns["closed"].tolist() == [True, False]
    assert campaigns["censored"].tolist() == [False, True]
    assert campaigns["terminal_value_usdc"].sum() == pytest.approx(0.005)


def test_campaign_reconstruction_fails_on_discontinuous_inventory_path() -> None:
    fills = pd.DataFrame(
        [
            _fill("BUY", 1_000, 100.0, 0.001, 0.0, 0.001),
            _fill("SELL", 2_000, 101.0, 0.001, 0.002, 0.001),
        ]
    )
    with pytest.raises(ValueError, match="discontinuous"):
        audit.reconstruct_campaigns(
            fills,
            day="2026-04-17",
            arm="q90_on",
            inventory_unit_btc=0.001,
            terminal_mark_price=100.0,
        )


def test_arm_summary_requires_exact_fill_and_campaign_accounting() -> None:
    fills = pd.DataFrame(
        [
            _fill("SELL", 1_000, 100.0, 0.001, 0.0, -0.001),
            _fill("BUY", 2_000, 99.0, 0.001, -0.001, 0.0),
        ]
    )
    campaigns = audit.reconstruct_campaigns(
        fills,
        day="2026-04-17",
        arm="q90_on",
        inventory_unit_btc=0.001,
        terminal_mark_price=99.0,
        expected_campaign_count=1,
        expected_closed_count=1,
    )
    result = {
        "fills_bid": 1,
        "fills_ask": 1,
        "terminal_mtm_pnl": 0.001,
        "pnl": 0.001,
        "abs_inventory_time_s": 1.0,
        "sq_inventory_time_s": 1e-6,
        "dynamic_fill_hazard_cancel_request_count": 3,
    }
    summary = audit.summarize_arm(
        result,
        fills,
        campaigns,
        day="2026-04-17",
        arm="q90_on",
        elapsed_hours=1.0,
    )
    assert summary["short_campaign_share"] == 1.0
    assert summary["multi_level_short_share_of_all"] == 0.0
    assert summary["sell_exposure_fill_count"] == 1
    assert summary["buy_reducing_fill_count"] == 1
    assert summary["q90_cancel_request_count"] == 3


def test_paired_daily_difference_is_always_q90_on_minus_off() -> None:
    rows = []
    for day in ("2026-04-17", "2026-04-18"):
        for arm, offset in (("q90_off", 0.0), ("q90_on", 1.0)):
            row = {"day": day, "arm": arm}
            row.update({metric: offset for metric in audit.PAIRED_METRICS})
            rows.append(row)
    paired = audit.paired_daily_differences(pd.DataFrame(rows))
    assert paired.filter(like="diff_").eq(1.0).all().all()


def test_mechanism_decision_requires_portfolio_bias_and_terminal_harm() -> None:
    inference = {
        metric: {"estimate": 0.0, "lcb95": -1.0, "ucb95": 1.0}
        for metric in audit.PAIRED_METRICS
    }
    inference["buy_exposure_fills_per_hour"] = {
        "estimate": -1.0,
        "lcb95": -2.0,
        "ucb95": -0.1,
    }
    for metric in (
        "exposure_side_imbalance_per_hour",
        "short_campaign_share",
        "multi_level_short_share_of_all",
    ):
        inference[metric] = {"estimate": 1.0, "lcb95": 0.1, "ucb95": 2.0}
    inference["terminal_mtm_pnl_usdc"] = {
        "estimate": -1.0,
        "lcb95": -2.0,
        "ucb95": -0.1,
    }
    decision = audit.mechanism_decision(inference)
    assert decision["portfolio_bias_supported"] is True
    assert decision["portfolio_harm_supported"] is True
    assert decision["decision"].endswith("supported_development")


def test_q90_arm_switch_changes_only_frozen_action_and_parity_flags(monkeypatch) -> None:
    base = {
        "dynamic_fill_hazard_action_enabled": True,
        "dynamic_fill_hazard_cpp_parity_enabled": True,
        "rng_seed": 42,
        "order_size": 0.001,
    }
    monkeypatch.setattr(audit.full_path, "_configure_params", lambda source, day: copy.deepcopy(base))
    spec = {
        "replay_contract": {
            "trace_fills_max_per_arm_day": 100,
            "q90_mismatch_trace_max": 25,
        }
    }
    off = audit._configure_arm_params({}, "2026-04-17", spec, q90_enabled=False)
    on = audit._configure_arm_params({}, "2026-04-17", spec, q90_enabled=True)
    differing = {key for key in set(off) | set(on) if off.get(key) != on.get(key)}
    assert differing == {
        "dynamic_fill_hazard_action_enabled",
        "dynamic_fill_hazard_cpp_parity_enabled",
    }
    assert off["rng_seed"] == on["rng_seed"] == 42
    audit._assert_arm_parameter_whitelist(off, on)
    on["rng_seed"] = 43
    with pytest.raises(ValueError, match="whitelist"):
        audit._assert_arm_parameter_whitelist(off, on)


def test_spec_validation_keeps_later_panels_and_live_permissions_closed() -> None:
    days = pd.date_range("2026-01-01", periods=40, freq="D").strftime("%Y-%m-%d").tolist()
    spec = {
        "schema_version": audit.SCHEMA_VERSION,
        "identity": audit.IDENTITY,
        "status": "frozen_before_development_pair_outcome_read",
        "panels": {
            "development_days": days,
            "development_primary_grade_a_days": days[:24],
            "development_sensitivity_grade_b_days": days[24:],
            "grade_b_policy": "sensitivity_only_never_pooled_into_primary_decision",
            "validation_days_not_read": ["2026-03-01"],
            "sealed_holdout_days_not_read": ["2026-03-03"],
        },
        "treatment_contract": {
            "control": "q90_off",
            "candidate": "q90_on",
            "changed_mechanism": "BUY_exposure_increasing_active_order_cancel_reenter_only",
            "sell_unchanged": True,
            "buy_reducing_unchanged": True,
            "future_fills_reused_between_arms": False,
        },
        "replay_contract": {
            "engine": "python_authoritative",
            "native_queue": "strict_snapshot_delta_exact_level",
            "initial_state": "daily_fresh_start",
            "fill_cooldown_clock": "wall_time_85n",
            "ml_enabled": False,
            "maker_fill_prob": 1.0,
            "sync_adjust_mode": "disabled_primary",
            "latency_path": "shared_between_arms",
            "rng_path": "shared_between_arms",
            "q90_on_cpp_scope": "native_book_and_buy_q90_kernel_lockstep_only",
            "full_cpp_tick_replay_authority": False,
            "trace_fills_max_per_arm_day": 100,
        },
        "inference_contract": {
            "paired_metrics": list(audit.PAIRED_METRICS),
            "cluster_unit": "UTC_day",
            "interval": "paired_day_bootstrap_95pct",
            "minimum_evaluated_days": 40,
            "minimum_primary_grade_a_days": 24,
            "minimum_sensitivity_grade_b_days": 16,
        },
        "permissions": {
            "development_pair_execution_allowed": True,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
            "automatic_live_rollback_authorized": False,
        },
    }
    spec["canonical_spec_sha256"] = audit.canonical_spec_sha256(spec)
    audit.validate_spec(spec)
    spec["permissions"]["live_deployment_authorized"] = True
    spec["canonical_spec_sha256"] = audit.canonical_spec_sha256(spec)
    with pytest.raises(ValueError, match="cannot grant"):
        audit.validate_spec(spec)
