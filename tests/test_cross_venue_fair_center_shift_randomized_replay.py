from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models.audit.experiment_scorecard import score_profile_contract
from research.families.f09_campaign_action_uplift.audit import (
    cross_venue_fair_center_shift_randomized_replay as randomized,
)
from research.families.f09_campaign_action_uplift.audit.cross_venue_fair_center_shift import (
    CANDIDATE_ACTION,
    CONTROL_ACTION,
)


def _spec() -> dict[str, object]:
    days = [f"2026-06-{day:02d}" for day in range(1, 13)]
    return {
        "canonical_spec_identity_sha256": "f" * 64,
        "panels": {
            "development_days": days,
            "grade_a_days": days,
            "grade_b_days": [],
        },
        "behavior_policy": {
            "random_seed": 17,
            "probabilities": {
                CONTROL_ACTION: 0.5,
                CANDIDATE_ACTION: 0.5,
            },
        },
        "actions": {"candidate": {"max_state_age_ms": 2_000.0}},
        "operational_config_identity": {
            "required_semantics": {
                "ml_enabled": False,
                "fill_cooldown_s": 85.0,
                "consecutive_reset": "opposite_fill_only",
                "reducing_cooldown_s": 0.0,
                "max_consecutive_losses": 3,
                "loss_cooldown_s": 30.0,
                "markout_side_asymmetry_sign": 1.0,
                "buy_q90_enabled_in_source": True,
            }
        },
        "replay_contract": {"trace_campaigns_max_per_day": 10_000},
        "scorecard_profile": score_profile_contract("action_alpha_v1"),
        "bootstrap": {"draws": 200, "seed": 20260801},
        "family_gates": {
            "actual_action_change_rate_lcb_min": 0.05,
            "minimum_activity_retention": 0.85,
            "side_nonharm_margin_usdc_per_assignment": 0.005,
            "tail_nonharm_margin_usdc_per_assignment": 0.005,
        },
    }


def _panel() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for variant in randomized.VARIANTS:
        for day_index, day in enumerate(_spec()["panels"]["development_days"]):
            for row_index in range(24):
                candidate = row_index % 2 == 1
                action = CANDIDATE_ACTION if candidate else CONTROL_ACTION
                side = "BUY" if (row_index // 2) % 2 == 0 else "SELL"
                reward = 0.03 if candidate else -0.03
                rows.append(
                    {
                        "variant": variant,
                        "day": day,
                        "decision_id": f"{variant}:{day}:{row_index}",
                        "campaign_id": day_index * 100 + row_index,
                        "side": side,
                        "opener_side": side,
                        "campaign_started": 1,
                        "action": action,
                        "behavior_propensity": 0.5,
                        "decision_to_campaign_terminal_value_usdc": reward,
                        "lineage_mae": -0.01 if candidate else -0.04,
                        "lineage_max_abs_inventory": 0.001,
                        "inventory_time_btc_s": 0.5 if candidate else 1.0,
                        "campaign_censored": 0 if candidate else 1,
                        "assignment_ts_ms": 1_000,
                        "campaign_terminal_ts_ms": (
                            11_000 if candidate else 101_000
                        ),
                        "campaign_terminal_reason": (
                            "flat" if candidate else "day_end_mtm_censored"
                        ),
                        "fill_count": 1,
                        "buy_fill_count": int(side == "BUY"),
                        "sell_fill_count": int(side == "SELL"),
                        "actual_final_action_change_count": int(candidate),
                        "candidate_coordinate_change_count": int(candidate),
                        "maker_violation_count": 0,
                        "action_generated_ioc_or_taker_count": 0,
                        "queue_reset_count": 0,
                        "replace_cancel_request_count": 0,
                        "order_submit_count": 2,
                        "support_valid": 1,
                        "transport_supported": 0,
                    }
                )
    return pd.DataFrame(rows)


def test_canonical_spec_identity_excludes_only_identity_field() -> None:
    payload = {"schema_version": randomized.SCHEMA_VERSION, "value": 1}
    identity = randomized.canonical_spec_sha256(payload)
    frozen = {**payload, "canonical_spec_identity_sha256": identity}

    assert randomized.canonical_spec_sha256(frozen) == identity
    assert randomized.canonical_spec_sha256({**frozen, "value": 2}) != identity


def test_config_freezes_q90_off_wall_clock_and_python_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_params = {
        "ml_enabled": False,
        "fill_cooldown": 85.0,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_reducing": 0.0,
        "max_consecutive_losses": 3,
        "cooldown_after_loss": 30.0,
        "markout_side_asymmetry_sign": 1.0,
        "dynamic_fill_hazard_action_enabled": True,
    }
    monkeypatch.setattr(
        randomized.full_path,
        "_configure_params",
        lambda base, day: dict(source_params),
    )

    params = randomized._configure_params(_spec(), {}, "2026-06-01")

    assert params["fill_cooldown_clock_mode"] == "wall_time"
    assert params["cross_venue_fair_center_shift_enabled"] is True
    assert params["dynamic_fill_hazard_action_enabled"] is False
    assert params["dynamic_fill_hazard_cpp_parity_enabled"] is False
    assert params["replay_promotion_eligible"] is False


def test_panel_requires_all_variants_days_and_exact_half_propensity() -> None:
    panel = _panel()
    randomized.validate_panel(panel, _spec())

    missing = panel[~panel["variant"].eq("leave_okx_out")].copy()
    with pytest.raises(ValueError, match="variants"):
        randomized.validate_panel(missing, _spec())

    wrong = panel.copy()
    wrong.loc[0, "behavior_propensity"] = 0.49
    with pytest.raises(ValueError, match="0.5"):
        randomized.validate_panel(wrong, _spec())


def test_all_venue_grade_a_positive_panel_passes_alpha_scorecard() -> None:
    panel = _panel()
    all_venues = panel[panel["variant"].eq("all_venues")].copy()

    report, evidence, scorecard = randomized.evaluate_scope(
        all_venues,
        scope_id="all_venues.grade_a_primary",
        spec=_spec(),
        primary=True,
    )

    assert report["actual_action_change"]["lcb95"] == pytest.approx(1.0)
    assert report["fill_retention"] == pytest.approx(1.0)
    assert report["activity_retention"] == pytest.approx(1.0)
    assert not report["family_gate_failures"]
    assert evidence is not None
    assert scorecard is not None
    assert scorecard["hard_gates"]["passed"]
    assert scorecard["ranking_eligible"]
    assert np.isfinite(scorecard["ranking_score"])


def test_side_harm_cannot_be_rescued_by_pooled_positive_reward() -> None:
    panel = _panel()
    all_venues = panel[panel["variant"].eq("all_venues")].copy()
    harmed = all_venues["side"].eq("SELL") & all_venues["action"].eq(
        CANDIDATE_ACTION
    )
    all_venues.loc[harmed, "decision_to_campaign_terminal_value_usdc"] = -0.20

    report, _, scorecard = randomized.evaluate_scope(
        all_venues,
        scope_id="all_venues.grade_a_primary",
        spec=_spec(),
        primary=True,
    )

    assert "sell_side_material_harm_not_excluded" in report["family_gate_failures"]
    assert scorecard is not None
    assert not scorecard["ranking_eligible"]


def test_day_checkpoint_persists_large_tables_before_worker_return(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result = {
        "day": "2026-06-01",
        "rows": [{"campaign_id": 1, "reward": 0.01}],
        "events": [{"lineage_id": 1, "event_seq": 1}],
        "summaries": [],
        "fair_support": {"day": "2026-06-01"},
        "runtime_s": 1.25,
    }
    monkeypatch.setattr(
        randomized,
        "_run_day",
        lambda *args, **kwargs: dict(result),
    )

    payload = randomized._persist_day_result(
        {},
        {},
        {},
        pd.DataFrame(),
        "2026-06-01",
        tmp_path,
        "a" * 64,
    )
    loaded = randomized._load_day_checkpoint(
        tmp_path / "2026-06-01.json",
        spec_sha256="a" * 64,
    )

    assert loaded == payload
    assert "rows" not in payload["result"]
    assert "events" not in payload["result"]
    assert pd.read_parquet(payload["rows"]["path"]).to_dict("records") == [
        {"campaign_id": 1, "reward": 0.01}
    ]
