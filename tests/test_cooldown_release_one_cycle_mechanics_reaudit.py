from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit import (
    cooldown_release_one_cycle_mechanics_reaudit as audit,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f09_campaign_action_uplift"
    / "docs"
    / "cooldown_release_one_cycle_mechanics_reaudit_v1_spec_20260730.json"
)


def _decision(
    campaign_id: int,
    ts_ms: int,
    *,
    allow_post: int,
    reason: str,
    action: str,
) -> dict[str, object]:
    return {
        "decision_id": f"d:{campaign_id}:{ts_ms}",
        "ts_ms": ts_ms,
        "side": "SELL",
        "allow_post": allow_post,
        "exposure_increasing": 1,
        "reason_text": reason,
        "action": action,
        "final_price": 100.0,
        "final_size": 0.001,
        "campaign_active": 1,
        "campaign_id": campaign_id,
        "order_active_before": 0,
        "fill_cooldown_elapsed_ms": 85_000,
        "fill_cooldown_total_ms": 85_000.0,
        "fill_cooldown_consecutive_units": 1.0,
    }


def _order(
    campaign_id: int,
    order_id: int,
    submit_ts: int,
    *,
    outcome: str,
    outcome_ts: int,
    fill_qty: float,
) -> dict[str, object]:
    return {
        "order_id": order_id,
        "side": "SELL",
        "campaign_id_at_submit": campaign_id,
        "inventory_role_at_submit": "add",
        "submit_ts": submit_ts,
        "activate_ts": submit_ts + 10,
        "quote_ts": submit_ts,
        "price": 100.0,
        "quantity": 0.001,
        "outcome": outcome,
        "outcome_ts": outcome_ts,
        "cancel_reason": "",
        "fill_qty": fill_qty,
        "remaining": max(0.0, 0.001 - fill_qty),
        "lifetime_ms": outcome_ts - submit_ts,
    }


def test_release_extractor_counts_masking_and_only_the_selected_cycle() -> None:
    decisions = [
        _decision(1, 1_000, allow_post=0, reason="markout", action="pause"),
        _decision(1, 2_000, allow_post=1, reason="none", action="place"),
        _decision(2, 3_000, allow_post=1, reason="none", action="place"),
        _decision(3, 4_000, allow_post=0, reason="q90", action="pause"),
    ]
    orders = [
        _order(1, 11, 2_000, outcome="fill", outcome_ts=2_500, fill_qty=0.001),
        _order(1, 12, 7_000, outcome="fill", outcome_ts=7_500, fill_qty=0.001),
        _order(2, 21, 3_000, outcome="cancel", outcome_ts=8_000, fill_qty=0.0),
    ]
    observer = [
        {
            "campaign_id": 1,
            "side": "SELL",
            "decision_ts_ms": 2_000,
            "action_effective": 0,
            "blocked_quote_cycles": 0,
            "reward": 0.02,
            "campaign_censored": 0,
        },
        {
            "campaign_id": 2,
            "side": "SELL",
            "decision_ts_ms": 3_000,
            "action_effective": 0,
            "blocked_quote_cycles": 0,
            "reward": -0.01,
            "campaign_censored": 0,
        },
    ]

    frame, sufficient = audit.extract_release_opportunities(
        "2026-04-20", "SELL", decisions, orders, observer
    )

    assert len(frame) == 3
    first = frame.set_index("campaign_id").loc[1]
    assert first["masked_at_release"] == 1
    assert first["release_to_eligible_ms"] == 1_000
    assert first["release_first_blocker"] == "markout"
    assert first["selected_cycle_any_fill"] == 1
    assert first["selected_cycle_fill_qty_btc"] == pytest.approx(0.001)
    assert frame.set_index("campaign_id").loc[2, "masked_at_release"] == 0
    assert frame.set_index("campaign_id").loc[3, "baseline_eligible_observed"] == 0
    assert sufficient == pytest.approx(
        {"count": 2.0, "sum": 0.01, "sum_sq": 0.0005}
    )


def test_core_trace_digest_ignores_observer_metadata() -> None:
    left = [_order(1, 10, 2_000, outcome="cancel", outcome_ts=3_000, fill_qty=0.0)]
    right = [
        {
            **left[0],
            "state_conditioned_rearm_intervention_id": 1,
            "state_conditioned_rearm_action": "baseline_rearm",
        }
    ]
    assert audit.core_trace_digest(left, audit.ORDER_CORE_FIELDS) == audit.core_trace_digest(
        right, audit.ORDER_CORE_FIELDS
    )


def test_mde_uses_within_day_variance_and_not_outcome_mean() -> None:
    centered = pd.DataFrame(
        {
            "mde_count": [2, 2],
            "mde_sum": [0.0, 0.0],
            "mde_sum_sq": [2.0, 8.0],
        }
    )
    shifted = centered.copy()
    shifted["mde_sum"] = [20.0, -40.0]
    shifted["mde_sum_sq"] = [202.0, 808.0]

    assert audit.mde_from_day_sufficient(
        centered, alpha=0.05, power=0.80
    ) == pytest.approx(
        audit.mde_from_day_sufficient(shifted, alpha=0.05, power=0.80)
    )


def test_frozen_spec_forbids_action_registration() -> None:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    with pytest.raises(
        (FileNotFoundError, ValueError),
        match=(
            "normalized L2 manifest|"
            "operational config (?:is missing|hash mismatch)"
        ),
    ):
        audit.validate_spec(spec)
    assert not spec["diagnostic_contract"][
        "may_register_randomized_action_on_pass"
    ]

    drifted = json.loads(json.dumps(spec))
    drifted["diagnostic_contract"]["may_register_randomized_action_on_pass"] = True
    with pytest.raises(ValueError, match="hash mismatch|cannot be re-registered"):
        audit.validate_spec(drifted)
