from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from live.config import Config, _validate_config
from models import backtest_tick
from strategy.maker_engine import MakerEngine, SidePolicyDecision, _load_state_conditioned_policy
from strategy.order_manager import Side
from strategy.state_conditioned_quote_policy import (
    LOCAL_QUOTE_ACTIONS,
    SCHEMA_VERSION,
    PolicyArtifact,
    StateConditionedQuotePolicy,
    inventory_role_for_quote,
)


def _artifact(*, status: str = "shadow_only", uplift_lcb: float = 0.2) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "policy_id": "unit-test-local-action-v1",
        "promotion_status": status,
        "trained_through_day": "2026-07-13",
        "input_scope": "local_only",
        "actions": list(LOCAL_QUOTE_ACTIONS),
        "features": [
            {"name": "inventory_ratio", "mean": 0.0, "scale": 1.0},
            {"name": "microprice_shift_bps", "mean": 0.0, "scale": 2.0},
        ],
        "gates": {
            "min_support_rows": 100,
            "min_behavior_probability": 0.05,
            "min_advantage": 0.01,
            "max_feature_age_ms": 50.0,
        },
        "models": {
            "BUY:add": {
                "baseline": {
                    "intercept": 0.0,
                    "coefficients": {},
                    "support_rows": 1_000,
                    "behavior_probability_floor": 0.7,
                    "uplift_lcb": 0.0,
                },
                "recenter_1tick": {
                    "intercept": 0.1,
                    "coefficients": {"microprice_shift_bps": 0.5},
                    "support_rows": 500,
                    "behavior_probability_floor": 0.1,
                    "uplift_lcb": uplift_lcb,
                },
            }
        },
    }


def test_shadow_reports_candidate_but_executes_baseline() -> None:
    policy = StateConditionedQuotePolicy(
        PolicyArtifact.from_dict(_artifact()),
        mode="shadow",
    )
    decision = policy.decide(
        side="BUY",
        inventory_role="add",
        features={"inventory_ratio": 0.4, "microprice_shift_bps": 2.0},
        decision_ts_ns=1_000_000_000,
        feature_ready_ts_ns=990_000_000,
    )

    assert decision.eligible is True
    assert decision.candidate_action == "recenter_1tick"
    assert decision.action == "baseline"
    assert decision.reason == "shadow_candidate"


@pytest.mark.parametrize("status", ["shadow_only", "promotion_eligible", "closed"])
def test_active_mechanics_do_not_interpret_research_annotation_as_permission(status) -> None:
    policy = StateConditionedQuotePolicy(
        PolicyArtifact.from_dict(_artifact(status=status)),
        mode="active",
    )
    decision = policy.decide(
        side="BUY",
        inventory_role="add",
        features={"inventory_ratio": 0.4, "microprice_shift_bps": 2.0},
        decision_ts_ns=1_000_000_000,
        feature_ready_ts_ns=990_000_000,
    )

    assert decision.action == "recenter_1tick"
    assert decision.advantage > 0.0


@pytest.mark.parametrize(
    ("role", "ready_ts", "features", "reason"),
    [
        ("reducing", 990_000_000, {"inventory_ratio": 0.4, "microprice_shift_bps": 2.0}, "unsupported_surface"),
        ("add", 1_001_000_000, {"inventory_ratio": 0.4, "microprice_shift_bps": 2.0}, "future_feature"),
        ("add", 900_000_000, {"inventory_ratio": 0.4, "microprice_shift_bps": 2.0}, "stale_feature"),
        ("add", 990_000_000, {"inventory_ratio": 0.4}, "missing_feature:microprice_shift_bps"),
    ],
)
def test_invalid_or_unsupported_state_falls_back_to_baseline(
    role: str,
    ready_ts: int,
    features: dict,
    reason: str,
) -> None:
    policy = StateConditionedQuotePolicy(
        PolicyArtifact.from_dict(_artifact()),
        mode="shadow",
    )
    decision = policy.decide(
        side="BUY",
        inventory_role=role,
        features=features,
        decision_ts_ns=1_000_000_000,
        feature_ready_ts_ns=ready_ts,
    )

    assert decision.action == "baseline"
    assert decision.eligible is False
    assert decision.reason == reason


def test_overlap_and_uplift_lower_bound_are_hard_gates() -> None:
    policy = StateConditionedQuotePolicy(
        PolicyArtifact.from_dict(_artifact(uplift_lcb=-0.1)),
        mode="shadow",
    )
    decision = policy.decide(
        side="BUY",
        inventory_role="add",
        features={"inventory_ratio": 0.4, "microprice_shift_bps": 2.0},
        decision_ts_ns=1_000_000_000,
        feature_ready_ts_ns=990_000_000,
    )

    assert decision.action == "baseline"
    assert decision.reason == "no_supported_candidate"


def test_inventory_role_is_side_specific() -> None:
    assert inventory_role_for_quote("BUY", 0.0, 0.001) == "opener"
    assert inventory_role_for_quote("BUY", 0.003, 0.001) == "add"
    assert inventory_role_for_quote("SELL", 0.003, 0.001) == "reducing"
    assert inventory_role_for_quote("SELL", -0.003, 0.001) == "add"


def test_live_config_checks_artifact_compatibility_without_environment_permission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = Config()
    cfg.strategy.state_conditioned_policy_mode = "shadow"
    with pytest.raises(ValueError, match="model artifact"):
        _validate_config(cfg)

    cfg.strategy.state_conditioned_policy_model_path = "policy.json"
    _validate_config(cfg)

    cfg.strategy.state_conditioned_policy_mode = "active"
    monkeypatch.delenv("NARROWGATE_ALLOW_STATE_CONDITIONED_POLICY_LIVE", raising=False)
    _validate_config(cfg)
    monkeypatch.setenv("NARROWGATE_ALLOW_STATE_CONDITIONED_POLICY_LIVE", "1")
    _validate_config(cfg)


def test_live_state_policy_loads_verified_envelope_bytes(tmp_path: Path) -> None:
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(_artifact(status="closed")), encoding="utf-8")
    authority = {
        "model_policy_member_paths": {"state_conditioned_quote_policy": str(path.resolve())},
        "model_policy_member_sha256": {
            "state_conditioned_quote_policy": hashlib.sha256(path.read_bytes()).hexdigest(),
        },
    }
    cfg = Config()
    cfg.strategy.state_conditioned_policy_mode = "active"
    cfg.strategy.state_conditioned_policy_model_path = "/wrong/policy.json"

    policy = _load_state_conditioned_policy(cfg, artifact_authority=authority)
    assert policy is not None
    assert policy.artifact.promotion_status == "closed"
    assert policy.mode == "active"

    path.write_text(json.dumps(_artifact(status="promotion_eligible")), encoding="utf-8")
    with pytest.raises(ValueError, match="file_sha256_mismatch"):
        _load_state_conditioned_policy(cfg, artifact_authority=authority)


@pytest.mark.parametrize(
    ("field", "replacement", "error"),
    [
        ("schema_version", "unknown", "schema mismatch"),
        ("actions", ["baseline", "cancel_all"], "action registry"),
        ("features", [{"name": "inventory_ratio", "scale": -1}], "invalid transform"),
    ],
)
def test_research_annotation_does_not_bypass_artifact_compatibility(
    field: str, replacement: object, error: str,
) -> None:
    raw = _artifact(status="promotion_eligible")
    raw[field] = replacement
    with pytest.raises(ValueError, match=error):
        PolicyArtifact.from_dict(raw)


def test_live_shadow_uses_same_surface_and_does_not_move_quote() -> None:
    engine = object.__new__(MakerEngine)
    engine.cfg = Config()
    engine.cfg.strategy.max_inventory = 0.02
    engine._best_bid = 99.8
    engine._best_ask = 100.2
    engine._state_conditioned_policy = StateConditionedQuotePolicy(
        PolicyArtifact.from_dict(_artifact()),
        mode="shadow",
    )
    engine._state_conditioned_policy_campaigns = set()
    engine._state_conditioned_policy_shadow_log_path = "unused.csv"
    campaign = SimpleNamespace(
        active=True,
        campaign_id=7,
        age_s=30.0,
        max_abs_qty=0.004,
        total_pnl=-0.2,
        adverse_excursion=-0.5,
        exposure_increasing_fills=2,
        reducing_fills=0,
    )
    engine.inventory = SimpleNamespace(campaign_snapshot=lambda: campaign)
    rows = []
    engine._append_row = lambda path, row: rows.append(row)
    decision = SidePolicyDecision(
        side="BUY",
        toxicity=0.4,
        markout_ema=-0.2,
        microprice_shift_bps=2.0,
        l2_near_depth_total=2.0,
    )

    selected, effective = engine._maybe_apply_state_conditioned_quote_policy(
        side=Side.BUY,
        mid=100.0,
        q=0.004,
        baseline_price=99.0,
        pre_guard_price=99.5,
        other_side_price=101.0,
        max_pair_spread=2.0,
        can_post=True,
        order_active=False,
        order_pending=False,
        decision=decision,
        best_bid=99.8,
        best_ask=100.2,
    )

    assert selected == pytest.approx(99.0)
    assert effective is False
    assert rows[0].candidate_action == "recenter_1tick"
    assert rows[0].candidate_price == pytest.approx(99.1)
    assert rows[0].executed_action == "baseline"

    engine._maybe_apply_state_conditioned_quote_policy(
        side=Side.BUY,
        mid=100.0,
        q=0.004,
        baseline_price=99.0,
        pre_guard_price=99.5,
        other_side_price=101.0,
        max_pair_spread=2.0,
        can_post=True,
        order_active=False,
        order_pending=False,
        decision=decision,
        best_bid=99.8,
        best_ask=100.2,
    )
    assert len(rows) == 1


def test_cpp_replay_fails_closed_for_state_conditioned_policy() -> None:
    with pytest.raises(NotImplementedError, match="state-conditioned"):
        backtest_tick._simulate_tick_with_engine(
            "cpp",
            None,
            None,
            None,
            {"state_conditioned_policy_mode": "shadow"},
        )
