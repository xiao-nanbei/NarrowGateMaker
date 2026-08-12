from __future__ import annotations

import copy
from pathlib import Path

import pandas as pd
import pytest

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_live_action_rate_transport_parity as audit,
)


def _spec() -> dict:
    return audit.load_spec(audit.DEFAULT_SPEC_PATH)


def _shadow_row(
    *,
    timestamp: float,
    client_order_id: str,
    action: str,
    valid: int,
    role: str = "opener",
    edge_ms: int = 0,
    score: float = 0.001,
) -> dict[str, object]:
    spec = _spec()
    policy = spec["policy_identity"]
    favorable = 0.001
    return {
        "timestamp": timestamp,
        "symbol": "BTCUSDC",
        "model_family_id": policy["model_family_id"],
        "model_file_sha256": policy["model_file_sha256"],
        "client_order_id": client_order_id,
        "side": "BUY",
        "inventory_role": role,
        "valid": valid,
        "reason": "ok" if valid else "deep_book_invalid",
        "edge_ms": edge_ms,
        "elapsed_ms": float(edge_ms),
        "missed_edges": 0,
        "feature_source_ts_ns": int(timestamp * 1e9) - 1,
        "feature_ready_ts_ns": int(timestamp * 1e9),
        "deep_generation": 1,
        "deep_age_ms": 25.0,
        "favorable_probability": favorable if valid else float("nan"),
        "adverse_probability": (
            favorable + score if valid else float("nan")
        ),
        "action_authorized": int(valid and role in {"opener", "add"}),
        "executed_action": action,
    }


def _action_row(
    *,
    timestamp: float,
    client_order_id: str,
    event: str,
    score: float,
    hold_age_ms: float,
) -> dict[str, object]:
    spec = _spec()
    policy = spec["policy_identity"]
    favorable = 0.001
    return {
        "timestamp": timestamp,
        "symbol": "BTCUSDC",
        "policy_id": policy["policy_id"],
        "policy_file_sha256": policy["policy_file_sha256"],
        "model_file_sha256": policy["model_file_sha256"],
        "client_order_id": client_order_id,
        "inventory_role": "opener",
        "event": event,
        "adverse_value": score,
        "entry_threshold": policy["entry_threshold"],
        "favorable_probability": favorable,
        "adverse_probability": favorable + score,
        "order_state": (
            "PENDING_CANCEL" if event == "cancel_request" else "CANCELED"
        ),
        "cancel_succeeded": 1,
        "hold_age_ms": hold_age_ms,
        "deep_generation": 1,
        "deep_age_ms": 20.0,
    }


def test_frozen_spec_keeps_all_permissions_closed() -> None:
    spec = _spec()
    assert spec["identity"] == audit.IDENTITY
    assert not any(spec["permissions"].values())


def test_live_shadow_factorization_uses_only_at_risk_rows(
    tmp_path: Path,
) -> None:
    spec = _spec()
    start = spec["live_observation_identity"]["window_start_epoch_s"]
    threshold = spec["policy_identity"]["entry_threshold"]
    rows = [
        _shadow_row(
            timestamp=start,
            client_order_id="a",
            action="keep",
            valid=1,
            edge_ms=0,
        ),
        _shadow_row(
            timestamp=start + 0.1,
            client_order_id="a",
            action="cancel",
            valid=1,
            edge_ms=100,
            score=threshold + 0.001,
        ),
        _shadow_row(
            timestamp=start + 0.2,
            client_order_id="a",
            action="hold",
            valid=1,
            edge_ms=200,
            score=threshold + 0.001,
        ),
        _shadow_row(
            timestamp=start + 0.3,
            client_order_id="b",
            action="invalid_keep",
            valid=0,
            edge_ms=0,
        ),
        _shadow_row(
            timestamp=start + 0.4,
            client_order_id="c",
            action="baseline",
            valid=1,
            role="reducing",
            edge_ms=0,
        ),
    ]
    path = tmp_path / "shadow.csv"
    pd.DataFrame(rows).to_csv(path, index=False)

    summary, daily = audit.summarize_live_shadow(
        path,
        spec=spec,
        chunksize=2,
        sample_stride=1,
    )

    assert summary["rows"] == 5
    assert summary["at_risk_evaluations"] == 3
    assert summary["valid_at_risk_evaluations"] == 2
    assert summary["first_threshold_crossings"] == 1
    assert summary["action_counts"]["hold"] == 1
    assert daily["shadow_cancel_actions"].sum() == 1


def test_action_parser_pairs_cancel_recovery_and_next_cancel(
    tmp_path: Path,
) -> None:
    spec = _spec()
    start = spec["live_observation_identity"]["window_start_epoch_s"]
    threshold = spec["policy_identity"]["entry_threshold"]
    rows = [
        _action_row(
            timestamp=start,
            client_order_id="a",
            event="cancel_request",
            score=threshold + 0.001,
            hold_age_ms=0.0,
        ),
        _action_row(
            timestamp=start + 0.2,
            client_order_id="a",
            event="score_recovered",
            score=threshold - 0.001,
            hold_age_ms=200.0,
        ),
        _action_row(
            timestamp=start + 1.0,
            client_order_id="b",
            event="cancel_request",
            score=threshold + 0.002,
            hold_age_ms=0.0,
        ),
    ]
    path = tmp_path / "actions.csv"
    pd.DataFrame(rows).to_csv(path, index=False)

    summary, episodes, next_cancels = audit.summarize_live_actions(
        path,
        spec=spec,
    )

    assert summary["cancel_request_count"] == 2
    assert summary["paired_cancel_recovery_count"] == 1
    assert summary["unmatched_cancel_count"] == 1
    assert episodes.iloc[0]["cancel_to_recovery_s"] == pytest.approx(0.2)
    assert next_cancels.iloc[0]["recovery_to_next_cancel_s"] == pytest.approx(
        0.8
    )


def test_rate_decomposition_closes_and_coarse_comparison_localizes_ratio() -> None:
    shadow = {
        "rows": 10_000,
        "eligible_order_time_s": 100.0,
        "at_risk_evaluations": 1_000,
        "valid_at_risk_evaluations": 800,
        "first_threshold_crossings": 10,
        "action_counts": {"cancel": 10},
    }
    actions = {"cancel_request_count": 10}
    decomposition = audit.build_live_rate_decomposition(
        shadow=shadow,
        actions=actions,
        wall_hours=2.0,
        closure_tolerance=1e-12,
    )
    assert decomposition["closure_passed"] is True
    assert decomposition["observed_cancel_requests_per_hour"] == 5.0

    replay = {
        "evaluations_per_hour": 5_000.0,
        "cancel_requests_per_evaluation": 1e-6,
        "cancel_requests_per_hour": 0.005,
    }
    comparison = audit.compare_live_historical_rates(
        live_shadow=shadow,
        live_actions=actions,
        live_hours=2.0,
        replay=replay,
    )
    assert comparison["observed_cancel_rate_ratio"] == pytest.approx(1_000.0)
    assert (
        comparison["dominant_observed_coarse_factor"]
        == "cancel_requests_per_evaluation_ratio"
    )


def test_classifier_does_not_call_churn_an_engineering_bug() -> None:
    decision = audit.classify_transport(
        semantic_identity={"q90_sensitive_semantics_equal": True},
        live_decomposition={
            "closure_passed": True,
            "shadow_action_count_matches_action_log": True,
        },
        comparison={
            "dominant_observed_coarse_factor": (
                "cancel_requests_per_evaluation_ratio"
            )
        },
        action_summary={"paired_cancel_recovery_count": 20},
    )
    assert decision["classification"] == (
        "score_state_or_market_regime_transport_divergence"
    )
    assert decision["state_machine_churn_supported"] is True
    assert decision["engineerable_bug_identified"] is False
    assert decision["transport_supported"] is False


def test_spec_rejects_permission_escalation() -> None:
    spec = copy.deepcopy(_spec())
    spec["permissions"]["live_deployment_authorized"] = True
    spec["canonical_spec_sha256"] = audit.canonical_spec_sha256(spec)
    with pytest.raises(ValueError, match="cannot grant permissions"):
        audit.validate_spec(spec)
