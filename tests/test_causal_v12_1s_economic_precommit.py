from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

from data.quality.calendar_gap_manifest import canonical_sha256
from data_paths import resolve_portable_path
from models.audit.experiment_scorecard_v2 import (
    score_profile_contract,
    score_profile_payload,
)
from research.governance.public_machine_projection import (
    projection_for,
    source_document_path,
    source_identity_sha256,
)

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_cadence_full_path_economic_precommit_v1_20260805.json"
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _spec() -> dict:
    return json.loads(SPEC.read_text(encoding="utf-8"))


def _load_bound(identity: dict, path_key: str, sha_key: str) -> dict:
    path = ROOT / identity[path_key]
    assert source_identity_sha256(path) == identity[sha_key]
    source_path = source_document_path(path, require_private=False)
    return json.loads(source_path.read_text(encoding="utf-8"))


def _assert_bound(identity: dict, path_key: str, sha_key: str) -> None:
    path = ROOT / identity[path_key]
    assert source_identity_sha256(path) == identity[sha_key]


def _load_public_projection_bound(identity: dict, path_key: str, sha_key: str) -> dict:
    path = ROOT / identity[path_key]
    projection = projection_for(path)
    assert projection is not None
    assert _sha256(path) == projection.public_projection_sha256 == identity[sha_key]
    assert source_identity_sha256(path) == projection.source_private_sha256
    source_path = source_document_path(path, require_private=False)
    return json.loads(source_path.read_text(encoding="utf-8"))


def _calendar_days(start: str, end: str) -> list[str]:
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    return [
        (first + timedelta(days=offset)).isoformat() for offset in range((last - first).days + 1)
    ]


def test_precommit_binds_current_v9_and_exact_native_40_days() -> None:
    payload = _spec()
    baseline = payload["baseline"]
    source = payload["native_development_panel"]

    assert payload["status"] == (
        "frozen_before_candidate_training_predictions_or_economic_outcomes"
    )
    identity = _load_bound(baseline, "identity_path", "identity_sha256")
    pointer_path = ROOT / baseline["current_pointer_path"]
    pointer_projection = projection_for(pointer_path)
    assert pointer_projection is not None
    assert _sha256(pointer_path) == pointer_projection.public_projection_sha256
    assert source_identity_sha256(pointer_path) == pointer_projection.source_private_sha256
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer_identity = _load_public_projection_bound(
        pointer, "identity_path", "identity_sha256"
    )
    current_config = resolve_portable_path(pointer["live_config_path"], root=ROOT)
    assert _sha256(current_config) == pointer["live_config_sha256"]
    source_profile = _load_bound(source, "source_profile_path", "source_profile_sha256")
    # ``operational_baseline_current.json`` is intentionally mutable.  The
    # precommit remains bound to the immutable v9 identity even after this
    # pointer advances; the current pointer must instead validate its own
    # identity and config.
    if _sha256(pointer_path) == baseline["current_pointer_sha256"]:
        assert pointer["identity_path"] == baseline["identity_path"]
        assert pointer["identity_sha256"] == baseline["identity_sha256"]
    assert pointer["historical_frozen_specs_rewritten"] is False
    assert pointer_identity["baseline_id"] == pointer["baseline_id"]
    assert (
        ROOT
        / resolve_portable_path(
            identity["config"]["canonical_private_source"], root=ROOT
        )
    ).resolve() == (ROOT / resolve_portable_path(baseline["config_path"], root=ROOT)).resolve()
    assert identity["config"]["sha256"] == baseline["config_sha256"]
    assert identity["operational_status"] == "active_operational_and_backtest_baseline"
    assert identity["config"]["ml_enabled"] is True
    assert identity["config"]["dynamic_fill_hazard_action_enabled"] is False
    assert identity["config"]["buy_fill_selection_live_enabled"] is False
    assert identity["config"]["buy_fill_selection_shadow_enabled"] is False
    assert baseline["ml_enabled"] == identity["config"]["ml_enabled"]
    assert (
        baseline["q90_action_enabled_both_arms"]
        is identity["config"]["dynamic_fill_hazard_action_enabled"]
    )
    assert (
        baseline["buy_fill_selection_enabled_both_arms"]
        is identity["config"]["buy_fill_selection_live_enabled"]
    )
    assert (
        baseline["buy_fill_selection_shadow_enabled_both_arms"]
        is identity["config"]["buy_fill_selection_shadow_enabled"]
    )
    assert (
        baseline["quote_snapshot_atomicity_contract"]
        == pointer["quote_snapshot_atomicity_contract"]
    )
    assert source["day_count"] == len(source["days"]) == 40
    assert source["days"] == sorted(set(source["days"]))
    assert source["days"] == source_profile["development_40"]["days"]
    assert source["source_authority"] == source_profile["profile"]["profile_id"]
    assert source["independent_confirmation"] is False


def test_only_model_and_cadence_change_between_arms() -> None:
    payload = _spec()
    intervention = payload["intervention"]
    baseline = payload["baseline"]

    assert intervention["axis"] == "model_feature_and_inference_cadence"
    assert intervention["candidate_cadence_ms"] == 1000
    assert intervention["control_cadence_ms"] == 10000
    unchanged = [key for key, value in intervention.items() if key.endswith("changed")]
    assert unchanged
    assert all(intervention[key] is False for key in unchanged)
    assert baseline["q90_action_enabled_both_arms"] is False
    assert baseline["buy_fill_selection_enabled_both_arms"] is False
    assert baseline["p3_identity_shared_by_both_arms"] is True
    training = payload["candidate_training"]
    _assert_bound(training, "training_contract_path", "training_contract_sha256")


def test_continuous_scorecard_and_owner_route_are_frozen_fail_closed() -> None:
    payload = _spec()
    scorecard = payload["scorecard"]
    gates = payload["common_noncompensable_gates"]
    owner = payload["owner_progression_path"]
    permissions = payload["execution_permissions"]
    governance = payload["governance"]

    assert scorecard["contract"] == score_profile_contract("action_alpha_v2")
    assert _sha256(ROOT / scorecard["implementation_path"]) == scorecard["implementation_sha256"]
    payload_path = ROOT / scorecard["frozen_payload_path"]
    assert _sha256(payload_path) == scorecard["frozen_payload_sha256"]
    assert json.loads(payload_path.read_text(encoding="utf-8")) == (
        score_profile_payload("action_alpha_v2")
    )
    profile_payload = score_profile_payload("action_alpha_v2")
    assert (
        gates["fill_retention_minimum"]
        == profile_payload["hard_gates"]["minimum_fills_retention"]
        == 0.85
    )
    assert gates["fill_retention_maximum"] == 1.2
    assert gates["candidate_rate_minimum"] == profile_payload["hard_gates"]["candidate_rate"][0]
    assert gates["candidate_rate_maximum"] == profile_payload["hard_gates"]["candidate_rate"][1]
    assert (
        gates["minimum_reward_daily_positive_rate"]
        == profile_payload["hard_gates"]["minimum_reward_daily_positive_rate"]
    )
    assert gates["ranking_score_null_on_any_gate_failure"] is True
    dual_path_contract = ROOT / governance["dual_path_contract_path"]
    dual_path_text = dual_path_contract.read_text(encoding="utf-8")
    assert len(governance["dual_path_contract_sha256"]) == 64
    assert "research_supported_promotion" in dual_path_text
    assert "owner_risk_accepted_promotion" in dual_path_text
    assert owner["promotion_label"] == "owner_risk_accepted_promotion"
    assert owner["native_40_day_primary_and_continuous_71_day_sensitivity_both_required"]
    assert owner["action_alpha_v2_scorecard_result_preserved_exactly"] is True
    assert owner["ranking_score_must_remain_null_if_action_alpha_v2_fails"] is True
    assert owner["accepted_owner_override"] == {
        "field": "fills_retention",
        "scorecard_minimum": 0.85,
        "owner_minimum": 0.8,
        "owner_maximum": 1.2,
        "hard_gate_failure_preserved_when_below_scorecard_minimum": True,
    }
    assert owner["all_non_overridden_common_noncompensable_gates_required"] is True
    assert permissions["native_development_pnl_read_authorized_after_execution_amendment"]
    assert permissions["validation_read_authorized"] is False
    assert permissions["live_canary_authorized"] is False


def test_precommit_cannot_attach_confirmation_panels_after_development() -> None:
    payload = _spec()
    panels = payload["confirmation_panels"]
    research = payload["research_supported_path"]

    for name in ("validation", "family_specific_sealed_holdout"):
        panel = panels[name]
        assert panel["status"] == "not_registered_in_this_identity"
        assert panel["days"] == []
        assert panel["post_development_attachment_allowed"] is False
    assert (
        panels[
            "research_supported_successor_requires_new_precommit_before_any_successor_outcome_read"
        ]
        is True
    )
    assert research["available_from_current_40_day_panel"] is False
    assert research["available_in_this_identity"] is False


def test_direct_canary_requires_runtime_safety_and_no_long_shadow() -> None:
    payload = _spec()
    runtime = payload["runtime_gates_before_direct_canary"]
    owner = payload["owner_progression_path"]
    continuous = payload["continuous_confirmation"]

    manifest = _load_bound(continuous, "calendar_manifest_path", "calendar_manifest_sha256")
    expected_days = _calendar_days(continuous["calendar_start"], continuous["calendar_end"])
    assert continuous["calendar_days"] == expected_days
    assert continuous["calendar_day_count"] == len(expected_days) == 71
    assert manifest["calendar_days"][:71] == expected_days
    assert manifest["canonical_manifest_sha256"] == continuous["calendar_manifest_canonical_sha256"]
    assert canonical_sha256(manifest) == continuous["calendar_manifest_canonical_sha256"]
    assert manifest["anchor_target_days"] == payload["native_development_panel"]["days"]
    assert continuous["required_active_trading_day_count"] == 71
    assert continuous["whole_day_placeholders_allowed"] is False
    assert continuous["execution_amendment_required_before_pnl_read"] is True
    assert continuous["full_path_runner_bound_at_freeze"] is False
    assert continuous["continuous_execution_authorized_at_freeze"] is False
    assert continuous["required_for_direct_live_canary"] is True
    assert owner["long_observation_only_shadow_required"] is False
    assert runtime["canary_candidate_fraction"] == 0.1
    assert runtime["same_policy_function_offline_and_live_required"] is True
    assert runtime["python_cpp_parity_required"] is True
    assert runtime["automatic_rollback_required"] is True
    assert runtime["maximum_websocket_queue_drops"] == 0
    assert runtime["maximum_severe_startup_errors"] == 0


def test_no_candidate_prediction_or_economic_outcome_was_read_at_freeze() -> None:
    payload = _spec()
    access = payload["evidence_access_at_freeze"]
    permissions = payload["execution_permissions"]
    training = payload["candidate_training"]

    assert access and not any(access.values())
    assert training["candidate_model_bundle_path"] is None
    assert training["candidate_model_bundle_sha256"] is None
    assert training["candidate_feature_dag_path"] is None
    assert training["candidate_feature_dag_sha256"] is None
    assert permissions["validation_read_authorized"] is False
    assert permissions["sealed_holdout_read_authorized"] is False
    assert permissions["action_authorized"] is False
    assert permissions["live_canary_authorized"] is False
    assert permissions["baseline_replacement_authorized"] is False
