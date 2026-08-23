from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_closeout_amendment_v2 as subject,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_closeout_v1 as v1,
)


def _band(hypothesis: str, *, lcb: float = 0.2) -> dict[str, Any]:
    return {
        "hypothesis": hypothesis,
        "mean_usdc": 0.3,
        "standard_error_usdc": 0.05,
        "lcb_usdc": lcb,
        "ucb_usdc": 0.4,
        "day_count": 20,
    }


def _family(hypotheses: tuple[str, ...], *, lcb: float = 0.2) -> dict[str, Any]:
    return {
        "confidence": 0.95,
        "critical_value": 2.0,
        "draws": 99_999,
        "seed": 20260813,
        "shared_days": [f"2026-07-{day:02d}" for day in range(1, 21)],
        "bands": {name: _band(name, lcb=lcb) for name in hypotheses},
    }


def _joint_report() -> dict[str, Any]:
    candidates = tuple(
        f"{side}:{candidate}" for side in ("BUY", "SELL") for candidate in v1.EXPECTED_CANDIDATES
    )
    hierarchy_hypotheses = tuple(
        f"successor:{side}:{suffix}"
        for side in ("BUY", "SELL")
        for suffix in ("E1-B0", "E2-E1", "E3-E2", "M2-E3", "CONTINUOUS-BOOLEAN")
    )
    confirmatory_hypotheses = tuple(
        f"successor:{side}:{suffix}"
        for side in ("BUY", "SELL")
        for suffix, _candidate, _reference in nested.CONFIRMATORY_COMPARISONS
    )
    risk_hypotheses = tuple(
        f"{candidate}:{metric}" for candidate in candidates for metric in nested.RISK_METRIC_COLUMNS
    )
    confirmatory_day = _family(confirmatory_hypotheses)
    confirmatory_week = _family(confirmatory_hypotheses)
    for hypothesis in confirmatory_hypotheses:
        if ":CONTINUOUS-" in hypothesis:
            confirmatory_day["bands"][hypothesis]["lcb_usdc"] = -0.2
            confirmatory_week["bands"][hypothesis]["lcb_usdc"] = -0.2
    report = {
        "schema_version": f"{v1.IDENTITY}.joint_oof_statistics.v1",
        "source_formal_schema_version": nested.IDENTITY,
        "oof_evidence_scope": nested.OOF_EVIDENCE_SCOPE,
        "exact_final_artifact_oof_available": False,
        "final_refit_performed": False,
        "candidate_reports": {name: {} for name in candidates},
        "stability": {name: {} for name in candidates},
        "candidate_bands": _family(candidates),
        "candidate_week_bands": _family(candidates),
        "hierarchy_bands": _family(hierarchy_hypotheses),
        "hierarchy_week_bands": _family(hierarchy_hypotheses),
        "confirmatory_bands": confirmatory_day,
        "confirmatory_week_bands": confirmatory_week,
        "risk_bands": _family(risk_hypotheses),
        "risk_week_bands": _family(risk_hypotheses),
        "scorecards": {name: {"profile": {"profile_id": "action_alpha_v1"}} for name in candidates},
        "hierarchy": {"steps": {}, "supported_sides": []},
        "score_profile_contract": nested.SCORE_PROFILE_CONTRACT,
        "outer_oof_row_count": 520,
        "outer_fold_count_by_side": {"BUY": 4, "SELL": 4},
        "simultaneous_family_sides": ["BUY", "SELL"],
        "permissions": {
            "final_policy_frozen": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    return report


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False) + "\n",
        encoding="ascii",
    )
    os.chmod(path, 0o600)


def _v1_closeout(
    tmp_path: Path,
) -> tuple[Path, dict[str, Any], dict[str, str]]:
    root = tmp_path / "v1"
    root.mkdir()
    report = _joint_report()
    report_path = root / "joint_oof_report.json"
    _write_json(report_path, report)

    owner = {
        "schema_version": f"{v1.IDENTITY}.owner_decision.v1",
        "identity": v1.IDENTITY,
        "status": "owner_override_recorded_artifact_not_yet_frozen",
        "research_supported": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "formal_closeout_mutated": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "formal_hard_gate_failures": ["conditional_net_value_lower_bound_not_positive"],
        "evidence_boundary": {
            "panel_role": "Development",
            "learning_algorithm_oof_only": True,
            "exact_final_artifact_oof_available": False,
            "old_oof_estimate_applies_to_exact_owner_artifact": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "new_economic_arm_run": False,
        },
        "permissions": subject.LOCKED_PERMISSIONS,
    }
    owner["canonical_owner_decision_sha256"] = v1.document_sha256(
        owner, "canonical_owner_decision_sha256"
    )
    owner_path = root / "owner_decision.json"
    _write_json(owner_path, owner)

    days = [f"2026-07-{day:02d}" for day in range(1, 21)]
    rows = pd.DataFrame(
        [
            {
                "side": side,
                "utc_day": day,
                "candidate_name": candidate,
                "point_identified": True,
            }
            for side in ("BUY", "SELL")
            for candidate in v1.EXPECTED_CANDIDATES
            for day in days
        ]
    )
    rows_path = root / "joint_outer_oof_rows.parquet"
    rows.to_parquet(rows_path, index=False)
    os.chmod(rows_path, 0o600)

    files = {}
    for path in (report_path, owner_path, rows_path):
        files[path.name] = {
            "sha256": v1.file_sha256(path),
            "size_bytes": path.stat().st_size,
            "mode": "0600",
        }
    manifest = {
        "schema_version": f"{v1.IDENTITY}.closeout_manifest.v1",
        "identity": v1.IDENTITY,
        "status": "formal_statistics_rebuilt_owner_override_recorded",
        "files": files,
        "row_frames": {
            "joint": {
                "rows": 520,
                "frame_sha256": replay_adapter._frame_sha256(rows),
            }
        },
        "permissions": subject.LOCKED_PERMISSIONS,
    }
    manifest["canonical_manifest_sha256"] = v1.document_sha256(
        manifest, "canonical_manifest_sha256"
    )
    manifest_path = root / "manifest.json"
    _write_json(manifest_path, manifest)
    identity = {
        "manifest_file_sha256": v1.file_sha256(manifest_path),
        "manifest_canonical_sha256": manifest["canonical_manifest_sha256"],
        "joint_report_file_sha256": v1.file_sha256(report_path),
        "joint_report_canonical_sha256": v1.canonical_sha256(report),
        "owner_decision_file_sha256": v1.file_sha256(owner_path),
        "owner_decision_canonical_sha256": owner["canonical_owner_decision_sha256"],
        "joint_rows_file_sha256": v1.file_sha256(rows_path),
        "joint_rows_frame_sha256": replay_adapter._frame_sha256(rows),
    }
    return root, report, identity


def test_complete_hierarchy_requires_every_simplified_and_matched_contrast() -> None:
    report = _joint_report()
    failed = "successor:BUY:E1_FULL_EMA_BANK-ACTION_MATCHED"
    report["confirmatory_week_bands"]["bands"][failed]["lcb_usdc"] = -0.01

    hierarchy = subject.build_complete_joint_hierarchy(report)

    buy_e1 = hierarchy["steps"]["BUY"][0]
    assert buy_e1["tested"] is True
    assert buy_e1["passed"] is False
    assert buy_e1["reason"] == "required_positive_contrast_failed"
    assert [item["hypothesis"] for item in buy_e1["required_contrasts"]] == [
        "successor:BUY:E1-B0",
        "successor:BUY:E1-B1",
        "successor:BUY:E1-B2",
        "successor:BUY:E1-B3",
        "successor:BUY:E1_FULL_EMA_BANK-ACTION_MATCHED",
    ]
    assert hierarchy["steps"]["BUY"][1]["tested"] is False
    assert "BUY" not in hierarchy["supported_sides"]


def test_complete_hierarchy_applies_all_continuous_dominance_blockers() -> None:
    report = _joint_report()
    dominated = "successor:SELL:CONTINUOUS-E3_HIGHER_ORDER_BOOLEAN"
    report["confirmatory_bands"]["bands"][dominated]["lcb_usdc"] = 0.01

    hierarchy = subject.build_complete_joint_hierarchy(report)

    sell_representation = hierarchy["steps"]["SELL"][-1]
    assert sell_representation["tested"] is True
    assert sell_representation["passed"] is False
    assert sell_representation["reason"] == "continuous_representation_proven_superior"
    assert len(sell_representation["required_contrasts"]) == 4
    assert hierarchy["supported_sides"] == ["BUY"]


def test_amendment_binds_joint_report_hierarchy_and_false_permissions(tmp_path: Path) -> None:
    v1_root, _report, identity = _v1_closeout(tmp_path)
    output = tmp_path / "v2"

    manifest = subject.run_amendment(
        v1_closeout_dir=v1_root,
        output_dir=output,
        expected_v1_identity=identity,
    )
    decision = json.loads((output / "owner_decision_amendment_v2.json").read_text())
    hierarchy = json.loads((output / "complete_joint_hierarchy.json").read_text())

    assert manifest["status"] == "complete_joint_hierarchy_amendment_frozen"
    assert decision["joint_oof_report_binding"]["file_sha256"] == v1.file_sha256(
        v1_root / "joint_oof_report.json"
    )
    assert decision["joint_oof_report_binding"]["canonical_sha256"] == v1.canonical_sha256(_report)
    assert decision["complete_joint_hierarchy_sha256"] == v1.canonical_sha256(hierarchy)
    assert decision["formal_hierarchy_passed"] is False
    assert decision["formal_closeout_mutated"] is False
    assert decision["permissions"] == subject.LOCKED_PERMISSIONS
    assert decision["evidence_boundary"] == subject.LOCKED_EVIDENCE_BOUNDARY
    assert manifest["permissions"] == subject.LOCKED_PERMISSIONS


def test_amendment_rejects_joint_report_file_drift(tmp_path: Path) -> None:
    v1_root, report, identity = _v1_closeout(tmp_path)
    report["outer_oof_row_count"] = 519
    _write_json(v1_root / "joint_oof_report.json", report)

    with pytest.raises(subject.OwnerBuyE3CloseoutAmendmentError, match="file binding drifted"):
        subject.run_amendment(
            v1_closeout_dir=v1_root,
            output_dir=tmp_path / "v2",
            expected_v1_identity=identity,
        )
