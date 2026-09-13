from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_finalizer as finalizer,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_training as training,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_boolean_cooldown_rule_learner import (
    OuterOOFGate,
)

DAYS = ("2026-01-01", "2026-01-02")
BUY_ACTIONS = ("CONTROL_85N", *(f"BUY_{index}" for index in range(1, 8)))
SELL_ACTIONS = ("CONTROL_85N", *(f"SELL_{index}" for index in range(1, 8)))
PREDICATES = ("predicate::gold", "predicate::death")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _contract(root: Path) -> training.TrainingContract:
    return training.TrainingContract(
        spec_path=str(root / "spec.json"),
        spec_sha256="1" * 64,
        outcome_blind_path=str(root / "outcome-blind.json"),
        outcome_blind_sha256="2" * 64,
        ordered_utc_days=DAYS,
        expected_opportunities=4,
        expected_arm_rows=32,
        actions_by_side={"BUY": BUY_ACTIONS, "SELL": SELL_ACTIONS},
        predicate_names=PREDICATES,
        required_outer_folds=1,
        outer_fold_source_path=str(root / "folds.json"),
        outer_fold_source_sha256="4" * 64,
        outer_fold_field="chronological_oof",
        outer_test_days_by_fold=((DAYS[1],),),
        outer_fold_binding_sha256="5" * 64,
        predicate_schema_sha256=_canonical_sha(list(PREDICATES)),
        synthetic_test_only=True,
    )


def _admitted(root: Path, contract: training.TrainingContract) -> training.AdmittedPanel:
    rows: list[dict[str, Any]] = []
    for day_index, day in enumerate(DAYS):
        for side in ("BUY", "SELL"):
            actions = contract.actions_by_side[side]
            opportunity_id = f"{day}:{side}"
            for action_index, action in enumerate(actions):
                rows.append(
                    {
                        "opportunity_id": opportunity_id,
                        "utc_day": day,
                        "side": side,
                        "campaign_side_id": f"campaign:{day}:{side}",
                        "assignment_ts_ns": (day_index + 1) * 1_000_000_000,
                        "washout_ts_ns": (day_index + 1) * 1_000_000_000 + 1,
                        "candidate_policy_id": action,
                        "assignment_to_washout_value_usdc": action_index / 100.0,
                        "joint_censored": False,
                        "predicate::gold": side == "BUY",
                        "predicate::death": side == "SELL",
                    }
                )
    part_bindings: tuple[dict[str, Any], ...] = ()
    return training.AdmittedPanel(
        frame=pd.DataFrame(rows),
        arm_manifest_path=str(root / training.ARM_MANIFEST_NAME),
        arm_manifest_sha256="6" * 64,
        training_manifest_path=str(root / training.TRAINING_MANIFEST_NAME),
        training_manifest_sha256="7" * 64,
        census_manifest_path=str(root / "census_manifest.json"),
        census_manifest_sha256="8" * 64,
        execution_identity_sha256="9" * 64,
        opportunity_count=4,
        arm_row_count=32,
        joint_censored_opportunities=0,
        training_label_opportunities=4,
        part_bindings=part_bindings,
        part_bindings_sha256=_canonical_sha(part_bindings),
    )


def _formal_identity(
    admitted: training.AdmittedPanel,
    contract: training.TrainingContract,
) -> dict[str, Any]:
    return finalizer._expected_formal_identity(admitted, contract).artifact()


def _oof() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "outer_fold": 0,
                "opportunity_id": f"{DAYS[1]}:{side}",
                "side": side,
                "utc_day": DAYS[1],
                "campaign_side_id": f"campaign:{DAYS[1]}:{side}",
                "campaign_weight": 1.0,
                "chosen_action": "CONTROL_85N",
                "chosen_value_usdc": 0.0,
                "control_value_usdc": 0.0,
                "policy_minus_control_usdc": 0.0,
                "policy_sha256": ("a" if side == "BUY" else "b") * 64,
                "training_side": side,
            }
            for side in ("BUY", "SELL")
        ]
    )


def _publish_training(
    root: Path,
    admitted: training.AdmittedPanel,
    contract: training.TrainingContract,
) -> Path:
    root.mkdir()
    oof = _oof()
    evidence = pd.DataFrame(
        [
            {
                "outer_fold": 0,
                "selected": True,
                "max_literals_per_clause": 1,
                "max_clauses": 1,
                "training_side": side,
            }
            for side in ("BUY", "SELL")
        ]
    )
    chronology = pd.DataFrame(
        [
            {
                "outer_fold": 0,
                "train_max_day": DAYS[0],
                "test_min_day": DAYS[1],
                "future_training_leakage": False,
                "outer_outcomes_used_for_fit": False,
                "training_side": side,
            }
            for side in ("BUY", "SELL")
        ]
    )
    oof.to_parquet(root / "oof.parquet", index=False)
    evidence.to_parquet(root / "complexity_evidence.parquet", index=False)
    chronology.to_parquet(root / "chronology_audit.parquet", index=False)

    formal_identity = _formal_identity(admitted, contract)
    policy_input_contract = {
        "spec_sha256": contract.spec_sha256,
        "outcome_blind_sha256": contract.outcome_blind_sha256,
        "outer_fold_source_sha256": contract.outer_fold_source_sha256,
        "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
        "execution_identity_sha256": admitted.execution_identity_sha256,
        "part_bindings_sha256": admitted.part_bindings_sha256,
        "formal_input_identity_sha256": formal_identity[
            "formal_input_identity_sha256"
        ],
    }
    learner_sha = _sha(Path(finalizer.learner.__file__).resolve())
    outer_policies = {
        "identity": finalizer.IDENTITY,
        "development_only": True,
        "input_contract": policy_input_contract,
        "policies": {
            "BUY": [{"implementation_sha256": learner_sha}],
            "SELL": [{"implementation_sha256": learner_sha}],
        },
        "permissions": finalizer.LEARNER_PERMISSIONS,
    }
    _write_json(root / "outer_policies.json", outer_policies)

    side_results: dict[str, Any] = {}
    for side in ("BUY", "SELL"):
        summary = training._side_summary(oof.loc[oof["side"].eq(side)])
        side_results[side] = {
            **summary,
            "panel_audit": {
                "input_opportunities": 2,
                "eligible_opportunities": 2,
                "joint_censored_opportunities": 0,
                "excluded_opportunity_ids": [],
                "campaign_weight_min": 1.0,
                "campaign_weight_max": 1.0,
            },
            "outer_policy_count": 1,
        }
    report = {
        "identity": finalizer.IDENTITY,
        "status": "development_nested_chronological_oof_complete_no_authority",
        "economic_outcomes_read": True,
        "evidence_role": "historical_native_development_only",
        "input": {
            "spec_path": contract.spec_path,
            "spec_sha256": contract.spec_sha256,
            "outcome_blind_path": contract.outcome_blind_path,
            "outcome_blind_sha256": contract.outcome_blind_sha256,
            "arm_trace_manifest_path": admitted.arm_manifest_path,
            "arm_trace_manifest_sha256": admitted.arm_manifest_sha256,
            "joint_outcome_training_manifest_path": admitted.training_manifest_path,
            "joint_outcome_training_manifest_sha256": admitted.training_manifest_sha256,
            "census_manifest_path": admitted.census_manifest_path,
            "census_manifest_sha256": admitted.census_manifest_sha256,
            "execution_identity_sha256": admitted.execution_identity_sha256,
            "outer_fold_source_path": contract.outer_fold_source_path,
            "outer_fold_source_sha256": contract.outer_fold_source_sha256,
            "outer_fold_field": contract.outer_fold_field,
            "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
            "predicate_schema_sha256": contract.predicate_schema_sha256,
            "part_bindings_sha256": admitted.part_bindings_sha256,
            "formal_input_identity": formal_identity,
        },
        "denominator": {
            "ordered_utc_days": list(DAYS),
            "opportunity_count": 4,
            "arm_row_count": 32,
            "actions_per_opportunity": 8,
            "joint_censored_opportunities": 0,
            "training_label_opportunities": 4,
            "whole_opportunity_censor_exclusion_used": False,
            "complete_case_filtering_used": False,
        },
        "training_settings": {
            "bootstrap_samples": 500,
            "confidence": 0.95,
            "economic_epsilon_usdc": 0.0,
            "side_pooling": "forbidden",
        },
        "side_results": side_results,
        "permissions": finalizer.FINAL_PERMISSIONS,
    }
    _write_json(root / "development_report.json", report)

    input_bindings = {
        "spec_path": contract.spec_path,
        "spec_sha256": contract.spec_sha256,
        "outcome_blind_path": contract.outcome_blind_path,
        "outcome_blind_sha256": contract.outcome_blind_sha256,
        "outer_fold_source_path": contract.outer_fold_source_path,
        "outer_fold_source_sha256": contract.outer_fold_source_sha256,
        "outer_fold_binding_sha256": contract.outer_fold_binding_sha256,
        "execution_identity_sha256": admitted.execution_identity_sha256,
        "census_manifest_path": admitted.census_manifest_path,
        "census_manifest_sha256": admitted.census_manifest_sha256,
        "arm_trace_manifest_path": admitted.arm_manifest_path,
        "arm_trace_manifest_sha256": admitted.arm_manifest_sha256,
        "joint_outcome_training_manifest_path": admitted.training_manifest_path,
        "joint_outcome_training_manifest_sha256": admitted.training_manifest_sha256,
        "predicate_schema_sha256": contract.predicate_schema_sha256,
        "part_bindings": [],
        "part_bindings_sha256": admitted.part_bindings_sha256,
        "formal_input_identity": formal_identity,
    }
    artifact_rows = {
        "oof": len(oof),
        "complexity_evidence": len(evidence),
        "chronology_audit": len(chronology),
        "outer_policies": None,
        "development_report": None,
    }
    manifest = {
        "schema_version": f"{training.OUTPUT_SCHEMA_VERSION}.artifact_manifest",
        "identity": finalizer.IDENTITY,
        "status": "atomic_development_training_artifacts_admitted",
        "input_execution_identity_sha256": admitted.execution_identity_sha256,
        "input_bindings": input_bindings,
        "formal_denominator": {
            "ordered_utc_days": list(DAYS),
            "opportunity_count": 4,
            "arm_row_count": 32,
            "predicate_column_count": len(PREDICATES),
            "outer_test_days_by_zero_based_fold": [[DAYS[1]]],
        },
        "training_orchestrator_sha256": _sha(Path(training.__file__).resolve()),
        "artifacts": {
            name: {
                "path": filename,
                "sha256": _sha(root / filename),
                "rows": artifact_rows[name],
            }
            for name, filename in finalizer.EXPECTED_TRAINING_ARTIFACTS.items()
        },
        "permissions": finalizer.FINAL_PERMISSIONS,
    }
    manifest_path = root / finalizer.TRAINING_MANIFEST_NAME
    _write_json(manifest_path, manifest)
    (root / "_SUCCESS").write_text(f"{_sha(manifest_path)}\n", encoding="ascii")
    return root


@dataclass
class _FakeFreeze:
    side: str
    outer_oof_gate: OuterOOFGate

    def artifact(self) -> dict[str, Any]:
        return {
            "identity": finalizer.IDENTITY,
            "side": self.side,
            "outer_oof_gate": self.outer_oof_gate.artifact(),
            "permissions": finalizer.LEARNER_PERMISSIONS,
        }


def _patch_inputs(
    monkeypatch: pytest.MonkeyPatch,
    admitted: training.AdmittedPanel,
) -> None:
    monkeypatch.setattr(training, "load_formal_arm_panel", lambda *_args, **_kwargs: admitted)
    monkeypatch.setattr(training, "_validate_nested_result", lambda *_args, **_kwargs: None)


def _patch_gates(
    monkeypatch: pytest.MonkeyPatch,
    *,
    passing_sides: set[str],
    freeze_calls: list[str],
) -> None:
    def evaluate(_result: Any, *, side: str, **kwargs: Any) -> OuterOOFGate:
        assert kwargs == {
            "economic_epsilon_usdc": 0.0,
            "confidence": 0.95,
            "bootstrap_samples": 500,
            "synthetic_mode": True,
        }
        passed = side in passing_sides
        return OuterOOFGate(
            side=side,
            point_uplift_usdc=0.1 if passed else -0.1,
            lower_confidence_bound_usdc=0.01 if passed else -0.2,
            economic_epsilon_usdc=0.0,
            confidence=0.95,
            bootstrap_samples=500,
            outer_folds=(0,),
            test_days=(DAYS[1],),
            campaign_count=1,
            passed=passed,
            synthetic_test_only=True,
        )

    def freeze(_panel: pd.DataFrame, _result: Any, *, side: str, **kwargs: Any) -> _FakeFreeze:
        assert side in passing_sides
        assert kwargs["confidence"] == 0.95
        assert kwargs["bootstrap_samples"] == 500
        assert kwargs["economic_epsilon_usdc"] == 0.0
        assert kwargs["formal_input_identity"].arm_row_count == 32
        freeze_calls.append(side)
        return _FakeFreeze(
            side=side,
            outer_oof_gate=evaluate(
                _result,
                side=side,
                economic_epsilon_usdc=kwargs["economic_epsilon_usdc"],
                confidence=kwargs["confidence"],
                bootstrap_samples=kwargs["bootstrap_samples"],
                synthetic_mode=kwargs["synthetic_mode"],
            ),
        )

    monkeypatch.setattr(finalizer, "evaluate_outer_oof_gate", evaluate)
    monkeypatch.setattr(finalizer, "freeze_final_side_policy", freeze)


def test_no_passing_side_publishes_explicit_atomic_closure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arm_root = tmp_path / "arm"
    arm_root.mkdir()
    contract = _contract(arm_root)
    admitted = _admitted(arm_root, contract)
    training_dir = _publish_training(tmp_path / "training", admitted, contract)
    _patch_inputs(monkeypatch, admitted)
    freeze_calls: list[str] = []
    _patch_gates(monkeypatch, passing_sides=set(), freeze_calls=freeze_calls)

    output = tmp_path / "final"
    report = finalizer.finalize_post_training(
        training_dir, arm_root, output, contract=contract
    )

    assert freeze_calls == []
    assert report["status"] == "development_closed_no_side_policy_frozen"
    assert report["permissions"] == finalizer.FINAL_PERMISSIONS
    assert (output / "closure.json").is_file()
    assert not (output / "final_policy_bundle.json").exists()
    closure = json.loads((output / "closure.json").read_text(encoding="utf-8"))
    assert closure["status"] == "closed_no_side_passed_frozen_outer_oof_gate"
    assert closure["permissions"] == finalizer.FINAL_PERMISSIONS
    manifest = output / finalizer.FINALIZER_MANIFEST_NAME
    assert (output / "_SUCCESS").read_text(encoding="ascii").strip() == _sha(manifest)
    assert not list(tmp_path.glob(".final.staging.*"))


def test_only_gate_passing_side_is_refit_and_permissions_stay_locked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arm_root = tmp_path / "arm"
    arm_root.mkdir()
    contract = _contract(arm_root)
    admitted = _admitted(arm_root, contract)
    training_dir = _publish_training(tmp_path / "training", admitted, contract)
    _patch_inputs(monkeypatch, admitted)
    freeze_calls: list[str] = []
    _patch_gates(monkeypatch, passing_sides={"BUY"}, freeze_calls=freeze_calls)

    output = tmp_path / "final"
    report = finalizer.finalize_post_training(
        training_dir, arm_root, output, contract=contract
    )

    assert freeze_calls == ["BUY"]
    assert report["passing_sides"] == ["BUY"]
    assert report["full_development_refit_sides"] == ["BUY"]
    assert report["permissions"] == finalizer.FINAL_PERMISSIONS
    bundle = json.loads((output / "final_policy_bundle.json").read_text(encoding="utf-8"))
    assert bundle["policy_count"] == 1
    assert set(bundle["side_policies"]) == {"BUY"}
    assert bundle["permissions"] == finalizer.LEARNER_PERMISSIONS
    assert not (output / "closure.json").exists()


def test_tampered_training_artifact_fails_before_gate_or_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arm_root = tmp_path / "arm"
    arm_root.mkdir()
    contract = _contract(arm_root)
    admitted = _admitted(arm_root, contract)
    training_dir = _publish_training(tmp_path / "training", admitted, contract)
    _patch_inputs(monkeypatch, admitted)
    (training_dir / "oof.parquet").write_bytes(
        (training_dir / "oof.parquet").read_bytes() + b"tampered"
    )

    with pytest.raises(finalizer.PostTrainingFinalizerError, match="SHA256 mismatch"):
        finalizer.finalize_post_training(
            training_dir,
            arm_root,
            tmp_path / "final",
            contract=contract,
        )
    assert not (tmp_path / "final").exists()


def test_training_success_must_bind_exact_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arm_root = tmp_path / "arm"
    arm_root.mkdir()
    contract = _contract(arm_root)
    admitted = _admitted(arm_root, contract)
    training_dir = _publish_training(tmp_path / "training", admitted, contract)
    _patch_inputs(monkeypatch, admitted)
    (training_dir / "_SUCCESS").write_text(f"{'f' * 64}\n", encoding="ascii")

    with pytest.raises(
        finalizer.PostTrainingFinalizerError,
        match="_SUCCESS does not bind",
    ):
        finalizer.finalize_post_training(
            training_dir,
            arm_root,
            tmp_path / "final",
            contract=contract,
        )
    assert not (tmp_path / "final").exists()


def test_rehashed_but_wrong_formal_input_identity_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    arm_root = tmp_path / "arm"
    arm_root.mkdir()
    contract = _contract(arm_root)
    admitted = _admitted(arm_root, contract)
    training_dir = _publish_training(tmp_path / "training", admitted, contract)
    _patch_inputs(monkeypatch, admitted)

    manifest_path = training_dir / finalizer.TRAINING_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    identity = manifest["input_bindings"]["formal_input_identity"]
    identity["arm_row_count"] += 8
    body = {
        key: value
        for key, value in identity.items()
        if key != "formal_input_identity_sha256"
    }
    identity["formal_input_identity_sha256"] = _canonical_sha(body)
    _write_json(manifest_path, manifest)
    (training_dir / "_SUCCESS").write_text(
        f"{_sha(manifest_path)}\n", encoding="ascii"
    )

    with pytest.raises(
        finalizer.PostTrainingFinalizerError,
        match="differs from the original arm panel",
    ):
        finalizer.finalize_post_training(
            training_dir,
            arm_root,
            tmp_path / "final",
            contract=contract,
        )
    assert not (tmp_path / "final").exists()
