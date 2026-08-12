from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_training as training,
)

BUY_ACTIONS = (
    "CONTROL_85N",
    "FIXED_79S",
    "FIXED_173S",
    "FIXED_223S",
    "FIXED_356S",
    "FIXED_640S",
    "FIXED_709S",
    "FIXED_2048S",
)
SELL_ACTIONS = (
    "CONTROL_85N",
    "FIXED_79S",
    "FIXED_166S",
    "FIXED_211S",
    "FIXED_349S",
    "FIXED_660S",
    "FIXED_686S",
    "FIXED_1748S",
)
DAYS = ("2026-01-01", "2026-01-02", "2026-01-03")
PREDICATES = ("predicate::gold", "predicate::not_death")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_sha(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _contract() -> training.TrainingContract:
    return training.TrainingContract(
        spec_path="synthetic-spec.json",
        spec_sha256="1" * 64,
        outcome_blind_path="synthetic-outcome-blind.json",
        outcome_blind_sha256="2" * 64,
        ordered_utc_days=DAYS,
        expected_opportunities=6,
        expected_arm_rows=48,
        actions_by_side={"BUY": BUY_ACTIONS, "SELL": SELL_ACTIONS},
        predicate_names=PREDICATES,
        required_outer_folds=2,
        outer_fold_source_path="synthetic-folds.json",
        outer_fold_source_sha256="4" * 64,
        outer_fold_field="chronological_oof",
        outer_test_days_by_fold=((DAYS[1],), (DAYS[2],)),
        outer_fold_binding_sha256="5" * 64,
        predicate_schema_sha256=_canonical_sha(list(PREDICATES)),
        synthetic_test_only=True,
    )


def _part_frame(day_index: int) -> pd.DataFrame:
    day = DAYS[day_index]
    rows: list[dict[str, object]] = []
    for side_index, side in enumerate(("BUY", "SELL")):
        opportunity = f"{day}:{side}"
        censored = day_index == 0 and side == "BUY"
        actions = BUY_ACTIONS if side == "BUY" else SELL_ACTIONS
        assignment = (day_index + 1) * 10_000_000_000 + side_index * 1_000_000
        washout = assignment + 100_000_000
        for action_index, action in enumerate(actions):
            right_censored = censored and action_index == 3
            rows.append(
                {
                    "opportunity_id": opportunity,
                    "utc_day": day,
                    "side": side,
                    "campaign_side_id": f"campaign:{day}:{side}",
                    "assignment_ts_ns": assignment,
                    "washout_ts_ns": washout,
                    "duration_policy_id": action,
                    "assignment_to_washout_value_usdc": (
                        np.nan if censored else float(action_index) / 100.0
                    ),
                    "joint_censored": censored,
                    "joint_washout_complete": not censored,
                    "training_label_eligible": not censored,
                    "right_censored": right_censored,
                    "arm_washout_complete": not right_censored,
                    "washout_ts_is_joint_economic_washout": not censored,
                    "predicate::gold": day_index % 2 == 0,
                    "predicate::not_death": side == "SELL",
                }
            )
    return pd.DataFrame(rows)


def _formal_input(root: Path) -> tuple[Path, list[Path]]:
    root.mkdir()
    parts: list[dict[str, object]] = []
    census_parts: list[dict[str, object]] = []
    trace_paths: list[Path] = []
    schema_hash = _canonical_sha(list(PREDICATES))
    for day_index, day in enumerate(DAYS):
        frame = _part_frame(day_index)
        census_root = root / "census" / day
        census_root.mkdir(parents=True)
        census = census_root / "opportunities.parquet"
        frame.drop_duplicates("opportunity_id").loc[
            :, ["opportunity_id", "utc_day", "side", *PREDICATES]
        ].to_parquet(census, index=False)
        census_day_manifest = census_root / "manifest.json"
        _write_json(
            census_day_manifest,
            {
                "identity": training.IDENTITY,
                "utc_day": day,
                "execution_identity_sha256": "3" * 64,
                "data_path": str(census),
                "data_sha256": _sha(census),
                "opportunity_count": 2,
                "exact_formal_fork_task_count": 16,
                "all_legal_exposure_increasing_fill_opportunities_included": True,
                "formal_sampling": "none_full_coverage",
                "economic_outcomes_read": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        )
        census_parts.append(
            {
                "utc_day": day,
                "manifest_path": str(census_day_manifest),
                "manifest_sha256": _sha(census_day_manifest),
                "data_path": str(census),
                "data_sha256": _sha(census),
                "opportunity_count": 2,
                "fork_task_count": 16,
            }
        )

        run_root = root / "runs" / "formal" / day
        run_root.mkdir(parents=True)
        trace = run_root / "arm_traces.parquet"
        frame.to_parquet(trace, index=False)
        trace_paths.append(trace)
        part_manifest = run_root / "manifest.json"
        part_payload = {
            "identity": training.IDENTITY,
            "scope": "formal",
            "utc_day": day,
            "execution_identity_sha256": "3" * 64,
            "census_data_sha256": _sha(census),
            "census_opportunity_count": 2,
            "included_opportunity_count": 2,
            "expected_task_count": len(frame),
            "data_path": str(trace),
            "data_sha256": _sha(trace),
            "arm_trace_row_count": len(frame),
            "limited_diagnostic": False,
            "formal_full_opportunity_coverage": True,
            "python_parity": False,
            "python_full_arm_execution_allowed": False,
            "joint_complete_case_filtering_allowed": False,
            "censor_marks_are_terminal_bounds": False,
            "economic_outcomes_interpreted": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        }
        _write_json(part_manifest, part_payload)
        (run_root / "_SUCCESS").write_text(f"{_sha(part_manifest)}\n", encoding="ascii")
        parts.append(
            {
                "utc_day": day,
                "census_path": str(census),
                "census_sha256": _sha(census),
                "arm_trace_path": str(trace),
                "arm_trace_sha256": _sha(trace),
                "opportunity_count": 2,
                "arm_trace_row_count": len(frame),
                "predicate_column_count": len(PREDICATES),
                "predicate_schema_sha256": schema_hash,
                "manifest_path": str(part_manifest),
                "manifest_sha256": _sha(part_manifest),
            }
        )
    census_manifest = root / "census_manifest.json"
    _write_json(
        census_manifest,
        {
            "identity": training.IDENTITY,
            "status": "formal_full_development_census",
            "execution_identity_sha256": "3" * 64,
            "ordered_utc_days": list(DAYS),
            "day_count": len(DAYS),
            "opportunity_count": 6,
            "exact_formal_fork_task_count": 48,
            "formal_sampling": "none_full_coverage",
            "parts": census_parts,
            "economic_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    )
    arm_manifest = root / training.ARM_MANIFEST_NAME
    arm_payload = {
        "identity": training.IDENTITY,
        "status": "formal_full_development_arm_traces_admitted",
        "execution_identity_sha256": "3" * 64,
        "census_manifest_path": census_manifest.name,
        "census_manifest_sha256": _sha(census_manifest),
        "ordered_utc_days": list(DAYS),
        "opportunity_count": 6,
        "arm_trace_rows": 48,
        "expected_actions_per_side": 8,
        "joint_washout_opportunities": 5,
        "joint_censored_opportunities": 1,
        "training_label_opportunities": 5,
        "parts": parts,
        "formal_sampling": "none_full_cartesian_coverage",
        "joint_complete_case_filtering_allowed": False,
        "censor_marks_are_terminal_bounds": False,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _write_json(arm_manifest, arm_payload)
    training_manifest = root / training.TRAINING_MANIFEST_NAME
    _write_json(
        training_manifest,
        {
            "identity": training.IDENTITY,
            "status": (
                "formal_joint_outcome_training_panel_admitted_with_whole_"
                "opportunity_censor_exclusion"
            ),
            "execution_identity_sha256": "3" * 64,
            "arm_trace_manifest_path": arm_manifest.name,
            "arm_trace_manifest_sha256": _sha(arm_manifest),
            "opportunity_count": 6,
            "arm_rows": 48,
            "actions_per_opportunity": 8,
            "all_materialized_opportunities_have_all_eight_arms": True,
            "joint_censored_opportunities": 1,
            "training_label_eligible_opportunities": 5,
            "whole_opportunity_censor_exclusion_used": True,
            "label_field": "assignment_to_washout_value_usdc",
            "censor_time_marks_in_training_label": False,
            "complete_case_filtering_used": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    )
    return root, trace_paths


def _rebind_manifest_chain(root: Path) -> None:
    census_path = root / "census_manifest.json"
    census = json.loads(census_path.read_text(encoding="utf-8"))
    for row in census["parts"]:
        row["data_sha256"] = _sha(Path(row["data_path"]))
        row["manifest_sha256"] = _sha(Path(row["manifest_path"]))
    _write_json(census_path, census)

    arm_path = root / training.ARM_MANIFEST_NAME
    arm = json.loads(arm_path.read_text(encoding="utf-8"))
    arm["census_manifest_sha256"] = _sha(census_path)
    for row in arm["parts"]:
        row["census_sha256"] = _sha(Path(row["census_path"]))
        row["arm_trace_sha256"] = _sha(Path(row["arm_trace_path"]))
        manifest_path = Path(row["manifest_path"])
        row["manifest_sha256"] = _sha(manifest_path)
        (manifest_path.parent / "_SUCCESS").write_text(
            f"{row['manifest_sha256']}\n", encoding="ascii"
        )
    _write_json(arm_path, arm)

    training_path = root / training.TRAINING_MANIFEST_NAME
    training_manifest = json.loads(training_path.read_text(encoding="utf-8"))
    training_manifest["arm_trace_manifest_sha256"] = _sha(arm_path)
    _write_json(training_path, training_manifest)


def _fake_nested(panel: pd.DataFrame, *, side: str, **_: object) -> SimpleNamespace:
    assert set(panel["side"]) == {side}
    groups = panel.groupby("opportunity_id", sort=True)
    # The loader must preserve all eight arms of the jointly censored opportunity.
    assert all(len(rows) == 8 for _, rows in groups)
    if side == "BUY":
        censored = panel.loc[panel["joint_censored"].astype(bool)]
        assert len(censored) == 8
    raw_opportunities = panel.drop_duplicates("opportunity_id")
    eligible = raw_opportunities.loc[
        ~raw_opportunities["joint_censored"].astype(bool)
        & raw_opportunities["utc_day"].astype(str).isin(DAYS[1:])
    ]
    assert len(eligible) >= 2
    artifacts = []
    for fold in range(2):
        body = {
            "schema_version": f"{training.IDENTITY}.boolean_policy.v1",
            "identity": training.IDENTITY,
            "side": side,
            "ordered_rules": [],
            "default_action": "CONTROL_85N",
            "predicate_columns": sorted(PREDICATES),
            "predicate_artifact_sha256": "2" * 64,
            "duration_spec_sha256": "1" * 64,
            "economic_epsilon_usdc": 0.0,
            "training_fold_identities": [f"outer{fold}.full_train"],
            "simultaneous_band_family_id": f"synthetic:{side}:{fold}",
            "simultaneous_band_family_sha256": "6" * 64,
            "simultaneous_band_family_size": 1,
            "simultaneous_critical_usdc": 0.0,
            "selection_aware_policy_lcb_usdc": 0.0,
            "implementation_sha256": "7" * 64,
            "synthetic_test_only": True,
            "permissions": {
                "action_authorized": False,
                "live_authorized": False,
                "f09_registration_authorized": False,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
        }
        artifacts.append({**body, "policy_sha256": _canonical_sha(body)})
    oof_rows = []
    for fold, (_, row) in enumerate(eligible.sort_values("utc_day").iterrows()):
        oof_rows.append(
            {
                "outer_fold": fold,
                "opportunity_id": row["opportunity_id"],
                "side": side,
                "utc_day": row["utc_day"],
                "campaign_side_id": row["campaign_side_id"],
                "campaign_weight": 1.0,
                "chosen_action": "CONTROL_85N",
                "chosen_value_usdc": 0.0,
                "control_value_usdc": 0.0,
                "policy_minus_control_usdc": 0.0,
                "policy_sha256": artifacts[fold]["policy_sha256"],
            }
        )
    evidence = pd.DataFrame(
        {
            "outer_fold": [0, 1],
            "selected": [True, True],
            "max_literals_per_clause": [1, 1],
            "max_clauses": [2, 2],
        }
    )
    chronology = pd.DataFrame(
        {
            "outer_fold": [0, 1],
            "train_max_day": [DAYS[0], DAYS[1]],
            "test_min_day": [DAYS[1], DAYS[2]],
            "future_training_leakage": [False, False],
            "outer_outcomes_used_for_fit": [False, False],
        }
    )
    censored_ids = tuple(
        raw_opportunities.loc[
            raw_opportunities["joint_censored"].astype(bool), "opportunity_id"
        ].astype(str)
    )
    return SimpleNamespace(
        oof=pd.DataFrame(oof_rows),
        complexity_evidence=evidence,
        chronology_audit=chronology,
        outer_policy_artifacts=tuple(artifacts),
        panel_audit={
            "input_opportunities": 3,
            "eligible_opportunities": 3 - len(censored_ids),
            "joint_censored_opportunities": len(censored_ids),
            "excluded_opportunity_ids": censored_ids,
            "campaign_weight_min": 1.0,
            "campaign_weight_max": 1.0,
        },
        permissions={
            "action_authorized": False,
            "live_authorized": False,
            "f09_registration_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    )


def test_atomic_training_runs_sides_separately_and_preserves_joint_censor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    monkeypatch.setattr(training, "_load_training_contract", _contract)
    calls: list[str] = []

    def run(panel: pd.DataFrame, *, side: str, **kwargs: object) -> SimpleNamespace:
        assert kwargs["economic_epsilon_usdc"] == 0.0
        identity = kwargs["formal_input_identity"]
        assert isinstance(identity, training.FormalInputIdentity)
        assert identity.opportunity_count == 6
        assert identity.arm_row_count == 48
        calls.append(side)
        return _fake_nested(panel, side=side)

    monkeypatch.setattr(training, "run_nested_chronological_oof", run)
    output = tmp_path / "published"
    report = training.train_formal_panel(input_root, output)

    assert calls == ["BUY", "SELL"]
    assert output.is_dir()
    assert not list(tmp_path.glob(".published.staging.*"))
    assert {
        "oof.parquet",
        "complexity_evidence.parquet",
        "chronology_audit.parquet",
        "outer_policies.json",
        "development_report.json",
        "training_artifact_manifest.json",
        "_SUCCESS",
    } == {path.name for path in output.iterdir()}
    assert report["denominator"]["joint_censored_opportunities"] == 1
    assert report["denominator"]["complete_case_filtering_used"] is False
    assert report["permissions"]["action_authorized"] is False
    assert report["permissions"]["live_authorized"] is False
    manifest_path = output / "training_artifact_manifest.json"
    assert (output / "_SUCCESS").read_text(encoding="ascii").strip() == _sha(
        manifest_path
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    bindings = manifest["input_bindings"]
    assert bindings["execution_identity_sha256"] == "3" * 64
    assert bindings["outer_fold_source_sha256"] == "4" * 64
    assert bindings["outer_fold_binding_sha256"] == "5" * 64
    assert len(bindings["part_bindings"]) == len(DAYS)
    assert bindings["part_bindings_sha256"] == _canonical_sha(
        bindings["part_bindings"]
    )
    formal_identity = bindings["formal_input_identity"]
    assert formal_identity["opportunity_count"] == 6
    assert formal_identity["arm_row_count"] == 48
    assert len(formal_identity["formal_input_identity_sha256"]) == 64
    assert all(
        {
            "arm_trace_sha256",
            "manifest_sha256",
            "success_sha256",
            "census_sha256",
            "census_manifest_sha256",
            "opportunity_projection_sha256",
            "predicate_schema_sha256",
        }
        <= set(row)
        for row in bindings["part_bindings"]
    )
    assert manifest["formal_denominator"] == {
        "ordered_utc_days": list(DAYS),
        "opportunity_count": 6,
        "arm_row_count": 48,
        "predicate_column_count": len(PREDICATES),
        "outer_test_days_by_zero_based_fold": [[DAYS[1]], [DAYS[2]]],
    }


def test_formal_chronology_validates_inner_fold_complexity_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel = pd.concat([_part_frame(index) for index in range(len(DAYS))], ignore_index=True)
    side_panel = panel.loc[panel["side"].eq("BUY")].copy()
    result = _fake_nested(side_panel, side="BUY")
    contract = replace(_contract(), synthetic_test_only=False)

    for index, artifact in enumerate(result.outer_policy_artifacts):
        body = {
            key: value
            for key, value in artifact.items()
            if key != "policy_sha256"
        }
        body["synthetic_test_only"] = False
        updated = {**body, "policy_sha256": _canonical_sha(body)}
        result.outer_policy_artifacts = tuple(
            updated if offset == index else value
            for offset, value in enumerate(result.outer_policy_artifacts)
        )
        result.oof.loc[result.oof["outer_fold"].eq(index), "policy_sha256"] = updated[
            "policy_sha256"
        ]

    complexities = ((1, 2), (1, 4), (2, 2), (2, 4))
    expected_folds = []
    chronology_rows: list[dict[str, object]] = []
    for outer_fold, outer_test_day in enumerate(DAYS[1:]):
        inner_folds = []
        for inner_fold in range(2):
            train_day = DAYS[outer_fold]
            inner_test_day = outer_test_day
            inner = SimpleNamespace(
                fold=inner_fold,
                train_days=(train_day,),
                test_days=(inner_test_day,),
            )
            inner_folds.append(inner)
            for max_literals, max_clauses in complexities:
                chronology_rows.append(
                    {
                        "outer_fold": outer_fold,
                        "inner_fold": inner_fold,
                        "max_literals_per_clause": max_literals,
                        "max_clauses": max_clauses,
                        "train_max_day": train_day,
                        "test_min_day": inner_test_day,
                        "future_training_leakage": False,
                        "outer_outcomes_used_for_fit": False,
                    }
                )
        expected_folds.append(
            SimpleNamespace(fold=outer_fold, inner_folds=tuple(inner_folds))
        )
    result.chronology_audit = pd.DataFrame(chronology_rows)
    monkeypatch.setattr(
        training,
        "load_frozen_search_contract",
        lambda: SimpleNamespace(
            max_literals_per_clause=(1, 2),
            max_clauses=(2, 4),
        ),
    )
    monkeypatch.setattr(
        training,
        "build_nested_chronological_folds",
        lambda *_args, **_kwargs: tuple(expected_folds),
    )

    training._validate_nested_result(
        result,
        side="BUY",
        side_panel=side_panel,
        contract=contract,
    )


def test_policy_identity_preserves_frozen_predicate_order() -> None:
    panel = pd.concat([_part_frame(index) for index in range(len(DAYS))], ignore_index=True)
    side_panel = panel.loc[panel["side"].eq("BUY")].copy()
    result = _fake_nested(side_panel, side="BUY")
    frozen_order = tuple(reversed(PREDICATES))
    contract = replace(
        _contract(),
        predicate_names=frozen_order,
        predicate_schema_sha256=_canonical_sha(list(frozen_order)),
    )
    updated_artifacts = []
    for fold, artifact in enumerate(result.outer_policy_artifacts):
        body = {
            key: value
            for key, value in artifact.items()
            if key != "policy_sha256"
        }
        body["predicate_columns"] = list(frozen_order)
        updated = {**body, "policy_sha256": _canonical_sha(body)}
        updated_artifacts.append(updated)
        result.oof.loc[result.oof["outer_fold"].eq(fold), "policy_sha256"] = updated[
            "policy_sha256"
        ]
    result.outer_policy_artifacts = tuple(updated_artifacts)

    training._validate_nested_result(
        result,
        side="BUY",
        side_panel=side_panel,
        contract=contract,
    )


def test_loader_fails_closed_on_tampered_part_hash(tmp_path: Path) -> None:
    input_root, trace_paths = _formal_input(tmp_path / "input")
    trace_paths[0].write_bytes(trace_paths[0].read_bytes() + b"tampered")
    with pytest.raises(training.TrainingAdmissionError, match="SHA256 mismatch"):
        training.load_formal_arm_panel(input_root, contract=_contract())


def test_loader_relocates_one_explicit_admitted_execution_root(tmp_path: Path) -> None:
    original = tmp_path / "original"
    input_root, _ = _formal_input(original)
    archive = tmp_path / "archive"
    input_root.rename(archive)

    admitted = training.load_formal_arm_panel(
        archive,
        contract=_contract(),
        relocated_from_root=original,
    )

    assert admitted.opportunity_count == 6
    assert admitted.arm_row_count == 48
    assert admitted.joint_censored_opportunities == 1


def test_loader_fails_closed_on_validation_or_holdout(tmp_path: Path) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    manifest_path = input_root / training.TRAINING_MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["validation_read"] = True
    _write_json(manifest_path, manifest)
    with pytest.raises(training.TrainingAdmissionError, match="Validation"):
        training.load_formal_arm_panel(input_root, contract=_contract())

    manifest["validation_read"] = False
    manifest["sealed_holdout_read"] = True
    _write_json(manifest_path, manifest)
    with pytest.raises(training.TrainingAdmissionError, match="holdout"):
        training.load_formal_arm_panel(input_root, contract=_contract())


def test_joint_panel_rejects_missing_arm_or_reduced_denominator() -> None:
    panel = pd.concat([_part_frame(index) for index in range(3)], ignore_index=True)
    reduced = panel.drop(panel.index[0]).reset_index(drop=True)
    with pytest.raises(training.TrainingAdmissionError, match="arm-row denominator"):
        training._validate_joint_panel(reduced, _contract())


def test_failed_second_side_leaves_no_partial_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    monkeypatch.setattr(training, "_load_training_contract", _contract)
    first_calls: list[str] = []

    def run(panel: pd.DataFrame, *, side: str, **_: object) -> SimpleNamespace:
        first_calls.append(side)
        if side == "SELL":
            raise ValueError("synthetic SELL failure")
        return _fake_nested(panel, side=side)

    monkeypatch.setattr(training, "run_nested_chronological_oof", run)
    output = tmp_path / "published"
    with pytest.raises(ValueError, match="synthetic SELL failure"):
        training.train_formal_panel(input_root, output)
    assert not output.exists()
    assert not list(tmp_path.glob(".published.staging.*"))
    assert first_calls == ["BUY", "SELL"]

    resumed_calls: list[str] = []

    def resume(panel: pd.DataFrame, *, side: str, **_: object) -> SimpleNamespace:
        resumed_calls.append(side)
        return _fake_nested(panel, side=side)

    monkeypatch.setattr(training, "run_nested_chronological_oof", resume)
    training.train_formal_panel(input_root, output)
    assert resumed_calls == ["SELL"]
    assert output.is_dir()
    assert not (input_root / ".training_checkpoints").exists()


def test_side_checkpoint_tamper_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    monkeypatch.setattr(training, "_load_training_contract", _contract)

    def fail_sell(panel: pd.DataFrame, *, side: str, **_: object) -> SimpleNamespace:
        if side == "SELL":
            raise ValueError("synthetic SELL failure")
        return _fake_nested(panel, side=side)

    monkeypatch.setattr(training, "run_nested_chronological_oof", fail_sell)
    output = tmp_path / "published"
    with pytest.raises(ValueError, match="synthetic SELL failure"):
        training.train_formal_panel(input_root, output)
    checkpoint = next((input_root / ".training_checkpoints").glob("buy-*"))
    oof = checkpoint / "oof.parquet"
    oof.write_bytes(oof.read_bytes() + b"tampered")

    with pytest.raises(training.TrainingAdmissionError, match="oof hash drifted"):
        training.train_formal_panel(input_root, output)
    assert not output.exists()


def test_real_frozen_contract_binds_exact_days_folds_and_denominators() -> None:
    contract = training._load_training_contract()

    assert contract.spec_sha256 == training.FROZEN_SPEC_SHA256
    assert contract.outcome_blind_sha256 == training.FROZEN_OUTCOME_BLIND_SHA256
    assert contract.outer_fold_source_sha256 == training.FROZEN_FOLD_SOURCE_SHA256
    assert len(contract.ordered_utc_days) == training.EXPECTED_FORMAL_DAYS
    assert contract.expected_opportunities == training.EXPECTED_FORMAL_OPPORTUNITIES
    assert contract.expected_arm_rows == training.EXPECTED_FORMAL_ARM_ROWS
    assert len(contract.predicate_names) == training.EXPECTED_PREDICATE_COLUMNS
    assert len(contract.outer_test_days_by_fold) == training.EXPECTED_OUTER_FOLDS
    assert tuple(day for fold in contract.outer_test_days_by_fold for day in fold) == (
        contract.ordered_utc_days[-24:]
    )


def test_loader_rejects_internally_rebound_census_execution_identity(
    tmp_path: Path,
) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    day_manifest_path = input_root / "census" / DAYS[0] / "manifest.json"
    day_manifest = json.loads(day_manifest_path.read_text(encoding="utf-8"))
    day_manifest["execution_identity_sha256"] = "8" * 64
    _write_json(day_manifest_path, day_manifest)
    _rebind_manifest_chain(input_root)

    with pytest.raises(training.TrainingAdmissionError, match="census admission drifted"):
        training.load_formal_arm_panel(input_root, contract=_contract())


def test_loader_rejects_tampered_success_marker(tmp_path: Path) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    success = input_root / "runs" / "formal" / DAYS[0] / "_SUCCESS"
    success.write_text(f"{'9' * 64}\n", encoding="ascii")

    with pytest.raises(training.TrainingAdmissionError, match="success marker"):
        training.load_formal_arm_panel(input_root, contract=_contract())


def test_part_schema_rejects_predicate_name_substitution(tmp_path: Path) -> None:
    path = tmp_path / "bad-predicates.parquet"
    _part_frame(0).rename(
        columns={"predicate::gold": "predicate::substituted"}
    ).to_parquet(path, index=False)

    with pytest.raises(training.TrainingAdmissionError, match="predicate schema/order"):
        training._validate_part_schema(
            path,
            contract=_contract(),
            expected_schema_sha256=_contract().predicate_schema_sha256,
        )


def test_loader_rejects_rehashed_census_arm_predicate_state_divergence(
    tmp_path: Path,
) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    census_path = input_root / "census" / DAYS[0] / "opportunities.parquet"
    census = pd.read_parquet(census_path)
    census.loc[census.index[0], "predicate::gold"] = not bool(
        census.loc[census.index[0], "predicate::gold"]
    )
    census.to_parquet(census_path, index=False)

    census_manifest_path = census_path.parent / "manifest.json"
    census_manifest = json.loads(census_manifest_path.read_text(encoding="utf-8"))
    census_manifest["data_sha256"] = _sha(census_path)
    _write_json(census_manifest_path, census_manifest)
    part_manifest_path = input_root / "runs" / "formal" / DAYS[0] / "manifest.json"
    part_manifest = json.loads(part_manifest_path.read_text(encoding="utf-8"))
    part_manifest["census_data_sha256"] = _sha(census_path)
    _write_json(part_manifest_path, part_manifest)
    _rebind_manifest_chain(input_root)

    with pytest.raises(training.TrainingAdmissionError, match="opportunity state drifted"):
        training.load_formal_arm_panel(input_root, contract=_contract())


def test_joint_panel_rejects_arm_level_censor_hidden_by_joint_flags() -> None:
    panel = pd.concat([_part_frame(index) for index in range(3)], ignore_index=True)
    opportunity = f"{DAYS[0]}:BUY"
    selected = panel["opportunity_id"].eq(opportunity)
    panel.loc[selected, "joint_censored"] = False
    panel.loc[selected, "joint_washout_complete"] = True
    panel.loc[selected, "training_label_eligible"] = True
    panel.loc[selected, "washout_ts_is_joint_economic_washout"] = True

    with pytest.raises(training.TrainingAdmissionError, match="joint-censor contract"):
        training._validate_joint_panel(panel, _contract())


def test_outer_oof_day_drift_fails_without_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    monkeypatch.setattr(training, "_load_training_contract", _contract)

    def run(panel: pd.DataFrame, *, side: str, **_: object) -> SimpleNamespace:
        result = _fake_nested(panel, side=side)
        result.oof.loc[result.oof.index[0], "utc_day"] = DAYS[0]
        return result

    monkeypatch.setattr(training, "run_nested_chronological_oof", run)
    output = tmp_path / "published"
    with pytest.raises(training.TrainingAdmissionError, match="test days drifted"):
        training.train_formal_panel(input_root, output)
    assert not output.exists()
    assert not list(tmp_path.glob(".published.staging.*"))


def test_learner_must_report_exact_whole_opportunity_exclusion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    monkeypatch.setattr(training, "_load_training_contract", _contract)

    def run(panel: pd.DataFrame, *, side: str, **_: object) -> SimpleNamespace:
        result = _fake_nested(panel, side=side)
        if side == "BUY":
            result.panel_audit["excluded_opportunity_ids"] = ()
        return result

    monkeypatch.setattr(training, "run_nested_chronological_oof", run)
    with pytest.raises(training.TrainingAdmissionError, match="whole-opportunity exclusion"):
        training.train_formal_panel(input_root, tmp_path / "published")


def test_artifact_write_failure_atomically_cleans_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root, _ = _formal_input(tmp_path / "input")
    monkeypatch.setattr(training, "_load_training_contract", _contract)
    monkeypatch.setattr(training, "run_nested_chronological_oof", _fake_nested)
    original_write = training._write_json

    def fail_on_report(path: Path, value: Any) -> None:
        if path.name == "development_report.json":
            raise OSError("synthetic artifact failure")
        original_write(path, value)

    monkeypatch.setattr(training, "_write_json", fail_on_report)
    output = tmp_path / "published"
    with pytest.raises(OSError, match="synthetic artifact failure"):
        training.train_formal_panel(input_root, output)
    assert not output.exists()
    assert not list(tmp_path.glob(".published.staging.*"))


def test_formal_bootstrap_and_confidence_are_not_caller_tunable(tmp_path: Path) -> None:
    with pytest.raises(training.TrainingAdmissionError, match="settings are frozen"):
        training.train_formal_panel(
            tmp_path / "not-read",
            tmp_path / "not-written",
            bootstrap_samples=training.FORMAL_BOOTSTRAP_SAMPLES - 1,
        )
