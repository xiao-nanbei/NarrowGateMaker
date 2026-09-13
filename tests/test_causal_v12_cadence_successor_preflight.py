from __future__ import annotations

import json
from pathlib import Path

import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_cadence_successor_preflight as preflight,
)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _fixture_design(tmp_path: Path) -> Path:
    design = json.loads(preflight.DEFAULT_DESIGN_PATH.read_text(encoding="utf-8"))
    examples = sorted(
        {
            feature
            for group in design["feature_basis_inventory"]
            for feature in group["required_feature_examples"]
        }
    )

    baseline_identity_path = tmp_path / "baseline_identity.json"
    _write_json(baseline_identity_path, {"identity": "test-baseline"})
    baseline_pointer_path = tmp_path / "baseline_pointer.json"
    baseline_pointer = {
        **design["current_reference"]["operational_baseline"],
        "identity_sha256": preflight.sha256_file(baseline_identity_path),
    }
    _write_json(baseline_pointer_path, baseline_pointer)

    bundle_path = tmp_path / "bundle.json"
    _write_json(bundle_path, {"targets": list(preflight.EXPECTED_HEADS)})

    feature_generator_path = tmp_path / "feature_generator.py"
    feature_generator_path.write_text(
        "RESAMPLE_SEC = 10\nLABEL_HORIZONS = [10, 30, 60]\nTOXICITY_HORIZONS = [5, 10]\n",
        encoding="utf-8",
    )
    signal_path = tmp_path / "signal.py"
    signal_path.write_text(
        "Only produces a NEW prediction when a complete 10s bucket has elapsed\n"
        "return list(range(start_ms, completed_exclusive_ms, 10_000))\n",
        encoding="utf-8",
    )

    validation_days = ["2026-01-01"]
    test_days = ["2026-01-02"]
    feature_manifest_path = tmp_path / "features.json"
    feature_manifest = {
        "feature_bucket_ms": 10_000,
        "feature_ready_offset_ms": 10_000,
        "feature_dag_id": "live_10s_signal_cutoff.v1",
        "feature_semantics_version": 6,
        "label_semantics_version": 3,
        "label_window_semantics": "left_closed_right_open_[t,t+h)",
        "split": {
            "validation": validation_days,
            "test": test_days,
        },
    }
    _write_json(feature_manifest_path, feature_manifest)

    native_transport_path = tmp_path / "native_transport.json"
    _write_json(
        native_transport_path,
        {
            "panels": [
                {
                    "role": "historical_native_transport_development",
                    "days": validation_days,
                    "independent_confirmation": False,
                },
                {
                    "role": "historical_native_late_diagnostic",
                    "days": test_days,
                    "independent_confirmation": False,
                },
            ]
        },
    )
    postfit_path = tmp_path / "postfit.json"
    _write_json(
        postfit_path,
        {
            "panels": [
                {
                    "role": "postfit_native_oos_diagnostic_grade_a",
                    "days": ["2026-01-03"],
                    "independent_confirmation": False,
                },
                {
                    "role": "postfit_native_gap_sensitivity",
                    "days": ["2026-01-04"],
                    "independent_confirmation": False,
                },
            ]
        },
    )

    training_spec_path = tmp_path / "training_spec.json"
    _write_json(
        training_spec_path,
        {
            "experiment_id": "causal-v12-test",
            "head_names": list(preflight.EXPECTED_HEADS),
            "fit_days": ["2025-01-01"],
            "selection_days": ["2025-01-02"],
            "refit_days": ["2025-01-01", "2025-01-02"],
        },
    )
    training_summary_path = tmp_path / "training_summary.json"
    _write_json(
        training_summary_path,
        {
            "targets": list(preflight.EXPECTED_HEADS),
            "metrics": [
                {
                    "name": name,
                    "feature_bucket_ms": 10_000,
                    "feature_cols": examples,
                }
                for name in preflight.EXPECTED_HEADS
            ],
        },
    )

    paths = {
        "baseline_identity": baseline_identity_path,
        "baseline_pointer": baseline_pointer_path,
        "bundle_meta": bundle_path,
        "feature_generator": feature_generator_path,
        "feature_manifest": feature_manifest_path,
        "live_signal_implementation": signal_path,
        "native_transport_spec": native_transport_path,
        "postfit_native_spec": postfit_path,
        "training_spec": training_spec_path,
        "training_summary": training_summary_path,
    }
    design["artifact_identities"] = {
        name: {"path": str(path), "sha256": preflight.sha256_file(path)}
        for name, path in paths.items()
    }
    design_path = tmp_path / "design.json"
    _write_json(design_path, design)
    return design_path


def _read_design(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_frozen_design_contract_is_valid_without_reading_host_artifacts() -> None:
    design = json.loads(preflight.DEFAULT_DESIGN_PATH.read_text(encoding="utf-8"))
    preflight.validate_design(design)


def test_preflight_inventories_fixed_horizons_and_separate_cadences(
    tmp_path: Path,
) -> None:
    report = preflight.run_preflight(_fixture_design(tmp_path))

    assert report["inventory_complete"] is True
    assert report["retraining_execution_eligible"] is False
    assert report["audit_scope"] == {
        "economic_outcomes_read": False,
        "prediction_outcomes_read": False,
        "artifacts_mutated": False,
        "model_training_executed": False,
        "current_live_baseline_preserved": True,
    }
    assert report["current_v12"]["live_inference_cadence_ms"] == 10_000
    assert report["current_v12"]["maximum_label_future_dependency_s"] == 120
    assert report["current_v12"]["feature_basis_windows_are_estimand_horizons"] is False

    candidates = report["cadence_identities"]["candidates"]
    assert [row["inference_cadence_ms"] for row in candidates] == [1_000, 2_000, 5_000]
    assert len({row["feature_dag_id"] for row in candidates}) == 3
    assert all(row["label_horizon_change_in_scope"] is False for row in candidates)
    assert all(row["outcome_selected"] is False for row in candidates)
    assert len(report["blockers"]["retraining"]) == 24


def test_fill_conditioned_head_discloses_two_horizon_dependency(tmp_path: Path) -> None:
    report = preflight.run_preflight(_fixture_design(tmp_path))
    heads = {row["name"]: row for row in report["current_v12"]["head_contracts"]}

    assert heads["ret_60s"]["reach_window_s"] == 60
    assert heads["ret_60s"]["post_fill_markout_horizon_s"] == 60
    assert heads["ret_60s"]["decision_outcome_span_s"] == [60, 120]
    assert heads["vol_60s"]["maximum_future_dependency_s"] == 60
    assert heads["tox_bid_5s"]["decision_outcome_span_s"] == [5, 10]


def test_all_previously_read_2026_panels_are_diagnostic_only(tmp_path: Path) -> None:
    report = preflight.run_preflight(_fixture_design(tmp_path))
    panels = report["historical_panel_inventory"]

    assert panels["total_previously_read_2026_days"] == 4
    assert panels["all_2026_panels_diagnostic_only"] is True
    assert panels["independent_confirmation_available"] is False
    assert all(row["diagnostic_only"] for row in panels["panels"])


def test_preflight_is_read_only(tmp_path: Path) -> None:
    design_path = _fixture_design(tmp_path)
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}

    preflight.run_preflight(design_path)

    after = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    assert after == before


def test_artifact_hash_drift_fails_closed(tmp_path: Path) -> None:
    design_path = _fixture_design(tmp_path)
    design = _read_design(design_path)
    design["artifact_identities"]["bundle_meta"]["sha256"] = "0" * 64
    _write_json(design_path, design)

    with pytest.raises(preflight.PreflightError, match="hash mismatch"):
        preflight.run_preflight(design_path)


def test_economic_input_path_fails_closed(tmp_path: Path) -> None:
    design_path = _fixture_design(tmp_path)
    design = _read_design(design_path)
    design["artifact_identities"]["bundle_meta"]["path"] = str(tmp_path / "pnl_report.json")
    _write_json(design_path, design)

    with pytest.raises(preflight.PreflightError, match="economic/lifecycle outcome"):
        preflight.run_preflight(design_path)


def test_duplicate_candidate_identity_fails_contract_validation(tmp_path: Path) -> None:
    design_path = _fixture_design(tmp_path)
    design = _read_design(design_path)
    design["candidate_cadence_identities"][1]["identity"] = design["candidate_cadence_identities"][
        0
    ]["identity"]

    with pytest.raises(preflight.PreflightError, match="identity values"):
        preflight.validate_design(design)


def test_target_drift_fails_closed(tmp_path: Path) -> None:
    design_path = _fixture_design(tmp_path)
    design = _read_design(design_path)
    summary_identity = design["artifact_identities"]["training_summary"]
    summary_path = Path(summary_identity["path"])
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["targets"][-1] = "tox_ask_20s"
    _write_json(summary_path, summary)
    summary_identity["sha256"] = preflight.sha256_file(summary_path)
    _write_json(design_path, design)

    with pytest.raises(preflight.PreflightError, match="target identity"):
        preflight.run_preflight(design_path)


def test_operational_baseline_drift_fails_closed(tmp_path: Path) -> None:
    design_path = _fixture_design(tmp_path)
    design = _read_design(design_path)
    pointer_identity = design["artifact_identities"]["baseline_pointer"]
    pointer_path = Path(pointer_identity["path"])
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["dynamic_fill_hazard_action_enabled"] = True
    _write_json(pointer_path, pointer)
    pointer_identity["sha256"] = preflight.sha256_file(pointer_path)
    _write_json(design_path, design)

    with pytest.raises(preflight.PreflightError, match="operational baseline drifted"):
        preflight.run_preflight(design_path)


def test_cli_has_no_training_or_output_artifact_option() -> None:
    destinations = {action.dest for action in preflight._build_parser()._actions}
    assert "output" not in destinations
    assert "train" not in destinations
    assert "model_dir" not in destinations
