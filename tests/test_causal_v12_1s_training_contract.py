from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
import pytest

from research.families.f03_causal_13_head.audit.causal_v12_1s_training_contract import (
    DEFAULT_DESIGN_PATH,
    TrainingContractError,
    load_and_validate_training_design,
    overlap_adjusted_sample_weights,
    validate_training_design,
)


def _design() -> dict:
    return json.loads(DEFAULT_DESIGN_PATH.read_text(encoding="utf-8"))


def test_frozen_design_inherits_exact_train_only_days() -> None:
    report = load_and_validate_training_design()
    assert len(report["fit_days"]) == 52
    assert report["embargo_days"] == ["2025-11-23"]
    assert len(report["selection_days"]) == 13
    assert len(report["refit_days"]) == 66
    assert report["head_count"] == 13
    assert report["training_execution_eligible"] is False
    assert report["missing_execution_artifacts"] == [
        "model_output_identity",
        "one_second_feature_panel_manifest",
        "training_implementation_sha256",
    ]
    assert report["bound_execution_artifacts"] == [
        "one_second_label_generator_identity",
        "one_second_python_cpp_parity_contract",
        "one_second_source_manifest",
    ]
    assert report["economic_outcomes_read"] is False


def test_design_rejects_cadence_search() -> None:
    design = _design()
    design["candidate_cadence_set_ms"] = [1_000, 2_000, 5_000]
    with pytest.raises(TrainingContractError, match="only the 1s cadence"):
        validate_training_design(design)


def test_design_rejects_inherited_hash_drift() -> None:
    design = copy.deepcopy(_design())
    design["inherited_train_only_membership"]["sha256"] = "0" * 64
    with pytest.raises(TrainingContractError, match="SHA256 mismatch"):
        validate_training_design(design)


def test_design_rejects_bound_execution_artifact_hash_drift() -> None:
    design = copy.deepcopy(_design())
    design["required_execution_artifacts"]["one_second_label_generator_identity"][
        "sha256"
    ] = "0" * 64
    with pytest.raises(TrainingContractError, match="execution artifact.*SHA256 mismatch"):
        validate_training_design(design)


def test_overlap_weights_preserve_valid_day_base_weight_sum() -> None:
    timestamps = np.arange(6, dtype=np.int64) * 1_000_000_000 + 1_000_000_000
    valid = np.array([True, True, True, True, False, False])
    adjusted, uniqueness = overlap_adjusted_sample_weights(
        timestamps,
        valid,
        maximum_future_dependency_s=3,
    )

    np.testing.assert_allclose(
        uniqueness[:4],
        np.array([11 / 18, 7 / 18, 7 / 18, 11 / 18]),
    )
    assert adjusted[valid].sum() == pytest.approx(4.0)
    assert np.all(adjusted[~valid] == 0.0)
    assert adjusted[0] > adjusted[1]
    assert adjusted[3] > adjusted[2]


def test_overlap_weights_require_tail_censoring() -> None:
    timestamps = np.arange(5, dtype=np.int64) * 1_000_000_000 + 1_000_000_000
    with pytest.raises(TrainingContractError, match="must be censored"):
        overlap_adjusted_sample_weights(
            timestamps,
            np.ones(5, dtype=bool),
            maximum_future_dependency_s=3,
        )


def test_overlap_weights_reject_intraday_grid_gap() -> None:
    timestamps = np.array([1, 2, 4], dtype=np.int64) * 1_000_000_000
    with pytest.raises(TrainingContractError, match="complete 1s grid"):
        overlap_adjusted_sample_weights(
            timestamps,
            np.array([True, False, False]),
            maximum_future_dependency_s=1,
        )


def test_overlap_uniqueness_clamps_only_prefix_sum_roundoff() -> None:
    timestamps = np.arange(20_000, dtype=np.int64) * 1_000_000_000 + 1_000_000_000
    valid = np.zeros(len(timestamps), dtype=bool)
    valid[np.arange(0, len(timestamps) - 10, 20)] = True

    _, uniqueness = overlap_adjusted_sample_weights(
        timestamps,
        valid,
        maximum_future_dependency_s=10,
    )

    assert np.all(uniqueness[valid] == 1.0)
    assert np.all(uniqueness[~valid] == 0.0)


def test_design_json_is_valid_json() -> None:
    assert Path(DEFAULT_DESIGN_PATH).is_file()
    assert _design()["inference_cadence_ms"] == 1_000
