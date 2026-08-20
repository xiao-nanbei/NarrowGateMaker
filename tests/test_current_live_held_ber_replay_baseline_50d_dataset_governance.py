from __future__ import annotations

import json

import pytest

from models.audit import dataset_governance
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as baseline50,
)


def test_dataset_binding_is_canonical_and_precedes_future_reexecution(
    tmp_path,
) -> None:
    spec = baseline50._spec()
    receipt = baseline50._ensure_dataset_binding(tmp_path, spec)
    binding_path = tmp_path / baseline50.DATASET_BINDING_NAME
    universe_path = tmp_path / baseline50.DATASET_UNIVERSE_NAME

    binding, result = dataset_governance.load_dataset_binding(
        binding_path,
        expected_file_sha256=receipt["sha256"],
        expected_experiment_id=baseline50.IDENTITY,
        project_root=baseline50.ROOT,
    )

    assert result["valid"] is True
    assert binding["execution_denominator"]["days"] == baseline50.ordered_days(spec)
    assert binding["binding_scope"] == "future_reexecution_governance_only"
    assert binding["historical_result_pre_execution_binding_claimed"] is False
    assert universe_path.is_file()


def test_dataset_binding_drift_blocks_future_reexecution(tmp_path) -> None:
    spec = baseline50._spec()
    baseline50._ensure_dataset_binding(tmp_path, spec)
    binding_path = tmp_path / baseline50.DATASET_BINDING_NAME
    payload = json.loads(binding_path.read_text(encoding="utf-8"))
    payload["eligible_days"] = payload["eligible_days"][:-1]
    binding_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(baseline50.Baseline50Error, match="binding drifted"):
        baseline50._ensure_dataset_binding(tmp_path, spec)
