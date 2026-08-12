from __future__ import annotations

import pandas as pd
import pytest

from research.families.f06_placement_fill_cif.audit.paired_lifecycle_contract import (
    assert_common_prediction_clock,
    prediction_clock_contract_from_spec,
    verify_prediction_clock_source_identity,
)


def _clock_contract():
    return prediction_clock_contract_from_spec(
        {
            "prediction_clock_contract": {
                "schema_version": "paired_prediction_clock_contract.v1",
                "selected_source_id": "baseline_policy_schedule_v1",
                "allowed_sources": {
                    "baseline_policy_schedule_v1": {
                        "clock_column": "scheduled_clock_ms",
                        "unit": "ms_since_activation",
                        "causal_cut": "submit_ts_ns",
                        "source_identity_sha256": "0" * 64,
                        "ex_ante": True,
                        "cohort_common": True,
                        "outcome_dependent": False,
                    }
                },
            }
        }
    )


def _clock_frame(clock: tuple[float, float, float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cohort_id": ["c1", "c1", "c1"],
            "action": ["closer_1tick", "current", "farther_1tick"],
            "scheduled_clock_ms": list(clock),
            "realized_exposure_ms": [1_000.0, 3_000.0, 5_000.0],
        }
    )


def test_common_scheduled_clock_passes_despite_different_realized_exposure() -> None:
    diagnostics = assert_common_prediction_clock(
        _clock_frame((5_000.0, 5_000.0, 5_000.0)),
        clock_contract=_clock_contract(),
    )
    assert diagnostics["all_common"] is True
    assert diagnostics["violating_groups"] == 0


def test_action_specific_realized_clock_fails_fast() -> None:
    with pytest.raises(RuntimeError, match="cohort-common ex-ante prediction clock"):
        assert_common_prediction_clock(
            _clock_frame((1_000.0, 3_000.0, 5_000.0)),
            clock_contract=_clock_contract(),
        )


def test_incomplete_action_cohort_fails_fast() -> None:
    incomplete = _clock_frame((5_000.0, 5_000.0, 5_000.0)).iloc[:2].copy()
    with pytest.raises(RuntimeError, match="every cohort to contain every Spec action"):
        assert_common_prediction_clock(
            incomplete,
            clock_contract=_clock_contract(),
        )


def test_unapproved_or_outcome_dependent_clock_source_is_rejected() -> None:
    spec = {
        "prediction_clock_contract": {
            "schema_version": "paired_prediction_clock_contract.v1",
            "selected_source_id": "realized_fill_clock",
            "allowed_sources": {
                "baseline_policy_schedule_v1": {
                    "clock_column": "scheduled_clock_ms",
                }
            },
        }
    }
    with pytest.raises(ValueError, match="not Spec-allowed"):
        prediction_clock_contract_from_spec(spec)

    spec["prediction_clock_contract"]["allowed_sources"]["realized_fill_clock"] = {
        "clock_column": "realized_exposure_ms",
        "unit": "ms_since_activation",
        "causal_cut": "fill_ts_ns",
        "source_identity_sha256": "1" * 64,
        "ex_ante": False,
        "cohort_common": False,
        "outcome_dependent": True,
    }
    with pytest.raises(ValueError, match="ex-ante and cohort-common"):
        prediction_clock_contract_from_spec(spec)


def test_clock_source_identity_verifies_actual_artifact_bytes(tmp_path) -> None:
    artifact = tmp_path / "clock-producer.json"
    artifact.write_text('{"clock_ms":5000}\n', encoding="utf-8")
    import hashlib

    expected = hashlib.sha256(artifact.read_bytes()).hexdigest()
    spec = {
        "prediction_clock_contract": {
            "schema_version": "paired_prediction_clock_contract.v1",
            "selected_source_id": "baseline_policy_schedule_v1",
            "allowed_sources": {
                "baseline_policy_schedule_v1": {
                    "clock_column": "scheduled_clock_ms",
                    "unit": "ms_since_activation",
                    "causal_cut": "submit_ts_ns",
                    "source_identity_sha256": expected,
                    "ex_ante": True,
                    "cohort_common": True,
                    "outcome_dependent": False,
                }
            },
        }
    }
    contract = prediction_clock_contract_from_spec(spec)
    identity = verify_prediction_clock_source_identity(contract, artifact)
    assert identity["sha256"] == expected

    artifact.write_text('{"clock_ms":6000}\n', encoding="utf-8")
    with pytest.raises(RuntimeError, match="producer identity mismatch"):
        verify_prediction_clock_source_identity(contract, artifact)
