import hashlib
from pathlib import Path

from research.families.f06_placement_fill_cif.audit.placement_fill_spec import (
    load_placement_fill_spec,
)
from research.governance.public_machine_projection import (
    projection_for,
    source_identity_sha256,
)

FAMILY_DOCS = Path("research/families/f06_placement_fill_cif/docs")


def test_v3_spec_hash_locks_and_inherits_v2_contract() -> None:
    spec = load_placement_fill_spec(
        Path(
            "research/families/f06_placement_fill_cif/docs/placement_fill_cif_v3_spec_20260727.json"
        )
    )
    assert spec["family_id"] == "placement_fill_cif_v3"
    assert len(spec["panels"]["development"]["days"]) == 40
    assert spec["prediction_gates"]["calibration_level"]["point_intercept_limit"] is None
    assert spec["development_fit"]["rolling_calibration"]["outcome_source_action"] == "current"


def test_parent_hash_binds_source_identity_not_public_projection_bytes() -> None:
    parent = FAMILY_DOCS / "placement_fill_cif_v2_spec_20260727.json"
    projection = projection_for(parent)
    assert projection is not None
    assert source_identity_sha256(parent) == (
        "54c74b8a5185cfe55ff00cd7bbf95896649133d6093de0707ddf6b3b7ce906e9"
    )
    assert source_identity_sha256(parent) == projection.source_private_sha256
    assert hashlib.sha256(parent.read_bytes()).hexdigest() == (projection.public_projection_sha256)
    assert projection.public_projection_sha256 != projection.source_private_sha256


def test_v4_keeps_zero_centered_calibration_as_diagnostic() -> None:
    spec = load_placement_fill_spec(
        Path(
            "research/families/f06_placement_fill_cif/docs/placement_fill_cif_v4_spec_20260727.json"
        )
    )
    calibration = spec["prediction_gates"]["calibration_level"]
    assert spec["family_id"] == "placement_fill_cif_v4"
    assert calibration["point_intercept_limit"] is None
    assert not calibration["day_clustered_observed_minus_expected_ci_must_contain_zero"]
    assert calibration["pooled_absolute_bias_must_not_exceed_inner_oof_drift_envelope"]


def test_request_state_v2_freezes_corrected_context_and_50_day_development() -> None:
    spec = load_placement_fill_spec(
        Path(
            "research/families/f06_placement_fill_cif/docs/placement_fill_request_state_race_v2_spec_20260728.json"
        )
    )

    assert spec["family_id"] == "placement_fill_request_state_race_v2"
    assert len(spec["panels"]["development"]["days"]) == 50
    assert len(spec["panels"]["validation"]["days"]) == 10
    assert len(spec["panels"]["sealed_holdout"]["days"]) == 14
    assert spec["good_day_contract"]["order_level_days"] == 76
    assert spec["source_identity"]["feature_context_manifest_sha256"] == (
        "4b4cef9fb3542badfd552f51f0b973a13d5af62dfbbf7b92d3826dd3002d7e3c"
    )
    assert spec["cache_contract"]["baseline_window_cache_version"] == 12
    assert spec["invalidated_artifacts"][0]["reuse_allowed"] is False
    assert spec["permissions"]["validation_access"] is False
    assert spec["permissions"]["sealed_holdout_access"] is False
