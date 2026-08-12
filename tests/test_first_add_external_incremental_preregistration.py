from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f04_external_market_alpha"
    / "docs"
    / "first_add_external_incremental_value_m0_m1_v1_preregistration_20260730.json"
)


def _spec() -> dict:
    return json.loads(SPEC_PATH.read_text(encoding="utf-8"))


def test_first_add_external_incremental_canonical_hash_is_stable() -> None:
    spec = _spec()
    expected = spec.pop("canonical_spec_sha256")
    payload = json.dumps(
        spec,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    assert hashlib.sha256(payload).hexdigest() == expected


def test_first_add_external_incremental_identity_is_prediction_only() -> None:
    spec = _spec()
    assert spec["identity"] == "first_add_external_incremental_value_m0_m1_v1"
    assert spec["estimand"]["column"] == "decision_to_campaign_terminal_value_usdc"
    assert spec["estimand"]["unit"] == "USDC_per_first_add_decision"
    assert spec["estimand"]["future_direction_or_markout_target_forbidden"] is True
    assert spec["inference_and_gates"]["ranking_score"] is None
    assert not any(spec["permissions"].values())


def test_first_add_external_incremental_requires_distinct_days_and_true_loo() -> None:
    spec = _spec()
    admission = spec["capture_admission"]
    assert admission["minimum_distinct_valid_utc_days"] == 30
    assert admission["minimum_requested_or_observed_duration_s"] == 3500
    assert admission["duplicate_same_utc_day_windows_do_not_increase_day_count"] is True
    assert spec["chronology"] == {
        "ordered_unit": "distinct_valid_utc_capture_day",
        "initial_train_days": 20,
        "embargo_days": 1,
        "chronological_test_days": 5,
        "late_panel_days": 4,
        "minimum_total_days": 30,
        "threshold_or_feature_selection_on_test_or_late_forbidden": True,
        "exact_day_lists_and_all_input_hashes_frozen_before_target_read": True,
    }
    loo = spec["leave_one_venue_out"]
    assert loo["true_rebuild_from_raw_venue_tapes"] is True
    assert loo["subtracting_a_venue_from_full_consensus_forbidden"] is True
    assert set(loo["profiles"]) == {
        "full",
        "leave_bitget_out",
        "leave_bybit_out",
        "leave_okx_out",
    }


def test_first_add_external_incremental_m0_m1_boundary_is_exact() -> None:
    models = _spec()["models"]
    assert models["estimator"] == "standardized_ridge_direct_usdc"
    assert models["ridge_alpha"] == 1.0
    assert models["hyperparameter_search"] is False
    assert models["m0"]["external_venue_features"] is False
    assert models["m0"]["binance_reference_bridge_in_primary_m0"] is False
    assert models["m1"]["external_venues"] == ["bitget", "bybit", "okx"]
    assert models["m1"]["feature_ready_after_decision_forbidden"] is True
