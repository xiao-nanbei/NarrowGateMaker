from __future__ import annotations

from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as baseline50,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_strict_native_latency_baseline_50d as strict50,
)


def test_frozen_panel_extends_the_immutable_prefix_by_ten_days() -> None:
    spec = baseline50._spec()
    days = baseline50.ordered_days(spec)

    assert len(days) == 50
    assert len(set(days)) == 50
    assert days[:40] == spec["immutable_prefix"]["ordered_utc_days"]
    assert days[40:] == spec["added_panel"]["ordered_utc_days"]
    assert spec["added_panel"]["quality"] == "Grade A"


def test_successor_requires_external_v12_overlay_and_detaches_v11() -> None:
    spec = baseline50._spec()
    contract = spec["execution_contract"]

    assert contract["old_v11_window_ml_data_allowed"] is False
    assert contract["external_v12_overlay_required"] is True
    assert contract["prefix_40_cpp_reproduction_required"] is True
    assert contract["python_cpp_fill_path_mismatch_required"] == 0


def test_added_panel_remains_historical_diagnostic() -> None:
    spec = baseline50._spec()

    assert spec["added_panel"]["role"] == "historical_native_late_diagnostic_previously_read"
    assert spec["combined_panel"]["independent_confirmation"] is False
    assert spec["permissions"]["action_authority"] is False
    assert spec["permissions"]["live_action_authority"] is False


def test_frozen_cpp_denominator_is_explicitly_non_native_and_zero_latency() -> None:
    spec = baseline50._spec()
    params, _ = baseline50._base_params(spec)

    assert params["exchange_book_queue_mode"] == "disabled"
    assert params["new_order_latency_ms"] == 0.0
    assert params["cancel_order_latency_ms"] == 0.0
    assert params["exec_book_visibility_delay_mean_ms"] == 0.0


def test_strict_native_latency_successor_separates_truth_and_visibility() -> None:
    spec = strict50._spec()

    assert spec["exchange_truth"]["engine"] == "python"
    assert spec["exchange_truth"]["mode"] == "strict"
    assert spec["exchange_truth"]["warmup_hours"] == 24
    assert spec["strategy_visibility"]["mode"] == "sampled"
    assert spec["strategy_visibility"]["exact_historical_receive_tape_available_for_panel"] is False
    assert spec["gateway_latency"]["new_order_samples"] > 0
    assert spec["gateway_latency"]["cancel_order_samples"] > 0
    assert spec["queue_calibration"]["schema_version"] == "narrowgate_queue_calibration.v3"
    assert spec["queue_calibration"]["apply_mode"] == "frozen_default"
    assert spec["queue_calibration"]["fit_days"] == ["2026-07-10", "2026-07-11"]
    assert set(spec["queue_calibration"]["replay_params"]) == {
        "queue_ahead_base_mult",
        "queue_deplete_base_mult",
        "queue_ahead_buy_exposure_mult",
        "queue_ahead_buy_reducing_mult",
        "queue_ahead_sell_exposure_mult",
        "queue_ahead_sell_reducing_mult",
    }
    assert spec["permissions"]["live_action_authority"] is False


def test_prepare_reuses_the_admitted_execution_plan(tmp_path) -> None:
    spec = baseline50._spec()
    plan = {
        "schema_version": f"{baseline50.IDENTITY}.execution_plan.v1",
        "identity": baseline50.IDENTITY,
        "created_at_utc": "2026-08-10T00:00:00+00:00",
        "spec_path": str(baseline50.SPEC.resolve()),
        "spec_sha256": baseline50._sha256_file(baseline50.SPEC),
        "ordered_utc_days": baseline50.ordered_days(spec),
        "prefix_days": list(spec["immutable_prefix"]["ordered_utc_days"]),
        "added_days": list(spec["added_panel"]["ordered_utc_days"]),
        "added_windows": {},
        "added_overlays": [],
        "old_v11_window_ml_data_used": False,
        "output_root": str(baseline50.DEFAULT_OUTPUT),
        "cache_root": str(tmp_path.resolve()),
    }
    plan["identity_sha256"] = baseline50._canonical_sha256(plan)
    baseline50._atomic_json(tmp_path / "execution-plan.json", plan)
    baseline50._atomic_text(
        tmp_path / "_PLAN_SUCCESS",
        baseline50._sha256_file(tmp_path / "execution-plan.json") + "\n",
    )
    before = (tmp_path / "execution-plan.json").read_bytes()

    assert baseline50.prepare(tmp_path) == plan
    assert (tmp_path / "execution-plan.json").read_bytes() == before
