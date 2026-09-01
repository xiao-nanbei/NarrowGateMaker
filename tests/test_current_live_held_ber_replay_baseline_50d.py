from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_replay_baseline_50d as baseline50,
)
from research.families.f10_live_replay_attribution.audit import (
    current_live_held_ber_strict_native_latency_baseline_50d as strict50,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("NARROWGATE_PRIVATE_RESEARCH_ROOT"),
    reason="private historical operational denominator is not configured",
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
        "spec_path": baseline50.SPEC_LOCATOR,
        "spec_sha256": baseline50._sha256_file(baseline50._spec_path()),
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


def test_runner_backend_contract_separates_python_and_cpp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = {"commit": "1" * 40, "tree": "2" * 40}
    monkeypatch.setattr(
        strict50,
        "_git_source_identity",
        lambda **_kwargs: dict(source),
    )
    python_backend = strict50._backend_contract("python")
    assert python_backend["engine"] == "python"
    assert python_backend["authoritative"] is True
    assert python_backend["qualification_under_test"] is False
    assert len(python_backend["backend_identity_root"]) == 64

    receipt = tmp_path / "qualification.json"
    receipt.write_text('{"schema_version":"test"}\n', encoding="utf-8")
    receipt_root = hashlib.sha256(receipt.read_bytes()).hexdigest()
    cpp_backend = strict50._backend_contract(
        "cpp",
        cpp_qualification_receipt=receipt,
        cpp_qualification_receipt_sha256=receipt_root,
    )
    assert cpp_backend == {
        "engine": "cpp",
        "backend_identity_root": receipt_root,
        "source_identity": source,
        "authoritative": True,
        "qualification_under_test": False,
        "qualification_receipt_path": str(receipt.resolve()),
    }

    with pytest.raises(strict50.StrictNativeLatencyError, match="cannot consume"):
        strict50._backend_contract(
            "python",
            cpp_qualification_receipt=receipt,
            cpp_qualification_receipt_sha256=receipt_root,
        )
    with pytest.raises(strict50.StrictNativeLatencyError, match="root drifted"):
        strict50._backend_contract(
            "cpp",
            cpp_qualification_receipt=receipt,
            cpp_qualification_receipt_sha256="3" * 64,
        )


def test_cpp_source_identity_rejects_a_dirty_tracked_worktree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(command, **_kwargs):
        if command[1:3] == ["rev-parse", "HEAD"]:
            return SimpleNamespace(stdout="1" * 40 + "\n")
        if command[1:3] == ["rev-parse", "HEAD^{tree}"]:
            return SimpleNamespace(stdout="2" * 40 + "\n")
        if command[1:3] == ["status", "--porcelain"]:
            return SimpleNamespace(stdout=" M models/backtest_tick.py\n")
        raise AssertionError(command)

    monkeypatch.setattr(strict50.subprocess, "run", fake_run)
    with pytest.raises(
        strict50.StrictNativeLatencyError,
        match="tracked-clean worktree",
    ):
        strict50._git_source_identity(require_tracked_clean=True)


def test_cpp_current_policy_setup_uses_central_receipt_validator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = "4" * 64
    backend = {
        "engine": "cpp",
        "backend_identity_root": root,
        "source_identity": {"commit": "1" * 40, "tree": "2" * 40},
        "authoritative": True,
        "qualification_under_test": False,
        "qualification_receipt_path": "/private/qualification.json",
    }
    runtime = object()

    class Adapter:
        def compile_cpp_runtime(self, cpp, **kwargs):
            assert cpp.name == "current-extension"
            assert kwargs == {
                "parity_qualified": True,
                "parity_qualification_sha256": root,
            }
            return runtime

        def cpp_window_arrays(self):
            return {"left_ts_ns": [1]}

    def validate(params, *, require_full_replay):
        assert require_full_replay is True
        assert params["cooldown_duration_policy_cpp_runtime"] is runtime
        assert params["cooldown_duration_policy_cpp_parity_receipt_sha256"] == root
        assert "cooldown_duration_policy_cpp_qualification_under_test" not in params
        assert "cooldown_duration_policy_cpp_event_loop_parity_qualified" not in params
        params["_cooldown_duration_policy_cpp_validated_receipt_sha256"] = root
        return runtime

    monkeypatch.setattr(
        strict50.bt,
        "_load_cpp_tick_replay",
        lambda: SimpleNamespace(name="current-extension"),
    )
    monkeypatch.setattr(
        strict50.bt,
        "_validate_f05_cpp_cooldown_runtime",
        validate,
    )
    params = {"cooldown_duration_policy_evaluator": Adapter()}
    strict50._configure_cpp_current_policy(
        params,
        adapter=params["cooldown_duration_policy_evaluator"],
        backend=backend,
    )
    assert params["_cooldown_duration_policy_cpp_window_arrays"] == {
        "left_ts_ns": [1]
    }


def test_day_cache_rejects_mixed_engines_and_backend_roots(tmp_path: Path) -> None:
    day = "2026-04-19"
    directory = strict50._day_dir(tmp_path, day)
    directory.mkdir(parents=True)
    manifest = {
        "identity": strict50.CURRENT_IDENTITY,
        "day": day,
        "engine": "python",
        "backend_identity_root": "5" * 64,
        "backend_authoritative": True,
        "qualification_under_test": False,
        "current_config_sha256": "6" * 64,
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    (directory / strict50.DAY_SUCCESS).write_text(
        strict50.parent._sha256_file(manifest_path) + "\n",
        encoding="ascii",
    )

    with pytest.raises(strict50.StrictNativeLatencyError, match="engine drifted"):
        strict50._load_day(
            tmp_path,
            day,
            engine="cpp",
            backend_identity_root="7" * 64,
            identity=strict50.CURRENT_IDENTITY,
            config_sha256="6" * 64,
        )
    with pytest.raises(
        strict50.StrictNativeLatencyError,
        match="backend identity drifted",
    ):
        strict50._load_day(
            tmp_path,
            day,
            engine="python",
            backend_identity_root="7" * 64,
            identity=strict50.CURRENT_IDENTITY,
            config_sha256="6" * 64,
        )
