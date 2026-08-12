from __future__ import annotations

import copy
import json
import subprocess
import threading
import time
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from models.replay.order_lifecycle_v2_replay_adapter_strict_native import (
    OrderLifecycleV2ReplayAdapter,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_replay_emitter as emitter,
)

ROOT = Path(__file__).resolve().parents[1]
MODULE_WORKER_AMENDMENT = ROOT / (
    "research/families/f07_active_order_continuation/docs/"
    "order_lifecycle_v2_40day_replay_emitter_v1_1_"
    "module_worker_amendment_20260805.json"
)
VENV_ENTRYPOINT_AMENDMENT = ROOT / (
    "research/families/f07_active_order_continuation/docs/"
    "order_lifecycle_v2_40day_replay_emitter_v1_2_"
    "venv_entrypoint_amendment_20260805.json"
)
QUEUE_AUTHORITY_AMENDMENT = ROOT / (
    "research/families/f07_active_order_continuation/docs/"
    "order_lifecycle_v2_40day_replay_emitter_v1_3_"
    "native_queue_authority_amendment_20260805.json"
)
MODEL_OVERLAY_AMENDMENT = ROOT / (
    "research/families/f07_active_order_continuation/docs/"
    "order_lifecycle_v2_40day_replay_emitter_v1_4_"
    "model_overlay_dag_binding_amendment_20260805.json"
)
STRICT_NATIVE_AMENDMENT = ROOT / (
    "research/families/f07_active_order_continuation/docs/"
    "order_lifecycle_v2_40day_replay_emitter_v1_5_"
    "strict_native_overlay_runtime_amendment_20260805.json"
)
V14_DIAGNOSTIC_MARKER = ROOT / (
    "research/families/f07_active_order_continuation/docs/"
    "order_lifecycle_v2_40day_replay_emitter_v1_4_"
    "first_day_diagnostic_not_admitted_20260805.json"
)


def _write_bytes(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    return path


def _fixture_model_overlay(day: str) -> dict[str, object]:
    identity = {
        "schema_version": "narrowgate.model_overlay_day.v1.1",
        "schema_sha256": "9" * 64,
        "dag_identity_sha256": "a" * 64,
        "dag_node": "model_overlay_day",
        "symbol": "BTCUSDC",
        "day": day,
        "market_context_identity_sha256": "b" * 64,
        "feature_source_identity_sha256": "c" * 64,
        "model_bundle_identity_sha256": "d" * 64,
        "toxicity_horizon_s": 10,
        "cross_market_enabled": True,
        "run_ml_inference": True,
    }
    parity = {
        "window_sha256": "8" * 64,
        "market_context_identity_sha256": identity["market_context_identity_sha256"],
        "exact_trades_and_rolling_arrays": True,
        "exact_bbo_l2_leaf_and_output_parity": True,
        "book_parity_identity_sha256": "1" * 64,
    }
    book_parity = {
        "schema_version": "f07_bbo_l2_leaf_output_parity.v1",
        "day": day,
        "independent_source_reload_parity": True,
        "bound_leaf_identity_sha256": "2" * 64,
        "window_book_payload_fingerprint_sha256": "3" * 64,
        "independent_book_payload_fingerprint_sha256": "3" * 64,
    }
    book_parity["identity_sha256"] = emitter.canonical_sha256(book_parity)
    overlay_contract = {
        "row_count": 1,
        "gap_policy": dict(emitter._OVERLAY_GAP_POLICY),
        "main_arrays_finite": True,
        "required_feature_keys_sha256": emitter.canonical_sha256(["feature"]),
    }
    overlay_contract["identity_sha256"] = emitter.canonical_sha256(overlay_contract)
    generation_receipt = {
        "schema_version": "f07_model_overlay_generation_receipt.v1",
        "kind": (
            "independent_reinference_exact_array_parity"
            if day == "2026-04-17"
            else "bound_component_schema_and_payload_validation"
        ),
        "day": day,
        "passed": True,
    }
    generation_receipt["identity_sha256"] = emitter.canonical_sha256(
        generation_receipt
    )
    return {
        "cache_root": "/fixture/model-overlay",
        "identity": identity,
        "identity_sha256": emitter.canonical_sha256(identity),
        "manifest": {
            "path": f"overlay/{day}/manifest.json",
            "size_bytes": 1,
            "sha256": "e" * 64,
        },
        "data": {
            "path": f"overlay/{day}/model_overlay.npz",
            "size_bytes": 1,
            "sha256": "f" * 64,
        },
        "market_context_output_parity": {
            **parity,
            "identity_sha256": emitter.canonical_sha256(parity),
        },
        "book_leaf_output_parity": book_parity,
        "overlay_contract": overlay_contract,
        "generation_receipt": generation_receipt,
    }


def _fixture_plan(tmp_path: Path) -> tuple[dict[str, object], Path]:
    frozen, _ = emitter.load_frozen_v1_panel()
    days = list(frozen["panel"]["ordered_utc_days"])
    global_identity = {
        "config_sha256": "1" * 64,
        "model_sha256": "2" * 64,
        "p3_sha256": "3" * 64,
        "feature_dag_sha256": "4" * 64,
        "code_sha256": "5" * 64,
        "cpp_abi_sha256": "6" * 64,
        "latency_sha256": "7" * 64,
        "model_overlay_bundle_identity_sha256": "d" * 64,
        "model_overlay_contract": {
            "required_heads": ["fixture_head"],
            "required_heads_sha256": emitter.canonical_sha256(["fixture_head"]),
            "required_feature_keys": ["feature"],
            "required_feature_keys_sha256": emitter.canonical_sha256(["feature"]),
            "overlay_gap_policy": dict(emitter._OVERLAY_GAP_POLICY),
            "independent_reinference_required_day_count": 1,
        },
    }
    global_identity_sha = emitter.canonical_sha256(global_identity)
    rows: list[dict[str, object]] = []
    for interval in frozen["panel"]["day_intervals"]:
        day = str(interval["day"])
        rows.append(
            emitter._plan_day_identity(
                global_identity_sha256=global_identity_sha,
                day=day,
                interval=interval,
                window_cache={
                    "path": f"inputs/{day}.pkl",
                    "size_bytes": 1,
                    "sha256": "8" * 64,
                },
                model_overlay=_fixture_model_overlay(day),
                native_book_artifacts=[],
            )
        )
    plan: dict[str, object] = {
        "schema_version": emitter.PLAN_SCHEMA_VERSION,
        "identity": emitter.IDENTITY,
        "status": "prepared_not_executed",
        "prepared_at_utc": "2026-08-05T00:00:00+00:00",
        "cache_root": str(tmp_path.resolve()),
        "window_cache_root": str((tmp_path / "inputs").resolve()),
        "global_execution_identity": global_identity,
        "global_execution_identity_sha256": global_identity_sha,
        "source_contract_path": str((tmp_path / "source.json").resolve()),
        "native_orderbook_root": str((tmp_path / "native").resolve()),
        "ordered_utc_days": days,
        "days": rows,
        "execution_contract": {
            "engine": "python_authoritative_tick_replay",
            "daily_process_isolation": True,
            "initial_state": "daily_fresh_start",
            "atomic_day_publish": "staging_directory_fsync_os_replace",
            "resume_key": "day_execution_identity_sha256",
            "journal_adapter": (
                "order_lifecycle_journal_v2.python_replay_adapter.strict_native.v1"
            ),
            "journal_storage_format": "parquet",
            "strict_native_only": True,
            "native_unsupported_policy": (
                "explicit_journal_censor_keep_baseline_trajectory"
            ),
            "cif_eligibility": "exact_native_spells_only",
        },
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "formal_40day_replay_executed": False,
            "formal_40day_lockstep_executed": False,
        },
        "permissions": {
            "cif_training": False,
            "economic_evaluation": False,
            "q90_action": False,
            "live_transport": False,
            "live_deployment": False,
        },
    }
    plan["canonical_plan_sha256"] = emitter.canonical_sha256(plan)
    path = _write_json(tmp_path / "execution_plan.json", plan)
    return plan, path


def _emit_mock_day(
    *,
    command: list[str],
    plan: dict[str, object],
    economic_field: tuple[str, object] | None = None,
) -> subprocess.CompletedProcess[str]:
    day = command[command.index("--day") + 1]
    staging = Path(command[command.index("--staging-root") + 1])
    staging.mkdir(parents=True)
    day_row = next(row for row in plan["days"] if row["day"] == day)
    session_root = staging / "journal" / f"session-f07-{day}"
    part_root = session_root / "parts"
    data_path = _write_bytes(part_root / "part-000001.parquet", b"mock-parquet")
    part_manifest = {
        "data_file": data_path.name,
        "data_sha256": emitter.file_sha256(data_path),
        "row_count": 1,
    }
    part_manifest_path = _write_json(part_root / "part-000001.manifest.json", part_manifest)
    writer_runtime_identity = {
        **day_row["runtime_identity"],
        "replay_adapter_id": (
            "order_lifecycle_journal_v2.python_replay_adapter.strict_native.v1"
        ),
        "economic_outcomes_read": False,
        "q90_action_authorized": False,
    }
    writer_runtime_sha = emitter.canonical_sha256(writer_runtime_identity)
    runtime_path = _write_json(
        session_root / "runtime_identity.json",
        {
            "runtime_identity": writer_runtime_identity,
            "runtime_identity_sha256": writer_runtime_sha,
        },
    )
    health_path = _write_json(
        session_root / "health.json",
        {
            "rows_committed": 1,
            "callbacks_committed": 1,
            "rows_dropped": 0,
            "error_count": 0,
            "closed": True,
            "formal_collection_valid": True,
        },
    )
    journal: dict[str, object] = {
        "session_root": str(session_root.relative_to(staging)),
        "writer_runtime_identity_sha256": writer_runtime_sha,
        "runtime_identity_artifact": emitter.artifact_identity(runtime_path, relative_to=staging),
        "health_artifact": emitter.artifact_identity(health_path, relative_to=staging),
        "part_manifest_artifacts": [
            emitter.artifact_identity(part_manifest_path, relative_to=staging)
        ],
        "part_data_artifacts": [emitter.artifact_identity(data_path, relative_to=staging)],
        "row_count": 1,
        "writer": {
            "rows_committed": 1,
            "callbacks_committed": 1,
            "rows_dropped": 0,
            "error_count": 0,
            "closed": True,
            "formal_collection_valid": True,
        },
        "dual_clock": {
            "required_exchange_event_count": 1,
            "missing_exchange_clock_count": 0,
            "exchange_after_visibility_count": 0,
            "invalid_exchange_exposure_count": 0,
            "passed": True,
        },
        "counters": {
            "lifecycle_count": 1,
            "event_count": 1,
            "terminal_observation_count": 1,
            "event_counts": {"exchange_terminal": 1},
            "terminal_reason_counts": {"cancel_ack": 1},
            "cancel_reject_count": 0,
            "cancel_reject_to_active_count": 0,
            "cancel_reject_to_partially_filled_count": 0,
            "sub_lot_partial_remaining_count": 0,
            "full_fill_exact_zero_count": 0,
            "terminal_positive_remainder_count": 0,
            "exact_native_lifecycle_count": 1,
            "native_queue_censored_lifecycle_count": 0,
        },
        "cif_eligibility": {
            "rule": "all_fill_risk_rows_exact_native",
            "eligible_lifecycle_count": 1,
            "censored_lifecycle_count": 0,
            "censor_reason_counts": {},
        },
    }
    if economic_field is not None:
        journal[economic_field[0]] = economic_field[1]
    manifest: dict[str, object] = {
        "schema_version": emitter.DAY_MANIFEST_SCHEMA_VERSION,
        "identity": emitter.IDENTITY,
        "day": day,
        "plan_sha256": plan["canonical_plan_sha256"],
        "day_execution_identity_sha256": day_row["day_execution_identity_sha256"],
        "status": "complete",
        "atomic_publish_method": "parent_staging_directory_fsync_os_replace",
        "replay": {
            "engine": "python_authoritative_tick_replay",
            "initial_state": "daily_fresh_start",
            "session_scope": "fresh_start_per_target_day",
            "q90_action_enabled": False,
            "strict_native_only": True,
        },
        "bindings": {
            "global_execution_identity_sha256": plan["global_execution_identity_sha256"],
            "daily_source_identity_sha256": day_row["daily_source_identity_sha256"],
            "config_sha256": "1" * 64,
            "model_bundle_sha256": "2" * 64,
            "model_overlay_identity_sha256": day_row["model_overlay"]["identity_sha256"],
            "p3_sha256": "3" * 64,
            "feature_dag_semantic_sha256": "4" * 64,
            "runtime_code_identity_sha256": "5" * 64,
            "cpp_abi_version": "fixture-cpp-abi-v1",
            "cpp_module_sha256": "6" * 64,
            "latency_profile_sha256": "7" * 64,
        },
        "journal_v2": journal,
        "scope": {
            "mechanics_only": True,
            "economic_outcomes_read": False,
            "formal_40day_replay_executed": False,
            "formal_40day_lockstep_executed": False,
        },
        "permissions": {
            "cif_training": False,
            "economic_evaluation": False,
            "q90_action": False,
            "live_transport": False,
            "live_deployment": False,
        },
    }
    manifest["canonical_manifest_sha256"] = emitter.canonical_sha256(manifest)
    _write_json(staging / "day_manifest.json", manifest)
    return subprocess.CompletedProcess(command, 0, stdout="", stderr="")


def test_frozen_v1_denominator_is_ordered_40_days() -> None:
    frozen, path = emitter.load_frozen_v1_panel()

    days = frozen["panel"]["ordered_utc_days"]
    assert path == emitter.DEFAULT_FROZEN_V1.resolve()
    assert len(days) == 40
    assert days == sorted(set(days))
    assert frozen["baseline_runtime_identity"]["initial_state_mode"] == ("daily_fresh_start")


def test_worker_command_is_execution_only_and_uses_explicit_staging(
    tmp_path: Path,
) -> None:
    _, plan_path = _fixture_plan(tmp_path)
    command = emitter.build_worker_command(
        python_executable=Path(".venv/bin/python"),
        plan_path=plan_path,
        day="2026-04-17",
        staging_root=tmp_path / ".staging" / "fixture",
    )

    assert command[1:4] == ["-m", emitter.WORKER_MODULE, "_run-day"]
    assert command[0] == str((emitter.ROOT / ".venv/bin/python").absolute())
    assert command[command.index("--day") + 1] == "2026-04-17"
    assert command[command.index("--staging-root") + 1].endswith("/.staging/fixture")
    assert not any(
        fragment in " ".join(command).lower() for fragment in emitter._FORBIDDEN_ECONOMIC_FRAGMENTS
    )


def test_module_worker_amendment_is_canonical_and_execution_only() -> None:
    payload = json.loads(MODULE_WORKER_AMENDMENT.read_text(encoding="utf-8"))

    assert emitter.canonical_document_sha256(
        payload, "canonical_amendment_sha256"
    ) == payload["canonical_amendment_sha256"]
    assert payload["failure_observed"]["economic_outcomes_read"] is False
    assert payload["failure_observed"]["journal_rows_admitted"] == 0
    assert payload["repair"]["denominator_action_or_output_schema_changed"] is False
    assert payload["permissions"]["economic_evaluation"] is False


def test_venv_entrypoint_amendment_is_canonical_and_execution_only() -> None:
    payload = json.loads(VENV_ENTRYPOINT_AMENDMENT.read_text(encoding="utf-8"))

    assert emitter.canonical_document_sha256(
        payload, "canonical_amendment_sha256"
    ) == payload["canonical_amendment_sha256"]
    assert payload["failure_observed"]["economic_outcomes_read"] is False
    assert payload["failure_observed"]["journal_rows_admitted"] == 0
    assert payload["repair"]["venv_site_packages_preserved"] is True
    assert payload["permissions"]["economic_evaluation"] is False


def test_native_queue_authority_amendment_is_canonical_and_execution_only() -> None:
    payload = json.loads(QUEUE_AUTHORITY_AMENDMENT.read_text(encoding="utf-8"))

    assert emitter.canonical_document_sha256(
        payload, "canonical_amendment_sha256"
    ) == payload["canonical_amendment_sha256"]
    assert payload["failure_observed"]["economic_outcomes_read"] is False
    assert payload["failure_observed"]["journal_rows_admitted"] == 0
    assert payload["repair"]["native_tape_is_sole_queue_authority"] is True
    assert payload["permissions"]["economic_evaluation"] is False


def test_model_overlay_amendment_is_canonical_and_execution_only() -> None:
    payload = json.loads(MODEL_OVERLAY_AMENDMENT.read_text(encoding="utf-8"))

    assert emitter.canonical_document_sha256(
        payload, "canonical_amendment_sha256"
    ) == payload["canonical_amendment_sha256"]
    assert payload["failure_observed"]["economic_outcomes_read"] is False
    assert payload["failure_observed"]["journal_rows_admitted"] == 0
    assert payload["repair"]["model_overlay_is_separate_dag_node"] is True
    assert payload["permissions"]["economic_evaluation"] is False


def test_v15_amendment_and_v14_diagnostic_marker_are_canonical() -> None:
    amendment = json.loads(STRICT_NATIVE_AMENDMENT.read_text(encoding="utf-8"))
    marker = json.loads(V14_DIAGNOSTIC_MARKER.read_text(encoding="utf-8"))

    assert emitter.canonical_document_sha256(
        amendment, "canonical_amendment_sha256"
    ) == amendment["canonical_amendment_sha256"]
    assert amendment["execution_contract"]["strict_native_only"] is True
    assert amendment["execution_contract"]["cif_eligibility"] == (
        "exact_native_spells_only"
    )
    assert amendment["scope"]["economic_outcomes_read"] is False
    assert emitter.canonical_document_sha256(
        marker, "canonical_marker_sha256"
    ) == marker["canonical_marker_sha256"]
    assert marker["status"] == "diagnostic_not_admitted"
    assert marker["economic_outcomes_read"] is False
    assert marker["admitted_day_count"] == 0


def test_bound_feature_locator_matches_overlay_when_warmup_file_is_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from models import data_windows
    from models.replay_cache_components import references_sha256

    feature_root = tmp_path / "features_bound"
    feature_root.mkdir()
    (feature_root / "features_2026-04-17.parquet").write_bytes(b"target")
    (feature_root / "causal_feature_manifest.json").write_text(
        "{}\n", encoding="utf-8"
    )
    signatures = data_windows._glob_signatures(
        feature_root,
        ("features_2026-04-16.parquet", "features_2026-04-17.parquet"),
    )
    signatures.append(
        data_windows._file_signature(feature_root / "causal_feature_manifest.json")
    )
    expected = references_sha256(data_windows._signature_references(signatures))
    monkeypatch.setattr("data_paths.data_root", lambda: tmp_path)

    observed_dir, observed_references = emitter._find_bound_feature_dir(
        day="2026-04-17",
        expected_identity_sha256=expected,
    )

    assert observed_dir == feature_root.resolve()
    assert references_sha256(observed_references) == expected
    assert all("2026-04-16" not in str(reference) for reference in observed_references)


def test_market_context_window_need_not_claim_exact_queue_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = _write_bytes(tmp_path / "window.pkl", b"fixture")

    class LoadedWindow:
        @staticmethod
        def to_dict() -> dict[str, object]:
            return {
                "formal_lifecycle_replay_eligible": True,
                "exact_queue_policy_eligible": False,
                "ml_data": {"prediction": [0.0]},
            }

    monkeypatch.setattr("models.data_windows._load_cached_window", lambda _: LoadedWindow())
    day_row = {"day": "2026-04-17", "window_cache": emitter.artifact_identity(cache)}

    window, loaded_path = emitter._load_replay_window(day_row)

    assert loaded_path == cache.resolve()
    assert window["exact_queue_policy_eligible"] is False


def test_bound_model_overlay_round_trips_as_a_separate_dag_node(tmp_path: Path) -> None:
    from models.replay_cache_components import write_model_overlay

    day = "2026-04-17"
    identity = _fixture_model_overlay(day)["identity"]
    ml_data = (np.array([1_776_384_010_000], dtype=np.int64),) + tuple(
        np.array([float(index)]) for index in range(1, 21)
    ) + (
        {"feature": np.array([1.0])},
    )
    artifact = write_model_overlay(
        cache_root=tmp_path,
        identity=identity,
        ml_data=ml_data,
    )
    manifest_path = artifact.directory / "manifest.json"
    data_path = artifact.directory / "model_overlay.npz"
    overlay_contract = emitter._validate_overlay_payload(
        ml_data,
        day=day,
        required_feature_keys=["feature"],
    )
    book_parity = {
        "schema_version": "f07_bbo_l2_leaf_output_parity.v1",
        "day": day,
        "independent_source_reload_parity": True,
        "bound_leaf_identity_sha256": "1" * 64,
        "window_book_payload_fingerprint_sha256": "2" * 64,
        "independent_book_payload_fingerprint_sha256": "2" * 64,
    }
    book_parity["identity_sha256"] = emitter.canonical_sha256(book_parity)
    generation_receipt = {
        "schema_version": "f07_model_overlay_generation_receipt.v1",
        "kind": "independent_reinference_exact_array_parity",
        "day": day,
        "passed": True,
    }
    generation_receipt["identity_sha256"] = emitter.canonical_sha256(
        generation_receipt
    )
    binding = {
        "cache_root": str(tmp_path.resolve()),
        "identity": identity,
        "identity_sha256": emitter.canonical_sha256(identity),
        "manifest": emitter.artifact_identity(manifest_path),
        "data": emitter.artifact_identity(data_path),
        "market_context_output_parity": {
            "window_sha256": "8" * 64,
            "market_context_identity_sha256": identity[
                "market_context_identity_sha256"
            ],
            "exact_trades_and_rolling_arrays": True,
            "exact_bbo_l2_leaf_and_output_parity": True,
            "book_parity_identity_sha256": book_parity["identity_sha256"],
        },
        "book_leaf_output_parity": book_parity,
        "overlay_contract": overlay_contract,
        "generation_receipt": generation_receipt,
    }
    binding["market_context_output_parity"]["identity_sha256"] = (
        emitter.canonical_sha256(binding["market_context_output_parity"])
    )

    loaded = emitter._load_bound_model_overlay(
        {
            "day": day,
            "window_cache": {"sha256": "8" * 64},
            "model_overlay": binding,
        },
        required_feature_keys=["feature"],
    )

    assert len(loaded) == 22
    assert np.array_equal(loaded[0], np.array([1_776_384_010_000]))
    assert np.array_equal(loaded[-1]["feature"], np.array([1.0]))


def _overlay_fixture(timestamps: list[int]) -> tuple[object, ...]:
    rows = len(timestamps)
    return (np.asarray(timestamps, dtype=np.int64),) + tuple(
        np.full(rows, float(index), dtype=np.float64) for index in range(1, 21)
    ) + ({"feature": np.ones(rows, dtype=np.float64)},)


def test_overlay_contract_freezes_canonical_rows_finite_schema_and_20s_gap() -> None:
    payload = _overlay_fixture(
        [1_776_384_010_000, 1_776_384_020_000, 1_776_384_040_000]
    )
    contract = emitter._validate_overlay_payload(
        payload,
        day="2026-04-17",
        required_feature_keys=["feature"],
    )

    assert contract["row_count"] == 3
    assert contract["twenty_second_gap_count"] == 1
    assert contract["maximum_observed_gap_ms"] == 20_000
    assert contract["gap_policy"] == emitter._OVERLAY_GAP_POLICY

    too_large_gap = _overlay_fixture([1_776_384_010_000, 1_776_384_040_000])
    with pytest.raises(emitter.ReplayEmitterError, match="exceeds frozen policy"):
        emitter._validate_overlay_payload(
            too_large_gap,
            day="2026-04-17",
            required_feature_keys=["feature"],
        )

    nonfinite = list(payload)
    nonfinite[1] = np.array([1.0, np.inf, 1.0])
    with pytest.raises(emitter.ReplayEmitterError, match="non-finite"):
        emitter._validate_overlay_payload(
            tuple(nonfinite),
            day="2026-04-17",
            required_feature_keys=["feature"],
        )

    with pytest.raises(emitter.ReplayEmitterError, match="lacks required keys"):
        emitter._validate_overlay_payload(
            payload,
            day="2026-04-17",
            required_feature_keys=["missing_feature"],
        )


def test_book_fingerprint_is_leaf_sensitive() -> None:
    class Book:
        source = "fixture"
        ts_ms = np.array([1, 2], dtype=np.int64)
        best_bid = np.array([10.0, 10.1])
        best_ask = np.array([10.2, 10.3])
        bid_qty = np.array([1.0, 2.0])
        ask_qty = np.array([3.0, 4.0])

    first = emitter._book_payload_fingerprint(Book(), kind="bbo")
    changed = Book()
    changed.best_bid = np.array([10.0, 10.2])
    second = emitter._book_payload_fingerprint(changed, kind="bbo")

    assert first["identity_sha256"] != second["identity_sha256"]


def _strict_native_fixture(tmp_path: Path) -> tuple[dict[str, object], object]:
    artifacts: list[dict[str, object]] = []
    paths: list[Path] = []
    for role, date in (
        ("native_book_warmup", "2026-04-16"),
        ("native_book_target", "2026-04-17"),
    ):
        for hour in range(24):
            path = _write_bytes(
                tmp_path / date / f"{hour:02d}.parquet.zst",
                f"{date}-{hour}".encode(),
            )
            artifacts.append({"role": role, **emitter.artifact_identity(path)})
            paths.append(path)

    class Tape:
        source_paths = paths

    return {"day": "2026-04-17", "native_book_artifacts": artifacts}, Tape()


def test_strict_native_tape_is_the_sole_queue_authority(tmp_path: Path) -> None:
    day_row, tape = _strict_native_fixture(tmp_path)

    emitter._assert_strict_native_queue_authority(
        day_row=day_row,
        tape=tape,
        params={
            "exchange_book_queue_mode": "strict",
            "queue_l2_cancel_ahead_enabled": False,
        },
    )


def test_native_queue_authority_rejects_non_strict_or_drifted_tape(tmp_path: Path) -> None:
    day_row, tape = _strict_native_fixture(tmp_path)

    with pytest.raises(emitter.ReplayEmitterError, match="requires strict mode"):
        emitter._assert_strict_native_queue_authority(
            day_row=day_row,
            tape=tape,
            params={
                "exchange_book_queue_mode": "diagnostic",
                "queue_l2_cancel_ahead_enabled": False,
            },
        )

    tape.source_paths = list(reversed(tape.source_paths))
    with pytest.raises(emitter.ReplayEmitterError, match="path identity differs"):
        emitter._assert_strict_native_queue_authority(
            day_row=day_row,
            tape=tape,
            params={
                "exchange_book_queue_mode": "strict",
                "queue_l2_cancel_ahead_enabled": False,
            },
        )


def test_native_queue_authority_rejects_missing_bound_file(tmp_path: Path) -> None:
    day_row, tape = _strict_native_fixture(tmp_path)
    missing = Path(day_row["native_book_artifacts"][-1]["path"])
    missing.unlink()

    with pytest.raises(emitter.ReplayEmitterError, match="artifact is missing"):
        emitter._assert_strict_native_queue_authority(
            day_row=day_row,
            tape=tape,
            params={
                "exchange_book_queue_mode": "strict",
                "queue_l2_cancel_ahead_enabled": False,
            },
        )


def test_day_identity_binds_source_and_global_runtime() -> None:
    interval = emitter._utc_interval("2026-04-17")
    first = emitter._plan_day_identity(
        global_identity_sha256="1" * 64,
        day="2026-04-17",
        interval=interval,
        window_cache={"path": "a", "size_bytes": 1, "sha256": "2" * 64},
        model_overlay=_fixture_model_overlay("2026-04-17"),
        native_book_artifacts=[
            {"role": "native_book_target", "path": "b", "size_bytes": 2, "sha256": "3" * 64}
        ],
    )
    second = emitter._plan_day_identity(
        global_identity_sha256="1" * 64,
        day="2026-04-17",
        interval=interval,
        window_cache={"path": "a", "size_bytes": 1, "sha256": "4" * 64},
        model_overlay=_fixture_model_overlay("2026-04-17"),
        native_book_artifacts=[
            {"role": "native_book_target", "path": "b", "size_bytes": 2, "sha256": "3" * 64}
        ],
    )
    third = emitter._plan_day_identity(
        global_identity_sha256="5" * 64,
        day="2026-04-17",
        interval=interval,
        window_cache={"path": "a", "size_bytes": 1, "sha256": "2" * 64},
        model_overlay=_fixture_model_overlay("2026-04-17"),
        native_book_artifacts=[
            {"role": "native_book_target", "path": "b", "size_bytes": 2, "sha256": "3" * 64}
        ],
    )

    assert first["daily_source_identity_sha256"] != second["daily_source_identity_sha256"]
    assert first["day_execution_identity_sha256"] != second["day_execution_identity_sha256"]
    assert first["day_execution_identity_sha256"] != third["day_execution_identity_sha256"]


def test_native_book_roles_use_parent_date_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = []
    for day in ("2026-04-16", "2026-04-17"):
        for hour in range(24):
            paths.append(
                _write_bytes(
                    tmp_path
                    / "binance_futures"
                    / day
                    / f"{hour:02d}"
                    / "BTCUSDC_orderbook.parquet.zst",
                    f"{day}-{hour}".encode(),
                )
            )

    class FakeTape:
        def __init__(self, **_: object) -> None:
            self.source_paths = paths

    monkeypatch.setattr("models.exchange_book_replay.CryptoHFTExchangeBookTape", FakeTape)
    artifacts = emitter._native_book_artifacts(
        raw_root=tmp_path,
        day="2026-04-17",
        tick_size=0.1,
        warmup_hours=24,
    )

    roles = [item["role"] for item in artifacts]
    assert roles.count("native_book_warmup") == 24
    assert roles.count("native_book_target") == 24


def test_real_adapter_journal_populates_required_mechanics_counters(
    tmp_path: Path,
) -> None:
    day = "2026-04-17"
    runtime_identity = {
        "identity": emitter.IDENTITY,
        "day": day,
        "economic_outcomes_read": False,
    }
    day_row = {
        "day": day,
        "runtime_identity": runtime_identity,
        "runtime_identity_sha256": emitter.canonical_sha256(runtime_identity),
    }
    adapter = OrderLifecycleV2ReplayAdapter(
        root=tmp_path / "journal",
        session_id=f"f07-{day}",
        runtime_identity=runtime_identity,
        symbol="BTCUSDC",
        storage_format="parquet",
    )
    partial = {
        "trace_id": 1,
        "side": "BUY",
        "submit_ts": 1_000,
        "quote_ts": 1_000,
        "quantity": 0.001,
        "remaining": 0.001,
    }
    adapter.submit(partial, 1_000)
    adapter.activate(partial, visibility_ts_ms=1_110, exchange_ts_ms=1_100)
    adapter.fill(
        partial,
        remaining_after=0.0004,
        visibility_ts_ms=1_210,
        exchange_ts_ms=1_200,
        full_fill=False,
    )
    adapter.request_cancel(partial, 1_220)
    adapter.cancel_reject(
        partial,
        visibility_ts_ms=1_240,
        exchange_ts_ms=1_230,
    )
    adapter.fill(
        partial,
        remaining_after=0.0,
        visibility_ts_ms=1_260,
        exchange_ts_ms=1_250,
        full_fill=True,
    )
    cancelled = {
        "trace_id": 2,
        "side": "SELL",
        "submit_ts": 2_000,
        "quote_ts": 2_000,
        "quantity": 0.001,
        "remaining": 0.001,
    }
    adapter.submit(cancelled, 2_000)
    adapter.activate(cancelled, visibility_ts_ms=2_110, exchange_ts_ms=2_100)
    adapter.request_cancel(cancelled, 2_200)
    adapter.cancel_ack(
        cancelled,
        visibility_ts_ms=2_230,
        exchange_ts_ms=2_220,
    )
    adapter.close()

    journal = emitter._journal_mechanics_summary(
        staging_root=tmp_path,
        day_row=day_row,
        lot_size_btc=0.001,
    )

    assert journal["dual_clock"]["passed"] is True
    assert journal["writer"]["rows_dropped"] == 0
    assert journal["writer"]["error_count"] == 0
    assert journal["counters"]["lifecycle_count"] == 2
    assert journal["counters"]["terminal_observation_count"] == 2
    assert journal["counters"]["cancel_reject_count"] == 1
    assert journal["counters"]["cancel_reject_to_partially_filled_count"] == 1
    assert journal["counters"]["sub_lot_partial_remaining_count"] == 1
    assert journal["counters"]["full_fill_exact_zero_count"] == 1
    assert journal["counters"]["terminal_positive_remainder_count"] == 0
    assert journal["counters"]["exact_native_lifecycle_count"] == 0
    assert journal["counters"]["native_queue_censored_lifecycle_count"] == 2
    assert journal["cif_eligibility"]["eligible_lifecycle_count"] == 0
    assert journal["cif_eligibility"]["censor_reason_counts"] == {
        "queue_source:pending_activation": 2
    }
    rows = []
    for part in sorted((tmp_path / "journal").rglob("part-*.parquet")):
        rows.extend(pq.read_table(part).to_pylist())
    from research.families.f07_active_order_continuation.audit.order_lifecycle_v2_cpp_event_stream_binding_v2 import (
        audit_cpp_event_stream_lockstep,
    )

    lockstep = audit_cpp_event_stream_lockstep(rows)
    assert lockstep["mechanics_lockstep_passed"] is True
    assert lockstep["abi_version"] == (
        "order_lifecycle_journal_v2_cpp_event_stream_mirror.v2"
    )


def test_strict_native_adapter_rejects_unclassified_activation(tmp_path: Path) -> None:
    adapter = OrderLifecycleV2ReplayAdapter(
        root=tmp_path / "journal",
        session_id="strict-native",
        runtime_identity={"identity": emitter.IDENTITY},
        symbol="BTCUSDC",
        storage_format="jsonl",
        strict_native_only=True,
    )
    order = {
        "trace_id": 1,
        "side": "BUY",
        "submit_ts": 1_000,
        "quantity": 0.001,
        "remaining": 0.001,
    }
    adapter.submit(order, 1_000)
    with pytest.raises(ValueError, match="lacks an explicit queue source"):
        adapter.activate(order, visibility_ts_ms=1_110, exchange_ts_ms=1_100)
    adapter.close()


def test_cif_eligibility_ignores_valid_post_terminal_no_queue_rows(
    tmp_path: Path,
) -> None:
    day = "2026-04-17"
    runtime_identity = {
        "identity": emitter.IDENTITY,
        "day": day,
        "economic_outcomes_read": False,
    }
    day_row = {
        "day": day,
        "runtime_identity": runtime_identity,
        "runtime_identity_sha256": emitter.canonical_sha256(runtime_identity),
    }
    adapter = OrderLifecycleV2ReplayAdapter(
        root=tmp_path / "journal",
        session_id=f"f07-{day}",
        runtime_identity=runtime_identity,
        symbol="BTCUSDC",
        storage_format="parquet",
        strict_native_only=True,
    )
    order = {
        "trace_id": 7,
        "side": "BUY",
        "submit_ts": 1_000,
        "quote_ts": 1_000,
        "quantity": 0.001,
        "remaining": 0.001,
        "simulator_queue_source": "native_exchange_book",
        "exact_queue_path_valid": True,
    }
    adapter.submit(order, 1_000)
    adapter.activate(order, visibility_ts_ms=1_110, exchange_ts_ms=1_100)
    adapter.request_cancel(order, 1_200)
    adapter.cancel_ack(order, visibility_ts_ms=1_230, exchange_ts_ms=1_220)

    key, lifecycle = adapter._lifecycle(order)
    post_terminal = {
        **order,
        "simulator_queue_source": "post_terminal_no_fill_risk",
        "exact_queue_path_valid": False,
    }
    lifecycle.enter_post_cancel_recovery(1_240_000_000)
    adapter._commit(
        order=post_terminal,
        key=key,
        callback_type="post_cancel_recovery",
        received_ts_ms=1_240,
        exchange_ts_ms=None,
    )
    lifecycle.mark_reentry_eligible(1_250_000_000)
    adapter._commit(
        order=post_terminal,
        key=key,
        callback_type="reentry_eligible",
        received_ts_ms=1_250,
        exchange_ts_ms=None,
    )
    adapter.close()

    journal = emitter._journal_mechanics_summary(
        staging_root=tmp_path,
        day_row=day_row,
        lot_size_btc=0.001,
    )

    assert journal["cif_eligibility"]["eligible_lifecycle_count"] == 1
    assert journal["cif_eligibility"]["censored_lifecycle_count"] == 0
    assert journal["cif_eligibility"]["censor_reason_counts"] == {}


def test_pre_activation_cancel_enters_pending_only_after_activation(
    tmp_path: Path,
) -> None:
    adapter = OrderLifecycleV2ReplayAdapter(
        root=tmp_path / "journal",
        session_id="pre-activation-active",
        runtime_identity={"identity": "test", "economic_outcomes_read": False},
        symbol="BTCUSDC",
        storage_format="parquet",
        strict_native_only=True,
    )
    order = {
        "trace_id": 91,
        "side": "BUY",
        "submit_ts": 1_000,
        "quote_ts": 1_000,
        "quantity": 0.001,
        "remaining": 0.001,
        "simulator_queue_source": "native_exchange_book",
        "exact_queue_path_valid": True,
    }
    adapter.submit(order, 1_000)
    adapter.request_cancel(order, 1_020)
    assert adapter._lifecycles[91].phase.value == "SUBMITTED"
    adapter.activate(order, visibility_ts_ms=1_110, exchange_ts_ms=1_100)
    assert adapter._lifecycles[91].phase.value == "CANCEL_PENDING"
    adapter.cancel_ack(order, visibility_ts_ms=1_140, exchange_ts_ms=1_130)
    health = adapter.close()
    session = tmp_path / "journal" / "session-pre-activation-active"
    rows, _, _ = emitter._read_journal_parts(session)
    assert [row["lifecycle_event"] for row in rows] == [
        "submit",
        "activate",
        "cancel_request",
        "exchange_terminal",
    ]
    assert rows[2]["phase_before"] == "ACTIVE"
    assert rows[2]["phase_after"] == "CANCEL_PENDING"
    assert health["adapter_pre_activation_cancel_request_count"] == 1
    assert health["adapter_pre_activation_cancel_pending_count"] == 0


def test_pre_activation_cancel_ack_is_explicit_no_activation_censor(
    tmp_path: Path,
) -> None:
    day = "2026-04-17"
    runtime_identity = {
        "identity": emitter.IDENTITY,
        "day": day,
        "economic_outcomes_read": False,
    }
    day_row = {
        "day": day,
        "runtime_identity": runtime_identity,
        "runtime_identity_sha256": emitter.canonical_sha256(runtime_identity),
    }
    adapter = OrderLifecycleV2ReplayAdapter(
        root=tmp_path / "journal",
        session_id=f"f07-{day}",
        runtime_identity=runtime_identity,
        symbol="BTCUSDC",
        storage_format="parquet",
        strict_native_only=True,
    )
    order = {
        "trace_id": 92,
        "side": "SELL",
        "submit_ts": 2_000,
        "quote_ts": 2_000,
        "quantity": 0.001,
        "remaining": 0.001,
        "simulator_queue_source": "pending_activation",
        "exact_queue_path_valid": False,
    }
    adapter.submit(order, 2_000)
    adapter.request_cancel(order, 2_020)
    adapter.cancel_ack(order, visibility_ts_ms=2_080, exchange_ts_ms=2_070)
    health = adapter.close()
    journal = emitter._journal_mechanics_summary(
        staging_root=tmp_path,
        day_row=day_row,
        lot_size_btc=0.001,
    )
    assert journal["dual_clock"]["passed"] is True
    assert journal["cif_eligibility"]["eligible_lifecycle_count"] == 0
    assert journal["cif_eligibility"]["censored_lifecycle_count"] == 1
    assert journal["cif_eligibility"]["censor_reason_counts"] == {"no_activation": 1}
    assert health["adapter_pre_activation_cancel_ack_count"] == 1


def test_plan_sha_lock_is_exclusive(tmp_path: Path) -> None:
    plan_sha = "a" * 64
    with emitter._exclusive_plan_lock(tmp_path, plan_sha):
        with pytest.raises(emitter.ReplayEmitterError, match="already held"):
            with emitter._exclusive_plan_lock(tmp_path, plan_sha):
                pass


def test_execute_plan_atomically_publishes_then_resumes(tmp_path: Path) -> None:
    plan, plan_path = _fixture_plan(tmp_path)
    calls: list[list[str]] = []

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return _emit_mock_day(command=command, plan=plan)

    first = emitter.execute_plan(
        plan_path=plan_path,
        python_executable=Path(".venv/bin/python"),
        days=["2026-04-17"],
        command_runner=runner,
    )
    second = emitter.execute_plan(
        plan_path=plan_path,
        python_executable=Path(".venv/bin/python"),
        days=["2026-04-17"],
        command_runner=runner,
    )

    assert first["requested"] == [{"day": "2026-04-17", "status": "executed"}]
    assert second["requested"] == [{"day": "2026-04-17", "status": "resumed"}]
    assert len(calls) == 1
    assert (tmp_path / "days/2026-04-17/day_manifest.json").is_file()
    assert not list((tmp_path / ".staging").iterdir())
    assert first["formal_40day_journal_emission_complete"] is False
    assert first["economic_outcomes_read"] is False


def test_single_plan_owner_can_run_independent_days_in_parallel(tmp_path: Path) -> None:
    plan, plan_path = _fixture_plan(tmp_path)
    requested = ["2026-04-17", "2026-04-18"]
    guard = threading.Lock()
    active = 0
    maximum_active = 0

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        nonlocal active, maximum_active
        with guard:
            active += 1
            maximum_active = max(maximum_active, active)
        try:
            time.sleep(0.05)
            return _emit_mock_day(command=command, plan=plan)
        finally:
            with guard:
                active -= 1

    result = emitter.execute_plan(
        plan_path=plan_path,
        python_executable=Path(".venv/bin/python"),
        days=requested,
        command_runner=runner,
        workers=2,
    )

    assert maximum_active == 2
    assert result["requested"] == [
        {"day": day, "status": "executed"} for day in requested
    ]
    for day in requested:
        assert (tmp_path / "days" / day / "day_manifest.json").is_file()
    assert not list((tmp_path / ".staging").iterdir())


@pytest.mark.parametrize("workers", [0, 9])
def test_parallel_worker_count_is_bounded(tmp_path: Path, workers: int) -> None:
    _, plan_path = _fixture_plan(tmp_path)

    with pytest.raises(emitter.ReplayEmitterError, match="between 1 and 8"):
        emitter.execute_plan(
            plan_path=plan_path,
            python_executable=Path(".venv/bin/python"),
            days=["2026-04-17"],
            workers=workers,
        )


def test_resume_rejects_mutated_journal_payload(tmp_path: Path) -> None:
    plan, plan_path = _fixture_plan(tmp_path)

    emitter.execute_plan(
        plan_path=plan_path,
        python_executable=Path(".venv/bin/python"),
        days=["2026-04-17"],
        command_runner=lambda command: _emit_mock_day(command=command, plan=plan),
    )
    payload = next((tmp_path / "days/2026-04-17/journal").rglob("*.parquet"))
    payload.write_bytes(b"mutated")

    with pytest.raises(emitter.ReplayEmitterError, match="artifact size differs"):
        emitter.execute_plan(
            plan_path=plan_path,
            python_executable=Path(".venv/bin/python"),
            days=["2026-04-17"],
            command_runner=lambda command: _emit_mock_day(command=command, plan=plan),
        )


@pytest.mark.parametrize(
    "field",
    ["pnl_usdc", "reward", "maker_markout_10s", "campaign_economics"],
)
def test_economic_fields_are_rejected_before_publication(tmp_path: Path, field: str) -> None:
    plan, plan_path = _fixture_plan(tmp_path)

    with pytest.raises(emitter.ReplayEmitterError, match="economic field is forbidden"):
        emitter.execute_plan(
            plan_path=plan_path,
            python_executable=Path(".venv/bin/python"),
            days=["2026-04-17"],
            command_runner=lambda command: _emit_mock_day(
                command=command,
                plan=plan,
                economic_field=(field, 1.0),
            ),
        )
    assert not (tmp_path / "days/2026-04-17").exists()
    assert not list((tmp_path / ".staging").iterdir())


def test_unrecognized_terminal_value_field_is_rejected_by_exact_schema(
    tmp_path: Path,
) -> None:
    plan, plan_path = _fixture_plan(tmp_path)

    with pytest.raises(emitter.ReplayEmitterError, match="journal manifest schema differs"):
        emitter.execute_plan(
            plan_path=plan_path,
            python_executable=Path(".venv/bin/python"),
            days=["2026-04-17"],
            command_runner=lambda command: _emit_mock_day(
                command=command,
                plan=plan,
                economic_field=("terminal_value", 1.0),
            ),
        )
    assert not (tmp_path / "days/2026-04-17").exists()
    assert not list((tmp_path / ".staging").iterdir())


def test_worker_failure_leaves_no_admitted_or_staged_day(tmp_path: Path) -> None:
    _, plan_path = _fixture_plan(tmp_path)

    with pytest.raises(emitter.ReplayEmitterError, match="replay worker failed"):
        emitter.execute_plan(
            plan_path=plan_path,
            python_executable=Path(".venv/bin/python"),
            days=["2026-04-17"],
            command_runner=lambda command: subprocess.CompletedProcess(
                command, 17, stdout="", stderr="fixture failure"
            ),
        )
    assert not (tmp_path / "days/2026-04-17").exists()
    assert not list((tmp_path / ".staging").iterdir())


def test_plan_validation_rejects_identity_drift(tmp_path: Path) -> None:
    plan, _ = _fixture_plan(tmp_path)
    tampered = copy.deepcopy(plan)
    tampered["days"][0]["daily_source_identity_sha256"] = "f" * 64
    tampered["canonical_plan_sha256"] = emitter.canonical_document_sha256(
        tampered, "canonical_plan_sha256"
    )

    with pytest.raises(emitter.ReplayEmitterError, match="daily source identity differs"):
        emitter.validate_execution_plan(tampered)
