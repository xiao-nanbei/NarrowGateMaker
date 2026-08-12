from __future__ import annotations

import importlib.util
import json
import shutil
import sys
import time
from pathlib import Path

import pytest
import yaml

from scripts.build_prospective_lifecycle_narrow_release import (
    MANIFEST_NAME,
    NEW_FILES,
    PATCH_EXISTING_FILES,
    REMOTE_SOURCE_DEFAULTS,
    build_release,
    validate_staging,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def built_releases(tmp_path_factory: pytest.TempPathFactory):
    root = tmp_path_factory.mktemp("prospective-lifecycle-release")
    first = root / "first"
    second = root / "second"
    first_manifest, first_validation = build_release(
        repo_root=ROOT,
        output_root=first,
    )
    second_manifest, second_validation = build_release(
        repo_root=ROOT,
        output_root=second,
    )
    return (
        first,
        second,
        first_manifest,
        second_manifest,
        first_validation,
        second_validation,
    )


def test_two_builds_are_byte_deterministic_and_unauthorized(built_releases) -> None:
    first, second, first_manifest, second_manifest, first_validation, second_validation = (
        built_releases
    )

    assert first_manifest == second_manifest
    assert first_validation == second_validation
    assert first_validation["payload_valid"] is True
    assert first_validation["local_overlay_import_smoke_passed"] is True
    assert first_validation["deployment_authorized"] is False
    for record in first_manifest["files"]:
        relative = record["path"]
        assert (first / relative).read_bytes() == (second / relative).read_bytes()


def test_payload_classifies_order_lifecycle_as_new_file(built_releases) -> None:
    first, _, manifest, *_ = built_releases
    records = {row["path"]: row for row in manifest["files"]}

    assert records["execution/order_lifecycle.py"]["mode"] == "add_new"
    assert manifest["new_file_remote_absence"]["execution/order_lifecycle.py"] is True
    assert manifest["transplant_contract"]["order_lifecycle_py_classification"] == (
        "new_file"
    )
    assert set(PATCH_EXISTING_FILES).issubset(records)
    assert set(NEW_FILES).issubset(records)
    assert not any(path.startswith("cpp/") for path in records)
    assert not any(path.startswith("features/") for path in records)
    assert (first / "execution/prospective_lifecycle_state_capture_v1.py").is_file()


def test_release_config_changes_only_lifecycle_top_level_section(
    built_releases,
) -> None:
    first, _, manifest, *_ = built_releases
    predecessor = yaml.safe_load(
        REMOTE_SOURCE_DEFAULTS["live/config.yaml"].read_text(encoding="utf-8")
    )
    candidate = yaml.safe_load(
        (first / "live/config.yaml").read_text(encoding="utf-8")
    )
    lifecycle = candidate.pop("lifecycle_journal_v2")

    assert candidate == predecessor
    assert lifecycle == manifest["config_semantics"]["lifecycle_journal_v2"]
    assert lifecycle["enabled"] is True
    assert lifecycle["storage_profile"] == "bounded_remote_spool"
    assert manifest["config_semantics"]["all_existing_fields_unchanged"] is True


def test_release_retains_lifecycle_config_dataclass_decorator(
    built_releases,
) -> None:
    first = built_releases[0]
    text = (first / "live/config.py").read_text(encoding="utf-8")

    assert "@dataclass\nclass LifecycleJournalV2Config:" in text


def test_tampered_remote_predecessor_fails_before_output(tmp_path: Path) -> None:
    tampered = tmp_path / "main.py"
    tampered.write_bytes(REMOTE_SOURCE_DEFAULTS["live/main.py"].read_bytes() + b"\n")
    sources = dict(REMOTE_SOURCE_DEFAULTS)
    sources["live/main.py"] = tampered
    output = tmp_path / "release"

    with pytest.raises(ValueError, match="remote live/main.py SHA256 mismatch"):
        build_release(
            repo_root=ROOT,
            output_root=output,
            remote_sources=sources,
        )
    assert not output.exists()
    assert not list(tmp_path.glob(".release.partial-*"))


def test_tampered_candidate_file_or_manifest_fails_closed(
    tmp_path: Path,
    built_releases,
) -> None:
    first = built_releases[0]
    file_tamper = tmp_path / "file-tamper"
    shutil.copytree(first, file_tamper)
    target = file_tamper / "strategy/order_manager.py"
    target.write_bytes(target.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="release file hash mismatch"):
        validate_staging(file_tamper)

    manifest_tamper = tmp_path / "manifest-tamper"
    shutil.copytree(first, manifest_tamper)
    path = manifest_tamper / MANIFEST_NAME
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["deployment_authorized"] = True
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="canonical hash mismatch"):
        validate_staging(manifest_tamper)


def test_build_does_not_modify_current_runtime_files(
    tmp_path: Path,
    built_releases,
) -> None:
    del built_releases
    guarded = [
        ROOT / "live/main.py",
        ROOT / "live/config.py",
        ROOT / "strategy/maker_engine.py",
    ]
    before = {path: path.read_bytes() for path in guarded}
    build_release(repo_root=ROOT, output_root=tmp_path / "release")
    after = {path: path.read_bytes() for path in guarded}
    assert before == after


def test_staged_order_manager_emits_submit_cancel_and_terminal_callbacks(
    monkeypatch: pytest.MonkeyPatch,
    built_releases,
) -> None:
    first = built_releases[0]
    lifecycle_path = first / "execution/order_lifecycle.py"
    lifecycle_spec = importlib.util.spec_from_file_location(
        "execution.order_lifecycle",
        lifecycle_path,
    )
    assert lifecycle_spec is not None and lifecycle_spec.loader is not None
    lifecycle_module = importlib.util.module_from_spec(lifecycle_spec)
    monkeypatch.setitem(sys.modules, "execution.order_lifecycle", lifecycle_module)
    lifecycle_spec.loader.exec_module(lifecycle_module)

    manager_path = first / "strategy/order_manager.py"
    manager_spec = importlib.util.spec_from_file_location(
        "staged_order_manager",
        manager_path,
    )
    assert manager_spec is not None and manager_spec.loader is not None
    manager_module = importlib.util.module_from_spec(manager_spec)
    monkeypatch.setitem(sys.modules, "staged_order_manager", manager_module)
    manager_spec.loader.exec_module(manager_module)

    callbacks: list[tuple[str, str]] = []

    def on_lifecycle(order, event_type, _event) -> None:
        callbacks.append((order.client_order_id, event_type))

    manager = manager_module.OrderManager(on_lifecycle_event=on_lifecycle)
    now_ns = time.time_ns()
    client_order_id = manager.create_order(
        "BTCUSDC",
        manager_module.Side.BUY,
        60_000.0,
        0.001,
    )
    manager.confirm_new(
        client_order_id,
        123,
        exchange_ts_ns=now_ns - 3_000_000,
    )
    manager.mark_pending_cancel(client_order_id)
    assert manager.cancel_rejected(
        client_order_id,
        "cancel rejected",
        exchange_ts_ns=now_ns - 2_000_000,
    )
    manager.on_order_update(
        {
            "c": client_order_id,
            "i": 123,
            "X": "CANCELED",
            "T": (now_ns - 1_000_000) // 1_000_000,
            "z": "0",
            "L": "0",
            "_local_receive_ts_ns": now_ns + 1_000_000,
        }
    )

    assert [event for _, event in callbacks] == [
        "submit",
        "rest_ack",
        "cancel_request",
        "cancel_rejected",
        "cancel_ack",
    ]
    snapshot = manager.lifecycle_snapshot(client_order_id)
    assert snapshot is not None
    assert snapshot["phase"] == "EXCHANGE_TERMINAL"
    assert snapshot["terminal_reason"] == "cancel_ack"
