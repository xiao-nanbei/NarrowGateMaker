from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from live import main as subject

RUNNING_CONFIG_SHA256 = "1" * 64


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _snapshot() -> dict:
    return {
        "commit": "a" * 40,
        "tree": "b" * 40,
        "status_porcelain_sha256": subject._sha256_bytes(b""),  # noqa: SLF001
        "status_entry_count": 0,
        "worktree_clean": True,
        "snapshot_internally_stable": True,
    }


def _source_rows() -> list[dict]:
    rows = []
    for _module, relative in subject.KEY_LOADED_RUNTIME_MODULES.values():
        rows.append(
            {
                "path": relative,
                "working_file_sha256": "c" * 64,
                "head_blob_sha256": "c" * 64,
                "working_size_bytes": 10,
                "head_blob_size_bytes": 10,
                "matches_head_blob": True,
            }
        )
    return sorted(rows, key=lambda row: row["path"])


def _loaded_origins(rows: list[dict]) -> dict:
    source_by_path = {row["path"]: row for row in rows}
    return {
        role: {
            "module_name": module,
            "origin_path": str(subject.ROOT / relative),
            "repository_relative_path": relative,
            "source_sha256": source_by_path[relative]["working_file_sha256"],
        }
        for role, (module, relative) in subject.KEY_LOADED_RUNTIME_MODULES.items()
    }


class _Engine:
    def __init__(
        self,
        *,
        buy_identity: str = "B0",
        remaining_ms: int = 0,
        restore_mode: str = "fresh_b0_no_checkpoint",
        checkpoint_loaded: bool = False,
        checkpoint_sequence: int = 0,
        active_identity: str = "B0",
        active_release: dict[str, str] | None = None,
    ) -> None:
        self.buy_identity = buy_identity
        self.remaining_ms = remaining_ms
        self.restore_mode = restore_mode
        self.checkpoint_loaded = checkpoint_loaded
        self.checkpoint_sequence = checkpoint_sequence
        self.active_identity = active_identity
        self.active_release = (
            dict(active_release)
            if active_release is not None
            else (
                {
                    "path": "/private/release.json",
                    "file_sha256": "e" * 64,
                    "file_canonical_sha256": "f" * 64,
                    "execution_commit": "a" * 40,
                    "execution_tree": "b" * 40,
                    "annotated_operational_tag": "f05-buy-e3-active-v1",
                    "annotated_operational_tag_object": "c" * 40,
                    "active_config_file_sha256": RUNNING_CONFIG_SHA256,
                    "disabled_config_file_sha256": "2" * 64,
                }
                if active_identity.startswith("BUY_E3:")
                else {
                    "path": "",
                    "file_sha256": "",
                    "file_canonical_sha256": "",
                    "execution_commit": "",
                    "execution_tree": "",
                    "annotated_operational_tag": "",
                    "annotated_operational_tag_object": "",
                    "active_config_file_sha256": "",
                    "disabled_config_file_sha256": "",
                }
            )
        )

    def _active_buy_e3_deadline_identity(self) -> str:
        return self.active_identity

    def fill_cooldown_state_snapshot(self) -> dict:
        return {
            "schema_version": "narrowgate_fill_cooldown_state.v2",
            "buy_deadline_identity": self.buy_identity,
            "buy_remaining_ms": self.remaining_ms,
            "restore_mode": self.restore_mode,
            "checkpoint_loaded": self.checkpoint_loaded,
            "checkpoint_sequence": self.checkpoint_sequence,
        }

    def buy_e3_active_release_identity(self) -> dict[str, str]:
        return dict(self.active_release)

    def shadow_runtime_snapshot(self) -> dict:
        return {
            "schema_version": "narrowgate_shadow_runtime_identity.v1",
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
            "global_flow_native_requested": False,
            "global_flow_native_effective": False,
            "global_flow_backend": {
                "native": 0,
                "market_count": 0,
                "trade_batches": 0,
                "trade_events_seen": 0,
                "trade_events_accepted": 0,
                "book_events_seen": 0,
                "book_events_accepted": 0,
                "out_of_order_events": 0,
                "stale_trade_events": 0,
                "trade_overflow_events": 0,
                "book_overflow_events": 0,
            },
            "global_reference_bridge_basis_sample_count": 0,
            "state_restore_contract": "shadow_state_never_restored",
            "global_flow_shadow_config_explicit": True,
            "global_reference_shadow_config_explicit": True,
        }


def _bind_clean_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = _source_rows()
    monkeypatch.setattr(subject, "_git_snapshot", _snapshot)
    monkeypatch.setattr(subject, "_runtime_source_rows", lambda: rows)
    monkeypatch.setattr(subject, "_loaded_module_origins", _loaded_origins)
    monkeypatch.setattr(
        subject,
        "_file_byte_identity",
        lambda path: {
            "reported_path": str(Path(path).absolute()),
            "resolved_path": str(Path(path).absolute()),
            "sha256": "d" * 64,
            "size_bytes": 10,
        },
    )
    monkeypatch.setattr(subject, "_native_runtime_file_identity", lambda _runtime: None)
    monkeypatch.setattr(
        subject,
        "_git_output",
        lambda *args: (
            f"{subject.ROOT}\n".encode() if args == ("rev-parse", "--show-toplevel") else b""
        ),
    )


def test_startup_attestation_accepts_only_clean_fresh_b0(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_clean_checkout(monkeypatch)
    attestation = subject.build_startup_attestation(
        engine=_Engine(),
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256=RUNNING_CONFIG_SHA256,
    )

    assert attestation["status"] == "accepted"
    assert attestation["errors"] == []
    assert all(attestation["gates"].values())
    assert set(attestation) == {
        "schema_version",
        "status",
        "attested_at_utc",
        "fill_cooldown_state",
        "shadow_runtime_identity",
        "buy_e3_active_release",
        "running_checkout",
        "loaded_module_origins",
        "interpreter_identity",
        "native_runtime_identity",
        "gates",
        "errors",
    }
    assert set(attestation["gates"]) == set(subject.STARTUP_ATTESTATION_GATE_NAMES)
    assert attestation["running_checkout"]["runtime_source_manifest_sha256"] == (
        subject._runtime_source_manifest_sha256(  # noqa: SLF001
            attestation["running_checkout"]["runtime_source_files"]
        )
    )


def test_startup_attestation_accepts_exact_same_artifact_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_clean_checkout(monkeypatch)
    artifact_identity = f"BUY_E3:{'a' * 64}"
    attestation = subject.build_startup_attestation(
        engine=_Engine(
            buy_identity=artifact_identity,
            remaining_ms=1_500_000,
            restore_mode="exact_same_artifact_resume",
            checkpoint_loaded=True,
            checkpoint_sequence=8,
            active_identity=artifact_identity,
        ),
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256=RUNNING_CONFIG_SHA256,
    )

    assert attestation["status"] == "accepted"
    assert attestation["errors"] == []
    assert attestation["fill_cooldown_state"]["restore_mode"] == ("exact_same_artifact_resume")


def test_startup_attestation_rejects_release_bound_to_another_active_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_clean_checkout(monkeypatch)
    artifact_identity = f"BUY_E3:{'a' * 64}"
    engine = _Engine(
        buy_identity=artifact_identity,
        remaining_ms=1_500_000,
        restore_mode="exact_same_artifact_resume",
        checkpoint_loaded=True,
        checkpoint_sequence=8,
        active_identity=artifact_identity,
    )
    attestation = subject.build_startup_attestation(
        engine=engine,
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256="9" * 64,
    )

    assert attestation["status"] == "rejected"
    assert "buy_e3_active_release_matches_running_config" in attestation["errors"]
    assert attestation["gates"]["safe_to_start_live_loops"] is False


def test_startup_attestation_rejects_active_e3_without_checkpoint_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_clean_checkout(monkeypatch)
    artifact_identity = f"BUY_E3:{'a' * 64}"
    attestation = subject.build_startup_attestation(
        engine=_Engine(active_identity=artifact_identity),
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256=RUNNING_CONFIG_SHA256,
    )

    assert attestation["status"] == "rejected"
    assert "fill_cooldown_checkpoint_binding_valid" in attestation["errors"]
    assert "fill_cooldown_artifact_contract_valid" in attestation["errors"]
    assert attestation["gates"]["safe_to_start_live_loops"] is False


def test_startup_attestation_accepts_rollback_to_residual_b0(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_clean_checkout(monkeypatch)
    attestation = subject.build_startup_attestation(
        engine=_Engine(
            buy_identity="B0",
            remaining_ms=170_000,
            restore_mode="rollback_to_b0",
            checkpoint_loaded=True,
            checkpoint_sequence=12,
        ),
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256=RUNNING_CONFIG_SHA256,
    )

    assert attestation["status"] == "accepted"
    assert attestation["errors"] == []


def test_startup_attestation_accepts_artifact_drift_only_after_b0_conversion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_clean_checkout(monkeypatch)
    attestation = subject.build_startup_attestation(
        engine=_Engine(
            buy_identity="B0",
            remaining_ms=85_000,
            restore_mode="artifact_identity_changed_to_b0",
            checkpoint_loaded=True,
            checkpoint_sequence=5,
            active_identity=f"BUY_E3:{'b' * 64}",
        ),
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256=RUNNING_CONFIG_SHA256,
    )

    assert attestation["status"] == "accepted"
    assert attestation["errors"] == []


def test_startup_attestation_rejects_e3_identity_under_b0_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bind_clean_checkout(monkeypatch)
    attestation = subject.build_startup_attestation(
        engine=_Engine(
            buy_identity=f"BUY_E3:{'a' * 64}",
            remaining_ms=2_048_000,
            restore_mode="rollback_to_b0",
            checkpoint_loaded=True,
            checkpoint_sequence=2,
        ),
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256=RUNNING_CONFIG_SHA256,
    )

    assert attestation["status"] == "rejected"
    assert "fill_cooldown_deadline_contract_valid" in attestation["errors"]
    assert attestation["gates"]["safe_to_start_live_loops"] is False


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        (
            lambda state: state.update(
                {"global_flow_shadow_config_explicit": False}
            ),
            "shadow_config_explicit",
        ),
        (
            lambda state: state["global_flow_backend"].update(
                {"out_of_order_events": 1}
            ),
            "global_flow_shadow_backend_contract_valid",
        ),
        (
            lambda state: state.update(
                {"global_reference_bridge_basis_sample_count": 1}
            ),
            "global_reference_shadow_state_contract_valid",
        ),
        (
            lambda state: state.update(
                {"global_reference_bridge_basis_sample_count": False}
            ),
            "global_reference_shadow_state_contract_valid",
        ),
        (
            lambda state: state.update(
                {"global_reference_bridge_basis_sample_count": 0.0}
            ),
            "global_reference_shadow_state_contract_valid",
        ),
        (
            lambda state: state.update({"global_flow_shadow_enabled": True}),
            "global_flow_shadow_backend_contract_valid",
        ),
        (
            lambda state: state.update({"global_reference_shadow_enabled": True}),
            "global_reference_shadow_state_contract_valid",
        ),
        (
            lambda state: state.update({"global_flow_native_effective": True}),
            "global_flow_shadow_backend_contract_valid",
        ),
    ],
)
def test_startup_attestation_rejects_disabled_shadow_identity_drift(
    monkeypatch: pytest.MonkeyPatch,
    mutation,
    expected_error: str,
) -> None:
    _bind_clean_checkout(monkeypatch)
    engine = _Engine()
    state = engine.shadow_runtime_snapshot()
    mutation(state)
    engine.shadow_runtime_snapshot = lambda: state
    attestation = subject.build_startup_attestation(
        engine=engine,
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256=RUNNING_CONFIG_SHA256,
    )
    assert attestation["status"] == "rejected"
    assert expected_error in attestation["errors"]
    assert attestation["gates"]["safe_to_start_live_loops"] is False


@pytest.mark.parametrize("drift_kind", ["tracked", "untracked"])
def test_git_snapshot_includes_tracked_and_untracked_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift_kind: str,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.name", "NarrowGate Test")
    _git(root, "config", "user.email", "narrowgate@example.invalid")
    tracked = root / "tracked.py"
    tracked.write_text("FROZEN = True\n", encoding="ascii")
    _git(root, "add", "tracked.py")
    _git(root, "commit", "-m", "frozen")
    monkeypatch.setattr(subject, "ROOT", root)

    clean = subject._git_snapshot()  # noqa: SLF001
    assert clean["worktree_clean"] is True
    assert clean["status_entry_count"] == 0

    if drift_kind == "tracked":
        tracked.write_text("FROZEN = False\n", encoding="ascii")
    else:
        (root / "untracked.py").write_text("DRIFT = True\n", encoding="ascii")

    drifted = subject._git_snapshot()  # noqa: SLF001
    assert drifted["worktree_clean"] is False
    assert drifted["status_entry_count"] == 1
    assert drifted["status_porcelain_sha256"] != subject._sha256_bytes(b"")  # noqa: SLF001

    rows = [
        {
            "path": "tracked.py",
            "working_file_sha256": "c" * 64,
            "head_blob_sha256": "c" * 64,
            "working_size_bytes": 10,
            "head_blob_size_bytes": 10,
            "matches_head_blob": True,
        }
    ]
    monkeypatch.setattr(subject, "_runtime_source_rows", lambda: rows)
    monkeypatch.setattr(
        subject,
        "_loaded_module_origins",
        lambda _rows: {
            role: {
                "module_name": module,
                "origin_path": str(root / "tracked.py"),
                "repository_relative_path": "tracked.py",
                "source_sha256": "c" * 64,
            }
            for role, (module, _relative) in subject.KEY_LOADED_RUNTIME_MODULES.items()
        },
    )
    monkeypatch.setattr(
        subject,
        "_file_byte_identity",
        lambda path: {
            "reported_path": str(Path(path).absolute()),
            "resolved_path": str(Path(path).absolute()),
            "sha256": "d" * 64,
            "size_bytes": 10,
        },
    )
    monkeypatch.setattr(subject, "_native_runtime_file_identity", lambda _runtime: None)

    attestation = subject.build_startup_attestation(
        engine=_Engine(),
        native_runtime={"profile": "test", "module": "disabled"},
        running_config_sha256=RUNNING_CONFIG_SHA256,
    )
    assert attestation["status"] == "rejected"
    assert "git_pre_worktree_clean" in attestation["errors"]
    assert "git_post_worktree_clean" in attestation["errors"]
    assert attestation["gates"]["safe_to_start_live_loops"] is False
