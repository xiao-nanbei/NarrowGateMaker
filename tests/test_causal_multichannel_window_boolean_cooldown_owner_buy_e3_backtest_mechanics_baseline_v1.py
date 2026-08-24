from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_backtest_mechanics_baseline_v1 as baseline,
)


def _private_file(path: Path, data: bytes) -> Path:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(0o600)
    return path


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _public_contract_path() -> Path:
    return (
        Path(__file__).absolute().parents[1] / "research/families/f05_fill_quality_quote_ev/docs/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "backtest_mechanics_baseline_v1_20260825.json"
    )


def _v14_publication_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> SimpleNamespace:
    predecessor = {
        "schema_version": baseline.V13_SCHEMA_VERSION,
        "baseline_id": baseline.V13_BASELINE_ID,
        "effective_at_utc": baseline.V13_EFFECTIVE_AT_UTC,
        "operational_status": baseline.V13_OPERATIONAL_STATUS,
        "promotion_class": baseline.V13_PROMOTION_CLASS,
        "permissions": dict(baseline.V13_PERMISSIONS),
        "current_live": {
            "config": {"sha256": baseline.ACTIVE_SOURCE_CONFIG_FILE_SHA256},
            "buy_e3_enabled": True,
            "economic_outcomes_read": False,
            "economic_values_persisted": False,
            "private_release_and_evidence_chain_remain_authority": True,
        },
        "backtest_default": {
            "config_sha256": baseline.PREDECESSOR_V12_CONFIG_FILE_SHA256,
            "exact_buy_e3_replay_baseline_available": False,
            "current_live_config_may_replace_backtest_default": False,
            "current_live_evidence_is_backtest_economic_authority": False,
        },
    }
    predecessor_bytes = json.dumps(predecessor, sort_keys=True).encode("ascii")
    predecessor_path = tmp_path / "v13.json"
    predecessor_path.write_bytes(predecessor_bytes)
    predecessor_path.chmod(0o644)
    predecessor_sha = _sha(predecessor_bytes)
    monkeypatch.setattr(baseline, "V13_PREDECESSOR_FILE_SHA256", predecessor_sha)
    projection_bytes = b"{}\n"
    monkeypatch.setattr(baseline, "HOST_NEUTRAL_CONFIG_FILE_SHA256", _sha(projection_bytes))
    monkeypatch.setattr(baseline, "HOST_NEUTRAL_CONFIG_SIZE_BYTES", len(projection_bytes))
    monkeypatch.setattr(
        baseline, "HOST_NEUTRAL_CONFIG_MAPPING_SHA256", baseline.canonical_sha256({})
    )
    contract_path = _public_contract_path()
    contract_sha = _sha(contract_path.read_bytes())
    contract_document = json.loads(contract_path.read_text(encoding="ascii"))
    contract_binding = MappingProxyType(
        {
            "identity": baseline.IDENTITY,
            "file_sha256": contract_sha,
            "canonical_sha256": contract_document["canonical_contract_sha256"],
        }
    )
    monkeypatch.setattr(
        baseline,
        "_public_contract_binding",
        lambda _path, **_kwargs: contract_binding,
    )
    receipt = baseline.create_v14_mechanics_governance_receipt(
        predecessor_v13_identity_path=predecessor_path,
        predecessor_v13_file_sha256=predecessor_sha,
        public_contract_path=contract_path,
        public_contract_file_sha256=contract_sha,
        effective_at_utc=baseline.V14_EFFECTIVE_AT_UTC,
    )
    cold_publisher = {
        "execution_commit": "1" * 40,
        "execution_tree": "2" * 40,
        "annotated_tag": baseline.V14_COLD_PUBLISHER_TAG,
        "annotated_tag_object": "3" * 40,
        "factory_git_blob": "4" * 40,
        "factory_source_sha256": "5" * 64,
        "runtime_source_set_sha256": baseline.canonical_sha256(
            dict(baseline.RUNTIME_SOURCE_SHA256)
        ),
        "execution_module_origin_keyset_sha256": "6" * 64,
    }
    private_root = tmp_path / "private-root"
    private_root.mkdir(mode=0o700)
    bundle_parent = private_root.joinpath(*baseline.FORMAL_PRIVATE_BUNDLE_RELATIVE.parent.parts)
    bundle_parent.mkdir(mode=0o700, parents=True)
    metadata_root = tmp_path / "metadata-root"
    metadata_root.mkdir(mode=0o700)
    relative_locators = {
        role: (
            f"metadata/{role}.json"
            if role in baseline.OWNER_METADATA_INPUT_ROLES
            else ("layer4" if role == "amended_layer4_root" else f"inputs/{role}.json")
        )
        for role in baseline.OWNER_PRIVATE_INPUT_ROLES
    }
    input_entries = {
        role: {
            "relative_locator": relative_locators[role],
            "locator_base": (
                "metadata_repository"
                if role in baseline.OWNER_METADATA_INPUT_ROLES
                else "durable_evidence"
            ),
            "absolute_locator_persisted": False,
            "kind": "directory" if role == "amended_layer4_root" else "file",
        }
        for role in baseline.OWNER_PRIVATE_INPUT_ROLES
    }
    private_input_contract = {
        "schema_version": baseline.OWNER_PRIVATE_INPUT_SCHEMA,
        "identity": baseline.IDENTITY,
        "status": "exact_owner_private_relative_inputs_recursively_bound",
        "locator_bases": {
            "durable_evidence": {
                "environment_variable": baseline.PRIVATE_EVIDENCE_ROOT_ENV,
                "roles": [
                    role
                    for role in baseline.OWNER_PRIVATE_INPUT_ROLES
                    if role not in baseline.OWNER_METADATA_INPUT_ROLES
                ],
            },
            "metadata_repository": {
                "environment_variable": baseline.METADATA_REPOSITORY_ROOT_ENV,
                "roles": [
                    role
                    for role in baseline.OWNER_PRIVATE_INPUT_ROLES
                    if role in baseline.OWNER_METADATA_INPUT_ROLES
                ],
            },
            "absolute_locator_persisted": False,
        },
        "input_count": baseline.OWNER_PRIVATE_INPUT_COUNT,
        "inputs": input_entries,
        "v13_committed_predecessor": {"transaction_committed": True},
        "economic_or_holdout_inputs_present": False,
        "permissions": dict(baseline.PERMISSIONS),
    }
    private_input_contract["canonical_owner_private_inputs_sha256"] = baseline.document_sha256(
        private_input_contract, "canonical_owner_private_inputs_sha256"
    )
    capability = {
        "schema_version": f"{baseline.IDENTITY}.loaded_capability_receipt.v1",
        "identity": baseline.IDENTITY,
        "owner_private_inputs_sha256": private_input_contract[
            "canonical_owner_private_inputs_sha256"
        ],
        "default_day_overlay_factory_executable": True,
        "permissions": dict(baseline.PERMISSIONS),
    }
    capability["canonical_loaded_capability_sha256"] = baseline.document_sha256(
        capability, "canonical_loaded_capability_sha256"
    )
    smoke = MappingProxyType(
        {
            "capability": dict(capability),
            "owner_private_inputs": dict(private_input_contract),
            "loaded_repo_module_count": 17,
            "loaded_repo_module_origins_sha256": "7" * 64,
            "all_loaded_repo_modules_within_cold_root": True,
            "day_overlay_smoke": {
                "utc_day": baseline.FORMAL_E3_MECHANICS_DAYS[0],
                "artifact_sha256": baseline.EXACT_E3_ARTIFACT_SHA256,
                "canonical_day_overlay_sha256": "8" * 64,
                "buy_e3_installed": True,
                "sell_delegates_exact_b0": True,
                "d_plus_1_exact_b0_washout": True,
            },
        }
    )

    class FakeLoaded:
        def __init__(self) -> None:
            self.projected_config_bytes = projection_bytes
            self.closed = False

        def close(self) -> None:
            self.closed = True

    def load_fake(**kwargs: object) -> tuple[object, Mapping[str, object], Mapping[str, object]]:
        assert dict(kwargs["relative_locators"]) == relative_locators
        expected = kwargs.get("expected_private_input_contract")
        if expected is not None:
            assert dict(expected) == private_input_contract
        return FakeLoaded(), MappingProxyType(private_input_contract), MappingProxyType(capability)

    monkeypatch.setattr(baseline, "load_owner_buy_e3_default_from_private_inputs", load_fake)
    monkeypatch.setattr(baseline, "_cold_subprocess_capability_smoke", lambda **_kwargs: smoke)
    monkeypatch.setattr(
        baseline,
        "capture_cold_publisher",
        lambda *_args, **_kwargs: MappingProxyType(cold_publisher),
    )
    return SimpleNamespace(
        predecessor_path=predecessor_path,
        predecessor_sha=predecessor_sha,
        contract_path=contract_path,
        contract_sha=contract_sha,
        receipt=receipt,
        cold_publisher=cold_publisher,
        projection_bytes=projection_bytes,
        baseline=SimpleNamespace(projected_config_bytes=projection_bytes),
        private_root=private_root,
        metadata_root=metadata_root,
        relative_locators=relative_locators,
        private_input_contract=private_input_contract,
        capability=capability,
        smoke=smoke,
        destination=private_root.joinpath(*baseline.FORMAL_PRIVATE_BUNDLE_RELATIVE.parts),
        runtime_root=Path(__file__).absolute().parents[1],
    )


def test_secure_snapshot_distinguishes_missing_from_hash_drift(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="is missing"):
        baseline._secure_snapshot(
            missing,
            expected_sha256="0" * 64,
            label="fixture",
        )
    path = _private_file(tmp_path / "value.json", b"{}")
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="SHA256 drifted"):
        baseline._secure_snapshot(
            path,
            expected_sha256="0" * 64,
            label="fixture",
        )


def test_secure_snapshot_rejects_final_and_ancestor_symlinks(tmp_path: Path) -> None:
    target = _private_file(tmp_path / "real" / "value.json", b"{}")
    final_link = tmp_path / "value-link.json"
    final_link.symlink_to(target)
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="symlink"):
        baseline._secure_snapshot(
            final_link,
            expected_sha256=_sha(b"{}"),
            label="final link",
        )
    ancestor = tmp_path / "ancestor-link"
    ancestor.symlink_to(target.parent, target_is_directory=True)
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="ancestor"):
        baseline._secure_snapshot(
            ancestor / target.name,
            expected_sha256=_sha(b"{}"),
            label="ancestor link",
        )


def test_secure_snapshot_rejects_hardlink_and_public_mode(tmp_path: Path) -> None:
    path = _private_file(tmp_path / "value.json", b"{}")
    os.link(path, tmp_path / "second-link.json")
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="single-link"):
        baseline._secure_snapshot(
            path,
            expected_sha256=_sha(b"{}"),
            label="hardlink",
        )
    path.unlink()
    path = _private_file(tmp_path / "mode.json", b"{}")
    path.chmod(0o644)
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="mode must be 0600"):
        baseline._secure_snapshot(
            path,
            expected_sha256=_sha(b"{}"),
            label="mode",
        )


def test_secure_snapshot_rejects_foreign_owner_and_writable_trust_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owned = _private_file(tmp_path / "owned" / "value.json", b"{}")
    actual_uid = os.getuid()
    monkeypatch.setattr(baseline.os, "getuid", lambda: actual_uid + 1)
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="current uid"):
        baseline._secure_snapshot(
            owned,
            expected_sha256=_sha(b"{}"),
            label="foreign owner",
        )
    monkeypatch.setattr(baseline.os, "getuid", lambda: actual_uid)
    writable_parent = tmp_path / "writable"
    writable = _private_file(writable_parent / "value.json", b"{}")
    writable_parent.chmod(0o777)
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="trust root"):
        baseline._secure_snapshot(
            writable,
            expected_sha256=_sha(b"{}"),
            label="writable root",
        )


def test_secure_snapshot_rejects_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _private_file(tmp_path / "value.json", b"original")
    backup = tmp_path / "opened.json"
    real_read = baseline.os.read
    swapped = False

    def swap_after_first_read(descriptor: int, size: int) -> bytes:
        nonlocal swapped
        block = real_read(descriptor, size)
        if block and not swapped:
            swapped = True
            path.rename(backup)
            _private_file(path, b"original")
        return block

    monkeypatch.setattr(baseline.os, "read", swap_after_first_read)
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="changed during read|path changed",
    ):
        baseline._secure_snapshot(
            path,
            expected_sha256=_sha(b"original"),
            label="swapped",
        )


@pytest.mark.parametrize(
    "payload,error",
    [
        (b'{"a":1,"a":2}', "duplicate JSON key"),
        (b'{"a":NaN}', "non-finite JSON"),
        (b"[]", "root is not an object"),
    ],
)
def test_strict_json_rejects_ambiguous_documents(payload: bytes, error: str) -> None:
    with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match=error):
        baseline._parse_strict_json(payload, label="fixture")


def test_host_neutral_projection_changes_only_exact_allowlisted_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = {
        "strategy": {
            "fill_cooldown": 85.0,
            "boolean_cooldown_policy_path": "/host/b0-policy.json",
            "boolean_cooldown_predicate_bundle_path": "/host/b0-bundle.json",
            "buy_e3_cooldown_artifact_manifest_path": "/host/e3-manifest.json",
            "buy_e3_cooldown_policy_path": "/host/e3-policy.json",
            "buy_e3_cooldown_predicate_bundle_path": "/host/e3-bundle.json",
        },
        "lifecycle_journal_v2": {
            "enabled": True,
            "required_mount": "/private/host",
            "root": "/private/host/orders",
            "prospective_epoch_root": "/private/host/epochs",
            "remote_spool_allowlisted_roots": ["/private/host"],
            "baseline_identity_path": "/host/v13.json",
            "baseline_identity_sha256": "0" * 64,
        },
    }
    projected = json.loads(json.dumps(source))
    for path, value in baseline.HOST_NEUTRAL_MUTATIONS.items():
        baseline._set_nested(projected, path, value)
    encoded = (
        json.dumps(projected, sort_keys=True, indent=2, ensure_ascii=True).encode("ascii") + b"\n"
    )
    monkeypatch.setattr(
        baseline,
        "HOST_NEUTRAL_CONFIG_MAPPING_SHA256",
        baseline.canonical_sha256(projected),
    )
    monkeypatch.setattr(baseline, "HOST_NEUTRAL_CONFIG_FILE_SHA256", _sha(encoded))
    monkeypatch.setattr(baseline, "HOST_NEUTRAL_CONFIG_SIZE_BYTES", len(encoded))
    observed, observed_bytes = baseline._project_host_neutral_config(source)
    assert tuple(baseline._mapping_difference_paths(source, observed)) == tuple(
        sorted(baseline.HOST_NEUTRAL_CHANGED_PATHS)
    )
    assert observed_bytes == encoded
    assert source["lifecycle_journal_v2"]["enabled"] is True


def test_frozen_source_and_replay_delta_keysets_are_exact() -> None:
    assert len(baseline.SOURCE_CONFIG_DELTA_PATHS) == 15
    assert baseline.canonical_sha256(list(baseline.SOURCE_CONFIG_DELTA_PATHS)) == (
        baseline.SOURCE_CONFIG_DELTA_PATHS_SHA256
    )


def test_execution_module_origin_matrix_rejects_a_preloaded_wrong_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository_root = Path(__file__).absolute().parents[1]
    monkeypatch.setattr(
        baseline.parity_v1,
        "__file__",
        str(tmp_path / "wrong-checkout" / "parity_v1.py"),
    )
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="execution module origin drifted",
    ):
        baseline._verify_execution_module_origins(repository_root)


def test_replay_param_projection_never_mutates_process_owner_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_policy = baseline.importlib.import_module("live.runtime_policy")
    keys = (
        runtime_policy.F05_BOOLEAN_COOLDOWN_OWNER_OVERRIDE_ENV,
        runtime_policy.F05_BUY_E3_OWNER_OVERRIDE_ENV,
    )
    monkeypatch.setenv(keys[0], "predecessor-owner")
    monkeypatch.delenv(keys[1], raising=False)
    monkeypatch.setattr(
        baseline.live_config,
        "_parse",
        lambda _raw: (time.sleep(0.01), object())[1],
    )
    monkeypatch.setattr(
        baseline.live_config,
        "to_backtest_params",
        lambda _cfg: {"model_dir": ""},
    )
    monkeypatch.setattr(
        baseline.backtest_config,
        "apply_tick_defaults",
        lambda params, **_kwargs: params,
    )
    samples: list[tuple[str | None, str | None]] = []
    stop = threading.Event()

    def observe() -> None:
        while not stop.is_set():
            samples.append((os.environ.get(keys[0]), os.environ.get(keys[1])))

    observer = threading.Thread(target=observe)
    observer.start()
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            tuple(
                pool.map(
                    lambda index: baseline._load_replay_params(
                        tmp_path / f"config-{index}.json", {}
                    ),
                    range(2),
                )
            )
    finally:
        stop.set()
        observer.join()

    assert samples
    assert set(samples) == {("predecessor-owner", None)}
    assert os.environ[keys[0]] == "predecessor-owner"
    assert keys[1] not in os.environ
    assert len(baseline.REPLAY_ABI_SOURCE_DELTA_PATHS) == 9
    assert baseline.canonical_sha256(list(baseline.REPLAY_ABI_SOURCE_DELTA_PATHS)) == (
        baseline.REPLAY_ABI_SOURCE_DELTA_PATHS_SHA256
    )
    assert len(baseline.REPLAY_ABI_FINAL_DELTA_PATHS) == 8
    assert baseline.canonical_sha256(list(baseline.REPLAY_ABI_FINAL_DELTA_PATHS)) == (
        baseline.REPLAY_ABI_FINAL_DELTA_PATHS_SHA256
    )


def test_v13_committed_gate_uses_isolated_no_pythonpath_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_root = tmp_path / "runtime"
    script = runtime_root / "scripts" / "f05_reconcile_live_config_locator_v1.py"
    script.parent.mkdir(mode=0o700, parents=True)
    script.write_bytes(b"# frozen validator\n")
    script.chmod(0o644)
    monkeypatch.setattr(
        baseline,
        "V13_RECONCILIATION_VALIDATOR_SHA256",
        _sha(script.read_bytes()),
    )
    durable_root = tmp_path / "durable"
    durable_root.mkdir(mode=0o700)
    v12 = tmp_path / "metadata" / "v12.yaml"
    active = durable_root / "inputs" / "active.yaml"
    manifest = durable_root / "inputs" / "v13-manifest.json"
    v12.parent.mkdir(mode=0o700)
    active.parent.mkdir(mode=0o700)
    manifest_document = {
        "transaction": {
            "outputs": {"backtest_v12_archive": str(v12.absolute())},
            "active_config_source": {
                "path": str(active.absolute()),
                "sha256": baseline.ACTIVE_SOURCE_CONFIG_FILE_SHA256,
            },
        }
    }
    _private_file(
        manifest,
        json.dumps(manifest_document, sort_keys=True).encode("ascii"),
    )
    manifest_raw = manifest.read_bytes()
    recorded: dict[str, object] = {}

    def isolated_run(command: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        recorded["command"] = tuple(command)  # type: ignore[arg-type]
        recorded["environment"] = dict(kwargs["env"])  # type: ignore[arg-type]
        result = {
            "writes_performed": False,
            "state_before": {
                "immutable": {"archive": "published_nlink1"},
                "receipt": "published_nlink1",
                "stable_alias": "successor",
                "pointer": "successor",
                "catalog": "successor",
                "pending": {"archive": "absent"},
            },
            "manifest": {
                "file_sha256": _sha(manifest_raw),
                "size_bytes": len(manifest_raw),
            },
            "receipt": {
                "file_sha256": "1" * 64,
                "canonical_sha256": "2" * 64,
            },
        }
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(result, sort_keys=True).encode("ascii"),
            stderr=b"",
        )

    monkeypatch.setattr(baseline.subprocess, "run", isolated_run)
    inputs = baseline.OwnerPrivateInputs(
        v13_reconciliation_manifest=manifest,
        predecessor_v12_config=v12,
        active_source_config=active,
        e3_artifact_paths=baseline.ExactE3ArtifactPaths(active, active, active),
        b0_artifact_paths=baseline.ExactB0ArtifactPaths(v12, v12),
        parity_evidence_paths=baseline.ParityEvidencePaths(v12, v12, v12, v12, v12, v12),
        relative_locators=MappingProxyType({}),
    )
    observed = baseline._validate_committed_v13_reconciliation(
        runtime_repository_root=runtime_root,
        durable_evidence_root=durable_root,
        inputs=inputs,
    )
    command = recorded["command"]
    environment = recorded["environment"]
    assert "-I" in command and "-B" in command and "-X" in command
    assert any(str(item).startswith("pycache_prefix=") for item in command)
    assert "PYTHONPATH" not in environment
    assert observed["transaction_committed"] is True


def _fake_factory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> baseline.OwnerBuyE3MechanicsBaseline:
    journal = {
        "enabled": True,
        "required_mount": "/host",
        "root": "/host/orders",
        "prospective_epoch_root": "/host/epochs",
        "remote_spool_allowlisted_roots": ["/host"],
    }
    predecessor = {"version": "v12", "lifecycle_journal_v2": dict(journal)}
    active = {"version": "active", "lifecycle_journal_v2": dict(journal)}
    predecessor_bytes = b"version: v12\n"
    active_bytes = b"version: active\n"

    def secure(path: Path, **_kwargs: object) -> baseline._Snapshot:
        data = active_bytes if Path(path).name.startswith("active") else predecessor_bytes
        return baseline._Snapshot(data=data, sha256=_sha(data), size_bytes=len(data))

    monkeypatch.setattr(baseline, "_secure_snapshot", secure)
    monkeypatch.setattr(
        baseline,
        "_parse_strict_yaml",
        lambda data, **_kwargs: active if data == active_bytes else predecessor,
    )
    # Preserve the real recursive helper for parameter-map comparisons.
    real_difference = baseline.__dict__["_mapping_difference_paths"]

    def differences(left: object, right: object, prefix: str = "") -> list[str]:
        if left is predecessor and right is active:
            return list(baseline.SOURCE_CONFIG_DELTA_PATHS)
        return real_difference(left, right, prefix)

    monkeypatch.setattr(baseline, "_mapping_difference_paths", differences)
    monkeypatch.setattr(baseline, "_validate_source_config", lambda _value: None)
    projected = b"{}\n"
    monkeypatch.setattr(
        baseline,
        "_project_host_neutral_config",
        lambda value: (dict(value), projected),
    )
    monkeypatch.setattr(
        baseline,
        "_verify_runtime_sources",
        lambda _root: MappingProxyType({"source.py": "1" * 64}),
    )
    monkeypatch.setattr(
        baseline,
        "_verify_execution_module_origins",
        lambda _root: MappingProxyType({"source.py": "/safe/source.py"}),
    )

    def replay_params(path: Path, _raw: object) -> dict[str, object]:
        result: dict[str, object] = {"stable": 1, "dynamic_fill_hazard_shadow_enabled": True}
        if "active" in path.name:
            result.update(
                {
                    name: True if name.endswith("enabled") else f"active-{index}"
                    for index, name in enumerate(baseline.REPLAY_ABI_FINAL_DELTA_PATHS)
                }
            )
            result["dynamic_fill_hazard_shadow_enabled"] = False
        else:
            result.update(
                {
                    name: False if name.endswith("enabled") else f"v12-{index}"
                    for index, name in enumerate(baseline.REPLAY_ABI_FINAL_DELTA_PATHS)
                }
            )
        return result

    monkeypatch.setattr(baseline, "_load_replay_params", replay_params)
    active_final = baseline._finalized_replay_params(
        replay_params(tmp_path / "active.host_neutral.json", active)
    )
    monkeypatch.setattr(
        baseline,
        "EXACT_ACTIVE_REPLAY_ABI_SHA256",
        baseline.canonical_sha256(active_final),
    )
    monkeypatch.setattr(baseline, "_stage_e3", lambda *_args: SimpleNamespace())
    frozen_bundle = SimpleNamespace(file_sha256=baseline.EXACT_B0_PREDICATE_BUNDLE_FILE_SHA256)
    monkeypatch.setattr(
        baseline,
        "_stage_b0",
        lambda root, _paths: (root / "b0-policy", root / "b0-bundle", frozen_bundle),
    )
    evidence = baseline.ParityEvidenceBinding(
        synthetic_receipts=MappingProxyType({}),
        layer4_contract_file_sha256=baseline.LAYER4_CONTRACT_FILE_SHA256,
        layer4_contract_canonical_sha256=baseline.LAYER4_CONTRACT_CANONICAL_SHA256,
        layer4_final_file_sha256=baseline.LAYER4_FINAL_FILE_SHA256,
        layer4_final_canonical_sha256=baseline.LAYER4_FINAL_CANONICAL_SHA256,
        layer4_day_receipts_sha256=baseline.LAYER4_DAY_RECEIPTS_SHA256,
        formal_e3_mechanics_panel_days=baseline.FORMAL_E3_MECHANICS_DAYS,
    )
    monkeypatch.setattr(baseline, "_validate_parity_evidence", lambda *_args, **_kwargs: evidence)
    return baseline.create_owner_buy_e3_backtest_mechanics_baseline(
        runtime_repository_root=tmp_path,
        predecessor_v12_config_path=tmp_path / "predecessor.yaml",
        active_source_config_path=tmp_path / "active.yaml",
        e3_artifact_paths=baseline.ExactE3ArtifactPaths(
            tmp_path / "manifest", tmp_path / "policy", tmp_path / "predicate"
        ),
        b0_artifact_paths=baseline.ExactB0ArtifactPaths(
            tmp_path / "b0-policy", tmp_path / "b0-bundle"
        ),
        parity_evidence_paths=baseline.ParityEvidencePaths(
            *[tmp_path / f"evidence-{index}" for index in range(6)]
        ),
    )


def test_factory_uses_active_3d846_derived_params_and_no_shadow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = _fake_factory(tmp_path, monkeypatch)
    try:
        assert loaded.base_params["buy_e3_cooldown_policy_enabled"] is True
        assert loaded.base_params["dynamic_fill_hazard_shadow_enabled"] is False
        assert loaded.identity["reduced_support"] is True
        assert loaded.identity["formal_e3_mechanics_panel_day_count"] == 30
        assert all(value is False for value in baseline.PERMISSIONS.values())
    finally:
        loaded.close()


def test_day_factory_installs_fresh_emitter_and_compiled_e3_b0_evaluator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = _fake_factory(tmp_path, monkeypatch)
    emitters: list[object] = []
    evaluators: list[SimpleNamespace] = []
    modes: list[str] = []

    def emitter(*_args: object, **_kwargs: object) -> object:
        value = object()
        emitters.append(value)
        return value

    def evaluator(**kwargs: object) -> SimpleNamespace:
        assert kwargs["b0_policy_path"] == loaded._staged_b0_policy
        assert kwargs["b0_predicate_bundle_path"] == loaded._staged_b0_bundle
        modes.append("compiled")
        value = SimpleNamespace(
            binding_valid=True,
            policy_sha256=baseline.EXACT_E3_FILE_SHA256["policy"],
            predicate_bundle_sha256=baseline.EXACT_E3_FILE_SHA256["predicate_bundle"],
            audit=lambda: {
                "target_side": "BUY",
                "opposite_side_delegates_exact_b0": True,
                "d_plus_1_new_target_assignments_allowed": False,
            },
        )
        evaluators.append(value)
        return value

    monkeypatch.setattr(baseline.replay_adapter, "_build_day_snapshot_emitter", emitter)
    monkeypatch.setattr(
        baseline.replay_adapter,
        "_day_identity_hashes",
        lambda _request: {"config_sha256": "1" * 64},
    )
    monkeypatch.setattr(baseline, "_explicit_path_lockstep_evaluator", evaluator)
    request = SimpleNamespace(utc_day="2026-06-27")
    replay = SimpleNamespace(
        utc_day="2026-06-27",
        continuation_day="2026-06-28",
        params={},
        trades=[1, 2, 3],
    )
    try:
        first = loaded.build_day_overlay(request, replay, utc_day="2026-06-27")
        second = loaded.build_day_overlay(request, replay, utc_day="2026-06-27")
        assert first.snapshot_emitter is not second.snapshot_emitter
        assert first.compiled_evaluator is not second.compiled_evaluator
        assert modes == ["compiled", "compiled"]
        assert first.target_start_ns == 1_782_518_400_000_000_000
        assert first.target_cutoff_ns == first.target_start_ns + 86_400_000_000_000
        assert first.receipt["d_plus_1_exact_b0_washout"] is True
        assert first.receipt["sell_delegates_exact_b0"] is True
        assert first.receipt["restart_requires_complete_day_replay"] is True
        assert first.params["cooldown_v2_snapshot_emitter"] is first.snapshot_emitter
        assert first.params["cooldown_duration_policy_evaluator"] is first.compiled_evaluator
    finally:
        loaded.close()


def test_day_factory_rejects_nonformal_day_and_preinstalled_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = _fake_factory(tmp_path, monkeypatch)
    try:
        with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="outside"):
            loaded.build_day_overlay(
                SimpleNamespace(utc_day="2026-06-29"),
                SimpleNamespace(
                    utc_day="2026-06-29",
                    continuation_day="2026-06-30",
                    params={},
                ),
                utc_day="2026-06-29",
            )
        with pytest.raises(baseline.OwnerBuyE3MechanicsBaselineError, match="already"):
            loaded.build_day_overlay(
                SimpleNamespace(utc_day="2026-06-27"),
                SimpleNamespace(
                    utc_day="2026-06-27",
                    continuation_day="2026-06-28",
                    params={"cooldown_v2_snapshot_emitter": object()},
                ),
                utc_day="2026-06-27",
            )
    finally:
        loaded.close()


def test_public_contract_is_canonical_private_unavailable_and_authority_false() -> None:
    path = (
        Path(__file__).resolve().parents[1] / "research/families/f05_fill_quality_quote_ev/docs/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "backtest_mechanics_baseline_v1_20260825.json"
    )
    document = json.loads(path.read_text(encoding="ascii"))
    assert _sha(path.read_bytes()) == baseline.V14_PUBLIC_CONTRACT_FILE_SHA256
    assert document["canonical_contract_sha256"] == baseline.document_sha256(
        document, "canonical_contract_sha256"
    )
    assert document["factory"]["private_input_availability"] == "private_not_distributed"
    assert document["support"]["reduced_support"] is True
    assert document["support"]["day_count"] == 30
    assert all(value is False for value in document["permissions"].values())
    text = path.read_text(encoding="ascii")
    assert "/" + "Users/" not in text
    assert "/" + "Volumes/" not in text
    assert "terminal_mtm_pnl" not in text


def test_v14_governance_is_sequential_mechanics_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    predecessor = {
        "schema_version": baseline.V13_SCHEMA_VERSION,
        "baseline_id": baseline.V13_BASELINE_ID,
        "effective_at_utc": baseline.V13_EFFECTIVE_AT_UTC,
        "operational_status": baseline.V13_OPERATIONAL_STATUS,
        "promotion_class": baseline.V13_PROMOTION_CLASS,
        "permissions": dict(baseline.V13_PERMISSIONS),
        "current_live": {
            "config": {"sha256": baseline.ACTIVE_SOURCE_CONFIG_FILE_SHA256},
            "buy_e3_enabled": True,
            "economic_outcomes_read": False,
            "economic_values_persisted": False,
            "private_release_and_evidence_chain_remain_authority": True,
        },
        "backtest_default": {
            "config_sha256": baseline.PREDECESSOR_V12_CONFIG_FILE_SHA256,
            "exact_buy_e3_replay_baseline_available": False,
            "current_live_config_may_replace_backtest_default": False,
            "current_live_evidence_is_backtest_economic_authority": False,
        },
    }
    predecessor_bytes = json.dumps(predecessor, sort_keys=True).encode("ascii")
    predecessor_path = tmp_path / "v13.json"
    predecessor_path.write_bytes(predecessor_bytes)
    predecessor_path.chmod(0o644)
    monkeypatch.setattr(baseline, "V13_PREDECESSOR_FILE_SHA256", _sha(predecessor_bytes))
    contract_path = (
        Path(__file__).resolve().parents[1] / "research/families/f05_fill_quality_quote_ev/docs/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "backtest_mechanics_baseline_v1_20260825.json"
    )
    contract_sha = _sha(contract_path.read_bytes())
    receipt = baseline.create_v14_mechanics_governance_receipt(
        predecessor_v13_identity_path=predecessor_path,
        predecessor_v13_file_sha256=_sha(predecessor_bytes),
        public_contract_path=contract_path,
        public_contract_file_sha256=contract_sha,
        effective_at_utc=baseline.V14_EFFECTIVE_AT_UTC,
    )
    validated = baseline.validate_v14_mechanics_governance_receipt(
        receipt,
        predecessor_v13_identity_path=predecessor_path,
        predecessor_v13_file_sha256=_sha(predecessor_bytes),
        public_contract_path=contract_path,
        public_contract_file_sha256=contract_sha,
    )
    assert validated["successor_baseline_version"] == "v14"
    assert validated["promotion_class"] == "owner_requested_mechanics_only"
    assert validated["default_arm"]["current_v12_50_day_economic_control_replaced"] is False
    assert validated["permissions"]["backtest_mechanics_available"] is True
    assert validated["permissions"]["backtest_default_arm_resolution_authorized"] is True
    assert validated["permissions"]["economic_authority"] is False
    assert validated["permissions"]["promotion_authority"] is False


def test_v14_governance_rejects_unfrozen_caller_sha(tmp_path: Path) -> None:
    path = tmp_path / "fake-v13.json"
    path.write_text("{}", encoding="ascii")
    path.chmod(0o644)
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="not the frozen identity",
    ):
        baseline._validate_v13_predecessor(path, expected_file_sha256="0" * 64)


def test_v14_governance_rejects_invalid_time_and_extra_fields(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _v14_publication_fixture(tmp_path, monkeypatch)
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="canonical UTC-Z|precedes|frozen sequential",
    ):
        baseline.create_v14_mechanics_governance_receipt(
            predecessor_v13_identity_path=fixture.predecessor_path,
            predecessor_v13_file_sha256=fixture.predecessor_sha,
            public_contract_path=fixture.contract_path,
            public_contract_file_sha256=fixture.contract_sha,
            effective_at_utc="2026-08-24T23:59:59Z",
        )
    amended = dict(fixture.receipt)
    amended["unexpected"] = False
    amended["canonical_governance_receipt_sha256"] = baseline.document_sha256(
        amended, "canonical_governance_receipt_sha256"
    )
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="extra or missing fields",
    ):
        baseline.validate_v14_mechanics_governance_receipt(
            amended,
            predecessor_v13_identity_path=fixture.predecessor_path,
            predecessor_v13_file_sha256=fixture.predecessor_sha,
            public_contract_path=fixture.contract_path,
            public_contract_file_sha256=fixture.contract_sha,
        )


def _publish(fixture: SimpleNamespace) -> dict[str, object]:
    return dict(
        baseline.publish_v14_private_bundle(
            runtime_repository_root=fixture.runtime_root,
            durable_evidence_root=fixture.private_root,
            metadata_repository_root=fixture.metadata_root,
            relative_locators=fixture.relative_locators,
            predecessor_v13_identity_path=fixture.predecessor_path,
            public_contract_path=fixture.contract_path,
            cold_repository_root=fixture.runtime_root,
        )
    )


def test_v14_private_bundle_publish_validate_and_idempotence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _v14_publication_fixture(tmp_path, monkeypatch)
    first = _publish(fixture)
    second = _publish(fixture)
    validated = baseline.validate_v14_private_bundle(
        runtime_repository_root=fixture.runtime_root,
        durable_evidence_root=fixture.private_root,
        metadata_repository_root=fixture.metadata_root,
        predecessor_v13_identity_path=fixture.predecessor_path,
        public_contract_path=fixture.contract_path,
        cold_repository_root=fixture.runtime_root,
    )
    assert first == second == validated
    assert first["status"] == "current_default_buy_e3_mechanics_bundle_complete"
    assert first["permissions"]["backtest_default_arm_resolution_authorized"] is True
    assert fixture.destination.stat().st_mode & 0o777 == 0o700
    assert {path.name for path in fixture.destination.iterdir()} == {
        "transaction.json",
        "owner_private_inputs.json",
        "config.host_neutral.replay_projection.json",
        "loaded_capability_receipt.json",
        "v14_mechanics_governance_receipt.json",
        "manifest.json",
    }
    for path in fixture.destination.iterdir():
        metadata = path.stat()
        assert metadata.st_mode & 0o777 == 0o600
        assert metadata.st_nlink == 1
        assert metadata.st_uid == os.getuid()


@pytest.mark.parametrize("failure_name", baseline._DEFAULT_BUNDLE_FILE_NAMES)
def test_v14_private_bundle_recovers_every_exact_crash_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_name: str,
) -> None:
    fixture = _v14_publication_fixture(tmp_path, monkeypatch)
    seen: list[str] = []

    def fail_after(name: str) -> None:
        seen.append(name)
        if name == failure_name:
            raise RuntimeError("simulated crash")

    with pytest.raises(RuntimeError, match="simulated crash"):
        baseline.publish_v14_private_bundle(
            runtime_repository_root=fixture.runtime_root,
            durable_evidence_root=fixture.private_root,
            metadata_repository_root=fixture.metadata_root,
            relative_locators=fixture.relative_locators,
            predecessor_v13_identity_path=fixture.predecessor_path,
            public_contract_path=fixture.contract_path,
            cold_repository_root=fixture.runtime_root,
            _failure_hook=fail_after,
        )
    assert failure_name in seen
    assert _publish(fixture)["status"] == ("current_default_buy_e3_mechanics_bundle_complete")


def test_v14_private_bundle_recovers_precreated_empty_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _v14_publication_fixture(tmp_path, monkeypatch)
    fixture.destination.mkdir(mode=0o700)
    assert _publish(fixture)["status"] == ("current_default_buy_e3_mechanics_bundle_complete")


def test_v14_private_bundle_recovers_manifest_final_pending_hardlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _v14_publication_fixture(tmp_path, monkeypatch)
    first = _publish(fixture)
    manifest = fixture.destination / "manifest.json"
    pending = fixture.destination / f".manifest.json.pending-{_sha(manifest.read_bytes())}"
    os.link(manifest, pending)
    assert manifest.stat().st_nlink == 2
    assert _publish(fixture) == first
    assert manifest.stat().st_nlink == 1
    assert not pending.exists()


def test_v14_private_bundle_rejects_conflict_symlink_and_unsafe_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _v14_publication_fixture(tmp_path, monkeypatch)
    fixture.private_root.chmod(0o777)
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="owner or permissions are unsafe",
    ):
        _publish(fixture)
    fixture.private_root.chmod(0o700)

    real_root = tmp_path / "real-private-root"
    real_root.mkdir(mode=0o700)
    real_root.joinpath(*baseline.FORMAL_PRIVATE_BUNDLE_RELATIVE.parent.parts).mkdir(
        mode=0o700, parents=True
    )
    linked_root = tmp_path / "linked-private-root"
    linked_root.symlink_to(real_root, target_is_directory=True)
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="symlink or non-directory ancestor",
    ):
        baseline.publish_v14_private_bundle(
            runtime_repository_root=fixture.runtime_root,
            durable_evidence_root=linked_root,
            metadata_repository_root=fixture.metadata_root,
            relative_locators=fixture.relative_locators,
            predecessor_v13_identity_path=fixture.predecessor_path,
            public_contract_path=fixture.contract_path,
            cold_repository_root=fixture.runtime_root,
        )

    fixture.destination.mkdir(mode=0o700)
    _private_file(fixture.destination / "transaction.json", b'{"conflict":true}\n')
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="size drifted|SHA256 drifted|content conflict",
    ):
        _publish(fixture)


def test_create_only_install_uses_staging_and_rejects_pending_poison(tmp_path: Path) -> None:
    directory = tmp_path / "bundle"
    directory.mkdir(mode=0o700)
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        data = b"exact private bytes\n"
        digest = _sha(data)
        orphan = directory / f"{baseline._staging_prefix('value.json', digest)}1-dead"
        _private_file(orphan, b"short residue")
        baseline._install_private_file_at(descriptor, "value.json", data)
        assert (directory / "value.json").read_bytes() == data
        assert not orphan.exists()

        poison_data = b"another exact value\n"
        poison_digest = _sha(poison_data)
        pending = directory / f".poison.json.pending-{poison_digest}"
        _private_file(pending, b"wrong deterministic pending")
        with pytest.raises(
            baseline.OwnerBuyE3MechanicsBaselineError,
            match="metadata drifted|size drifted|SHA256 drifted",
        ):
            baseline._install_private_file_at(descriptor, "poison.json", poison_data)
        assert pending.read_bytes() == b"wrong deterministic pending"
        assert not (directory / "poison.json").exists()
    finally:
        os.close(descriptor)


def test_create_only_install_recovers_staging_to_pending_hardlink(tmp_path: Path) -> None:
    directory = tmp_path / "bundle"
    directory.mkdir(mode=0o700)
    data = b"exact recovery bytes\n"
    digest = _sha(data)
    staging = directory / f"{baseline._staging_prefix('value.json', digest)}1-live"
    pending = directory / f".value.json.pending-{digest}"
    _private_file(staging, data)
    os.link(staging, pending)
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        baseline._install_private_file_at(descriptor, "value.json", data)
    finally:
        os.close(descriptor)
    assert (directory / "value.json").read_bytes() == data
    assert not staging.exists()
    assert not pending.exists()


def test_v14_private_bundle_validator_rejects_mode_and_hardlink_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _v14_publication_fixture(tmp_path, monkeypatch)
    _publish(fixture)
    projection = fixture.destination / "config.host_neutral.replay_projection.json"
    projection.chmod(0o644)
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="metadata drifted",
    ):
        baseline.validate_v14_private_bundle(
            runtime_repository_root=fixture.runtime_root,
            durable_evidence_root=fixture.private_root,
            metadata_repository_root=fixture.metadata_root,
            predecessor_v13_identity_path=fixture.predecessor_path,
            public_contract_path=fixture.contract_path,
            cold_repository_root=fixture.runtime_root,
        )
    projection.chmod(0o600)
    os.link(projection, fixture.private_root / "projection-hardlink.json")
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="metadata drifted",
    ):
        baseline.validate_v14_private_bundle(
            runtime_repository_root=fixture.runtime_root,
            durable_evidence_root=fixture.private_root,
            metadata_repository_root=fixture.metadata_root,
            predecessor_v13_identity_path=fixture.predecessor_path,
            public_contract_path=fixture.contract_path,
            cold_repository_root=fixture.runtime_root,
        )


def test_public_publisher_does_not_accept_a_caller_supplied_baseline(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="live exact factory instance",
    ):
        baseline._baseline_capability_receipt(
            SimpleNamespace(_closed=False), private_input_contract={}
        )
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        baseline.publish_v14_private_bundle(  # type: ignore[call-arg]
            runtime_repository_root=tmp_path,
            durable_evidence_root=tmp_path,
            metadata_repository_root=tmp_path,
            relative_locators={},
            predecessor_v13_identity_path=tmp_path,
            public_contract_path=tmp_path,
            cold_repository_root=tmp_path,
            baseline=SimpleNamespace(),
        )


def test_formal_backtest_entry_uses_isolated_default_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from models import backtest_config as shared
    from models import backtest_tick

    monkeypatch.setenv("NARROWGATE_PRIVATE_EVIDENCE_ROOT", str(tmp_path / "private"))
    monkeypatch.setenv("NARROWGATE_METADATA_REPOSITORY_ROOT", str(tmp_path / "metadata"))
    monkeypatch.setattr(shared, "_current_backtest_mechanics_pointer", lambda: {})
    cold = MappingProxyType(
        {
            "execution_commit": "1" * 40,
            "execution_tree": "2" * 40,
            "annotated_tag": baseline.V14_COLD_PUBLISHER_TAG,
            "annotated_tag_object": "3" * 40,
        }
    )
    monkeypatch.setattr(baseline, "capture_cold_publisher", lambda *_args, **_kwargs: cold)
    calls: list[tuple[object, ...]] = []
    payload = {
        "status": "current_default_buy_e3_mechanics_day_complete",
        "utc_day": "2026-06-27",
        "mechanics_receipt": {
            "identity": baseline.IDENTITY,
            "utc_day": "2026-06-27",
            "sell_delegates_exact_b0": True,
            "d_plus_1_exact_b0_washout": True,
        },
        "policy_audit": {
            "target_side_evaluations": 1,
            "b0_delegated_evaluations": 1,
            "d_plus_1_exact_b0_fallback_count": 1,
        },
        "emitter_audit": {"snapshots_emitted": 1},
        "authorities": dict(baseline.PERMISSIONS),
    }

    def run(command: tuple[object, ...], **kwargs: object) -> SimpleNamespace:
        calls.append(command)
        assert kwargs["check"] is True
        assert kwargs["capture_output"] is True
        assert "input" not in kwargs
        assert "NARROWGATE_PRIVATE_EVIDENCE_ROOT" in kwargs["env"]
        return SimpleNamespace(stdout=json.dumps(payload, sort_keys=True).encode("ascii"))

    monkeypatch.setattr(shared.subprocess, "run", run)
    observed = backtest_tick.run_current_default_tick_mechanics_day(utc_day="2026-06-27")
    assert observed["_default_mechanics_isolated_execution"]["fresh_isolated_subprocess"] is True
    assert len(calls) == 1
    assert "-I" in calls[0]
    assert "models/backtest_tick.py" in str(calls[0])
    assert "--day" in calls[0]


def test_formal_default_runner_fails_closed_without_private_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from models import backtest_config as shared

    monkeypatch.setattr(shared, "_current_backtest_mechanics_pointer", lambda: {})
    monkeypatch.delenv("NARROWGATE_PRIVATE_EVIDENCE_ROOT", raising=False)
    monkeypatch.delenv("NARROWGATE_METADATA_REPOSITORY_ROOT", raising=False)
    with pytest.raises(RuntimeError, match="private_not_distributed"):
        shared.run_default_tick_mechanics_day(utc_day="2026-06-27")


def test_formal_default_runner_never_falls_back_after_child_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from models import backtest_config as shared

    monkeypatch.setattr(shared, "_current_backtest_mechanics_pointer", lambda: {})
    monkeypatch.setenv("NARROWGATE_PRIVATE_EVIDENCE_ROOT", str(tmp_path / "private"))
    monkeypatch.setenv("NARROWGATE_METADATA_REPOSITORY_ROOT", str(tmp_path / "metadata"))
    monkeypatch.setattr(
        baseline,
        "capture_cold_publisher",
        lambda *_args, **_kwargs: MappingProxyType({"exact": True}),
    )

    def fail(*args: object, **kwargs: object) -> object:
        raise subprocess.CalledProcessError(31, args[0])

    monkeypatch.setattr(shared.subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="isolated tagged runner"):
        shared.run_default_tick_mechanics_day(utc_day="2026-06-27")


def test_formal_programmatic_runner_rejects_caller_executable_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from models import backtest_config as shared

    executed = False

    class ExecutablePayload:
        def __reduce__(self) -> object:
            nonlocal executed
            executed = True
            return (dict, ())

    with pytest.raises(TypeError, match="positional"):
        shared.run_default_tick_mechanics_day(  # type: ignore[call-arg]
            ExecutablePayload(), utc_day="2026-06-27"
        )
    assert executed is False


def test_existing_backtest_cli_implicit_day_reexecs_current_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from models import backtest_tick

    monkeypatch.setattr(backtest_tick.sys, "argv", ["backtest_tick.py", "--day", "2026-06-27"])
    monkeypatch.delenv("MM_LIVE_CONFIG", raising=False)
    monkeypatch.delenv(backtest_tick._DEFAULT_MECHANICS_COLD_CHILD_ENV, raising=False)
    monkeypatch.setattr(backtest_tick, "_reexec_implicit_current_default_day", lambda: 31)
    monkeypatch.setattr(
        backtest_tick,
        "load_tick_base_params",
        lambda **_kwargs: pytest.fail("legacy B0 default must not load"),
    )
    with pytest.raises(SystemExit) as stopped:
        backtest_tick.main()
    assert stopped.value.code == 31


def test_backtest_tick_direct_script_binds_lexical_checkout_before_conflicting_pythonpath(
    tmp_path: Path,
) -> None:
    repository_root = Path(__file__).absolute().parents[1]
    conflict_root = tmp_path / "conflicting-editable-origin"
    conflict_execution = conflict_root / "execution"
    conflict_execution.mkdir(parents=True)
    (conflict_execution / "__init__.py").write_text(
        'raise RuntimeError("conflicting editable origin imported")\n',
        encoding="ascii",
    )
    conflict_models = conflict_root / "models"
    conflict_models.mkdir(parents=True)
    (conflict_models / "__init__.py").write_text("", encoding="ascii")
    (conflict_models / "symbol_paths.py").write_text(
        'raise RuntimeError("conflicting editable origin imported")\n',
        encoding="ascii",
    )
    bootstrap = (
        "import json,runpy,sys;"
        "runpy.run_path(sys.argv[1],run_name='narrowgate_checkout_origin_probe');"
        "from models import symbol_paths;"
        "from research.families.f05_fill_quality_quote_ev.audit import "
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "backtest_mechanics_baseline_v1 as baseline;"
        "print(json.dumps({'symbol_paths':symbol_paths.__file__,"
        "'baseline':baseline.__file__},sort_keys=True))"
    )
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(conflict_root)
    completed = subprocess.run(
        (
            sys.executable,
            "-B",
            "-c",
            bootstrap,
            str(repository_root / "models/backtest_tick.py"),
        ),
        cwd=tmp_path,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    origins = json.loads(completed.stdout)
    assert Path(origins["symbol_paths"]).absolute() == (repository_root / "models/symbol_paths.py")
    assert Path(origins["baseline"]).absolute() == (
        repository_root / "research/families/f05_fill_quality_quote_ev/audit/"
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "backtest_mechanics_baseline_v1.py"
    )


def test_backtest_tick_lexical_checkout_rejects_symlink_entrypoint(tmp_path: Path) -> None:
    from models import backtest_tick

    models = tmp_path / "checkout/models"
    models.mkdir(parents=True)
    entrypoint = models / "backtest_tick.py"
    entrypoint.symlink_to(Path(backtest_tick.__file__).absolute())
    with pytest.raises(RuntimeError, match="symlink"):
        backtest_tick._lexical_checkout_root(entrypoint)


def test_forged_cold_child_marker_in_normal_process_still_reexecs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from models import backtest_tick

    assert backtest_tick.sys.flags.isolated == 0
    monkeypatch.setattr(backtest_tick.sys, "argv", ["backtest_tick.py", "--day", "2026-06-27"])
    monkeypatch.delenv("MM_LIVE_CONFIG", raising=False)
    monkeypatch.setenv(backtest_tick._DEFAULT_MECHANICS_COLD_CHILD_ENV, "1")
    monkeypatch.setattr(backtest_tick, "_reexec_implicit_current_default_day", lambda: 37)
    monkeypatch.setattr(
        backtest_tick,
        "_run_implicit_current_default_day_in_cold_child",
        lambda _day: pytest.fail("a forged marker must not enter the cold-child branch"),
    )
    with pytest.raises(SystemExit) as stopped:
        backtest_tick.main()
    assert stopped.value.code == 37


@pytest.mark.parametrize(
    "arguments,unsupported",
    [
        (["--day", "2026-06-27", "--engine", "cpp"], "--engine"),
        (["--day", "2026-06-27", "--gamma", "0.05"], "--gamma"),
        (["--day", "2026-06-27", "--symbol", "ETHUSDC"], "--symbol"),
        (["--day", "2026-06-27", "--sweep"], "--sweep"),
        (
            [
                "--start-time",
                "2026-06-27 00:00",
                "--end-time",
                "2026-06-28 00:00",
            ],
            "--end-time",
        ),
    ],
)
def test_implicit_current_default_rejects_every_unsupported_cli_shape(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    unsupported: str,
) -> None:
    from models import backtest_tick

    monkeypatch.setattr(backtest_tick.sys, "argv", ["backtest_tick.py", *arguments])
    monkeypatch.delenv("MM_LIVE_CONFIG", raising=False)
    monkeypatch.delenv(backtest_tick._DEFAULT_MECHANICS_COLD_CHILD_ENV, raising=False)
    monkeypatch.setattr(
        backtest_tick,
        "_reexec_implicit_current_default_day",
        lambda: pytest.fail("unsupported invocation must not execute"),
    )
    with pytest.raises(RuntimeError, match=unsupported):
        backtest_tick.main()


def test_existing_backtest_cli_cold_child_builds_real_overlay_path(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from models import backtest_config as shared
    from models import backtest_tick
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_b0_mechanics_adapter_v1 as b0_projection,
    )
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_panel_builder_v1 as panel_builder,
    )

    calls: list[tuple[object, object, str]] = []

    class Loaded:
        _staged_b0_policy = Path("b0-policy")
        _staged_b0_bundle = Path("b0-bundle")
        _staged_predecessor_source_config = Path("v12-config")

        def run_default_day_replay(
            self, request: object, replay: object, *, utc_day: str
        ) -> Mapping[str, object]:
            calls.append((request, replay, utc_day))
            return {
                "_default_buy_e3_mechanics_receipt": {
                    "identity": baseline.IDENTITY,
                    "utc_day": utc_day,
                    "sell_delegates_exact_b0": True,
                    "d_plus_1_exact_b0_washout": True,
                },
                "_cooldown_duration_policy_audit": {
                    "target_side": "BUY",
                    "opposite_side_delegates_exact_b0": True,
                },
                "_cooldown_v2_snapshot_emitter_audit": {"fresh": True},
                "_default_mechanics_authorities": dict(baseline.PERMISSIONS),
            }

        def close(self) -> None:
            return None

    loaded = Loaded()

    def load_noisy() -> Loaded:
        print("internal diagnostic")
        return loaded

    monkeypatch.setattr(shared, "load_default_tick_mechanics_baseline", load_noisy)
    monkeypatch.setattr(
        panel_builder,
        "_default_cli_paths",
        lambda: {
            name: Path(name)
            for name in (
                "source_manifest",
                "book_view_root",
                "native_observation_manifest",
                "native_observation_root",
                "features_manifest",
            )
        },
    )
    inputs = SimpleNamespace(selected_days=("2026-06-27",))
    monkeypatch.setattr(panel_builder, "validate_inputs", lambda **_kwargs: inputs)
    request = SimpleNamespace(utc_day="2026-06-27")
    replay = SimpleNamespace(utc_day="2026-06-27")
    monkeypatch.setattr(panel_builder, "_day_request", lambda *_args: request)
    monkeypatch.setattr(b0_projection, "_materialize_replay_inputs", lambda _request: replay)
    assert backtest_tick._run_implicit_current_default_day_in_cold_child("2026-06-27") == 0
    assert calls == [(request, replay, "2026-06-27")]
    captured = capsys.readouterr()
    output = json.loads(captured.out)
    assert output["status"] == "current_default_buy_e3_mechanics_day_complete"
    assert output["authorities"] == dict(baseline.PERMISSIONS)
    assert "internal diagnostic" in captured.err


def test_explicit_non_current_arm_label_is_attached_to_machine_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from models import backtest_tick

    monkeypatch.setattr(
        backtest_tick,
        "simulate_tick",
        lambda *_args, **_kwargs: {"pnl": 1.0},
    )
    result = backtest_tick._simulate_tick_with_engine(
        "python",
        object(),
        object(),
        object(),
        {
            "baseline_selection": "explicit_non_current_e3_config_arm",
            "current_e3_mechanics_default": False,
        },
    )
    assert result["baseline_selection"] == "explicit_non_current_e3_config_arm"
    assert result["current_e3_mechanics_default"] is False
    assert result["_baseline_selection_receipt"] == {
        "baseline_selection": "explicit_non_current_e3_config_arm",
        "current_e3_mechanics_default": False,
        "explicit_non_current_arm": True,
    }


@pytest.mark.parametrize(
    "audit_name,count_name",
    [
        ("policy", "target_side_evaluations"),
        ("policy", "b0_delegated_evaluations"),
        ("policy", "d_plus_1_exact_b0_fallback_count"),
        ("emitter", "snapshots_emitted"),
    ],
)
def test_default_day_replay_requires_actual_e3_b0_and_emitter_execution(
    monkeypatch: pytest.MonkeyPatch,
    audit_name: str,
    count_name: str,
) -> None:
    emitter = object()
    evaluator = object()
    overlay = SimpleNamespace(
        params={
            "cooldown_v2_snapshot_emitter": emitter,
            "cooldown_duration_policy_evaluator": evaluator,
        },
        snapshot_emitter=emitter,
        compiled_evaluator=evaluator,
        receipt={"artifact_sha256": baseline.EXACT_E3_ARTIFACT_SHA256},
    )

    class Loaded:
        def build_day_overlay(self, _request: object, _replay: object, *, utc_day: str) -> object:
            assert utc_day == "2026-06-27"
            return overlay

    policy_audit = {
        "policy_sha256": baseline.EXACT_E3_FILE_SHA256["policy"],
        "predicate_bundle_sha256": baseline.EXACT_E3_FILE_SHA256["predicate_bundle"],
        "target_side": "BUY",
        "opposite_side_delegates_exact_b0": True,
        "d_plus_1_new_target_assignments_allowed": False,
        "target_side_evaluations": 1,
        "b0_delegated_evaluations": 1,
        "d_plus_1_exact_b0_fallback_count": 1,
    }
    emitter_audit = {"snapshots_emitted": 1}
    target = policy_audit if audit_name == "policy" else emitter_audit
    target[count_name] = 0
    result = {
        "_cooldown_duration_policy_audit": policy_audit,
        "_cooldown_v2_snapshot_emitter_audit": emitter_audit,
    }
    monkeypatch.setattr(
        baseline.importlib,
        "import_module",
        lambda _name: SimpleNamespace(_simulate_tick_with_engine=lambda *_args, **_kwargs: result),
    )
    replay = SimpleNamespace(
        trades=(),
        var_ts_ms=(),
        var_ssq=(),
        ml_data=None,
        bbo_data=None,
        l2_data=None,
        var_ti=None,
        var_retsq=None,
    )
    with pytest.raises(
        baseline.OwnerBuyE3MechanicsBaselineError,
        match="execution audit drifted",
    ):
        baseline.OwnerBuyE3MechanicsBaseline.run_default_day_replay(
            Loaded(), object(), replay, utc_day="2026-06-27"
        )


def test_default_day_replay_returns_mechanics_only_without_economic_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emitter = object()
    evaluator = object()
    overlay = SimpleNamespace(
        params={
            "cooldown_v2_snapshot_emitter": emitter,
            "cooldown_duration_policy_evaluator": evaluator,
        },
        snapshot_emitter=emitter,
        compiled_evaluator=evaluator,
        receipt={"artifact_sha256": baseline.EXACT_E3_ARTIFACT_SHA256},
    )

    class Loaded:
        def build_day_overlay(self, _request: object, _replay: object, *, utc_day: str) -> object:
            return overlay

    policy_audit = {
        "policy_sha256": baseline.EXACT_E3_FILE_SHA256["policy"],
        "predicate_bundle_sha256": baseline.EXACT_E3_FILE_SHA256["predicate_bundle"],
        "target_side": "BUY",
        "opposite_side_delegates_exact_b0": True,
        "d_plus_1_new_target_assignments_allowed": False,
        "target_side_evaluations": 1,
        "b0_delegated_evaluations": 1,
        "d_plus_1_exact_b0_fallback_count": 1,
    }
    emitter_audit = {"snapshots_emitted": 1}
    monkeypatch.setattr(
        baseline.importlib,
        "import_module",
        lambda _name: SimpleNamespace(
            _simulate_tick_with_engine=lambda *_args, **_kwargs: {
                "_cooldown_duration_policy_audit": policy_audit,
                "_cooldown_v2_snapshot_emitter_audit": emitter_audit,
                "terminal_mtm_pnl": 999.0,
                "markout": 123.0,
            }
        ),
    )
    replay = SimpleNamespace(
        trades=(),
        var_ts_ms=(),
        var_ssq=(),
        ml_data=None,
        bbo_data=None,
        l2_data=None,
        var_ti=None,
        var_retsq=None,
    )
    completed = baseline.OwnerBuyE3MechanicsBaseline.run_default_day_replay(
        Loaded(), object(), replay, utc_day="2026-06-27"
    )
    assert set(completed) == {
        "_default_buy_e3_mechanics_receipt",
        "_cooldown_duration_policy_audit",
        "_cooldown_v2_snapshot_emitter_audit",
        "_default_mechanics_authorities",
    }
    assert all(value is False for value in completed["_default_mechanics_authorities"].values())
