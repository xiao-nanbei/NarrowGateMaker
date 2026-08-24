from __future__ import annotations

import hashlib
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from scripts import f05_buy_e3_active_release as legacy_release
from scripts import f05_buy_e3_direct_owner_release_v3 as subject
from scripts import f05_buy_e3_lifecycle_reject_fix_supplement as lifecycle_supplement
from strategy import boolean_cooldown_buy_e3 as runtime

MANIFEST_FILE_SHA256 = "c64f8551268d0aaabab1a17bfc2f184cc576a2570cad3d0efb63fdcbc33c9929"
POLICY_FILE_SHA256 = "ba041dac4f082829f72e9f6838bc50b0c5dce61b24fcb5e1897ef2ac6c2c754b"
PREDICATE_FILE_SHA256 = "4e127745fcc7987fb2eddc3bbf3ceaa19d64251c20ec156bb6d9b5d57edef915"
EXECUTION = {
    "execution_commit": "a" * 40,
    "execution_tree": "b" * 40,
    "annotated_operational_tag": "f05-owner-buy-e3-no-shadow-runtime-v1",
    "annotated_operational_tag_object": "c" * 40,
    "tag_peeled_commit": "a" * 40,
}
SUPPLEMENT_BINDING = {
    "schema_version": subject.RUNTIME_SUPPLEMENT_SCHEMA,
    "status": subject.RUNTIME_SUPPLEMENT_STATUS,
    "file_sha256": "1" * 64,
    "canonical_field": subject.RUNTIME_SUPPLEMENT_CANONICAL_FIELD,
    "canonical_sha256": "2" * 64,
    "size_bytes": 4321,
    "mode": "0600",
}
CHANGED_FILES = {
    "live/main.py": {"git_blob_sha1": "3" * 40, "file_sha256": "4" * 64},
    "strategy/signal.py": {"git_blob_sha1": "5" * 40, "file_sha256": "6" * 64},
}


def _binding(role: str, file_sha256: str, canonical_sha256: str) -> dict[str, Any]:
    contract = legacy_release._CONTRACTS[role]  # noqa: SLF001
    return {
        "role": role,
        "path": f"artifact/{role}-{file_sha256}.json",
        "file_sha256": file_sha256,
        "size_bytes": 100,
        "mode": "0600",
        "device": None,
        "inode": None,
        "schema_version": contract.schema,
        "identity": contract.identity,
        "status": contract.status,
        "canonical_field": contract.canonical_field,
        "canonical_sha256": canonical_sha256,
    }


def _roles() -> dict[str, dict[str, Any]]:
    return {
        "manifest": _binding(
            "manifest", MANIFEST_FILE_SHA256, subject.EXACT_ARTIFACT_SHA256
        ),
        "policy": _binding("policy", POLICY_FILE_SHA256, "7" * 64),
        "predicate_bundle": _binding(
            "predicate_bundle", PREDICATE_FILE_SHA256, "8" * 64
        ),
    }


def _config_pair() -> dict[str, Any]:
    return {
        "schema_version": "f05_buy_e3_no_shadow_config_pair.v1",
        "status": "exact_no_shadow_config_pair_frozen",
        "predecessor": {
            "disabled_file_sha256": subject.OLD_DISABLED_CONFIG_SHA256,
            "active_file_sha256": subject.OLD_ACTIVE_CONFIG_SHA256,
        },
        "disabled": {
            "file_sha256": subject.NEW_DISABLED_CONFIG_SHA256,
            "semantic_sha256": subject.NEW_DISABLED_CONFIG_SEMANTIC_SHA256,
            "size_bytes": subject.NEW_DISABLED_CONFIG_SIZE,
            "mode": "0600",
        },
        "active": {
            "file_sha256": subject.NEW_ACTIVE_CONFIG_SHA256,
            "semantic_sha256": subject.NEW_ACTIVE_CONFIG_SEMANTIC_SHA256,
            "size_bytes": subject.NEW_ACTIVE_CONFIG_SIZE,
            "mode": "0600",
        },
        "old_to_new_semantic_additions": list(subject.NEW_CONFIG_ADDITIONS),
        "active_disabled_only_difference": subject.CONFIG_PAIR_DIFFERENCE,
        "required_false_paths": list(subject.REQUIRED_FALSE_CONFIG_PATHS),
        "external_shadow_only_marker_inert": True,
        "release_fields_present_in_yaml": False,
    }


def _payload() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": subject.SCHEMA_VERSION,
        "identity": subject.IDENTITY,
        "status": subject.STATUS,
        "generated_utc": "2026-08-24T00:00:00Z",
        "research_supported": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "action_authorized": True,
        "live_authorized": True,
        "authorization_basis": deepcopy(subject.AUTHORIZATION_BASIS),
        "scope": deepcopy(subject.SCOPE),
        "execution": deepcopy(EXECUTION),
        "parent_runtime_authority": {
            "release": deepcopy(subject.PARENT_RELEASE_V2_BINDING),
            "execution": deepcopy(subject.PARENT_EXECUTION),
        },
        "exact_artifact": {
            "artifact_sha256": subject.EXACT_ARTIFACT_SHA256,
            "roles": _roles(),
        },
        "historical_evidence": deepcopy(subject.HISTORICAL_EVIDENCE),
        "config_pair": _config_pair(),
        "runtime_fix_contract": deepcopy(subject.RUNTIME_FIX_CONTRACT),
        "runtime_fix_supplement": deepcopy(SUPPLEMENT_BINDING),
        "changed_repository_files": deepcopy(CHANGED_FILES),
        "no_shadow_runtime_contract": deepcopy(subject.NO_SHADOW_RUNTIME_CONTRACT),
        "pending_current_runtime_evidence": deepcopy(
            subject.PENDING_CURRENT_RUNTIME_EVIDENCE
        ),
        "rollback": deepcopy(subject.ROLLBACK),
        "evidence_boundary": deepcopy(subject.EVIDENCE_BOUNDARY),
    }
    payload[subject.CANONICAL_FIELD] = legacy_release.document_sha256(
        payload, subject.CANONICAL_FIELD
    )
    return payload


def _runtime_validate(payload: dict[str, Any]) -> dict[str, str]:
    return runtime._validate_active_release(  # noqa: SLF001
        payload,
        expected_canonical_sha256=payload[subject.CANONICAL_FIELD],
        expected_artifact_sha256=subject.EXACT_ARTIFACT_SHA256,
        expected_manifest_file_sha256=MANIFEST_FILE_SHA256,
        expected_policy_file_sha256=POLICY_FILE_SHA256,
        expected_predicate_bundle_file_sha256=PREDICATE_FILE_SHA256,
    )


def _rewrite_canonical(payload: dict[str, Any]) -> None:
    payload[subject.CANONICAL_FIELD] = legacy_release.document_sha256(
        payload, subject.CANONICAL_FIELD
    )


def test_runtime_accepts_exact_v3_release_and_preserves_action_vocabulary() -> None:
    validated = _runtime_validate(_payload())
    assert validated["execution_commit"] == EXECUTION["execution_commit"]
    assert list(runtime.BUY_ACTIONS) == subject.RUNTIME_FIX_CONTRACT["buy_action_vocabulary"]


def test_release_loader_does_not_change_buy_e3_decision_ast() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    parent = subprocess.run(
        (
            "git",
            "show",
            f"{subject.PARENT_EXECUTION['execution_commit']}:strategy/boolean_cooldown_buy_e3.py",
        ),
        cwd=repository_root,
        check=True,
        capture_output=True,
    ).stdout
    current = (repository_root / "strategy/boolean_cooldown_buy_e3.py").read_bytes()
    assert lifecycle_supplement._semantic_ast_hashes(  # noqa: SLF001
        current
    ) == lifecycle_supplement._semantic_ast_hashes(parent)  # noqa: SLF001


@pytest.mark.parametrize(
    ("field", "mutation"),
    [
        ("authorization_basis", {"owner_risk_scope_unchanged": False}),
        ("historical_evidence", {"old_runtime_evidence_reused_for_new_runtime_admission": True}),
        ("pending_current_runtime_evidence", {"resource_gate_complete": True}),
        ("runtime_fix_contract", {"lifecycle_only_v2_contract_reused": True}),
        ("no_shadow_runtime_contract", {"global_flow_evaluator_effective": True}),
        ("evidence_boundary", {"shadow_created": True}),
        ("runtime_fix_supplement", {"canonical_field": "wrong"}),
    ],
)
def test_runtime_rejects_v3_authority_or_shadow_drift(
    field: str, mutation: dict[str, Any]
) -> None:
    payload = _payload()
    payload[field].update(mutation)
    _rewrite_canonical(payload)
    with pytest.raises(ValueError):
        _runtime_validate(payload)


def test_runtime_rejects_config_hash_extra_diff_and_yaml_release_claims() -> None:
    for mutation in (
        {"disabled": {"file_sha256": "f" * 64}},
        {"active_disabled_only_difference": "strategy.gamma"},
        {"release_fields_present_in_yaml": True},
    ):
        payload = _payload()
        if isinstance(mutation.get("disabled"), dict):
            payload["config_pair"]["disabled"].update(mutation["disabled"])
        else:
            payload["config_pair"].update(mutation)
        _rewrite_canonical(payload)
        with pytest.raises(ValueError):
            _runtime_validate(payload)


def _base_config(*, enabled: bool) -> dict[str, Any]:
    return {
        "strategy": {
            "buy_e3_cooldown_policy_enabled": enabled,
            "buy_fill_selection_shadow_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "cross_venue_fair_price_shadow_enabled": False,
        },
        "multi_market": {},
        "external_venues": {"enabled": False, "shadow_only": True},
        "depth_execution": {"shadow_enabled": False},
        "logging": {
            "inventory_campaign_shadow_enabled": False,
            "market_tape_enabled": False,
        },
        "gamma": 1,
    }


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    path.chmod(0o600)


def _patch_config_identities(
    monkeypatch: pytest.MonkeyPatch,
    paths: dict[str, Path],
) -> None:
    for name, path in paths.items():
        monkeypatch.setattr(
            subject,
            {
                "old_disabled": "OLD_DISABLED_CONFIG_SHA256",
                "old_active": "OLD_ACTIVE_CONFIG_SHA256",
                "disabled": "NEW_DISABLED_CONFIG_SHA256",
                "active": "NEW_ACTIVE_CONFIG_SHA256",
            }[name],
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
    monkeypatch.setattr(subject, "NEW_DISABLED_CONFIG_SIZE", paths["disabled"].stat().st_size)
    monkeypatch.setattr(subject, "NEW_ACTIVE_CONFIG_SIZE", paths["active"].stat().st_size)
    monkeypatch.setattr(
        subject,
        "NEW_DISABLED_CONFIG_SEMANTIC_SHA256",
        legacy_release.canonical_sha256(
            yaml.safe_load(paths["disabled"].read_text(encoding="utf-8"))
        ),
    )
    monkeypatch.setattr(
        subject,
        "NEW_ACTIVE_CONFIG_SEMANTIC_SHA256",
        legacy_release.canonical_sha256(
            yaml.safe_load(paths["active"].read_text(encoding="utf-8"))
        ),
    )


def _config_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Path]:
    old_disabled = _base_config(enabled=False)
    old_active = _base_config(enabled=True)
    disabled = deepcopy(old_disabled)
    active = deepcopy(old_active)
    for payload in (disabled, active):
        payload["multi_market"].update(
            {
                "global_flow_shadow_enabled": False,
                "global_reference_shadow_enabled": False,
            }
        )
    paths = {name: tmp_path / f"{name}.yaml" for name in (
        "old_disabled", "old_active", "disabled", "active"
    )}
    for name, payload in (
        ("old_disabled", old_disabled),
        ("old_active", old_active),
        ("disabled", disabled),
        ("active", active),
    ):
        _write_yaml(paths[name], payload)
    _patch_config_identities(monkeypatch, paths)
    return paths


def _load_config_pair(paths: dict[str, Path]) -> dict[str, Any]:
    return subject._config_pair(  # noqa: SLF001
        old_disabled_path=paths["old_disabled"],
        old_active_path=paths["old_active"],
        disabled_path=paths["disabled"],
        active_path=paths["active"],
    )


def test_config_pair_accepts_only_two_false_additions_and_e3_pair_bit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _config_files(tmp_path, monkeypatch)
    pair = _load_config_pair(paths)
    assert pair["old_to_new_semantic_additions"] == list(subject.NEW_CONFIG_ADDITIONS)
    assert pair["active_disabled_only_difference"] == subject.CONFIG_PAIR_DIFFERENCE
    assert pair["release_fields_present_in_yaml"] is False


@pytest.mark.parametrize("mutation", ["extra_diff", "enabled_shadow", "yaml_release"])
def test_config_pair_rejects_tamper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    paths = _config_files(tmp_path, monkeypatch)
    active = yaml.safe_load(paths["active"].read_text(encoding="utf-8"))
    if mutation == "extra_diff":
        active["gamma"] = 2
    elif mutation == "enabled_shadow":
        active["multi_market"]["global_flow_shadow_enabled"] = True
    else:
        active["strategy"]["buy_e3_active_release_file_sha256"] = "f" * 64
    _write_yaml(paths["active"], active)
    monkeypatch.setattr(
        subject,
        "NEW_ACTIVE_CONFIG_SHA256",
        hashlib.sha256(paths["active"].read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(subject, "NEW_ACTIVE_CONFIG_SIZE", paths["active"].stat().st_size)
    monkeypatch.setattr(
        subject,
        "NEW_ACTIVE_CONFIG_SEMANTIC_SHA256",
        legacy_release.canonical_sha256(active),
    )
    with pytest.raises(subject.DirectOwnerReleaseV3Error):
        _load_config_pair(paths)


def test_runtime_supplement_must_bind_execution_changed_map_configs_and_false_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supplement_payload = {
        "parent_execution": deepcopy(subject.PARENT_EXECUTION),
        "execution": deepcopy(EXECUTION),
        "changed_repository_files": deepcopy(CHANGED_FILES),
        "runtime_semantic_contract": {},
        "no_shadow_runtime_contract": {
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
            "global_flow_evaluator_effective": False,
            "global_reference_evaluator_effective": False,
        },
        "config_pair": {
            "disabled_file_sha256": subject.NEW_DISABLED_CONFIG_SHA256,
            "active_file_sha256": subject.NEW_ACTIVE_CONFIG_SHA256,
        },
    }
    module = SimpleNamespace(
        validate_content_receipt=lambda _path: (
            deepcopy(supplement_payload),
            deepcopy(SUPPLEMENT_BINDING),
        )
    )
    monkeypatch.setitem(
        sys.modules,
        "scripts.f05_buy_e3_no_shadow_runtime_fix_supplement",
        module,
    )
    _payload_out, binding = subject._runtime_supplement(  # noqa: SLF001
        Path("supplement.json"),
        execution=EXECUTION,
        config_pair=_config_pair(),
        changed_files=CHANGED_FILES,
    )
    assert binding == SUPPLEMENT_BINDING
    supplement_payload["execution"] = {**EXECUTION, "execution_tree": "f" * 40}
    with pytest.raises(subject.DirectOwnerReleaseV3Error):
        subject._runtime_supplement(  # noqa: SLF001
            Path("supplement.json"),
            execution=EXECUTION,
            config_pair=_config_pair(),
            changed_files=CHANGED_FILES,
        )


def test_builder_inherits_parent_authority_without_claiming_pending_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_payload = {
        "exact_artifact": {
            "artifact_sha256": subject.EXACT_ARTIFACT_SHA256,
            "roles": _roles(),
        }
    }
    monkeypatch.setattr(legacy_release, "_operational_git_identity", lambda *_: EXECUTION)
    monkeypatch.setattr(subject.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(returncode=0))
    monkeypatch.setattr(
        subject,
        "_parent_release",
        lambda _path: (deepcopy(parent_payload), deepcopy(subject.PARENT_RELEASE_V2_BINDING)),
    )
    monkeypatch.setattr(subject, "_artifact", lambda _paths, expected: deepcopy(expected))
    monkeypatch.setattr(subject, "_config_pair", lambda **_kwargs: _config_pair())
    monkeypatch.setattr(
        subject, "_changed_repository_files", lambda *_args: deepcopy(CHANGED_FILES)
    )
    monkeypatch.setattr(
        subject,
        "_runtime_supplement",
        lambda *_args, **_kwargs: ({}, deepcopy(SUPPLEMENT_BINDING)),
    )
    payload = subject.build_direct_owner_release_v3(
        repository_root=Path("/repo"),
        annotated_operational_tag="tag",
        artifact_paths={},
        parent_release_v2_path=Path("parent.json"),
        old_disabled_config_path=Path("old-disabled.yaml"),
        old_active_config_path=Path("old-active.yaml"),
        disabled_config_path=Path("disabled.yaml"),
        active_config_path=Path("active.yaml"),
        runtime_fix_supplement_path=Path("supplement.json"),
        generated_utc="2026-08-24T00:00:00Z",
    )
    assert payload["parent_runtime_authority"]["execution"] == subject.PARENT_EXECUTION
    assert payload["runtime_fix_contract"]["lifecycle_only_v2_contract_reused"] is False
    assert all(value is False for value in payload["pending_current_runtime_evidence"].values())
    assert payload["research_supported"] is False
    assert payload["evidence_boundary"]["shadow_or_companion_collection_enabled"] is False
