from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts import f05_buy_e3_no_shadow_post_release_config_correction as subject


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _semantic(path: Path) -> str:
    return hashlib.sha256(
        json.dumps(
            yaml.safe_load(path.read_text(encoding="utf-8")),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _write_yaml(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    path.chmod(0o600)
    return path


def _base_config(*, active: bool) -> dict[str, Any]:
    return {
        "strategy": {
            "buy_e3_cooldown_policy_enabled": active,
            "buy_fill_selection_shadow_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "cross_venue_fair_price_shadow_enabled": False,
        },
        "external_venues": {"enabled": False},
        "multi_market": {},
        "depth_execution": {"shadow_enabled": False},
        "logging": {
            "inventory_campaign_shadow_enabled": False,
            "market_tape_enabled": False,
        },
    }


def _config_pair(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path, Path, Path]:
    old_disabled = _write_yaml(tmp_path / "old-disabled.yaml", _base_config(active=False))
    old_active = _write_yaml(tmp_path / "old-active.yaml", _base_config(active=True))
    new_disabled_payload = _base_config(active=False)
    new_active_payload = _base_config(active=True)
    for payload in (new_disabled_payload, new_active_payload):
        payload["multi_market"] = {
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
        }
    new_disabled = _write_yaml(tmp_path / "new-disabled.yaml", new_disabled_payload)
    new_active = _write_yaml(tmp_path / "new-active.yaml", new_active_payload)
    values = {
        "PARENT_DISABLED_SHA256": _sha(old_disabled),
        "PARENT_ACTIVE_SHA256": _sha(old_active),
        "PARENT_DISABLED_SEMANTIC_SHA256": _semantic(old_disabled),
        "PARENT_ACTIVE_SEMANTIC_SHA256": _semantic(old_active),
        "PARENT_DISABLED_SIZE": old_disabled.stat().st_size,
        "PARENT_ACTIVE_SIZE": old_active.stat().st_size,
        "CORRECTED_DISABLED_SHA256": _sha(new_disabled),
        "CORRECTED_ACTIVE_SHA256": _sha(new_active),
        "CORRECTED_DISABLED_SEMANTIC_SHA256": _semantic(new_disabled),
        "CORRECTED_ACTIVE_SEMANTIC_SHA256": _semantic(new_active),
        "CORRECTED_DISABLED_SIZE": new_disabled.stat().st_size,
        "CORRECTED_ACTIVE_SIZE": new_active.stat().st_size,
    }
    for name, value in values.items():
        monkeypatch.setattr(subject, name, value)
    return old_disabled, old_active, new_disabled, new_active


def _supplement_binding() -> dict[str, Any]:
    return {
        "schema_version": subject.release_v3.RUNTIME_SUPPLEMENT_SCHEMA,
        "status": subject.release_v3.RUNTIME_SUPPLEMENT_STATUS,
        "file_sha256": "a" * 64,
        "canonical_field": subject.release_v3.RUNTIME_SUPPLEMENT_CANONICAL_FIELD,
        "canonical_sha256": "b" * 64,
        "size_bytes": 1234,
        "mode": "0600",
    }


def _execution() -> dict[str, str]:
    return {
        "execution_commit": "1" * 40,
        "execution_tree": "2" * 40,
        "annotated_operational_tag": "f05-buy-e3-no-shadow-runtime-test",
        "annotated_operational_tag_object": "3" * 40,
        "tag_peeled_commit": "1" * 40,
    }


def _freeze(monkeypatch: pytest.MonkeyPatch, release_path: Path | None = None) -> None:
    monkeypatch.setattr(subject, "FROZEN_RUNTIME_EXECUTION", _execution())
    monkeypatch.setattr(subject, "FROZEN_RUNTIME_SUPPLEMENT_BINDING", _supplement_binding())
    monkeypatch.setattr(subject, "FROZEN_RELEASE_FILE_SHA256", "4" * 64)
    monkeypatch.setattr(subject, "FROZEN_RELEASE_CANONICAL_SHA256", "5" * 64)
    monkeypatch.setattr(subject, "FROZEN_RELEASE_SIZE_BYTES", 123)
    if release_path is not None:
        monkeypatch.setattr(subject, "FROZEN_RELEASE_FILE_SHA256", _sha(release_path))
        monkeypatch.setattr(subject, "FROZEN_RELEASE_SIZE_BYTES", release_path.stat().st_size)
        payload = json.loads(release_path.read_text(encoding="utf-8"))
        monkeypatch.setattr(
            subject,
            "FROZEN_RELEASE_CANONICAL_SHA256",
            payload[subject.release_v3.CANONICAL_FIELD],
        )


def _release(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    _freeze(monkeypatch)
    payload: dict[str, Any] = {
        field: {}
        for field in subject.release_v3.TOP_LEVEL_FIELDS - {subject.release_v3.CANONICAL_FIELD}
    }
    payload.update(
        {
            "schema_version": subject.release_v3.SCHEMA_VERSION,
            "identity": subject.release_v3.IDENTITY,
            "status": subject.release_v3.STATUS,
            "execution": _execution(),
            "runtime_fix_supplement": _supplement_binding(),
            "no_shadow_runtime_contract": dict(subject.release_v3.NO_SHADOW_RUNTIME_CONTRACT),
            "pending_current_runtime_evidence": dict(
                subject.release_v3.PENDING_CURRENT_RUNTIME_EVIDENCE
            ),
            "research_supported": False,
            "owner_risk_accepted": True,
            "action_authorized": True,
            "live_authorized": True,
        }
    )
    payload[subject.release_v3.CANONICAL_FIELD] = subject._canonical(  # noqa: SLF001
        payload, subject.release_v3.CANONICAL_FIELD
    )
    path = tmp_path / "release-v3.json"
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    path.chmod(0o600)
    _freeze(monkeypatch, path)
    return path


def _collector() -> dict[str, Any]:
    return {
        "repository_root": "/collector",
        "execution_commit": "6" * 40,
        "execution_tree": "7" * 40,
        "annotated_tag": "f05-buy-e3-no-shadow-collector-test",
        "annotated_tag_object": "8" * 40,
        "tag_peeled_commit": "6" * 40,
        "direct_successor_commit_is_ancestor": False,
        "runtime_authority_checkout": False,
    }


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[dict[str, Any], Path]:
    old_disabled, old_active, new_disabled, new_active = _config_pair(tmp_path, monkeypatch)
    release = _release(tmp_path, monkeypatch)
    monkeypatch.setattr(subject, "capture_collector_execution", lambda *_args: _collector())
    payload = subject.build_receipt(
        collector_repository_root=tmp_path,
        collector_annotated_tag="test",
        direct_release_v3_path=release,
        predecessor_disabled_config_path=old_disabled,
        predecessor_active_config_path=old_active,
        corrected_disabled_config_path=new_disabled,
        corrected_active_config_path=new_active,
        generated_utc="2026-08-24T00:00:00Z",
    )
    return payload, release


def _write(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    path.chmod(0o600)
    return path


def test_real_frozen_config_pair_is_exact() -> None:
    paths = (
        Path(
            "/private/tmp/f05-buy-e3-no-external-v4-20260824/"
            "config.direct_owner_disabled.no_external.yaml"
        ),
        Path(
            "/private/tmp/f05-buy-e3-no-external-v4-20260824/"
            "config.direct_owner_active.no_external.yaml"
        ),
        Path("/private/tmp/config.direct_owner_disabled.no_shadow.v2.yaml"),
        Path("/private/tmp/config.direct_owner_active.no_shadow.v2.yaml"),
    )
    if not all(path.is_file() for path in paths):
        pytest.skip("local frozen config fixtures are unavailable")
    predecessor, corrected = subject._validate_pair(  # noqa: SLF001
        predecessor_disabled_path=paths[0],
        predecessor_active_path=paths[1],
        corrected_disabled_path=paths[2],
        corrected_active_path=paths[3],
    )
    assert predecessor["disabled"]["file_sha256"] == subject.PARENT_DISABLED_SHA256
    assert corrected["active"]["file_sha256"] == subject.CORRECTED_ACTIVE_SHA256


def test_frozen_authority_is_exact_release_v3() -> None:
    subject._frozen_authority_ready()  # noqa: SLF001
    assert subject.FROZEN_RUNTIME_EXECUTION == {
        "execution_commit": "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de",
        "execution_tree": "0343bd5586b337385cf2aa0d7a643f5c32b0da77",
        "annotated_operational_tag": "f05-owner-buy-e3-no-shadow-runtime-v3-20260824",
        "annotated_operational_tag_object": "3878ea05252ef8f274b6f74ee7a984431c53b892",
        "tag_peeled_commit": "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de",
    }
    assert subject.FROZEN_RUNTIME_SUPPLEMENT_BINDING == {
        "schema_version": "f05_buy_e3_no_global_flow_shadow_runtime_fix_supplement.v1",
        "status": "runtime_no_shadow_fix_verified_no_e3_or_sell_semantic_change",
        "file_sha256": "4dc5a379e927380fe282d8dd5167291f3ca3caba3699dbf4457cedb5e3b4ebb7",
        "canonical_field": "canonical_supplement_sha256",
        "canonical_sha256": "bd157ac169d0158ce19c6caf8e4686faf4b47a8a44a8179039a7223d1484393e",
        "size_bytes": 11_880,
        "mode": "0600",
    }


def test_build_and_portable_validate_round_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload, _release_path = _build(tmp_path, monkeypatch)
    receipt = _write(tmp_path / "correction.json", payload)
    observed, binding = subject.validate_content_receipt(receipt)
    assert observed == payload
    assert set(binding) == subject.CONTENT_BINDING_FIELDS
    assert payload["runtime_authority"]["schema_version"] == subject.release_v3.SCHEMA_VERSION
    assert payload["semantic_diff"]["added_false_paths"] == list(subject.ADDED_FALSE_PATHS)
    assert payload["semantic_diff"]["release_fields_present_in_yaml"] is False


def test_pair_rejects_shadow_true_or_yaml_release_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old_disabled, old_active, new_disabled, new_active = _config_pair(tmp_path, monkeypatch)
    payload = yaml.safe_load(new_active.read_text(encoding="utf-8"))
    payload["multi_market"]["global_flow_shadow_enabled"] = True
    _write_yaml(new_active, payload)
    monkeypatch.setattr(subject, "CORRECTED_ACTIVE_SHA256", _sha(new_active))
    monkeypatch.setattr(subject, "CORRECTED_ACTIVE_SEMANTIC_SHA256", _semantic(new_active))
    monkeypatch.setattr(subject, "CORRECTED_ACTIVE_SIZE", new_active.stat().st_size)
    with pytest.raises(subject.NoShadowConfigCorrectionError, match="pair drifted|did not disable"):
        subject._validate_pair(  # noqa: SLF001
            predecessor_disabled_path=old_disabled,
            predecessor_active_path=old_active,
            corrected_disabled_path=new_disabled,
            corrected_active_path=new_active,
        )

    _config_pair(tmp_path, monkeypatch)
    payload = yaml.safe_load(new_active.read_text(encoding="utf-8"))
    payload["strategy"]["active_release_path"] = "/forbidden/release.json"
    _write_yaml(new_active, payload)
    monkeypatch.setattr(subject, "CORRECTED_ACTIVE_SHA256", _sha(new_active))
    monkeypatch.setattr(subject, "CORRECTED_ACTIVE_SEMANTIC_SHA256", _semantic(new_active))
    monkeypatch.setattr(subject, "CORRECTED_ACTIVE_SIZE", new_active.stat().st_size)
    with pytest.raises(
        subject.NoShadowConfigCorrectionError,
        match="pair drifted|outside two shadow additions|release authority field",
    ):
        subject._validate_pair(  # noqa: SLF001
            predecessor_disabled_path=old_disabled,
            predecessor_active_path=old_active,
            corrected_disabled_path=new_disabled,
            corrected_active_path=new_active,
        )


def test_content_validator_rejects_recanonicalized_authority_or_extra_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload, _release_path = _build(tmp_path, monkeypatch)
    payload["runtime_authority"]["file_sha256"] = "9" * 64
    payload[subject.CANONICAL_FIELD] = subject._canonical(  # noqa: SLF001
        payload, subject.CANONICAL_FIELD
    )
    receipt = _write(tmp_path / "wrong-authority.json", payload)
    with pytest.raises(subject.NoShadowConfigCorrectionError, match="identity drifted"):
        subject.validate_content_receipt(receipt)

    payload, _release_path = _build(tmp_path, monkeypatch)
    payload["unexpected"] = True
    payload[subject.CANONICAL_FIELD] = subject._canonical(  # noqa: SLF001
        payload, subject.CANONICAL_FIELD
    )
    receipt = _write(tmp_path / "extra.json", payload)
    with pytest.raises(subject.NoShadowConfigCorrectionError, match="identity drifted"):
        subject.validate_content_receipt(receipt)


def test_create_only_writer_rejects_existing_path(tmp_path: Path) -> None:
    target = tmp_path / "reserved.json"
    target.write_text("reserved\n", encoding="ascii")
    with pytest.raises(FileExistsError):
        subject._write_exclusive(target, {"status": "must-not-replace"})  # noqa: SLF001
    assert os.stat(target).st_nlink == 1
