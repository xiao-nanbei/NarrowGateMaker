from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

import pytest
import yaml

from scripts import f05_buy_e3_operational_safety_successor_config as subject


def _config(*, buy_e3: bool) -> dict[str, Any]:
    return {
        "api": {
            "key": "",
            "secret": "",
            "testnet": False,
        },
        "strategy": {
            "max_spread_bps": 20,
            "buy_fill_selection_shadow_enabled": False,
            "dynamic_fill_hazard_shadow_enabled": False,
            "cross_venue_fair_price_shadow_enabled": False,
            "buy_e3_cooldown_policy_enabled": buy_e3,
            "artifact": "unchanged",
        },
        "depth_execution": {"shadow_enabled": False},
        "multi_market": {
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
        },
        "external_venues": {
            "enabled": False,
            "shadow_only": True,
            "sources": [
                {
                    "venue": "test",
                    "enabled": True,
                    "record_enabled": False,
                    "record_trades": True,
                }
            ],
        },
        "logging": {
            "inventory_campaign_shadow_enabled": False,
            "market_tape_enabled": False,
            "market_tape_record_books": True,
            "market_tape_record_trades": True,
        },
        "execution": {"unchanged": True},
    }


def _raw(payload: dict[str, Any]) -> bytes:
    return yaml.safe_dump(payload, sort_keys=False).encode("utf-8")


def _write_private(path: Path, raw: bytes) -> None:
    path.write_bytes(raw)
    path.chmod(0o600)


def _document(raw: bytes, label: str) -> subject._ConfigDocument:  # noqa: SLF001
    return subject._ConfigDocument(  # noqa: SLF001
        payload=subject._parse_yaml(raw, label),  # noqa: SLF001
        raw=raw,
    )


def _patch_frozen_bindings(
    monkeypatch: pytest.MonkeyPatch,
    disabled_raw: bytes,
    active_raw: bytes,
) -> None:
    disabled = _document(disabled_raw, "disabled")
    active = _document(active_raw, "active")
    disabled_successor = subject._successor_document(disabled, "disabled successor")  # noqa: SLF001
    active_successor = subject._successor_document(active, "active successor")  # noqa: SLF001
    monkeypatch.setattr(subject, "PREDECESSOR_DISABLED", subject._binding(disabled))  # noqa: SLF001
    monkeypatch.setattr(subject, "PREDECESSOR_ACTIVE", subject._binding(active))  # noqa: SLF001
    monkeypatch.setattr(
        subject,
        "SUCCESSOR_DISABLED",
        subject._binding(disabled_successor),  # noqa: SLF001
    )
    monkeypatch.setattr(
        subject,
        "SUCCESSOR_ACTIVE",
        subject._binding(active_successor),  # noqa: SLF001
    )


def _inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    disabled: dict[str, Any] | None = None,
    active: dict[str, Any] | None = None,
) -> tuple[Path, Path]:
    disabled_raw = _raw(disabled or _config(buy_e3=False))
    active_raw = _raw(active or _config(buy_e3=True))
    _patch_frozen_bindings(monkeypatch, disabled_raw, active_raw)
    disabled_path = tmp_path / "predecessor.disabled.yaml"
    active_path = tmp_path / "predecessor.active.yaml"
    _write_private(disabled_path, disabled_raw)
    _write_private(active_path, active_raw)
    return disabled_path, active_path


def _finalize(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict[str, Any], Path, Path, Path]:
    disabled, active = _inputs(tmp_path, monkeypatch)
    output = tmp_path / "successor"
    result = subject.finalize_config_pair(
        predecessor_disabled_path=disabled,
        predecessor_active_path=active,
        output_dir=output,
        generated_utc="2026-08-25T10:00:00Z",
    )
    return result, disabled, active, output


def _all_strings(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [item for child in value.values() for item in _all_strings(child)]
    if isinstance(value, list):
        return [item for child in value for item in _all_strings(child)]
    return [value] if isinstance(value, str) else []


def test_frozen_predecessor_and_successor_hashes() -> None:
    assert subject.PREDECESSOR_DISABLED == {
        "file_sha256": "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204",
        "semantic_sha256": "3e8f1c6b829f88ce250896e7ff810c22d3c9102bc4c10f8d9ead883facedc2a8",
        "size_bytes": 27_444,
        "mode": "0600",
    }
    assert subject.PREDECESSOR_ACTIVE["file_sha256"] == (
        "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
    )
    assert subject.SUCCESSOR_DISABLED == {
        "file_sha256": "209435ddfe91efe17c23e32ec36fe0d25633a23f640c2151c520d515465a707b",
        "semantic_sha256": "64496cca76733c88517f6e4f3bc12f2d90bd626547a4648aa5d8fce2439eb85e",
        "size_bytes": 27_497,
        "mode": "0600",
    }
    assert subject.SUCCESSOR_ACTIVE == {
        "file_sha256": "a126eaae9d48d08e7c0621ca298f0216c4ff091c01bfd4da4e8559bd2a74cc39",
        "semantic_sha256": "1fd90dab0c1537fb370f610c6baebd3195659b6e62f46c6edd81fcb640c3d2a4",
        "size_bytes": 27_496,
        "mode": "0600",
    }


def test_build_changes_only_timeout_and_spread_mode_and_preserves_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled_path, active_path = _inputs(tmp_path, monkeypatch)
    build = subject.build_config_pair(
        predecessor_disabled_path=disabled_path,
        predecessor_active_path=active_path,
        generated_utc="2026-08-25T10:00:00Z",
    )
    old_disabled = subject._parse_yaml(disabled_path.read_bytes(), "old disabled")  # noqa: SLF001
    old_active = subject._parse_yaml(active_path.read_bytes(), "old active")  # noqa: SLF001
    disabled = subject._parse_yaml(build.disabled_raw, "disabled")  # noqa: SLF001
    active = subject._parse_yaml(build.active_raw, "active")  # noqa: SLF001
    assert subject._leaf_diff(old_disabled, disabled) == list(subject.SUCCESSOR_CHANGES)  # noqa: SLF001
    assert subject._leaf_diff(old_active, active) == list(subject.SUCCESSOR_CHANGES)  # noqa: SLF001
    assert subject._leaf_diff(disabled, active) == [subject.PAIR_DIFFERENCE]  # noqa: SLF001
    assert disabled["api"]["timeout_s"] == pytest.approx(5.0)
    assert disabled["strategy"]["spread_cap_mode"] == "pause_exposure"
    assert disabled["strategy"]["buy_e3_cooldown_policy_enabled"] is False
    assert active["strategy"]["buy_e3_cooldown_policy_enabled"] is True
    assert disabled["external_venues"]["sources"][0]["record_enabled"] is False
    assert disabled["external_venues"]["sources"][0]["record_trades"] is True
    assert disabled["logging"]["market_tape_record_books"] is True
    assert build.receipt[subject.CANONICAL_FIELD] == subject.canonical_sha256(
        build.receipt, subject.CANONICAL_FIELD
    )
    assert all(
        not value.startswith(("/", "~/", "file://", "ssh://"))
        for value in _all_strings(build.receipt)
    )


def test_finalize_is_0600_receipt_last_valid_and_idempotent_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, disabled, active, output = _finalize(tmp_path, monkeypatch)
    assert first["publication_semantics"] == {
        "disabled": "first_writer",
        "active": "first_writer",
        "receipt": "first_writer",
    }
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    for filename in subject.PUBLICATION_ORDER:
        info = (output / filename).stat()
        assert stat.S_IMODE(info.st_mode) == 0o600
        assert info.st_nlink == 1
    validated = subject.validate_config_pair(
        predecessor_disabled_path=disabled,
        predecessor_active_path=active,
        output_dir=output,
    )
    assert validated["receipt"] == first["receipt"]
    second = subject.finalize_config_pair(
        predecessor_disabled_path=disabled,
        predecessor_active_path=active,
        output_dir=output,
        generated_utc="2026-08-25T10:00:00Z",
    )
    assert set(second["publication_semantics"].values()) == {"exact_existing_reused"}


def test_output_mutation_is_rejected_without_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _first, disabled, active, output = _finalize(tmp_path, monkeypatch)
    active_output = output / subject.ACTIVE_FILENAME
    mutated = active_output.read_bytes().replace(b"pause_exposure", b"compress")
    _write_private(active_output, mutated)
    with pytest.raises(subject.OperationalSafetyConfigError, match="mutated"):
        subject.validate_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=output,
        )
    with pytest.raises(subject.OperationalSafetyConfigError, match="create-only conflict"):
        subject.finalize_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=output,
            generated_utc="2026-08-25T10:00:00Z",
        )
    assert active_output.read_bytes() == mutated


def test_duplicate_yaml_key_fails_closed_before_any_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled, active = _inputs(tmp_path, monkeypatch)
    duplicate = disabled.read_bytes() + b"api:\n  testnet: false\n"
    _write_private(disabled, duplicate)
    monkeypatch.setattr(
        subject,
        "PREDECESSOR_DISABLED",
        {
            "file_sha256": subject.file_sha256_bytes(duplicate),
            "semantic_sha256": "unused",
            "size_bytes": len(duplicate),
            "mode": "0600",
        },
    )
    output = tmp_path / "successor"
    with pytest.raises(subject.OperationalSafetyConfigError, match="duplicate YAML key"):
        subject.finalize_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=output,
        )
    assert not output.exists()


def test_predecessor_extra_difference_and_collection_activation_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active_payload = _config(buy_e3=True)
    active_payload["execution"]["unchanged"] = False
    disabled, active = _inputs(
        tmp_path,
        monkeypatch,
        active=active_payload,
    )
    with pytest.raises(subject.OperationalSafetyConfigError, match="outside BUY E3"):
        subject.build_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
        )

    disabled_payload = _config(buy_e3=False)
    active_payload = _config(buy_e3=True)
    disabled_payload["external_venues"]["sources"][0]["record_enabled"] = True
    active_payload["external_venues"]["sources"][0]["record_enabled"] = True
    second = tmp_path / "second"
    second.mkdir()
    disabled_raw = _raw(disabled_payload)
    active_raw = _raw(active_payload)
    disabled = second / "predecessor.disabled.yaml"
    active = second / "predecessor.active.yaml"
    _write_private(disabled, disabled_raw)
    _write_private(active, active_raw)
    monkeypatch.setattr(
        subject,
        "PREDECESSOR_DISABLED",
        subject._binding(_document(disabled_raw, "disabled")),  # noqa: SLF001
    )
    monkeypatch.setattr(
        subject,
        "PREDECESSOR_ACTIVE",
        subject._binding(_document(active_raw, "active")),  # noqa: SLF001
    )
    with pytest.raises(subject.OperationalSafetyConfigError, match="activation flag"):
        subject.build_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
        )


def test_create_only_conflict_is_preserved_and_publishes_no_suffix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled, active = _inputs(tmp_path, monkeypatch)
    output = tmp_path / "successor"
    output.mkdir(mode=0o700)
    conflict = output / subject.DISABLED_FILENAME
    _write_private(conflict, b"reserved\n")
    with pytest.raises(subject.OperationalSafetyConfigError, match="create-only conflict"):
        subject.finalize_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=output,
            generated_utc="2026-08-25T10:00:00Z",
        )
    assert conflict.read_bytes() == b"reserved\n"
    assert not (output / subject.ACTIVE_FILENAME).exists()
    assert not (output / subject.RECEIPT_FILENAME).exists()


def test_exact_pending_prefix_recovers_but_wrong_pending_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled, active = _inputs(tmp_path, monkeypatch)
    build = subject.build_config_pair(
        predecessor_disabled_path=disabled,
        predecessor_active_path=active,
        generated_utc="2026-08-25T10:00:00Z",
    )
    output = tmp_path / "successor"
    output.mkdir(mode=0o700)
    disabled_final = output / subject.DISABLED_FILENAME
    pending = subject._pending_path(disabled_final, build.disabled_raw)  # noqa: SLF001
    _write_private(pending, build.disabled_raw)
    result = subject.finalize_config_pair(
        predecessor_disabled_path=disabled,
        predecessor_active_path=active,
        output_dir=output,
        generated_utc="2026-08-25T10:00:00Z",
    )
    assert result["publication_semantics"]["disabled"] == "exact_pending_recovered"
    assert not pending.exists()
    assert disabled_final.read_bytes() == build.disabled_raw

    wrong_root = tmp_path / "wrong"
    wrong_root.mkdir()
    wrong_disabled, wrong_active = _inputs(wrong_root, monkeypatch)
    wrong_output = wrong_root / "successor"
    wrong_output.mkdir(mode=0o700)
    expected_final = wrong_output / subject.DISABLED_FILENAME
    wrong_pending = subject._pending_path(expected_final, build.disabled_raw)  # noqa: SLF001
    _write_private(wrong_pending, b"wrong\n")
    with pytest.raises(subject.OperationalSafetyConfigError, match="pending bytes drifted"):
        subject.finalize_config_pair(
            predecessor_disabled_path=wrong_disabled,
            predecessor_active_path=wrong_active,
            output_dir=wrong_output,
            generated_utc="2026-08-25T10:00:00Z",
        )
    assert wrong_pending.read_bytes() == b"wrong\n"
    assert not expected_final.exists()


def test_out_of_order_recovery_and_interrupted_write_fail_safely(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled, active = _inputs(tmp_path, monkeypatch)
    build = subject.build_config_pair(
        predecessor_disabled_path=disabled,
        predecessor_active_path=active,
        generated_utc="2026-08-25T10:00:00Z",
    )
    output = tmp_path / "successor"
    output.mkdir(mode=0o700)
    _write_private(output / subject.ACTIVE_FILENAME, build.active_raw)
    with pytest.raises(subject.OperationalSafetyConfigError, match="precedes disabled"):
        subject.finalize_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=output,
            generated_utc="2026-08-25T10:00:00Z",
        )
    assert not (output / subject.DISABLED_FILENAME).exists()

    interrupted_root = tmp_path / "interrupted"
    interrupted_root.mkdir()
    interrupted_disabled, interrupted_active = _inputs(interrupted_root, monkeypatch)
    interrupted_output = interrupted_root / "successor"

    def fail_write(_descriptor: int, _raw: bytes) -> None:
        raise OSError("injected write failure")

    monkeypatch.setattr(subject, "_write_all", fail_write)
    with pytest.raises(OSError, match="injected"):
        subject.finalize_config_pair(
            predecessor_disabled_path=interrupted_disabled,
            predecessor_active_path=interrupted_active,
            output_dir=interrupted_output,
            generated_utc="2026-08-25T10:00:00Z",
        )
    assert list(interrupted_output.iterdir()) == []


def test_receipt_tamper_mode_and_absolute_locator_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _first, disabled, active, output = _finalize(tmp_path, monkeypatch)
    receipt_path = output / subject.RECEIPT_FILENAME
    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    payload["semantic_contract"]["absolute"] = "/home/private/config.yaml"
    payload[subject.CANONICAL_FIELD] = subject.canonical_sha256(
        payload, subject.CANONICAL_FIELD
    )
    _write_private(
        receipt_path,
        (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8"),
    )
    with pytest.raises(subject.OperationalSafetyConfigError, match="absolute locator"):
        subject.validate_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=output,
        )
    receipt_path.chmod(0o644)
    with pytest.raises(subject.OperationalSafetyConfigError, match="private file identity"):
        subject.validate_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=output,
        )


def test_output_directory_symlink_and_mode_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled, active = _inputs(tmp_path, monkeypatch)
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    link = tmp_path / "successor"
    os.symlink(target, link)
    with pytest.raises(subject.OperationalSafetyConfigError, match="opened safely"):
        subject.finalize_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=link,
        )
    link.unlink()
    target.chmod(0o755)
    with pytest.raises(subject.OperationalSafetyConfigError, match="owner-private"):
        subject.finalize_config_pair(
            predecessor_disabled_path=disabled,
            predecessor_active_path=active,
            output_dir=target,
        )
