from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import models.backtest_config as backtest_config
from models.backtest_config import (
    load_operational_baseline_binding,
    resolve_backtest_config_path,
)
ROOT = Path(__file__).resolve().parents[1]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _set_nested(payload: dict, dotted: str, value: object) -> None:
    target = payload
    parts = dotted.split(".")
    for part in parts[:-1]:
        target = target[part]
    target[parts[-1]] = value


def _write_binding(root: Path) -> tuple[Path, Path]:
    config = root / "docs" / "private" / "live_config.current.local.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("ml:\n  enabled: true\n", encoding="utf-8")
    config.chmod(0o600)

    model = root / "models" / "v12"
    model.mkdir(parents=True)
    bundle_meta = model / "bundle_meta.json"
    training_summary = model / "training_summary.json"
    p3 = model / "fill_prob_params.json"
    bundle_meta.write_text("{}\n", encoding="utf-8")
    training_summary.write_text("{}\n", encoding="utf-8")
    p3.write_text("{}\n", encoding="utf-8")

    identity = (
        root
        / "research"
        / "families"
        / "f10_live_replay_attribution"
        / "docs"
        / "operational_baseline_identity.json"
    )
    identity.parent.mkdir(parents=True)
    identity.write_text(
        json.dumps(
            {
                "baseline_id": "baseline-v12",
                "config": {"sha256": _sha256(config)},
                "model": {
                    "directory": str(model.relative_to(root)),
                    "bundle_meta_sha256": _sha256(bundle_meta),
                    "training_summary_sha256": _sha256(training_summary),
                },
                "p3": {
                    "path": str(p3.relative_to(root)),
                    "sha256": _sha256(p3),
                },
                "permissions": {
                    "operational_baseline_active": True,
                    "baseline_promotion_authorized": True,
                    "backtest_default_control_authorized": True,
                },
            }
        ),
        encoding="utf-8",
    )

    pointer = identity.with_name("operational_baseline_current.json")
    pointer.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_operational_baseline_pointer.v1",
                "baseline_id": "baseline-v12",
                "identity_path": str(identity.relative_to(root)),
                "identity_sha256": _sha256(identity),
                "live_config_path": str(config.relative_to(root)),
                "live_config_sha256": _sha256(config),
                "backtest_control_arm": "v12_ml_on",
                "model_directory": str(model.relative_to(root)),
                "bundle_meta_sha256": _sha256(bundle_meta),
                "backtest_default_control_authorized": True,
            }
        ),
        encoding="utf-8",
    )
    return pointer, config


def _write_split_v2_binding(root: Path) -> tuple[Path, Path, Path]:
    private = root / "docs" / "private"
    private.mkdir(parents=True)
    archive = private / "live_config.backtest_v12.800f4c025663.local.yaml"
    alias = private / "live_config.current.local.yaml"
    archive.write_text("ml:\n  enabled: true\n", encoding="utf-8")
    alias.write_text("ml:\n  enabled: false\n", encoding="utf-8")
    archive.chmod(0o600)
    alias.chmod(0o600)

    model = root / "models" / "v12"
    model.mkdir(parents=True)
    bundle_meta = model / "bundle_meta.json"
    training_summary = model / "training_summary.json"
    p3 = model / "fill_prob_params.json"
    bundle_meta.write_text("{}\n", encoding="utf-8")
    training_summary.write_text("{}\n", encoding="utf-8")
    p3.write_text("{}\n", encoding="utf-8")
    bundle_meta.chmod(0o600)
    training_summary.chmod(0o600)
    p3.chmod(0o600)

    docs = root / "research" / "families" / "f10_live_replay_attribution" / "docs"
    docs.mkdir(parents=True)
    v12 = docs / "operational_baseline_identity_v12.json"
    v12_config = {
        "sha256": _sha256(archive),
        "ml_enabled": True,
        "buy_fill_selection_shadow_enabled": False,
        "buy_fill_selection_live_enabled": False,
        "dynamic_fill_hazard_shadow_enabled": True,
        "dynamic_fill_hazard_action_enabled": False,
        "cross_venue_fair_price_shadow_enabled": False,
        "inventory_campaign_shadow_enabled": False,
        "depth_execution_shadow_enabled": False,
        "depth_imbalance_asymmetry_enabled": True,
        "boolean_cooldown_policy_enabled": True,
        "boolean_cooldown_evidence_route": "owner_risk_accepted_promotion",
        "max_exec_book_visible_age_s": 5.0,
        "max_exec_book_source_lag_s": 5.0,
    }
    v12_policy = {
        "identity": "causal_multichannel_window_boolean_cooldown_owner_policy_v1",
        "policy_sha256": "8" * 64,
        "predicate_bundle_sha256": "9" * 64,
        "owner_override_required": True,
    }
    v12_model = {
        "directory": str(model.relative_to(root)),
        "bundle_meta_sha256": _sha256(bundle_meta),
        "training_summary_sha256": _sha256(training_summary),
    }
    v12_p3 = {
        "path": str(p3.relative_to(root)),
        "sha256": _sha256(p3),
        "schema_version": "test.p3.v1",
    }
    v12.write_text(
        json.dumps(
            {
                "baseline_id": "baseline-v12",
                "research_gate_passed": False,
                "baseline_integrity_gate_passed": True,
                "config": v12_config,
                "f05_boolean_cooldown": v12_policy,
                "model": v12_model,
                "p3": v12_p3,
                "runtime_code": {
                    "deployment_scope": "frozen-v12",
                    "models/backtest_config.py": _sha256(ROOT / "models/backtest_config.py"),
                },
                "permissions": {
                    "operational_baseline_active": True,
                    "baseline_promotion_authorized": True,
                    "backtest_default_control_authorized": True,
                    "owner_risk_accepted_live_authorized": True,
                },
            }
        ),
        encoding="utf-8",
    )
    private_boundary = docs / "operational_baseline_private_boundary.json"
    private_boundary.write_text(
        json.dumps(
            {
                "schema_version": (
                    "narrowgate_operational_baseline_identity.private_boundary.v1"
                ),
                "baseline_id": "governance-private-boundary",
                "effective_at_utc": "2026-08-25T00:00:00Z",
                "operational_status": "private_deployment_not_distributed",
                "promotion_class": "public_backtest_locator_no_live_or_economic_authority",
                "predecessor": {
                    "baseline_id": "baseline-v12",
                    "identity": str(v12.relative_to(root)),
                    "identity_sha256": _sha256(v12),
                    "historical_identity_modified": False,
                    "status": "immutable_backtest_default_control",
                },
                "current_live": {"availability": "private_not_distributed"},
                "backtest_default": {
                    "status": "immutable_backtest_default_control",
                    "identity": str(v12.relative_to(root)),
                    "identity_sha256": _sha256(v12),
                    "config_locator": str(archive.relative_to(root)),
                    "config_sha256": _sha256(archive),
                    "exact_buy_e3_replay_baseline_available": False,
                    "current_live_config_may_replace_backtest_default": False,
                    "current_live_evidence_is_backtest_economic_authority": False,
                    "control_arm": "v12_ml_on",
                    "predecessor_control_arm": "v11",
                    "replay_baseline_path": "",
                    "replay_baseline_sha256": "",
                    "replay_ber_clock_semantics": "",
                    "historical_v12_economic_evidence_reinterpreted": False,
                },
                "config": {
                    "scope": "backtest_default_immutable_v12_control",
                    "canonical_private_source": str(archive.relative_to(root)),
                    "sha256": _sha256(archive),
                },
                "model": v12_model,
                "p3": {**v12_p3, "changed_by_successor": False},
                "permissions": {
                    "operational_baseline_active": True,
                    "governance_locator_publication_authorized": True,
                    "baseline_promotion_authorized": False,
                    "backtest_default_control_authorized": True,
                    "private_release_authority_required": True,
                },
            }
        ),
        encoding="utf-8",
    )
    pointer = docs / "operational_baseline_current.json"
    pointer.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_operational_baseline_pointer.v2",
                "updated_at_utc": "2026-08-25T00:00:00Z",
                "baseline_id": "governance-private-boundary",
                "identity_path": str(private_boundary.relative_to(root)),
                "identity_sha256": _sha256(private_boundary),
                "current_live_binding": {"availability": "private_not_distributed"},
                "backtest_default_binding": {
                    "baseline_id": "baseline-v12",
                    "identity_path": str(v12.relative_to(root)),
                    "identity_sha256": _sha256(v12),
                    "config_path": str(archive.relative_to(root)),
                    "config_sha256": _sha256(archive),
                    "current_live_alias_allowed": False,
                    "exact_buy_e3_replay_baseline_available": False,
                    "current_live_e3_evidence_is_backtest_economic_authority": False,
                    "control_arm": "v12_ml_on",
                    "predecessor_control_arm": "v11",
                    "replay_baseline_path": "",
                    "replay_baseline_sha256": "",
                    "replay_ber_clock_semantics": "",
                },
                "backtest_default_flat_projection": {
                    "model_directory": str(model.relative_to(root)),
                    "bundle_meta_sha256": _sha256(bundle_meta),
                    "ml_enabled": True,
                    "buy_fill_selection_shadow_enabled": False,
                    "dynamic_fill_hazard_action_enabled": False,
                    "buy_fill_selection_live_enabled": False,
                    "dynamic_fill_hazard_shadow_enabled": True,
                    "cross_venue_fair_price_shadow_enabled": False,
                    "inventory_campaign_shadow_enabled": False,
                    "depth_execution_shadow_enabled": False,
                    "depth_imbalance_asymmetry_enabled": True,
                    "boolean_cooldown_policy_enabled": True,
                    "boolean_cooldown_policy_identity": v12_policy["identity"],
                    "boolean_cooldown_policy_sha256": v12_policy["policy_sha256"],
                    "boolean_cooldown_predicate_bundle_sha256": v12_policy[
                        "predicate_bundle_sha256"
                    ],
                    "boolean_cooldown_evidence_route": ("owner_risk_accepted_promotion"),
                    "boolean_cooldown_research_hard_gates_passed": False,
                    "boolean_cooldown_owner_risk_accepted": True,
                    "boolean_cooldown_owner_live_authorized": True,
                    "q90_action_status": "suspended_terminal_active_riskset_integrity",
                    "quote_snapshot_atomicity_contract": "v2",
                    "max_exec_book_visible_age_s": 5.0,
                    "max_exec_book_source_lag_s": 5.0,
                    "baseline_integrity_gate_passed": True,
                },
                "backtest_default_control_authorized": True,
                "backtest_default_control_scope": "immutable_backtest_control",
                "historical_v12_identity_modified": False,
                "no_cross_layer_substitution": True,
            }
        ),
        encoding="utf-8",
    )
    return pointer, archive, alias


def test_current_baseline_pointer_resolves_hash_bound_config(tmp_path: Path) -> None:
    pointer, config = _write_binding(tmp_path)

    binding = load_operational_baseline_binding(
        root=tmp_path,
        pointer_path=pointer,
    )

    assert binding is not None
    assert binding["config_exists"] is True
    assert binding["pointer"]["backtest_control_arm"] == "v12_ml_on"
    assert (
        resolve_backtest_config_path(
            root=tmp_path,
            pointer_path=pointer,
        )
        == config.resolve()
    )


def test_current_baseline_config_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    pointer, config = _write_binding(tmp_path)
    config.write_text("ml:\n  enabled: false\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="config SHA256 mismatch"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_public_checkout_without_private_config_uses_template(tmp_path: Path) -> None:
    pointer, config = _write_binding(tmp_path)
    config.unlink()
    public = tmp_path / "live" / "config.yaml"
    public.parent.mkdir(parents=True)
    public.write_text("ml:\n  enabled: false\n", encoding="utf-8")

    assert (
        resolve_backtest_config_path(
            root=tmp_path,
            pointer_path=pointer,
        )
        == public.resolve()
    )


def test_remote_candidate_with_same_hash_is_current_baseline(tmp_path: Path) -> None:
    pointer, config = _write_binding(tmp_path)
    remote = tmp_path / "live" / "config.yaml"
    remote.parent.mkdir(parents=True)
    remote.write_bytes(config.read_bytes())
    config.unlink()
    payload = json.loads(pointer.read_text(encoding="utf-8"))
    payload["live_config_candidates"] = [
        "docs/private/live_config.current.local.yaml",
        "live/config.yaml",
    ]
    pointer.write_text(json.dumps(payload), encoding="utf-8")

    binding = load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)

    assert binding is not None
    assert binding["config_exists"] is True
    assert binding["config_path"] == remote.resolve()


def test_runtime_code_hashes_distinguish_exact_baseline_from_overlay(
    tmp_path: Path,
) -> None:
    pointer, _ = _write_binding(tmp_path)
    runtime = tmp_path / "live" / "main.py"
    runtime.parent.mkdir(parents=True, exist_ok=True)
    runtime.write_text("DEPLOYED = True\n", encoding="utf-8")

    pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
    identity = tmp_path / pointer_payload["identity_path"]
    identity_payload = json.loads(identity.read_text(encoding="utf-8"))
    identity_payload["runtime_code"] = {
        "deployment_scope": "test",
        "live/main.py": _sha256(runtime),
    }
    identity.write_text(json.dumps(identity_payload), encoding="utf-8")
    pointer_payload["identity_sha256"] = _sha256(identity)
    pointer.write_text(json.dumps(pointer_payload), encoding="utf-8")

    exact = load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)
    assert exact is not None
    assert exact["runtime_code_audit"]["matches"] is True

    runtime.write_text("DEPLOYED = False\n", encoding="utf-8")
    overlay = load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)
    assert overlay is not None
    assert overlay["runtime_code_audit"]["matches"] is False
    assert list(overlay["runtime_code_audit"]["mismatched_paths"]) == ["live/main.py"]


def test_v2_split_binding_never_uses_current_live_alias_for_backtest(
    tmp_path: Path,
) -> None:
    pointer, archive, alias = _write_split_v2_binding(tmp_path)

    binding = load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)

    assert binding is not None
    assert binding["config_path"] == archive.resolve()
    assert binding["config_path"] != alias.resolve()
    assert binding["config_scope"] == "immutable_v12_backtest_default"
    assert binding["pointer"]["baseline_id"] == "baseline-v12"
    assert binding["governance_pointer"]["baseline_id"] == "governance-private-boundary"
    assert binding["identity"]["baseline_id"] == "baseline-v12"
    assert binding["current_live_identity"]["baseline_id"] == "governance-private-boundary"
    assert binding["runtime_code_audit"]["declared"] is True
    assert resolve_backtest_config_path(root=tmp_path, pointer_path=pointer) == archive.resolve()


def test_v2_corrupt_archive_fails_closed_even_when_live_alias_is_valid(
    tmp_path: Path,
) -> None:
    pointer, archive, _alias = _write_split_v2_binding(tmp_path)
    archive.write_text("drift: true\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="config SHA256 mismatch"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_v2_missing_archive_fails_closed_in_owner_checkout(tmp_path: Path) -> None:
    pointer, archive, alias = _write_split_v2_binding(tmp_path)
    archive.unlink()
    public = tmp_path / "live" / "config.yaml"
    public.parent.mkdir(parents=True)
    public.write_text("public: true\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="Immutable v12 backtest config is missing"):
        resolve_backtest_config_path(root=tmp_path, pointer_path=pointer)
    assert alias.is_file()


def test_v2_public_clone_without_private_root_uses_template(tmp_path: Path) -> None:
    pointer, archive, alias = _write_split_v2_binding(tmp_path)
    archive.unlink()
    alias.unlink()
    archive.parent.rmdir()
    public = tmp_path / "live" / "config.yaml"
    public.parent.mkdir(parents=True)
    public.write_text("public: true\n", encoding="utf-8")

    assert resolve_backtest_config_path(root=tmp_path, pointer_path=pointer) == public.resolve()


@pytest.mark.parametrize(
    "raw",
    [
        '{"schema_version":"one","schema_version":"two"}\n',
        '{"nested":{"authority":false,"authority":true}}\n',
    ],
)
def test_operational_pointer_rejects_duplicate_json_keys(
    tmp_path: Path,
    raw: str,
) -> None:
    pointer, _archive, _alias = _write_split_v2_binding(tmp_path)
    pointer.write_text(raw, encoding="utf-8")
    with pytest.raises(RuntimeError, match="Duplicate JSON key"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_operational_identity_and_replay_reject_duplicate_json_keys(
    tmp_path: Path,
) -> None:
    pointer, _archive, _alias = _write_split_v2_binding(tmp_path)
    pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
    identity = tmp_path / pointer_payload["identity_path"]
    duplicate_identity = b'{"baseline_id":"one","baseline_id":"two"}\n'
    identity.write_bytes(duplicate_identity)
    pointer_payload["identity_sha256"] = hashlib.sha256(duplicate_identity).hexdigest()
    pointer.write_text(json.dumps(pointer_payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="Duplicate JSON key"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)

    pointer, _config = _write_binding(tmp_path / "replay")
    replay = tmp_path / "replay" / "research" / "replay.json"
    replay.parent.mkdir(parents=True, exist_ok=True)
    replay_raw = b'{"permissions":{},"permissions":{}}\n'
    replay.write_bytes(replay_raw)
    pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
    pointer_payload["backtest_replay_baseline_path"] = str(replay.relative_to(tmp_path / "replay"))
    pointer_payload["backtest_replay_baseline_sha256"] = hashlib.sha256(replay_raw).hexdigest()
    pointer.write_text(json.dumps(pointer_payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="Duplicate JSON key"):
        load_operational_baseline_binding(root=tmp_path / "replay", pointer_path=pointer)


@pytest.mark.parametrize("target_kind", ["pointer", "identity", "config"])
def test_operational_authority_rejects_final_symlinks(
    tmp_path: Path,
    target_kind: str,
) -> None:
    pointer, archive, _alias = _write_split_v2_binding(tmp_path)
    pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
    targets = {
        "pointer": pointer,
        "identity": tmp_path / pointer_payload["identity_path"],
        "config": archive,
    }
    target = targets[target_kind]
    original = target.with_name(f"{target.name}.original")
    target.replace(original)
    target.symlink_to(original)
    with pytest.raises(RuntimeError, match="unsafe|symlink"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_operational_config_rejects_hardlink_identity(tmp_path: Path) -> None:
    pointer, archive, _alias = _write_split_v2_binding(tmp_path)
    archive.with_name(f"{archive.name}.other-link").hardlink_to(archive)
    with pytest.raises(RuntimeError, match="unsafe"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_v2_private_archive_rejects_world_readable_mode(tmp_path: Path) -> None:
    pointer, archive, _alias = _write_split_v2_binding(tmp_path)
    archive.chmod(0o644)
    with pytest.raises(RuntimeError, match="unsafe"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_v2_private_model_authority_rejects_world_readable_mode(tmp_path: Path) -> None:
    pointer, _archive, _alias = _write_split_v2_binding(tmp_path)
    bundle_meta = tmp_path / "models" / "v12" / "bundle_meta.json"
    bundle_meta.chmod(0o644)
    with pytest.raises(RuntimeError, match="unsafe"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_v1_public_model_authority_rejects_private_mode(tmp_path: Path) -> None:
    pointer, _config = _write_binding(tmp_path)
    bundle_meta = tmp_path / "models" / "v12" / "bundle_meta.json"
    bundle_meta.chmod(0o600)
    with pytest.raises(RuntimeError, match="unsafe"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_public_operational_pointer_rejects_private_mode(tmp_path: Path) -> None:
    pointer, _archive, _alias = _write_split_v2_binding(tmp_path)
    pointer.chmod(0o600)
    with pytest.raises(RuntimeError, match="unsafe"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_projection_private_source_requires_owner_only_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    public = tmp_path / "public.json"
    private = tmp_path / "private.json"
    public.write_text("{}\n", encoding="utf-8")
    private.write_text("{}\n", encoding="utf-8")
    public.chmod(0o644)
    private.chmod(0o644)
    raw = public.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    monkeypatch.setattr(
        backtest_config,
        "projection_for",
        lambda *_args, **_kwargs: SimpleNamespace(
            materialized_identity="public_projection",
            public_projection_sha256=digest,
            source_private_sha256=digest,
            private_source_available=True,
            private_source_path=private,
        ),
    )
    with pytest.raises(RuntimeError, match="unsafe"):
        backtest_config._verified_projection_identity(
            public,
            raw,
            public_allowed_modes=backtest_config.PUBLIC_AUTHORITY_MODES,
        )


def test_operational_config_rejects_symlinked_ancestor(tmp_path: Path) -> None:
    pointer, archive, _alias = _write_split_v2_binding(tmp_path)
    private = archive.parent
    real_private = private.with_name("private-real")
    private.replace(real_private)
    private.symlink_to(real_private, target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlinked ancestor"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_pointer_swap_between_snapshot_and_projection_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pointer, _archive, _alias = _write_split_v2_binding(tmp_path)
    original_projection_for = backtest_config.projection_for
    swapped = False

    def swap_then_project(path: Path, **kwargs: object):
        nonlocal swapped
        if not swapped and Path(path) == pointer:
            swapped = True
            pointer.write_text('{"schema_version":"swapped"}\n', encoding="utf-8")
        return original_projection_for(path, **kwargs)

    monkeypatch.setattr(backtest_config, "projection_for", swap_then_project)
    with pytest.raises(RuntimeError, match="changed during validation"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)
    assert swapped is True


def test_verified_config_snapshot_rejects_path_drift_and_parses_bound_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pointer, archive, _alias = _write_split_v2_binding(tmp_path)
    binding = load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)
    assert binding is not None
    archive.write_text("ml:\n  enabled: false\n", encoding="utf-8")
    archive.chmod(0o600)
    with pytest.raises(RuntimeError, match="changed after verification"):
        backtest_config._verified_binding_config_params(binding)

    pointer, archive, _alias = _write_split_v2_binding(tmp_path / "same-bytes")
    binding = load_operational_baseline_binding(root=tmp_path / "same-bytes", pointer_path=pointer)
    assert binding is not None
    original_parse = backtest_config.parse_config_snapshot

    def mutate_after_recheck(source: bytes, **kwargs: object):
        archive.write_text("ml:\n  enabled: false\n", encoding="utf-8")
        archive.chmod(0o600)
        return original_parse(source, **kwargs)

    monkeypatch.setattr(backtest_config, "parse_config_snapshot", mutate_after_recheck)
    params = backtest_config._verified_binding_config_params(binding)
    assert params["ml_enabled"] is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("current_live_binding.availability", "public"),
        ("backtest_default_binding.unexpected_economic_authority", True),
        ("backtest_default_flat_projection.ml_enabled", False),
        ("backtest_default_flat_projection.dynamic_fill_hazard_shadow_enabled", False),
    ],
)
def test_v2_pointer_layer_mutations_fail_closed(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    pointer, _archive, _alias = _write_split_v2_binding(tmp_path)
    payload = json.loads(pointer.read_text(encoding="utf-8"))
    _set_nested(payload, field, value)
    pointer.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="drifted"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("predecessor.identity_sha256", "0" * 64),
        ("backtest_default.config_sha256", "1" * 64),
        ("config.canonical_private_source", "${WRONG_CONFIG}"),
        ("config.sha256", "2" * 64),
        ("current_live.availability", "public"),
        ("backtest_default.current_live_alias_allowed", True),
        ("permissions.unexpected_authority", True),
        ("effective_at_utc", "2026-08-25T00:00:01Z"),
        ("model.directory", "models/wrong"),
        ("p3.sha256", "6" * 64),
    ],
)
def test_v2_private_boundary_identity_layer_mutations_fail_closed(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    pointer, _archive, _alias = _write_split_v2_binding(tmp_path)
    pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
    identity = tmp_path / pointer_payload["identity_path"]
    identity_payload = json.loads(identity.read_text(encoding="utf-8"))
    _set_nested(identity_payload, field, value)
    identity.write_text(json.dumps(identity_payload), encoding="utf-8")
    pointer_payload["identity_sha256"] = _sha256(identity)
    pointer.write_text(json.dumps(pointer_payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="drifted"):
        load_operational_baseline_binding(root=tmp_path, pointer_path=pointer)


def test_public_repository_has_no_operational_baseline_binding() -> None:
    assert load_operational_baseline_binding(root=ROOT) is None
