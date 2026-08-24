#!/usr/bin/env python3
"""Freeze the no-shadow runtime fix without widening BUY E3 authority."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import stat
import subprocess
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA = "f05_buy_e3_no_global_flow_shadow_runtime_fix_supplement.v1"
STATUS = "runtime_no_shadow_fix_verified_no_e3_or_sell_semantic_change"
CANONICAL_FIELD = "canonical_supplement_sha256"
PARENT_EXECUTION = {
    "git_commit": "07ef93733a3a685caba945c7761a48473e403072",
    "git_tree": "ff505cd81a8eb11f2087d2ae27e7986fd99b0444",
    "annotated_operational_tag": "f05-owner-buy-e3-direct-live-v4-20260824",
    "annotated_operational_tag_object": "da83fa0b4aed00e4d04ea3faa212b2fb27a81f0d",
}
CONFIG_IDENTITIES = {
    "disabled": {
        "sha256": "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204",
        "size_bytes": 27444,
        "mode": 0o600,
    },
    "active": {
        "sha256": "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e",
        "size_bytes": 27443,
        "mode": 0o600,
    },
}
AST_BASELINE = {
    "strategy/boolean_cooldown_buy_e3.py:ReceiveTimeFullMidEmaWindows.observe_depth": "e1734c7bcf2b87c78c64b7453051a140e0bb35560287e33d555c1341d0b6cac1",
    "strategy/boolean_cooldown_buy_e3.py:LiveBuyE3CooldownPolicy.evaluate": "cde0b0893bdf2e2d60e4e130fce71c53f9d234fbb8e38cb35591accc6cda2e09",
    "strategy/boolean_cooldown_live.py:ReceiveTimeMidEmaWindows.observe_depth": "ee332bb8992ca36181866d52a0406da13270102858e8a62c2013b4e130e7b647",
    "strategy/boolean_cooldown_live.py:LiveBooleanCooldownPolicy.observe_depth": "655ad44af0c0c6041e65ad1965091364f296266447fa4066c3351d6dfb8e2e8c",
    "strategy/boolean_cooldown_live.py:LiveBooleanCooldownPolicy.evaluate": "62d5b8ea0ecb5b9d67361f7ec5d161f4f1b989b5a4b270571463fbcb204ea012",
    "strategy/maker_engine.py:MakerEngine._select_boolean_cooldown_duration": "b5dca6c25cbbea4f194b645a7598f43eb4701e73fea71613bb92bcd2987a6227",
    "strategy/maker_engine.py:MakerEngine._select_buy_e3_cooldown_duration": "049454364176320e2ffaeec338827815a38c6ad463bca245068688f6921a4a1a",
    "strategy/maker_engine.py:MakerEngine._on_fill": "0e33055db2ccd57fb3e04968b5a5112bbd31d4017649d6d938c66e9954a61bed",
}
BACKEND_ZERO_FIELDS = (
    "native",
    "market_count",
    "trade_batches",
    "trade_events_seen",
    "trade_events_accepted",
    "book_events_seen",
    "book_events_accepted",
    "out_of_order_events",
    "stale_trade_events",
    "trade_overflow_events",
    "book_overflow_events",
)


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    unsigned = dict(payload)
    unsigned.pop(CANONICAL_FIELD, None)
    raw = json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _git(repo: Path, *args: str) -> bytes:
    return subprocess.run(
        ("git", *args),
        cwd=repo,
        check=True,
        capture_output=True,
        timeout=20.0,
    ).stdout


def _function_hash(source: bytes, class_name: str, function_name: str) -> str:
    tree = ast.parse(source.decode("utf-8"))
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for child in node.body:
                if (
                    isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and child.name == function_name
                ):
                    return hashlib.sha256(
                        ast.dump(child, include_attributes=False).encode("utf-8")
                    ).hexdigest()
    raise ValueError(f"missing frozen function {class_name}.{function_name}")


def _ast_manifest(repo: Path, commit: str) -> dict[str, str]:
    result = {}
    sources: dict[str, bytes] = {}
    for identity in AST_BASELINE:
        path, target = identity.split(":", 1)
        class_name, function_name = target.split(".", 1)
        source = sources.setdefault(path, _git(repo, "show", f"{commit}:{path}"))
        result[identity] = _function_hash(source, class_name, function_name)
    return result


def _changed_files(repo: Path, execution_commit: str) -> dict[str, dict[str, str]]:
    names = (
        _git(
            repo,
            "diff",
            "--name-only",
            "--diff-filter=ACMRT",
            f"{PARENT_EXECUTION['git_commit']}..{execution_commit}",
        )
        .decode("utf-8")
        .splitlines()
    )
    if not names or names != sorted(set(names)):
        raise ValueError("changed repository file list is empty, duplicated, or unordered")
    result = {}
    for path in names:
        raw = _git(repo, "show", f"{execution_commit}:{path}")
        result[path] = {
            "git_blob_sha1": _git(repo, "rev-parse", f"{execution_commit}:{path}")
            .decode("ascii")
            .strip(),
            "file_sha256": hashlib.sha256(raw).hexdigest(),
        }
    return result


def _config_identity(path: Path, role: str) -> dict[str, Any]:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError(f"{role} config must be a regular non-symlink file")
    raw = path.read_bytes()
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ValueError(f"{role} config changed during identity read")
    identity = {
        "logical_role": role,
        "filename": path.name,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "size_bytes": len(raw),
        "mode": stat.S_IMODE(before.st_mode),
    }
    if {key: identity[key] for key in ("sha256", "size_bytes", "mode")} != CONFIG_IDENTITIES[role]:
        raise ValueError(f"{role} config exact identity drifted")
    return identity


def build_supplement(
    *,
    repo: Path,
    execution_commit: str,
    execution_tag: str,
    execution_tag_object: str,
    disabled_config: Path,
    active_config: Path,
) -> dict[str, Any]:
    execution_commit = _git(repo, "rev-parse", f"{execution_commit}^{{commit}}")
    execution_commit = execution_commit.decode("ascii").strip()
    execution_tree = _git(repo, "rev-parse", f"{execution_commit}^{{tree}}")
    execution_tree = execution_tree.decode("ascii").strip()
    resolved_tag_object = _git(repo, "rev-parse", execution_tag).decode("ascii").strip()
    resolved_tag_commit = _git(repo, "rev-parse", f"{execution_tag}^{{commit}}")
    resolved_tag_commit = resolved_tag_commit.decode("ascii").strip()
    if resolved_tag_object != execution_tag_object or resolved_tag_commit != execution_commit:
        raise ValueError("execution annotated tag identity drifted")
    if _git(repo, "cat-file", "-t", execution_tag_object).decode("ascii").strip() != "tag":
        raise ValueError("execution tag is not annotated")
    if execution_commit == PARENT_EXECUTION["git_commit"]:
        raise ValueError("runtime fix execution must succeed the immutable parent")
    _git(
        repo,
        "merge-base",
        "--is-ancestor",
        PARENT_EXECUTION["git_commit"],
        execution_commit,
    )

    parent_ast = _ast_manifest(repo, PARENT_EXECUTION["git_commit"])
    execution_ast = _ast_manifest(repo, execution_commit)
    if parent_ast != AST_BASELINE or execution_ast != AST_BASELINE:
        raise ValueError("BUY E3/SELL/fill decision AST drifted")
    changed = _changed_files(repo, execution_commit)
    required_changed = {
        "live/config.py",
        "live/main.py",
        "strategy/maker_engine.py",
        "strategy/signal.py",
    }
    if not required_changed.issubset(changed):
        raise ValueError("runtime fix changed-file map is incomplete")
    config_pair = {
        "disabled": _config_identity(disabled_config, "disabled"),
        "active": _config_identity(active_config, "active"),
        "external_venues_enabled": False,
        "global_flow_shadow_enabled": False,
        "global_reference_shadow_enabled": False,
        "active_disabled_only_difference": "strategy.buy_e3_cooldown_policy_enabled",
        "yaml_active_release_fields_added": False,
    }
    payload = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "parent_execution": dict(PARENT_EXECUTION),
        "execution": {
            "git_commit": execution_commit,
            "git_tree": execution_tree,
            "annotated_operational_tag": execution_tag,
            "annotated_operational_tag_object": execution_tag_object,
        },
        "changed_repository_files": changed,
        "runtime_semantic_contract": {
            "parent_function_ast_sha256": parent_ast,
            "execution_function_ast_sha256": execution_ast,
            "buy_e3_and_sell_decision_ast_unchanged": True,
            "buy_action_allowlist_seconds": [79, 173, 223, 356, 640, 709, 2048],
            "buy_default_fallback": "CONTROL_85N",
            "buy_scope": "BUY exposure-increasing fill callback only",
            "sell_runtime_unchanged": True,
            "quote_price_size_ber_p3_q90_inventory_limits_unchanged": True,
            "research_supported": False,
            "owner_risk_accepted_inherited": True,
            "authority_widened": False,
            "economics_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "no_shadow_runtime_contract": {
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
            "global_flow_evaluator_effective": False,
            "global_reference_evaluator_effective": False,
            "live_config_dataclass_defaults_false": True,
            "live_config_strict_boolean": True,
            "live_config_requires_explicit_fields_for_startup": True,
            "signal_constructor_compatibility_default_true_offline_only": True,
            "maker_engine_passes_live_config_explicitly": True,
            "global_flow_native_requested_is_capability_only": True,
            "global_flow_native_effective": False,
            "cross_trade_bar_aggregator_preserved": True,
            "python_global_flow_delivery_enabled": False,
            "native_global_flow_backend_constructed": False,
            "global_flow_backend_absolute_zero_fields": list(BACKEND_ZERO_FIELDS),
            "global_reference_evaluator_called": False,
            "global_reference_bridge_basis_accumulation_enabled": False,
            "global_reference_bridge_basis_sample_count": 0,
            "market_state_book_history_bbo_cross_bars_preserved": True,
            "buy_e3_partial_depth_observer_preserved": True,
            "both_flags_restart_only": True,
            "shadow_state_restore_contract": "shadow_state_never_restored",
            "health_disabled_reason": "disabled_by_config",
            "health_state_error": 0,
            "startup_attestation_binds_explicit_and_effective_state": True,
        },
        "config_pair": config_pair,
        "permissions": {
            "apply_or_deploy_performed": False,
            "ssh_performed": False,
            "orico_written": False,
            "economics_executed_or_read": False,
            "validation_or_holdout_read": False,
            "new_strategy_arm_created": False,
            "authority_widened": False,
        },
    }
    payload[CANONICAL_FIELD] = _canonical_sha256(payload)
    return payload


def _validate_hex(value: Any, length: int, label: str) -> str:
    text = str(value)
    if len(text) != length or any(char not in "0123456789abcdef" for char in text):
        raise ValueError(f"{label} is not lowercase hex{length}")
    return text


def validate_content_receipt(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    before = path.lstat()
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise ValueError("supplement must be a regular non-symlink file")
    raw = path.read_bytes()
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise ValueError("supplement changed during validation")
    payload = json.loads(raw)
    expected_fields = {
        "schema_version",
        "status",
        "created_at_utc",
        "parent_execution",
        "execution",
        "changed_repository_files",
        "runtime_semantic_contract",
        "no_shadow_runtime_contract",
        "config_pair",
        "permissions",
        CANONICAL_FIELD,
    }
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise ValueError("supplement top-level fields drifted")
    if payload["schema_version"] != SCHEMA or payload["status"] != STATUS:
        raise ValueError("supplement schema/status drifted")
    if payload["parent_execution"] != PARENT_EXECUTION:
        raise ValueError("supplement parent execution drifted")
    if payload[CANONICAL_FIELD] != _canonical_sha256(payload):
        raise ValueError("supplement canonical SHA256 mismatch")
    execution = payload["execution"]
    if not isinstance(execution, dict) or set(execution) != set(PARENT_EXECUTION):
        raise ValueError("supplement execution fields drifted")
    _validate_hex(execution["git_commit"], 40, "execution commit")
    _validate_hex(execution["git_tree"], 40, "execution tree")
    _validate_hex(execution["annotated_operational_tag_object"], 40, "execution tag object")
    if not str(execution["annotated_operational_tag"]).strip():
        raise ValueError("execution tag is empty")
    changed = payload["changed_repository_files"]
    if not isinstance(changed, dict) or list(changed) != sorted(changed) or not changed:
        raise ValueError("supplement changed file map drifted")
    for relative, identity in changed.items():
        if Path(relative).is_absolute() or ".." in Path(relative).parts:
            raise ValueError("supplement changed file path is not portable")
        if not isinstance(identity, dict) or set(identity) != {"git_blob_sha1", "file_sha256"}:
            raise ValueError("supplement changed file identity fields drifted")
        _validate_hex(identity["git_blob_sha1"], 40, f"{relative} blob")
        _validate_hex(identity["file_sha256"], 64, f"{relative} SHA256")
    semantic = payload["runtime_semantic_contract"]
    semantic_fields = {
        "parent_function_ast_sha256",
        "execution_function_ast_sha256",
        "buy_e3_and_sell_decision_ast_unchanged",
        "buy_action_allowlist_seconds",
        "buy_default_fallback",
        "buy_scope",
        "sell_runtime_unchanged",
        "quote_price_size_ber_p3_q90_inventory_limits_unchanged",
        "research_supported",
        "owner_risk_accepted_inherited",
        "authority_widened",
        "economics_read",
        "validation_read",
        "sealed_holdout_read",
    }
    if not isinstance(semantic, dict) or set(semantic) != semantic_fields:
        raise ValueError("supplement runtime semantic fields drifted")
    if (
        semantic.get("parent_function_ast_sha256") != AST_BASELINE
        or semantic.get("execution_function_ast_sha256") != AST_BASELINE
    ):
        raise ValueError("supplement frozen decision AST drifted")
    expected_semantic = {
        "buy_e3_and_sell_decision_ast_unchanged": True,
        "buy_action_allowlist_seconds": [79, 173, 223, 356, 640, 709, 2048],
        "buy_default_fallback": "CONTROL_85N",
        "buy_scope": "BUY exposure-increasing fill callback only",
        "sell_runtime_unchanged": True,
        "quote_price_size_ber_p3_q90_inventory_limits_unchanged": True,
        "research_supported": False,
        "owner_risk_accepted_inherited": True,
        "authority_widened": False,
        "economics_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    if any(semantic.get(key) != value for key, value in expected_semantic.items()):
        raise ValueError("supplement runtime semantic contract drifted")
    no_shadow = payload["no_shadow_runtime_contract"]
    no_shadow_fields = {
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
        "global_flow_evaluator_effective",
        "global_reference_evaluator_effective",
        "live_config_dataclass_defaults_false",
        "live_config_strict_boolean",
        "live_config_requires_explicit_fields_for_startup",
        "signal_constructor_compatibility_default_true_offline_only",
        "maker_engine_passes_live_config_explicitly",
        "global_flow_native_requested_is_capability_only",
        "global_flow_native_effective",
        "cross_trade_bar_aggregator_preserved",
        "python_global_flow_delivery_enabled",
        "native_global_flow_backend_constructed",
        "global_flow_backend_absolute_zero_fields",
        "global_reference_evaluator_called",
        "global_reference_bridge_basis_accumulation_enabled",
        "global_reference_bridge_basis_sample_count",
        "market_state_book_history_bbo_cross_bars_preserved",
        "buy_e3_partial_depth_observer_preserved",
        "both_flags_restart_only",
        "shadow_state_restore_contract",
        "health_disabled_reason",
        "health_state_error",
        "startup_attestation_binds_explicit_and_effective_state",
    }
    if not isinstance(no_shadow, dict) or set(no_shadow) != no_shadow_fields:
        raise ValueError("supplement no-shadow fields drifted")
    if no_shadow.get("global_flow_backend_absolute_zero_fields") != list(BACKEND_ZERO_FIELDS):
        raise ValueError("supplement zero backend field set drifted")
    expected_no_shadow = {
        "global_flow_shadow_enabled": False,
        "global_reference_shadow_enabled": False,
        "global_flow_evaluator_effective": False,
        "global_reference_evaluator_effective": False,
        "live_config_dataclass_defaults_false": True,
        "live_config_strict_boolean": True,
        "live_config_requires_explicit_fields_for_startup": True,
        "signal_constructor_compatibility_default_true_offline_only": True,
        "maker_engine_passes_live_config_explicitly": True,
        "global_flow_native_requested_is_capability_only": True,
        "global_flow_native_effective": False,
        "cross_trade_bar_aggregator_preserved": True,
        "python_global_flow_delivery_enabled": False,
        "native_global_flow_backend_constructed": False,
        "global_reference_evaluator_called": False,
        "global_reference_bridge_basis_accumulation_enabled": False,
        "global_reference_bridge_basis_sample_count": 0,
        "market_state_book_history_bbo_cross_bars_preserved": True,
        "buy_e3_partial_depth_observer_preserved": True,
        "both_flags_restart_only": True,
        "shadow_state_restore_contract": "shadow_state_never_restored",
        "health_disabled_reason": "disabled_by_config",
        "health_state_error": 0,
        "startup_attestation_binds_explicit_and_effective_state": True,
    }
    if any(no_shadow.get(key) != value for key, value in expected_no_shadow.items()):
        raise ValueError("supplement no-shadow contract drifted")
    configs = payload["config_pair"]
    if not isinstance(configs, dict) or set(configs) != {
        "disabled",
        "active",
        "external_venues_enabled",
        "global_flow_shadow_enabled",
        "global_reference_shadow_enabled",
        "active_disabled_only_difference",
        "yaml_active_release_fields_added",
    }:
        raise ValueError("supplement config pair fields drifted")
    for role in ("disabled", "active"):
        identity = configs.get(role)
        if (
            not isinstance(identity, dict)
            or set(identity)
            != {
                "logical_role",
                "filename",
                "sha256",
                "size_bytes",
                "mode",
            }
            or {key: identity.get(key) for key in ("sha256", "size_bytes", "mode")}
            != CONFIG_IDENTITIES[role]
        ):
            raise ValueError(f"supplement {role} config identity drifted")
    if any(
        configs.get(key) != value
        for key, value in {
            "external_venues_enabled": False,
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
            "active_disabled_only_difference": "strategy.buy_e3_cooldown_policy_enabled",
            "yaml_active_release_fields_added": False,
        }.items()
    ):
        raise ValueError("supplement config pair contract drifted")
    permissions = payload["permissions"]
    if not isinstance(permissions, dict) or any(
        value is not False for value in permissions.values()
    ):
        raise ValueError("supplement permissions widened")
    mode = stat.S_IMODE(before.st_mode)
    exact7 = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "file_sha256": hashlib.sha256(raw).hexdigest(),
        "canonical_field": CANONICAL_FIELD,
        "canonical_sha256": payload[CANONICAL_FIELD],
        "size_bytes": len(raw),
        "mode": f"{mode:04o}",
    }
    return payload, exact7


def _publish(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to replace supplement: {path}")
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.link(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--execution-commit", required=True)
    parser.add_argument("--execution-tag", required=True)
    parser.add_argument("--execution-tag-object", required=True)
    parser.add_argument("--disabled-config", type=Path, required=True)
    parser.add_argument("--active-config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = build_supplement(
        repo=args.repo.resolve(),
        execution_commit=args.execution_commit,
        execution_tag=args.execution_tag,
        execution_tag_object=args.execution_tag_object,
        disabled_config=args.disabled_config,
        active_config=args.active_config,
    )
    raw = (json.dumps(payload, sort_keys=True, indent=2) + "\n").encode("utf-8")
    _publish(args.output, raw)
    validate_content_receipt(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
