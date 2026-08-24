#!/usr/bin/env python3
"""Freeze the config-only successor that disables external-venue shadow input.

The immutable direct-v4 release-v2 remains the sole action/live authority.  This
receipt proves that the old disabled/active config pair was changed only at
``external_venues.enabled`` and that the corrected pair still differs only at
the BUY E3 enable flag.  It reads no market, lifecycle, economic, Validation,
or holdout data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import yaml

OWNER: Final = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
SCHEMA_VERSION: Final = f"{OWNER}.external_venues_disabled_config_successor.v1"
STATUS: Final = "external_venue_shadow_disabled_config_pair_frozen"
CANONICAL_FIELD: Final = "canonical_config_successor_sha256"

OLD_DISABLED_CONFIG_SHA256: Final = (
    "d08df3958f4243109036555ba60d58c2599d88560990305f176744d62959c7ef"
)
OLD_ACTIVE_CONFIG_SHA256: Final = (
    "2f61532126cbe633424476cb093c6c978bab1f935f69a30e06677d677008cae6"
)
NEW_DISABLED_CONFIG_SHA256: Final = (
    "10158a92177cd87b77fdb24a2a477dcab4b41cfb29208cf96c19953edafe166f"
)
NEW_ACTIVE_CONFIG_SHA256: Final = (
    "ad153012b14e725a3ac24f0ddbe02bc353168a13ec827b777cc94761020524ec"
)
RELEASE_V2_BINDING: Final = {
    "schema_version": (
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_"
        "direct_owner_active_release.v2"
    ),
    "status": "owner_authorized_direct_live_lifecycle_repair_pending_evidence",
    "file_sha256": "ff888f4b5973563275c2b97e1554d45c9d686ef15d686440bf096521aab17fc2",
    "canonical_field": "canonical_active_release_sha256",
    "canonical_sha256": "823ca1e4d53e968eb0afc53c4d2cad99cc17aac696548baa1700e800a4579702",
    "size_bytes": 7757,
    "mode": "0600",
}
EXPECTED_CHANGED_PATH: Final = "external_venues.enabled"
EXPECTED_PAIR_DIFFERENCE: Final = "strategy.buy_e3_cooldown_policy_enabled"
DIRECT_V4_EXECUTION_COMMIT: Final = "07ef93733a3a685caba945c7761a48473e403072"
CONTENT_BINDING_FIELDS: Final = frozenset(
    {
        "schema_version",
        "status",
        "file_sha256",
        "canonical_field",
        "canonical_sha256",
        "size_bytes",
        "mode",
    }
)

PERMISSIONS: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "hypothetical_live_actions_scored": False,
}
AUTHORITY_DESIGN: Final = {
    "runtime_authority": "immutable_direct_v4_owner_release_v2",
    "runtime_authority_replaced": False,
    "release_v2_reissued": False,
    "config_correction_is_additive_only": True,
    "config_correction_grants_no_action_or_live_authority": True,
    "old_v4_evidence_remains_immutable_historical": True,
    "fresh_resource_active_transport_lifecycle_and_final_evidence_required": True,
}


class ConfigSuccessorError(RuntimeError):
    """Raised when the config-only successor fails closed."""


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def document_sha256(payload: Mapping[str, Any], canonical_field: str) -> str:
    material = dict(payload)
    material.pop(canonical_field, None)
    return hashlib.sha256(
        json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def _private_regular(path: Path, label: str, *, expected_mode: int = 0o600) -> os.stat_result:
    try:
        info = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ConfigSuccessorError(f"{label} is unavailable") from exc
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ConfigSuccessorError(f"{label} is not a single-link regular file")
    if stat.S_IMODE(info.st_mode) != expected_mode:
        raise ConfigSuccessorError(f"{label} mode drifted")
    return info


def _load_yaml(path: Path, expected_sha256: str, label: str) -> dict[str, Any]:
    _private_regular(path, label)
    if file_sha256(path) != expected_sha256:
        raise ConfigSuccessorError(f"{label} SHA256 drifted")
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ConfigSuccessorError(f"{label} is unreadable") from exc
    if not isinstance(payload, dict):
        raise ConfigSuccessorError(f"{label} root is not a mapping")
    return payload


def _load_json(path: Path, label: str) -> tuple[dict[str, Any], os.stat_result]:
    info = _private_regular(path, label)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigSuccessorError(f"{label} is unreadable") from exc
    if not isinstance(payload, dict):
        raise ConfigSuccessorError(f"{label} root is not a mapping")
    return payload, info


def _leaf_diff(left: Any, right: Any, prefix: str = "") -> list[str]:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        paths: list[str] = []
        for key in sorted(set(left) | set(right)):
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in left or key not in right:
                paths.append(path)
            else:
                paths.extend(_leaf_diff(left[key], right[key], path))
        return paths
    if isinstance(left, list) and isinstance(right, list):
        paths = []
        for index in range(max(len(left), len(right))):
            path = f"{prefix}[{index}]"
            if index >= len(left) or index >= len(right):
                paths.append(path)
            else:
                paths.extend(_leaf_diff(left[index], right[index], path))
        return paths
    return [] if left == right else [prefix]


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigSuccessorError(f"{label} is not a mapping")
    return value


def _validate_pair(
    old_disabled: Mapping[str, Any],
    old_active: Mapping[str, Any],
    new_disabled: Mapping[str, Any],
    new_active: Mapping[str, Any],
) -> None:
    if _leaf_diff(old_disabled, new_disabled) != [EXPECTED_CHANGED_PATH]:
        raise ConfigSuccessorError("disabled config changed outside external_venues.enabled")
    if _leaf_diff(old_active, new_active) != [EXPECTED_CHANGED_PATH]:
        raise ConfigSuccessorError("active config changed outside external_venues.enabled")
    if _leaf_diff(old_disabled, old_active) != [EXPECTED_PAIR_DIFFERENCE]:
        raise ConfigSuccessorError("predecessor config pair difference drifted")
    if _leaf_diff(new_disabled, new_active) != [EXPECTED_PAIR_DIFFERENCE]:
        raise ConfigSuccessorError("corrected config pair differs outside BUY E3 enablement")
    for label, payload in (("old disabled", old_disabled), ("old active", old_active)):
        external = _mapping(payload.get("external_venues"), f"{label} external_venues")
        if external.get("enabled") is not True or external.get("shadow_only") is not True:
            raise ConfigSuccessorError(f"{label} external venue predecessor semantics drifted")
    for label, payload in (("new disabled", new_disabled), ("new active", new_active)):
        external = _mapping(payload.get("external_venues"), f"{label} external_venues")
        if external.get("enabled") is not False:
            raise ConfigSuccessorError(f"{label} external venues were not disabled")
        strategy = _mapping(payload.get("strategy"), f"{label} strategy")
        logging = _mapping(payload.get("logging"), f"{label} logging")
        if any(
            strategy.get(name) is not False
            for name in (
                "buy_fill_selection_shadow_enabled",
                "dynamic_fill_hazard_shadow_enabled",
                "cross_venue_fair_price_shadow_enabled",
            )
        ) or any(
            logging.get(name) is not False
            for name in ("inventory_campaign_shadow_enabled", "market_tape_enabled")
        ):
            raise ConfigSuccessorError(f"{label} contains another enabled shadow collector")
    old_strategy = _mapping(old_disabled.get("strategy"), "old disabled strategy")
    new_disabled_strategy = _mapping(new_disabled.get("strategy"), "new disabled strategy")
    new_active_strategy = _mapping(new_active.get("strategy"), "new active strategy")
    if (
        old_strategy.get("buy_e3_cooldown_policy_enabled") is not False
        or new_disabled_strategy.get("buy_e3_cooldown_policy_enabled") is not False
        or new_active_strategy.get("buy_e3_cooldown_policy_enabled") is not True
    ):
        raise ConfigSuccessorError("BUY E3 config pair semantics drifted")


def _release_binding(path: Path) -> dict[str, Any]:
    payload, info = _load_json(path, "direct-v4 release-v2")
    observed = {
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": file_sha256(path),
        "canonical_field": RELEASE_V2_BINDING["canonical_field"],
        "canonical_sha256": payload.get(RELEASE_V2_BINDING["canonical_field"]),
        "size_bytes": info.st_size,
        "mode": format(stat.S_IMODE(info.st_mode), "04o"),
    }
    if observed != RELEASE_V2_BINDING:
        raise ConfigSuccessorError("direct-v4 release-v2 binding drifted")
    if document_sha256(payload, str(RELEASE_V2_BINDING["canonical_field"])) != observed[
        "canonical_sha256"
    ]:
        raise ConfigSuccessorError("direct-v4 release-v2 canonical drifted")
    return observed


def _git_execution(repository_root: Path, annotated_tag: str) -> dict[str, Any]:
    root = repository_root.resolve(strict=True)
    commands = {
        "execution_commit": ["git", "rev-parse", "HEAD"],
        "execution_tree": ["git", "rev-parse", "HEAD^{tree}"],
        "annotated_tag_object": ["git", "rev-parse", annotated_tag],
        "tag_peeled_commit": ["git", "rev-parse", f"{annotated_tag}^{{}}"],
        "status": ["git", "status", "--porcelain", "--untracked-files=all"],
        "tag_type": ["git", "cat-file", "-t", annotated_tag],
    }
    values: dict[str, Any] = {}
    for key, command in commands.items():
        result = subprocess.run(command, cwd=root, check=True, capture_output=True, text=True)
        values[key] = result.stdout.strip()
    if values.pop("status") or values.pop("tag_type") != "tag":
        raise ConfigSuccessorError("collector checkout is not clean at an annotated tag")
    if values["execution_commit"] != values["tag_peeled_commit"]:
        raise ConfigSuccessorError("collector tag does not peel to HEAD")
    ancestor = (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", DIRECT_V4_EXECUTION_COMMIT, "HEAD"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        ).returncode
        == 0
    )
    if ancestor:
        raise ConfigSuccessorError("collector must remain independent from direct-v4 authority")
    values["repository_root"] = str(root)
    values["annotated_tag"] = annotated_tag
    values["direct_v4_commit_is_ancestor"] = False
    values["runtime_authority_checkout"] = False
    return values


def _content_binding(path: Path, payload: Mapping[str, Any], info: os.stat_result) -> dict[str, Any]:
    return {
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": file_sha256(path),
        "canonical_field": CANONICAL_FIELD,
        "canonical_sha256": payload.get(CANONICAL_FIELD),
        "size_bytes": info.st_size,
        "mode": format(stat.S_IMODE(info.st_mode), "04o"),
    }


def validate_content_receipt(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate the complete portable receipt without reopening config paths."""

    payload, info = _load_json(path, "config successor receipt")
    expected_fields = {
        "schema_version",
        "identity",
        "status",
        "generated_utc",
        "collector_execution",
        "runtime_authority",
        "predecessor_config_pair",
        "corrected_config_pair",
        "semantic_diff",
        "required_successor_evidence",
        "authority_design",
        "permissions",
        "evidence_boundary",
        CANONICAL_FIELD,
    }
    execution = payload.get("collector_execution")
    if not isinstance(execution, Mapping) or set(execution) != {
        "repository_root",
        "execution_commit",
        "execution_tree",
        "annotated_tag",
        "annotated_tag_object",
        "tag_peeled_commit",
        "direct_v4_commit_is_ancestor",
        "runtime_authority_checkout",
    }:
        raise ConfigSuccessorError("config successor collector execution fields drifted")
    if (
        set(payload) != expected_fields
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("identity") != OWNER
        or payload.get("status") != STATUS
        or payload.get("runtime_authority") != RELEASE_V2_BINDING
        or payload.get("predecessor_config_pair")
        != {
            "disabled_sha256": OLD_DISABLED_CONFIG_SHA256,
            "active_sha256": OLD_ACTIVE_CONFIG_SHA256,
            "external_venues_enabled": True,
            "historical_only": True,
        }
        or payload.get("corrected_config_pair")
        != {
            "disabled_sha256": NEW_DISABLED_CONFIG_SHA256,
            "active_sha256": NEW_ACTIVE_CONFIG_SHA256,
            "external_venues_enabled": False,
            "active_disabled_only_difference": EXPECTED_PAIR_DIFFERENCE,
        }
        or payload.get("semantic_diff")
        != {
            "changed_paths": [EXPECTED_CHANGED_PATH],
            "old_value": True,
            "new_value": False,
            "source_entries_retained_but_not_started": True,
            "external_network_shadow_disabled": True,
            "e3_artifact_and_decision_semantics_unchanged": True,
        }
        or payload.get("required_successor_evidence")
        != {
            "fresh_disabled_resource_gate": True,
            "fresh_active_process_capture": True,
            "fresh_cross_host_admission": True,
            "fresh_3600s_lifecycle_admission": True,
            "fresh_final_evidence_chain": True,
            "fresh_pointer_catalog_epoch": True,
        }
        or payload.get("authority_design") != AUTHORITY_DESIGN
        or payload.get("permissions") != PERMISSIONS
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or execution.get("tag_peeled_commit") != execution.get("execution_commit")
        or execution.get("direct_v4_commit_is_ancestor") is not False
        or execution.get("runtime_authority_checkout") is not False
        or payload.get(CANONICAL_FIELD) != document_sha256(payload, CANONICAL_FIELD)
    ):
        raise ConfigSuccessorError("config successor receipt semantic identity drifted")
    binding = _content_binding(path, payload, info)
    if set(binding) != CONTENT_BINDING_FIELDS:
        raise ConfigSuccessorError("config successor content binding fields drifted")
    return dict(payload), binding


def build_receipt(
    *,
    repository_root: Path,
    annotated_tag: str,
    old_disabled_config: Path,
    old_active_config: Path,
    new_disabled_config: Path,
    new_active_config: Path,
    direct_release: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    old_disabled = _load_yaml(old_disabled_config, OLD_DISABLED_CONFIG_SHA256, "old disabled config")
    old_active = _load_yaml(old_active_config, OLD_ACTIVE_CONFIG_SHA256, "old active config")
    new_disabled = _load_yaml(new_disabled_config, NEW_DISABLED_CONFIG_SHA256, "new disabled config")
    new_active = _load_yaml(new_active_config, NEW_ACTIVE_CONFIG_SHA256, "new active config")
    _validate_pair(old_disabled, old_active, new_disabled, new_active)
    timestamp = generated_utc or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    if not timestamp.endswith("Z"):
        raise ConfigSuccessorError("generated_utc must be UTC Z")
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER,
        "status": STATUS,
        "generated_utc": timestamp,
        "collector_execution": _git_execution(repository_root, annotated_tag),
        "runtime_authority": _release_binding(direct_release),
        "predecessor_config_pair": {
            "disabled_sha256": OLD_DISABLED_CONFIG_SHA256,
            "active_sha256": OLD_ACTIVE_CONFIG_SHA256,
            "external_venues_enabled": True,
            "historical_only": True,
        },
        "corrected_config_pair": {
            "disabled_sha256": NEW_DISABLED_CONFIG_SHA256,
            "active_sha256": NEW_ACTIVE_CONFIG_SHA256,
            "external_venues_enabled": False,
            "active_disabled_only_difference": EXPECTED_PAIR_DIFFERENCE,
        },
        "semantic_diff": {
            "changed_paths": [EXPECTED_CHANGED_PATH],
            "old_value": True,
            "new_value": False,
            "source_entries_retained_but_not_started": True,
            "external_network_shadow_disabled": True,
            "e3_artifact_and_decision_semantics_unchanged": True,
        },
        "required_successor_evidence": {
            "fresh_disabled_resource_gate": True,
            "fresh_active_process_capture": True,
            "fresh_cross_host_admission": True,
            "fresh_3600s_lifecycle_admission": True,
            "fresh_final_evidence_chain": True,
            "fresh_pointer_catalog_epoch": True,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(PERMISSIONS),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = document_sha256(payload, CANONICAL_FIELD)
    return payload


def _write_exclusive(path: Path, payload: Mapping[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return file_sha256(path)


def validate_receipt(
    path: Path,
    *,
    repository_root: Path,
    annotated_tag: str,
    old_disabled_config: Path,
    old_active_config: Path,
    new_disabled_config: Path,
    new_active_config: Path,
    direct_release: Path,
) -> dict[str, Any]:
    payload, _info = _load_json(path, "config successor receipt")
    validate_content_receipt(path)
    expected = build_receipt(
        repository_root=repository_root,
        annotated_tag=annotated_tag,
        old_disabled_config=old_disabled_config,
        old_active_config=old_active_config,
        new_disabled_config=new_disabled_config,
        new_active_config=new_active_config,
        direct_release=direct_release,
        generated_utc=payload.get("generated_utc"),
    )
    if payload != expected or payload.get(CANONICAL_FIELD) != document_sha256(payload, CANONICAL_FIELD):
        raise ConfigSuccessorError("config successor receipt drifted")
    return payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("finalize", "validate"))
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--annotated-tag", required=True)
    parser.add_argument("--old-disabled-config", type=Path, required=True)
    parser.add_argument("--old-active-config", type=Path, required=True)
    parser.add_argument("--new-disabled-config", type=Path, required=True)
    parser.add_argument("--new-active-config", type=Path, required=True)
    parser.add_argument("--direct-release", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    kwargs = {
        "repository_root": args.repository_root,
        "annotated_tag": args.annotated_tag,
        "old_disabled_config": args.old_disabled_config,
        "old_active_config": args.old_active_config,
        "new_disabled_config": args.new_disabled_config,
        "new_active_config": args.new_active_config,
        "direct_release": args.direct_release,
    }
    if args.command == "finalize":
        payload = build_receipt(**kwargs)
        print(f"file_sha256={_write_exclusive(args.output, payload)}")
        print(f"canonical_sha256={payload[CANONICAL_FIELD]}")
    else:
        payload = validate_receipt(args.output, **kwargs)
        print(payload[CANONICAL_FIELD])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
