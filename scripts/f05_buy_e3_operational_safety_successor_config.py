#!/usr/bin/env python3
"""Create the exact private config pair for the BUY E3 safety successor.

The frozen release-v3 active/disabled YAML files are the only accepted inputs.
The successor adds an explicit five-second synchronous REST timeout and changes
the maximum-spread behavior to ``pause_exposure``.  It preserves every other
semantic value, including the exact BUY E3 artifact and the inert source
selectors behind disabled collection gates.

Outputs are private, create-only, receipt-last files.  An interrupted exact
prefix can be recovered, while mismatched, out-of-order, duplicate-key,
symlink, hard-link, permission, or mutation states fail closed.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
import stat
import sys
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import f05_buy_e3_direct_owner_release_v3 as release_v3  # noqa: E402

OWNER: Final = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
SCHEMA_VERSION: Final = f"{OWNER}.operational_safety_successor_config_pair.v1"
STATUS: Final = "exact_timeout_pause_exposure_no_shadow_config_pair_frozen"
VISIBILITY: Final = "local_only_do_not_publish"
CANONICAL_FIELD: Final = "canonical_config_pair_receipt_sha256"

PREDECESSOR_DISABLED: Final = {
    "file_sha256": "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204",
    "semantic_sha256": "3e8f1c6b829f88ce250896e7ff810c22d3c9102bc4c10f8d9ead883facedc2a8",
    "size_bytes": 27_444,
    "mode": "0600",
}
PREDECESSOR_ACTIVE: Final = {
    "file_sha256": "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e",
    "semantic_sha256": "f5cb3d238edfdff4c2353e2d7cd5c2d948d36ef0be1e7733ce0dda73ea7a21c2",
    "size_bytes": 27_443,
    "mode": "0600",
}
SUCCESSOR_DISABLED: Final = {
    "file_sha256": "209435ddfe91efe17c23e32ec36fe0d25633a23f640c2151c520d515465a707b",
    "semantic_sha256": "64496cca76733c88517f6e4f3bc12f2d90bd626547a4648aa5d8fce2439eb85e",
    "size_bytes": 27_497,
    "mode": "0600",
}
SUCCESSOR_ACTIVE: Final = {
    "file_sha256": "a126eaae9d48d08e7c0621ca298f0216c4ff091c01bfd4da4e8559bd2a74cc39",
    "semantic_sha256": "1fd90dab0c1537fb370f610c6baebd3195659b6e62f46c6edd81fcb640c3d2a4",
    "size_bytes": 27_496,
    "mode": "0600",
}

DISABLED_FILENAME: Final = "config.live_safety_successor.disabled.v1.yaml"
ACTIVE_FILENAME: Final = "config.live_safety_successor.active.v1.yaml"
RECEIPT_FILENAME: Final = "config_pair.live_safety_successor.v1.json"
PUBLICATION_ORDER: Final = (DISABLED_FILENAME, ACTIVE_FILENAME, RECEIPT_FILENAME)

PAIR_DIFFERENCE: Final = "strategy.buy_e3_cooldown_policy_enabled"
SUCCESSOR_CHANGES: Final = ("api.timeout_s", "strategy.spread_cap_mode")
EXPLICIT_SAFETY_VALUES: Final = {
    "api.timeout_s": 5.0,
    "strategy.spread_cap_mode": "pause_exposure",
}
REQUIRED_FALSE_PATHS: Final = tuple(release_v3.REQUIRED_FALSE_CONFIG_PATHS)
TIMEOUT_ANCHOR: Final = b"  testnet: false"
TIMEOUT_LINE: Final = b"  timeout_s: 5.0\n"
SPREAD_ANCHOR: Final = b"  max_spread_bps: 20"
SPREAD_LINE: Final = b'  spread_cap_mode: "pause_exposure"\n'
MAX_PRIVATE_FILE_BYTES: Final = 2 * 1024 * 1024

PERMISSIONS: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "shadow_created": False,
    "companion_created": False,
    "external_collection_created": False,
    "runtime_authority_granted": False,
}


class OperationalSafetyConfigError(RuntimeError):
    """Raised when the private successor config transaction fails closed."""


@dataclass(frozen=True)
class _ConfigDocument:
    payload: dict[str, Any]
    raw: bytes


@dataclass(frozen=True)
class ConfigPairBuild:
    """Portable bytes produced before any create-only publication."""

    disabled_raw: bytes
    active_raw: bytes
    receipt: dict[str, Any]
    receipt_raw: bytes


class _UniqueSafeLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueSafeLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    output: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in output:
            raise OperationalSafetyConfigError(f"duplicate YAML key: {key}")
        output[key] = loader.construct_object(value_node, deep=deep)
    return output


_UniqueSafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def file_sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def canonical_sha256(payload: Mapping[str, Any], excluded_field: str | None = None) -> str:
    material = dict(payload)
    if excluded_field is not None:
        material.pop(excluded_field, None)
    return hashlib.sha256(
        json.dumps(
            material,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()


def _reject_non_finite(value: Any) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise OperationalSafetyConfigError("config contains a non-finite number")
    if isinstance(value, Mapping):
        for child in value.values():
            _reject_non_finite(child)
    elif isinstance(value, list):
        for child in value:
            _reject_non_finite(child)


def _parse_yaml(raw: bytes, label: str) -> dict[str, Any]:
    try:
        payload = yaml.load(raw.decode("utf-8"), Loader=_UniqueSafeLoader)
    except OperationalSafetyConfigError:
        raise
    except (UnicodeError, yaml.YAMLError) as exc:
        raise OperationalSafetyConfigError(f"{label} is unreadable YAML") from exc
    if not isinstance(payload, dict):
        raise OperationalSafetyConfigError(f"{label} root is not a mapping")
    _reject_non_finite(payload)
    return payload


def _binding(document: _ConfigDocument) -> dict[str, Any]:
    return {
        "file_sha256": file_sha256_bytes(document.raw),
        "semantic_sha256": canonical_sha256(document.payload),
        "size_bytes": len(document.raw),
        "mode": "0600",
    }


def _open_predecessor(
    path: Path,
    label: str,
    expected_binding: Mapping[str, Any],
) -> _ConfigDocument:
    try:
        opened = release_v3._open_config(path, label)  # noqa: SLF001
    except Exception as exc:
        raise OperationalSafetyConfigError(
            f"{label} could not be opened safely: {exc}"
        ) from exc
    document = _ConfigDocument(payload=opened.payload, raw=opened.raw)
    if _binding(document) != dict(expected_binding):
        raise OperationalSafetyConfigError(f"{label} frozen identity drifted")
    return document


def _leaf_diff(left: Any, right: Any, prefix: str = "") -> list[str]:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        output: list[str] = []
        for key in sorted(set(left) | set(right)):
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in left or key not in right:
                output.append(path)
            else:
                output.extend(_leaf_diff(left[key], right[key], path))
        return output
    if isinstance(left, list) and isinstance(right, list):
        output = []
        for index in range(max(len(left), len(right))):
            path = f"{prefix}[{index}]"
            if index >= len(left) or index >= len(right):
                output.append(path)
            else:
                output.extend(_leaf_diff(left[index], right[index], path))
        return output
    return [] if left == right else [prefix]


def _path_value(payload: Mapping[str, Any], path: str) -> Any:
    value: Any = payload
    for component in path.split("."):
        if not isinstance(value, Mapping) or component not in value:
            raise OperationalSafetyConfigError(f"config field missing: {path}")
        value = value[component]
    return value


def _iter_leaves(value: Any, prefix: str = "") -> Iterator[tuple[str, str, Any]]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            name = str(key)
            path = f"{prefix}.{name}" if prefix else name
            if isinstance(child, (Mapping, list)):
                yield from _iter_leaves(child, path)
            else:
                yield path, name, child
    elif isinstance(value, list):
        for index, child in enumerate(value):
            path = f"{prefix}[{index}]"
            if isinstance(child, (Mapping, list)):
                yield from _iter_leaves(child, path)
            else:
                yield path, f"[{index}]", child


def _validate_no_collection(payload: Mapping[str, Any], label: str) -> list[str]:
    for path in REQUIRED_FALSE_PATHS:
        if _path_value(payload, path) is not False:
            raise OperationalSafetyConfigError(f"{label} did not explicitly disable {path}")

    explicit_false_paths: list[str] = list(REQUIRED_FALSE_PATHS)
    for path, name, value in _iter_leaves(payload):
        lowered = name.lower()
        activation_flag = (
            ("shadow" in lowered and lowered.endswith("enabled"))
            or ("companion" in lowered and lowered.endswith("enabled"))
            or lowered == "record_enabled"
        )
        if activation_flag:
            if value is not False:
                raise OperationalSafetyConfigError(
                    f"{label} collection activation flag is not false: {path}"
                )
            explicit_false_paths.append(path)

    external = _path_value(payload, "external_venues")
    if not isinstance(external, Mapping):
        raise OperationalSafetyConfigError(f"{label} external_venues is not a mapping")
    sources = external.get("sources")
    if not isinstance(sources, list) or not sources:
        raise OperationalSafetyConfigError(f"{label} external source list is empty")
    for index, source in enumerate(sources):
        if not isinstance(source, Mapping) or source.get("record_enabled") is not False:
            raise OperationalSafetyConfigError(
                f"{label} external source {index} recording gate is not false"
            )

    try:
        release_v3._reject_yaml_release_fields(payload)  # noqa: SLF001
    except Exception as exc:
        raise OperationalSafetyConfigError(f"{label} embeds release authority") from exc
    return sorted(set(explicit_false_paths))


def _validate_predecessor_pair(
    disabled: _ConfigDocument,
    active: _ConfigDocument,
) -> list[str]:
    if _leaf_diff(disabled.payload, active.payload) != [PAIR_DIFFERENCE]:
        raise OperationalSafetyConfigError(
            "predecessor pair differs outside BUY E3 enablement"
        )
    if _path_value(disabled.payload, PAIR_DIFFERENCE) is not False:
        raise OperationalSafetyConfigError("predecessor disabled config does not select B0")
    if _path_value(active.payload, PAIR_DIFFERENCE) is not True:
        raise OperationalSafetyConfigError("predecessor active config does not enable BUY E3")
    disabled_false = _validate_no_collection(disabled.payload, "predecessor disabled config")
    active_false = _validate_no_collection(active.payload, "predecessor active config")
    if disabled_false != active_false:
        raise OperationalSafetyConfigError("predecessor no-collection flag set differs")
    for path in SUCCESSOR_CHANGES:
        try:
            _path_value(disabled.payload, path)
        except OperationalSafetyConfigError:
            continue
        raise OperationalSafetyConfigError(f"predecessor unexpectedly defines {path}")
    return disabled_false


def _insert_after_unique_prefix(raw: bytes, prefix: bytes, insertion: bytes, label: str) -> bytes:
    if not raw.endswith(b"\n") or b"\r" in raw:
        raise OperationalSafetyConfigError(f"{label} line-ending identity drifted")
    lines = raw.splitlines(keepends=True)
    matches = [index for index, line in enumerate(lines) if line.startswith(prefix)]
    if len(matches) != 1:
        raise OperationalSafetyConfigError(f"{label} insertion anchor is not unique")
    index = matches[0]
    return b"".join((*lines[: index + 1], insertion, *lines[index + 1 :]))


def _successor_document(predecessor: _ConfigDocument, label: str) -> _ConfigDocument:
    raw = _insert_after_unique_prefix(predecessor.raw, TIMEOUT_ANCHOR, TIMEOUT_LINE, label)
    raw = _insert_after_unique_prefix(raw, SPREAD_ANCHOR, SPREAD_LINE, label)
    payload = _parse_yaml(raw, label)
    if _leaf_diff(predecessor.payload, payload) != list(SUCCESSOR_CHANGES):
        raise OperationalSafetyConfigError(
            f"{label} changed outside the two operational safety fields"
        )
    for path, expected in EXPLICIT_SAFETY_VALUES.items():
        if _path_value(payload, path) != expected:
            raise OperationalSafetyConfigError(f"{label} safety value drifted: {path}")
    _validate_no_collection(payload, label)
    return _ConfigDocument(payload=payload, raw=raw)


def _timestamp(value: str | None) -> str:
    timestamp = value or datetime.now(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    if not timestamp.endswith("Z"):
        raise OperationalSafetyConfigError("generated_utc must use UTC Z notation")
    try:
        parsed = datetime.fromisoformat(timestamp[:-1] + "+00:00")
    except ValueError as exc:
        raise OperationalSafetyConfigError("generated_utc is invalid") from exc
    if parsed.tzinfo != UTC:
        raise OperationalSafetyConfigError("generated_utc is not UTC")
    return timestamp


def _receipt_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _reject_absolute_locators(value: Any) -> None:
    if isinstance(value, Mapping):
        for child in value.values():
            _reject_absolute_locators(child)
    elif isinstance(value, list):
        for child in value:
            _reject_absolute_locators(child)
    elif isinstance(value, str) and (
        value.startswith(("/", "file://"))
        or ":\\" in value
        or value.startswith(("~/", "ssh://"))
    ):
        raise OperationalSafetyConfigError("receipt contains an absolute locator")


def build_config_pair(
    *,
    predecessor_disabled_path: Path,
    predecessor_active_path: Path,
    generated_utc: str | None = None,
) -> ConfigPairBuild:
    """Build and validate the complete pair before performing any write."""

    disabled_parent = _open_predecessor(
        predecessor_disabled_path,
        "predecessor disabled config",
        PREDECESSOR_DISABLED,
    )
    active_parent = _open_predecessor(
        predecessor_active_path,
        "predecessor active config",
        PREDECESSOR_ACTIVE,
    )
    explicit_false_paths = _validate_predecessor_pair(disabled_parent, active_parent)
    disabled = _successor_document(disabled_parent, "successor disabled config")
    active = _successor_document(active_parent, "successor active config")
    disabled_binding = _binding(disabled)
    active_binding = _binding(active)
    if disabled_binding != dict(SUCCESSOR_DISABLED):
        raise OperationalSafetyConfigError("successor disabled frozen identity drifted")
    if active_binding != dict(SUCCESSOR_ACTIVE):
        raise OperationalSafetyConfigError("successor active frozen identity drifted")
    if _leaf_diff(disabled.payload, active.payload) != [PAIR_DIFFERENCE]:
        raise OperationalSafetyConfigError("successor pair differs outside BUY E3 enablement")
    if _path_value(disabled.payload, PAIR_DIFFERENCE) is not False:
        raise OperationalSafetyConfigError("successor disabled config does not select B0")
    if _path_value(active.payload, PAIR_DIFFERENCE) is not True:
        raise OperationalSafetyConfigError("successor active config does not enable BUY E3")

    receipt: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER,
        "status": STATUS,
        "visibility": VISIBILITY,
        "generated_utc": _timestamp(generated_utc),
        "predecessor_config_pair": {
            "disabled": dict(PREDECESSOR_DISABLED),
            "active": dict(PREDECESSOR_ACTIVE),
            "active_disabled_only_difference": PAIR_DIFFERENCE,
        },
        "successor_config_pair": {
            "disabled": {"filename": DISABLED_FILENAME, **disabled_binding},
            "active": {"filename": ACTIVE_FILENAME, **active_binding},
            "active_disabled_only_difference": PAIR_DIFFERENCE,
        },
        "semantic_contract": {
            "predecessor_to_successor_changed_paths": list(SUCCESSOR_CHANGES),
            "explicit_safety_values": dict(EXPLICIT_SAFETY_VALUES),
            "explicit_false_collection_activation_paths": explicit_false_paths,
            "all_other_semantic_values_preserved": True,
            "latent_source_and_record_content_selectors_preserved": True,
            "no_companion_activation_field_present": True,
            "release_authority_embedded_in_yaml": False,
        },
        "publication_contract": {
            "output_files": list(PUBLICATION_ORDER),
            "private_mode": "0600",
            "directory_mode": "0700",
            "immutable_create_only": True,
            "receipt_published_last": True,
            "recovery_requires_exact_prefix": True,
            "mismatched_or_out_of_order_recovery_forbidden": True,
        },
        "permissions": dict(PERMISSIONS),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    _reject_absolute_locators(receipt)
    receipt[CANONICAL_FIELD] = canonical_sha256(receipt, CANONICAL_FIELD)
    return ConfigPairBuild(
        disabled_raw=disabled.raw,
        active_raw=active.raw,
        receipt=receipt,
        receipt_raw=_receipt_bytes(receipt),
    )


def _reject_symlink_components(path: Path, label: str) -> None:
    candidate = path.expanduser().absolute()
    current = Path(candidate.anchor)
    for component in candidate.parts[1:]:
        current /= component
        try:
            info = current.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(info.st_mode):
            raise OperationalSafetyConfigError(f"{label} contains a symlink component")


def _ensure_output_directory(path: Path, *, create: bool) -> tuple[Path, int]:
    directory = path.expanduser().absolute()
    _reject_symlink_components(directory.parent, "output directory parent")
    created = False
    if create:
        try:
            os.mkdir(directory, 0o700)
            created = True
        except FileExistsError:
            pass
        except FileNotFoundError as exc:
            raise OperationalSafetyConfigError(
                "output directory parent is unavailable"
            ) from exc
    elif not directory.exists() and not directory.is_symlink():
        raise OperationalSafetyConfigError("output directory is unavailable")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError as exc:
        raise OperationalSafetyConfigError("output directory could not be opened safely") from exc
    info = os.fstat(descriptor)
    if created:
        os.fchmod(descriptor, 0o700)
        info = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(info.st_mode)
        or stat.S_IMODE(info.st_mode) != 0o700
        or info.st_uid != os.geteuid()
    ):
        os.close(descriptor)
        raise OperationalSafetyConfigError("output directory is not owner-private 0700")
    _reject_symlink_components(directory, "output directory")
    return directory, descriptor


def _read_private_artifact(
    path: Path,
    label: str,
    *,
    allowed_links: frozenset[int] = frozenset({1}),
) -> tuple[bytes, os.stat_result]:
    _reject_symlink_components(path, label)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise OperationalSafetyConfigError(f"{label} could not be opened safely") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.geteuid()
            or before.st_nlink not in allowed_links
            or before.st_size > MAX_PRIVATE_FILE_BYTES
        ):
            raise OperationalSafetyConfigError(f"{label} private file identity drifted")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise OperationalSafetyConfigError(f"{label} was truncated while read")
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
            before.st_nlink,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
            after.st_nlink,
        ):
            raise OperationalSafetyConfigError(f"{label} changed while read")
        return raw, after
    finally:
        os.close(descriptor)


def _pending_path(final_path: Path, raw: bytes) -> Path:
    return final_path.with_name(f".{final_path.name}.pending.{file_sha256_bytes(raw)[:16]}")


def _unexpected_pending(final_path: Path, expected_pending: Path) -> list[Path]:
    prefix = f".{final_path.name}.pending."
    return sorted(
        path
        for path in final_path.parent.iterdir()
        if path.name.startswith(prefix) and path != expected_pending
    )


def _preflight_artifact(path: Path, raw: bytes, label: str) -> tuple[bool, bool]:
    pending = _pending_path(path, raw)
    if _unexpected_pending(path, pending):
        raise OperationalSafetyConfigError(f"{label} has an unexpected pending artifact")
    final_exists = path.exists() or path.is_symlink()
    pending_exists = pending.exists() or pending.is_symlink()
    final_info: os.stat_result | None = None
    pending_info: os.stat_result | None = None
    if final_exists:
        observed, final_info = _read_private_artifact(
            path,
            label,
            allowed_links=frozenset({1, 2}),
        )
        if observed != raw:
            raise OperationalSafetyConfigError(f"{label} create-only conflict")
    if pending_exists:
        observed, pending_info = _read_private_artifact(
            pending,
            f"{label} pending",
            allowed_links=frozenset({1, 2}),
        )
        if observed != raw:
            raise OperationalSafetyConfigError(f"{label} pending bytes drifted")
    if final_info is not None and pending_info is not None:
        if (
            final_info.st_dev,
            final_info.st_ino,
            final_info.st_nlink,
        ) != (
            pending_info.st_dev,
            pending_info.st_ino,
            2,
        ):
            raise OperationalSafetyConfigError(f"{label} final/pending recovery is ambiguous")
    if final_info is not None and pending_info is None and final_info.st_nlink != 1:
        raise OperationalSafetyConfigError(f"{label} final hard-link count drifted")
    if final_info is None and pending_info is not None and pending_info.st_nlink != 1:
        raise OperationalSafetyConfigError(f"{label} pending hard-link count drifted")
    return final_exists, pending_exists


def _preflight_publication(directory: Path, build: ConfigPairBuild) -> None:
    artifacts = (
        (directory / DISABLED_FILENAME, build.disabled_raw, "successor disabled config"),
        (directory / ACTIVE_FILENAME, build.active_raw, "successor active config"),
        (directory / RECEIPT_FILENAME, build.receipt_raw, "config pair receipt"),
    )
    states = [_preflight_artifact(path, raw, label) for path, raw, label in artifacts]
    effective = [final or pending for final, pending in states]
    if effective[1] and not states[0][0]:
        raise OperationalSafetyConfigError("active publication precedes disabled final")
    if effective[2] and not (states[0][0] and states[1][0]):
        raise OperationalSafetyConfigError("receipt publication precedes config finals")


def _write_all(descriptor: int, raw: bytes) -> None:
    view = memoryview(raw)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OperationalSafetyConfigError("create-only pending write made no progress")
        view = view[written:]


def _publish_one(path: Path, raw: bytes, label: str, directory_fd: int) -> str:
    pending = _pending_path(path, raw)
    final_exists, pending_exists = _preflight_artifact(path, raw, label)
    if final_exists:
        if pending_exists:
            os.unlink(pending)
            os.fsync(directory_fd)
            observed, info = _read_private_artifact(path, label)
            if observed != raw or info.st_nlink != 1:
                raise OperationalSafetyConfigError(f"{label} link recovery failed")
            return "exact_final_pending_link_recovered"
        return "exact_existing_reused"

    if not pending_exists:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = -1
        created = False
        try:
            descriptor = os.open(pending, flags, 0o600)
            created = True
            os.fchmod(descriptor, 0o600)
            _write_all(descriptor, raw)
            os.fsync(descriptor)
        except BaseException:
            if descriptor >= 0:
                os.close(descriptor)
                descriptor = -1
            if created:
                pending.unlink(missing_ok=True)
            raise
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        os.fsync(directory_fd)
        semantics = "first_writer"
    else:
        semantics = "exact_pending_recovered"

    try:
        os.link(pending, path, follow_symlinks=False)
    except OSError as exc:
        raise OperationalSafetyConfigError(f"{label} create-only link failed") from exc
    os.fsync(directory_fd)
    os.unlink(pending)
    os.fsync(directory_fd)
    observed, info = _read_private_artifact(path, label)
    if observed != raw or info.st_nlink != 1:
        raise OperationalSafetyConfigError(f"{label} publication verification failed")
    return semantics


def _content_binding(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    raw, info = _read_private_artifact(path, "config pair receipt")
    return {
        "file_sha256": file_sha256_bytes(raw),
        "canonical_field": CANONICAL_FIELD,
        "canonical_sha256": payload.get(CANONICAL_FIELD),
        "size_bytes": info.st_size,
        "mode": "0600",
    }


def finalize_config_pair(
    *,
    predecessor_disabled_path: Path,
    predecessor_active_path: Path,
    output_dir: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    """Publish an exact recoverable prefix and receipt last, without overwrite."""

    build = build_config_pair(
        predecessor_disabled_path=predecessor_disabled_path,
        predecessor_active_path=predecessor_active_path,
        generated_utc=generated_utc,
    )
    directory, descriptor = _ensure_output_directory(output_dir, create=True)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        _preflight_publication(directory, build)
        semantics = {
            "disabled": _publish_one(
                directory / DISABLED_FILENAME,
                build.disabled_raw,
                "successor disabled config",
                descriptor,
            ),
            "active": _publish_one(
                directory / ACTIVE_FILENAME,
                build.active_raw,
                "successor active config",
                descriptor,
            ),
            "receipt": _publish_one(
                directory / RECEIPT_FILENAME,
                build.receipt_raw,
                "config pair receipt",
                descriptor,
            ),
        }
    finally:
        os.close(descriptor)
    return {
        "receipt": build.receipt,
        "receipt_binding": _content_binding(directory / RECEIPT_FILENAME, build.receipt),
        "publication_semantics": semantics,
    }


def _load_receipt(path: Path) -> tuple[dict[str, Any], bytes]:
    raw, _info = _read_private_artifact(path, "config pair receipt")
    try:
        payload = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise OperationalSafetyConfigError("config pair receipt is unreadable JSON") from exc
    if not isinstance(payload, dict):
        raise OperationalSafetyConfigError("config pair receipt root is not a mapping")
    _reject_absolute_locators(payload)
    if payload.get(CANONICAL_FIELD) != canonical_sha256(payload, CANONICAL_FIELD):
        raise OperationalSafetyConfigError("config pair receipt canonical identity drifted")
    return payload, raw


def validate_config_pair(
    *,
    predecessor_disabled_path: Path,
    predecessor_active_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    """Reopen and validate the materialized pair and portable receipt."""

    directory, descriptor = _ensure_output_directory(output_dir, create=False)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        receipt, receipt_raw = _load_receipt(directory / RECEIPT_FILENAME)
        generated_utc = receipt.get("generated_utc")
        if not isinstance(generated_utc, str):
            raise OperationalSafetyConfigError("config pair receipt timestamp drifted")
        expected = build_config_pair(
            predecessor_disabled_path=predecessor_disabled_path,
            predecessor_active_path=predecessor_active_path,
            generated_utc=generated_utc,
        )
        observed_disabled, _ = _read_private_artifact(
            directory / DISABLED_FILENAME, "successor disabled config"
        )
        observed_active, _ = _read_private_artifact(
            directory / ACTIVE_FILENAME, "successor active config"
        )
        if observed_disabled != expected.disabled_raw:
            raise OperationalSafetyConfigError("successor disabled config mutated")
        if observed_active != expected.active_raw:
            raise OperationalSafetyConfigError("successor active config mutated")
        if receipt != expected.receipt or receipt_raw != expected.receipt_raw:
            raise OperationalSafetyConfigError("config pair receipt semantic identity drifted")
        _preflight_publication(directory, expected)
    finally:
        os.close(descriptor)
    return {
        "receipt": receipt,
        "receipt_binding": _content_binding(directory / RECEIPT_FILENAME, receipt),
        "config_pair": receipt["successor_config_pair"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("finalize", "validate"))
    parser.add_argument("--predecessor-disabled", type=Path, required=True)
    parser.add_argument("--predecessor-active", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--generated-utc")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    kwargs = {
        "predecessor_disabled_path": args.predecessor_disabled,
        "predecessor_active_path": args.predecessor_active,
        "output_dir": args.output_dir,
    }
    if args.command == "finalize":
        result = finalize_config_pair(generated_utc=args.generated_utc, **kwargs)
        print(f"receipt_file_sha256={result['receipt_binding']['file_sha256']}")
        print(f"receipt_canonical_sha256={result['receipt_binding']['canonical_sha256']}")
    else:
        if args.generated_utc is not None:
            raise OperationalSafetyConfigError("validate does not accept --generated-utc")
        result = validate_config_pair(**kwargs)
        print(result["receipt_binding"]["canonical_sha256"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
