#!/usr/bin/env python3
"""Plan or publish the fully no-shadow BUY E3 metadata-v6 successor.

This is additive operational evidence plumbing.  It does not contact a live
host, read economic outcomes, or alter a strategy.  ``prepare-manifest`` is the
one create-only durable evidence write and publishes only the recursively
validated formal manifest.  ``run`` remains write-free unless ``--apply`` is
explicitly supplied.  Runtime/action authority remains the immutable direct
owner release v3.  The metadata receipt, pointer, and catalog only resolve the
already-admitted evidence chain; they never replace that authority.

All current source identities are frozen in this module.  Pointer health is
anchored only to the durable post-lifecycle receipt and its two source-frozen
HEALTH rows.  This module does not invent a later heartbeat or "latest live"
authority.

The three-file publication is an ordered, resumable transaction:

    immutable replacement receipt -> mutable current pointer -> mutable catalog

Each individual publication is atomic.  The receipt is create-only, and its
deterministic pending hardlink can be repaired after a crash without accepting
an nlink=2 final identity.  Reversed or otherwise mixed publication order is
rejected fail closed.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from scripts import audit_private_evidence, live_remote_pointer
from scripts import f05_buy_e3_cross_host_transport_v6 as transport_v6
from scripts import f05_buy_e3_final_evidence_v4 as historical_final_v4
from scripts import f05_buy_e3_final_evidence_v6 as final_v6
from scripts import f05_buy_e3_lifecycle_context_v1 as lifecycle_context_v1
from scripts import f05_buy_e3_no_shadow_post_release_config_correction as config_correction_v1
from scripts import f05_buy_e3_post_lifecycle_live_health_v1 as post_lifecycle_v1

MANIFEST_SCHEMA: Final = "f05_buy_e3_operational_metadata_v6_activation_manifest.v1"
MANIFEST_STATUS: Final = "release_v3_no_shadow_metadata_inputs_frozen"
MANIFEST_CANONICAL_FIELD: Final = "canonical_operational_metadata_manifest_sha256"

RECEIPT_SCHEMA: Final = "narrowgate.live_replacement_activation_receipt.v3"
RECEIPT_STATUS: Final = "completed_active_release_v3_no_shadow_evidence_closed"
RECEIPT_CANONICAL_FIELD: Final = "canonical_replacement_activation_receipt_sha256"
PREDECESSOR_RECEIPT_SCHEMA: Final = "narrowgate.live_replacement_activation_receipt.v1"
PREDECESSOR_RECEIPT_STATUS: Final = "completed_active_direct_v4_evidence_closed"

POINTER_SCHEMA: Final = "narrowgate_live_remote_pointer.v1"
CATALOG_SCHEMA: Final = "narrowgate_private_artifact_catalog_v1"
SUPERSEDED_REASON: Final = "superseded_due_global_shadow_runtime_enabled"

EXECUTION: Final = {
    "execution_commit": "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de",
    "execution_tree": "0343bd5586b337385cf2aa0d7a643f5c32b0da77",
    "annotated_operational_tag": "f05-owner-buy-e3-no-shadow-runtime-v3-20260824",
    "annotated_operational_tag_object": "3878ea05252ef8f274b6f74ee7a984431c53b892",
    "tag_peeled_commit": "eacb6ccb1f4437d99d8385ba3f46ba6012f5c1de",
}
RELEASE: Final = {
    "schema_version": transport_v6.FROZEN_FINAL_RELEASE_SCHEMA,
    "status": transport_v6.FROZEN_FINAL_RELEASE_STATUS,
    "file_sha256": "310d86d30bd875a7029b3e2f784877c6802ab7b05b0f639383e68bb81a458f49",
    "canonical_field": "canonical_active_release_sha256",
    "canonical_sha256": "81d4449301d29828162a7fb57f52c855803682a697639b6d9cfa2d38a2846b8f",
    "size_bytes": 15_386,
    "mode": "0600",
}
ACTIVE_CONFIG_SHA256: Final = "3d8463c47c1cc2ff2017c9f6e7a963c77a8edb0cc692c48d89b03ee09bff772e"
DISABLED_CONFIG_SHA256: Final = "d92fdec7ce89586f56fb1a6c80a6bc6fbe96b50023bd8c481cae730606c75204"
ARTIFACT_SHA256: Final = "17e99df737157c6587602e6b496eadbecbed0a98d025da1d1db4cc8ef670786d"
CURRENT_PID: Final = 90_462
CURRENT_PID_START_TICKS: Final = 6_717_209
CURRENT_EPOCH_ID: Final = "prospective-1787568574639266387-ac669869e7ed"
CURRENT_EPOCH_STARTED_UTC: Final = "2026-08-24T10:49:34.639266387Z"
PREDECESSOR_V4_LAST_EVIDENCED_UTC: Final = "2026-08-24T05:02:05Z"
ACTIVE_RUNTIME_IDENTITY_RECORDED_UTC: Final = "2026-08-24T10:49:34.457092Z"
ACTIVE_CAPTURED_UTC: Final = "2026-08-24T10:50:22.583892Z"
POST_LIFECYCLE_RECEIPT_GENERATED_UTC: Final = "2026-08-24T13:32:42.644563Z"
FORMAL_RECEIPT_ID: Final = "f05-buy-e3-no-shadow-operational-metadata-v6-20260824-v1"
MANIFEST_MAX_CLOCK_SKEW_SECONDS: Final = 5
CURRENT_HOST_CORE: Final = {
    "provider": "aws",
    "region": "ap-northeast-1",
    "public_ipv4": "13.158.101.253",
    "instance_id": "i-00fe03a8b2fb49a31",
    "instance_type": "c7i-flex.large",
}

NO_NEW_AUTHORITY: Final = {"research": False, "action": False, "live": False}
EVIDENCE_BOUNDARY: Final = {
    "economic_outcomes_read": False,
    "economic_values_persisted": False,
    "validation_read": False,
    "sealed_holdout_read": False,
    "new_economic_arm_run": False,
    "shadow_created": False,
    "companion_created": False,
    "shadow_or_companion_collection_enabled": False,
    "hypothetical_live_actions_scored": False,
}

CONTENT_FIELDS: Final = (
    "path",
    "schema_version",
    "status",
    "file_sha256",
    "canonical_field",
    "canonical_sha256",
    "size_bytes",
    "mode",
)
CONTENT_FIELDS_NO_PATH: Final = CONTENT_FIELDS[1:]
PUBLISHER_FIELDS: Final = (
    "module_route",
    "annotated_tag",
    "annotated_tag_object",
    "commit",
    "tree",
    "script_sha256",
)
PUBLISHER_MODULE_ROUTE: Final = "scripts.f05_closeout_operational_metadata_v6"
PUBLISHER_TAG: Final = "f05-owner-buy-e3-no-shadow-operational-metadata-v6-collector-v1-20260824"
RECEIPT_FILENAME: Final = (
    "live_remote_replacement_activation_aws_tokyo_13_158_101_253_"
    "i_00fe03a8b2fb49a31_buy_e3_no_shadow_v6_20260824_v1.json"
)
PREDECESSOR_RECEIPT_FILENAME: Final = (
    "live_remote_replacement_activation_aws_tokyo_13_158_101_253_"
    "i_00fe03a8b2fb49a31_buy_e3_v4_20260824_v1.json"
)
PREDECESSOR_ARTIFACT_ID: Final = (
    "repository-live-replacement-activation-aws-tokyo-buy-e3-v4-20260824-v1"
)
REPLACEMENT_ARTIFACT_ID: Final = (
    "repository-live-replacement-activation-aws-tokyo-buy-e3-no-shadow-v6-20260824-v1"
)
FROZEN_PREDECESSOR: Final = {
    "pointer": {
        "file_sha256": "c8a9d9121529daef0bdc9ab8d2bdd0686837b5907693843dd9d1b95f5eba5d71",
        "size_bytes": 27_934,
    },
    "catalog": {
        "file_sha256": "a39aec408e16270a9408fd437701d7345b0a82bb4df723057c1a948280e434fb",
        "size_bytes": 1_388_541,
    },
    "activation": {
        "schema_version": PREDECESSOR_RECEIPT_SCHEMA,
        "status": PREDECESSOR_RECEIPT_STATUS,
        "file_sha256": "5390a971bca7c1b223d17b6f0fa5b2e19cf36b124882f30f69670faa7b0f99f8",
        "canonical_field": RECEIPT_CANONICAL_FIELD,
        "canonical_sha256": "10c803df51aaf3b31a3adefe74628616a452040e9d9912fa8d16f88d8d67d773",
        "size_bytes": 20_420,
        "mode": "0600",
    },
}

EVIDENCE_ROOT: Final = (
    "/Volumes/ORICO/MarketData/NarrowGate_BTCUSDC/reports/f05_owner_buy_e3_v1/"
    "direct_no_shadow_live_evidence_v6_20260824"
)
LIFECYCLE_ADMISSION_PATH: Final = (
    "/Volumes/ORICO/MarketData/NarrowGate_BTCUSDC/formal_collection/"
    "prospective_lifecycle_journal_v2/"
    "session-prospective-1787568574639266387-ac669869e7ed/admission_manifest.json"
)
FORMAL_MANIFEST_PATH: Final = f"{EVIDENCE_ROOT}/operational_metadata/activation_manifest_v6.json"


def _source(
    path: str,
    schema: str,
    status: str | None,
    file_sha256: str,
    canonical_field: str,
    canonical_sha256: str,
    size_bytes: int,
    mode: str = "0600",
) -> dict[str, Any]:
    return {
        "path": path,
        "schema_version": schema,
        "status": status,
        "file_sha256": file_sha256,
        "canonical_field": canonical_field,
        "canonical_sha256": canonical_sha256,
        "size_bytes": size_bytes,
        "mode": mode,
    }


FROZEN_SOURCES: Final = {
    "direct_release": _source(
        f"{EVIDENCE_ROOT}/authority_sources/direct_owner_active_release_v3.json",
        str(RELEASE["schema_version"]),
        str(RELEASE["status"]),
        str(RELEASE["file_sha256"]),
        str(RELEASE["canonical_field"]),
        str(RELEASE["canonical_sha256"]),
        int(RELEASE["size_bytes"]),
    ),
    "cross_host_admission": _source(
        f"{EVIDENCE_ROOT}/cross_host_admission/cross_host_admission.json",
        transport_v6.ADMISSION_SCHEMA,
        transport_v6.ADMISSION_STATUS,
        "78cff62bab68ead22fcc21ba40b4a69d96c9f3d452db4f1a6f7dc24bdaba00fd",
        transport_v6.ADMISSION_CANONICAL_FIELD,
        "24f9e2e7f92f29e35fc86692e53a1dd0e899ecd5b78d5e160e7ccb5a2bdfdb64",
        27_860,
    ),
    "config_correction": _source(
        f"{EVIDENCE_ROOT}/cross_host_admission/config_correction.json",
        config_correction_v1.SCHEMA_VERSION,
        config_correction_v1.STATUS,
        transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_FILE_SHA256,
        config_correction_v1.CANONICAL_FIELD,
        transport_v6.FROZEN_FINAL_CONFIG_CORRECTION_CANONICAL_SHA256,
        4_226,
    ),
    "resource_gate": _source(
        f"{EVIDENCE_ROOT}/cross_host_admission/current_host_resource_gate.json",
        transport_v6.FROZEN_FINAL_RESOURCE_SCHEMA,
        transport_v6.FROZEN_FINAL_RESOURCE_STATUS,
        transport_v6.FROZEN_FINAL_RESOURCE_FILE_SHA256,
        "canonical_resource_receipt_sha256",
        transport_v6.FROZEN_FINAL_RESOURCE_CANONICAL_SHA256,
        24_264,
    ),
    "active_process_capture": _source(
        f"{EVIDENCE_ROOT}/cross_host_admission/active_process_capture.json",
        transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_SCHEMA,
        transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_STATUS,
        transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_FILE_SHA256,
        "canonical_active_capture_sha256",
        transport_v6.FROZEN_FINAL_ACTIVE_CAPTURE_CANONICAL_SHA256,
        41_099,
    ),
    "remote_active_attestation": _source(
        f"{EVIDENCE_ROOT}/cross_host_admission/remote_active_attestation.json",
        transport_v6.REMOTE_ATTESTATION_SCHEMA,
        transport_v6.REMOTE_ATTESTATION_STATUS,
        "1aa7915b0f7c75a32af0a14b26c7487ae116907381f4cccf50223401852aba3c",
        transport_v6.REMOTE_ATTESTATION_CANONICAL_FIELD,
        "2ce16d821c61c840dcda537efb833dbec58268de47e1672ac3a112a37391db9b",
        25_390,
    ),
    "lifecycle_admission": _source(
        LIFECYCLE_ADMISSION_PATH,
        final_v6.base.LIFECYCLE_SCHEMA,
        None,
        "8b2c08b49bb2f4c272b958b3f3ed3e7f47c914577267fec45c48fe6052a17aaf",
        "admission_identity_sha256",
        "50afb8064a43a81a92388766b5b4c0e31ae8d768e017da11d7ccdcf12507878d",
        2_469,
        "0644",
    ),
    "lifecycle_context": _source(
        f"{EVIDENCE_ROOT}/lifecycle_context/lifecycle_admission_context.json",
        lifecycle_context_v1.SCHEMA_VERSION,
        lifecycle_context_v1.STATUS,
        "a00e2c7f3e45cb5c338bb25b76624fd32ff3a82d28cb73466e9046f7fd183387",
        lifecycle_context_v1.CANONICAL_FIELD,
        "f3282651f55a63576242b178c4a8b95cb82ce14c88eab47498518779c8bc86d3",
        14_854,
    ),
    "post_lifecycle_health": _source(
        f"{EVIDENCE_ROOT}/post_lifecycle_health/post_lifecycle_live_health.json",
        post_lifecycle_v1.SCHEMA_VERSION,
        post_lifecycle_v1.STATUS,
        "4dd90fdc9be0d00d378256bbfca490186382e05229c63060cc2ec073082cc0b4",
        post_lifecycle_v1.CANONICAL_FIELD,
        "d9204c4bdba7a14fbd71d2c1186ffc685ce496f1dc13925cf6145bc95656b417",
        43_200,
    ),
    "final_activation_envelope": _source(
        f"{EVIDENCE_ROOT}/final_evidence_chain/activation_envelope_v6.json",
        final_v6.ENVELOPE_SCHEMA,
        final_v6.ENVELOPE_STATUS,
        "8d3f0e5fe1cc6a0e464a7e674222412911f99cdbbc2430d97274e299d7acfb84",
        final_v6.ENVELOPE_CANONICAL_FIELD,
        "1b9f08beb37784c8a33ad10598b58386716d807268f410bbb41f2d6e15bb61fb",
        40_594,
    ),
    "final_operational_completion": _source(
        f"{EVIDENCE_ROOT}/final_evidence_chain/operational_completion_v6.json",
        final_v6.COMPLETION_SCHEMA,
        final_v6.COMPLETION_STATUS,
        "aa8c185a1a07623a4f8166a7a721544cbef36366527a481867ec63dac7ee84e7",
        final_v6.COMPLETION_CANONICAL_FIELD,
        "8841fbd226b4286ecd764ad9de08f31a54ba2d1c70999ed9ae385f0b1c3eb1b3",
        65_398,
    ),
    "final_composition": _source(
        f"{EVIDENCE_ROOT}/final_evidence_chain/final_composition_v6.json",
        final_v6.COMPOSITION_SCHEMA,
        final_v6.COMPOSITION_STATUS,
        "5de62535139ab4ce16ec1326e36935f7af7f604f987f6a92790e9576aec41e16",
        final_v6.COMPOSITION_CANONICAL_FIELD,
        "9131a2792ad529370db7d9bb3a5ccc62be7a06ea68b000856bff6e89edd02902",
        47_785,
    ),
    "final_attempt": _source(
        f"{EVIDENCE_ROOT}/final_evidence_chain/operational_attempt_final_v6.json",
        final_v6.ATTEMPT_FINAL_SCHEMA,
        final_v6.ATTEMPT_FINAL_STATUS,
        "df92b322edb8c96fe84c9ba3e3e522ed66cb4a6bf4200be58c7ad70a418d581d",
        final_v6.ATTEMPT_FINAL_CANONICAL_FIELD,
        "8445d6edd40989216718a6d92c6aa908008c3e387c7d9d350d61c2924697d23c",
        43_127,
    ),
    "final_proof": _source(
        f"{EVIDENCE_ROOT}/final_evidence_chain/proof_evidence_release_v6.json",
        final_v6.EVIDENCE_RELEASE_SCHEMA,
        final_v6.EVIDENCE_RELEASE_STATUS,
        "44216df8d0dc560e017a74c5bbb1d137aa8847fbfb6e6b292c29bbaa56132618",
        final_v6.EVIDENCE_RELEASE_CANONICAL_FIELD,
        "b004a5792494a4927ba3b25c7df37666d8a457ac2b3952e17db5c3c83082ff86",
        44_308,
    ),
    "historical_v4_release": _source(
        f"{EVIDENCE_ROOT}/authority_sources/historical_v4_direct_owner_active_release_v2.json",
        "causal_multichannel_window_boolean_cooldown_owner_buy_e3_direct_owner_active_release.v2",
        "owner_authorized_direct_live_lifecycle_repair_pending_evidence",
        "ff888f4b5973563275c2b97e1554d45c9d686ef15d686440bf096521aab17fc2",
        "canonical_active_release_sha256",
        "823ca1e4d53e968eb0afc53c4d2cad99cc17aac696548baa1700e800a4579702",
        7_757,
    ),
    "historical_v4_proof": _source(
        "/Volumes/ORICO/MarketData/NarrowGate_BTCUSDC/reports/f05_owner_buy_e3_v1/"
        "direct_v4_live_evidence_20260824/final_evidence_chain/"
        "proof_evidence_release_v4.json",
        historical_final_v4.EVIDENCE_RELEASE_SCHEMA,
        historical_final_v4.EVIDENCE_RELEASE_STATUS,
        "0f85849289cb9e42de7333117c7719e1a95a1561cf42e8a00366e0e8500df28f",
        historical_final_v4.EVIDENCE_RELEASE_CANONICAL_FIELD,
        "21ca796d12c0df733e3c1daba2fe8e326979ec0bb80b8b653275e14a6af97880",
        8_031,
    ),
}

SOURCE_IDENTITIES: Final = {
    role: (binding["schema_version"], binding["status"], binding["canonical_field"])
    for role, binding in FROZEN_SOURCES.items()
}

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_GIT_OID_RE: Final = re.compile(r"^[0-9a-f]{40}$")
MAX_JSON_BYTES: Final = 64 << 20


class OperationalMetadataV6Error(RuntimeError):
    """Raised when any input or transaction state fails closed."""


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return _sha(encoded)


def _document_sha(payload: Mapping[str, Any], field: str) -> str:
    projected = dict(payload)
    projected.pop(field, None)
    return _canonical(projected)


def _render(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, indent=2, ensure_ascii=False, allow_nan=False) + "\n").encode()


def _timestamp_ns(value: Any, label: str) -> int:
    if not isinstance(value, str):
        raise OperationalMetadataV6Error(f"{label} must be an explicit UTC Z timestamp")
    matched = re.fullmatch(
        r"(?P<whole>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(?P<fraction>\d{1,9}))?Z",
        value,
    )
    if matched is None:
        raise OperationalMetadataV6Error(f"{label} must be an explicit UTC Z timestamp")
    try:
        parsed = datetime.strptime(matched.group("whole"), "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
    except ValueError as exc:
        raise OperationalMetadataV6Error(f"{label} is invalid") from exc
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = parsed - epoch
    seconds = delta.days * 86_400 + delta.seconds
    fraction = (matched.group("fraction") or "").ljust(9, "0")
    return seconds * 1_000_000_000 + int(fraction or "0")


def _timestamp(value: Any, label: str) -> str:
    _timestamp_ns(value, label)
    return value


def _now_utc_ns() -> int:
    observed = datetime.now(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = observed - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


def _validate_manifest_generated_utc(value: Any) -> str:
    generated_ns = _timestamp_ns(value, "manifest generated_utc")
    lower_bound_ns = _timestamp_ns(
        POST_LIFECYCLE_RECEIPT_GENERATED_UTC,
        "post-lifecycle receipt generated_utc",
    )
    if generated_ns < lower_bound_ns:
        raise OperationalMetadataV6Error(
            "manifest generated_utc precedes the frozen post-lifecycle receipt"
        )
    if generated_ns > _now_utc_ns() + MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000:
        raise OperationalMetadataV6Error("manifest generated_utc is in the future")
    return str(value)


def _validate_manifest_creation_utc(value: Any) -> str:
    generated_ns = _timestamp_ns(value, "manifest generated_utc")
    _validate_manifest_generated_utc(value)
    if abs(_now_utc_ns() - generated_ns) > MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000:
        raise OperationalMetadataV6Error(
            "manifest generated_utc is not contemporaneous with creation"
        )
    return str(value)


def _nanosecond_utc(value: Any, label: str) -> str:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise OperationalMetadataV6Error(f"{label} must be a nonnegative integer")
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    whole = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{whole}.{nanoseconds:09d}Z"


def _utc_z(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise OperationalMetadataV6Error(f"{label} must be a timestamp string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OperationalMetadataV6Error(f"{label} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise OperationalMetadataV6Error(f"{label} is not UTC")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "--no-optional-locks", "-C", str(root), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise OperationalMetadataV6Error(
            f"publisher git check failed ({' '.join(args)}): {result.stderr.strip()}"
        )
    return result.stdout.strip()


def _observe_publisher_checkout(root: Path) -> dict[str, Any]:
    tag = PUBLISHER_TAG
    tag_object = _git(root, "rev-parse", f"refs/tags/{tag}")
    if _git(root, "cat-file", "-t", tag_object) != "tag":
        raise OperationalMetadataV6Error("publisher tag is not annotated")
    observed = {
        "module_route": PUBLISHER_MODULE_ROUTE,
        "annotated_tag": tag,
        "annotated_tag_object": tag_object,
        "commit": _git(root, "rev-parse", f"refs/tags/{tag}^{{commit}}"),
        "tree": _git(root, "rev-parse", f"refs/tags/{tag}^{{tree}}"),
    }
    if _git(root, "rev-parse", "HEAD") != observed["commit"]:
        raise OperationalMetadataV6Error("publisher checkout HEAD differs from frozen tag")
    if _git(root, "rev-parse", "HEAD^{tree}") != observed["tree"]:
        raise OperationalMetadataV6Error("publisher checkout tree differs from frozen tag")
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all"):
        raise OperationalMetadataV6Error("publisher checkout is not clean")
    script_path = root / "scripts" / "f05_closeout_operational_metadata_v6.py"
    if script_path.resolve() != Path(__file__).resolve():
        raise OperationalMetadataV6Error("publisher module was not loaded from frozen checkout")
    direct_modules = {
        audit_private_evidence: "scripts/audit_private_evidence.py",
        live_remote_pointer: "scripts/live_remote_pointer.py",
        transport_v6: "scripts/f05_buy_e3_cross_host_transport_v6.py",
        historical_final_v4: "scripts/f05_buy_e3_final_evidence_v4.py",
        final_v6: "scripts/f05_buy_e3_final_evidence_v6.py",
        lifecycle_context_v1: "scripts/f05_buy_e3_lifecycle_context_v1.py",
        config_correction_v1: "scripts/f05_buy_e3_no_shadow_post_release_config_correction.py",
        post_lifecycle_v1: "scripts/f05_buy_e3_post_lifecycle_live_health_v1.py",
    }
    for module, relative in direct_modules.items():
        origin = getattr(module, "__file__", None)
        if not isinstance(origin, str) or Path(origin).resolve() != (root / relative).resolve():
            raise OperationalMetadataV6Error(
                f"publisher validator module origin drifted: {relative}"
            )
    resolved_root = root.resolve()
    for name, module in tuple(sys.modules.items()):
        if not (name.startswith("scripts.") or name.startswith("research.")):
            continue
        origin = getattr(module, "__file__", None)
        if not isinstance(origin, str):
            continue
        try:
            Path(origin).resolve().relative_to(resolved_root)
        except ValueError as exc:
            raise OperationalMetadataV6Error(
                f"publisher transitive validator module origin drifted: {name}"
            ) from exc
    script_raw, _metadata = _read_regular(script_path, mode=0o644)
    observed["script_sha256"] = _sha(script_raw)
    return observed


def _publisher_checkout(root: Path, expected: Mapping[str, Any]) -> dict[str, Any]:
    if set(expected) != set(PUBLISHER_FIELDS):
        raise OperationalMetadataV6Error("publisher source identity fields drifted")
    if (
        expected.get("module_route") != PUBLISHER_MODULE_ROUTE
        or expected.get("annotated_tag") != PUBLISHER_TAG
    ):
        raise OperationalMetadataV6Error("publisher module/tag route drifted")
    for field in ("annotated_tag_object", "commit", "tree"):
        _git_oid(expected.get(field), f"publisher {field}")
    _sha256(expected.get("script_sha256"), "publisher script_sha256")
    observed = _observe_publisher_checkout(root)
    if observed != dict(expected):
        raise OperationalMetadataV6Error("publisher checkout identity drifted")
    return observed


def _sha256(value: Any, label: str) -> str:
    normalized = str(value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise OperationalMetadataV6Error(f"{label} is not a lowercase SHA256")
    return normalized


def _git_oid(value: Any, label: str) -> str:
    normalized = str(value)
    if _GIT_OID_RE.fullmatch(normalized) is None:
        raise OperationalMetadataV6Error(f"{label} is not a lowercase Git SHA-1 object id")
    return normalized


def _json_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise OperationalMetadataV6Error(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_regular(
    path: Path,
    *,
    mode: int | None = None,
    allowed_nlinks: frozenset[int] = frozenset({1}),
) -> tuple[bytes, os.stat_result]:
    target = Path(os.path.abspath(os.fspath(path.expanduser())))
    before = target.lstat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.getuid()
        or before.st_nlink not in allowed_nlinks
        or before.st_size < 0
        or before.st_size > MAX_JSON_BYTES
        or (mode is not None and stat.S_IMODE(before.st_mode) != mode)
    ):
        raise OperationalMetadataV6Error(f"unsafe file identity: {target}")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(target, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise OperationalMetadataV6Error(f"file changed while opening: {target}")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_JSON_BYTES:
                raise OperationalMetadataV6Error(f"JSON file is too large: {target}")
            chunks.append(chunk)
        after_fd = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    after_path = target.lstat()

    def identity(value: os.stat_result) -> tuple[int, ...]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_uid,
            value.st_gid,
            value.st_nlink,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    if identity(before) != identity(after_fd) or identity(before) != identity(after_path):
        raise OperationalMetadataV6Error(f"file changed while reading: {target}")
    return b"".join(chunks), before


def _load_json(
    path: Path,
    *,
    mode: int | None = None,
    allowed_nlinks: frozenset[int] = frozenset({1}),
) -> tuple[dict[str, Any], bytes, os.stat_result]:
    raw, metadata = _read_regular(path, mode=mode, allowed_nlinks=allowed_nlinks)
    try:
        payload = json.loads(raw, object_pairs_hook=_json_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OperationalMetadataV6Error(f"invalid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise OperationalMetadataV6Error(f"JSON root is not an object: {path}")
    return payload, raw, metadata


def _content(payload: Mapping[str, Any], path: Path, raw: bytes, mode: int) -> dict[str, Any]:
    field = payload.get("canonical_field")
    if not isinstance(field, str):
        candidates = [
            key
            for key in payload
            if key == "admission_identity_sha256"
            or (key.startswith("canonical_") and key.endswith("sha256"))
        ]
        if len(candidates) != 1:
            raise OperationalMetadataV6Error(f"canonical field is ambiguous: {path}")
        field = candidates[0]
    canonical = payload.get(field)
    _sha256(canonical, f"{path.name} canonical SHA256")
    if _document_sha(payload, field) != canonical:
        raise OperationalMetadataV6Error(f"canonical recomputation drift: {path}")
    return {
        "path": str(Path(os.path.abspath(os.fspath(path.expanduser())))),
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": _sha(raw),
        "canonical_field": field,
        "canonical_sha256": canonical,
        "size_bytes": len(raw),
        "mode": f"{mode:04o}",
    }


def _without_path(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {key: binding[key] for key in CONTENT_FIELDS_NO_PATH}


def _json_pointer(payload: Any, pointer: str) -> Any:
    if not isinstance(pointer, str) or not pointer.startswith("/"):
        raise OperationalMetadataV6Error("JSON assertion pointer must start with '/'")
    current = payload
    for raw_component in pointer[1:].split("/"):
        component = raw_component.replace("~1", "/").replace("~0", "~")
        if isinstance(current, Mapping) and component in current:
            current = current[component]
        elif isinstance(current, list) and component.isdigit() and int(component) < len(current):
            current = current[int(component)]
        else:
            raise OperationalMetadataV6Error(f"JSON assertion path is missing: {pointer}")
    return current


def _contains_mapping(payload: Any, expected: Mapping[str, Any]) -> bool:
    if isinstance(payload, Mapping):
        if all(payload.get(key) == value for key, value in expected.items()):
            return True
        return any(_contains_mapping(value, expected) for value in payload.values())
    if isinstance(payload, list):
        return any(_contains_mapping(value, expected) for value in payload)
    return False


def _execution_projection_matches(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    return (
        value.get("execution_commit") == EXECUTION["execution_commit"]
        and value.get("execution_tree") == EXECUTION["execution_tree"]
        and value.get("annotated_tag", value.get("annotated_operational_tag"))
        == EXECUTION["annotated_operational_tag"]
        and value.get("annotated_tag_object", value.get("annotated_operational_tag_object"))
        == EXECUTION["annotated_operational_tag_object"]
        and value.get("tag_peeled_commit") == EXECUTION["tag_peeled_commit"]
    )


def _frozen_sources() -> dict[str, dict[str, Any]]:
    frozen = deepcopy(FROZEN_SOURCES)
    if set(frozen) != set(SOURCE_IDENTITIES):
        raise OperationalMetadataV6Error("frozen source roles drifted")
    for role, binding in frozen.items():
        if not isinstance(binding, Mapping) or set(binding) != set(CONTENT_FIELDS):
            raise OperationalMetadataV6Error(f"frozen {role} exact7 fields drifted")
        path = str(binding.get("path", ""))
        schema = binding.get("schema_version")
        status_value = binding.get("status")
        canonical_field = binding.get("canonical_field")
        size = binding.get("size_bytes")
        mode = binding.get("mode")
        if not path or not Path(path).expanduser().is_absolute():
            raise OperationalMetadataV6Error(f"frozen {role} path is not absolute")
        if not isinstance(schema, str) or not schema:
            raise OperationalMetadataV6Error(f"frozen {role} schema is missing")
        if status_value is not None and (not isinstance(status_value, str) or not status_value):
            raise OperationalMetadataV6Error(f"frozen {role} status is missing")
        _sha256(binding.get("file_sha256"), f"frozen {role} file SHA256")
        _sha256(binding.get("canonical_sha256"), f"frozen {role} canonical SHA256")
        if (
            not isinstance(canonical_field, str)
            or not canonical_field
            or not (
                canonical_field == "admission_identity_sha256"
                or (canonical_field.startswith("canonical_") and canonical_field.endswith("sha256"))
            )
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size <= 0
            or mode not in {"0600", "0644"}
            or (role == "lifecycle_admission") != (mode == "0644")
        ):
            raise OperationalMetadataV6Error(f"frozen {role} exact7 is malformed")
    return frozen


def _validate_manifest_payload(
    payload: Mapping[str, Any], path: Path, raw: bytes
) -> tuple[dict[str, Any], dict[str, Any]]:
    observed_path = Path(os.path.abspath(os.fspath(path.expanduser())))
    expected_path = Path(os.path.abspath(os.fspath(Path(FORMAL_MANIFEST_PATH).expanduser())))
    if observed_path != expected_path:
        raise OperationalMetadataV6Error("formal manifest path drifted")
    expected_top = {
        "schema_version",
        "status",
        "generated_utc",
        "receipt_id",
        "publisher_root",
        "metadata_repository_root",
        "publisher_source",
        "transaction",
        "validation_roots",
        "sources",
        "permissions",
        "evidence_boundary",
        MANIFEST_CANONICAL_FIELD,
    }
    if (
        set(payload) != expected_top
        or payload.get("schema_version") != MANIFEST_SCHEMA
        or payload.get("status") != MANIFEST_STATUS
        or payload.get("permissions") != NO_NEW_AUTHORITY
        or payload.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or payload.get(MANIFEST_CANONICAL_FIELD) != _document_sha(payload, MANIFEST_CANONICAL_FIELD)
    ):
        raise OperationalMetadataV6Error("operational metadata manifest identity drifted")
    _validate_manifest_generated_utc(payload["generated_utc"])
    if payload["receipt_id"] != FORMAL_RECEIPT_ID:
        raise OperationalMetadataV6Error("manifest receipt_id is not the frozen formal id")
    publisher_root = Path(str(payload["publisher_root"])).expanduser()
    metadata_root = Path(str(payload["metadata_repository_root"])).expanduser()
    if not publisher_root.is_absolute() or not metadata_root.is_absolute():
        raise OperationalMetadataV6Error("manifest publisher/metadata roots must be absolute")
    try:
        publisher_stat = publisher_root.resolve(strict=True).stat()
        metadata_stat = metadata_root.resolve(strict=True).stat()
    except OSError as exc:
        raise OperationalMetadataV6Error("publisher/metadata root is unavailable") from exc
    if (
        not stat.S_ISDIR(publisher_stat.st_mode)
        or not stat.S_ISDIR(metadata_stat.st_mode)
        or (publisher_stat.st_dev, publisher_stat.st_ino)
        == (metadata_stat.st_dev, metadata_stat.st_ino)
    ):
        raise OperationalMetadataV6Error("publisher and mutable metadata roots must be distinct")
    publisher = payload.get("publisher_source")
    if not isinstance(publisher, Mapping) or set(publisher) != set(PUBLISHER_FIELDS):
        raise OperationalMetadataV6Error("manifest publisher source identity drifted")
    if (
        publisher.get("module_route") != PUBLISHER_MODULE_ROUTE
        or publisher.get("annotated_tag") != PUBLISHER_TAG
    ):
        raise OperationalMetadataV6Error("manifest publisher module/tag drifted")
    for field in ("annotated_tag_object", "commit", "tree"):
        _git_oid(publisher.get(field), f"manifest publisher {field}")
    _sha256(publisher.get("script_sha256"), "manifest publisher script SHA256")

    transaction = payload.get("transaction")
    expected_transaction = {
        "pointer_path",
        "catalog_path",
        "replacement_receipt_path",
        "predecessor_pointer",
        "predecessor_catalog",
        "predecessor_activation",
        "predecessor_activation_artifact_id",
        "replacement_activation_artifact_id",
    }
    if not isinstance(transaction, Mapping) or set(transaction) != expected_transaction:
        raise OperationalMetadataV6Error("manifest transaction fields drifted")
    for name in ("pointer_path", "catalog_path", "replacement_receipt_path"):
        value = Path(str(transaction[name])).expanduser()
        if not value.is_absolute():
            raise OperationalMetadataV6Error(f"manifest {name} must be absolute")
    for name in ("predecessor_pointer", "predecessor_catalog"):
        value = transaction.get(name)
        if not isinstance(value, Mapping) or set(value) != {"file_sha256", "size_bytes"}:
            raise OperationalMetadataV6Error(f"manifest {name} binding drifted")
        _sha256(value["file_sha256"], f"manifest {name} SHA256")
        if (
            not isinstance(value["size_bytes"], int)
            or isinstance(value["size_bytes"], bool)
            or value["size_bytes"] <= 0
        ):
            raise OperationalMetadataV6Error(f"manifest {name} size is invalid")
    if (
        transaction["predecessor_pointer"] != FROZEN_PREDECESSOR["pointer"]
        or transaction["predecessor_catalog"] != FROZEN_PREDECESSOR["catalog"]
    ):
        raise OperationalMetadataV6Error("manifest predecessor pointer/catalog identity drifted")
    predecessor = transaction.get("predecessor_activation")
    if not isinstance(predecessor, Mapping) or set(predecessor) != set(CONTENT_FIELDS):
        raise OperationalMetadataV6Error("manifest predecessor activation binding drifted")
    if (
        predecessor.get("schema_version") != PREDECESSOR_RECEIPT_SCHEMA
        or predecessor.get("status") != PREDECESSOR_RECEIPT_STATUS
        or predecessor.get("canonical_field") != RECEIPT_CANONICAL_FIELD
        or predecessor.get("mode") != "0600"
    ):
        raise OperationalMetadataV6Error("manifest predecessor v4 activation identity drifted")
    _sha256(predecessor.get("file_sha256"), "predecessor activation file SHA256")
    _sha256(predecessor.get("canonical_sha256"), "predecessor activation canonical SHA256")
    if (
        not isinstance(predecessor.get("size_bytes"), int)
        or isinstance(predecessor.get("size_bytes"), bool)
        or predecessor["size_bytes"] <= 0
        or not isinstance(transaction["predecessor_activation_artifact_id"], str)
        or not transaction["predecessor_activation_artifact_id"]
        or not isinstance(transaction["replacement_activation_artifact_id"], str)
        or not transaction["replacement_activation_artifact_id"]
        or transaction["predecessor_activation_artifact_id"] != PREDECESSOR_ARTIFACT_ID
        or transaction["replacement_activation_artifact_id"] != REPLACEMENT_ARTIFACT_ID
        or predecessor
        != {
            "path": str(metadata_root / "docs" / "private" / PREDECESSOR_RECEIPT_FILENAME),
            **FROZEN_PREDECESSOR["activation"],
        }
    ):
        raise OperationalMetadataV6Error("manifest catalog artifact ids are invalid")

    validation_roots = payload.get("validation_roots")
    required_roots = {
        "current_runtime_root",
        "current_release_v3",
        "historical_v4_root",
        "historical_v4_release_v2",
    }
    if not isinstance(validation_roots, Mapping) or set(validation_roots) != required_roots:
        raise OperationalMetadataV6Error("manifest validation roots drifted")
    for name, value in validation_roots.items():
        if not isinstance(value, str) or not Path(value).expanduser().is_absolute():
            raise OperationalMetadataV6Error(f"manifest {name} must be absolute")

    frozen_sources = _frozen_sources()
    if payload.get("sources") != frozen_sources:
        raise OperationalMetadataV6Error("manifest sources differ from frozen exact7 identities")
    if validation_roots["current_release_v3"] != frozen_sources["direct_release"]["path"]:
        raise OperationalMetadataV6Error("current release validation root drifted")
    if (
        validation_roots["historical_v4_release_v2"]
        != frozen_sources["historical_v4_release"]["path"]
    ):
        raise OperationalMetadataV6Error("historical release validation root drifted")

    binding = _content(payload, path, raw, 0o600)
    return dict(payload), binding


def _validate_manifest_file_time(payload: Mapping[str, Any], metadata: os.stat_result) -> None:
    generated_ns = _timestamp_ns(payload["generated_utc"], "manifest generated_utc")
    if abs(metadata.st_mtime_ns - generated_ns) > MANIFEST_MAX_CLOCK_SKEW_SECONDS * 1_000_000_000:
        raise OperationalMetadataV6Error("formal manifest mtime is not bound to generated_utc")


def _load_manifest_candidate(
    candidate_path: Path,
    *,
    formal_path: Path,
    allowed_nlinks: frozenset[int],
) -> tuple[dict[str, Any], dict[str, Any], bytes, os.stat_result]:
    payload, raw, metadata = _load_json(
        candidate_path,
        mode=0o600,
        allowed_nlinks=allowed_nlinks,
    )
    validated = _validate_manifest_payload(payload, formal_path, raw)
    _validate_manifest_file_time(payload, metadata)
    return validated[0], validated[1], raw, metadata


def _validate_manifest(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    validated = _load_manifest_candidate(
        path,
        formal_path=path,
        allowed_nlinks=frozenset({1}),
    )
    payload, binding, _raw, _metadata = validated
    return payload, binding


def _assert_manifest_inputs(
    payload: Mapping[str, Any],
    *,
    publisher_root: Path,
    metadata_repository_root: Path,
    current_runtime_root: Path,
    historical_v4_root: Path,
    receipt_id: str,
) -> None:
    expected_inputs = {
        "publisher_root": str(Path(os.path.abspath(os.fspath(Path(publisher_root).expanduser())))),
        "metadata_repository_root": str(
            Path(os.path.abspath(os.fspath(Path(metadata_repository_root).expanduser())))
        ),
    }
    expected_roots = {
        "current_runtime_root": str(
            Path(os.path.abspath(os.fspath(Path(current_runtime_root).expanduser())))
        ),
        "historical_v4_root": str(
            Path(os.path.abspath(os.fspath(Path(historical_v4_root).expanduser())))
        ),
    }
    if (
        payload.get("receipt_id") != receipt_id
        or any(payload.get(key) != value for key, value in expected_inputs.items())
        or any(
            payload.get("validation_roots", {}).get(key) != value
            for key, value in expected_roots.items()
        )
    ):
        raise OperationalMetadataV6Error("existing formal manifest inputs drifted")


def build_activation_manifest(
    *,
    publisher_root: Path,
    metadata_repository_root: Path,
    current_runtime_root: Path,
    historical_v4_root: Path,
    generated_utc: str,
    receipt_id: str,
) -> dict[str, Any]:
    """Build the deterministic formal manifest from frozen predecessor/current inputs."""

    publisher_root = Path(os.path.abspath(os.fspath(publisher_root.expanduser())))
    metadata_repository_root = Path(
        os.path.abspath(os.fspath(metadata_repository_root.expanduser()))
    )
    current_runtime_root = Path(os.path.abspath(os.fspath(current_runtime_root.expanduser())))
    historical_v4_root = Path(os.path.abspath(os.fspath(historical_v4_root.expanduser())))
    _validate_manifest_creation_utc(generated_utc)
    if receipt_id != FORMAL_RECEIPT_ID:
        raise OperationalMetadataV6Error("manifest receipt_id is not the frozen formal id")
    private_root = metadata_repository_root / "docs" / "private"
    pointer_path = private_root / "live_remote.current.local.json"
    catalog_path = private_root / "catalog.current.local.json"
    predecessor_path = private_root / PREDECESSOR_RECEIPT_FILENAME
    receipt_path = private_root / RECEIPT_FILENAME
    pointer_payload, pointer_raw, _pointer_meta = _load_json(pointer_path, mode=0o600)
    catalog_payload, catalog_raw, _catalog_meta = _load_json(catalog_path, mode=0o600)
    predecessor_payload, predecessor_raw, _predecessor_meta = _load_json(
        predecessor_path, mode=0o600
    )
    predecessor_binding = _content(predecessor_payload, predecessor_path, predecessor_raw, 0o600)
    if (
        {"file_sha256": _sha(pointer_raw), "size_bytes": len(pointer_raw)}
        != FROZEN_PREDECESSOR["pointer"]
        or {"file_sha256": _sha(catalog_raw), "size_bytes": len(catalog_raw)}
        != FROZEN_PREDECESSOR["catalog"]
        or predecessor_binding
        != {"path": str(predecessor_path), **FROZEN_PREDECESSOR["activation"]}
    ):
        raise OperationalMetadataV6Error("formal predecessor exact chain drifted")
    expected_pointer_receipt = {
        "path": str(predecessor_path),
        "sha256": predecessor_binding["file_sha256"],
        "canonical_sha256": predecessor_binding["canonical_sha256"],
        "bytes": predecessor_binding["size_bytes"],
    }
    if pointer_payload.get("current_activation_receipt") != expected_pointer_receipt:
        raise OperationalMetadataV6Error("formal predecessor pointer-to-receipt drifted")
    transaction = {
        "pointer_path": str(pointer_path),
        "catalog_path": str(catalog_path),
        "replacement_receipt_path": str(receipt_path),
        "predecessor_pointer": dict(FROZEN_PREDECESSOR["pointer"]),
        "predecessor_catalog": dict(FROZEN_PREDECESSOR["catalog"]),
        "predecessor_activation": predecessor_binding,
        "predecessor_activation_artifact_id": PREDECESSOR_ARTIFACT_ID,
        "replacement_activation_artifact_id": REPLACEMENT_ARTIFACT_ID,
    }
    _catalog_context(catalog_payload, transaction)
    payload: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA,
        "status": MANIFEST_STATUS,
        "generated_utc": generated_utc,
        "receipt_id": receipt_id,
        "publisher_root": str(publisher_root),
        "metadata_repository_root": str(metadata_repository_root),
        "publisher_source": _observe_publisher_checkout(publisher_root),
        "transaction": transaction,
        "validation_roots": {
            "current_runtime_root": str(current_runtime_root),
            "current_release_v3": FROZEN_SOURCES["direct_release"]["path"],
            "historical_v4_root": str(historical_v4_root),
            "historical_v4_release_v2": FROZEN_SOURCES["historical_v4_release"]["path"],
        },
        "sources": _frozen_sources(),
        "permissions": dict(NO_NEW_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[MANIFEST_CANONICAL_FIELD] = _document_sha(payload, MANIFEST_CANONICAL_FIELD)
    return payload


def _ensure_private_directory(path: Path) -> None:
    if path.exists():
        metadata = path.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise OperationalMetadataV6Error(f"unsafe manifest directory: {path}")
        return
    parent = path.parent
    parent_meta = parent.lstat()
    if not stat.S_ISDIR(parent_meta.st_mode) or parent_meta.st_uid != os.getuid():
        raise OperationalMetadataV6Error("manifest directory parent is unsafe")
    os.mkdir(path, 0o700)
    _fsync_dir(parent)
    created = path.lstat()
    if (
        not stat.S_ISDIR(created.st_mode)
        or created.st_uid != os.getuid()
        or stat.S_IMODE(created.st_mode) != 0o700
    ):
        raise OperationalMetadataV6Error("created manifest directory identity drifted")


def _open_metadata_transaction_lock(metadata_repository_root: Path) -> int:
    private_root = (
        Path(os.path.abspath(os.fspath(Path(metadata_repository_root).expanduser())))
        / "docs"
        / "private"
    )
    before = private_root.lstat()
    if not stat.S_ISDIR(before.st_mode) or before.st_uid != os.getuid() or before.st_mode & 0o077:
        raise OperationalMetadataV6Error("metadata transaction directory is unsafe")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(private_root, flags)
    opened = os.fstat(descriptor)
    if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
        os.close(descriptor)
        raise OperationalMetadataV6Error("metadata transaction directory changed while opening")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def _close_metadata_transaction_lock(descriptor: int) -> None:
    fcntl.flock(descriptor, fcntl.LOCK_UN)
    os.close(descriptor)


def _validate_manifest_predecessor_state(manifest: Mapping[str, Any]) -> None:
    metadata_root = Path(str(manifest["metadata_repository_root"]))
    transaction = manifest["transaction"]
    pointer_path = metadata_root / "docs" / "private" / "live_remote.current.local.json"
    catalog_path = metadata_root / "docs" / "private" / "catalog.current.local.json"
    predecessor_path = Path(str(transaction["predecessor_activation"]["path"]))
    pointer, pointer_raw, _pointer_meta = _load_json(pointer_path, mode=0o600)
    catalog, catalog_raw, _catalog_meta = _load_json(catalog_path, mode=0o600)
    predecessor, predecessor_raw, _predecessor_meta = _load_json(predecessor_path, mode=0o600)
    predecessor_binding = _content(predecessor, predecessor_path, predecessor_raw, 0o600)
    if (
        {
            "file_sha256": _sha(pointer_raw),
            "size_bytes": len(pointer_raw),
        }
        != transaction["predecessor_pointer"]
        or {
            "file_sha256": _sha(catalog_raw),
            "size_bytes": len(catalog_raw),
        }
        != transaction["predecessor_catalog"]
        or predecessor_binding != transaction["predecessor_activation"]
        or pointer.get("current_activation_receipt")
        != {
            "path": predecessor_binding["path"],
            "sha256": predecessor_binding["file_sha256"],
            "canonical_sha256": predecessor_binding["canonical_sha256"],
            "bytes": predecessor_binding["size_bytes"],
        }
    ):
        raise OperationalMetadataV6Error("formal manifest predecessor state drifted")
    _catalog_context(catalog, transaction)


def validate_activation_manifest(path: Path, *, recursive: bool = True) -> dict[str, Any]:
    payload, binding = _validate_manifest(path)
    source_count = 0
    if recursive:
        _context, sources = _validate_sources(payload)
        source_count = len(sources)
    return {
        "manifest": binding,
        "publisher_source": dict(payload["publisher_source"]),
        "source_count": source_count,
        "recursive_validation_passed": recursive,
    }


def finalize_activation_manifest(
    payload: Mapping[str, Any],
    *,
    output_path: Path | None = None,
    recursive: bool = True,
) -> dict[str, Any]:
    if recursive is not True:
        raise OperationalMetadataV6Error(
            "formal manifest publication requires recursive validation"
        )
    metadata_root = payload.get("metadata_repository_root")
    if not isinstance(metadata_root, str) or not Path(metadata_root).expanduser().is_absolute():
        raise OperationalMetadataV6Error("manifest metadata repository root is invalid")
    descriptor = _open_metadata_transaction_lock(Path(metadata_root))
    try:
        return _finalize_activation_manifest_locked(payload, output_path=output_path)
    finally:
        _close_metadata_transaction_lock(descriptor)


def _finalize_activation_manifest_locked(
    payload: Mapping[str, Any],
    *,
    output_path: Path | None = None,
) -> dict[str, Any]:
    path = Path(FORMAL_MANIFEST_PATH if output_path is None else output_path)
    path = Path(os.path.abspath(os.fspath(path.expanduser())))
    expected = Path(os.path.abspath(os.fspath(Path(FORMAL_MANIFEST_PATH).expanduser())))
    if path != expected:
        raise OperationalMetadataV6Error("formal manifest output path drifted")
    pending = _pending(path, "create")
    final_payload = deepcopy(dict(payload))
    raw = _render(final_payload)
    _validate_manifest_payload(final_payload, path, raw)
    _validate_sources(final_payload)
    publication_state = (path.exists(), pending.exists())
    if publication_state[0] is False or publication_state[1] is True:
        _validate_manifest_predecessor_state(final_payload)
    _ensure_private_directory(path.parent)
    if (path.exists(), pending.exists()) != publication_state:
        raise OperationalMetadataV6Error("formal manifest publication state changed during setup")
    if publication_state == (False, False):
        final_payload["generated_utc"] = _nanosecond_utc(
            _now_utc_ns(), "manifest finalization clock"
        )
        final_payload[MANIFEST_CANONICAL_FIELD] = _document_sha(
            final_payload, MANIFEST_CANONICAL_FIELD
        )
        raw = _render(final_payload)
        _validate_manifest_payload(final_payload, path, raw)
        _validate_manifest_creation_utc(final_payload["generated_utc"])
    _publish_create_only(path, raw)
    result = validate_activation_manifest(path, recursive=True)
    result["write_semantics"] = "create_only_idempotent_exact_conflict_rejected"
    return result


def prepare_activation_manifest(
    *,
    publisher_root: Path,
    metadata_repository_root: Path,
    current_runtime_root: Path,
    historical_v4_root: Path,
    receipt_id: str,
    output_path: Path | None = None,
    recursive: bool = True,
) -> dict[str, Any]:
    if recursive is not True:
        raise OperationalMetadataV6Error(
            "formal manifest publication requires recursive validation"
        )
    descriptor = _open_metadata_transaction_lock(metadata_repository_root)
    try:
        return _prepare_activation_manifest_locked(
            publisher_root=publisher_root,
            metadata_repository_root=metadata_repository_root,
            current_runtime_root=current_runtime_root,
            historical_v4_root=historical_v4_root,
            receipt_id=receipt_id,
            output_path=output_path,
        )
    finally:
        _close_metadata_transaction_lock(descriptor)


def _prepare_activation_manifest_locked(
    *,
    publisher_root: Path,
    metadata_repository_root: Path,
    current_runtime_root: Path,
    historical_v4_root: Path,
    receipt_id: str,
    output_path: Path | None = None,
) -> dict[str, Any]:
    path = Path(FORMAL_MANIFEST_PATH if output_path is None else output_path)
    path = Path(os.path.abspath(os.fspath(path.expanduser())))
    expected = Path(os.path.abspath(os.fspath(Path(FORMAL_MANIFEST_PATH).expanduser())))
    if path != expected:
        raise OperationalMetadataV6Error("formal manifest output path drifted")
    pending = _pending(path, "create")
    if path.exists():
        existing, _binding, raw, metadata = _load_manifest_candidate(
            path,
            formal_path=path,
            allowed_nlinks=frozenset({1, 2}),
        )
        if metadata.st_nlink == 1 and pending.exists():
            raise OperationalMetadataV6Error("orphan formal manifest pending path is ambiguous")
        _assert_manifest_inputs(
            existing,
            publisher_root=publisher_root,
            metadata_repository_root=metadata_repository_root,
            current_runtime_root=current_runtime_root,
            historical_v4_root=historical_v4_root,
            receipt_id=receipt_id,
        )
        _validate_sources(existing)
        if metadata.st_nlink == 2:
            _validate_manifest_predecessor_state(existing)
        if metadata.st_nlink == 2:
            _publish_create_only(path, raw)
            semantics = "create_only_hardlink_crash_recovered"
        else:
            semantics = "create_only_idempotent_existing_exact_reused"
        result = validate_activation_manifest(path, recursive=True)
        result["write_semantics"] = semantics
        return result
    if pending.exists():
        existing, _binding, raw, _metadata = _load_manifest_candidate(
            pending,
            formal_path=path,
            allowed_nlinks=frozenset({1}),
        )
        _assert_manifest_inputs(
            existing,
            publisher_root=publisher_root,
            metadata_repository_root=metadata_repository_root,
            current_runtime_root=current_runtime_root,
            historical_v4_root=historical_v4_root,
            receipt_id=receipt_id,
        )
        _validate_sources(existing)
        _validate_manifest_predecessor_state(existing)
        _publish_create_only(path, raw)
        result = validate_activation_manifest(path, recursive=True)
        result["write_semantics"] = "create_only_pending_crash_recovered"
        return result
    generated_utc = _nanosecond_utc(_now_utc_ns(), "manifest generation clock")
    payload = build_activation_manifest(
        publisher_root=publisher_root,
        metadata_repository_root=metadata_repository_root,
        current_runtime_root=current_runtime_root,
        historical_v4_root=historical_v4_root,
        generated_utc=generated_utc,
        receipt_id=receipt_id,
    )
    return _finalize_activation_manifest_locked(payload, output_path=output_path)


def _validate_sources(manifest: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    publisher_before = _publisher_checkout(
        Path(str(manifest["publisher_root"])), manifest["publisher_source"]
    )
    payloads: dict[str, Any] = {}
    bindings: dict[str, Any] = {}
    for role, expected in manifest["sources"].items():
        path = Path(str(expected["path"]))
        mode = int(str(expected["mode"]), 8)
        payload, raw, _metadata = _load_json(path, mode=mode)
        actual = _content(payload, path, raw, mode)
        if actual != dict(expected):
            raise OperationalMetadataV6Error(f"{role} exact content identity drifted")
        schema, status_value, canonical_field = SOURCE_IDENTITIES[role]
        if (
            (schema is not None and actual["schema_version"] != schema)
            or (status_value is not None and actual["status"] != status_value)
            or (canonical_field is not None and actual["canonical_field"] != canonical_field)
        ):
            raise OperationalMetadataV6Error(f"{role} schema/status identity drifted")
        payloads[role] = payload
        bindings[role] = actual

    roots = {name: Path(str(value)) for name, value in manifest["validation_roots"].items()}
    try:
        cross = transport_v6.validate_cross_host_admission(
            Path(bindings["cross_host_admission"]["path"]),
            direct_repository_root=roots["current_runtime_root"],
            direct_release_path=roots["current_release_v3"],
        )
        lifecycle_context = lifecycle_context_v1.validate_lifecycle_context_against_admission(
            Path(bindings["lifecycle_context"]["path"]),
            lifecycle_admission_path=Path(bindings["lifecycle_admission"]["path"]),
            runtime_repository_root=roots["current_runtime_root"],
        )
        post = post_lifecycle_v1.validate_content_projection(payloads["post_lifecycle_health"])
        final_roots = {
            "current_runtime_root": roots["current_runtime_root"],
            "current_release_v3": roots["current_release_v3"],
            "historical_v4_root": roots["historical_v4_root"],
            "historical_v4_release_v2": roots["historical_v4_release_v2"],
        }
        validated_final = {
            "final_activation_envelope": final_v6.validate_activation_envelope(
                Path(bindings["final_activation_envelope"]["path"]), **final_roots
            ),
            "final_operational_completion": final_v6.validate_completion(
                Path(bindings["final_operational_completion"]["path"]), **final_roots
            ),
            "final_composition": final_v6.validate_composition(
                Path(bindings["final_composition"]["path"]), **final_roots
            ),
            "final_attempt": final_v6.validate_attempt_final(
                Path(bindings["final_attempt"]["path"]), **final_roots
            ),
            "final_proof": final_v6.validate_evidence_release(
                Path(bindings["final_proof"]["path"]), **final_roots
            ),
        }
    except Exception as exc:
        raise OperationalMetadataV6Error("current no-shadow evidence recursion failed") from exc
    if cross != payloads["cross_host_admission"]:
        raise OperationalMetadataV6Error("cross-host admission bytes differ from validation")
    if any(validated_final[role] != payloads[role] for role in validated_final):
        raise OperationalMetadataV6Error("final-v6 source bytes differ from recursive validation")

    admitted = cross.get("admitted_files")
    if not isinstance(admitted, Mapping):
        raise OperationalMetadataV6Error("cross-host admitted file set is missing")
    admitted_roles = {
        "config_correction": "config_correction",
        "resource_gate": "current_host_resource_gate",
        "active_process_capture": "active_process_capture",
        "remote_active_attestation": "remote_active_attestation",
    }
    if set(admitted) != set(admitted_roles.values()):
        raise OperationalMetadataV6Error("cross-host admitted file roles drifted")
    for role, admitted_role in admitted_roles.items():
        row = admitted.get(admitted_role)
        binding = bindings[role]
        if not isinstance(row, Mapping) or (
            row.get("path"),
            row.get("file_sha256"),
            row.get("size_bytes"),
            row.get("mode"),
            row.get("nlink"),
        ) != (
            binding["path"],
            binding["file_sha256"],
            binding["size_bytes"],
            binding["mode"],
            1,
        ):
            raise OperationalMetadataV6Error(f"{role} is not the exact cross-host admitted file")

    portable = cross.get("portable_evidence")
    if not isinstance(portable, Mapping):
        raise OperationalMetadataV6Error("cross-host portable evidence is missing")
    active = portable.get("active_runtime")
    if not isinstance(active, Mapping):
        raise OperationalMetadataV6Error("cross-host active runtime is missing")
    host = portable.get("host")
    if not isinstance(host, Mapping) or any(
        host.get(name) != value for name, value in CURRENT_HOST_CORE.items()
    ):
        raise OperationalMetadataV6Error("current host identity drifted")
    if (
        portable.get("runtime_execution") != EXECUTION
        or portable.get("runtime_authority")
        != {
            **_without_path(bindings["direct_release"]),
            "execution": EXECUTION,
            "runtime_authority": True,
        }
        or portable.get("exact_artifact", {}).get("artifact_sha256") != ARTIFACT_SHA256
        or active.get("config_sha256") != ACTIVE_CONFIG_SHA256
    ):
        raise OperationalMetadataV6Error("cross-host current authority drifted")
    health_window = active.get("active_health_window")
    if (
        not isinstance(health_window, Mapping)
        or health_window.get("active_pid") != CURRENT_PID
        or health_window.get("active_pid_start_ticks") != CURRENT_PID_START_TICKS
    ):
        raise OperationalMetadataV6Error("cross-host active PID/start drifted")
    active_capture = payloads["active_process_capture"]
    captured_process = active_capture.get("active_process")
    runtime_identity = active_capture.get("runtime_identity")
    if not isinstance(captured_process, Mapping) or not isinstance(runtime_identity, Mapping):
        raise OperationalMetadataV6Error("active capture process/runtime identity is missing")
    runtime_identity_recorded_utc = _utc_z(
        runtime_identity.get("recorded_at_utc"), "runtime identity recorded_at_utc"
    )
    active_captured_utc = _timestamp(
        captured_process.get("captured_utc"), "active process captured_utc"
    )
    if (
        runtime_identity_recorded_utc != ACTIVE_RUNTIME_IDENTITY_RECORDED_UTC
        or active_captured_utc != ACTIVE_CAPTURED_UTC
        or captured_process.get("pid") != CURRENT_PID
        or captured_process.get("pid_start_ticks") != CURRENT_PID_START_TICKS
        or runtime_identity.get("f05_buy_e3_active_release_path") in (None, "")
    ):
        raise OperationalMetadataV6Error("active capture timestamp/process route drifted")

    lifecycle = lifecycle_context.get("lifecycle_projection")
    if (
        not isinstance(lifecycle, Mapping)
        or lifecycle.get("baseline_epoch_id") != CURRENT_EPOCH_ID
        or lifecycle.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or lifecycle.get("runtime_source_file_count") != 65
        or lifecycle.get("runtime_source_files_canonical_sha256")
        != lifecycle_context_v1.RUNTIME_SOURCE_FILES_CANONICAL_SHA256
        or lifecycle.get("runtime_code_sha256") != lifecycle_context_v1.RUNTIME_CODE_SHA256
        or lifecycle.get("external_effective_stream_and_recording_disabled") is not True
    ):
        raise OperationalMetadataV6Error("formal lifecycle context drifted")

    post_process = post.get("active_process")
    post_checks = post.get("checks")
    if (
        not isinstance(post_process, Mapping)
        or post_process.get("pid") != CURRENT_PID
        or post_process.get("pid_start_ticks") != CURRENT_PID_START_TICKS
        or post_process.get("config_sha256") != ACTIVE_CONFIG_SHA256
        or post.get("runtime_execution") != EXECUTION
        or post.get("portable_projection", {}).get("lifecycle_epoch_id") != CURRENT_EPOCH_ID
        or not isinstance(post_checks, Mapping)
        or dict(post_checks) != post_lifecycle_v1.CHECKS
        or post.get("evidence_boundary") != post_lifecycle_v1.EVIDENCE_BOUNDARY
    ):
        raise OperationalMetadataV6Error("post-lifecycle no-shadow evidence drifted")
    portable_post = post.get("portable_projection")
    main_window = post.get("main_health_window")
    lifecycle_health = post.get("lifecycle_health")
    if (
        not isinstance(portable_post, Mapping)
        or not isinstance(main_window, Mapping)
        or not isinstance(lifecycle_health, Mapping)
        or portable_post.get("generated_utc") != post.get("generated_utc")
        or portable_post.get("lifecycle_epoch_id") != CURRENT_EPOCH_ID
        or lifecycle_health.get("order_lifecycle_v2_errors") != 0
        or lifecycle_health.get("order_lifecycle_v2_drops") != 0
    ):
        raise OperationalMetadataV6Error("post-lifecycle portable health drifted")
    rows = main_window.get("rows")
    if not isinstance(rows, list) or len(rows) != 2:
        raise OperationalMetadataV6Error("post-lifecycle main HEALTH rows drifted")
    safe_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise OperationalMetadataV6Error("post-lifecycle main HEALTH row is malformed")
        projection = row.get("projection")
        readiness = row.get("readiness")
        if not isinstance(projection, Mapping) or not isinstance(readiness, Mapping):
            raise OperationalMetadataV6Error("post-lifecycle readiness projection is missing")
        counters = projection.get("counter_values")
        shadow = projection.get("shadow_disabled_state")
        if not isinstance(counters, Mapping) or not isinstance(shadow, Mapping):
            raise OperationalMetadataV6Error("post-lifecycle disabled-state projection is missing")
        safe_rows.append(
            {
                "row_index": index,
                "fresh_generation": row["fresh_generation"],
                "line_sha256": row["line_sha256"],
                "wall_timestamp_s": row["main_wall_timestamp_s"],
                "boolean_cooldown_updates": projection["boolean_cooldown_updates"],
                "completed_windows": readiness["completed_windows"],
                "runtime_loaded": readiness["runtime_loaded"],
                "warmup_time_admitted": readiness["warmup_time_admitted"],
                "gap_resets": readiness["gap_resets"],
                "resets": readiness["resets"],
                "invalid_updates": readiness["invalid_updates"],
                "external_sources": shadow["externalSources"],
                "external_errors": counters["externalErrors"],
                "global_flow_shadow_enabled": shadow["globalFlowShadowEnabled"],
                "global_flow_state_error": shadow["globalFlowStateError"],
                "global_reference_shadow_enabled": shadow["globalRefShadowEnabled"],
                "global_reference_state_error": shadow["globalRefStateError"],
            }
        )
    first_safe, second_safe = safe_rows
    zero_fields = (
        "gap_resets",
        "resets",
        "invalid_updates",
        "external_sources",
        "external_errors",
        "global_flow_shadow_enabled",
        "global_flow_state_error",
        "global_reference_shadow_enabled",
        "global_reference_state_error",
    )
    if (
        any(row["runtime_loaded"] is not True for row in safe_rows)
        or any(row["warmup_time_admitted"] is not True for row in safe_rows)
        or any(row[name] != 0 for row in safe_rows for name in zero_fields)
        or second_safe["boolean_cooldown_updates"] <= first_safe["boolean_cooldown_updates"]
        or second_safe["completed_windows"] <= first_safe["completed_windows"]
    ):
        raise OperationalMetadataV6Error("post-lifecycle readiness/no-shadow rows drifted")

    proof = payloads["final_proof"]
    state = proof.get("evidence_state")
    authority = proof.get("authority_design")
    if (
        proof.get("runtime_execution") != EXECUTION
        or not isinstance(state, Mapping)
        or state.get("shadow_or_companion_collection_enabled") is not False
        or state.get("two_explicit_disabled_evaluators_error0_absolute0") is not True
        or state.get("runtime_authority_replaced") is not False
        or state.get("does_not_replace_runtime_active_release") is not True
        or not isinstance(authority, Mapping)
        or authority.get("runtime_authority") != "direct_owner_release_v3"
        or authority.get("proof_release_replaces_runtime_authority") is not False
        or proof.get("evidence_boundary") != final_v6.EVIDENCE_BOUNDARY
    ):
        raise OperationalMetadataV6Error("final-v6 proof authority drifted")

    post_generated = _timestamp(post.get("generated_utc"), "post-lifecycle generated_utc")
    if post_generated != POST_LIFECYCLE_RECEIPT_GENERATED_UTC:
        raise OperationalMetadataV6Error("post-lifecycle generated timestamp drifted")
    metadata_generated = _timestamp(manifest.get("generated_utc"), "metadata generated_utc")
    if datetime.fromisoformat(post_generated.replace("Z", "+00:00")) > datetime.fromisoformat(
        metadata_generated.replace("Z", "+00:00")
    ):
        raise OperationalMetadataV6Error("metadata predates durable post-lifecycle health")
    epoch_start_ns = int(lifecycle["epoch_start_ts_ns"])
    epoch_started_utc = _nanosecond_utc(epoch_start_ns, "lifecycle epoch_start_ts_ns")
    if epoch_started_utc != CURRENT_EPOCH_STARTED_UTC:
        raise OperationalMetadataV6Error("lifecycle epoch UTC projection drifted")
    publisher_after = _publisher_checkout(
        Path(str(manifest["publisher_root"])), manifest["publisher_source"]
    )
    if publisher_after != publisher_before:
        raise OperationalMetadataV6Error("publisher checkout changed during validation")
    context = {
        "host_core": {name: host[name] for name in CURRENT_HOST_CORE},
        "process": {
            "pid": CURRENT_PID,
            "pid_start_ticks": CURRENT_PID_START_TICKS,
            "process_identity_sha256": post_process["process_identity_sha256"],
            "stable_process_identity_sha256": post_process["stable_process_identity_sha256"],
            "runtime_identity_recorded_utc": runtime_identity_recorded_utc,
            "active_capture_utc": active_captured_utc,
            "post_lifecycle_health_utc": post["generated_utc"],
            "active_release_remote_path": runtime_identity["f05_buy_e3_active_release_path"],
        },
        "epoch": {
            "epoch_id": CURRENT_EPOCH_ID,
            "started_ts_ns": epoch_start_ns,
            "started_utc": epoch_started_utc,
            "identity_sha256": bindings["lifecycle_admission"]["canonical_sha256"],
        },
        "lifecycle": {
            "admitted_ts_ns": lifecycle["admitted_ts_ns"],
            "runtime_source_file_count": 65,
            "runtime_source_files_canonical_sha256": lifecycle[
                "runtime_source_files_canonical_sha256"
            ],
            "runtime_code_sha256": lifecycle["runtime_code_sha256"],
            "external_effective_stream_and_recording_disabled": True,
        },
        "post_lifecycle_health": {
            "generated_utc": post["generated_utc"],
            "checks": dict(post_checks),
            "economic_values_persisted": False,
        },
        "pointer_health_snapshot": {
            "snapshot_utc": post["generated_utc"],
            "source_semantics": "durable_post_lifecycle_health_not_latest_heartbeat",
            "source_receipt": dict(bindings["post_lifecycle_health"]),
            "pid": CURRENT_PID,
            "pid_start_ticks": CURRENT_PID_START_TICKS,
            "live_maker_running_at_post_lifecycle_capture": True,
            "main_health_rows": safe_rows,
            "completed_windows_and_updates_strictly_increase": True,
            "buy_e3_and_sell_owner_enabled_both_rows": True,
            "runtime_loaded_and_warmup_time_admitted_both_rows": True,
            "gap_resets_resets_invalid_absolute_zero": True,
            "config_sha256": ACTIVE_CONFIG_SHA256,
            "execution_commit": EXECUTION["execution_commit"],
            "execution_tree": EXECUTION["execution_tree"],
            "external_sources_and_errors_absolute_zero": True,
            "global_flow_explicit_disabled_error_state_value_backend_absolute_zero": True,
            "global_reference_explicit_disabled_error_state_value_absolute_zero": True,
            "lifecycle_health_observed_utc": portable_post["lifecycle_health"]["observed_utc"],
            "lifecycle_error_count": 0,
            "lifecycle_drop_count": 0,
            "economic_outcomes_read": False,
            "economic_values_persisted": False,
            "latest_live_status_claimed": False,
        },
        "publisher_source": publisher_after,
    }
    return context, bindings


def _finding_fingerprints(audit: Mapping[str, Any]) -> tuple[str, ...]:
    if (
        audit.get("schema_version") != audit_private_evidence.AUDIT_SCHEMA
        or audit.get("mode") != audit_private_evidence.METADATA_ONLY
        or audit.get("deny_locked") is not True
        or audit.get("validation_read") is not False
        or audit.get("sealed_holdout_read") is not False
    ):
        raise OperationalMetadataV6Error("metadata audit envelope drifted")
    findings = audit.get("findings")
    if not isinstance(findings, list):
        raise OperationalMetadataV6Error("metadata audit findings are missing")
    return tuple(sorted(_canonical(finding) for finding in findings))


def _audit_baseline(audit: Mapping[str, Any]) -> dict[str, Any]:
    fingerprints = _finding_fingerprints(audit)
    return {
        "schema_version": audit.get("schema_version"),
        "mode": audit.get("mode"),
        "deny_locked": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "comparison_semantics": "after_finding_set_minus_before_finding_set",
        "preexisting_findings_may_remain": True,
        "required_new_finding_count": 0,
        "finding_fingerprints": list(fingerprints),
        "finding_set_sha256": _canonical(list(fingerprints)),
        "finding_count": len(fingerprints),
    }


def _assert_no_new_findings(
    baseline: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, Any]:
    comparison = _compare_audit_findings(baseline, after)
    if not comparison["passed"]:
        raise OperationalMetadataV6Error(
            f"metadata audit introduced {comparison['new_finding_count']} new finding(s)"
        )
    return comparison


def _compare_audit_findings(
    baseline: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, Any]:
    before = set(baseline.get("finding_fingerprints", []))
    if (
        baseline.get("schema_version") != audit_private_evidence.AUDIT_SCHEMA
        or baseline.get("mode") != audit_private_evidence.METADATA_ONLY
        or baseline.get("deny_locked") is not True
        or baseline.get("validation_read") is not False
        or baseline.get("sealed_holdout_read") is not False
    ):
        raise OperationalMetadataV6Error("metadata audit baseline envelope drifted")
    observed_fingerprints = _finding_fingerprints(after)
    observed = set(observed_fingerprints)
    findings = after.get("findings")
    if not isinstance(findings, list):
        raise OperationalMetadataV6Error("metadata audit findings are missing")
    new_pairs = sorted(
        (
            (_canonical(finding), deepcopy(finding))
            for finding in findings
            if _canonical(finding) not in before
        ),
        key=lambda pair: pair[0],
    )
    return {
        "comparison_semantics": "after_finding_set_minus_before_finding_set",
        "baseline_finding_count": len(before),
        "after_finding_count": len(observed),
        "new_finding_count": len(new_pairs),
        "new_finding_fingerprints": [fingerprint for fingerprint, _finding in new_pairs],
        "new_findings": [finding for _fingerprint, finding in new_pairs],
        "passed": not new_pairs,
    }


def _catalog_context(catalog: Mapping[str, Any], transaction: Mapping[str, Any]) -> dict[str, Any]:
    entries = catalog.get("entries")
    if catalog.get("schema_version") != CATALOG_SCHEMA or not isinstance(entries, list):
        raise OperationalMetadataV6Error("predecessor catalog schema drifted")
    pointer = [
        entry for entry in entries if entry.get("artifact_id") == "repository-live-remote-current"
    ]
    old_id = transaction["predecessor_activation_artifact_id"]
    old = [entry for entry in entries if entry.get("artifact_id") == old_id]
    if len(pointer) != 1 or len(old) != 1:
        raise OperationalMetadataV6Error("predecessor catalog entries are ambiguous")
    if {"pointer_path", "predecessor_pointer", "predecessor_activation"}.issubset(transaction) and (
        pointer[0].get("local_path") != transaction.get("pointer_path")
        or pointer[0].get("sha256") != transaction.get("predecessor_pointer", {}).get("file_sha256")
        or pointer[0].get("bytes") != transaction.get("predecessor_pointer", {}).get("size_bytes")
        or old[0].get("local_path") != transaction.get("predecessor_activation", {}).get("path")
        or old[0].get("sha256") != transaction.get("predecessor_activation", {}).get("file_sha256")
        or old[0].get("bytes") != transaction.get("predecessor_activation", {}).get("size_bytes")
    ):
        raise OperationalMetadataV6Error("predecessor catalog content bindings drifted")
    return {
        "generated_at_utc": catalog.get("generated_at_utc"),
        "entry_count": len(entries),
        "pointer_entry": deepcopy(pointer[0]),
        "predecessor_activation_entry": deepcopy(old[0]),
    }


def _receipt_payload(
    manifest: Mapping[str, Any],
    manifest_binding: Mapping[str, Any],
    bindings: Mapping[str, Any],
    source_context: Mapping[str, Any],
    predecessor_pointer: Mapping[str, Any],
    catalog_context: Mapping[str, Any],
    audit_baseline: Mapping[str, Any],
) -> dict[str, Any]:
    old_host = {
        field: predecessor_pointer.get(field)
        for field in (
            "provider",
            "region",
            "city",
            "ssh_target",
            "public_ipv4",
            "instance_id",
            "instance_type",
            "repo_root",
        )
    }
    if (
        not all(isinstance(value, str) and value for value in old_host.values())
        or str(old_host["provider"]).lower() != source_context["host_core"]["provider"]
        or any(
            old_host[field] != source_context["host_core"][field]
            for field in ("region", "public_ipv4", "instance_id", "instance_type")
        )
    ):
        raise OperationalMetadataV6Error("predecessor pointer differs from current host identity")
    predecessor_health = predecessor_pointer.get("current_evidence_health")
    if (
        not isinstance(predecessor_health, Mapping)
        or predecessor_health.get("snapshot_utc") != PREDECESSOR_V4_LAST_EVIDENCED_UTC
    ):
        raise OperationalMetadataV6Error("predecessor v4 last evidenced timestamp drifted")
    # Preserve the predecessor pointer's resolver presentation byte-for-byte.
    # The portable source is used only for a case-insensitive/core identity check.
    host = dict(old_host)
    process = dict(source_context["process"])
    epoch = dict(source_context["epoch"])
    payload: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA,
        "status": RECEIPT_STATUS,
        "receipt_id": manifest["receipt_id"],
        "generated_utc": manifest["generated_utc"],
        "scope": "release_v3_same_process_no_shadow_final_v6_operational_metadata",
        "activation_manifest": dict(manifest_binding),
        "predecessor_pointer": {
            "file_sha256": manifest["transaction"]["predecessor_pointer"]["file_sha256"],
            "size_bytes": manifest["transaction"]["predecessor_pointer"]["size_bytes"],
            "snapshot": deepcopy(predecessor_pointer),
        },
        "predecessor_catalog_context": deepcopy(catalog_context),
        "catalog_transaction": {
            "predecessor_activation_artifact_id": manifest["transaction"][
                "predecessor_activation_artifact_id"
            ],
            "replacement_activation_artifact_id": manifest["transaction"][
                "replacement_activation_artifact_id"
            ],
        },
        "historical_superseded_v4": {
            "classification": SUPERSEDED_REASON,
            "activation_receipt": dict(manifest["transaction"]["predecessor_activation"]),
            "direct_owner_release": dict(bindings["historical_v4_release"]),
            "proof_evidence_release": dict(bindings["historical_v4_proof"]),
            "activation_bytes_preserved_immutable": True,
            "release_bytes_preserved_immutable": True,
            "proof_bytes_preserved_immutable": True,
            "used_as_current_authority": False,
            "last_durable_verified_evidence_utc": PREDECESSOR_V4_LAST_EVIDENCED_UTC,
            "exact_process_stop_claimed": False,
        },
        "fail_closed_transition": {
            "interval_semantics": "open_start_open_end_utc",
            "start_exclusive_utc": PREDECESSOR_V4_LAST_EVIDENCED_UTC,
            "end_exclusive_utc": CURRENT_EPOCH_STARTED_UTC,
            "classification": (
                "verified_local_history_gap_containing_unknown_v4_tail_and_"
                "unadmitted_transition_attempts"
            ),
            "v4_process_stop_claimed": False,
            "downtime_claimed": False,
            "v4_stopped_at_interval_start_claimed": False,
            "rows_in_interval_must_fail_closed": True,
        },
        "current": {
            "host": host,
            "process": process,
            "epoch": epoch,
            "execution": dict(EXECUTION),
            "runtime_code_sha256": source_context["lifecycle"]["runtime_code_sha256"],
            "active_config_sha256": ACTIVE_CONFIG_SHA256,
            "disabled_config_sha256": DISABLED_CONFIG_SHA256,
            "artifact_sha256": ARTIFACT_SHA256,
            "release_source_path": bindings["direct_release"]["path"],
            "release": dict(bindings["direct_release"]),
            "publisher_source": dict(source_context["publisher_source"]),
        },
        "current_operational_evidence": {
            role: dict(bindings[role])
            for role in (
                "direct_release",
                "cross_host_admission",
                "config_correction",
                "resource_gate",
                "active_process_capture",
                "remote_active_attestation",
                "lifecycle_admission",
                "lifecycle_context",
                "post_lifecycle_health",
                "final_activation_envelope",
                "final_operational_completion",
                "final_composition",
                "final_attempt",
                "final_proof",
            )
        },
        "lifecycle": deepcopy(source_context["lifecycle"]),
        "post_lifecycle_health": deepcopy(source_context["post_lifecycle_health"]),
        "post_lifecycle_pointer_health_snapshot": deepcopy(
            source_context["pointer_health_snapshot"]
        ),
        "metadata_audit_baseline": deepcopy(audit_baseline),
        "authority": {
            "research_supported": False,
            "owner_risk_accepted": True,
            "runtime_authority": "immutable_direct_owner_release_v3",
            "runtime_authority_replaced": False,
            "proof_release_replaces_runtime_authority": False,
            "new_authority_granted": False,
        },
        "permissions": dict(NO_NEW_AUTHORITY),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[RECEIPT_CANONICAL_FIELD] = _document_sha(payload, RECEIPT_CANONICAL_FIELD)
    return payload


def _binding_from_bytes(
    path: Path, payload: Mapping[str, Any], data: bytes, canonical_field: str
) -> dict[str, Any]:
    return {
        "path": str(Path(os.path.abspath(os.fspath(path.expanduser())))),
        "schema_version": payload["schema_version"],
        "status": payload["status"],
        "file_sha256": _sha(data),
        "canonical_field": canonical_field,
        "canonical_sha256": payload[canonical_field],
        "size_bytes": len(data),
        "mode": "0600",
    }


def _pointer_payload(
    receipt: Mapping[str, Any], receipt_binding: Mapping[str, Any]
) -> dict[str, Any]:
    payload = deepcopy(receipt["predecessor_pointer"]["snapshot"])
    if payload.get("schema_version") != POINTER_SCHEMA or payload.get("status") != "current_active":
        raise OperationalMetadataV6Error("predecessor pointer schema/status drifted")
    runtime = receipt["current"]
    process = runtime["process"]
    epoch = runtime["epoch"]
    prior_epoch_id = payload.get("prospective_epoch_id")
    if prior_epoch_id == epoch["epoch_id"]:
        raise OperationalMetadataV6Error("successor epoch must differ from superseded v4 epoch")
    old_current_evidence = deepcopy(payload.get("current_operational_evidence"))
    if not isinstance(old_current_evidence, Mapping):
        raise OperationalMetadataV6Error("predecessor current evidence is missing")
    host = runtime["host"]
    for field in (
        "provider",
        "region",
        "city",
        "ssh_target",
        "public_ipv4",
        "instance_id",
        "instance_type",
        "repo_root",
    ):
        payload[field] = host[field]
    payload.update(
        {
            "status": "current_active",
            "activated_utc": epoch["started_utc"],
            "activated_utc_semantics": "formal_evidence_epoch_start_not_process_start",
            "maker_started_utc": None,
            "maker_started_utc_status": "unknown_not_inferred_from_epoch_start",
            "runtime_identity_recorded_utc": process["runtime_identity_recorded_utc"],
            "prospective_epoch_id": epoch["epoch_id"],
            "prospective_epoch_started_ts_ns": epoch["started_ts_ns"],
            "prospective_epoch_identity_sha256": epoch["identity_sha256"],
            "runtime_code_sha256": runtime["runtime_code_sha256"],
            "config_sha256": ACTIVE_CONFIG_SHA256,
            "pointer_publication_status": RECEIPT_STATUS,
            "current_process_id": process["pid"],
            "current_process_start_ticks": process["pid_start_ticks"],
            "current_activation_receipt": {
                "path": receipt_binding["path"],
                "sha256": receipt_binding["file_sha256"],
                "canonical_sha256": receipt_binding["canonical_sha256"],
                "bytes": receipt_binding["size_bytes"],
            },
            "current_buy_e3_release": {
                "identity": RELEASE["schema_version"],
                "status": RELEASE["status"],
                "immutable_release_status": RELEASE["status"],
                "active_release_path": process["active_release_remote_path"],
                "durable_release_evidence_path": runtime["release_source_path"],
                "active_release_file_sha256": RELEASE["file_sha256"],
                "active_release_canonical_sha256": RELEASE["canonical_sha256"],
                "research_supported": False,
                "owner_risk_accepted": True,
                "scope": "BUY_exposure_increasing_executed_fill_only",
                "sell_owner_policy_unchanged": True,
                "shadow_or_companion_created": False,
                "external_venues_enabled": False,
                "global_flow_shadow_enabled": False,
                "global_reference_shadow_enabled": False,
                "execution_commit": EXECUTION["execution_commit"],
                "execution_tree": EXECUTION["execution_tree"],
                "annotated_tag": EXECUTION["annotated_operational_tag"],
                "annotated_tag_object": EXECUTION["annotated_operational_tag_object"],
                "active_config_sha256": ACTIVE_CONFIG_SHA256,
                "disabled_config_sha256": DISABLED_CONFIG_SHA256,
                "post_release_evidence_status": final_v6.EVIDENCE_RELEASE_STATUS,
                "proof_release_replaces_runtime_authority": False,
            },
            "current_operational_evidence": {
                role: dict(binding)
                for role, binding in receipt["current_operational_evidence"].items()
            },
            "current_evidence_health": {
                **deepcopy(receipt["post_lifecycle_pointer_health_snapshot"]),
                "snapshot_semantics": (
                    "durable_post_lifecycle_receipt_capture_not_publication_time_or_latest_live"
                ),
                "attestation_status": "accepted_at_frozen_sources",
                "identity_exact": True,
                "process_id": process["pid"],
                "process_start_ticks": process["pid_start_ticks"],
                "lifecycle_session_id": epoch["epoch_id"],
                "lifecycle_runtime_source_file_count": receipt["lifecycle"][
                    "runtime_source_file_count"
                ],
                "lifecycle_external_stream_and_recording_disabled": receipt["lifecycle"][
                    "external_effective_stream_and_recording_disabled"
                ],
                "final_proof_status": final_v6.EVIDENCE_RELEASE_STATUS,
                "final_proof_canonical_sha256": receipt["current_operational_evidence"][
                    "final_proof"
                ]["canonical_sha256"],
                "research_supported": False,
                "owner_risk_accepted": True,
                "validation_read": False,
                "sealed_holdout_read": False,
            },
            "historical_superseded_operational_evidence": [
                *deepcopy(payload.get("historical_superseded_operational_evidence", [])),
                {
                    "prospective_epoch_id": prior_epoch_id,
                    "classification": SUPERSEDED_REASON,
                    "activation_receipt": deepcopy(
                        receipt["historical_superseded_v4"]["activation_receipt"]
                    ),
                    "direct_owner_release": deepcopy(
                        receipt["historical_superseded_v4"]["direct_owner_release"]
                    ),
                    "proof_evidence_release": deepcopy(
                        receipt["historical_superseded_v4"]["proof_evidence_release"]
                    ),
                    "operational_evidence_snapshot": old_current_evidence,
                    "current_authority": False,
                },
            ],
        }
    )

    epochs = payload.get("host_epochs")
    if not isinstance(epochs, list):
        raise OperationalMetadataV6Error("predecessor pointer host_epochs are missing")
    current_rows = [row for row in epochs if row.get("status") == "current_active"]
    if len(current_rows) != 1 or current_rows[0].get("prospective_epoch_id") != prior_epoch_id:
        raise OperationalMetadataV6Error("predecessor current epoch is ambiguous")
    if any(row.get("prospective_epoch_id") == epoch["epoch_id"] for row in epochs):
        raise OperationalMetadataV6Error("successor epoch already exists in predecessor pointer")
    old_row = current_rows[0]
    old_row["status"] = "historical_superseded_same_instance_epoch"
    old_row["superseded_reason"] = SUPERSEDED_REASON
    old_row.pop("live_authority_end_utc", None)
    old_row["verified_evidence_end_utc"] = PREDECESSOR_V4_LAST_EVIDENCED_UTC
    old_row["exact_process_stop_claimed"] = False
    old_row["superseded_by_prospective_epoch_id"] = epoch["epoch_id"]
    epochs.append(
        {
            "epoch_key": ":".join(
                (
                    str(host["provider"]).lower(),
                    host["region"],
                    host["instance_id"],
                    epoch["epoch_id"],
                )
            ),
            "network_locator_key": ":".join(
                (str(host["provider"]).lower(), host["region"], host["public_ipv4"])
            ),
            "status": "current_active",
            "state_sync_start_utc": epoch["started_utc"],
            "runtime_identity_recorded_utc": process["runtime_identity_recorded_utc"],
            "process_start_utc": None,
            "process_start_utc_status": "unknown_not_inferred_from_epoch_start",
            "prospective_epoch_id": epoch["epoch_id"],
            "prospective_epoch_started_ts_ns": epoch["started_ts_ns"],
            "prospective_epoch_identity_sha256": epoch["identity_sha256"],
            "public_ipv4": host["public_ipv4"],
            "instance_id": host["instance_id"],
            "instance_type": host["instance_type"],
            "runtime_code_sha256": runtime["runtime_code_sha256"],
            "config_sha256": ACTIVE_CONFIG_SHA256,
            "external_venues_enabled": False,
            "global_flow_shadow_enabled": False,
            "global_reference_shadow_enabled": False,
            "runtime_shadow_classification": "fully_no_shadow_release_v3",
            "buy_e3_active_release_file_sha256": RELEASE["file_sha256"],
            "buy_e3_active_release_canonical_sha256": RELEASE["canonical_sha256"],
            "active_pid": process["pid"],
            "active_pid_start_ticks": process["pid_start_ticks"],
            **EXECUTION,
            "lifecycle_admission_manifest_sha256": receipt["current_operational_evidence"][
                "lifecycle_admission"
            ]["file_sha256"],
            "cross_host_admission_sha256": receipt["current_operational_evidence"][
                "cross_host_admission"
            ]["file_sha256"],
            "final_proof_sha256": receipt["current_operational_evidence"]["final_proof"][
                "file_sha256"
            ],
            "final_proof_canonical_sha256": receipt["current_operational_evidence"]["final_proof"][
                "canonical_sha256"
            ],
        }
    )

    gaps = payload.get("evidence_coverage_gaps")
    if not isinstance(gaps, list):
        raise OperationalMetadataV6Error("predecessor pointer coverage gaps are missing")
    gaps.append(
        {
            **deepcopy(receipt["fail_closed_transition"]),
            "reason": (
                "v4 may have continued after its last durable snapshot; exact stop and tail "
                "are unknown, and no single admitted epoch spans the transition"
            ),
        }
    )

    policy = payload.get("current_query_policy")
    if not isinstance(policy, Mapping) or not isinstance(
        policy.get("fill_trade_query_order"), list
    ):
        raise OperationalMetadataV6Error("predecessor query policy is missing")
    prior_order = list(policy["fill_trade_query_order"])
    policy["fill_trade_query_order"] = [
        "partition_request_by_instance_id_and_prospective_epoch_id_before_reading_rows",
        f"query_current_epoch_{epoch['epoch_id']}_from_{epoch['started_utc']}",
        f"query_superseded_epoch_{prior_epoch_id}_only_through_last_verified_evidence_"
        f"{PREDECESSOR_V4_LAST_EVIDENCED_UTC}",
        f"fail_closed_strictly_after_{PREDECESSOR_V4_LAST_EVIDENCED_UTC}_and_strictly_"
        f"before_{epoch['started_utc']}",
        *[
            row
            for row in prior_order
            if row
            != "partition_request_by_instance_id_and_prospective_epoch_id_before_reading_rows"
            and str(prior_epoch_id) not in row
            and str(prior_epoch_id).replace("-", "_") not in row
        ],
    ]
    policy["historical_same_instance_epoch_rule"] = policy.get("same_instance_epoch_rule")
    policy["same_instance_epoch_rule"] = (
        "legacy_v3_rejected_predecessor_minus5022_shadow_enabled_v4_and_"
        "fully_no_shadow_release_v3_successor_are_distinct"
    )
    policy["current_operational_metadata_updated_utc"] = receipt["generated_utc"]
    return payload


def _catalog_payload(
    predecessor: Mapping[str, Any],
    receipt: Mapping[str, Any],
    receipt_binding: Mapping[str, Any],
    pointer_data: bytes,
) -> dict[str, Any]:
    payload = deepcopy(predecessor)
    if payload.get("schema_version") != CATALOG_SCHEMA or not isinstance(
        payload.get("entries"), list
    ):
        raise OperationalMetadataV6Error("predecessor catalog schema drifted")
    context = receipt["predecessor_catalog_context"]
    if (
        _catalog_context(
            payload,
            {
                "predecessor_activation_artifact_id": receipt["catalog_transaction"][
                    "predecessor_activation_artifact_id"
                ]
            },
        )
        != context
    ):
        raise OperationalMetadataV6Error("predecessor catalog patch context drifted")
    entries = payload["entries"]
    pointer_entry = next(
        entry for entry in entries if entry.get("artifact_id") == "repository-live-remote-current"
    )
    old_id = receipt["catalog_transaction"]["predecessor_activation_artifact_id"]
    old_entry = next(entry for entry in entries if entry.get("artifact_id") == old_id)
    new_id = receipt["catalog_transaction"]["replacement_activation_artifact_id"]
    if any(entry.get("artifact_id") == new_id for entry in entries):
        raise OperationalMetadataV6Error("replacement catalog artifact id already exists")
    pointer_entry.update(
        {
            "sha256": _sha(pointer_data),
            "bytes": len(pointer_data),
            "last_verified_utc": receipt["generated_utc"],
            "notes": "Mutable current remote resolution is bound to release-v3, eacb, transport-v6, the formal lifecycle/context, durable post-lifecycle frozen health (not a latest heartbeat), and final-v6 evidence. It grants no authority.",
        }
    )
    old_entry["operational_status"] = SUPERSEDED_REASON
    old_entry["superseded_by_artifact_id"] = new_id
    old_entry["last_verified_utc"] = receipt["generated_utc"]
    entries.append(
        {
            "artifact_id": new_id,
            "role": "live_release_v3_no_shadow_final_v6_metadata_receipt",
            "local_path": receipt_binding["path"],
            "source_document": None,
            "source_line_before_migration": None,
            "sha256": receipt_binding["file_sha256"],
            "bytes": receipt_binding["size_bytes"],
            "availability": "private_not_distributed",
            "panel_role": "operational",
            "read_gate": "owner_only",
            "last_verified_utc": receipt["generated_utc"],
            "related_public_docs": ["docs/live_host_and_historical_data_access_20260811.md"],
            "public_projection": None,
            "notes": "Private create-only metadata receipt for the current fully no-shadow BUY E3 process. No research, action, or live authority is granted by this metadata successor.",
        }
    )
    payload["generated_at_utc"] = receipt["generated_utc"]
    return payload


def _catalog_predecessor_from_successor(
    successor: Mapping[str, Any], receipt: Mapping[str, Any]
) -> dict[str, Any]:
    payload = deepcopy(successor)
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise OperationalMetadataV6Error("successor catalog entries are missing")
    context = receipt["predecessor_catalog_context"]
    new_id = receipt["catalog_transaction"]["replacement_activation_artifact_id"]
    old_id = receipt["catalog_transaction"]["predecessor_activation_artifact_id"]
    if len([entry for entry in entries if entry.get("artifact_id") == new_id]) != 1:
        raise OperationalMetadataV6Error("successor catalog replacement entry is ambiguous")
    entries[:] = [entry for entry in entries if entry.get("artifact_id") != new_id]
    pointer_indexes = [
        index
        for index, entry in enumerate(entries)
        if entry.get("artifact_id") == "repository-live-remote-current"
    ]
    old_indexes = [
        index for index, entry in enumerate(entries) if entry.get("artifact_id") == old_id
    ]
    if len(pointer_indexes) != 1 or len(old_indexes) != 1:
        raise OperationalMetadataV6Error("successor catalog patch entries are ambiguous")
    entries[pointer_indexes[0]] = deepcopy(context["pointer_entry"])
    entries[old_indexes[0]] = deepcopy(context["predecessor_activation_entry"])
    payload["generated_at_utc"] = context["generated_at_utc"]
    return payload


def _normalized_key(value: Any) -> str:
    with_boundaries = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", str(value))
    return re.sub(r"[^a-z0-9]+", "_", with_boundaries.lower()).strip("_")


def _reject_secrets(value: Any, path: str = "$") -> None:
    forbidden_keys = {
        "access_key",
        "access_key_id",
        "access_token",
        "api_key",
        "api_token",
        "apikey",
        "api_secret",
        "apisecret",
        "auth_token",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "bearer_token",
        "client_secret",
        "consumer_secret",
        "credential",
        "credentials",
        "id_token",
        "oauth_token",
        "passphrase",
        "refresh_token",
        "secret",
        "secret_key",
        "password",
        "token",
        "private_key",
        "private_key_pem",
        "secret_access_key",
    }
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = _normalized_key(key)
            if (
                normalized in forbidden_keys
                or normalized.endswith(
                    (
                        "_access_token",
                        "_client_secret",
                        "_password",
                        "_private_key",
                        "_refresh_token",
                        "_secret",
                        "_secret_key",
                        "_token",
                    )
                )
                or normalized.startswith(("password_", "private_key_"))
            ):
                raise OperationalMetadataV6Error(f"secret-shaped catalog field: {path}.{key}")
            _reject_secrets(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_secrets(child, f"{path}[{index}]")
    elif isinstance(value, str):
        patterns = (
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
            r"AKIA[0-9A-Z]{16}",
            r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{16,}",
            r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b",
            r"\bgh[pousr]_[A-Za-z0-9]{20,}\b",
            r"\bxox[baprs]-[A-Za-z0-9-]{16,}\b",
            r"\bAIza[0-9A-Za-z_-]{20,}\b",
            r"\b(?:sk|rk)_live_[0-9A-Za-z]{16,}\b",
            r"(?i)(?:api[_-]?key|api[_-]?secret|password|secret[_-]?access[_-]?key)\s*[:=]\s*[^\s,}]+",
            r"(?i)\b(?:https?|ssh)://[^/\s:@]+:[^/\s@]+@",
        )
        if any(re.search(pattern, value) for pattern in patterns):
            raise OperationalMetadataV6Error(f"credential-shaped catalog value: {path}")


def _reject_economic_fields(value: Any, path: str = "$") -> None:
    """Reject persistently copied economic values and raw market/order identifiers."""

    exact_forbidden = {
        "amount",
        "ask",
        "avg_price",
        "average_price",
        "balance",
        "bid",
        "client_order_id",
        "client_trade_id",
        "cost",
        "fee",
        "fees",
        "fill_id",
        "fills",
        "inventory",
        "loss",
        "markout",
        "mid",
        "notional",
        "order_id",
        "order_ids",
        "order_price",
        "order_qty",
        "order_quantity",
        "order_size",
        "pnl",
        "position_amt",
        "position_amount",
        "position_price",
        "position_qty",
        "position_quantity",
        "position_size",
        "price",
        "profit",
        "quantity",
        "quote",
        "raw",
        "raw_line",
        "raw_payload",
        "realized_pnl",
        "size",
        "spread",
        "trade_id",
        "trade_ids",
        "trade_price",
        "trade_qty",
        "trade_quantity",
        "trade_size",
        "trades",
        "unrealized_pnl",
    }
    suffixes = (
        "_avg_price",
        "_balance",
        "_cost",
        "_fee",
        "_fees",
        "_inventory",
        "_loss",
        "_markout",
        "_notional",
        "_order_id",
        "_trade_id",
        "_position_amt",
        "_position_amount",
        "_position_qty",
        "_position_quantity",
        "_position_size",
        "_profit",
        "_realized_pnl",
        "_spread",
        "_unrealized_pnl",
    )
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = _normalized_key(key)
            if normalized in exact_forbidden or normalized.endswith(suffixes):
                raise OperationalMetadataV6Error(
                    f"economic/raw field is forbidden in metadata: {path}.{key}"
                )
            _reject_economic_fields(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_economic_fields(child, f"{path}[{index}]")


def _resolver_projection(pointer: Mapping[str, Any]) -> dict[str, Any]:
    required = (
        "provider",
        "region",
        "city",
        "ssh_target",
        "public_ipv4",
        "instance_id",
        "instance_type",
        "repo_root",
        "prospective_epoch_id",
        "prospective_epoch_identity_sha256",
        "current_process_id",
        "current_process_start_ticks",
        "config_sha256",
        "runtime_code_sha256",
    )
    if pointer.get("schema_version") != POINTER_SCHEMA or pointer.get("status") != "current_active":
        raise OperationalMetadataV6Error("successor pointer is not current_active")
    projection = {field: pointer.get(field) for field in required}
    if any(value in (None, "") for value in projection.values()):
        raise OperationalMetadataV6Error("successor resolver projection is incomplete")
    return projection


def _validate_metadata(
    manifest: Mapping[str, Any],
    receipt: Mapping[str, Any],
    receipt_data: bytes,
    pointer: Mapping[str, Any],
    pointer_data: bytes,
    catalog: Mapping[str, Any],
    catalog_data: bytes,
) -> dict[str, Any]:
    if (
        receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("status") != RECEIPT_STATUS
        or receipt.get("permissions") != NO_NEW_AUTHORITY
        or receipt.get("evidence_boundary") != EVIDENCE_BOUNDARY
        or receipt.get(RECEIPT_CANONICAL_FIELD) != _document_sha(receipt, RECEIPT_CANONICAL_FIELD)
    ):
        raise OperationalMetadataV6Error("replacement receipt canonical drifted")
    transaction = manifest["transaction"]
    receipt_path = Path(str(transaction["replacement_receipt_path"]))
    receipt_binding = _binding_from_bytes(
        receipt_path, receipt, receipt_data, RECEIPT_CANONICAL_FIELD
    )
    if pointer.get("current_activation_receipt") != {
        "path": receipt_binding["path"],
        "sha256": receipt_binding["file_sha256"],
        "canonical_sha256": receipt_binding["canonical_sha256"],
        "bytes": receipt_binding["size_bytes"],
    }:
        raise OperationalMetadataV6Error("pointer-to-receipt binding drifted")
    runtime = receipt.get("current")
    if not isinstance(runtime, Mapping):
        raise OperationalMetadataV6Error("replacement receipt current identity is missing")
    host = runtime.get("host")
    process = runtime.get("process")
    epoch = runtime.get("epoch")
    if not all(isinstance(value, Mapping) for value in (host, process, epoch)):
        raise OperationalMetadataV6Error("replacement receipt resolver identity is malformed")
    if (
        runtime.get("execution") != EXECUTION
        or runtime.get("active_config_sha256") != ACTIVE_CONFIG_SHA256
        or runtime.get("disabled_config_sha256") != DISABLED_CONFIG_SHA256
        or runtime.get("artifact_sha256") != ARTIFACT_SHA256
        or process.get("pid") != CURRENT_PID
        or process.get("pid_start_ticks") != CURRENT_PID_START_TICKS
        or process.get("runtime_identity_recorded_utc") != ACTIVE_RUNTIME_IDENTITY_RECORDED_UTC
        or process.get("active_capture_utc") != ACTIVE_CAPTURED_UTC
        or epoch.get("epoch_id") != CURRENT_EPOCH_ID
        or epoch.get("started_utc") != CURRENT_EPOCH_STARTED_UTC
        or runtime.get("publisher_source") != manifest.get("publisher_source")
    ):
        raise OperationalMetadataV6Error("replacement receipt current authority drifted")
    expected_resolver = {
        **host,
        "prospective_epoch_id": epoch["epoch_id"],
        "prospective_epoch_identity_sha256": epoch["identity_sha256"],
        "current_process_id": process["pid"],
        "current_process_start_ticks": process["pid_start_ticks"],
        "config_sha256": ACTIVE_CONFIG_SHA256,
        "runtime_code_sha256": runtime["runtime_code_sha256"],
    }
    if _resolver_projection(pointer) != expected_resolver:
        raise OperationalMetadataV6Error("successor resolver identity drifted")
    predecessor_host = receipt["predecessor_pointer"]["snapshot"]
    for field in (
        "provider",
        "region",
        "city",
        "ssh_target",
        "public_ipv4",
        "instance_id",
        "instance_type",
        "repo_root",
    ):
        if pointer.get(field) != predecessor_host.get(field):
            raise OperationalMetadataV6Error("successor host resolver presentation drifted")
    if (
        pointer.get("runtime_identity_recorded_utc") != ACTIVE_RUNTIME_IDENTITY_RECORDED_UTC
        or pointer.get("maker_started_utc") is not None
        or pointer.get("maker_started_utc_status") != "unknown_not_inferred_from_epoch_start"
        or pointer.get("activated_utc") != CURRENT_EPOCH_STARTED_UTC
    ):
        raise OperationalMetadataV6Error("successor timestamp semantics drifted")
    current_epochs = [
        row for row in pointer["host_epochs"] if row.get("status") == "current_active"
    ]
    if (
        len(current_epochs) != 1
        or current_epochs[0].get("prospective_epoch_id") != epoch["epoch_id"]
    ):
        raise OperationalMetadataV6Error("successor current epoch is ambiguous")
    historical = pointer.get("historical_superseded_operational_evidence", [])
    if not any(row.get("classification") == SUPERSEDED_REASON for row in historical):
        raise OperationalMetadataV6Error("superseded v4 history is missing")
    if set(pointer.get("current_operational_evidence", {})) != {
        "direct_release",
        "cross_host_admission",
        "config_correction",
        "resource_gate",
        "active_process_capture",
        "remote_active_attestation",
        "lifecycle_admission",
        "lifecycle_context",
        "post_lifecycle_health",
        "final_activation_envelope",
        "final_operational_completion",
        "final_composition",
        "final_attempt",
        "final_proof",
    }:
        raise OperationalMetadataV6Error("successor current evidence roles drifted")
    health = pointer.get("current_evidence_health")
    if (
        not isinstance(health, Mapping)
        or health.get("snapshot_utc") != POST_LIFECYCLE_RECEIPT_GENERATED_UTC
        or health.get("live_maker_running_at_post_lifecycle_capture") is not True
        or health.get("latest_live_status_claimed") is not False
        or health.get("economic_values_persisted") is not False
        or "live_maker_running" in health
        or health.get("main_health_rows")
        != receipt["post_lifecycle_pointer_health_snapshot"]["main_health_rows"]
    ):
        raise OperationalMetadataV6Error("pointer health is not anchored to durable post receipt")
    expected_gap = receipt["fail_closed_transition"]
    gaps = pointer.get("evidence_coverage_gaps")
    if not isinstance(gaps, list) or not any(
        isinstance(gap, Mapping)
        and all(gap.get(key) == value for key, value in expected_gap.items())
        for gap in gaps
    ):
        raise OperationalMetadataV6Error("successor fail-closed transition gap is missing")
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise OperationalMetadataV6Error("successor catalog entries are missing")
    pointer_entries = [
        entry for entry in entries if entry.get("artifact_id") == "repository-live-remote-current"
    ]
    receipt_entries = [
        entry
        for entry in entries
        if entry.get("artifact_id") == transaction["replacement_activation_artifact_id"]
    ]
    old_entries = [
        entry
        for entry in entries
        if entry.get("artifact_id") == transaction["predecessor_activation_artifact_id"]
    ]
    if len(pointer_entries) != 1 or len(receipt_entries) != 1 or len(old_entries) != 1:
        raise OperationalMetadataV6Error("successor catalog cross-binding is ambiguous")
    if (pointer_entries[0].get("sha256"), pointer_entries[0].get("bytes")) != (
        _sha(pointer_data),
        len(pointer_data),
    ) or (receipt_entries[0].get("sha256"), receipt_entries[0].get("bytes")) != (
        _sha(receipt_data),
        len(receipt_data),
    ):
        raise OperationalMetadataV6Error("successor catalog content binding drifted")
    if old_entries[0].get("operational_status") != SUPERSEDED_REASON:
        raise OperationalMetadataV6Error("catalog does not preserve v4 as superseded history")
    _reject_secrets(receipt)
    _reject_secrets(pointer)
    _reject_secrets(catalog)
    _reject_economic_fields(receipt)
    _reject_economic_fields(pointer)
    _reject_economic_fields(catalog)
    return {
        "receipt": {
            "file_sha256": _sha(receipt_data),
            "canonical_sha256": receipt[RECEIPT_CANONICAL_FIELD],
            "size_bytes": len(receipt_data),
        },
        "pointer": {"file_sha256": _sha(pointer_data), "size_bytes": len(pointer_data)},
        "catalog": {"file_sha256": _sha(catalog_data), "size_bytes": len(catalog_data)},
        "resolver_exact": True,
        "catalog_secret_scan_passed": True,
        "economic_value_and_raw_identifier_scan_passed": True,
        "cross_bind_passed": True,
    }


def _candidate_audit_proof(
    *,
    metadata_root: Path,
    receipt: Mapping[str, Any],
    receipt_data: bytes,
    pointer: Mapping[str, Any],
    pointer_data: bytes,
    catalog: Mapping[str, Any],
    catalog_data: bytes,
) -> dict[str, Any]:
    """Prove the exact metadata delta cannot add an audit finding before writes.

    The metadata-only auditor derives catalog findings from owner-root invariants
    plus catalog entries.  Owner-root state is unchanged by this transaction;
    the only catalog changes are the exact pointer entry, v4 supersession fields,
    and one operational/owner-only receipt entry validated below.  Receipt and
    pointer bytes are also scanned recursively before any official path changes.
    """

    baseline = receipt.get("metadata_audit_baseline")
    if not isinstance(baseline, Mapping):
        raise OperationalMetadataV6Error("candidate audit baseline is missing")
    if (
        baseline.get("schema_version") != audit_private_evidence.AUDIT_SCHEMA
        or baseline.get("mode") != audit_private_evidence.METADATA_ONLY
        or baseline.get("deny_locked") is not True
        or baseline.get("validation_read") is not False
        or baseline.get("sealed_holdout_read") is not False
    ):
        raise OperationalMetadataV6Error("candidate audit baseline envelope drifted")
    private_root = metadata_root / "docs" / "private"
    receipt_path = private_root / RECEIPT_FILENAME
    pointer_path = private_root / "live_remote.current.local.json"
    catalog_path = private_root / "catalog.current.local.json"
    entries = catalog.get("entries")
    if not isinstance(entries, list):
        raise OperationalMetadataV6Error("candidate audit catalog entries are missing")
    new_rows = [entry for entry in entries if entry.get("artifact_id") == REPLACEMENT_ARTIFACT_ID]
    pointer_rows = [
        entry for entry in entries if entry.get("artifact_id") == "repository-live-remote-current"
    ]
    old_rows = [entry for entry in entries if entry.get("artifact_id") == PREDECESSOR_ARTIFACT_ID]
    if len(new_rows) != 1 or len(pointer_rows) != 1 or len(old_rows) != 1:
        raise OperationalMetadataV6Error("candidate audit catalog delta is ambiguous")
    new_row = new_rows[0]
    if (
        new_row.get("local_path") != str(receipt_path)
        or new_row.get("sha256") != _sha(receipt_data)
        or new_row.get("bytes") != len(receipt_data)
        or new_row.get("panel_role") != "operational"
        or new_row.get("read_gate") != "owner_only"
        or pointer_rows[0].get("local_path") != str(pointer_path)
        or pointer_rows[0].get("sha256") != _sha(pointer_data)
        or pointer_rows[0].get("bytes") != len(pointer_data)
        or old_rows[0].get("operational_status") != SUPERSEDED_REASON
        or catalog_path.name != "catalog.current.local.json"
    ):
        raise OperationalMetadataV6Error("candidate audit exact catalog delta drifted")
    _reject_secrets(receipt)
    _reject_secrets(pointer)
    _reject_secrets(catalog)
    _reject_economic_fields(receipt)
    _reject_economic_fields(pointer)
    _reject_economic_fields(catalog)
    if catalog_data != _render(catalog):
        raise OperationalMetadataV6Error("candidate audit catalog bytes drifted")
    return {
        "proof_semantics": "deterministic_metadata_only_audit_delta_before_official_writes",
        "owner_root_state_unchanged": True,
        "exact_catalog_delta_validated": True,
        "receipt_pointer_catalog_secret_scan_passed": True,
        "receipt_pointer_catalog_economic_value_scan_passed": True,
        "candidate_new_finding_count": 0,
        "passed": True,
    }


def _fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_new(path: Path, data: bytes) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OperationalMetadataV6Error(f"short write: {path}")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _pending(path: Path, kind: str) -> Path:
    return path.with_name(f".{path.name}.{kind}-pending-v6")


def _read_exact_any_link(path: Path, data: bytes, links: frozenset[int]) -> os.stat_result:
    observed, metadata = _read_regular(path, mode=0o600, allowed_nlinks=links)
    if observed != data:
        raise OperationalMetadataV6Error(f"pending/published bytes differ from plan: {path}")
    return metadata


def _publish_create_only(path: Path, data: bytes) -> None:
    """Create an immutable file and recover the link-before-unlink crash point."""

    pending = _pending(path, "create")
    if path.exists():
        metadata = _read_exact_any_link(path, data, frozenset({1, 2}))
        if metadata.st_nlink == 2:
            if not pending.exists():
                raise OperationalMetadataV6Error("published nlink=2 receipt lacks recovery link")
            pending_meta = _read_exact_any_link(pending, data, frozenset({2}))
            if (metadata.st_dev, metadata.st_ino) != (pending_meta.st_dev, pending_meta.st_ino):
                raise OperationalMetadataV6Error("receipt recovery hardlink inode drifted")
            pending.unlink()
            _fsync_dir(path.parent)
            _read_exact_any_link(path, data, frozenset({1}))
        elif pending.exists():
            raise OperationalMetadataV6Error("orphan receipt pending path is ambiguous")
        return
    if pending.exists():
        _read_exact_any_link(pending, data, frozenset({1}))
    else:
        _write_new(pending, data)
        _fsync_dir(path.parent)
    os.link(pending, path, follow_symlinks=False)
    _fsync_dir(path.parent)
    # A crash here leaves two exact links.  The first branch repairs it.
    pending.unlink()
    _fsync_dir(path.parent)
    _read_exact_any_link(path, data, frozenset({1}))


def _atomic_replace(path: Path, data: bytes, *, kind: str) -> None:
    pending = _pending(path, kind)
    if pending.exists():
        _read_exact_any_link(pending, data, frozenset({1}))
    else:
        _write_new(pending, data)
        _fsync_dir(path.parent)
    os.replace(pending, path)
    _fsync_dir(path.parent)
    _read_exact_any_link(path, data, frozenset({1}))


AuditFn = Callable[[Path], Mapping[str, Any]]
FailureFn = Callable[[str], None]


def _default_audit(root: Path) -> Mapping[str, Any]:
    return audit_private_evidence.audit(
        root,
        mode=audit_private_evidence.METADATA_ONLY,
        deny_locked=True,
        allowlist_manifest=None,
    )


def execute(
    manifest_path: Path,
    *,
    apply: bool = False,
    audit_fn: AuditFn = _default_audit,
    failure_hook: FailureFn | None = None,
) -> dict[str, Any]:
    if os.environ.get("NARROWGATE_LIVE_REMOTE_POINTER") or os.environ.get("NARROWGATE_LIVE_REMOTE"):
        raise OperationalMetadataV6Error("live resolver environment overrides are forbidden")
    manifest, manifest_binding = _validate_manifest(manifest_path)
    transaction = manifest["transaction"]
    pointer_path = Path(str(transaction["pointer_path"]))
    catalog_path = Path(str(transaction["catalog_path"]))
    receipt_path = Path(str(transaction["replacement_receipt_path"]))
    metadata_repository_root = Path(str(manifest["metadata_repository_root"]))
    expected_private = metadata_repository_root / "docs" / "private"
    expected_paths = (
        expected_private / "live_remote.current.local.json",
        expected_private / "catalog.current.local.json",
        expected_private / RECEIPT_FILENAME,
    )
    if (
        len({pointer_path, catalog_path, receipt_path}) != 3
        or (pointer_path, catalog_path, receipt_path) != expected_paths
        or len({pointer_path.parent, catalog_path.parent, receipt_path.parent}) != 1
    ):
        raise OperationalMetadataV6Error(
            "metadata transaction paths are not the exact private targets"
        )
    if not pointer_path.parent.is_dir():
        raise OperationalMetadataV6Error("metadata transaction directory is unavailable")

    lock_descriptor = _open_metadata_transaction_lock(metadata_repository_root)
    try:
        source_context, bindings = _validate_sources(manifest)

        pointer_payload, pointer_raw, _pointer_meta = _load_json(pointer_path, mode=0o600)
        catalog_payload, catalog_raw, _catalog_meta = _load_json(catalog_path, mode=0o600)
        predecessor_activation_path = Path(str(transaction["predecessor_activation"]["path"]))
        old_activation, old_raw, _old_meta = _load_json(predecessor_activation_path, mode=0o600)
        old_binding = _content(old_activation, predecessor_activation_path, old_raw, 0o600)
        if old_binding != dict(transaction["predecessor_activation"]):
            raise OperationalMetadataV6Error("predecessor v4 activation receipt drifted")
        pointer_old = (_sha(pointer_raw), len(pointer_raw)) == (
            transaction["predecessor_pointer"]["file_sha256"],
            transaction["predecessor_pointer"]["size_bytes"],
        )
        catalog_old = (_sha(catalog_raw), len(catalog_raw)) == (
            transaction["predecessor_catalog"]["file_sha256"],
            transaction["predecessor_catalog"]["size_bytes"],
        )
        if pointer_old and pointer_payload.get("current_activation_receipt") != {
            "path": old_binding["path"],
            "sha256": old_binding["file_sha256"],
            "canonical_sha256": old_binding["canonical_sha256"],
            "bytes": old_binding["size_bytes"],
        }:
            raise OperationalMetadataV6Error("predecessor pointer-to-v4 receipt binding drifted")

        receipt_exists = receipt_path.exists()
        receipt_pending_path = _pending(receipt_path, "create")
        receipt_pending_exists = receipt_pending_path.exists()
        receipt_state = "missing"
        if not receipt_exists and not receipt_pending_exists:
            if not pointer_old or not catalog_old:
                raise OperationalMetadataV6Error(
                    "publication order inverted: pointer/catalog advanced before receipt"
                )
            audit_baseline = _audit_baseline(audit_fn(metadata_repository_root))
            context = _catalog_context(catalog_payload, transaction)
            planned_receipt = _receipt_payload(
                manifest,
                manifest_binding,
                bindings,
                source_context,
                pointer_payload,
                context,
                audit_baseline,
            )
            receipt_data = _render(planned_receipt)
        else:
            if not pointer_old and not receipt_exists:
                raise OperationalMetadataV6Error(
                    "publication order inverted: pointer advanced before receipt"
                )
            candidate = receipt_path if receipt_exists else receipt_pending_path
            allowed_links = frozenset({1, 2}) if receipt_exists else frozenset({1})
            published, receipt_data, receipt_meta = _load_json(
                candidate, mode=0o600, allowed_nlinks=allowed_links
            )
            receipt_state = "published" if receipt_exists else "pending_create_only"
            if receipt_exists and receipt_meta.st_nlink == 2:
                if not receipt_pending_path.exists():
                    raise OperationalMetadataV6Error("nlink=2 receipt is not recoverable")
                pending_meta = _read_exact_any_link(
                    receipt_pending_path, receipt_data, frozenset({2})
                )
                if (receipt_meta.st_dev, receipt_meta.st_ino) != (
                    pending_meta.st_dev,
                    pending_meta.st_ino,
                ):
                    raise OperationalMetadataV6Error("nlink=2 recovery inode drifted")
                receipt_state = "published_recoverable_nlink2"
            elif receipt_exists and receipt_pending_path.exists():
                raise OperationalMetadataV6Error("orphan receipt pending path is ambiguous")
            planned_receipt = published
            if (
                planned_receipt.get("activation_manifest") != manifest_binding
                or planned_receipt.get("predecessor_pointer", {}).get("file_sha256")
                != transaction["predecessor_pointer"]["file_sha256"]
            ):
                raise OperationalMetadataV6Error("published receipt manifest/precondition drifted")
            expected_receipt = _receipt_payload(
                manifest,
                manifest_binding,
                bindings,
                source_context,
                planned_receipt["predecessor_pointer"]["snapshot"],
                planned_receipt["predecessor_catalog_context"],
                planned_receipt["metadata_audit_baseline"],
            )
            if planned_receipt != expected_receipt or receipt_data != _render(expected_receipt):
                raise OperationalMetadataV6Error("published replacement receipt differs from plan")

        receipt_binding = _binding_from_bytes(
            receipt_path, planned_receipt, receipt_data, RECEIPT_CANONICAL_FIELD
        )
        planned_pointer = _pointer_payload(planned_receipt, receipt_binding)
        planned_pointer_data = _render(planned_pointer)
        if not pointer_old and pointer_raw != planned_pointer_data:
            raise OperationalMetadataV6Error("pointer is neither predecessor nor exact successor")
        pointer_new = pointer_raw == planned_pointer_data

        if catalog_old:
            planned_catalog = _catalog_payload(
                catalog_payload, planned_receipt, receipt_binding, planned_pointer_data
            )
            planned_catalog_data = _render(planned_catalog)
            catalog_new = False
        else:
            predecessor_catalog = _catalog_predecessor_from_successor(
                catalog_payload, planned_receipt
            )
            predecessor_catalog_data = _render(predecessor_catalog)
            if (_sha(predecessor_catalog_data), len(predecessor_catalog_data)) != (
                transaction["predecessor_catalog"]["file_sha256"],
                transaction["predecessor_catalog"]["size_bytes"],
            ):
                raise OperationalMetadataV6Error("successor catalog cannot reconstruct predecessor")
            planned_catalog = _catalog_payload(
                predecessor_catalog, planned_receipt, receipt_binding, planned_pointer_data
            )
            planned_catalog_data = _render(planned_catalog)
            if catalog_raw != planned_catalog_data:
                raise OperationalMetadataV6Error(
                    "catalog is neither predecessor nor exact successor"
                )
            catalog_new = True

        if catalog_new and not pointer_new:
            raise OperationalMetadataV6Error(
                "publication order inverted: catalog advanced before pointer"
            )
        validated = _validate_metadata(
            manifest,
            planned_receipt,
            receipt_data,
            planned_pointer,
            planned_pointer_data,
            planned_catalog,
            planned_catalog_data,
        )
        candidate_audit = _candidate_audit_proof(
            metadata_root=metadata_repository_root,
            receipt=planned_receipt,
            receipt_data=receipt_data,
            pointer=planned_pointer,
            pointer_data=planned_pointer_data,
            catalog=planned_catalog,
            catalog_data=planned_catalog_data,
        )
        prewrite_audit = _assert_no_new_findings(
            planned_receipt["metadata_audit_baseline"],
            audit_fn(metadata_repository_root),
        )
        for target, data, kind in (
            (pointer_path, planned_pointer_data, "pointer"),
            (catalog_path, planned_catalog_data, "catalog"),
        ):
            pending = _pending(target, kind)
            if pending.exists():
                _read_exact_any_link(pending, data, frozenset({1}))
        state_before = {
            "receipt": receipt_state,
            "pointer": "successor" if pointer_new else "predecessor",
            "catalog": "successor" if catalog_new else "predecessor",
        }
        result: dict[str, Any] = {
            **validated,
            "mode": "apply" if apply else "dry_run",
            "status": "planned_or_resumed_exact_transaction",
            "state_before": state_before,
            "ordered_steps": ["receipt", "pointer", "catalog"],
            "metadata_audit_contract": {
                "comparison_semantics": "after_finding_set_minus_before_finding_set",
                "preexisting_findings_may_remain": True,
                "required_new_finding_count": 0,
            },
            "prepublication_candidate_audit": candidate_audit,
            "prewrite_metadata_audit": prewrite_audit,
            "source_count": len(bindings),
            "writes_performed": False,
        }
        if not apply:
            return result

        _publish_create_only(receipt_path, receipt_data)
        if failure_hook is not None:
            failure_hook("receipt")
        if not pointer_new:
            _atomic_replace(pointer_path, planned_pointer_data, kind="pointer")
        if failure_hook is not None:
            failure_hook("pointer")
        if not catalog_new:
            _atomic_replace(catalog_path, planned_catalog_data, kind="catalog")
        if failure_hook is not None:
            failure_hook("catalog")

        actual_receipt, actual_receipt_raw, receipt_meta = _load_json(receipt_path, mode=0o600)
        actual_pointer, actual_pointer_raw, pointer_meta = _load_json(pointer_path, mode=0o600)
        actual_catalog, actual_catalog_raw, catalog_meta = _load_json(catalog_path, mode=0o600)
        if (actual_receipt_raw, actual_pointer_raw, actual_catalog_raw) != (
            receipt_data,
            planned_pointer_data,
            planned_catalog_data,
        ) or (receipt_meta.st_nlink, pointer_meta.st_nlink, catalog_meta.st_nlink) != (1, 1, 1):
            raise OperationalMetadataV6Error("post-publication byte/link identity drifted")
        _validate_metadata(
            manifest,
            actual_receipt,
            actual_receipt_raw,
            actual_pointer,
            actual_pointer_raw,
            actual_catalog,
            actual_catalog_raw,
        )
        expected_live_remote = {
            field: str(actual_pointer[field])
            for field in ("ssh_target", "provider", "region", "city", "public_ipv4", "repo_root")
        }
        if (
            live_remote_pointer.active_live_remote_fields(metadata_repository_root)
            != expected_live_remote
        ):
            raise OperationalMetadataV6Error(
                "published pointer is incompatible with active_live_remote_fields"
            )
        try:
            audit_after = audit_fn(metadata_repository_root)
            result["metadata_audit"] = _compare_audit_findings(
                planned_receipt["metadata_audit_baseline"], audit_after
            )
        except Exception as exc:  # post-commit diagnostics must not imply rollback
            result["metadata_audit"] = {
                "comparison_semantics": "post_commit_diagnostic_unavailable",
                "passed": None,
                "new_finding_count": None,
                "new_finding_fingerprints": [],
                "new_findings": [],
                "diagnostic_error_type": type(exc).__name__,
                "diagnostic_error_message": str(exc),
            }
        result["writes_performed"] = True
        result["post_write_verified"] = True
        result["transaction_committed"] = True
        audit_passed = result["metadata_audit"]["passed"]
        result["post_audit_diagnostic_error_detected"] = audit_passed is None
        result["post_audit_drift_detected"] = None if audit_passed is None else not audit_passed
        result["post_audit_drift_attribution"] = (
            "unattributed_after_commit" if audit_passed is not True else "none"
        )
        if audit_passed is None:
            result["status"] = "committed_exact_transaction_with_post_audit_diagnostic_error"
        elif audit_passed is False:
            result["status"] = "committed_exact_transaction_with_unattributed_post_audit_drift"
        else:
            result["status"] = "completed_exact_transaction"
        result["active_live_remote_fields_compatible"] = True
        result["state_after"] = {
            "receipt": "published_nlink1",
            "pointer": "successor",
            "catalog": "successor",
        }
        return result
    finally:
        _close_metadata_transaction_lock(lock_descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser(
        "prepare-manifest", help="create and recursively validate the durable manifest"
    )
    prepare.add_argument("--publisher-root", type=Path, required=True)
    prepare.add_argument("--metadata-repository-root", type=Path, required=True)
    prepare.add_argument("--current-runtime-root", type=Path, required=True)
    prepare.add_argument("--historical-v4-root", type=Path, required=True)
    prepare.add_argument("--receipt-id", required=True)
    prepare.add_argument("--output", type=Path, default=Path(FORMAL_MANIFEST_PATH))
    validate = commands.add_parser(
        "validate-manifest", help="recursively validate the durable manifest"
    )
    validate.add_argument("--manifest", type=Path, required=True)
    run = commands.add_parser("run", help="plan or apply the ordered metadata transaction")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--apply", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "prepare-manifest":
        result = prepare_activation_manifest(
            publisher_root=args.publisher_root,
            metadata_repository_root=args.metadata_repository_root,
            current_runtime_root=args.current_runtime_root,
            historical_v4_root=args.historical_v4_root,
            receipt_id=args.receipt_id,
            output_path=args.output,
        )
    elif args.command == "validate-manifest":
        result = validate_activation_manifest(args.manifest)
    else:
        result = execute(args.manifest, apply=args.apply)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
