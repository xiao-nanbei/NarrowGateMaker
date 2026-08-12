#!/usr/bin/env python3
"""Build the bounded v11 strict-native mechanics receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from data_paths import data_root
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as features,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_shared_prefix as shared_prefix,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_strict_labels as strict_labels,
)

IDENTITY = strict_labels.IDENTITY
RECEIPT_IDENTITY = f"{IDENTITY}_benchmark_v11"
RECEIPT_SCHEMA_VERSION = f"{IDENTITY}.engineering_benchmark_admission.v2"
TARGET_DAY = "2026-04-17"
FEATURE_BLOCK = "M2"
SUPPORT_IDENTITY = strict_labels.FULL_SUPPORT_IDENTITY
MAX_OPPORTUNITIES = 1
DATA_ROOT = data_root(Path(__file__).resolve().parents[4])
DEFAULT_OUTPUT = DATA_ROOT / (
    "reports/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "engineering_benchmarks/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_benchmark_v11"
)
DEFAULT_TARGET_RECEIPT = DATA_ROOT / (
    "reports/"
    "causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "strict_native_one_shot_labels/panel_runner/formal_full_support_41d/"
    "native_cache_prebuild_union_v3/targets/2026-04-17.json"
)
DEFAULT_MARKET_CACHE = DATA_ROOT / (
    "cache/"
    "current_live_held_ber_baseline_50d_20260810"
)
DEFAULT_NATIVE_CACHE = DATA_ROOT / (
    "cache/replay_dag/"
    "native_exchange_book_hour_v1"
)


class MechanicsReceiptError(RuntimeError):
    """Raised when bounded mechanics evidence is incomplete or drifts."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MechanicsReceiptError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MechanicsReceiptError(f"JSON root must be an object: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.parent / f".{path.name}.staging.{os.getpid()}.{uuid.uuid4().hex}"
    with staging.open("x", encoding="ascii") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(staging, path)


def _bound_path(root: Path, value: str, *, role: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = root / path
    path = path.resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise MechanicsReceiptError(f"{role} escapes receipt root: {path}") from exc
    if not path.is_file():
        raise MechanicsReceiptError(f"{role} is missing: {path}")
    return path


def _check_binding(
    root: Path,
    binding: Mapping[str, Any],
    *,
    role: str,
) -> Path:
    path = _bound_path(root, str(binding.get("path", "")), role=role)
    if str(binding.get("sha256", "")) != _sha256(path):
        raise MechanicsReceiptError(f"{role} hash drifted")
    size = binding.get("size_bytes")
    if size is not None and int(size) != path.stat().st_size:
        raise MechanicsReceiptError(f"{role} size drifted")
    return path


def _file_binding(root: Path, path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise MechanicsReceiptError(f"artifact escapes receipt root: {path}") from exc
    return {
        "path": str(relative),
        "sha256": _sha256(resolved),
        "size_bytes": resolved.stat().st_size,
    }


def _validate_v9_before_replay() -> dict[str, Any]:
    from research.families.f05_fill_quality_quote_ev.audit import (
        causal_multichannel_window_boolean_cooldown_v2_preflight as preflight,
    )

    spec = preflight._load_json(preflight.SPEC)
    amendments = preflight._validate_amendments(spec)
    preflight._validate_component_bindings(spec, amendments)
    return dict(amendments["execution_v9"])


def _audit_day(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    day_root = (
        root
        / f"support_identity={SUPPORT_IDENTITY}"
        / f"feature_block={FEATURE_BLOCK}"
        / f"execution_identity={strict_labels.FORMAL_EXECUTION_IDENTITY}"
        / "days"
        / TARGET_DAY
    )
    day_manifest_path = day_root / "manifest.json"
    day_success_path = day_root / "_SUCCESS"
    day_manifest = strict_labels._validate_day_admission(
        day_root,
        expected_day=TARGET_DAY,
        expected_feature_block=FEATURE_BLOCK,
        expected_support_identity=SUPPORT_IDENTITY,
        expected_max_opportunities=MAX_OPPORTUNITIES,
    )
    day_success = _load_json(day_success_path)
    if day_success.get("manifest_sha256") != _sha256(day_manifest_path):
        raise MechanicsReceiptError("day success marker hash drifted")
    if day_manifest.get("schema_version") != strict_labels.DAY_SCHEMA_VERSION:
        raise MechanicsReceiptError("day schema drifted")
    if day_manifest.get("economic_outcomes_read_by_runner") is not False:
        raise MechanicsReceiptError("bounded runner claims economic outcomes were read")
    for flag in ("nested_oof_run", "action_authorized", "live_authorized"):
        if day_manifest.get(flag) is not False:
            raise MechanicsReceiptError(f"bounded runner permission drifted: {flag}")

    snapshot_path = _check_binding(
        root,
        day_manifest.get("assignment_snapshots", {}),
        role="assignment snapshots",
    )
    source_path = _check_binding(
        root,
        day_manifest.get("source_contract", {}),
        role="source contract",
    )
    label_bindings = day_manifest.get("one_shot_label_manifests")
    if not isinstance(label_bindings, list) or len(label_bindings) != 1:
        raise MechanicsReceiptError("receipt requires exactly one opportunity")
    opportunity_path = _check_binding(
        root,
        label_bindings[0],
        role="opportunity manifest",
    )
    opportunity = _load_json(opportunity_path)
    if opportunity.get("schema_version") != (
        shared_prefix.OPPORTUNITY_MANIFEST_SCHEMA_VERSION
    ):
        raise MechanicsReceiptError("opportunity schema drifted")
    if int(opportunity.get("arm_count", -1)) != 8:
        raise MechanicsReceiptError("opportunity must contain eight arms")
    arm_bindings = opportunity.get("arms")
    if not isinstance(arm_bindings, list) or len(arm_bindings) != 8:
        raise MechanicsReceiptError("opportunity arm bindings are incomplete")

    arm_rows: list[dict[str, Any]] = []
    arm_ids: set[str] = set()
    side: str | None = None
    for binding in arm_bindings:
        if not isinstance(binding, Mapping):
            raise MechanicsReceiptError("arm binding is not an object")
        arm_path = _check_binding(
            root,
            {
                **binding,
                "path": str(opportunity_path.parent / str(binding.get("path", ""))),
            },
            role="arm result",
        )
        arm = _load_json(arm_path)
        if arm.get("schema_version") != shared_prefix.ARM_RESULT_SCHEMA_VERSION:
            raise MechanicsReceiptError("arm schema drifted")
        arm_id = str(arm.get("arm_id", ""))
        arm_ids.add(arm_id)
        execution = arm.get("strict_execution_contract")
        if not isinstance(execution, Mapping):
            raise MechanicsReceiptError("arm strict execution contract is missing")
        missing_count = int(execution.get("exchange_book_queue_missing_count", -1))
        missing_trace = execution.get("exchange_book_queue_missing_trace")
        if not isinstance(missing_trace, list) or len(missing_trace) != missing_count:
            raise MechanicsReceiptError("arm queue-missing trace is incomplete")
        trace_keys: set[tuple[str, int]] = set()
        for trace_row in missing_trace:
            if not isinstance(trace_row, Mapping) or set(trace_row) != (
                shared_prefix.MISSING_TRACE_FIELDS
            ):
                raise MechanicsReceiptError("arm queue-missing trace schema drifted")
            trace_key = (
                str(trace_row["order_id"]),
                int(trace_row["activate_ts_ms"]),
            )
            if trace_key in trace_keys:
                raise MechanicsReceiptError("arm queue-missing trace has duplicates")
            trace_keys.add(trace_key)

        eligible = execution.get("strict_native_label_eligible") is True
        point_status = str(execution.get("economic_point_label_status", ""))
        fork_trace = arm.get("fork_trace")
        if not isinstance(fork_trace, Mapping):
            raise MechanicsReceiptError("arm fork trace is missing")
        observed_side = str(fork_trace.get("side", "")).upper()
        if observed_side not in {"BUY", "SELL"}:
            raise MechanicsReceiptError("arm fork trace side is invalid")
        side = observed_side if side is None else side
        if observed_side != side:
            raise MechanicsReceiptError("opportunity pools BUY and SELL arms")
        if eligible:
            if point_status != "eligible":
                raise MechanicsReceiptError("eligible arm point-label status drifted")
        else:
            if point_status != "unsupported_redacted":
                raise MechanicsReceiptError("unsupported arm was not redacted")
            if fork_trace.get("assignment_to_washout_value_usdc") is not None:
                raise MechanicsReceiptError("unsupported arm retained a point label")
        arm_rows.append(
            {
                "arm_id": arm_id,
                "path": str(arm_path.relative_to(root)),
                "sha256": _sha256(arm_path),
                "strict_native_label_eligible": eligible,
                "economic_point_label_status": point_status,
                "queue_missing_count": missing_count,
                "queue_invalidated_order_count": int(
                    execution.get("exchange_book_queue_invalidated_order_count", -1)
                ),
                "queue_ambiguous_event_count": int(
                    execution.get("exchange_book_queue_ambiguous_event_count", -1)
                ),
            }
        )

    expected_ids = (
        set(features.BUY_DURATION_POLICY_IDS)
        if side == "BUY"
        else set(features.SELL_DURATION_POLICY_IDS)
    )
    if arm_ids != expected_ids:
        raise MechanicsReceiptError("opportunity duration-arm identity set drifted")
    unsupported_count = sum(not row["strict_native_label_eligible"] for row in arm_rows)
    if unsupported_count < 1:
        raise MechanicsReceiptError("v11 must exercise unsupported-arm redaction")

    parent_queue = day_manifest.get("strict_native_queue")
    if not isinstance(parent_queue, Mapping):
        raise MechanicsReceiptError("parent strict-native queue audit is missing")
    for field in (
        "missing_queue_seed_count",
        "invalidated_order_count",
        "ambiguous_event_count",
    ):
        if int(parent_queue.get(field, 0)) != 0:
            raise MechanicsReceiptError(f"parent/common-prefix queue failed: {field}")
    if int(parent_queue.get("source_gap_events", 0)) != 0:
        raise MechanicsReceiptError("parent native source gap is nonzero")

    return {
        "target_day": TARGET_DAY,
        "feature_block": FEATURE_BLOCK,
        "support_identity": SUPPORT_IDENTITY,
        "max_opportunities": MAX_OPPORTUNITIES,
        "day_manifest": _file_binding(root, day_manifest_path),
        "day_success_marker": _file_binding(root, day_success_path),
        "assignment_snapshots": _file_binding(root, snapshot_path),
        "source_contract": _file_binding(root, source_path),
        "opportunity_manifest": _file_binding(root, opportunity_path),
        "side": side,
        "arm_count": len(arm_rows),
        "eligible_arm_count": len(arm_rows) - unsupported_count,
        "unsupported_arm_count": unsupported_count,
        "redacted_arm_count": sum(
            row["economic_point_label_status"] == "unsupported_redacted"
            for row in arm_rows
        ),
        "queue_missing_trace_row_count": sum(
            row["queue_missing_count"] for row in arm_rows
        ),
        "queue_invalidated_order_count": sum(
            row["queue_invalidated_order_count"] for row in arm_rows
        ),
        "queue_ambiguous_event_count": sum(
            row["queue_ambiguous_event_count"] for row in arm_rows
        ),
        "arms": sorted(arm_rows, key=lambda row: row["arm_id"]),
        "aggregate_economic_values_read": False,
        "eligible_arm_point_values_accessed": False,
        "nested_oof_run": False,
        "action_authorized": False,
        "live_authorized": False,
    }


def validate_receipt(output: Path = DEFAULT_OUTPUT) -> dict[str, Any]:
    root = Path(output).expanduser().resolve()
    receipt_path = root / "admission_receipt.json"
    success_path = root / "_RECEIPT_SUCCESS"
    receipt = _load_json(receipt_path)
    success = _load_json(success_path)
    if success.get("receipt_sha256") != _sha256(receipt_path):
        raise MechanicsReceiptError("receipt success marker hash drifted")
    if receipt.get("schema_version") != RECEIPT_SCHEMA_VERSION:
        raise MechanicsReceiptError("receipt schema drifted")
    if receipt.get("identity") != IDENTITY:
        raise MechanicsReceiptError("receipt identity drifted")
    if receipt.get("benchmark_identity") != RECEIPT_IDENTITY:
        raise MechanicsReceiptError("benchmark identity drifted")
    audit = _audit_day(root)
    if receipt.get("audit") != audit:
        raise MechanicsReceiptError("receipt audit no longer matches bound artifacts")
    permissions = receipt.get("permissions")
    if not isinstance(permissions, Mapping) or any(permissions.values()):
        raise MechanicsReceiptError("receipt permissions exceed mechanics authority")
    return receipt


def run_receipt(
    *,
    output: Path = DEFAULT_OUTPUT,
    target_receipt: Path = DEFAULT_TARGET_RECEIPT,
    cache_root: Path = DEFAULT_MARKET_CACHE,
    native_cache: Path = DEFAULT_NATIVE_CACHE,
) -> dict[str, Any]:
    root = Path(output).expanduser().resolve()
    if (root / "admission_receipt.json").is_file():
        return validate_receipt(root)
    v9 = _validate_v9_before_replay()
    target_receipt = Path(target_receipt).expanduser().resolve()
    if not target_receipt.is_file():
        raise MechanicsReceiptError(
            f"prebuilt 72-hour target receipt is missing: {target_receipt}"
        )
    strict_labels.run_day(
        TARGET_DAY,
        feature_block=FEATURE_BLOCK,
        support_identity=SUPPORT_IDENTITY,
        max_opportunities=MAX_OPPORTUNITIES,
        output=root,
        cache_root=Path(cache_root),
        native_cache=Path(native_cache),
        native_cache_receipt=target_receipt,
    )
    audit = _audit_day(root)
    payload = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "benchmark_identity": RECEIPT_IDENTITY,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "execution_amendment_v9": {
            "path": str(v9["path"]),
            "sha256": str(v9["sha256"]),
        },
        "target_receipt": {
            "path": str(target_receipt),
            "sha256": _sha256(target_receipt),
        },
        "audit": audit,
        "permissions": {
            "economic_outcomes_read": False,
            "nested_oof_run": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    _atomic_json(root / "admission_receipt.json", payload)
    _atomic_json(
        root / "_RECEIPT_SUCCESS",
        {"receipt_sha256": _sha256(root / "admission_receipt.json")},
    )
    return validate_receipt(root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    run.add_argument("--target-receipt", type=Path, default=DEFAULT_TARGET_RECEIPT)
    run.add_argument("--cache-root", type=Path, default=DEFAULT_MARKET_CACHE)
    run.add_argument("--native-cache", type=Path, default=DEFAULT_NATIVE_CACHE)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.command == "run":
        payload = run_receipt(
            output=args.output,
            target_receipt=args.target_receipt,
            cache_root=args.cache_root,
            native_cache=args.native_cache,
        )
    else:
        payload = validate_receipt(args.output)
    print(json.dumps(payload, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
