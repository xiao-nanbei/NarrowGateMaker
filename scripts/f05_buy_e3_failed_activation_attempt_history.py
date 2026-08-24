#!/usr/bin/env python3
"""Bind failed BUY E3 activation attempts as historical, non-authoritative evidence.

This additive receipt does not turn any failed session token into an admitted
epoch.  It opens only the three retained aggregate JSON sources named on the
CLI and never reads economic outcomes, Validation, or sealed holdout data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_current_host_resource_gate_v7 as resource_v7,
)
from scripts import f05_buy_e3_active_release as release_io
from scripts import f05_buy_e3_evidence_completion as completion

OWNER: Final = completion.OWNER
SCHEMA_VERSION: Final = f"{OWNER}.failed_activation_attempt_history.v1"
STATUS: Final = "failed_activation_attempts_bound_historical_only_non_authority"
CANONICAL_FIELD: Final = "canonical_failed_activation_attempt_history_sha256"
FORMAL_MODULE_ROUTE: Final = "scripts.f05_buy_e3_failed_activation_attempt_history"

FAILED_ACTIVATION_SOURCE: Final = {
    "schema_version": "f05_buy_e3_rejected_predecessor_epoch_receipt.v1",
    "status": "rejected_not_admitted",
    "file_sha256": "c44f3f32ae61635ce683e5711f19fd59863e4235996a9401f48d62bc1af4d80b",
    "canonical_field": "canonical_rejected_epoch_receipt_sha256",
    "canonical_sha256": "4a3c01f7f178fa2d3f573a1696c637074fd74b51e846bd785886689ba44613d1",
    "size_bytes": 7_124,
    "mode": "0600",
}
FAILED_SESSION_TOKEN: Final = "prospective-1787532118813602859-5382e2bcdaeb"

V6_WRONG_ROUTE_BENCHMARK: Final = {
    "schema_version": f"{OWNER}.exact_four_file_host_benchmark.v4",
    "status": "exact_v4_four_file_aggregate_benchmark_passed",
    "file_sha256": "f7afb2cce38ad6886ac9d52fd0cb396f64c47ecc4ec93db59bd844afde3c0a13",
    "canonical_field": "canonical_benchmark_receipt_sha256",
    "canonical_sha256": "1706c90db32db2c7d135f344c3bfb5e805c42c1c4f2b8b3e8483ae395f4fb501",
    "size_bytes": 9_236,
    "mode": "0600",
}
V7_ATTEMPT2_BENCHMARK: Final = {
    "schema_version": resource_v7.BENCHMARK_SCHEMA,
    "status": resource_v7.BENCHMARK_STATUS,
    "file_sha256": "2d1b06f66bd4a4d60c880f0a4ea0849fc6068bb383ff250c6925663f63a500ad",
    "canonical_field": "canonical_benchmark_receipt_sha256",
    "canonical_sha256": "4aef96de07a9ab93741b28e09819bfd7c4774e53800da893cfba6635f085284f",
    "size_bytes": 9_253,
    "mode": "0600",
}

V7_ATTEMPT1_REMOTE_REPORT: Final = {
    "content_admitted": False,
    "exact7_binding_claimed": False,
    "reported_remote_file_sha256": (
        "90885b682a6c60cff2d2d23c96be2daec4137584d8940b0577eec0da396e55e3"
    ),
    "reported_remote_canonical_sha256": (
        "ae6db27aec1c9d92fc29423e1a1b3df9daa502ded5c35fbae6d4385213b769dc"
    ),
    "reported_remote_size_bytes": 9_265,
    "reported_remote_mode": "0600",
    "reported_schema_version": resource_v7.BENCHMARK_SCHEMA,
    "reported_status": resource_v7.BENCHMARK_STATUS,
}

CONTENT_BINDING_FIELDS: Final = {
    "schema_version",
    "status",
    "file_sha256",
    "canonical_field",
    "canonical_sha256",
    "size_bytes",
    "mode",
}
PERMISSIONS: Final = {"research": False, "action": False, "live": False}
AUTHORITY_DESIGN: Final = {
    "historical_only": True,
    "runtime_authority": False,
    "evidence_authority": False,
    "epoch_authority": False,
    "reusable_for_current": False,
    "failed_attempts_do_not_replace_current_runtime_authority": True,
}
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
    "active_process_started_by_history_builder": False,
    "resource_gate_retried_by_history_builder": False,
}

MAX_JSON_BYTES: Final = 16 << 20


class FailedActivationHistoryError(RuntimeError):
    """Raised when historical failure evidence is incomplete or drifts."""


@dataclass(frozen=True)
class OpenedJson:
    path: Path
    payload: dict[str, Any]
    raw: bytes
    metadata: os.stat_result


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _document_sha256(payload: Mapping[str, Any], field: str) -> str:
    body = dict(payload)
    body.pop(field, None)
    return _canonical_sha256(body)


def _timestamp(value: Any, label: str) -> str:
    normalized = str(value)
    try:
        parsed = datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise FailedActivationHistoryError(f"{label} is not canonical UTC") from exc
    if not normalized.endswith("Z") or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise FailedActivationHistoryError(f"{label} is not canonical UTC")
    return normalized


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise FailedActivationHistoryError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _open_private_json(path: Path, label: str) -> OpenedJson:
    candidate = path.expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_file():
        raise FailedActivationHistoryError(f"{label} is not a regular non-symlink file")
    target = candidate.resolve(strict=True)
    before = target.stat()
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_size <= 0
        or before.st_size > MAX_JSON_BYTES
    ):
        raise FailedActivationHistoryError(f"{label} is not private mode 0600 single-link")
    try:
        raw = target.read_bytes()
        payload = json.loads(
            raw,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                FailedActivationHistoryError(f"non-finite JSON value: {value}")
            ),
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FailedActivationHistoryError(f"{label} is unreadable JSON") from exc
    after = target.stat()
    if (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise FailedActivationHistoryError(f"{label} changed while read")
    if not isinstance(payload, dict):
        raise FailedActivationHistoryError(f"{label} root is not an object")
    return OpenedJson(target, payload, raw, before)


def _validate_content_source(
    path: Path,
    *,
    expected: Mapping[str, Any],
    label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if set(expected) != CONTENT_BINDING_FIELDS:
        raise FailedActivationHistoryError(f"{label} frozen binding fields drifted")
    opened = _open_private_json(path, label)
    payload = dict(opened.payload)
    canonical_field = str(expected["canonical_field"])
    observed = {
        "schema_version": payload.get("schema_version"),
        "status": payload.get("status"),
        "file_sha256": hashlib.sha256(opened.raw).hexdigest(),
        "canonical_field": canonical_field,
        "canonical_sha256": payload.get(canonical_field),
        "size_bytes": len(opened.raw),
        "mode": "0600",
    }
    if observed != dict(expected) or payload.get(canonical_field) != _document_sha256(
        payload, canonical_field
    ):
        raise FailedActivationHistoryError(f"{label} exact content identity drifted")
    return payload, observed


def _validate_failed_activation_source(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload, binding = _validate_content_source(
        path,
        expected=FAILED_ACTIVATION_SOURCE,
        label="historical failed activation source",
    )
    epoch = payload.get("epoch")
    rejection = payload.get("rejection")
    boundary = payload.get("authority_boundary")
    if (
        not isinstance(epoch, Mapping)
        or not isinstance(rejection, Mapping)
        or not isinstance(boundary, Mapping)
        or epoch.get("baseline_epoch_id") != FAILED_SESSION_TOKEN
        or rejection.get("error_count") != 1
        or rejection.get("drop_count") != 0
        or rejection.get("exchange_error_code") != -5022
        or rejection.get("formal_collection_valid") is not False
        or rejection.get("formal_admission_allowed") is not False
        or any(value is not False for value in boundary.values())
    ):
        raise FailedActivationHistoryError("historical failed activation semantics drifted")
    return payload, binding


def _validate_benchmark(
    path: Path,
    *,
    expected: Mapping[str, Any],
    label: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload, binding = _validate_content_source(path, expected=expected, label=label)
    checks = payload.get("checks")
    boundary = payload.get("evidence_boundary")
    if (
        not isinstance(checks, Mapping)
        or set(checks)
        != {
            "aggregate_only_no_action_rows",
            "callback_p99_at_most_2ms",
            "decision_p99_at_most_10ms",
            "exact_four_deployed_files_bound",
            "exactly_1000_decisions",
            "true_2x_observed_callback_rate",
        }
        or any(value is not True for value in checks.values())
        or not isinstance(boundary, Mapping)
        or boundary.get("aggregate_only") is not True
        or any(
            boundary.get(name) is not False
            for name in (
                "connected_to_live_market_stream",
                "benchmark_action_rows_persisted",
                "economic_values_persisted",
                "new_economic_arm_run",
                "validation_read",
                "sealed_holdout_read",
                "shadow_created",
                "companion_created",
                "hypothetical_live_actions_scored",
                "action_authorized_by_resource_receipt",
                "live_authorized_by_resource_receipt",
            )
        )
    ):
        raise FailedActivationHistoryError(f"{label} aggregate-only semantics drifted")
    return payload, binding


def _attempts(
    *,
    v6_binding: Mapping[str, Any],
    v7_attempt2_binding: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "resource_v5": {
            "source_content_available": False,
            "formal_benchmark_output_created": False,
            "formal_resource_receipt_created": False,
            "active_process_started": False,
            "failures": [
                {
                    "stage": "precondition",
                    "class": "cross_pid_old_tail_counter_regression_rejected",
                    "old_process_updates": 2_127,
                    "new_process_first_updates": 562,
                    "negative_delta_accepted": False,
                },
                {
                    "stage": "build",
                    "class": "predecessor_health_tail_not_current_process_window",
                    "formal_output_published": False,
                },
            ],
        },
        "resource_v6": {
            "benchmark": dict(v6_binding),
            "benchmark_semantics": "aggregate_performance_checks_passed",
            "failure": "child_module_route_emitted_v4_schema_while_v6_expected_v5",
            "formal_resource_receipt_created": False,
            "active_process_started": False,
        },
        "resource_v7_attempt1": {
            "benchmark": dict(V7_ATTEMPT1_REMOTE_REPORT),
            "failure_counter": "globalFlowOOO",
            "absolute_baseline": 29,
            "absolute_final": 31,
            "window_delta": 2,
            "zero_delta_gate_passed": False,
            "formal_resource_receipt_created": False,
            "active_process_started": False,
        },
        "resource_v7_attempt2": {
            "benchmark": dict(v7_attempt2_binding),
            "failure_counter": "globalFlowOOO",
            "absolute_baseline": 35,
            "absolute_final": 37,
            "window_delta": 2,
            "zero_delta_gate_passed": False,
            "formal_resource_receipt_created": False,
            "active_process_started": False,
        },
    }


def build_failed_activation_attempt_history(
    *,
    failed_activation_source_path: Path,
    v6_wrong_route_benchmark_path: Path,
    v7_attempt2_benchmark_path: Path,
    generated_utc: str | None = None,
) -> dict[str, Any]:
    failed, failed_binding = _validate_failed_activation_source(failed_activation_source_path)
    _v6, v6_binding = _validate_benchmark(
        v6_wrong_route_benchmark_path,
        expected=V6_WRONG_ROUTE_BENCHMARK,
        label="resource-v6 wrong-route benchmark",
    )
    _v7_attempt2, v7_attempt2_binding = _validate_benchmark(
        v7_attempt2_benchmark_path,
        expected=V7_ATTEMPT2_BENCHMARK,
        label="resource-v7 attempt2 benchmark",
    )
    epoch = failed["epoch"]
    rejection = failed["rejection"]
    timestamp = generated_utc or _now()
    _timestamp(timestamp, "history generated timestamp")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "identity": OWNER,
        "status": STATUS,
        "generated_utc": timestamp,
        "failed_activation_source": failed_binding,
        "failed_activation_projection": {
            "source_reported_unadmitted_session_token": FAILED_SESSION_TOKEN,
            "attempted_runtime": {
                "execution_commit": epoch["execution_commit"],
                "execution_tree": epoch["execution_tree"],
                "config_sha256": epoch["config_sha256"],
                "pid": epoch["pid"],
                "pid_start_ticks": epoch["pid_start_ticks"],
            },
            "rejection": {
                "error_count": rejection["error_count"],
                "drop_count": rejection["drop_count"],
                "exchange_error_code": rejection["exchange_error_code"],
                "formal_collection_valid": False,
                "formal_admission_allowed": False,
            },
            "epoch_established": False,
            "runtime_authority": False,
            "evidence_authority": False,
            "reusable_for_current": False,
        },
        "resource_gate_attempts": _attempts(
            v6_binding=v6_binding,
            v7_attempt2_binding=v7_attempt2_binding,
        ),
        "summary": {
            "failed_attempt_count": 5,
            "admitted_epoch_count": 0,
            "resource_receipt_count": 0,
            "active_process_started_in_resource_attempts": False,
            "fail_closed_without_retry_or_relaxation": True,
            "current_runtime_authority_derived_from_history": False,
        },
        "checks": {
            "misnamed_epoch_source_reclassified_as_unadmitted_session_token": True,
            "failed_activation_source_exact_file_and_canonical": True,
            "v6_wrong_route_benchmark_exact_file_and_canonical": True,
            "v7_attempt1_not_misrepresented_as_exact7": True,
            "v7_attempt2_benchmark_exact_file_and_canonical": True,
            "all_failed_resource_receipts_absent": True,
            "no_failed_attempt_reused_for_current": True,
        },
        "authority_design": dict(AUTHORITY_DESIGN),
        "permissions": dict(PERMISSIONS),
        "evidence_boundary": dict(EVIDENCE_BOUNDARY),
    }
    payload[CANONICAL_FIELD] = _document_sha256(payload, CANONICAL_FIELD)
    return payload


def validate_failed_activation_attempt_history(
    path: Path,
    *,
    failed_activation_source_path: Path,
    v6_wrong_route_benchmark_path: Path,
    v7_attempt2_benchmark_path: Path,
) -> dict[str, Any]:
    opened = _open_private_json(path, "failed activation attempt history")
    payload = dict(opened.payload)
    expected = build_failed_activation_attempt_history(
        failed_activation_source_path=failed_activation_source_path,
        v6_wrong_route_benchmark_path=v6_wrong_route_benchmark_path,
        v7_attempt2_benchmark_path=v7_attempt2_benchmark_path,
        generated_utc=_timestamp(payload.get("generated_utc"), "history generated timestamp"),
    )
    if payload != expected:
        raise FailedActivationHistoryError("failed activation attempt history drifted")
    return payload


def finalize_failed_activation_attempt_history(
    *,
    output_path: Path,
    **kwargs: Any,
) -> tuple[dict[str, Any], str]:
    payload = build_failed_activation_attempt_history(**kwargs)
    try:
        file_sha = release_io._write_exclusive(output_path, payload)  # noqa: SLF001
    except Exception as exc:
        raise FailedActivationHistoryError("history create-only write failed") from exc
    validator_kwargs = {key: value for key, value in kwargs.items() if key != "generated_utc"}
    observed = validate_failed_activation_attempt_history(output_path, **validator_kwargs)
    if observed != payload:
        raise FailedActivationHistoryError("history changed after write")
    return payload, file_sha


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("finalize", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--failed-activation-source", type=Path, required=True)
        command.add_argument("--v6-wrong-route-benchmark", type=Path, required=True)
        command.add_argument("--v7-attempt2-benchmark", type=Path, required=True)
        target = "--output" if name == "finalize" else "--receipt"
        command.add_argument(target, type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    inputs = {
        "failed_activation_source_path": args.failed_activation_source,
        "v6_wrong_route_benchmark_path": args.v6_wrong_route_benchmark,
        "v7_attempt2_benchmark_path": args.v7_attempt2_benchmark,
    }
    if args.command == "finalize":
        payload, file_sha = finalize_failed_activation_attempt_history(
            output_path=args.output,
            **inputs,
        )
    else:
        payload = validate_failed_activation_attempt_history(args.receipt, **inputs)
        file_sha = hashlib.sha256(args.receipt.read_bytes()).hexdigest()
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "status": payload["status"],
                "file_sha256": file_sha,
                "canonical_sha256": payload[CANONICAL_FIELD],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
