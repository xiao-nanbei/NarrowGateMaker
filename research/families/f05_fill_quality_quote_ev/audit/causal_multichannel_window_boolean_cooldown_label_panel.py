"""Admit strict-native cooldown-v2 snapshots and eight-arm labels.

The admission keeps the mechanics denominator separate from the economic
label denominator.  Unsupported features, right-censored arms, and strict
queue invalidations remain explicit rows; they are never silently removed to
manufacture a complete-case training panel.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    ARM_RESULT_SCHEMA_VERSION,
    MISSING_TRACE_FIELDS,
    OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    SNAPSHOT_SCHEMA_VERSION,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_strict_checkpoint import (
    BUY_ARMS,
    SELL_ARMS,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
PANEL_IDENTITY = f"{IDENTITY}.strict_native_label_panel.v3"
FORMAL_DAY_SCHEMA_VERSION = f"{IDENTITY}.strict_native_one_shot_labels.v1.day.v2"

class LabelPanelError(RuntimeError):
    """Raised when snapshots or arm labels cannot be admitted exactly."""


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise LabelPanelError(f"JSON root is not an object: {path}")
    return payload


def _merge_consistent(*mappings: Mapping[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for mapping in mappings:
        for key, value in mapping.items():
            if key in output:
                prior = output[key]
                both_nan = (
                    isinstance(prior, float)
                    and isinstance(value, float)
                    and math.isnan(prior)
                    and math.isnan(value)
                )
                if not both_nan and prior != value:
                    raise LabelPanelError(
                        f"snapshot/feature identity disagrees on {key!r}"
                    )
                continue
            output[key] = value
    return output


def _canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _same_scalar(left: Any, right: Any) -> bool:
    if left is None or right is None:
        other = right if left is None else left
        return other is None or (
            isinstance(other, float) and math.isnan(other)
        )
    if isinstance(left, float) and math.isnan(left):
        return isinstance(right, float) and math.isnan(right)
    if isinstance(right, float) and math.isnan(right):
        return False
    return bool(left == right)


def _require_scalar_match(label: str, left: Any, right: Any) -> None:
    if not _same_scalar(left, right):
        raise LabelPanelError(f"{label} drifted")


def _require_finite_close(label: str, left: Any, right: Any) -> None:
    try:
        left_value = float(left)
        right_value = float(right)
    except (TypeError, ValueError) as exc:
        raise LabelPanelError(f"{label} is not numeric") from exc
    if not (
        math.isfinite(left_value)
        and math.isfinite(right_value)
        and math.isclose(left_value, right_value, rel_tol=0.0, abs_tol=1e-12)
    ):
        raise LabelPanelError(f"{label} drifted")


_SNAPSHOT_SCALAR_FIELDS = (
    "snapshot_id",
    "assignment_id",
    "fill_event_id",
    "client_order_id",
    "lineage_id",
    "lineage_revision",
    "partial_fill_ordinal",
    "partial_fill_qty_btc",
    "visibility_profile",
    "receive_time_transport_eligible",
    "source_bundle_sha256",
    "feature_block",
    "policy_input_valid",
    "fallback_policy_id",
    "fallback_reason",
    "economic_outcomes_read",
)


def _decode_snapshot_row(
    raw_snapshot: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    snapshot = dict(raw_snapshot)
    required = {
        *_SNAPSHOT_SCALAR_FIELDS,
        "m0_context_json",
        "feature_row_json",
        "snapshot_payload_json",
        "snapshot_payload_sha256",
    }
    missing = sorted(required - set(snapshot))
    if missing:
        raise LabelPanelError(f"assignment snapshot schema is incomplete: {missing}")
    try:
        payload = json.loads(str(snapshot["snapshot_payload_json"]))
        m0_context = json.loads(str(snapshot["m0_context_json"]))
        feature_row = json.loads(str(snapshot["feature_row_json"]))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise LabelPanelError("assignment snapshot JSON is invalid") from exc
    if not all(
        isinstance(value, dict)
        for value in (payload, m0_context, feature_row)
    ):
        raise LabelPanelError("assignment snapshot JSON roots must be objects")
    payload_sha256 = str(snapshot["snapshot_payload_sha256"])
    if len(payload_sha256) != 64 or _canonical_sha256(payload) != payload_sha256:
        raise LabelPanelError("assignment snapshot payload SHA256 drifted")
    if payload.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
        raise LabelPanelError("assignment snapshot payload schema drifted")
    if payload.get("identity") != IDENTITY:
        raise LabelPanelError("assignment snapshot payload identity drifted")
    for field in _SNAPSHOT_SCALAR_FIELDS:
        _require_scalar_match(
            f"assignment snapshot payload field {field}",
            snapshot[field],
            payload.get(field),
        )
    if payload.get("m0_context") != m0_context:
        raise LabelPanelError("assignment snapshot M0 payload drifted")
    if payload.get("feature_row") != feature_row:
        raise LabelPanelError("assignment snapshot feature payload drifted")
    for name in ("clocks", "sources", "identity_hashes", "field_validity"):
        if not isinstance(payload.get(name), dict):
            raise LabelPanelError(f"assignment snapshot {name} payload is invalid")
    if payload.get("economic_outcomes_read") is not False:
        raise LabelPanelError("assignment snapshot read economic outcomes")
    _merge_consistent(m0_context, feature_row)
    mechanics = {
        key: value
        for key, value in snapshot.items()
        if key
        not in {
            "m0_context_json",
            "feature_row_json",
            "economic_outcomes_read",
        }
    }
    mechanics.update(
        {
            "snapshot_clocks_json": _canonical_json(payload["clocks"]),
            "snapshot_sources_json": _canonical_json(payload["sources"]),
            "snapshot_identity_hashes_json": _canonical_json(
                payload["identity_hashes"]
            ),
            "snapshot_field_validity_json": _canonical_json(
                payload["field_validity"]
            ),
        }
    )
    return mechanics, m0_context, feature_row, payload


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}")
    with temporary.open("x", encoding="ascii") as handle:
        handle.write(
            json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
        )
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _validate_canonical_result(payload: Mapping[str, Any]) -> None:
    expected = str(payload.get("canonical_result_sha256", ""))
    body = dict(payload)
    body.pop("canonical_result_sha256", None)
    if len(expected) != 64 or _canonical_sha256(body) != expected:
        raise LabelPanelError("arm canonical result SHA256 drifted")


def _arm_label(
    path: Path,
    *,
    expected_arm: str,
    opportunity_identity: str,
    opportunity: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _load_json(path)
    _validate_canonical_result(payload)
    if payload.get("schema_version") != ARM_RESULT_SCHEMA_VERSION:
        raise LabelPanelError("arm result schema drifted")
    if payload.get("identity") != IDENTITY:
        raise LabelPanelError("arm identity drifted")
    if payload.get("arm_id") != expected_arm:
        raise LabelPanelError("arm ordering or identity drifted")
    if payload.get("opportunity_identity_sha256") != opportunity_identity:
        raise LabelPanelError("arm opportunity identity drifted")
    trace = payload.get("fork_trace")
    execution = payload.get("strict_execution_contract")
    prefix_execution = payload.get("prefix_execution_contract")
    if (
        not isinstance(trace, dict)
        or not isinstance(execution, dict)
        or not isinstance(prefix_execution, dict)
    ):
        raise LabelPanelError("arm trace/execution contract is invalid")
    try:
        assignment_count = int(
            opportunity["exchange_book_queue_missing_count_at_assignment"]
        )
        assignment_cursor = int(
            opportunity["exchange_book_queue_missing_trace_cursor"]
        )
        prefix_count = int(prefix_execution["exchange_book_queue_missing_count"])
        prefix_assignment_count = int(
            prefix_execution["exchange_book_queue_missing_count_at_assignment"]
        )
        prefix_cursor = int(
            prefix_execution["exchange_book_queue_missing_trace_cursor"]
        )
        treatment_count = int(execution["exchange_book_queue_missing_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise LabelPanelError("queue-missing assignment cursor/count contract is invalid") from exc
    if min(
        assignment_count,
        assignment_cursor,
        prefix_count,
        prefix_assignment_count,
        prefix_cursor,
        treatment_count,
    ) < 0:
        raise LabelPanelError("queue-missing assignment cursor/count is negative")
    if not (
        assignment_count
        == assignment_cursor
        == prefix_count
        == prefix_assignment_count
        == prefix_cursor
    ):
        raise LabelPanelError("queue-missing assignment cursor/count drifted")
    missing_trace = execution.get("exchange_book_queue_missing_trace")
    if not isinstance(missing_trace, list) or len(missing_trace) != treatment_count:
        raise LabelPanelError("treatment queue-missing trace count drifted")
    observed_missing_keys: set[tuple[int, int]] = set()
    for missing_row in missing_trace:
        if not isinstance(missing_row, Mapping) or set(missing_row) != MISSING_TRACE_FIELDS:
            raise LabelPanelError("treatment queue-missing trace schema drifted")
        try:
            missing_key = (
                int(missing_row["order_id"]),
                int(missing_row["activate_ts_ms"]),
            )
        except (TypeError, ValueError) as exc:
            raise LabelPanelError("treatment queue-missing trace key drifted") from exc
        if missing_key in observed_missing_keys:
            raise LabelPanelError("treatment queue-missing trace contains duplicates")
        observed_missing_keys.add(missing_key)
    required_trace = {
        "schema_version",
        "action",
        "side",
        "campaign_id",
        "assignment_ts_ms",
        "baseline_duration_ms",
        "applied_duration_ms",
        "arm_washout_complete",
        "terminal_ts_ms",
        "terminal_reason",
        "right_censored",
        "assignment_to_washout_value_usdc",
        "censor_time_mid_mark_usdc",
        "censor_time_executable_mark_usdc",
        "censor_marks_are_terminal_bounds",
        "accounting_residual_usdc",
        "second_assignment_count",
        "reducing_quote_change_count",
        "inventory_time_btc_s",
        "mae_usdc",
        "max_abs_inventory_btc",
    }
    missing = sorted(required_trace - set(trace))
    if missing:
        raise LabelPanelError(f"arm fork trace is incomplete: {missing}")
    reasons: list[str] = []
    if execution.get("strict_native_label_eligible") is not True:
        reasons.extend(
            f"strict:{value}"
            for value in execution.get(
                "strict_native_label_unsupported_reasons", []
            )
        )
    queue_reason = "strict:exchange_book_queue_missing_count"
    point_label_status = execution.get("economic_point_label_status")
    expected_point_label_status = (
        "eligible"
        if execution.get("strict_native_label_eligible") is True
        else "unsupported_redacted"
    )
    if point_label_status != expected_point_label_status:
        raise LabelPanelError("arm economic point-label status drifted")
    if treatment_count > 0:
        if execution.get("strict_native_label_eligible") is True:
            raise LabelPanelError("queue-missing arm incorrectly claims strict eligibility")
        if queue_reason not in reasons:
            raise LabelPanelError("queue-missing arm lacks its unsupported reason")
    elif queue_reason in reasons:
        raise LabelPanelError("zero-missing arm carries a queue-missing reason")
    right_censored = bool(trace["right_censored"])
    washout_complete = bool(trace["arm_washout_complete"])
    if right_censored:
        reasons.append("right_censored")
    if not washout_complete:
        reasons.append("washout_incomplete")
    if int(trace["second_assignment_count"]) != 0:
        reasons.append("second_assignment")
    if int(trace["reducing_quote_change_count"]) != 0:
        reasons.append("reducing_quote_changed")
    if trace["censor_marks_are_terminal_bounds"] is not False:
        reasons.append("censor_marks_misrepresented_as_terminal_bounds")
    residual = float(trace["accounting_residual_usdc"])
    if not math.isfinite(residual) or abs(residual) > 1e-6:
        reasons.append("accounting_residual")
    value = trace["assignment_to_washout_value_usdc"]
    if point_label_status == "unsupported_redacted" and value is not None:
        raise LabelPanelError("unsupported arm retained a raw economic point label")
    if value is not None:
        value = float(value)
        if not math.isfinite(value):
            reasons.append("nonfinite_terminal_value")
    if not right_censored and value is None:
        reasons.append("complete_arm_missing_terminal_value")
    eligible = not reasons
    row = {
        "opportunity_id": opportunity_identity,
        "duration_policy_id": expected_arm,
        "terminal_value_usdc": value if eligible else None,
        "strict_native_label": bool(eligible),
        "economic_point_label_status": point_label_status,
        "label_unsupported_reasons_json": json.dumps(
            reasons,
            separators=(",", ":"),
        ),
        "right_censored": right_censored,
        "washout_complete": washout_complete,
        "terminal_ts_ms": int(trace["terminal_ts_ms"]),
        "terminal_reason": str(trace["terminal_reason"]),
        "baseline_duration_ms": float(trace["baseline_duration_ms"]),
        "applied_duration_ms": float(trace["applied_duration_ms"]),
        "censor_time_mid_mark_usdc": trace["censor_time_mid_mark_usdc"],
        "censor_time_executable_mark_usdc": trace[
            "censor_time_executable_mark_usdc"
        ],
        "accounting_residual_usdc": residual,
        "inventory_time_btc_s": float(trace["inventory_time_btc_s"]),
        "mae_usdc": float(trace["mae_usdc"]),
        "max_abs_inventory_btc": float(trace["max_abs_inventory_btc"]),
        "exchange_book_queue_missing_count_at_assignment": assignment_count,
        "exchange_book_queue_missing_trace_cursor": assignment_cursor,
        "treatment_exchange_book_queue_missing_count": treatment_count,
        "treatment_exchange_book_queue_missing_trace_json": _canonical_json(
            missing_trace
        ),
        "arm_result_path": str(path),
        "arm_result_sha256": _sha256(path),
    }
    return row, trace


def _opportunity_rows(
    manifest_path: Path,
    *,
    snapshots: pd.DataFrame,
    target_day: str,
    panel_role: str,
    expected_feature_block: str,
    expected_source_contract_sha256: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = _load_json(manifest_path)
    if manifest.get("schema_version") != OPPORTUNITY_MANIFEST_SCHEMA_VERSION:
        raise LabelPanelError("opportunity manifest schema drifted")
    if manifest.get("identity") != IDENTITY:
        raise LabelPanelError("opportunity identity drifted")
    opportunity_id = str(manifest.get("opportunity_identity_sha256", ""))
    contract = manifest.get("opportunity_contract")
    if len(opportunity_id) != 64 or not isinstance(contract, dict):
        raise LabelPanelError("opportunity manifest lacks a canonical identity")
    if _canonical_sha256(contract) != opportunity_id:
        raise LabelPanelError("opportunity contract canonical identity drifted")
    if contract.get("identity") != IDENTITY:
        raise LabelPanelError("opportunity contract identity drifted")
    if str(contract.get("target_day", "")) != target_day:
        raise LabelPanelError("opportunity contract target day drifted")
    if (
        str(contract.get("source_contract_sha256", ""))
        != expected_source_contract_sha256
    ):
        raise LabelPanelError("opportunity source contract identity drifted")
    opportunity = contract.get("opportunity")
    if not isinstance(opportunity, dict):
        raise LabelPanelError("opportunity contract is incomplete")
    snapshot_id = str(opportunity.get("cooldown_v2_snapshot_id", ""))
    matches = snapshots.loc[snapshots["snapshot_id"] == snapshot_id]
    if len(matches) != 1:
        raise LabelPanelError("opportunity does not join to exactly one snapshot")
    mechanics, m0_context, feature_row, snapshot_payload = _decode_snapshot_row(
        matches.iloc[0].to_dict()
    )
    if str(snapshot_payload["feature_block"]) != expected_feature_block:
        raise LabelPanelError("snapshot feature block drifted from day manifest")
    if contract.get("execution_identity_hashes") != snapshot_payload[
        "identity_hashes"
    ]:
        raise LabelPanelError("opportunity/snapshot execution identity drifted")
    side = str(opportunity["side"]).upper()
    role_at_fill = str(opportunity["role_at_fill"]).lower()
    _require_scalar_match("opportunity/snapshot side", side, str(m0_context["side"]).upper())
    _require_scalar_match(
        "opportunity/snapshot role",
        role_at_fill,
        str(m0_context["role_at_fill"]).lower(),
    )
    _require_scalar_match(
        "opportunity/snapshot source bundle",
        opportunity.get("cooldown_v2_source_bundle_sha256"),
        snapshot_payload["source_bundle_sha256"],
    )
    _require_scalar_match(
        "opportunity/snapshot partial-fill ordinal",
        int(opportunity.get("partial_fill_ordinal", -1)),
        int(snapshot_payload["partial_fill_ordinal"]),
    )
    _require_finite_close(
        "opportunity/snapshot fill quantity",
        opportunity.get("fill_qty_btc"),
        snapshot_payload["partial_fill_qty_btc"],
    )
    _require_finite_close(
        "opportunity/snapshot M0 fill quantity",
        snapshot_payload["partial_fill_qty_btc"],
        m0_context["fill_qty_btc"],
    )
    _require_scalar_match(
        "snapshot/M0 partial-fill ordinal",
        int(snapshot_payload["partial_fill_ordinal"]),
        int(m0_context["partial_fill_ordinal"]),
    )
    _require_finite_close(
        "opportunity/M0 baseline duration",
        opportunity["baseline_duration_ms"],
        m0_context["baseline_duration_ms"],
    )
    fill_visible_ns = int(opportunity["fill_visible_ts_ms"]) * 1_000_000
    fill_exchange_ns = int(opportunity["fill_exchange_ts_ms"]) * 1_000_000
    _require_scalar_match(
        "opportunity/M0 fill-visible clock",
        fill_visible_ns,
        int(m0_context["fill_visible_ts_ns"]),
    )
    _require_scalar_match(
        "opportunity/M0 assignment clock",
        fill_visible_ns,
        int(m0_context["assignment_ts_ns"]),
    )
    clocks = snapshot_payload["clocks"]
    for name, expected_ts_ns in (
        ("assignment", fill_visible_ns),
        ("fill_visible", fill_visible_ns),
        ("fill_exchange", fill_exchange_ns),
    ):
        clock = clocks.get(name)
        if not isinstance(clock, dict):
            raise LabelPanelError(f"snapshot {name} clock is invalid")
        _require_scalar_match(
            f"opportunity/snapshot {name} clock",
            int(expected_ts_ns),
            clock.get("ts_ns"),
        )
    _require_scalar_match(
        "opportunity/snapshot client order",
        f"replay-order-{opportunity['order_id']}",
        snapshot_payload["client_order_id"],
    )
    arms = BUY_ARMS if side == "BUY" else SELL_ARMS if side == "SELL" else ()
    manifest_arms = manifest.get("arms")
    if not arms or not isinstance(manifest_arms, list):
        raise LabelPanelError("opportunity side/arm vocabulary is invalid")
    if tuple(str(row.get("arm_id")) for row in manifest_arms) != tuple(arms):
        raise LabelPanelError("opportunity does not contain exact ordered arms")
    label_rows: list[dict[str, Any]] = []
    traces: list[dict[str, Any]] = []
    for row, expected_arm in zip(manifest_arms, arms, strict=True):
        arm_path = manifest_path.parent / str(row["path"])
        if _sha256(arm_path) != str(row["sha256"]):
            raise LabelPanelError("opportunity arm file hash drifted")
        label, trace = _arm_label(
            arm_path,
            expected_arm=expected_arm,
            opportunity_identity=opportunity_id,
            opportunity=opportunity,
        )
        label_rows.append(label)
        traces.append(trace)
    for trace in traces:
        _require_scalar_match(
            "opportunity/arm side",
            side,
            str(trace["side"]).upper(),
        )
        _require_scalar_match(
            "opportunity/arm assignment clock",
            int(opportunity["fill_visible_ts_ms"]),
            int(trace["assignment_ts_ms"]),
        )
        _require_finite_close(
            "opportunity/arm baseline duration",
            opportunity["baseline_duration_ms"],
            trace["baseline_duration_ms"],
        )
    campaigns = {str(trace["campaign_id"]) for trace in traces}
    if len(campaigns) != 1:
        raise LabelPanelError("duration arms disagree on campaign identity")
    context_campaign = str(opportunity["campaign_id"])
    if campaigns != {context_campaign}:
        raise LabelPanelError("arm campaign identity drifted from opportunity")
    joint_strict = all(row["strict_native_label"] for row in label_rows)
    unsupported_arm_ids = [
        str(row["duration_policy_id"])
        for row in label_rows
        if not bool(row["strict_native_label"])
    ]
    joined_state = _merge_consistent(mechanics, m0_context, feature_row)
    opportunity_identity = {
        "opportunity_id": opportunity_id,
        "utc_day": target_day,
        "panel_role": panel_role,
        "side": side,
        "role_at_fill": role_at_fill,
        "campaign_id": f"{target_day}:{side}:{context_campaign}",
        "snapshot_id": snapshot_id,
        "labels_generated": True,
        "joint_strict_native_label": joint_strict,
        "strict_arm_count": len(label_rows) - len(unsupported_arm_ids),
        "unsupported_arm_count": len(unsupported_arm_ids),
        "unsupported_arm_ids_json": _canonical_json(unsupported_arm_ids),
        "joint_right_censored": any(row["right_censored"] for row in label_rows),
        "joint_washout_complete": all(
            row["washout_complete"] for row in label_rows
        ),
        "opportunity_manifest_path": str(manifest_path),
        "opportunity_manifest_sha256": _sha256(manifest_path),
    }
    opportunity_row = _merge_consistent(joined_state, opportunity_identity)
    for row in label_rows:
        row.update(
            {
                "utc_day": target_day,
                "panel_role": panel_role,
                "side": side,
                "role_at_fill": opportunity_row["role_at_fill"],
                "campaign_id": opportunity_row["campaign_id"],
            }
        )
    return opportunity_row, label_rows


def assemble_day_label_panel(
    day_manifest_path: Path,
    *,
    destination: Path,
) -> dict[str, Any]:
    """Atomically admit one day without hiding unsupported opportunities."""

    day_manifest_path = Path(day_manifest_path).expanduser().resolve()
    day_manifest = _load_json(day_manifest_path)
    if day_manifest.get("schema_version") != FORMAL_DAY_SCHEMA_VERSION:
        raise LabelPanelError("formal day manifest schema drifted")
    strict_queue = day_manifest.get("strict_native_queue")
    if (
        not isinstance(strict_queue, Mapping)
        or strict_queue.get("missing_trace_unbounded") is not True
    ):
        raise LabelPanelError("formal day queue-missing trace is not unbounded")
    target_day = str(day_manifest.get("target_day", ""))
    feature_block = str(day_manifest.get("feature_block", ""))
    panel_role_raw = str(day_manifest.get("panel_role", ""))
    panel_role = {
        "prefix40_development": "prefix40",
        "added10_late_diagnostic": "added10",
    }.get(panel_role_raw)
    if panel_role is None:
        raise LabelPanelError("day manifest panel role drifted")
    source_contract = day_manifest.get("source_contract")
    if not isinstance(source_contract, dict):
        raise LabelPanelError("day manifest source contract is missing")
    expected_source_contract_sha256 = str(
        source_contract.get("canonical_identity_sha256", "")
    )
    if len(expected_source_contract_sha256) != 64:
        raise LabelPanelError("day manifest source contract identity is invalid")
    snapshots_path = Path(
        str(day_manifest["assignment_snapshots"]["path"])
    )
    if _sha256(snapshots_path) != str(
        day_manifest["assignment_snapshots"]["sha256"]
    ):
        raise LabelPanelError("assignment snapshot hash drifted")
    snapshots = pd.read_parquet(snapshots_path)
    if snapshots["snapshot_id"].duplicated().any():
        raise LabelPanelError("assignment snapshot IDs are not unique")
    manifest_rows = day_manifest.get("one_shot_label_manifests")
    if not isinstance(manifest_rows, list) or not manifest_rows:
        raise LabelPanelError("day manifest has no one-shot labels")

    opportunity_rows: list[dict[str, Any]] = []
    label_rows: list[dict[str, Any]] = []
    labeled_snapshot_ids: set[str] = set()
    for manifest_row in manifest_rows:
        manifest_path = Path(str(manifest_row["path"]))
        if _sha256(manifest_path) != str(manifest_row["sha256"]):
            raise LabelPanelError("bound opportunity manifest hash drifted")
        opportunity, labels = _opportunity_rows(
            manifest_path,
            snapshots=snapshots,
            target_day=target_day,
            panel_role=panel_role,
            expected_feature_block=feature_block,
            expected_source_contract_sha256=expected_source_contract_sha256,
        )
        opportunity_rows.append(opportunity)
        label_rows.extend(labels)
        labeled_snapshot_ids.add(str(opportunity["snapshot_id"]))

    for _, raw_snapshot in snapshots.iterrows():
        if str(raw_snapshot["snapshot_id"]) in labeled_snapshot_ids:
            continue
        mechanics, m0_context, feature_row, snapshot_payload = (
            _decode_snapshot_row(raw_snapshot.to_dict())
        )
        if str(snapshot_payload["feature_block"]) != feature_block:
            raise LabelPanelError("unlabeled snapshot feature block drifted")
        joined_state = _merge_consistent(mechanics, m0_context, feature_row)
        opportunity_identity = {
            "opportunity_id": None,
            "utc_day": target_day,
            "panel_role": panel_role,
            "side": str(m0_context["side"]).upper(),
            "role_at_fill": str(m0_context["role_at_fill"]).lower(),
            "campaign_id": None,
            "snapshot_id": str(snapshot_payload["snapshot_id"]),
            "labels_generated": False,
            "joint_strict_native_label": False,
            "strict_arm_count": 0,
            "unsupported_arm_count": 0,
            "unsupported_arm_ids_json": "[]",
            "joint_right_censored": None,
            "joint_washout_complete": None,
            "opportunity_manifest_path": None,
            "opportunity_manifest_sha256": None,
        }
        opportunity_rows.append(
            _merge_consistent(joined_state, opportunity_identity)
        )

    opportunity_frame = pd.DataFrame(opportunity_rows)
    label_frame = pd.DataFrame(label_rows)
    if opportunity_frame["snapshot_id"].duplicated().any():
        raise LabelPanelError("admitted opportunity snapshot IDs are duplicated")
    if label_frame.duplicated(["opportunity_id", "duration_policy_id"]).any():
        raise LabelPanelError("admitted opportunity/arm labels are duplicated")
    generated = opportunity_frame.loc[opportunity_frame["labels_generated"]]
    if not generated.empty and not generated["strict_arm_count"].add(
        generated["unsupported_arm_count"]
    ).eq(8).all():
        raise LabelPanelError("generated opportunities do not retain all eight arms")

    destination = Path(destination).expanduser().resolve()
    if destination.exists():
        raise LabelPanelError(f"label panel destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / (
        f".{destination.name}.staging.{os.getpid()}.{uuid.uuid4().hex}"
    )
    staging.mkdir()
    opportunities_path = staging / "opportunities.parquet"
    labels_path = staging / "labels.parquet"
    opportunity_frame.to_parquet(opportunities_path, index=False, compression="zstd")
    label_frame.to_parquet(labels_path, index=False, compression="zstd")
    for path in (opportunities_path, labels_path):
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    manifest = {
        "schema_version": PANEL_IDENTITY,
        "identity": IDENTITY,
        "target_day": target_day,
        "feature_block": feature_block,
        "panel_role": panel_role,
        "day_manifest": {
            "path": str(day_manifest_path),
            "sha256": _sha256(day_manifest_path),
        },
        "opportunities": {
            "path": str(destination / opportunities_path.name),
            "rows": int(len(opportunity_frame)),
            "labels_generated_rows": int(
                opportunity_frame["labels_generated"].sum()
            ),
            "joint_strict_rows": int(
                opportunity_frame["joint_strict_native_label"].sum()
            ),
            "unsupported_opportunity_rows": int(
                (
                    opportunity_frame["labels_generated"]
                    & ~opportunity_frame["joint_strict_native_label"]
                ).sum()
            ),
            "sha256": _sha256(opportunities_path),
        },
        "labels": {
            "path": str(destination / labels_path.name),
            "rows": int(len(label_frame)),
            "strict_rows": int(label_frame["strict_native_label"].sum()),
            "unsupported_rows": int((~label_frame["strict_native_label"]).sum()),
            "right_censored_rows": int(label_frame["right_censored"].sum()),
            "sha256": _sha256(labels_path),
        },
        "complete_case_filter_applied": False,
        "economic_outcomes_read": True,
        "nested_oof_run": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(staging / "manifest.json", manifest)
    _atomic_json(
        staging / "_SUCCESS",
        {"manifest_sha256": _sha256(staging / "manifest.json")},
    )
    os.replace(staging, destination)
    _fsync_directory(destination.parent)
    return manifest


__all__ = [
    "IDENTITY",
    "LabelPanelError",
    "PANEL_IDENTITY",
    "assemble_day_label_panel",
]
