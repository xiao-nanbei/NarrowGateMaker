"""Versioned baseline epochs for live/replay lifecycle estimation.

The manifest separates calendar segmentation from strategy identity.  UTC
midnight is an accounting boundary only; deployments, action permission
changes, clock/data semantics changes, and unrecoverable runtime restarts are
epoch boundaries.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "narrowgate_baseline_epoch_manifest.v1"

REQUIRED_IDENTITY_FIELDS = (
    "runtime_code_sha256",
    "config_sha256",
    "model_bundle_sha256",
    "p3_sha256",
    "feature_dag_sha256",
    "execution_abi_sha256",
    "action_enablement_sha256",
    "initial_runtime_state_sha256",
    "data_source_identity_sha256",
    "clock_semantics_sha256",
)

BOUNDARY_REASONS = frozenset(
    {
        "scope_start",
        "identity_evidence_begins",
        "code_deployment",
        "config_deployment",
        "model_artifact_change",
        "p3_artifact_change",
        "feature_dag_change",
        "execution_abi_change",
        "action_enablement_change",
        "data_source_change",
        "clock_semantics_change",
        "unrestored_process_restart",
        "state_restored_process_restart",
        "hot_reload_identity_change",
    }
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_ECONOMIC_KEY_PARTS = ("pnl", "reward", "markout")


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc_timestamp_ns(value: str) -> int:
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp must include UTC offset: {value!r}")
    return int(parsed.astimezone(UTC).timestamp() * 1_000_000_000)


def _walk_keys(value: Any) -> Sequence[str]:
    keys: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            keys.append(str(key).lower())
            keys.extend(_walk_keys(item))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        for item in value:
            keys.extend(_walk_keys(item))
    return keys


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and bool(_SHA256_RE.fullmatch(value))


def epoch_identity_sha256(identity: Mapping[str, Any]) -> str:
    required = {name: identity.get(name) for name in REQUIRED_IDENTITY_FIELDS}
    return canonical_sha256(required)


def _validate_interval(start: Any, end: Any, *, label: str) -> tuple[int, int]:
    if not isinstance(start, int) or not isinstance(end, int):
        raise ValueError(f"{label} timestamps must be integer nanoseconds")
    if start >= end:
        raise ValueError(f"{label} must have positive duration")
    return start, end


def _validate_epoch(epoch: Mapping[str, Any]) -> tuple[int, int]:
    epoch_id = epoch.get("epoch_id")
    if not isinstance(epoch_id, str) or not epoch_id.strip():
        raise ValueError("epoch_id must be non-empty")
    start, end = _validate_interval(
        epoch.get("start_ts_ns"),
        epoch.get("end_ts_ns"),
        label=f"epoch {epoch_id}",
    )
    reason = epoch.get("start_reason")
    if reason not in BOUNDARY_REASONS:
        raise ValueError(f"unsupported epoch start_reason: {reason!r}")
    boundary_status = epoch.get("boundary_status")
    if boundary_status not in {
        "first_decision_bound",
        "identity_effective_time_only",
        "inferred_boundary",
    }:
        raise ValueError(f"unsupported epoch boundary_status: {boundary_status!r}")
    identity = epoch.get("identity")
    if not isinstance(identity, Mapping):
        raise ValueError(f"epoch {epoch_id} identity must be an object")
    missing = [name for name in REQUIRED_IDENTITY_FIELDS if name not in identity]
    if missing:
        raise ValueError(f"epoch {epoch_id} identity fields missing: {missing}")
    invalid = [
        name
        for name in REQUIRED_IDENTITY_FIELDS
        if identity[name] is not None and not _is_sha256(identity[name])
    ]
    if invalid:
        raise ValueError(f"epoch {epoch_id} has invalid identity hashes: {invalid}")
    fully_bound = all(_is_sha256(identity[name]) for name in REQUIRED_IDENTITY_FIELDS)
    expected_status = "fully_bound" if fully_bound else "partially_bound"
    if epoch.get("binding_status") != expected_status:
        raise ValueError(
            f"epoch {epoch_id} binding_status must be {expected_status!r}"
        )
    expected_identity_sha = epoch_identity_sha256(identity)
    if epoch.get("identity_sha256") != expected_identity_sha:
        raise ValueError(f"epoch {epoch_id} identity_sha256 mismatch")
    if bool(epoch.get("lifecycle_estimation_authorized")) and not (
        fully_bound and boundary_status == "first_decision_bound"
    ):
        raise ValueError(
            f"epoch {epoch_id} cannot authorize lifecycle estimation without "
            "a fully bound identity and first-decision boundary"
        )
    if bool(epoch.get("continuous_economic_estimation_authorized")) and not (
        fully_bound and bool(epoch.get("initial_economic_state_complete"))
    ):
        raise ValueError(
            f"epoch {epoch_id} lacks complete initial economic state"
        )
    if bool(epoch.get("pooling_authorized")):
        raise ValueError("v1 forbids pre-estimation pooling across baseline epochs")
    return start, end


def validate_baseline_epoch_manifest(manifest: Mapping[str, Any]) -> None:
    """Fail closed on incomplete, overlapping, or economically contaminated epochs."""
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported baseline epoch manifest schema")
    if manifest.get("source_clock") != "utc_ns":
        raise ValueError("baseline epoch manifest source_clock must be utc_ns")
    if manifest.get("utc_midnight_splits_epoch") is not False:
        raise ValueError("UTC midnight must not split a baseline epoch")
    if manifest.get("pooled_estimation_authorized") is not False:
        raise ValueError("v1 requires epoch-specific estimation before pooling")
    if tuple(manifest.get("required_identity_fields", ())) != REQUIRED_IDENTITY_FIELDS:
        raise ValueError("required baseline epoch identity fields changed")
    forbidden = [
        key
        for key in _walk_keys(manifest)
        if any(part in key for part in _FORBIDDEN_ECONOMIC_KEY_PARTS)
    ]
    if forbidden:
        raise ValueError(f"baseline epoch manifest contains economic keys: {forbidden}")

    scope_start, scope_end = _validate_interval(
        manifest.get("scope_start_ts_ns"),
        manifest.get("scope_end_ts_ns"),
        label="manifest scope",
    )
    epochs = list(manifest.get("epochs", ()))
    if not epochs:
        raise ValueError("baseline epoch manifest must contain at least one epoch")
    intervals: list[tuple[int, int, str]] = []
    seen_ids: set[str] = set()
    for epoch in epochs:
        if not isinstance(epoch, Mapping):
            raise ValueError("each epoch must be an object")
        start, end = _validate_epoch(epoch)
        epoch_id = str(epoch["epoch_id"])
        if epoch_id in seen_ids:
            raise ValueError(f"duplicate epoch_id: {epoch_id}")
        seen_ids.add(epoch_id)
        if start < scope_start or end > scope_end:
            raise ValueError(f"epoch outside manifest scope: {epoch_id}")
        intervals.append((start, end, epoch_id))
    intervals.sort()
    for left, right in zip(intervals, intervals[1:], strict=False):
        if left[1] > right[0]:
            raise ValueError(f"overlapping epochs: {left[2]} and {right[2]}")

    unbound: list[tuple[int, int, str]] = []
    for row in manifest.get("unbound_intervals", ()):
        if not isinstance(row, Mapping):
            raise ValueError("each unbound interval must be an object")
        start, end = _validate_interval(
            row.get("start_ts_ns"), row.get("end_ts_ns"), label="unbound interval"
        )
        reason = row.get("reason")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("unbound interval reason must be non-empty")
        if start < scope_start or end > scope_end:
            raise ValueError("unbound interval outside manifest scope")
        unbound.append((start, end, reason))

    coverage = sorted(
        [(start, end, f"epoch:{epoch_id}") for start, end, epoch_id in intervals]
        + [(start, end, f"unbound:{reason}") for start, end, reason in unbound]
    )
    cursor = scope_start
    for start, end, label in coverage:
        if start != cursor:
            relation = "overlap" if start < cursor else "gap"
            raise ValueError(f"manifest coverage {relation} before {label}")
        cursor = end
    if cursor != scope_end:
        raise ValueError("manifest coverage does not reach scope_end_ts_ns")

    supplied_sha = manifest.get("canonical_manifest_sha256")
    normalized = dict(manifest)
    normalized.pop("canonical_manifest_sha256", None)
    if supplied_sha != canonical_sha256(normalized):
        raise ValueError("canonical_manifest_sha256 mismatch")


def finalize_manifest(payload: Mapping[str, Any]) -> dict[str, Any]:
    manifest = dict(payload)
    manifest.pop("canonical_manifest_sha256", None)
    manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)
    validate_baseline_epoch_manifest(manifest)
    return manifest


def load_and_validate_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    validate_baseline_epoch_manifest(manifest)
    return manifest


def _first_sha256(container: Mapping[str, Any], names: Sequence[str]) -> str | None:
    for name in names:
        value = container.get(name)
        if _is_sha256(value):
            return str(value)
    return None


def baseline_identity_effective_ts_ns(payload: Mapping[str, Any]) -> int:
    raw = payload.get("effective_at_utc") or payload.get("deployed_at_utc")
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("operational baseline identity lacks an effective timestamp")
    return utc_timestamp_ns(raw)


def extract_epoch_identity(
    payload: Mapping[str, Any],
    *,
    overrides: Mapping[str, str] | None = None,
) -> tuple[dict[str, str | None], dict[str, Any]]:
    """Extract only evidence actually bound by an operational identity.

    Missing fields remain null.  A hash of a vague label such as ``unknown``
    would create false authority, so callers must provide an explicit override
    artifact hash if a historical identity omitted a required component.
    """

    runtime_code = payload.get("runtime_code")
    config = payload.get("config")
    model = payload.get("model")
    p3 = payload.get("p3")
    runtime_profile = payload.get("runtime_profile")
    data_identity = payload.get("data_identity")
    action_config = {
        key: value
        for key, value in (config.items() if isinstance(config, Mapping) else ())
        if key != "ml_enabled"
        and (
            "action" in key
            or "shadow" in key
            or "selection" in key
            or key.endswith("_policy_enabled")
        )
    }
    auxiliary = payload.get("active_auxiliary_actions")
    if isinstance(auxiliary, Mapping):
        action_config["active_auxiliary_actions"] = auxiliary

    identity: dict[str, str | None] = {
        "runtime_code_sha256": (
            canonical_sha256(runtime_code) if isinstance(runtime_code, Mapping) else None
        ),
        "config_sha256": (
            str(config.get("sha256"))
            if isinstance(config, Mapping) and _is_sha256(config.get("sha256"))
            else None
        ),
        "model_bundle_sha256": (
            _first_sha256(
                model,
                (
                    "bundle_meta_sha256",
                    "bundle_tree_sha256",
                    "experiment_manifest_sha256",
                ),
            )
            if isinstance(model, Mapping)
            else None
        ),
        "p3_sha256": (
            str(p3.get("sha256"))
            if isinstance(p3, Mapping) and _is_sha256(p3.get("sha256"))
            else None
        ),
        "feature_dag_sha256": (
            str(model.get("feature_dag_sha256"))
            if isinstance(model, Mapping) and _is_sha256(model.get("feature_dag_sha256"))
            else None
        ),
        "execution_abi_sha256": (
            canonical_sha256(runtime_profile)
            if isinstance(runtime_profile, Mapping)
            else None
        ),
        "action_enablement_sha256": (
            canonical_sha256(action_config) if action_config else None
        ),
        "initial_runtime_state_sha256": None,
        "data_source_identity_sha256": (
            canonical_sha256(data_identity)
            if isinstance(data_identity, Mapping)
            else None
        ),
        "clock_semantics_sha256": None,
    }
    for key, value in (overrides or {}).items():
        if key not in REQUIRED_IDENTITY_FIELDS:
            raise ValueError(f"unsupported baseline identity override: {key}")
        if not _is_sha256(value):
            raise ValueError(f"baseline identity override is not SHA256: {key}")
        identity[key] = str(value)

    evidence = {
        "baseline_id": str(payload.get("baseline_id", "")),
        "baseline_schema_version": str(payload.get("schema_version", "")),
        "effective_ts_ns": baseline_identity_effective_ts_ns(payload),
        "bound_fields": [name for name, value in identity.items() if value is not None],
        "missing_fields": [name for name, value in identity.items() if value is None],
    }
    return identity, evidence


def _boundary_reason(
    previous: Mapping[str, str | None] | None,
    current: Mapping[str, str | None],
    *,
    deployment: Mapping[str, Any] | None,
) -> str:
    if previous is None:
        return "scope_start"
    if bool((deployment or {}).get("process_restarted")):
        return "unrestored_process_restart"
    ordered = (
        ("runtime_code_sha256", "code_deployment"),
        ("config_sha256", "config_deployment"),
        ("model_bundle_sha256", "model_artifact_change"),
        ("p3_sha256", "p3_artifact_change"),
        ("feature_dag_sha256", "feature_dag_change"),
        ("execution_abi_sha256", "execution_abi_change"),
        ("action_enablement_sha256", "action_enablement_change"),
        ("data_source_identity_sha256", "data_source_change"),
        ("clock_semantics_sha256", "clock_semantics_change"),
    )
    for field, reason in ordered:
        if previous.get(field) != current.get(field):
            return reason
    return "hot_reload_identity_change"


def build_manifest_from_baseline_identities(
    identity_paths: Sequence[Path],
    *,
    manifest_id: str,
    scope_start_ts_ns: int,
    scope_end_ts_ns: int,
    overrides_by_baseline_id: Mapping[str, Mapping[str, str]] | None = None,
    first_decision_ts_by_baseline_id: Mapping[str, int] | None = None,
    boundary_events: Sequence[Mapping[str, Any]] = (),
    restart_audit_complete: bool = False,
) -> dict[str, Any]:
    """Build a fail-closed epoch chain from frozen operational identities."""

    loaded: list[tuple[int, Path, dict[str, Any], dict[str, str | None], dict[str, Any]]] = []
    for raw_path in identity_paths:
        path = raw_path.expanduser().resolve()
        payload = json.loads(path.read_text(encoding="utf-8"))
        baseline_id = str(payload.get("baseline_id", ""))
        identity, evidence = extract_epoch_identity(
            payload,
            overrides=(overrides_by_baseline_id or {}).get(baseline_id),
        )
        evidence["identity_path"] = str(path)
        evidence["identity_file_sha256"] = file_sha256(path)
        effective = baseline_identity_effective_ts_ns(payload)
        first_decision = (first_decision_ts_by_baseline_id or {}).get(baseline_id)
        if first_decision is not None:
            if not isinstance(first_decision, int) or first_decision < effective:
                raise ValueError(
                    f"invalid first-decision timestamp for baseline {baseline_id}"
                )
            evidence["identity_recorded_or_effective_ts_ns"] = effective
            evidence["first_decision_ts_ns"] = first_decision
            evidence["boundary_precision"] = "first_decision_bound"
            effective = first_decision
        else:
            evidence["boundary_precision"] = "identity_effective_time_only"
        loaded.append((effective, path, payload, identity, evidence))
    loaded.sort(key=lambda item: item[0])
    selected_raw = [item for item in loaded if item[0] < scope_end_ts_ns]
    selected: list[
        tuple[int, Path, dict[str, Any], dict[str, str | None], dict[str, Any]]
    ] = []
    immutable_runtime_fields = (
        "runtime_code_sha256",
        "config_sha256",
        "model_bundle_sha256",
        "p3_sha256",
        "feature_dag_sha256",
    )
    for item in selected_raw:
        deployment = item[2].get("deployment")
        annotation_only = bool(
            isinstance(deployment, Mapping)
            and deployment.get("method") == "identity_promotion_without_runtime_change"
            and not bool(deployment.get("process_restarted"))
        )
        if annotation_only:
            if not selected:
                raise ValueError("identity-only annotation has no predecessor epoch")
            previous_identity = selected[-1][3]
            changed = [
                field
                for field in immutable_runtime_fields
                if previous_identity.get(field) != item[3].get(field)
            ]
            if changed:
                raise ValueError(
                    "identity-only annotation changed runtime identity fields: "
                    + ",".join(changed)
                )
            annotations = selected[-1][4].setdefault("authority_annotations", [])
            annotations.append(
                {
                    "baseline_id": item[2].get("baseline_id"),
                    "effective_ts_ns": item[0],
                    "identity_path": str(item[1]),
                    "identity_file_sha256": item[4]["identity_file_sha256"],
                }
            )
            continue
        selected.append(item)
    if not selected:
        raise ValueError("no operational baseline identity overlaps manifest scope")

    epochs: list[dict[str, Any]] = []
    unbound: list[dict[str, Any]] = []
    first_start = max(scope_start_ts_ns, selected[0][0])
    if scope_start_ts_ns < first_start:
        unbound.append(
            {
                "start_ts_ns": scope_start_ts_ns,
                "end_ts_ns": first_start,
                "reason": "no frozen operational baseline identity",
            }
        )
    previous_identity: Mapping[str, str | None] | None = None
    for index, (effective, _, payload, identity, evidence) in enumerate(selected):
        start = max(scope_start_ts_ns, effective)
        if start >= scope_end_ts_ns:
            continue
        next_effective = (
            selected[index + 1][0] if index + 1 < len(selected) else scope_end_ts_ns
        )
        end = min(scope_end_ts_ns, next_effective)
        if start >= end:
            previous_identity = identity
            continue
        fully_bound = all(_is_sha256(identity[name]) for name in REQUIRED_IDENTITY_FIELDS)
        start_reason = _boundary_reason(
            previous_identity,
            identity,
            deployment=(
                payload.get("deployment")
                if isinstance(payload.get("deployment"), Mapping)
                else None
            ),
        )
        if previous_identity is None and start > scope_start_ts_ns:
            start_reason = "identity_evidence_begins"
        epochs.append(
            {
                "epoch_id": f"E{len(epochs) + 1:03d}-{payload['baseline_id']}",
                "start_ts_ns": start,
                "end_ts_ns": end,
                "start_reason": start_reason,
                "identity": identity,
                "identity_sha256": epoch_identity_sha256(identity),
                "binding_status": "fully_bound" if fully_bound else "partially_bound",
                "identity_evidence": evidence,
                "boundary_status": str(evidence["boundary_precision"]),
                "initial_economic_state_complete": False,
                "lifecycle_estimation_authorized": bool(
                    fully_bound and restart_audit_complete
                ),
                "continuous_economic_estimation_authorized": False,
                "pooling_authorized": False,
            }
        )
        previous_identity = identity
    observed_boundaries = sorted(
        (dict(row) for row in boundary_events),
        key=lambda row: int(row["start_ts_ns"]),
    )
    for row in observed_boundaries:
        timestamp = row.get("start_ts_ns")
        if not isinstance(timestamp, int):
            raise ValueError("restart event start_ts_ns must be integer nanoseconds")
        reason = row.get("boundary_reason")
        if reason not in BOUNDARY_REASONS:
            raise ValueError(f"unsupported observed boundary reason: {reason!r}")
        updates = row.get("identity_updates", {})
        if not isinstance(updates, Mapping):
            raise ValueError("boundary event identity_updates must be an object")
        for key, value in updates.items():
            if key not in REQUIRED_IDENTITY_FIELDS:
                raise ValueError(f"unsupported boundary identity update: {key}")
            if value is not None and not _is_sha256(value):
                raise ValueError(f"boundary identity update is not SHA256: {key}")
        if not isinstance(row.get("source_evidence"), str) or not str(
            row["source_evidence"]
        ).strip():
            raise ValueError("restart event source_evidence must be non-empty")

    split_epochs: list[dict[str, Any]] = []
    for epoch in epochs:
        boundaries = [
            row
            for row in observed_boundaries
            if int(epoch["start_ts_ns"]) <= int(row["start_ts_ns"]) < int(epoch["end_ts_ns"])
        ]
        starts = [int(epoch["start_ts_ns"])] + [
            int(row["start_ts_ns"])
            for row in boundaries
            if int(row["start_ts_ns"]) > int(epoch["start_ts_ns"])
        ]
        starts = sorted(set(starts))
        boundary_by_ts = {int(row["start_ts_ns"]): row for row in boundaries}
        for part_index, start in enumerate(starts):
            end = starts[part_index + 1] if part_index + 1 < len(starts) else int(epoch["end_ts_ns"])
            part = dict(epoch)
            part["start_ts_ns"] = start
            part["end_ts_ns"] = end
            part["epoch_id"] = f"{epoch['epoch_id']}-R{part_index:02d}"
            restart = boundary_by_ts.get(start)
            if restart is not None:
                part["start_reason"] = str(restart["boundary_reason"])
                identity = dict(part["identity"])
                identity.update(dict(restart.get("identity_updates", {})))
                if part["start_reason"] in {
                    "state_restored_process_restart",
                    "unrestored_process_restart",
                }:
                    identity["initial_runtime_state_sha256"] = restart.get(
                        "initial_runtime_state_sha256"
                    )
                part["identity"] = identity
                fully_bound = all(
                    _is_sha256(identity[name]) for name in REQUIRED_IDENTITY_FIELDS
                )
                part["identity_sha256"] = epoch_identity_sha256(identity)
                part["binding_status"] = (
                    "fully_bound" if fully_bound else "partially_bound"
                )
                evidence = dict(part.get("identity_evidence", {}))
                evidence["restart_event"] = restart
                evidence["bound_fields"] = [
                    name for name, value in identity.items() if value is not None
                ]
                evidence["missing_fields"] = [
                    name for name, value in identity.items() if value is None
                ]
                part["identity_evidence"] = evidence
                part["boundary_status"] = str(
                    restart.get("boundary_status", "first_decision_bound")
                )
                part["initial_economic_state_complete"] = bool(
                    restart.get("initial_economic_state_complete", False)
                )
                part["lifecycle_estimation_authorized"] = bool(
                    fully_bound and restart_audit_complete
                )
                part["continuous_economic_estimation_authorized"] = bool(
                    fully_bound
                    and restart_audit_complete
                    and part["initial_economic_state_complete"]
                )
            split_epochs.append(part)

    return finalize_manifest(
        {
            "schema_version": SCHEMA_VERSION,
            "manifest_id": manifest_id,
            "source_clock": "utc_ns",
            "scope_start_ts_ns": int(scope_start_ts_ns),
            "scope_end_ts_ns": int(scope_end_ts_ns),
            "utc_midnight_splits_epoch": False,
            "pooled_estimation_authorized": False,
            "required_identity_fields": list(REQUIRED_IDENTITY_FIELDS),
            "restart_audit_complete": bool(restart_audit_complete),
            "observed_boundary_event_count": len(observed_boundaries),
            "epochs": split_epochs,
            "unbound_intervals": unbound,
        }
    )
