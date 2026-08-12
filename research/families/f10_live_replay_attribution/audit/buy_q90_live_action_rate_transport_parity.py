#!/usr/bin/env python3
"""Decompose the BUY q90 live/replay action-rate transport gap.

This module is deliberately mechanics-only. It reads q90 telemetry and the
frozen historical replay counters, but it never reads PnL, markout, campaign
reward, Validation, or holdout outcomes.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


SCHEMA_VERSION = "buy_q90_live_action_rate_transport_parity.v1"
IDENTITY = "buy_q90_live_action_rate_transport_parity_v1"
ROOT = Path(__file__).resolve().parents[4]
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "buy_q90_live_action_rate_transport_parity_v1_spec_20260731.json"
)

SHADOW_COLUMNS = (
    "timestamp",
    "symbol",
    "model_family_id",
    "model_file_sha256",
    "client_order_id",
    "side",
    "inventory_role",
    "valid",
    "reason",
    "edge_ms",
    "elapsed_ms",
    "missed_edges",
    "feature_source_ts_ns",
    "feature_ready_ts_ns",
    "deep_generation",
    "deep_age_ms",
    "favorable_probability",
    "adverse_probability",
    "action_authorized",
    "executed_action",
)
ACTION_COLUMNS = (
    "timestamp",
    "symbol",
    "policy_id",
    "policy_file_sha256",
    "model_file_sha256",
    "client_order_id",
    "inventory_role",
    "event",
    "adverse_value",
    "entry_threshold",
    "favorable_probability",
    "adverse_probability",
    "order_state",
    "cancel_succeeded",
    "hold_age_ms",
    "deep_generation",
    "deep_age_ms",
)

MAKER_Q90_METHODS = (
    "_release_dynamic_fill_hazard_action_hold",
    "_dynamic_fill_hazard_buy_blocked",
    "_apply_dynamic_fill_hazard_action",
    "_evaluate_dynamic_fill_hazard_shadow",
    "_on_dynamic_fill_hazard_order_terminal",
)
MODEL_RUNTIME_METHODS = (
    "__init__",
    "drop_inactive",
    "evaluate",
)
MODEL_BUNDLE_METHODS = (
    "_predict_model",
    "predict",
)
MODEL_POLICY_METHODS = (
    "eligible",
    "score",
    "cancel_required",
    "recovered",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_sha256", None)
    return canonical_sha256(normalized)


def load_spec(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("q90 transport spec must be a JSON object")
    validate_spec(payload)
    return payload


def validate_spec(spec: Mapping[str, Any]) -> None:
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected q90 transport schema")
    if spec.get("identity") != IDENTITY:
        raise ValueError("unexpected q90 transport identity")
    if spec.get("status") != (
        "frozen_after_aggregate_rate_anomaly_before_full_component_decomposition"
    ):
        raise ValueError("q90 transport status drifted")
    frozen_hash = str(spec.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(spec) != frozen_hash:
        raise ValueError("q90 transport canonical spec hash mismatch")

    policy = spec.get("policy_identity") or {}
    if (
        policy.get("side") != "BUY"
        or tuple(policy.get("eligible_roles") or ()) != ("opener", "add")
        or float(policy.get("evaluation_interval_ms", 0.0)) != 100.0
        or float(policy.get("entry_threshold", 0.0)) <= 0.0
        or not bool(policy.get("reducing_side_unchanged"))
        or not bool(policy.get("sell_side_unchanged"))
    ):
        raise ValueError("q90 transport policy identity drifted")

    events = spec.get("event_contract") or {}
    if tuple(events.get("at_risk_actions") or ()) != (
        "keep",
        "cancel",
        "retain_failed_keep",
        "invalid_keep",
    ):
        raise ValueError("q90 at-risk action contract drifted")
    if tuple(events.get("valid_at_risk_actions") or ()) != (
        "keep",
        "cancel",
        "retain_failed_keep",
    ):
        raise ValueError("q90 valid-at-risk action contract drifted")
    if tuple(events.get("first_threshold_crossing_actions") or ()) != (
        "cancel",
        "retain_failed_keep",
    ):
        raise ValueError("q90 threshold-crossing contract drifted")

    permissions = spec.get("permissions") or {}
    if not permissions or any(bool(value) for value in permissions.values()):
        raise ValueError("q90 transport identity cannot grant permissions")


def _require_file_identity(path: Path, expected_sha256: str, label: str) -> None:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"{label} is missing: {resolved}")
    actual = sha256_file(resolved)
    if actual != str(expected_sha256):
        raise ValueError(
            f"{label} hash mismatch: expected {expected_sha256}, found {actual}"
        )


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0.0:
        return math.nan
    return float(numerator / denominator)


def _finite_quantiles(
    values: Sequence[float],
    quantiles: Sequence[float] = (0.0, 0.1, 0.5, 0.9, 0.99, 1.0),
) -> dict[str, float]:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if not len(array):
        return {f"p{int(round(q * 100)):02d}": math.nan for q in quantiles}
    return {
        f"p{int(round(q * 100)):02d}": float(np.quantile(array, q))
        for q in quantiles
    }


def _utc_day(epoch_s: float) -> str:
    return datetime.fromtimestamp(float(epoch_s), timezone.utc).date().isoformat()


def _semantic_method_dump(
    path: Path,
    class_methods: Mapping[str, Sequence[str]],
) -> str:
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    classes = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
    }
    payload: list[str] = []
    for class_name, method_names in class_methods.items():
        class_node = classes.get(class_name)
        if class_node is None:
            raise ValueError(f"missing q90 class {class_name} in {path}")
        methods = {
            node.name: node
            for node in class_node.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for method_name in method_names:
            node = methods.get(method_name)
            if node is None:
                raise ValueError(
                    f"missing q90 method {class_name}.{method_name} in {path}"
                )
            payload.append(
                f"{class_name}.{method_name}:"
                + ast.dump(node, annotate_fields=True, include_attributes=False)
            )
    return "\n".join(payload)


def q90_semantic_identity(
    *,
    local_maker_engine: Path,
    ec2_maker_engine: Path,
    local_model: Path,
    ec2_model: Path,
) -> dict[str, Any]:
    maker_contract = {"MakerEngine": MAKER_Q90_METHODS}
    model_contract = {
        "DynamicFillHazardShadowRuntime": MODEL_RUNTIME_METHODS,
        "DynamicFillHazardBundle": MODEL_BUNDLE_METHODS,
        "DynamicFillHazardActionPolicy": MODEL_POLICY_METHODS,
    }
    local_maker_dump = _semantic_method_dump(local_maker_engine, maker_contract)
    ec2_maker_dump = _semantic_method_dump(ec2_maker_engine, maker_contract)
    local_model_dump = _semantic_method_dump(local_model, model_contract)
    ec2_model_dump = _semantic_method_dump(ec2_model, model_contract)
    return {
        "local_maker_engine_file_sha256": sha256_file(local_maker_engine),
        "ec2_maker_engine_file_sha256": sha256_file(ec2_maker_engine),
        "maker_q90_semantic_sha256_local": hashlib.sha256(
            local_maker_dump.encode("utf-8")
        ).hexdigest(),
        "maker_q90_semantic_sha256_ec2": hashlib.sha256(
            ec2_maker_dump.encode("utf-8")
        ).hexdigest(),
        "maker_q90_semantics_equal": local_maker_dump == ec2_maker_dump,
        "local_model_file_sha256": sha256_file(local_model),
        "ec2_model_file_sha256": sha256_file(ec2_model),
        "model_q90_semantic_sha256_local": hashlib.sha256(
            local_model_dump.encode("utf-8")
        ).hexdigest(),
        "model_q90_semantic_sha256_ec2": hashlib.sha256(
            ec2_model_dump.encode("utf-8")
        ).hexdigest(),
        "model_q90_semantics_equal": local_model_dump == ec2_model_dump,
        "q90_sensitive_semantics_equal": bool(
            local_maker_dump == ec2_maker_dump
            and local_model_dump == ec2_model_dump
        ),
    }


def _update_action_exposure(
    *,
    state: dict[str, tuple[float, bool]],
    client_order_id: str,
    timestamp: float,
    at_risk: bool,
    interval_s: float,
) -> float:
    previous = state.get(client_order_id)
    exposure_s = 0.0
    if at_risk:
        if previous is not None and previous[1]:
            exposure_s = max(0.0, float(timestamp) - previous[0])
        else:
            exposure_s = interval_s
    state[client_order_id] = (float(timestamp), bool(at_risk))
    return exposure_s


def summarize_live_shadow(
    path: Path,
    *,
    spec: Mapping[str, Any],
    chunksize: int = 250_000,
    sample_stride: int = 20,
) -> tuple[dict[str, Any], pd.DataFrame]:
    live = spec["live_observation_identity"]
    policy = spec["policy_identity"]
    events = spec["event_contract"]
    start = float(live["window_start_epoch_s"])
    end = float(live["window_end_epoch_s"])
    interval_s = float(policy["evaluation_interval_ms"]) / 1000.0
    at_risk_actions = set(map(str, events["at_risk_actions"]))
    valid_actions = set(map(str, events["valid_at_risk_actions"]))
    crossing_actions = set(
        map(str, events["first_threshold_crossing_actions"])
    )
    expected_model_hash = str(policy["model_file_sha256"])
    expected_family = str(policy["model_family_id"])

    counters: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    role_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    daily: dict[str, Counter[str]] = defaultdict(Counter)
    previous_order_state: dict[str, tuple[float, bool]] = {}
    cancel_by_order: Counter[str] = Counter()
    seen_orders: set[str] = set()
    score_sample: list[float] = []
    elapsed_sample: list[float] = []
    deep_age_sample: list[float] = []
    valid_sample_index = 0
    previous_timestamp = -math.inf
    previous_key: tuple[Any, ...] | None = None

    for frame in pd.read_csv(
        path,
        usecols=list(SHADOW_COLUMNS),
        chunksize=int(chunksize),
        low_memory=False,
    ):
        frame["timestamp"] = pd.to_numeric(frame["timestamp"], errors="raise")
        frame = frame[
            frame["timestamp"].between(start, end, inclusive="both")
        ].copy()
        if frame.empty:
            continue
        if not frame["timestamp"].is_monotonic_increasing:
            raise ValueError("q90 shadow rows are not timestamp-sorted")
        if float(frame["timestamp"].iloc[0]) < previous_timestamp:
            raise ValueError("q90 shadow chunk order regressed")
        previous_timestamp = float(frame["timestamp"].iloc[-1])

        if set(frame["side"].astype(str).str.upper()) != {"BUY"}:
            raise ValueError("q90 shadow contains a non-BUY side")
        if set(frame["model_file_sha256"].astype(str)) != {
            expected_model_hash
        }:
            raise ValueError("q90 shadow model hash drifted")
        if set(frame["model_family_id"].astype(str)) != {expected_family}:
            raise ValueError("q90 shadow model family drifted")

        for row in frame.itertuples(index=False):
            timestamp = float(row.timestamp)
            client_order_id = str(row.client_order_id)
            role = str(row.inventory_role).lower()
            action = str(row.executed_action)
            valid = bool(int(row.valid))
            key = (
                timestamp,
                client_order_id,
                int(row.edge_ms),
                action,
            )
            if key == previous_key:
                raise ValueError("q90 shadow contains a duplicate adjacent row")
            previous_key = key

            counters["rows"] += 1
            action_counts[action] += 1
            role_counts[role] += 1
            reason_counts[str(row.reason)] += 1
            if valid:
                counters["valid_rows"] += 1
            else:
                counters["invalid_rows"] += 1
            seen_orders.add(client_order_id)

            at_risk = role in {"opener", "add"} and action in at_risk_actions
            exposure_s = _update_action_exposure(
                state=previous_order_state,
                client_order_id=client_order_id,
                timestamp=timestamp,
                at_risk=at_risk,
                interval_s=interval_s,
            )
            day = _utc_day(timestamp)
            daily_row = daily[day]
            daily_row["rows"] += 1
            daily_row["valid_rows"] += int(valid)
            if at_risk:
                counters["at_risk_evaluations"] += 1
                counters["eligible_order_time_us"] += int(
                    round(exposure_s * 1_000_000.0)
                )
                daily_row["at_risk_evaluations"] += 1
                daily_row["eligible_order_time_us"] += int(
                    round(exposure_s * 1_000_000.0)
                )
                if action in valid_actions:
                    counters["valid_at_risk_evaluations"] += 1
                    daily_row["valid_at_risk_evaluations"] += 1
                if action in crossing_actions:
                    counters["first_threshold_crossings"] += 1
                    daily_row["first_threshold_crossings"] += 1
                if action == "cancel":
                    cancel_by_order[client_order_id] += 1
                    daily_row["shadow_cancel_actions"] += 1

            if valid and math.isfinite(float(row.favorable_probability)) and math.isfinite(
                float(row.adverse_probability)
            ):
                if valid_sample_index % max(1, int(sample_stride)) == 0:
                    score_sample.append(
                        float(row.adverse_probability)
                        - float(row.favorable_probability)
                    )
                    elapsed_sample.append(float(row.elapsed_ms))
                    deep_age_sample.append(float(row.deep_age_ms))
                valid_sample_index += 1

    if counters["rows"] <= 0:
        raise ValueError("q90 shadow has no rows in the frozen live window")
    eligible_time_s = counters["eligible_order_time_us"] / 1_000_000.0
    wall_hours = float(live["window_hours"])
    repeated_cancel_orders = sum(
        1 for count in cancel_by_order.values() if count > 1
    )
    repeated_cancel_excess = sum(
        max(0, count - 1) for count in cancel_by_order.values()
    )

    daily_rows = []
    for day, row in sorted(daily.items()):
        daily_rows.append(
            {
                "day": day,
                "rows": int(row["rows"]),
                "valid_rows": int(row["valid_rows"]),
                "at_risk_evaluations": int(row["at_risk_evaluations"]),
                "valid_at_risk_evaluations": int(
                    row["valid_at_risk_evaluations"]
                ),
                "first_threshold_crossings": int(
                    row["first_threshold_crossings"]
                ),
                "shadow_cancel_actions": int(row["shadow_cancel_actions"]),
                "eligible_order_time_s": float(
                    row["eligible_order_time_us"] / 1_000_000.0
                ),
            }
        )
    summary = {
        "path": str(Path(path).resolve()),
        "sha256": sha256_file(Path(path)),
        "rows": int(counters["rows"]),
        "valid_rows": int(counters["valid_rows"]),
        "invalid_rows": int(counters["invalid_rows"]),
        "unique_client_order_ids": int(len(seen_orders)),
        "new_order_activation_rate_per_hour": _safe_ratio(
            len(seen_orders), wall_hours
        ),
        "action_counts": dict(sorted(action_counts.items())),
        "role_counts": dict(sorted(role_counts.items())),
        "reason_counts": dict(reason_counts.most_common()),
        "at_risk_evaluations": int(counters["at_risk_evaluations"]),
        "valid_at_risk_evaluations": int(
            counters["valid_at_risk_evaluations"]
        ),
        "first_threshold_crossings": int(
            counters["first_threshold_crossings"]
        ),
        "eligible_order_time_s": float(eligible_time_s),
        "repeated_cancel_order_count": int(repeated_cancel_orders),
        "same_order_repeated_cancel_excess_count": int(
            repeated_cancel_excess
        ),
        "score_quantiles_sampled": _finite_quantiles(score_sample),
        "elapsed_ms_quantiles_sampled": _finite_quantiles(elapsed_sample),
        "deep_age_ms_quantiles_sampled": _finite_quantiles(deep_age_sample),
        "sample_stride": int(sample_stride),
    }
    return summary, pd.DataFrame(daily_rows)


def summarize_live_actions(
    path: Path,
    *,
    spec: Mapping[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    live = spec["live_observation_identity"]
    policy = spec["policy_identity"]
    start = float(live["window_start_epoch_s"])
    end = float(live["window_end_epoch_s"])
    frame = pd.read_csv(path, usecols=list(ACTION_COLUMNS), low_memory=False)
    frame["timestamp"] = pd.to_numeric(frame["timestamp"], errors="raise")
    frame = frame[
        frame["timestamp"].between(start, end, inclusive="both")
    ].copy()
    frame = frame.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if frame.empty:
        raise ValueError("q90 action log has no rows in the frozen live window")
    if frame.duplicated(["timestamp", "client_order_id", "event"]).any():
        raise ValueError("q90 action log contains duplicate events")
    if set(frame["policy_id"].astype(str)) != {str(policy["policy_id"])}:
        raise ValueError("q90 action policy id drifted")
    if set(frame["policy_file_sha256"].astype(str)) != {
        str(policy["policy_file_sha256"])
    }:
        raise ValueError("q90 action policy hash drifted")
    if set(frame["model_file_sha256"].astype(str)) != {
        str(policy["model_file_sha256"])
    }:
        raise ValueError("q90 action model hash drifted")
    if not np.allclose(
        pd.to_numeric(frame["entry_threshold"], errors="raise"),
        float(policy["entry_threshold"]),
        rtol=0.0,
        atol=1e-15,
    ):
        raise ValueError("q90 action entry threshold drifted")

    recovery_names = {"score_recovered", "reducing_role_release"}
    pairs: list[dict[str, Any]] = []
    unmatched_cancels = 0
    for client_order_id, group in frame.groupby(
        "client_order_id", sort=False
    ):
        records = list(group.itertuples(index=False))
        for index, row in enumerate(records):
            if str(row.event) != "cancel_request":
                continue
            recovery = next(
                (
                    candidate
                    for candidate in records[index + 1 :]
                    if str(candidate.event) in recovery_names
                ),
                None,
            )
            if recovery is None:
                unmatched_cancels += 1
                continue
            pairs.append(
                {
                    "client_order_id": str(client_order_id),
                    "inventory_role": str(row.inventory_role).lower(),
                    "cancel_ts_epoch_s": float(row.timestamp),
                    "recovery_ts_epoch_s": float(recovery.timestamp),
                    "cancel_to_recovery_s": float(
                        recovery.timestamp - row.timestamp
                    ),
                    "cancel_score": float(row.adverse_value),
                    "recovery_score": float(recovery.adverse_value),
                    "cancel_margin": float(
                        row.adverse_value - row.entry_threshold
                    ),
                    "recovery_margin": float(
                        row.entry_threshold - recovery.adverse_value
                    ),
                    "cancel_succeeded": int(row.cancel_succeeded),
                    "hold_age_ms": float(recovery.hold_age_ms),
                    "cancel_deep_generation": int(row.deep_generation),
                    "recovery_deep_generation": int(
                        recovery.deep_generation
                    ),
                    "deep_generation_delta": int(
                        recovery.deep_generation - row.deep_generation
                    ),
                    "cancel_deep_age_ms": float(row.deep_age_ms),
                    "recovery_deep_age_ms": float(recovery.deep_age_ms),
                }
            )
    episode_frame = pd.DataFrame(pairs)

    recoveries = frame[frame["event"].isin(recovery_names)].copy()
    cancels = frame[frame["event"].eq("cancel_request")].copy()
    next_cancel_rows: list[dict[str, Any]] = []
    if not recoveries.empty and not cancels.empty:
        cancel_times = cancels["timestamp"].to_numpy(dtype=float)
        cancel_ids = cancels["client_order_id"].astype(str).to_numpy()
        indices = np.searchsorted(
            cancel_times,
            recoveries["timestamp"].to_numpy(dtype=float),
            side="right",
        )
        for recovery_row, index in zip(
            recoveries.itertuples(index=False), indices
        ):
            if int(index) >= len(cancel_times):
                continue
            next_cancel_rows.append(
                {
                    "recovery_ts_epoch_s": float(recovery_row.timestamp),
                    "recovery_client_order_id": str(
                        recovery_row.client_order_id
                    ),
                    "next_cancel_ts_epoch_s": float(cancel_times[index]),
                    "next_cancel_client_order_id": str(cancel_ids[index]),
                    "recovery_to_next_cancel_s": float(
                        cancel_times[index] - recovery_row.timestamp
                    ),
                    "same_client_order_id": bool(
                        str(recovery_row.client_order_id)
                        == str(cancel_ids[index])
                    ),
                }
            )
    next_cancel_frame = pd.DataFrame(next_cancel_rows)

    event_counts = frame["event"].astype(str).value_counts().to_dict()
    role_table = (
        frame.groupby(["event", "inventory_role"], dropna=False)
        .size()
        .rename("count")
        .reset_index()
    )
    cancel_count = int(event_counts.get("cancel_request", 0))
    recovery_count = int(
        sum(event_counts.get(name, 0) for name in recovery_names)
    )
    summary = {
        "path": str(Path(path).resolve()),
        "sha256": sha256_file(Path(path)),
        "rows": int(len(frame)),
        "event_counts": {
            str(key): int(value)
            for key, value in sorted(event_counts.items())
        },
        "role_event_counts": role_table.to_dict("records"),
        "cancel_request_count": cancel_count,
        "recovery_count": recovery_count,
        "cancel_succeeded_count": int(
            pd.to_numeric(
                cancels["cancel_succeeded"], errors="coerce"
            ).fillna(0).sum()
        ),
        "paired_cancel_recovery_count": int(len(episode_frame)),
        "unmatched_cancel_count": int(unmatched_cancels),
        "recovery_per_cancel": _safe_ratio(recovery_count, cancel_count),
        "cancel_to_recovery_s": _finite_quantiles(
            episode_frame.get(
                "cancel_to_recovery_s", pd.Series(dtype=float)
            ).tolist()
        ),
        "recovery_to_next_cancel_s": _finite_quantiles(
            next_cancel_frame.get(
                "recovery_to_next_cancel_s", pd.Series(dtype=float)
            ).tolist()
        ),
        "cancel_margin": _finite_quantiles(
            episode_frame.get(
                "cancel_margin", pd.Series(dtype=float)
            ).tolist()
        ),
        "recovery_margin": _finite_quantiles(
            episode_frame.get(
                "recovery_margin", pd.Series(dtype=float)
            ).tolist()
        ),
        "hold_age_ms": _finite_quantiles(
            episode_frame.get(
                "hold_age_ms", pd.Series(dtype=float)
            ).tolist()
        ),
        "deep_generation_changed_during_hold_count": int(
            (
                episode_frame.get(
                    "deep_generation_delta", pd.Series(dtype=int)
                )
                != 0
            ).sum()
        ),
        "next_cancel_within_1s_rate": _safe_ratio(
            int(
                (
                    next_cancel_frame.get(
                        "recovery_to_next_cancel_s",
                        pd.Series(dtype=float),
                    )
                    <= 1.0
                ).sum()
            ),
            len(next_cancel_frame),
        ),
        "next_cancel_within_10s_rate": _safe_ratio(
            int(
                (
                    next_cancel_frame.get(
                        "recovery_to_next_cancel_s",
                        pd.Series(dtype=float),
                    )
                    <= 10.0
                ).sum()
            ),
            len(next_cancel_frame),
        ),
    }
    return summary, episode_frame, next_cancel_frame


def summarize_historical_replay(
    arm_daily_path: Path,
    *,
    spec: Mapping[str, Any],
) -> tuple[dict[str, Any], pd.DataFrame]:
    historical = spec["historical_replay_identity"]
    _require_file_identity(
        Path(arm_daily_path),
        str(historical["arm_daily_sha256"]),
        "historical q90 arm_daily",
    )
    frame = pd.read_parquet(arm_daily_path)
    required = {
        "day",
        "arm",
        "elapsed_hours",
        "q90_eval_count",
        "q90_cancel_request_count",
        "q90_cancel_ack_count",
        "q90_recovery_count",
        "q90_reentry_count",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(
            "historical q90 arm_daily is missing: " + ", ".join(missing)
        )
    on = frame[frame["arm"].astype(str).eq("q90_on")].copy()
    if len(on) != int(historical["development_days"]):
        raise ValueError("historical q90 Development day count drifted")
    hours = float(pd.to_numeric(on["elapsed_hours"]).sum())
    evaluations = int(pd.to_numeric(on["q90_eval_count"]).sum())
    cancels = int(pd.to_numeric(on["q90_cancel_request_count"]).sum())
    summary = {
        "days": int(len(on)),
        "elapsed_hours": hours,
        "eval_count": evaluations,
        "cancel_request_count": cancels,
        "cancel_ack_count": int(
            pd.to_numeric(on["q90_cancel_ack_count"]).sum()
        ),
        "recovery_count": int(
            pd.to_numeric(on["q90_recovery_count"]).sum()
        ),
        "reentry_count": int(
            pd.to_numeric(on["q90_reentry_count"]).sum()
        ),
        "evaluations_per_hour": _safe_ratio(evaluations, hours),
        "cancel_requests_per_evaluation": _safe_ratio(
            cancels, evaluations
        ),
        "cancel_requests_per_hour": _safe_ratio(cancels, hours),
        "five_factor_decomposition_supported": False,
        "unsupported_factors": [
            "eligible_order_time_seconds_per_wall_hour",
            "at_risk_evaluations_per_eligible_order_second",
            "valid_at_risk_probability",
            "first_threshold_crossing_probability_given_valid_at_risk",
            "cancel_request_probability_given_first_threshold_crossing",
        ],
        "reason": "The frozen replay artifact retained total evaluations and lifecycle actions but not role-specific at-risk/valid/crossing telemetry.",
    }
    return summary, on


def build_live_rate_decomposition(
    *,
    shadow: Mapping[str, Any],
    actions: Mapping[str, Any],
    wall_hours: float,
    closure_tolerance: float,
) -> dict[str, Any]:
    eligible_time_s = float(shadow["eligible_order_time_s"])
    at_risk = int(shadow["at_risk_evaluations"])
    valid = int(shadow["valid_at_risk_evaluations"])
    crossings = int(shadow["first_threshold_crossings"])
    cancel_requests = int(actions["cancel_request_count"])
    factors = {
        "eligible_order_time_seconds_per_wall_hour": _safe_ratio(
            eligible_time_s, wall_hours
        ),
        "at_risk_evaluations_per_eligible_order_second": _safe_ratio(
            at_risk, eligible_time_s
        ),
        "valid_at_risk_probability": _safe_ratio(valid, at_risk),
        "first_threshold_crossing_probability_given_valid_at_risk": (
            _safe_ratio(crossings, valid)
        ),
        "cancel_request_probability_given_first_threshold_crossing": (
            _safe_ratio(cancel_requests, crossings)
        ),
    }
    product = float(np.prod(list(factors.values())))
    observed = _safe_ratio(cancel_requests, wall_hours)
    error = abs(product - observed)
    shadow_cancel_count = int(
        (shadow.get("action_counts") or {}).get("cancel", 0)
    )
    return {
        "factors": factors,
        "product_cancel_requests_per_hour": product,
        "observed_cancel_requests_per_hour": observed,
        "closure_absolute_error_per_hour": error,
        "closure_passed": bool(error <= float(closure_tolerance)),
        "shadow_cancel_action_count": shadow_cancel_count,
        "action_log_cancel_request_count": cancel_requests,
        "shadow_action_count_matches_action_log": bool(
            shadow_cancel_count == cancel_requests
        ),
    }


def compare_live_historical_rates(
    *,
    live_shadow: Mapping[str, Any],
    live_actions: Mapping[str, Any],
    live_hours: float,
    replay: Mapping[str, Any],
) -> dict[str, Any]:
    live_evals = int(live_shadow["rows"])
    live_cancels = int(live_actions["cancel_request_count"])
    live_eval_rate = _safe_ratio(live_evals, live_hours)
    live_cancel_per_eval = _safe_ratio(live_cancels, live_evals)
    live_cancel_rate = _safe_ratio(live_cancels, live_hours)
    replay_eval_rate = float(replay["evaluations_per_hour"])
    replay_cancel_per_eval = float(replay["cancel_requests_per_evaluation"])
    replay_cancel_rate = float(replay["cancel_requests_per_hour"])
    factor_ratios = {
        "evaluations_per_hour_ratio": _safe_ratio(
            live_eval_rate, replay_eval_rate
        ),
        "cancel_requests_per_evaluation_ratio": _safe_ratio(
            live_cancel_per_eval, replay_cancel_per_eval
        ),
    }
    total_ratio = _safe_ratio(live_cancel_rate, replay_cancel_rate)
    reconstructed_ratio = float(np.prod(list(factor_ratios.values())))
    finite_positive = {
        key: value
        for key, value in factor_ratios.items()
        if math.isfinite(value) and value > 0.0
    }
    dominant = (
        max(finite_positive, key=lambda key: abs(math.log(finite_positive[key])))
        if finite_positive
        else ""
    )
    return {
        "live": {
            "evaluations_per_hour": live_eval_rate,
            "cancel_requests_per_evaluation": live_cancel_per_eval,
            "cancel_requests_per_hour": live_cancel_rate,
        },
        "historical_replay": {
            "evaluations_per_hour": replay_eval_rate,
            "cancel_requests_per_evaluation": replay_cancel_per_eval,
            "cancel_requests_per_hour": replay_cancel_rate,
        },
        "factor_ratios_live_over_historical_replay": factor_ratios,
        "observed_cancel_rate_ratio": total_ratio,
        "reconstructed_cancel_rate_ratio": reconstructed_ratio,
        "ratio_closure_absolute_error": abs(
            total_ratio - reconstructed_ratio
        ),
        "dominant_observed_coarse_factor": dominant,
    }


def classify_transport(
    *,
    semantic_identity: Mapping[str, Any],
    live_decomposition: Mapping[str, Any],
    comparison: Mapping[str, Any],
    action_summary: Mapping[str, Any],
) -> dict[str, Any]:
    semantic_equal = bool(
        semantic_identity.get("q90_sensitive_semantics_equal", False)
    )
    closure = bool(live_decomposition.get("closure_passed", False))
    action_match = bool(
        live_decomposition.get(
            "shadow_action_count_matches_action_log", False
        )
    )
    dominant = str(comparison.get("dominant_observed_coarse_factor", ""))
    if not semantic_equal or not closure or not action_match:
        classification = "implementation_or_clock_divergence"
    elif dominant == "evaluations_per_hour_ratio":
        classification = "evaluation_cadence_or_eligible_exposure_divergence"
    elif dominant == "cancel_requests_per_evaluation_ratio":
        classification = (
            "score_state_or_market_regime_transport_divergence"
        )
    else:
        classification = "mixed_or_unresolved"
    churn = bool(int(action_summary.get("paired_cancel_recovery_count", 0)) > 0)
    return {
        "classification": classification,
        "q90_sensitive_semantics_equal": semantic_equal,
        "live_rate_decomposition_closed": closure,
        "shadow_action_log_count_parity": action_match,
        "state_machine_churn_supported": churn,
        "historical_five_factor_decomposition_supported": False,
        "same_date_replay_required_before_market_regime_conclusion": True,
        "engineerable_bug_identified": False,
        "transport_supported": False,
        "action_or_live_authorization": False,
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _markdown_report(report: Mapping[str, Any]) -> str:
    live = report["rate_comparison"]["live"]
    replay = report["rate_comparison"]["historical_replay"]
    ratios = report["rate_comparison"][
        "factor_ratios_live_over_historical_replay"
    ]
    actions = report["live_action_summary"]
    classification = report["classification"]
    return "\n".join(
        [
            "# BUY q90 Live Action-Rate Transport Parity v1",
            "",
            "## Scope",
            "",
            "Mechanics/transport evidence only. No PnL, prediction promotion, "
            "policy change, rollback, or live authority is produced.",
            "",
            "## Rate decomposition",
            "",
            "| Metric | Live | Historical replay | Live / replay |",
            "|---|---:|---:|---:|",
            (
                f"| Evaluations/hour | {live['evaluations_per_hour']:.6f} | "
                f"{replay['evaluations_per_hour']:.6f} | "
                f"{ratios['evaluations_per_hour_ratio']:.3f}x |"
            ),
            (
                "| Cancel/evaluation | "
                f"{live['cancel_requests_per_evaluation']:.10f} | "
                f"{replay['cancel_requests_per_evaluation']:.10f} | "
                f"{ratios['cancel_requests_per_evaluation_ratio']:.3f}x |"
            ),
            (
                f"| Cancels/hour | {live['cancel_requests_per_hour']:.6f} | "
                f"{replay['cancel_requests_per_hour']:.6f} | "
                f"{report['rate_comparison']['observed_cancel_rate_ratio']:.3f}x |"
            ),
            "",
            "Dominant observed coarse factor: `"
            + str(
                report["rate_comparison"][
                    "dominant_observed_coarse_factor"
                ]
            )
            + "`.",
            "",
            "## Live state machine",
            "",
            f"- Cancel requests: {actions['cancel_request_count']:,}.",
            f"- Matched cancel to recovery: {actions['paired_cancel_recovery_count']:,}.",
            f"- Recovery/cancel ratio: {actions['recovery_per_cancel']:.4f}.",
            (
                "- Median cancel to recovery: "
                f"{actions['cancel_to_recovery_s']['p50']:.6f}s."
            ),
            (
                "- Median recovery to next cancel: "
                f"{actions['recovery_to_next_cancel_s']['p50']:.6f}s."
            ),
            "",
            "## Decision",
            "",
            f"`{classification['classification']}`",
            "",
            "The historical artifact lacks the role-specific at-risk, valid, "
            "and first-crossing counters needed for a five-factor replay "
            "decomposition. A same-date mechanics replay is therefore required "
            "before classifying the residual as a July market regime or a "
            "receive-time transport defect.",
            "",
            "All action, rollback, and live permissions remain `false`.",
            "",
        ]
    )


def run_audit(
    *,
    spec_path: Path,
    shadow_log: Path,
    action_log: Path,
    arm_daily: Path,
    quote_decisions: Path,
    local_maker_engine: Path,
    ec2_maker_engine: Path,
    local_model: Path,
    ec2_model: Path,
    output_dir: Path,
) -> dict[str, Any]:
    spec = load_spec(spec_path)
    live_identity = spec["live_observation_identity"]
    historical = spec["historical_replay_identity"]
    _require_file_identity(
        action_log,
        str(live_identity["action_log_sha256"]),
        "live q90 action log",
    )
    _require_file_identity(
        quote_decisions,
        str(live_identity["quote_decision_log_sha256"]),
        "live quote-decision log",
    )
    _require_file_identity(
        Path(historical["report_path"]),
        str(historical["report_sha256"]),
        "historical q90 report",
    )

    semantic_identity = q90_semantic_identity(
        local_maker_engine=local_maker_engine,
        ec2_maker_engine=ec2_maker_engine,
        local_model=local_model,
        ec2_model=ec2_model,
    )
    live_shadow, live_daily = summarize_live_shadow(
        shadow_log,
        spec=spec,
    )
    live_actions, episodes, next_cancels = summarize_live_actions(
        action_log,
        spec=spec,
    )
    replay, replay_daily = summarize_historical_replay(
        arm_daily,
        spec=spec,
    )
    live_decomposition = build_live_rate_decomposition(
        shadow=live_shadow,
        actions=live_actions,
        wall_hours=float(live_identity["window_hours"]),
        closure_tolerance=float(
            spec["rate_decomposition"][
                "closure_tolerance_absolute_per_hour"
            ]
        ),
    )
    comparison = compare_live_historical_rates(
        live_shadow=live_shadow,
        live_actions=live_actions,
        live_hours=float(live_identity["window_hours"]),
        replay=replay,
    )
    classification = classify_transport(
        semantic_identity=semantic_identity,
        live_decomposition=live_decomposition,
        comparison=comparison,
        action_summary=live_actions,
    )
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "spec_path": str(Path(spec_path).resolve()),
        "spec_sha256": sha256_file(Path(spec_path)),
        "canonical_spec_sha256": str(spec["canonical_spec_sha256"]),
        "live_window": {
            key: live_identity[key]
            for key in (
                "window_start_epoch_s",
                "window_end_epoch_s",
                "window_start_utc",
                "window_end_utc",
                "window_hours",
            )
        },
        "semantic_identity": semantic_identity,
        "live_shadow_summary": live_shadow,
        "live_action_summary": live_actions,
        "historical_replay_summary": replay,
        "live_five_factor_decomposition": live_decomposition,
        "rate_comparison": comparison,
        "classification": classification,
        "permissions": dict(spec["permissions"]),
        "validation_read": False,
        "sealed_holdout_read": False,
        "economic_outcomes_read": False,
    }

    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    live_daily.to_parquet(output_dir / "live_daily.parquet", index=False)
    replay_daily.to_parquet(output_dir / "historical_replay_daily.parquet", index=False)
    episodes.to_parquet(output_dir / "cancel_recovery_episodes.parquet", index=False)
    next_cancels.to_parquet(
        output_dir / "recovery_to_next_cancel.parquet", index=False
    )
    safe_report = _json_safe(report)
    (output_dir / "report.json").write_text(
        json.dumps(
            safe_report,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (output_dir / "report.md").write_text(
        _markdown_report(safe_report),
        encoding="utf-8",
    )
    manifest = {
        "schema_version": "buy_q90_live_action_rate_transport_execution.v1",
        "identity": IDENTITY,
        "spec_sha256": sha256_file(Path(spec_path)),
        "canonical_spec_sha256": str(spec["canonical_spec_sha256"]),
        "implementation_sha256": sha256_file(Path(__file__)),
        "test_sha256": sha256_file(
            ROOT / "tests/test_buy_q90_live_action_rate_transport_parity.py"
        ),
        "python_version": sys.version,
        "shadow_snapshot_boundary": {
            "window_start_epoch_s": float(
                live_identity["window_start_epoch_s"]
            ),
            "window_end_epoch_s": float(
                live_identity["window_end_epoch_s"]
            ),
            "complete_csv_rows": int(live_shadow["rows"]),
            "compressed_input": str(shadow_log).endswith(".gz"),
            "gzip_stream_validated_by_complete_parser_read": True,
        },
        "inputs": {
            "shadow_log": {
                "path": str(Path(shadow_log).resolve()),
                "sha256": sha256_file(Path(shadow_log)),
                "bytes": int(Path(shadow_log).stat().st_size),
            },
            "action_log": {
                "path": str(Path(action_log).resolve()),
                "sha256": sha256_file(Path(action_log)),
                "bytes": int(Path(action_log).stat().st_size),
            },
            "quote_decisions": {
                "path": str(Path(quote_decisions).resolve()),
                "sha256": sha256_file(Path(quote_decisions)),
                "bytes": int(Path(quote_decisions).stat().st_size),
            },
            "arm_daily": {
                "path": str(Path(arm_daily).resolve()),
                "sha256": sha256_file(Path(arm_daily)),
                "bytes": int(Path(arm_daily).stat().st_size),
            },
        },
        "outputs": {},
        "permissions": dict(spec["permissions"]),
    }
    for name in (
        "report.json",
        "report.md",
        "live_daily.parquet",
        "historical_replay_daily.parquet",
        "cancel_recovery_episodes.parquet",
        "recovery_to_next_cancel.parquet",
    ):
        path = output_dir / name
        manifest["outputs"][name] = {
            "sha256": sha256_file(path),
            "bytes": int(path.stat().st_size),
        }
    (output_dir / "manifest.json").write_text(
        json.dumps(
            manifest,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return safe_report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--shadow-log", type=Path, required=True)
    parser.add_argument("--action-log", type=Path, required=True)
    parser.add_argument("--arm-daily", type=Path, required=True)
    parser.add_argument("--quote-decisions", type=Path, required=True)
    parser.add_argument(
        "--local-maker-engine",
        type=Path,
        default=Path("strategy/maker_engine.py"),
    )
    parser.add_argument("--ec2-maker-engine", type=Path, required=True)
    parser.add_argument(
        "--local-model",
        type=Path,
        default=Path("strategy/dynamic_fill_hazard_model.py"),
    )
    parser.add_argument("--ec2-model", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _build_parser().parse_args(list(argv) if argv is not None else None)
    report = run_audit(
        spec_path=args.spec,
        shadow_log=args.shadow_log,
        action_log=args.action_log,
        arm_daily=args.arm_daily,
        quote_decisions=args.quote_decisions,
        local_maker_engine=args.local_maker_engine,
        ec2_maker_engine=args.ec2_maker_engine,
        local_model=args.local_model,
        ec2_model=args.ec2_model,
        output_dir=args.output_dir,
    )
    print(json.dumps(report["classification"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
