#!/usr/bin/env python3
"""Cross-day labels and 2025-pretrained EMA representation for F05 v1.2."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from data_paths import data_root, resolve_portable_path
from models.replay.f05_ema_add_wait_two_day_window import (
    F05ReplayDay,
    stitch_two_days,
)
from models.replay.f05_ema_provider_pretraining import (
    provider_ema_feature_batches,
)
from models.replay.f05_ema_source_encoder import (
    FullRankEmaEncoder,
    fit_full_rank_encoder,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_1_study as predecessor,
)
from research.families.f05_fill_quality_quote_ev.audit.multiscale_ema_add_wait_incremental_value import (
    ADD_NOW,
    WAIT_ONE_EPOCH,
    campaign_unit_weights,
    joint_washout_complete,
    model_feature_names,
)
from research.governance.public_machine_projection import (
    PublicMachineProjectionError,
    source_document_path,
    source_identity_sha256,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
IDENTITY = "multiscale_ema_add_wait_incremental_value_source_aware_v1_2"
SCHEMA_VERSION = f"{IDENTITY}.development.v1"
SPEC = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_2_spec_20260809.json"
)
EXECUTION_AMENDMENT = ROOT / (
    "research/families/f05_fill_quality_quote_ev/docs/"
    "multiscale_ema_add_wait_incremental_value_v1_2_execution_amendment_20260809.json"
)
PREDECESSOR_OUTPUT = DATA_ROOT / (
    "reports/"
    "multiscale_ema_add_wait_incremental_value_v1_1_20260809"
)
OUTPUT = DATA_ROOT / (
    "reports/"
    "multiscale_ema_add_wait_incremental_value_source_aware_v1_2_20260809"
)
PROVIDER_TRAIN_SPEC = ROOT / (
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_expanded_source_aware_semantics_v6_train_spec_20260802.json"
)
PROVIDER_CACHE_MANIFEST = DATA_ROOT / (
    "reports/"
    "cache_prewarm_provider_20250801_20251231_v13/manifest.json"
)
PROVIDER_ROOT = DATA_ROOT / "normalized_l2_research_union_v1"
PROVIDER_FEATURE_ROOT = DATA_ROOT / (
    "features_btcusdc_causal_v12_expanded_source_aware_semantics_v6_20260802"
)


class StudyError(RuntimeError):
    """Fail closed on any source, label, or model identity drift."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise StudyError(f"JSON artifact must be an object: {path}")
    return payload


def _source_identity(path: Path, *, role: str) -> str:
    try:
        return source_identity_sha256(path)
    except (OSError, PublicMachineProjectionError) as exc:
        raise StudyError(f"{role} source identity is unavailable: {path}") from exc


def _source_document(path: Path, *, role: str) -> Path:
    try:
        return source_document_path(path, require_private=False)
    except (OSError, PublicMachineProjectionError) as exc:
        raise StudyError(f"{role} source document is unavailable: {path}") from exc


def _spec_sha256() -> str:
    return _source_identity(SPEC, role="v1.2 Spec")


def _execution_amendment_sha256() -> str:
    return _source_identity(EXECUTION_AMENDMENT, role="v1.2 execution amendment")


def _require_predecessor_execution_available() -> None:
    public_predecessor = predecessor._load_json(predecessor.SPEC)
    pointer = public_predecessor["source_contract"]["operational_baseline_pointer"]
    if pointer.get("exact_bytes_status") != "available":
        raise StudyError(
            "predecessor frozen operational baseline pointer exact bytes are missing; "
            "v1.2 historical execution fails closed and must not substitute the current pointer"
        )


def _validate_artifact(binding: Mapping[str, Any], *, role: str) -> Path:
    path = resolve_portable_path(str(binding.get("path", "")), root=ROOT)
    if not path.is_absolute():
        path = ROOT / path
    path = path.expanduser().resolve()
    if not path.is_file() or _source_identity(path, role=role) != str(binding.get("sha256", "")):
        raise StudyError(f"{role} artifact drifted: {path}")
    return path


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        frame.to_parquet(temporary, index=False, compression="zstd")
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_npz(path: Path, **arrays: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{uuid.uuid4().hex}"
    try:
        with temporary.open("wb") as handle:
            np.savez_compressed(handle, **arrays)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _spec() -> dict[str, Any]:
    _require_predecessor_execution_available()
    spec = _load_json(_source_document(SPEC, role="v1.2 Spec"))
    if spec.get("identity") != IDENTITY:
        raise StudyError("v1.2 Spec identity drifted")
    predecessor_rows = spec["predecessor"]
    for role in (
        "spec",
        "execution_amendment",
        "selected_panel",
        "paired_labels",
        "censoring_failure",
    ):
        _validate_artifact(predecessor_rows[role], role=f"predecessor {role}")
    source = spec["source_aware_training"]
    for role in (
        "provider_training_spec",
        "provider_cache_manifest",
        "provider_source_manifest",
        "provider_feature_manifest",
    ):
        _validate_artifact(source[role], role=role)
    return spec


def _validate_execution_amendment() -> dict[str, Any]:
    if not EXECUTION_AMENDMENT.is_file():
        raise StudyError("v1.2 execution amendment is not frozen")
    amendment = _load_json(_source_document(EXECUTION_AMENDMENT, role="v1.2 execution amendment"))
    if amendment.get("identity") != IDENTITY:
        raise StudyError("v1.2 execution amendment identity drifted")
    for row in amendment.get("artifacts") or ():
        _validate_artifact(row, role=str(row.get("role", "execution artifact")))
    return amendment


def _predecessor_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    spec = _spec()["predecessor"]
    selected = pd.read_parquet(Path(spec["selected_panel"]["path"]))
    labels = pd.read_parquet(Path(spec["paired_labels"]["path"]))
    if len(selected) != 320 or len(labels) != 320:
        raise StudyError("predecessor panel row count drifted")
    if set(selected["opportunity_id"]) != set(labels["opportunity_id"]):
        raise StudyError("predecessor opportunity membership drifted")
    if int(labels["right_censored"].astype(bool).sum()) != 10:
        raise StudyError("predecessor censoring denominator drifted")
    return selected, labels


def _natural_next_day(day: str) -> str:
    return (date.fromisoformat(day) + timedelta(days=1)).isoformat()


def _replay_day(
    day: str,
    *,
    spec: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> tuple[F05ReplayDay, dict[str, Any], dict[str, Any]]:
    window, schedule, params, audit = predecessor._load_day_inputs(day, spec=spec, plan=plan)
    row = predecessor._day_row(plan, day)
    replay = F05ReplayDay(
        day=day,
        window=window,
        ml_data=schedule.ml_data,
        identities={
            "window": str(row["window"]["sha256"]),
            "overlay": str(schedule.identity_sha256),
            "source": str(row["daily_source_identity_sha256"]),
        },
    )
    return replay, params, audit


def _load_predecessor_trace(opportunity_id: str, action: str) -> dict[str, Any]:
    path = PREDECESSOR_OUTPUT / "arm_checkpoints" / opportunity_id / f"{action}.json"
    payload = _load_json(path)
    trace = payload.get("trace")
    if not isinstance(trace, dict):
        raise StudyError("predecessor arm checkpoint lacks trace")
    return trace


def _assert_prefix_parity(
    extended: Mapping[str, Any],
    predecessor_trace: Mapping[str, Any],
) -> None:
    exact = (
        "assignment_ts_ms",
        "assignment_inventory_btc",
        "frozen_baseline_action",
        "target_generation",
        "target_market_event_index",
        "frozen_release_ts_ms",
        "frozen_release_market_event_index",
        "frozen_release_generation",
        "target_actual_action",
        "target_order_id",
        "wait_release_ts_ms",
        "wait_release_generation",
    )
    for field in exact:
        if extended.get(field) != predecessor_trace.get(field):
            raise StudyError(f"cross-day replay changed frozen prefix field {field}")
    if not math.isclose(
        float(extended["assignment_equity_usdc"]),
        float(predecessor_trace["assignment_equity_usdc"]),
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise StudyError("cross-day replay changed assignment equity")


def _pair_traces(
    opportunity: Mapping[str, Any],
    add: Mapping[str, Any],
    wait: Mapping[str, Any],
) -> dict[str, Any]:
    if not math.isclose(
        float(add["assignment_equity_usdc"]),
        float(wait["assignment_equity_usdc"]),
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise StudyError("fork arms do not share assignment equity")
    observed = joint_washout_complete(
        predecessor._washout_state(add), predecessor._washout_state(wait)
    )
    if observed != bool(add["arm_washout_complete"] and wait["arm_washout_complete"]):
        raise StudyError("arm and joint washout contracts disagree")

    def marking(trace: Mapping[str, Any], bound: str) -> float:
        if trace["arm_washout_complete"]:
            return float(trace["decision_to_terminal_value_usdc"])
        return float(trace[f"censor_time_marking_{bound}_usdc"])

    row = dict(opportunity)
    row.update(
        {
            "add_arm_terminal_ts_ms": int(add["terminal_ts_ms"]),
            "wait_arm_terminal_ts_ms": int(wait["terminal_ts_ms"]),
            "joint_washout_ts_ms": int(max(add["terminal_ts_ms"], wait["terminal_ts_ms"])),
            "joint_washout_complete": bool(observed),
            "right_censored": not bool(observed),
            "add_value_usdc": add.get("decision_to_terminal_value_usdc"),
            "wait_value_usdc": wait.get("decision_to_terminal_value_usdc"),
            "add_minus_wait_value_usdc": (
                float(add["decision_to_terminal_value_usdc"])
                - float(wait["decision_to_terminal_value_usdc"])
                if observed
                else math.nan
            ),
            "censor_time_marking_delta_lower_usdc": (
                marking(add, "lower") - marking(wait, "upper") if not observed else math.nan
            ),
            "censor_time_marking_delta_upper_usdc": (
                marking(add, "upper") - marking(wait, "lower") if not observed else math.nan
            ),
            "censor_time_marking_semantics": (
                "contemporaneous_marks_not_eventual_terminal_bounds"
                if not observed
                else "not_applicable"
            ),
            "add_descendant_submit_count": int(add["descendant_submit_count"]),
            "wait_descendant_submit_count": int(wait["descendant_submit_count"]),
            "add_terminal_inventory_btc": float(add["terminal_inventory_btc"]),
            "wait_terminal_inventory_btc": float(wait["terminal_inventory_btc"]),
            "add_boundary_mid_value_usdc": float(add["boundary_mid_value_usdc"]),
            "wait_boundary_mid_value_usdc": float(wait["boundary_mid_value_usdc"]),
            "add_boundary_executable_value_usdc": float(add["boundary_executable_value_usdc"]),
            "wait_boundary_executable_value_usdc": float(wait["boundary_executable_value_usdc"]),
            "add_active_or_pending_order_count": int(add["active_or_pending_order_count"]),
            "wait_active_or_pending_order_count": int(wait["active_or_pending_order_count"]),
            "add_hazard_path_count": int(add["hazard_path_count"]),
            "wait_hazard_path_count": int(wait["hazard_path_count"]),
            "add_hazard_hold_active": bool(add["hazard_hold_active"]),
            "wait_hazard_hold_active": bool(wait["hazard_hold_active"]),
            "joint_terminal_identity": "absorbing_flat_quarantine_until_later_arm_washout.v1",
            "label_source_profile": "native_cross_day_continuation",
        }
    )
    return row


def repair_labels(*, output: Path = OUTPUT) -> dict[str, Any]:
    _spec()
    _validate_execution_amendment()
    selected, predecessor_labels = _predecessor_frames()
    censored = predecessor_labels.loc[predecessor_labels["right_censored"].astype(bool)].copy()
    predecessor_spec, plan = predecessor._spec_and_plan()
    plan_days = set(plan["identity_payload"]["ordered_utc_days"])
    repairs: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []
    loaded: dict[str, tuple[F05ReplayDay, dict[str, Any], dict[str, Any]]] = {}
    for opportunity in censored.to_dict("records"):
        day = str(opportunity["utc_day"])
        next_day = _natural_next_day(day)
        if next_day not in plan_days:
            raise StudyError(f"natural continuation day is not source-bound: {next_day}")
        for source_day in (day, next_day):
            if source_day not in loaded:
                loaded[source_day] = _replay_day(source_day, spec=predecessor_spec, plan=plan)
        first, first_params, first_input_audit = loaded[day]
        second, _, second_input_audit = loaded[next_day]
        if first_input_audit["projection"] != second_input_audit["projection"]:
            raise StudyError("continuation day changed the offline strategy projection")
        window, ml_data, stitch_audit = stitch_two_days(first, second)
        shared = {
            "ml_data": ml_data,
            "bbo_data": window.bbo_data,
            "l2_data": window.l2_data,
            "var_ti": window.var_ti,
            "var_retsq": window.var_retsq,
        }
        traces: dict[str, dict[str, Any]] = {}
        for action in (ADD_NOW, WAIT_ONE_EPOCH):
            trace = predecessor._run_arm(
                opportunity,
                action,
                window=window,
                base=first_params,
                shared=shared,
            )
            old_trace = _load_predecessor_trace(str(opportunity["opportunity_id"]), action)
            _assert_prefix_parity(trace, old_trace)
            checkpoint = (
                output
                / "repaired_arm_checkpoints"
                / str(opportunity["opportunity_id"])
                / f"{action}.json"
            )
            _atomic_json(
                checkpoint,
                {
                    "schema_version": f"{SCHEMA_VERSION}.arm_checkpoint",
                    "identity": IDENTITY,
                    "opportunity_id": str(opportunity["opportunity_id"]),
                    "action": action,
                    "spec_sha256": _spec_sha256(),
                    "execution_amendment_sha256": _execution_amendment_sha256(),
                    "predecessor_trace_sha256": predecessor._canonical_sha256(old_trace),
                    "stitch_audit": stitch_audit,
                    "trace": trace,
                },
            )
            traces[action] = trace
        repairs.append(_pair_traces(opportunity, traces[ADD_NOW], traces[WAIT_ONE_EPOCH]))
        audit_rows.append(
            {
                "opportunity_id": str(opportunity["opportunity_id"]),
                "assignment_day": day,
                "continuation_day": next_day,
                "joint_washout_complete": bool(
                    traces[ADD_NOW]["arm_washout_complete"]
                    and traces[WAIT_ONE_EPOCH]["arm_washout_complete"]
                ),
                "joint_washout_ts_ms": int(
                    max(
                        traces[ADD_NOW]["terminal_ts_ms"],
                        traces[WAIT_ONE_EPOCH]["terminal_ts_ms"],
                    )
                ),
                "first_day_prefix_rows": stitch_audit["first_day_prefix_rows"],
            }
        )
    repaired = pd.DataFrame(repairs)
    if set(repaired["opportunity_id"]) != set(censored["opportunity_id"]):
        raise StudyError("repaired label membership drifted")
    combined = predecessor_labels.loc[
        ~predecessor_labels["opportunity_id"].isin(repaired["opportunity_id"])
    ].copy()
    combined = pd.concat((combined, repaired), ignore_index=True)
    order = {value: index for index, value in enumerate(selected["opportunity_id"])}
    combined["_order"] = combined["opportunity_id"].map(order)
    combined = combined.sort_values("_order").drop(columns="_order").reset_index(drop=True)
    if len(combined) != 320 or set(combined["opportunity_id"]) != set(selected["opportunity_id"]):
        raise StudyError("v1.2 label panel changed the frozen denominator")
    label_path = output / "paired_labels.parquet"
    repair_path = output / "repaired_labels.parquet"
    _atomic_parquet(repair_path, repaired)
    _atomic_parquet(label_path, combined)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.label_manifest",
        "identity": IDENTITY,
        "spec_sha256": _spec_sha256(),
        "execution_amendment_sha256": _execution_amendment_sha256(),
        "predecessor_selected_panel_sha256": _sha256_file(
            PREDECESSOR_OUTPUT / "selected_opportunities.parquet"
        ),
        "predecessor_paired_labels_sha256": _sha256_file(
            PREDECESSOR_OUTPUT / "paired_labels.parquet"
        ),
        "label_path": str(label_path),
        "label_sha256": _sha256_file(label_path),
        "repaired_label_path": str(repair_path),
        "repaired_label_sha256": _sha256_file(repair_path),
        "rows": int(len(combined)),
        "predecessor_observed_rows_reused": 310,
        "cross_day_repaired_rows": int(len(repaired)),
        "right_censored_rows": int(combined["right_censored"].astype(bool).sum()),
        "continuation_audit": audit_rows,
        "development_outcomes_read": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(output / "label_manifest.json", manifest)
    return manifest


def _provider_days() -> tuple[str, ...]:
    spec = _spec()["source_aware_training"]["provider_training_spec"]
    payload = _load_json(PROVIDER_TRAIN_SPEC)
    days = tuple(str(day) for day in payload[str(spec["day_field"])])
    if len(days) != int(spec["expected_2025_days"]) or any(
        not day.startswith("2025-") for day in days
    ):
        raise StudyError("2025 provider pretraining denominator drifted")
    available = {str(row["day"]): row for row in _load_json(PROVIDER_CACHE_MANIFEST)["windows"]}
    if not set(days).issubset(available):
        raise StudyError("provider cache lacks a frozen pretraining day")
    return days


def fit_2025_encoder(*, output: Path = OUTPUT) -> dict[str, Any]:
    _spec()
    _validate_execution_amendment()
    days = _provider_days()
    source_receipts: list[dict[str, Any]] = []

    def batches() -> Any:
        for day in days:
            prior = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
            prior_path = PROVIDER_ROOT / "bbo" / f"BTCUSDC-bbo-{prior}.parquet"
            target_path = PROVIDER_ROOT / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
            feature_path = PROVIDER_FEATURE_ROOT / f"features_{day}.parquet"
            for path in (prior_path, target_path, feature_path):
                if not path.is_file():
                    raise StudyError(f"provider encoder source is missing: {path}")
            matrices = provider_ema_feature_batches(
                pd.read_parquet(prior_path),
                pd.read_parquet(target_path),
                pd.read_parquet(feature_path),
                day=day,
            )
            source_receipts.append(
                {
                    "day": day,
                    "prior_bbo": {"path": str(prior_path), "sha256": _sha256_file(prior_path)},
                    "target_bbo": {"path": str(target_path), "sha256": _sha256_file(target_path)},
                    "target_features": {
                        "path": str(feature_path),
                        "sha256": _sha256_file(feature_path),
                    },
                    "buy_rows": int(len(matrices["BUY"])),
                    "sell_rows": int(len(matrices["SELL"])),
                }
            )
            yield matrices["BUY"]
            yield matrices["SELL"]

    encoder = fit_full_rank_encoder(batches(), feature_names=model_feature_names())
    artifact_path = output / "ema_encoder_2025_provider.npz"
    _atomic_npz(
        artifact_path,
        feature_names=np.asarray(encoder.feature_names, dtype="U128"),
        mean=encoder.mean,
        scale=encoder.scale,
        components=encoder.components,
        eigenvalues=encoder.eigenvalues,
        training_rows=np.asarray([encoder.training_rows], dtype=np.int64),
    )
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.provider_encoder",
        "identity": IDENTITY,
        "spec_sha256": _spec_sha256(),
        "execution_amendment_sha256": _execution_amendment_sha256(),
        "artifact_path": str(artifact_path),
        "artifact_sha256": _sha256_file(artifact_path),
        "feature_names": list(encoder.feature_names),
        "component_count": len(encoder.feature_names),
        "component_selection": "none_full_rank",
        "training_days": list(days),
        "training_day_count": len(days),
        "training_rows": encoder.training_rows,
        "source_receipts": source_receipts,
        "economic_outcomes_read": False,
        "provider_queue_or_lifecycle_authority": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    _atomic_json(output / "ema_encoder_2025_provider_manifest.json", manifest)
    return manifest


def _load_encoder(output: Path) -> tuple[FullRankEmaEncoder, dict[str, Any]]:
    manifest = _load_json(output / "ema_encoder_2025_provider_manifest.json")
    artifact = Path(manifest["artifact_path"])
    if _sha256_file(artifact) != manifest.get("artifact_sha256"):
        raise StudyError("2025 EMA encoder artifact drifted")
    with np.load(artifact, allow_pickle=False) as values:
        encoder = FullRankEmaEncoder(
            feature_names=tuple(str(value) for value in values["feature_names"]),
            mean=np.array(values["mean"], copy=True),
            scale=np.array(values["scale"], copy=True),
            components=np.array(values["components"], copy=True),
            eigenvalues=np.array(values["eigenvalues"], copy=True),
            training_rows=int(values["training_rows"][0]),
        )
    encoder.validate()
    if int(manifest.get("training_day_count", -1)) != 66:
        raise StudyError("2025 EMA encoder day support drifted")
    return encoder, manifest


def _fit_predict_m1(
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    encoded_columns: Sequence[str],
) -> np.ndarray:
    weights = train["campaign_weight"].to_numpy(dtype=np.float64)
    x_train, x_test = predecessor._transform(train, test, predecessor.M0_FEATURES, weights)
    encoded_train = train[list(encoded_columns)].to_numpy(dtype=np.float64)
    encoded_test = test[list(encoded_columns)].to_numpy(dtype=np.float64)
    if not np.isfinite(encoded_train).all() or not np.isfinite(encoded_test).all():
        raise StudyError("2025-pretrained EMA scores are nonfinite")
    model = Ridge(alpha=10.0, fit_intercept=True, solver="svd")
    model.fit(
        np.column_stack((x_train, encoded_train)),
        train["add_minus_wait_value_usdc"].to_numpy(dtype=np.float64),
        sample_weight=weights,
    )
    return model.predict(np.column_stack((x_test, encoded_test)))


def evaluate(*, output: Path = OUTPUT) -> dict[str, Any]:
    _spec()
    _validate_execution_amendment()
    label_manifest = _load_json(output / "label_manifest.json")
    label_path = Path(label_manifest["label_path"])
    if _sha256_file(label_path) != label_manifest.get("label_sha256"):
        raise StudyError("v1.2 label panel drifted")
    panel = pd.read_parquet(label_path)
    if len(panel) != 320 or panel["right_censored"].astype(bool).any():
        raise StudyError("right-censored labels forbid M0/M1 evaluation")
    encoder, encoder_manifest = _load_encoder(output)
    raw_ema = panel[list(encoder.feature_names)].to_numpy(dtype=np.float64)
    encoded = encoder.transform(raw_ema)
    encoded_columns = tuple(f"ema_2025_encoder_pc_{index:03d}" for index in range(encoded.shape[1]))
    for index, name in enumerate(encoded_columns):
        panel[name] = encoded[:, index]
    panel["campaign_weight"] = campaign_unit_weights(
        panel,
        campaign_columns=("utc_day", "side", "prospective_campaign_side_id"),
    )
    weight_sums = panel.groupby(["utc_day", "side", "prospective_campaign_side_id"], observed=True)[
        "campaign_weight"
    ].sum()
    weight_error = float((weight_sums - 1.0).abs().max())
    if weight_error > 1e-12:
        raise StudyError("campaign total training weight drifted")
    predecessor_spec = _load_json(predecessor.SPEC)
    oof_rows: list[pd.DataFrame] = []
    for side in ("SELL", "BUY"):
        side_frame = panel.loc[panel["side"].eq(side)].copy()
        for fold in predecessor_spec["chronological_oof"]["folds"]:
            test = side_frame.loc[side_frame["utc_day"].isin(fold["test_days"])].copy()
            first_test_ts = int(test["ts_ms"].min())
            train = side_frame.loc[
                side_frame["utc_day"].isin(fold["fit_day_candidates_after_day_embargo"])
                & side_frame["joint_washout_ts_ms"].lt(first_test_ts)
            ].copy()
            if train.empty or test.empty:
                raise StudyError(f"{side} fold {fold['fold']} lacks train/test rows")
            m0 = predecessor._fit_predict(train, test, predecessor.M0_FEATURES)
            m1 = _fit_predict_m1(train, test, encoded_columns=encoded_columns)
            rows = test[
                [
                    "opportunity_id",
                    "utc_day",
                    "side",
                    "cooldown_phase",
                    "prospective_campaign_side_id",
                    "campaign_weight",
                    "add_minus_wait_value_usdc",
                ]
            ].copy()
            rows["fold"] = int(fold["fold"])
            rows["prediction_m0"] = m0
            rows["prediction_m1"] = m1
            oof_rows.append(rows)
    oof = pd.concat(oof_rows, ignore_index=True)
    if oof["opportunity_id"].duplicated().any():
        raise StudyError("native OOF opportunity rows overlap")
    oof["squared_error_reduction"] = (
        oof["add_minus_wait_value_usdc"] - oof["prediction_m0"]
    ) ** 2 - (oof["add_minus_wait_value_usdc"] - oof["prediction_m1"]) ** 2
    oof["absolute_error_reduction"] = (
        oof["add_minus_wait_value_usdc"] - oof["prediction_m0"]
    ).abs() - (oof["add_minus_wait_value_usdc"] - oof["prediction_m1"]).abs()
    oof_path = output / "native_oof_predictions.parquet"
    _atomic_parquet(oof_path, oof)
    side_reports: dict[str, Any] = {}
    for side in ("SELL", "BUY"):
        rows = oof.loc[oof["side"].eq(side)].copy()
        squared = predecessor._nested_cluster_interval(
            rows, "squared_error_reduction", draws=20_000, seed=20_260_809
        )
        absolute = predecessor._nested_cluster_interval(
            rows, "absolute_error_reduction", draws=20_000, seed=20_260_809
        )
        fold_support = {
            str(fold): int(count)
            for fold, count in rows.groupby("fold", observed=True).size().items()
        }
        side_reports[side] = {
            "native_oof_rows": int(len(rows)),
            "native_oof_days": int(rows["utc_day"].nunique()),
            "native_oof_campaigns": int(rows["prospective_campaign_side_id"].nunique()),
            "fold_support": fold_support,
            "squared_error_reduction": squared,
            "absolute_error_reduction": absolute,
            "m1_incremental_prediction_gate_passed": bool(
                squared["lcb_95"] > 0.0
                and absolute["lcb_95"] > 0.0
                and set(fold_support) == {"1", "2", "3", "4"}
            ),
        }
    report = {
        "schema_version": f"{SCHEMA_VERSION}.report",
        "identity": IDENTITY,
        "status": "development_native_oof_prediction_evidence_read",
        "spec_sha256": _spec_sha256(),
        "execution_amendment_sha256": _execution_amendment_sha256(),
        "label_sha256": _sha256_file(label_path),
        "provider_encoder_sha256": encoder_manifest["artifact_sha256"],
        "provider_training_days": int(encoder_manifest["training_day_count"]),
        "provider_training_rows": int(encoder_manifest["training_rows"]),
        "provider_economic_outcomes_read": False,
        "native_oof_predictions_path": str(oof_path),
        "native_oof_predictions_sha256": _sha256_file(oof_path),
        "selected_opportunities": int(len(panel)),
        "right_censored_labels": 0,
        "campaign_total_weight_max_abs_error": weight_error,
        "side_reports": side_reports,
        "f09_registration_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    _atomic_json(output / "report.json", report)
    return report


def preflight() -> dict[str, Any]:
    spec = _spec()
    amendment = _validate_execution_amendment()
    selected, labels = _predecessor_frames()
    return {
        "identity": IDENTITY,
        "spec_sha256": _spec_sha256(),
        "execution_amendment_sha256": _execution_amendment_sha256(),
        "selected_rows": len(selected),
        "predecessor_observed_rows": int((~labels["right_censored"].astype(bool)).sum()),
        "predecessor_right_censored_rows": int(labels["right_censored"].astype(bool).sum()),
        "provider_training_days": len(_provider_days()),
        "provider_economic_outcomes_read": False,
        "execution_artifacts": len(amendment.get("artifacts") or ()),
        "action_authorized": False,
        "live_authorized": False,
        "spec_status": spec["status_at_freeze"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("preflight", "repair-labels", "fit-2025-encoder", "evaluate")
    )
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    if args.command == "preflight":
        result = preflight()
    elif args.command == "repair-labels":
        result = repair_labels(output=args.output)
    elif args.command == "fit-2025-encoder":
        result = fit_2025_encoder(output=args.output)
    else:
        result = evaluate(output=args.output)
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
