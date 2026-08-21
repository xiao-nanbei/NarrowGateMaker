#!/usr/bin/env python3
"""Rebuild the completed F05 statistics before the BUY E3 owner refit.

This module consumes only the immutable BUY/SELL Development OOF artifacts
produced by the formal successor.  It first reproduces each side's published
report from its daily rows, then estimates one joint max-t family across both
sides.  It never fits a policy, runs a new economic arm, or reads Validation or
sealed holdout evidence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_formal_component_closeout_v1 as component_closeout,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_nested_oof_v1 as nested,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_replay_adapter_v1 as replay_adapter,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 import (
    SUCCESSOR_CANDIDATE_LADDER,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_persistent_policy_v3_inference import (
    webb_wild_day_max_t,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_owner_buy_e3_v1"
OWNER_CANDIDATE = "E3_HIGHER_ORDER_BOOLEAN"
MATCHED_OWNER_CANDIDATE = "ACTION_MATCHED_CONTROLS::E3_HIGHER_ORDER_BOOLEAN"
FORMAL_REPORT_FIELDS = (
    "candidate_reports",
    "candidate_bands",
    "candidate_week_bands",
    "hierarchy_bands",
    "hierarchy_week_bands",
    "confirmatory_bands",
    "confirmatory_week_bands",
    "risk_bands",
    "risk_week_bands",
    "scorecards",
    "hierarchy",
    "outer_oof_row_count",
)
OUTER_RECEIPT_CANDIDATE_ORDER = (
    "B0_CURRENT_EXACT",
    "B1_CAMPAIGN_AGE_ONLY",
    "B2_CAMPAIGN_PLUS_H16_H256",
    "B3_CURRENT_SEMANTIC_EQUIVALENT",
    "E1_FULL_EMA_BANK",
    "E2_DIRECTIONAL_EMA",
    "E3_HIGHER_ORDER_BOOLEAN",
    "M2_TRUE_INCREMENTAL",
    nested.CONTINUOUS_COMPARATOR,
    *nested.MATCHED_CONTROL_NAMES,
)
PROFILE_BY_CANDIDATE = {
    "B0_CURRENT_EXACT": "preregistered_fixed",
    "B1_CAMPAIGN_AGE_ONLY": "preregistered_fixed",
    "B2_CAMPAIGN_PLUS_H16_H256": "preregistered_fixed",
    "B3_CURRENT_SEMANTIC_EQUIVALENT": "preregistered_fixed",
    "E1_FULL_EMA_BANK": "e1_all_45_pairs_v1",
    "E2_DIRECTIONAL_EMA": "e2_all_pairs_all_semantics_v1",
    "E3_HIGHER_ORDER_BOOLEAN": "e3_high_order_multirule_dnf_v1",
    "M2_TRUE_INCREMENTAL": "m2_true_trade_depth_increment_v1",
    nested.CONTINUOUS_COMPARATOR: "continuous_full_feature_comparator_v1",
    **{name: "action_rate_and_duration_matched" for name in nested.MATCHED_CONTROL_NAMES},
}
EXPECTED_CANDIDATES = tuple(
    sorted(
        (set(SUCCESSOR_CANDIDATE_LADDER) - {"ACTION_MATCHED_CONTROLS"})
        | set(nested.MATCHED_CONTROL_NAMES)
        | {nested.CONTINUOUS_COMPARATOR}
    )
)
EXPECTED_BUY_RESULT_FILE_SHA256 = "a9976a37f6984efae41a3673c782c22d00cb5e08b58b6fee59e84a7b039b9fc0"
EXPECTED_BUY_REPORT_FILE_SHA256 = "6f0acfd5b99b2366abec6091e35f11ab5fd9514ecef08764965c8f801e9e7e9f"
EXPECTED_BUY_ROWS_FILE_SHA256 = "11ccf2d33184abb597b3fd1c65f64e0222520d9360d65780f0528e425dd99399"
EXPECTED_BUY_RESULT_CANONICAL_SHA256 = (
    "25ab17fa3c3f222aaa631249e8dbef3520bb4a26f18952e21c406bafcf62c6cd"
)
EXPECTED_SELL_RESULT_FILE_SHA256 = (
    "3fb2a030216d991285b0531aedc1ca6db51ccc0d85de0e2c2df330e764ff8395"
)
EXPECTED_SELL_REPORT_FILE_SHA256 = (
    "a213c558e3f76f64120bdcdc6e0670626eedca1fe53decc0c82fe2b2a3d0f685"
)
EXPECTED_SELL_RESULT_CANONICAL_SHA256 = (
    "b80b00cc0ea5f6e01e68a7ea1742cd81ad647686e96adcd20fadf7a76c4b2260"
)
EXPECTED_BUY_EXECUTION_MANIFEST_SHA256 = (
    "2021a70f2f15f4fff82240cdc494556413da0fc24d369be00fd60628bcf3395a"
)
EXPECTED_SELL_EXECUTION_MANIFEST_SHA256 = (
    "da4a55c4f870f64230493415d240769c2f1d346edd02731d573a8eab55177df8"
)
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class OwnerBuyE3CloseoutError(RuntimeError):
    """Raised when an immutable OOF or owner-decision binding drifts."""


def canonical_sha256(value: Any) -> str:
    return component_closeout.canonical_sha256(value)


def document_sha256(value: Mapping[str, Any], field: str) -> str:
    return component_closeout.document_sha256(value, field)


def file_sha256(path: Path) -> str:
    return component_closeout.file_sha256(path)


def _require_file_sha(path: Path, expected: str, *, label: str) -> str:
    if _SHA_RE.fullmatch(expected) is None:
        raise OwnerBuyE3CloseoutError(f"{label} expected SHA256 is invalid")
    observed = file_sha256(path)
    if observed != expected:
        raise OwnerBuyE3CloseoutError(f"{label} file SHA256 drifted: {observed} != {expected}")
    return observed


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        return component_closeout.load_json(path, label=label)
    except component_closeout.FormalComponentCloseoutError as exc:
        raise OwnerBuyE3CloseoutError(str(exc)) from exc


def _candidate(*, name: str, side: str, policy_sha256: str) -> nested.FittedCandidate:
    return nested.FittedCandidate(
        ladder_name=name,
        side=side,
        policy=None,
        selected_profile=PROFILE_BY_CANDIDATE[name],
        training_days=(),
        training_row_sha256=hashlib.sha256(b"").hexdigest(),
        policy_payload={},
        policy_sha256=policy_sha256,
        fit_audit={},
        feature_pool_audit=None,
    )


def _cache_outer_index(
    cache_root: Path,
    *,
    execution_manifest_sha256: str,
    side: str,
) -> dict[tuple[str, str, str], tuple[Path, Mapping[str, Any]]]:
    output: dict[tuple[str, str, str], tuple[Path, Mapping[str, Any]]] = {}
    for progress_path in sorted(cache_root.joinpath("progress").glob("*.json")):
        progress = _load_json(progress_path, label="SELL cache progress")
        key = progress.get("cache_key")
        if not isinstance(key, Mapping):
            continue
        if (
            key.get("execution_manifest_sha256") != execution_manifest_sha256
            or str(key.get("side", "")).upper() != side
            or key.get("stage") != "outer_oof"
        ):
            continue
        key_sha = str(progress.get("cache_key_sha256", ""))
        if progress.get("state") != "complete" or key_sha != progress_path.stem:
            raise OwnerBuyE3CloseoutError("SELL outer cache progress is incomplete")
        entry_path = cache_root / "entries" / key_sha / "manifest.json"
        entry = _load_json(entry_path, label="SELL outer cache entry")
        if (
            entry.get("cache_key") != dict(key)
            or entry.get("cache_key_sha256") != key_sha
            or entry.get("complete") is not True
            or entry.get("atomic_admission") is not True
            or entry.get("receipt_sha256") != document_sha256(entry, "receipt_sha256")
        ):
            raise OwnerBuyE3CloseoutError("SELL outer cache entry drifted")
        files = entry.get("files")
        rows_binding = files.get("rows") if isinstance(files, Mapping) else None
        if not isinstance(rows_binding, Mapping):
            raise OwnerBuyE3CloseoutError("SELL outer cache row binding is missing")
        rows_path = entry_path.parent / str(rows_binding.get("file", ""))
        if (
            not rows_path.is_file()
            or file_sha256(rows_path) != rows_binding.get("sha256")
            or int(rows_binding.get("rows", -1)) != 1
        ):
            raise OwnerBuyE3CloseoutError("SELL outer cache row file drifted")
        rows = pd.read_parquet(rows_path)
        if len(rows) != 1 or replay_adapter._frame_sha256(rows) != rows_binding.get("frame_sha256"):
            raise OwnerBuyE3CloseoutError("SELL outer cache frame SHA256 drifted")
        slot = (
            str(key.get("fold_id", "")),
            str(key.get("candidate_policy_sha256", "")),
            str(key.get("utc_day", "")),
        )
        if slot in output:
            raise OwnerBuyE3CloseoutError("SELL outer cache contains a duplicate slot")
        output[slot] = (rows_path, rows_binding)
    if len(output) != 260:
        raise OwnerBuyE3CloseoutError(f"SELL outer cache census drifted: {len(output)} != 260")
    return output


def reconstruct_sell_outer_oof_rows(
    *,
    result: Mapping[str, Any],
    cache_root: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    try:
        cache_audit = component_closeout.audit_cache(
            cache_root,
            execution_manifest_sha256=EXPECTED_SELL_EXECUTION_MANIFEST_SHA256,
            expected_side="SELL",
        )
    except component_closeout.FormalComponentCloseoutError as exc:
        raise OwnerBuyE3CloseoutError(str(exc)) from exc
    index = _cache_outer_index(
        cache_root,
        execution_manifest_sha256=EXPECTED_SELL_EXECUTION_MANIFEST_SHA256,
        side="SELL",
    )
    receipts = [
        receipt
        for receipt in result.get("sequential_replay_receipts", ())
        if isinstance(receipt, Mapping) and receipt.get("stage") == "outer_oof"
    ]
    if len(receipts) != 52:
        raise OwnerBuyE3CloseoutError("SELL outer replay receipt census drifted")
    by_fold: dict[str, list[Mapping[str, Any]]] = {}
    for receipt in receipts:
        if receipt.get("receipt_sha256") != document_sha256(receipt, "receipt_sha256"):
            raise OwnerBuyE3CloseoutError("SELL batch replay receipt hash drifted")
        if (
            receipt.get("side") != "SELL"
            or receipt.get("bindings", {}).get("execution_manifest_sha256")
            != EXPECTED_SELL_EXECUTION_MANIFEST_SHA256
            or receipt.get("repeated_sequential_policy") is not True
            or receipt.get("one_shot_effect_aggregation_used") is not False
        ):
            raise OwnerBuyE3CloseoutError("SELL batch replay receipt contract drifted")
        by_fold.setdefault(str(receipt.get("fold_id", "")), []).append(receipt)
    if set(by_fold) != {"outer1", "outer2", "outer3", "outer4"}:
        raise OwnerBuyE3CloseoutError("SELL outer fold receipt census drifted")

    parts: list[pd.DataFrame] = []
    consumed_slots: set[tuple[str, str, str]] = set()
    receipt_bindings: list[dict[str, Any]] = []
    for fold_id in ("outer1", "outer2", "outer3", "outer4"):
        fold_receipts = by_fold[fold_id]
        if len(fold_receipts) != len(OUTER_RECEIPT_CANDIDATE_ORDER):
            raise OwnerBuyE3CloseoutError("SELL fold candidate receipt count drifted")
        for name, receipt in zip(OUTER_RECEIPT_CANDIDATE_ORDER, fold_receipts, strict=True):
            policy_sha = str(receipt.get("candidate_policy_sha256", ""))
            days = tuple(str(day) for day in receipt.get("days", ()))
            if _SHA_RE.fullmatch(policy_sha) is None or len(days) != 5:
                raise OwnerBuyE3CloseoutError("SELL candidate receipt identity drifted")
            frames: list[pd.DataFrame] = []
            for day in days:
                slot = (fold_id, policy_sha, day)
                cached = index.get(slot)
                if cached is None or slot in consumed_slots:
                    raise OwnerBuyE3CloseoutError("SELL candidate/day cache slot drifted")
                consumed_slots.add(slot)
                frames.append(pd.read_parquet(cached[0]))
            rows = replay_adapter._concat_sequential_day_results(frames)
            rows["sequential_batch_receipt_sha256"] = receipt["receipt_sha256"]
            for field in (
                "execution_manifest_sha256",
                "source_manifest_sha256",
                "panel_manifest_sha256",
                "fold_manifest_sha256",
            ):
                rows[field] = receipt["bindings"][field]
            candidate = _candidate(name=name, side="SELL", policy_sha256=policy_sha)
            request = nested.EvaluationRequest(
                candidate=candidate,
                side="SELL",
                days=days,
                fold_id=fold_id,
                stage="outer_oof",
                panel_role=str(rows["panel_role"].iloc[0]),
            )
            validated = nested._validate_evaluation(rows, request)
            parts.append(validated)
            receipt_bindings.append(
                {
                    "fold_id": fold_id,
                    "candidate_name": name,
                    "candidate_policy_sha256": policy_sha,
                    "receipt_sha256": receipt["receipt_sha256"],
                    "days": list(days),
                }
            )
    if consumed_slots != set(index):
        raise OwnerBuyE3CloseoutError("SELL outer cache has unbound candidate/day slots")
    oof = replay_adapter._concat_sequential_day_results(parts)
    if len(oof) != 260:
        raise OwnerBuyE3CloseoutError("SELL reconstructed OOF row count drifted")
    return oof, {
        "cache_audit_canonical_sha256": cache_audit["canonical_cache_audit_sha256"],
        "cache_unit_set_sha256": cache_audit["cache_unit_set_sha256"],
        "outer_frame_count": 260,
        "outer_batch_receipt_count": 52,
        "outer_batch_receipt_set_sha256": canonical_sha256(receipt_bindings),
    }


def normalize_sparse_count_columns(
    frames: Sequence[pd.DataFrame],
) -> tuple[pd.DataFrame, ...]:
    """Materialize absent serialized count cells as structural integer zeros."""

    count_columns = sorted(
        {
            column
            for frame in frames
            for column in frame.columns
            if any(column.startswith(prefix) for prefix in nested.REQUIRED_COUNT_PREFIXES)
        }
    )
    normalized: list[pd.DataFrame] = []
    for frame in frames:
        result = frame.copy()
        for column in count_columns:
            if column not in result:
                result[column] = pd.Series(0, index=result.index, dtype="int64")
                continue
            values = pd.to_numeric(result[column], errors="coerce").fillna(0)
            if (values < 0).any() or values.mod(1).ne(0).any():
                raise OwnerBuyE3CloseoutError(f"serialized sparse count {column!r} is invalid")
            result[column] = values.astype("int64")
        normalized.append(result)
    return tuple(normalized)


def _statistical_payload(
    oof: pd.DataFrame,
    *,
    sides: Sequence[str],
    stability: Mapping[str, Any],
    config: nested.NestedOofConfig,
) -> dict[str, Any]:
    normalized_sides = tuple(str(side).upper() for side in sides)
    if normalized_sides != config.sides:
        raise OwnerBuyE3CloseoutError("statistical side contract drifted")
    observed = tuple(sorted(set(oof["candidate_name"].astype(str))))
    if observed != EXPECTED_CANDIDATES:
        raise OwnerBuyE3CloseoutError("OOF candidate census drifted")
    reports: dict[str, dict[str, Any]] = {}
    candidate_series: dict[str, pd.Series] = {}
    for side in normalized_sides:
        for name in EXPECTED_CANDIDATES:
            key = f"{side}:{name}"
            rows = oof.loc[(oof["side"] == side) & (oof["candidate_name"] == name)].copy()
            reports[key] = nested._candidate_report(
                rows, tolerance=config.zero_difference_tolerance_usdc
            )
            candidate_series[key] = nested._candidate_day_series(oof, side=side, candidate=name)
    candidate_bands = webb_wild_day_max_t(
        candidate_series,
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed,
        confidence=config.confidence,
    )
    candidate_week_bands = webb_wild_day_max_t(
        {name: nested._week_block_series(series) for name, series in candidate_series.items()},
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 10,
        confidence=config.confidence,
    )
    risk_series: dict[str, pd.Series] = {}
    for side in normalized_sides:
        for name in EXPECTED_CANDIDATES:
            for metric in nested.RISK_METRIC_COLUMNS:
                key = f"{side}:{name}:{metric}"
                risk_series[key] = nested._candidate_metric_day_series(
                    oof, side=side, candidate=name, metric=metric
                )
    risk_bands = webb_wild_day_max_t(
        risk_series,
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 3,
        confidence=config.confidence,
    )
    risk_week_bands = webb_wild_day_max_t(
        {name: nested._week_block_series(series) for name, series in risk_series.items()},
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 13,
        confidence=config.confidence,
    )
    comparisons = (
        ("E1-B0", config.hierarchy.e1, "B0_CURRENT_EXACT"),
        ("E2-E1", config.hierarchy.e2, config.hierarchy.e1),
        ("E3-E2", config.hierarchy.e3, config.hierarchy.e2),
        ("M2-E3", config.hierarchy.m2, config.hierarchy.e3),
        (
            "CONTINUOUS-BOOLEAN",
            config.hierarchy.continuous,
            config.hierarchy.boolean,
        ),
    )
    hierarchy_series: dict[str, pd.Series] = {}
    hierarchy_support: dict[str, tuple[int, int]] = {}
    for side in normalized_sides:
        for suffix, candidate, reference in comparisons:
            key = f"successor:{side}:{suffix}"
            series, identified, total = nested._paired_contrast(
                oof, side=side, candidate=candidate, reference=reference
            )
            hierarchy_series[key] = series
            hierarchy_support[key] = (identified, total)
    hierarchy_bands = webb_wild_day_max_t(
        hierarchy_series,
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 1,
        confidence=config.confidence,
    )
    hierarchy_week_bands = webb_wild_day_max_t(
        {name: nested._week_block_series(series) for name, series in hierarchy_series.items()},
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 11,
        confidence=config.confidence,
    )
    confirmatory_series: dict[str, pd.Series] = {}
    for side in normalized_sides:
        for suffix, candidate, reference in nested.CONFIRMATORY_COMPARISONS:
            key = f"successor:{side}:{suffix}"
            series, _identified, _total = nested._paired_contrast(
                oof, side=side, candidate=candidate, reference=reference
            )
            confirmatory_series[key] = series
    confirmatory_bands = webb_wild_day_max_t(
        confirmatory_series,
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 2,
        confidence=config.confidence,
    )
    confirmatory_week_bands = webb_wild_day_max_t(
        {name: nested._week_block_series(series) for name, series in confirmatory_series.items()},
        draws=config.simultaneous_draws,
        seed=config.simultaneous_seed + 12,
        confidence=config.confidence,
    )
    hierarchy = nested._hierarchy_report(
        hierarchy_bands,
        hierarchy_week_bands,
        hypothesis_support=hierarchy_support,
        sides=normalized_sides,
        epsilon=config.economic_epsilon_usdc,
    )
    scorecards = {
        key: nested._build_candidate_scorecard(
            side=key.split(":", 1)[0],
            candidate=key.split(":", 1)[1],
            report=report,
            candidate_bands=candidate_bands,
            candidate_week_bands=candidate_week_bands,
            risk_bands=risk_bands,
            risk_week_bands=risk_week_bands,
        )
        for key, report in sorted(reports.items())
    }
    return {
        "schema_version": f"{IDENTITY}.joint_oof_statistics.v1",
        "source_formal_schema_version": nested.IDENTITY,
        "oof_evidence_scope": nested.OOF_EVIDENCE_SCOPE,
        "exact_final_artifact_oof_available": False,
        "final_refit_performed": False,
        "candidate_reports": reports,
        "stability": dict(stability),
        "candidate_bands": nested._band_payload(candidate_bands),
        "candidate_week_bands": nested._band_payload(candidate_week_bands),
        "hierarchy_bands": nested._band_payload(hierarchy_bands),
        "hierarchy_week_bands": nested._band_payload(hierarchy_week_bands),
        "confirmatory_bands": nested._band_payload(confirmatory_bands),
        "confirmatory_week_bands": nested._band_payload(confirmatory_week_bands),
        "risk_bands": nested._band_payload(risk_bands),
        "risk_week_bands": nested._band_payload(risk_week_bands),
        "scorecards": scorecards,
        "hierarchy": hierarchy,
        "score_profile_contract": nested.SCORE_PROFILE_CONTRACT,
        "outer_oof_row_count": int(len(oof)),
        "outer_fold_count_by_side": {side: 4 for side in normalized_sides},
        "simultaneous_family_sides": list(normalized_sides),
        "permissions": {
            "final_policy_frozen": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }


def verify_formal_report_reproduction(
    *,
    oof: pd.DataFrame,
    report: Mapping[str, Any],
    side: str,
    draws: int = 99_999,
) -> dict[str, Any]:
    config = nested.NestedOofConfig(sides=(side,), simultaneous_draws=draws)
    rebuilt = _statistical_payload(
        oof,
        sides=(side,),
        stability=report["stability"],
        config=config,
    )
    mismatches = [
        field for field in FORMAL_REPORT_FIELDS if rebuilt.get(field) != report.get(field)
    ]
    if mismatches:
        raise OwnerBuyE3CloseoutError(
            f"{side} formal report was not exactly reproduced: {mismatches}"
        )
    return {
        "side": side,
        "reproduced_fields": list(FORMAL_REPORT_FIELDS),
        "source_report_canonical_sha256": canonical_sha256(report),
        "recomputed_statistics_sha256": canonical_sha256(
            {field: rebuilt[field] for field in FORMAL_REPORT_FIELDS}
        ),
        "status": "passed_exact_daily_row_statistical_reproduction",
    }


def build_owner_decision(
    *,
    formal_buy_report: Mapping[str, Any],
    joint_report: Mapping[str, Any],
    source_bindings: Mapping[str, Any],
) -> dict[str, Any]:
    buy_key = f"BUY:{OWNER_CANDIDATE}"
    matched_hypothesis = "successor:BUY:E3_HIGHER_ORDER_BOOLEAN-ACTION_MATCHED"
    formal_scorecard = formal_buy_report["scorecards"][buy_key]
    hard_gates = formal_scorecard.get("hard_gates")
    hard_gate_failures = (
        list(hard_gates.get("failures", ())) if isinstance(hard_gates, Mapping) else []
    )
    formal_hierarchy = formal_buy_report["hierarchy"]["steps"]["BUY"]
    if (
        not hard_gate_failures
        or hard_gates.get("passed") is not False
        or formal_scorecard.get("promotion_status") != "development_failed_family_closed"
        or all(step["passed"] for step in formal_hierarchy)
    ):
        raise OwnerBuyE3CloseoutError(
            "owner override requires preserved hierarchy and hard-gate failures"
        )
    if matched_hypothesis not in joint_report["confirmatory_bands"]["bands"]:
        raise OwnerBuyE3CloseoutError("BUY E3 matched-control contrast is missing")
    payload: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.owner_decision.v1",
        "identity": IDENTITY,
        "status": "owner_override_recorded_artifact_not_yet_frozen",
        "selected_side": "BUY",
        "selected_learning_algorithm": OWNER_CANDIDATE,
        "research_supported": False,
        "owner_risk_accepted": True,
        "outcome_informed_owner_override": True,
        "formal_closeout_mutated": False,
        "formal_hierarchy_passed": False,
        "formal_hard_gates_passed": False,
        "formal_hard_gate_failures": hard_gate_failures,
        "formal_promotion_status": formal_scorecard["promotion_status"],
        "formal_hierarchy_steps": formal_hierarchy,
        "matched_control_hypothesis": matched_hypothesis,
        "matched_control_joint_band": joint_report["confirmatory_bands"]["bands"][
            matched_hypothesis
        ],
        "evidence_boundary": {
            "panel_role": "Development",
            "learning_algorithm_oof_only": True,
            "exact_final_artifact_oof_available": False,
            "old_oof_estimate_applies_to_exact_owner_artifact": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "new_economic_arm_run": False,
        },
        "next_admitted_operation": ("one_full_development_refit_of_the_frozen_buy_e3_algorithm"),
        "forbidden_shortcuts": [
            "select_best_outer_fold",
            "merge_outer_fold_rules",
            "manually_delete_literals",
            "substitute_e2_for_live_convenience",
        ],
        "source_bindings": dict(source_bindings),
        "permissions": {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    payload["canonical_owner_decision_sha256"] = document_sha256(
        payload, "canonical_owner_decision_sha256"
    )
    return payload


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
    )


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> str:
    data = _json_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return hashlib.sha256(data).hexdigest()


def _atomic_parquet(path: Path, frame: pd.DataFrame) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        frame.to_parquet(temporary, index=False)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return file_sha256(path)


def run_closeout(
    *,
    buy_result_path: Path,
    buy_report_path: Path,
    buy_rows_path: Path,
    sell_result_path: Path,
    sell_report_path: Path,
    sell_cache_root: Path,
    output_dir: Path,
) -> dict[str, Any]:
    _require_file_sha(buy_result_path, EXPECTED_BUY_RESULT_FILE_SHA256, label="BUY result")
    _require_file_sha(buy_report_path, EXPECTED_BUY_REPORT_FILE_SHA256, label="BUY report")
    _require_file_sha(buy_rows_path, EXPECTED_BUY_ROWS_FILE_SHA256, label="BUY OOF rows")
    _require_file_sha(sell_result_path, EXPECTED_SELL_RESULT_FILE_SHA256, label="SELL result")
    _require_file_sha(sell_report_path, EXPECTED_SELL_REPORT_FILE_SHA256, label="SELL report")
    buy_result = _load_json(buy_result_path, label="BUY component result")
    buy_report = _load_json(buy_report_path, label="BUY nested OOF report")
    sell_result = _load_json(sell_result_path, label="SELL component result")
    sell_report = _load_json(sell_report_path, label="SELL nested OOF report")
    if (
        buy_result.get("canonical_component_result_sha256") != EXPECTED_BUY_RESULT_CANONICAL_SHA256
        or document_sha256(buy_result, "canonical_component_result_sha256")
        != EXPECTED_BUY_RESULT_CANONICAL_SHA256
        or buy_result.get("execution_manifest_sha256") != EXPECTED_BUY_EXECUTION_MANIFEST_SHA256
        or buy_result.get("nested_oof_report") != buy_report
    ):
        raise OwnerBuyE3CloseoutError("BUY immutable result binding drifted")
    if (
        sell_result.get("canonical_result_sha256") != EXPECTED_SELL_RESULT_CANONICAL_SHA256
        or document_sha256(sell_result, "canonical_result_sha256")
        != EXPECTED_SELL_RESULT_CANONICAL_SHA256
        or sell_result.get("execution_manifest_sha256") != EXPECTED_SELL_EXECUTION_MANIFEST_SHA256
        or sell_result.get("nested_oof_report") != sell_report
    ):
        raise OwnerBuyE3CloseoutError("SELL immutable result binding drifted")
    try:
        component_closeout.validate_nested_report(buy_report, expected_side="BUY")
        component_closeout.validate_nested_report(sell_report, expected_side="SELL")
    except component_closeout.FormalComponentCloseoutError as exc:
        raise OwnerBuyE3CloseoutError(str(exc)) from exc
    buy_rows = pd.read_parquet(buy_rows_path)
    if len(buy_rows) != 260 or set(buy_rows["side"].astype(str)) != {"BUY"}:
        raise OwnerBuyE3CloseoutError("BUY OOF row census drifted")
    sell_rows, sell_cache_receipt = reconstruct_sell_outer_oof_rows(
        result=sell_result,
        cache_root=sell_cache_root,
    )
    buy_reproduction = verify_formal_report_reproduction(
        oof=buy_rows, report=buy_report, side="BUY"
    )
    sell_reproduction = verify_formal_report_reproduction(
        oof=sell_rows, report=sell_report, side="SELL"
    )
    normalized_buy, normalized_sell = normalize_sparse_count_columns((buy_rows, sell_rows))
    joint_rows = replay_adapter._concat_sequential_day_results((normalized_buy, normalized_sell))
    stability = {**buy_report["stability"], **sell_report["stability"]}
    joint_report = _statistical_payload(
        joint_rows,
        sides=("BUY", "SELL"),
        stability=stability,
        config=nested.NestedOofConfig(),
    )
    source_bindings = {
        "BUY": {
            "result_canonical_sha256": EXPECTED_BUY_RESULT_CANONICAL_SHA256,
            "result_file_sha256": EXPECTED_BUY_RESULT_FILE_SHA256,
            "report_file_sha256": EXPECTED_BUY_REPORT_FILE_SHA256,
            "outer_oof_rows_file_sha256": EXPECTED_BUY_ROWS_FILE_SHA256,
            "execution_manifest_sha256": EXPECTED_BUY_EXECUTION_MANIFEST_SHA256,
            "formal_report_reproduction": buy_reproduction,
        },
        "SELL": {
            "result_canonical_sha256": EXPECTED_SELL_RESULT_CANONICAL_SHA256,
            "result_file_sha256": EXPECTED_SELL_RESULT_FILE_SHA256,
            "report_file_sha256": EXPECTED_SELL_REPORT_FILE_SHA256,
            "execution_manifest_sha256": EXPECTED_SELL_EXECUTION_MANIFEST_SHA256,
            "formal_report_reproduction": sell_reproduction,
            **sell_cache_receipt,
        },
    }
    owner_decision = build_owner_decision(
        formal_buy_report=buy_report,
        joint_report=joint_report,
        source_bindings=source_bindings,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "joint_oof_report.json": _atomic_json(output_dir / "joint_oof_report.json", joint_report),
        "owner_decision.json": _atomic_json(output_dir / "owner_decision.json", owner_decision),
        "sell_outer_oof_rows.parquet": _atomic_parquet(
            output_dir / "sell_outer_oof_rows.parquet", sell_rows
        ),
        "joint_outer_oof_rows.parquet": _atomic_parquet(
            output_dir / "joint_outer_oof_rows.parquet", joint_rows
        ),
    }
    manifest: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.closeout_manifest.v1",
        "identity": IDENTITY,
        "status": "formal_statistics_rebuilt_owner_override_recorded",
        "source_bindings": source_bindings,
        "files": {
            name: {
                "sha256": sha,
                "size_bytes": (output_dir / name).stat().st_size,
                "mode": format((output_dir / name).stat().st_mode & 0o777, "04o"),
            }
            for name, sha in sorted(files.items())
        },
        "row_frames": {
            "SELL": {
                "rows": len(sell_rows),
                "frame_sha256": replay_adapter._frame_sha256(sell_rows),
            },
            "joint": {
                "rows": len(joint_rows),
                "frame_sha256": replay_adapter._frame_sha256(joint_rows),
            },
        },
        "permissions": {
            "research_authorized": False,
            "action_authorized": False,
            "live_authorized": False,
            "validation_read": False,
            "sealed_holdout_read": False,
        },
    }
    manifest["canonical_manifest_sha256"] = document_sha256(manifest, "canonical_manifest_sha256")
    _atomic_json(output_dir / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--buy-result", type=Path, required=True)
    parser.add_argument("--buy-report", type=Path, required=True)
    parser.add_argument("--buy-rows", type=Path, required=True)
    parser.add_argument("--sell-result", type=Path, required=True)
    parser.add_argument("--sell-report", type=Path, required=True)
    parser.add_argument("--sell-cache-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    manifest = run_closeout(
        buy_result_path=args.buy_result.expanduser().resolve(),
        buy_report_path=args.buy_report.expanduser().resolve(),
        buy_rows_path=args.buy_rows.expanduser().resolve(),
        sell_result_path=args.sell_result.expanduser().resolve(),
        sell_report_path=args.sell_report.expanduser().resolve(),
        sell_cache_root=args.sell_cache_root.expanduser().resolve(),
        output_dir=args.output_dir.expanduser().resolve(),
    )
    print(json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
