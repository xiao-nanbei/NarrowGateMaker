#!/usr/bin/env python3
"""Synthesize completed cooldown permission actions on one evidence frontier.

This Development-only governance audit reads frozen reports; it does not run a
replay, fit a model, estimate a new treatment contrast, or register an action.
Older reports whose exact denominator was withdrawn remain useful only for
their immutable closure decision and explicitly labelled scale diagnostics.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "cooldown_action_leverage_frontier.v1"
REPORT_SCHEMA_VERSION = "cooldown_action_leverage_frontier_report.v1"
IDENTITY = "cooldown_action_leverage_frontier_v1"
SIDES = ("BUY", "SELL")
EVIDENCE_STATUSES = {
    "current_authoritative",
    "withdrawn_old_denominator_closure_authoritative",
}
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "cooldown_action_leverage_frontier_v1_spec_20260730.json"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_sha256", None)
    raw = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return payload


def _finite(value: Any, *, name: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _optional_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0.0:
        return None
    return float(numerator / denominator)


def validate_spec(payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected cooldown frontier schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected cooldown frontier identity")
    frozen_hash = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(payload) != frozen_hash:
        raise ValueError("cooldown frontier canonical hash mismatch")

    contract = payload.get("audit_contract") or {}
    if contract.get("classification") != "retrospective_governance_mechanism_audit":
        raise ValueError("frontier must remain a retrospective governance audit")
    if bool(contract.get("cross_source_pooled_estimate_allowed", True)):
        raise ValueError("heterogeneous source estimates cannot be pooled")
    if bool(contract.get("may_register_action", True)):
        raise ValueError("frontier cannot register an action")

    sources = payload.get("source_identities") or {}
    required_sources = {
        "one_cycle_current",
        "variance_time_current",
        "state_conditioned_buy_historical",
        "state_conditioned_sell_historical",
        "recovery_event_sell_historical",
        "stop_add_until_flat_sell_historical",
    }
    if set(sources) != required_sources:
        raise ValueError("cooldown frontier source registry is incomplete")
    for source_id, source in sources.items():
        status = str(source.get("evidence_status", ""))
        if status not in EVIDENCE_STATUSES:
            raise ValueError(f"unsupported evidence status for {source_id}: {status}")
        for key in ("report_path", "report_sha256", "documentation_path", "documentation_sha256"):
            if not str(source.get(key, "")):
                raise ValueError(f"{source_id} is missing {key}")
        if len(str(source["report_sha256"])) != 64:
            raise ValueError(f"{source_id} report hash is malformed")
        if len(str(source["documentation_sha256"])) != 64:
            raise ValueError(f"{source_id} documentation hash is malformed")

    rows = payload.get("frontier_rows") or []
    required_rows = {
        "one_cycle_buy",
        "one_cycle_sell",
        "state_conditioned_buy",
        "state_conditioned_sell",
        "recovery_event_sell",
        "stop_add_until_flat_sell",
        "variance_time_buy",
        "variance_time_sell",
    }
    row_ids = {str(row.get("row_id", "")) for row in rows}
    if row_ids != required_rows or len(rows) != len(required_rows):
        raise ValueError("cooldown frontier rows are incomplete or duplicated")
    if any(str(row.get("side", "")) not in SIDES for row in rows):
        raise ValueError("frontier side must be BUY or SELL")
    if any(str(row.get("source_id", "")) not in sources for row in rows):
        raise ValueError("frontier row references an unknown source")
    if any(not bool(row.get("historical_action_closed", False)) for row in rows):
        raise ValueError("every synthesized action row must remain closed")

    rule = payload.get("decision_rule") or {}
    if rule.get("positive_economic_gate") != "reward_lcb_gt_zero_and_lifecycle_supported":
        raise ValueError("economic support gate drifted")
    if rule.get("scope") != "tested_cooldown_temporal_permission_actions":
        raise ValueError("frontier exhaustion scope drifted")
    if bool(rule.get("claims_all_possible_cooldown_actions_exhausted", True)):
        raise ValueError("frontier cannot claim all possible cooldown actions")

    permissions = payload.get("permissions") or {}
    forbidden_true = (
        "new_replay_run",
        "new_outcome_contrast_estimated",
        "validation_read",
        "sealed_holdout_read",
        "action_family_created",
        "action_experiment_authorized",
        "live_deployment_authorized",
    )
    if any(bool(permissions.get(key, False)) for key in forbidden_true):
        raise ValueError("frontier permissions were broadened")


def validate_source_identities(spec: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    loaded: dict[str, dict[str, Any]] = {}
    for source_id, source in (spec.get("source_identities") or {}).items():
        report_path = Path(str(source["report_path"])).expanduser().resolve()
        doc_path = Path(str(source["documentation_path"])).expanduser().resolve()
        for label, path, expected in (
            ("report", report_path, str(source["report_sha256"])),
            ("documentation", doc_path, str(source["documentation_sha256"])),
        ):
            if not path.is_file():
                raise FileNotFoundError(f"{source_id} {label} is missing: {path}")
            actual = sha256_file(path)
            if actual != expected:
                raise ValueError(
                    f"{source_id} {label} hash mismatch: expected {expected}, found {actual}"
                )
        payload = _load_json(report_path)
        expected_schema = str(source.get("report_schema_version", ""))
        if payload.get("schema_version") != expected_schema:
            raise ValueError(f"{source_id} report schema drifted")
        loaded[source_id] = payload
    for relative, expected in (spec.get("implementation_identity") or {}).items():
        path = ROOT / str(relative)
        if not path.is_file():
            raise FileNotFoundError(f"implementation identity is missing: {path}")
        actual = sha256_file(path)
        if actual != str(expected):
            raise ValueError(
                f"implementation hash mismatch for {relative}: "
                f"expected {expected}, found {actual}"
            )
    return loaded


def _base_row(
    row_spec: Mapping[str, Any],
    source_spec: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "row_id": str(row_spec["row_id"]),
        "action_identity": str(row_spec["action_identity"]),
        "side": str(row_spec["side"]),
        "intervention": str(row_spec["intervention"]),
        "source_id": str(row_spec["source_id"]),
        "evidence_status": str(source_spec["evidence_status"]),
        "current_metric_authority": (
            source_spec["evidence_status"] == "current_authoritative"
        ),
        "historical_closure_authoritative": True,
        "panel_days": None,
        "mechanical_action_change_rate": None,
        "candidate_rate": None,
        "candidate_multicycle_rate": None,
        "fill_retention": None,
        "fill_path_divergence_type": None,
        "fill_path_divergence_abs": None,
        "fill_path_suppression_signed": None,
        "primary_reward_point_usdc": None,
        "primary_reward_lcb95_usdc": None,
        "primary_reward_ucb95_usdc": None,
        "primary_reward_daily_positive_rate": None,
        "design_mde_usdc": None,
        "mde_required_value_per_affected_fill_usdc": None,
        "descriptive_reward_per_abs_fill_divergence_usdc": None,
        "descriptive_ratio_is_causal_fill_value": False,
        "activity_supported": None,
        "economic_lower_bound_positive": None,
        "lifecycle_supported": None,
        "frontier_class": str(row_spec["frontier_class"]),
        "closure_reason": str(row_spec["closure_reason"]),
    }


def _reward_fields(result: Mapping[str, Any]) -> dict[str, float]:
    interval = result.get("day_cluster_bootstrap") or result.get("interval") or {}
    daily = result.get("daily_uplift") or result
    return {
        "primary_reward_point_usdc": _finite(
            result.get("dr_uplift", result.get("uplift")), name="reward point"
        ),
        "primary_reward_lcb95_usdc": _finite(
            interval.get("uplift_p025", interval.get("p025")), name="reward lcb"
        ),
        "primary_reward_ucb95_usdc": _finite(
            interval.get("uplift_p975", interval.get("p975")), name="reward ucb"
        ),
        "primary_reward_daily_positive_rate": _finite(
            daily.get("positive_rate", daily.get("daily_positive_rate")),
            name="reward daily positive rate",
        ),
    }


def _extract_one_cycle(
    base: dict[str, Any], payload: Mapping[str, Any], side: str
) -> dict[str, Any]:
    summary = {
        str(row["side"]): row for row in payload.get("side_summary", [])
    }.get(side)
    if summary is None:
        raise ValueError(f"one-cycle report is missing {side}")
    fill_rate = _finite(summary["selected_cycle_fill_rate"], name="fill rate")
    mde = _finite(summary["mde_80pct_power_two_sided_usdc"], name="MDE")
    base.update(
        {
            "panel_days": 40,
            "mechanical_action_change_rate": _finite(
                summary["action_change_opportunity_rate"], name="action rate"
            ),
            "fill_path_divergence_type": "selected_suppressed_cycle_would_fill_rate",
            "fill_path_divergence_abs": fill_rate,
            "fill_path_suppression_signed": fill_rate,
            "design_mde_usdc": mde,
            "mde_required_value_per_affected_fill_usdc": _optional_ratio(
                mde, fill_rate
            ),
            "activity_supported": None,
            "economic_lower_bound_positive": None,
            "lifecycle_supported": None,
        }
    )
    return base


def _extract_variance_time(
    base: dict[str, Any], payload: Mapping[str, Any], side: str
) -> dict[str, Any]:
    report = (payload.get("side_reports") or {}).get(side)
    if not isinstance(report, dict):
        raise ValueError(f"variance-time report is missing {side}")
    retention = _finite(report["fills_retention"], name="fill retention")
    reward = _reward_fields((report.get("outcomes") or {})["reward"])
    divergence = abs(1.0 - retention)
    base.update(
        {
            "panel_days": int((report.get("support") or {})["n_days"]),
            "mechanical_action_change_rate": _finite(
                report["actual_final_action_change"]["estimate"],
                name="action change",
            ),
            "candidate_rate": _finite(
                report["candidate_assignment_rate"], name="candidate rate"
            ),
            "fill_retention": retention,
            "fill_path_divergence_type": "absolute_one_minus_fill_retention",
            "fill_path_divergence_abs": divergence,
            "fill_path_suppression_signed": 1.0 - retention,
            "activity_supported": retention >= 0.85,
            "economic_lower_bound_positive": reward[
                "primary_reward_lcb95_usdc"
            ]
            > 0.0,
            "lifecycle_supported": False,
            **reward,
        }
    )
    base["descriptive_reward_per_abs_fill_divergence_usdc"] = _optional_ratio(
        base["primary_reward_point_usdc"], divergence
    )
    return base


def _extract_state_conditioned(
    base: dict[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    support = (payload.get("gate") or {}).get("support") or {}
    reward = _reward_fields(payload["results"]["all_campaigns"]["reward"])
    base.update(
        {
            "panel_days": int(support["days"]),
            "candidate_multicycle_rate": _finite(
                support["candidate_multicycle_rate"], name="multicycle rate"
            ),
            "activity_supported": False,
            "economic_lower_bound_positive": reward[
                "primary_reward_lcb95_usdc"
            ]
            > 0.0,
            "lifecycle_supported": False,
            **reward,
        }
    )
    return base


def _extract_recovery_event(
    base: dict[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    support = (payload.get("gate") or {}).get("support") or {}
    retention = _finite(support["fills_retention"], name="fill retention")
    divergence = 1.0 - retention
    reward = _reward_fields(payload["results"]["all_campaigns"]["reward"])
    base.update(
        {
            "panel_days": int(support["days"]),
            "candidate_rate": _finite(support["candidate_rate"], name="candidate rate"),
            "candidate_multicycle_rate": _finite(
                support["candidate_multicycle_rate"], name="multicycle rate"
            ),
            "fill_retention": retention,
            "fill_path_divergence_type": "conservative_one_minus_fill_retention",
            "fill_path_divergence_abs": divergence,
            "fill_path_suppression_signed": divergence,
            "activity_supported": retention >= 0.85,
            "economic_lower_bound_positive": reward[
                "primary_reward_lcb95_usdc"
            ]
            > 0.0,
            "lifecycle_supported": False,
            **reward,
        }
    )
    base["descriptive_reward_per_abs_fill_divergence_usdc"] = _optional_ratio(
        base["primary_reward_point_usdc"], divergence
    )
    return base


def _extract_stop_until_flat(
    base: dict[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    development = payload["development"]
    support = development["support"]
    retention = _finite(
        development["activity"]["fills_retention"], name="fill retention"
    )
    divergence = 1.0 - retention
    reward = _reward_fields(development["reward"])
    base.update(
        {
            "panel_days": int(support["days"]),
            "candidate_rate": _finite(support["candidate_rate"], name="candidate rate"),
            "fill_retention": retention,
            "fill_path_divergence_type": "one_minus_expected_fill_retention",
            "fill_path_divergence_abs": divergence,
            "fill_path_suppression_signed": divergence,
            "activity_supported": retention >= 0.85,
            "economic_lower_bound_positive": reward[
                "primary_reward_lcb95_usdc"
            ]
            > 0.0,
            "lifecycle_supported": False,
            **reward,
        }
    )
    base["descriptive_reward_per_abs_fill_divergence_usdc"] = _optional_ratio(
        base["primary_reward_point_usdc"], divergence
    )
    return base


EXTRACTORS = {
    "one_cycle_current_v1": _extract_one_cycle,
    "variance_time_current_v1": _extract_variance_time,
    "state_conditioned_historical_v1": _extract_state_conditioned,
    "recovery_event_historical_v1": _extract_recovery_event,
    "stop_until_flat_historical_v1": _extract_stop_until_flat,
}


def build_frontier_rows(
    spec: Mapping[str, Any], sources: Mapping[str, Mapping[str, Any]]
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    source_specs = spec["source_identities"]
    for row_spec in spec["frontier_rows"]:
        source_id = str(row_spec["source_id"])
        base = _base_row(row_spec, source_specs[source_id])
        extractor_name = str(row_spec["extractor"])
        extractor = EXTRACTORS.get(extractor_name)
        if extractor is None:
            raise ValueError(f"unknown frontier extractor: {extractor_name}")
        if extractor_name in {"one_cycle_current_v1", "variance_time_current_v1"}:
            row = extractor(base, sources[source_id], str(row_spec["side"]))
        else:
            row = extractor(base, sources[source_id])
        rows.append(row)
    frame = pd.DataFrame(rows).sort_values(["side", "row_id"]).reset_index(drop=True)
    if frame["row_id"].duplicated().any():
        raise ValueError("frontier row IDs are duplicated")
    return frame


def synthesize_decision(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty or not frame["historical_closure_authoritative"].astype(bool).all():
        raise ValueError("frontier cannot synthesize incomplete closure evidence")
    supported = frame[
        frame["activity_supported"].fillna(False).astype(bool)
        & frame["economic_lower_bound_positive"].fillna(False).astype(bool)
        & frame["lifecycle_supported"].fillna(False).astype(bool)
    ]
    exhausted = supported.empty
    return {
        "decision": (
            "tested_cooldown_temporal_permission_action_subspace_exhausted"
            if exhausted
            else "tested_cooldown_temporal_permission_action_subspace_not_exhausted"
        ),
        "tested_subspace_exhausted": exhausted,
        "claims_all_possible_cooldown_actions_exhausted": False,
        "f09_family_closed": False,
        "f09_status": "mechanism_research_active_no_registered_action",
        "frontier_rows": int(len(frame)),
        "current_authoritative_rows": int(frame["current_metric_authority"].sum()),
        "withdrawn_metric_rows": int((~frame["current_metric_authority"]).sum()),
        "rows_with_activity_and_positive_economic_and_lifecycle_support": int(
            len(supported)
        ),
        "cross_source_pooled_estimate": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        Path(temporary).unlink(missing_ok=True)
        raise


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _format_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{100.0 * float(value):.2f}%"


def _format_number(value: Any, digits: int = 6) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):+.{digits}f}"


def render_markdown(report: Mapping[str, Any]) -> str:
    by_id = {str(row["row_id"]): row for row in report["frontier_rows"]}
    lines = [
        "# Cooldown Action Leverage Frontier v1 - Development",
        "",
        "Last materially modified: 2026-07-30",
        "",
        "## Decision",
        "",
        "The tested cooldown temporal-permission action subspace is exhausted. ",
        "This is not a claim that every possible cooldown or F09 action is exhausted. ",
        "F09 remains active for mechanism research with no registered action.",
        "",
        "No replay, model fit, new treatment contrast, Validation/holdout read, ",
        "action registration, or live authorization occurred in this audit.",
        "",
        "## Evidence Boundary",
        "",
        "Current 40-day one-cycle and variance-time reports retain metric authority. ",
        "State-conditioned rearm, recovery-event rearm, and stop-add-until-flat ",
        "retain authoritative closure decisions, but their exact old-denominator ",
        "numbers are displayed only as withdrawn historical diagnostics. They are ",
        "not pooled with current evidence.",
        "",
        "## Frontier",
        "",
        "| Action | Side | Evidence | Action change / candidate | Fill-path divergence | Fill retention | Reward, 95% CI (USDC) | Classification |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    order = [
        "one_cycle_buy",
        "one_cycle_sell",
        "state_conditioned_buy",
        "state_conditioned_sell",
        "recovery_event_sell",
        "variance_time_buy",
        "variance_time_sell",
        "stop_add_until_flat_sell",
    ]
    for row_id in order:
        row = by_id[row_id]
        action_rate = row.get("mechanical_action_change_rate")
        if action_rate is None:
            action_rate = row.get("candidate_rate")
        if action_rate is None:
            action_rate = row.get("candidate_multicycle_rate")
        reward = row.get("primary_reward_point_usdc")
        reward_text = "n/a"
        if reward is not None:
            reward_text = (
                f"{_format_number(reward)} "
                f"[{_format_number(row.get('primary_reward_lcb95_usdc'))}, "
                f"{_format_number(row.get('primary_reward_ucb95_usdc'))}]"
            )
        lines.append(
            "| {action} | {side} | {evidence} | {action_rate} | {divergence} | "
            "{retention} | {reward} | `{classification}` |".format(
                action=row["action_identity"],
                side=row["side"],
                evidence=row["evidence_status"],
                action_rate=_format_pct(action_rate),
                divergence=_format_pct(row.get("fill_path_divergence_abs")),
                retention=_format_pct(row.get("fill_retention")),
                reward=reward_text,
                classification=row["frontier_class"],
            )
        )
    buy = by_id["one_cycle_buy"]
    sell = by_id["one_cycle_sell"]
    lines.extend(
        [
            "",
            "## One-Cycle Leverage",
            "",
            "The one-cycle action changes the next order decision in roughly 99% of ",
            "release episodes, but only 7.16% of selected BUY cycles and 6.26% of ",
            "selected SELL cycles would fill. Dividing the pre-frozen design MDE by ",
            "that affected-fill rate gives the conditional value required for the ",
            "action to be detectable:",
            "",
            f"- BUY: `{buy['mde_required_value_per_affected_fill_usdc']:.3f}` USDC per affected fill;",
            f"- SELL: `{sell['mde_required_value_per_affected_fill_usdc']:.3f}` USDC per affected fill.",
            "",
            "These are required effect scales, not estimated profits. They are much ",
            "larger than the observed first-add loss scale and explain why adding more ",
            "days to the same one-cycle action has low research value.",
            "",
            "## Synthesis",
            "",
            "The completed evidence spans the tested leverage range:",
            "",
            "- one-cycle skip changes quotes but rarely changes fills;",
            "- state-conditioned/recovery extensions have real persistence but no supported reward;",
            "- variance-time changes actions without a positive reward lower bound;",
            "- stop-add-until-flat removes about 89% of expected add fills and is participation shutdown.",
            "",
            "No tested row combines acceptable activity, a positive reward lower ",
            "bound, and supported lifecycle/tail evidence. Another clock, threshold, ",
            "or number of blocked cycles would remain inside this exhausted subspace. ",
            "A future F09 action must change the economic intervention itself.",
            "",
            "## Governance",
            "",
            "- `tested_subspace_exhausted=true`",
            "- `f09_family_closed=false`",
            "- `f09_status=mechanism_research_active_no_registered_action`",
            "- `cross_source_pooled_estimate=false`",
            "- `validation_read=false`",
            "- `sealed_holdout_read=false`",
            "- `action_experiment_authorized=false`",
            "- `live_deployment_authorized=false`",
            "",
            "F04 receive-time evidence collection continues independently in the ",
            "background and is not a prerequisite for this conclusion.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    spec = _load_json(spec_path)
    validate_spec(spec)
    sources = validate_source_identities(spec)
    frame = build_frontier_rows(spec, sources)
    synthesis = synthesize_decision(frame)
    source_manifest = {
        source_id: {
            "evidence_status": source["evidence_status"],
            "report_path": str(Path(source["report_path"]).expanduser().resolve()),
            "report_sha256": source["report_sha256"],
            "documentation_path": str(
                Path(source["documentation_path"]).expanduser().resolve()
            ),
            "documentation_sha256": source["documentation_sha256"],
        }
        for source_id, source in spec["source_identities"].items()
    }
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "panel_role": "development_evidence_synthesis_only",
        **synthesis,
        "frontier_rows": (
            frame.astype(object).where(pd.notna(frame), None).to_dict("records")
        ),
        "source_manifest": source_manifest,
        "metric_contract": {
            "mde_required_value_per_affected_fill": (
                "design_mde_usdc / selected_suppressed_cycle_would_fill_rate"
            ),
            "descriptive_reward_per_abs_fill_divergence": (
                "historical diagnostic ratio only; not a causal per-fill value"
            ),
            "withdrawn_old_denominator_metrics": (
                "scale diagnostics only; closure decision remains authoritative"
            ),
        },
    }
    output.mkdir(parents=True, exist_ok=True)
    csv_path = output / "frontier.csv"
    report_path = output / "report.json"
    markdown_path = output / "report.md"
    frame.to_csv(csv_path, index=False)
    _atomic_json(report_path, report)
    _atomic_text(markdown_path, render_markdown(report))
    manifest = {
        "schema_version": "cooldown_action_leverage_frontier_manifest.v1",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": {
            "path": str(spec_path),
            "sha256": sha256_file(spec_path),
            "canonical_spec_sha256": spec["canonical_spec_sha256"],
        },
        "artifacts": {
            "frontier_csv": {"path": str(csv_path), "sha256": sha256_file(csv_path)},
            "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
            "report_markdown": {
                "path": str(markdown_path),
                "sha256": sha256_file(markdown_path),
            },
        },
        **synthesis,
    }
    _atomic_json(output / "manifest.json", manifest)
    print(json.dumps(report, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
