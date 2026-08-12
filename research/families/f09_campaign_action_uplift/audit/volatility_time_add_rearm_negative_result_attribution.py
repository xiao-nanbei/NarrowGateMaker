#!/usr/bin/env python3
"""Post-result diagnostic attribution for the closed variance-time action.

This module is evidence-only. It reads the frozen 40-day randomized lineage
panel, decomposes campaign-terminal value around the randomization time, and
reports pre-treatment volatility/inventory slices. It cannot rank, promote,
retune, or authorize an action.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit.randomized_action_contrast import (
    randomized_itt_contrast,
)
from research.families.f09_campaign_action_uplift.audit.volatility_time_add_rearm_feasibility_v2 import (
    attach_reference_start_rates,
)
from strategy.fill_cooldown import (
    LINEAGE_CANDIDATE_ACTION,
    LINEAGE_CONTROL_ACTION,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "volatility_time_add_rearm_negative_result_attribution.v1"
FAMILY_ID = "volatility_time_add_rearm_negative_result_attribution_v1"
SIDES = ("BUY", "SELL")
BRIDGE_METRICS = (
    "reward",
    "terminal_campaign_pnl",
    "pre_assignment_campaign_pnl",
    "post_lineage_continuation_value",
    "decision_to_campaign_terminal_value",
    "fill_value",
    "campaign_cost_avoidance",
    "queue_cost_avoidance",
)
STRATIFIED_METRICS = (
    "reward",
    "decision_to_campaign_terminal_value",
    "post_lineage_continuation_value",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_identity(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    actual = sha256_file(path)
    if actual != str(expected_sha256):
        raise ValueError(
            f"{label} hash mismatch: expected={expected_sha256}, actual={actual}"
        )


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_spec(path: Path) -> dict[str, Any]:
    spec = _load_json(path)
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected negative-result attribution schema")
    if spec.get("mode") != "diagnostic_only_post_result":
        raise ValueError("attribution must remain diagnostic-only and post-result")
    if bool(spec.get("pre_registered", True)):
        raise ValueError("post-result attribution must not claim preregistration")
    permissions = spec.get("permissions") or {}
    forbidden = [key for key, value in permissions.items() if bool(value)]
    if forbidden:
        raise ValueError(
            "negative-result attribution exceeds authority: " + ", ".join(forbidden)
        )
    return spec


def validate_inputs(spec: Mapping[str, Any]) -> tuple[dict[str, Any], pd.DataFrame]:
    identities = spec["input_identity"]
    for label, identity in identities.items():
        if not isinstance(identity, Mapping) or "path" not in identity:
            continue
        require_identity(
            Path(str(identity["path"])),
            str(identity["sha256"]),
            label,
        )

    report = _load_json(Path(identities["randomized_report"]["path"]))
    if report.get("decision") != "close_variance_time_add_rearm_action_on_development":
        raise ValueError("predecessor randomized family is not frozen closed")
    if report.get("panel_role") != "development":
        raise ValueError("attribution input is not the frozen Development panel")
    if list(report.get("development_days", ())) != list(spec["development_days"]):
        raise ValueError("Development denominator differs from diagnostic contract")
    if any(bool(value) for value in (report.get("permissions") or {}).values()):
        raise ValueError("predecessor report unexpectedly grants authority")

    panel = pd.read_parquet(identities["randomized_lineage_panel"]["path"])
    required = {
        "day",
        "decision_id",
        "lineage_id",
        "decision_ts_ms",
        "side",
        "action",
        "behavior_propensity",
        "decision_inventory",
        "decision_mtm",
        "campaign_start_pnl",
        "lineage_terminal_mtm",
        "reward",
        "terminal_campaign_pnl",
        "fill_value",
        "campaign_cost",
        "queue_cost",
        "reward_identity_error",
        "actual_final_action_change_count",
    }
    missing = sorted(required - set(panel.columns))
    if missing:
        raise ValueError("randomized lineage panel is missing: " + ", ".join(missing))
    if panel.empty or panel.duplicated(["day", "decision_id"]).any():
        raise ValueError("randomized lineage panel is empty or duplicated")
    if panel.duplicated(["day", "lineage_id"]).any():
        raise ValueError("lineage identifiers are not unique within UTC day")
    if set(panel["day"].astype(str)) != set(spec["development_days"]):
        raise ValueError("attribution read outside frozen Development")
    if set(panel["action"].astype(str)) != {
        LINEAGE_CONTROL_ACTION,
        LINEAGE_CANDIDATE_ACTION,
    }:
        raise ValueError("randomized action support changed")
    propensity = pd.to_numeric(panel["behavior_propensity"], errors="coerce")
    if propensity.isna().any() or not np.allclose(propensity, 0.5, atol=1e-12):
        raise ValueError("behavior propensity differs from exact 0.5")
    return report, panel


def validate_bbo_sources(spec: Mapping[str, Any]) -> None:
    manifest_identity = spec["input_identity"]["randomized_market_source_manifest"]
    rows = _load_json(Path(manifest_identity["path"]))
    observed_days: set[str] = set()
    for row in rows:
        if row.get("source_type") != "normalized_bbo":
            continue
        path = Path(str(row["path"]))
        require_identity(path, str(row["sha256"]), f"normalized BBO {row['source_day']}")
        observed_days.add(str(row["source_day"]))
    needed = set(spec["development_days"])
    needed.update(
        (pd.Timestamp(day) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        for day in spec["development_days"]
    )
    missing = sorted(needed - observed_days)
    if missing:
        raise ValueError(f"frozen source manifest lacks BBO days: {missing}")


def build_attribution_panel(
    panel: pd.DataFrame,
    spec: Mapping[str, Any],
) -> pd.DataFrame:
    out = panel.copy()
    numeric_columns = (
        "decision_ts_ms",
        "decision_inventory",
        "decision_mtm",
        "campaign_start_pnl",
        "lineage_terminal_mtm",
        "reward",
        "terminal_campaign_pnl",
        "fill_value",
        "campaign_cost",
        "queue_cost",
        "reward_identity_error",
    )
    for column in numeric_columns:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    if not np.isfinite(out[list(numeric_columns)].to_numpy(dtype=float)).all():
        raise ValueError("attribution panel contains non-finite accounting values")
    if float(out["reward_identity_error"].abs().max()) > 1e-9:
        raise ValueError("frozen lineage reward identity is invalid")

    out["pre_assignment_campaign_pnl"] = (
        out["decision_mtm"] - out["campaign_start_pnl"]
    )
    out["post_lineage_continuation_value"] = (
        out["terminal_campaign_pnl"]
        - out["pre_assignment_campaign_pnl"]
        - out["reward"]
    )
    out["decision_to_campaign_terminal_value"] = (
        out["terminal_campaign_pnl"] - out["pre_assignment_campaign_pnl"]
    )
    out["campaign_cost_avoidance"] = -out["campaign_cost"]
    out["queue_cost_avoidance"] = -out["queue_cost"]
    terminal_error = (
        out["terminal_campaign_pnl"]
        - out["pre_assignment_campaign_pnl"]
        - out["reward"]
        - out["post_lineage_continuation_value"]
    )
    reward_error = (
        out["reward"]
        - out["fill_value"]
        - out["campaign_cost_avoidance"]
        - out["queue_cost_avoidance"]
    )
    if float(terminal_error.abs().max()) > 1e-9:
        raise ValueError("campaign-terminal bridge identity failed")
    if float(reward_error.abs().max()) > 1e-9:
        raise ValueError("lineage reward component identity failed")

    order_size = float(spec["diagnostic_strata"]["order_size_btc"])
    out["inventory_units_at_assignment"] = (
        out["decision_inventory"].abs() / order_size
    )
    inventory_edges = [-math.inf, 1.000001, 2.000001, 3.000001, math.inf]
    out["inventory_layer"] = pd.cut(
        out["inventory_units_at_assignment"],
        inventory_edges,
        labels=["1", "2", "3", "4+"],
    ).astype("string")

    variance_input = out[
        ["day", "side", "decision_ts_ms", "decision_id"]
    ].rename(
        columns={"decision_ts_ms": "episode_start_ts_ms", "decision_id": "episode_id"}
    )
    clock = spec["variance_clock"]
    variance = attach_reference_start_rates(
        variance_input,
        Path(str(spec["input_identity"]["normalized_l2_root"])),
        rolling_window_s=int(clock["rolling_window_s"]),
        max_bbo_source_age_ms=int(clock["max_bbo_source_age_ms"]),
        max_abs_return_bps_1s=float(clock["max_abs_return_bps_1s"]),
        max_feature_age_ms=int(clock["max_feature_age_ms"]),
        ready_delay_ms=int(clock["feature_ready_delay_ms"]),
    )
    variance = variance[
        [
            "episode_id",
            "start_variance_rate_bps2_per_s",
            "start_variance_valid",
            "start_feature_ready_ts_ms",
        ]
    ]
    out = out.merge(
        variance,
        left_on="decision_id",
        right_on="episode_id",
        how="left",
        validate="one_to_one",
    )
    reference = {
        str(side): float(value)
        for side, value in clock["reference_rate_bps2_per_s"].items()
    }
    out["variance_ratio_to_side_reference"] = [
        float(rate) / reference[str(side)] if bool(valid) else math.nan
        for rate, side, valid in zip(
            out["start_variance_rate_bps2_per_s"],
            out["side"],
            out["start_variance_valid"],
            strict=True,
        )
    ]
    cutpoints = [float(value) for value in spec["diagnostic_strata"]["variance_ratio_cutpoints"]]
    if cutpoints != [0.5, 1.0, 2.0]:
        raise ValueError("diagnostic variance cutpoints changed")
    out["variance_regime"] = pd.cut(
        out["variance_ratio_to_side_reference"],
        [-math.inf, *cutpoints, math.inf],
        labels=["<0.5x", "0.5-1x", "1-2x", ">=2x"],
    ).astype("string")
    return out


def _contrast_row(
    frame: pd.DataFrame,
    *,
    side: str,
    metric: str,
    grouping: str,
    stratum: str,
    spec: Mapping[str, Any],
    seed_offset: int,
) -> dict[str, Any]:
    actions = frame["action"].astype(str)
    action_rows = {
        action: int(actions.eq(action).sum())
        for action in (LINEAGE_CONTROL_ACTION, LINEAGE_CANDIDATE_ACTION)
    }
    if not all(action_rows.values()) or frame["day"].nunique() < 2:
        return {
            "side": side,
            "grouping": grouping,
            "stratum": stratum,
            "metric": metric,
            "rows": int(len(frame)),
            "days": int(frame["day"].nunique()),
            "control_rows": action_rows[LINEAGE_CONTROL_ACTION],
            "candidate_rows": action_rows[LINEAGE_CANDIDATE_ACTION],
            "supported": False,
        }
    bootstrap = spec["bootstrap"]
    result = randomized_itt_contrast(
        frame,
        outcome=metric,
        baseline_action=LINEAGE_CONTROL_ACTION,
        candidate_action=LINEAGE_CANDIDATE_ACTION,
        bootstrap_trials=int(bootstrap["draws"]),
        random_seed=int(bootstrap["seed"]) + int(seed_offset),
    )
    return {
        "side": side,
        "grouping": grouping,
        "stratum": stratum,
        "metric": metric,
        "rows": int(len(frame)),
        "days": int(frame["day"].nunique()),
        "control_rows": action_rows[LINEAGE_CONTROL_ACTION],
        "candidate_rows": action_rows[LINEAGE_CANDIDATE_ACTION],
        "supported": True,
        "uplift": float(result["uplift"]),
        "lcb95": float(result["interval"]["p025"]),
        "ucb95": float(result["interval"]["p975"]),
        "daily_positive_rate": float(result["daily_positive_rate"]),
        "pointwise_interval_only": True,
    }


def build_contrasts(
    panel: pd.DataFrame,
    spec: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    bridge_rows: list[dict[str, Any]] = []
    stratified_rows: list[dict[str, Any]] = []
    offset = 0
    for side in SIDES:
        scoped = panel.loc[panel["side"].astype(str).eq(side)]
        for metric in BRIDGE_METRICS:
            bridge_rows.append(
                _contrast_row(
                    scoped,
                    side=side,
                    metric=metric,
                    grouping="all_lineages",
                    stratum="all",
                    spec=spec,
                    seed_offset=offset,
                )
            )
            offset += 1
        for grouping in ("inventory_layer", "variance_regime"):
            for stratum, group in scoped.groupby(grouping, observed=True, sort=False):
                for metric in STRATIFIED_METRICS:
                    stratified_rows.append(
                        _contrast_row(
                            group,
                            side=side,
                            metric=metric,
                            grouping=grouping,
                            stratum=str(stratum),
                            spec=spec,
                            seed_offset=offset,
                        )
                    )
                    offset += 1
    return pd.DataFrame(bridge_rows), pd.DataFrame(stratified_rows)


def exact_path_match_coverage(
    panel: pd.DataFrame,
    spec: Mapping[str, Any],
) -> dict[str, Any]:
    identity = spec["input_identity"]["full_path_mechanical_panel"]
    mechanics = pd.read_parquet(identity["path"])
    keys = ["day", "side", "lineage_fill_ts_ms"]
    if mechanics.duplicated(keys).any():
        raise ValueError("full-path mechanics has duplicate fill-time keys")
    matched = panel.merge(
        mechanics[keys],
        left_on=["day", "side", "decision_ts_ms"],
        right_on=keys,
        how="left",
        indicator=True,
        validate="many_to_one",
    )
    rows = int(matched["_merge"].eq("both").sum())
    return {
        "matched_rows": rows,
        "total_rows": int(len(panel)),
        "match_rate": float(rows / max(len(panel), 1)),
        "transport_allowed": False,
        "reason": (
            "baseline full-path mechanics covers only a post-randomization subset; "
            "earlier/later and consecutive-unit labels are not joined to outcomes"
        ),
    }


def _row(frame: pd.DataFrame, side: str, metric: str) -> dict[str, Any]:
    selected = frame.loc[
        frame["side"].eq(side) & frame["metric"].eq(metric)
    ]
    if len(selected) != 1:
        raise ValueError(f"missing bridge row: {side} {metric}")
    return selected.iloc[0].to_dict()


def render_markdown(
    report: Mapping[str, Any],
    bridge: pd.DataFrame,
) -> str:
    def fmt(row: Mapping[str, Any]) -> str:
        return (
            f"{float(row['uplift']):+.6f} "
            f"[{float(row['lcb95']):+.6f}, {float(row['ucb95']):+.6f}]"
        )

    lines = [
        "# Volatility-Time Add-Rearm Negative-Result Attribution v1",
        "",
        "Last materially modified: 2026-07-29",
        "",
        "## Authority",
        "",
        "This is a post-result, Development-only diagnostic. It was not preregistered,",
        "does not change the frozen randomized-v1 decision, and cannot rank, retune,",
        "open Validation/holdout, authorize another action, or authorize live.",
        "",
        "## Value bridge",
        "",
        "All contrasts are candidate minus control with complete UTC-day clustering.",
        "The exact accounting bridge is:",
        "",
        "`campaign terminal = pre-assignment campaign PnL + lineage reward + post-lineage continuation`.",
        "",
        "| Side | Component | Uplift and pointwise 95% interval (USDC/lineage) |",
        "|---|---|---:|",
    ]
    labels = (
        ("pre_assignment_campaign_pnl", "Already accrued before assignment"),
        ("reward", "Decision-to-lineage-terminal reward"),
        ("post_lineage_continuation_value", "After-lineage campaign continuation"),
        ("decision_to_campaign_terminal_value", "Decision-to-campaign-terminal value"),
        ("terminal_campaign_pnl", "Original campaign-terminal metric"),
    )
    for side in SIDES:
        for metric, label in labels:
            lines.append(f"| {side} | {label} | {fmt(_row(bridge, side, metric))} |")
    lines.extend(
        [
            "",
            "BUY's positive original campaign-terminal lower bound is not a clean",
            "post-assignment effect. A material part was already present before the",
            "lineage assignment. After removing that carried PnL, the BUY",
            "decision-to-campaign-terminal interval crosses zero.",
            "",
            "SELL's small positive lineage reward does not persist. The estimated",
            "post-lineage continuation is negative and the decision-to-campaign-terminal",
            "point estimate reverses sign, with its interval crossing zero.",
            "",
            "## Diagnostic strata",
            "",
            f"Causal 60-second variance was available for {report['variance_state']['valid_rows']:,}",
            f"of {report['variance_state']['total_rows']:,} rows",
            f"({100.0 * report['variance_state']['valid_rate']:.2f}%). Variance bins are",
            "fixed multiples of the previously frozen side reference rate. Inventory",
            "bins use absolute inventory at assignment in 0.001 BTC units; they are not",
            "consecutive fill-unit labels.",
            "",
            "The machine-readable stratified table contains pointwise day-clustered",
            "intervals. These are multiple post-result comparisons and are only",
            "hypothesis-generating. A positive cell cannot register a state-conditioned",
            "variance-time action on this consumed Development panel.",
            "",
            "## Unsupported requested slices",
            "",
            f"Only {report['path_transport']['matched_rows']:,} /",
            f"{report['path_transport']['total_rows']:,} randomized lineages",
            f"({100.0 * report['path_transport']['match_rate']:.2f}%) exactly match the",
            "older baseline mechanics path. Assignment changes future fills and lineage",
            "creation, so that subset cannot transport earlier/later rearm or consecutive",
            "fill-unit labels to the randomized outcome panel. Those slices remain",
            "unsupported rather than being filled with baseline-path labels.",
            "",
            "## Decision",
            "",
            "`diagnostic_complete_randomized_v1_closure_unchanged`",
            "",
            "- `validation_read=false`",
            "- `sealed_holdout_read=false`",
            "- `ranking_score=null`",
            "- `action_experiment_authorized=false`",
            "- `live_deployment_authorized=false`",
            "",
            "The reusable asset is the full-path lineage-randomized infrastructure. The",
            "specific whole-clock replacement remains a high-quality negative action result.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def existing_storage_probe(path: Path) -> Path:
    probe = path
    while not probe.exists():
        parent = probe.parent
        if parent == probe:
            raise FileNotFoundError(f"no existing storage ancestor for {path}")
        probe = parent
    return probe


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"diagnostic output already exists: {output}")
    spec = load_spec(spec_path)
    report, raw_panel = validate_inputs(spec)
    validate_bbo_sources(spec)
    free_gib = shutil.disk_usage(existing_storage_probe(output.parent)).free / 1024**3
    if free_gib < float(spec["storage_gate"]["minimum_free_gib"]):
        raise RuntimeError(f"diagnostic storage gate failed: {free_gib:.2f} GiB free")

    panel = build_attribution_panel(raw_panel, spec)
    bridge, strata = build_contrasts(panel, spec)
    path_transport = exact_path_match_coverage(panel, spec)
    variance_valid = panel["start_variance_valid"].astype(bool)
    permissions = {
        "validation_read": False,
        "sealed_holdout_read": False,
        "ranking_or_selection_authorized": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    machine_report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "diagnostic_only_post_result",
        "pre_registered": False,
        "predecessor_decision": report["decision"],
        "decision": "diagnostic_complete_randomized_v1_closure_unchanged",
        "development_days": list(spec["development_days"]),
        "rows": int(len(panel)),
        "variance_state": {
            "valid_rows": int(variance_valid.sum()),
            "total_rows": int(len(panel)),
            "valid_rate": float(variance_valid.mean()),
            "policy_selection_allowed": False,
        },
        "path_transport": path_transport,
        "identities": {
            "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
            "predecessor_report": spec["input_identity"]["randomized_report"],
            "predecessor_panel": spec["input_identity"]["randomized_lineage_panel"],
        },
        "limitations": {
            "strata_are_post_result_multiple_comparisons": True,
            "pointwise_intervals_are_not_selection_bands": True,
            "earlier_later_rearm_outcome_attribution_supported": False,
            "consecutive_fill_unit_outcome_attribution_supported": False,
            "inventory_layer_is_not_consecutive_fill_units": True,
            "campaign_terminal_contains_pre_assignment_value": True,
        },
        "permissions": permissions,
        "ranking_score": None,
        "artifacts": {},
    }
    output.mkdir(parents=True)
    panel_path = output / "attribution_panel.parquet"
    bridge_path = output / "value_bridge.csv"
    strata_path = output / "diagnostic_strata.csv"
    report_json_path = output / "report.json"
    report_md_path = output / "report.md"
    manifest_path = output / "manifest.json"
    panel.to_parquet(panel_path, index=False)
    bridge.to_csv(bridge_path, index=False)
    strata.to_csv(strata_path, index=False)
    report_md_path.write_text(render_markdown(machine_report, bridge), encoding="utf-8")
    artifacts = {
        "attribution_panel": panel_path,
        "value_bridge": bridge_path,
        "diagnostic_strata": strata_path,
        "human_report": report_md_path,
    }
    machine_report["artifacts"] = {
        name: {"path": str(path), "sha256": sha256_file(path)}
        for name, path in artifacts.items()
    }
    report_json_path.write_text(
        json.dumps(machine_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest = {
        "schema_version": "volatility_time_add_rearm_negative_result_attribution_manifest.v1",
        "family_id": FAMILY_ID,
        "decision": machine_report["decision"],
        "report": {
            "path": str(report_json_path),
            "sha256": sha256_file(report_json_path),
        },
        "permissions": permissions,
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"decision": machine_report["decision"], "report": str(report_json_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
