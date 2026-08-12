#!/usr/bin/env python3
"""Owner-risk continuation for the frozen conditional-P3 price action.

The predecessor C++ screen had a positive point estimate but failed its PnL
lower-bound and campaign-q10 gates. This successor changes no action parameter.
It runs the authoritative Python lifecycle and permanently preserves the
predecessor failure in its identity. Economic success only unlocks production
engineering gates; it does not itself grant live authority.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import math
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from research.families.f09_campaign_action_uplift.audit import (
    causal_v12_toxicity_conditional_p3_reach_gate as predecessor,
)


ROOT = predecessor.ROOT
DATA_ROOT = predecessor.DATA_ROOT
IDENTITY = (
    "causal_v12_toxicity_outward_16tick_conditional_p3_reach_gate_owner_v1_1"
)
SCHEMA_VERSION = (
    "narrowgate_causal_v12_toxicity_conditional_p3_reach_gate.owner.v1_1"
)
CREATED_DATE = "2026-08-04"
OUTPUT_ROOT = (
    DATA_ROOT
    / "reports/causal_v12_toxicity_outward_16tick_conditional_p3_"
    "reach_gate_owner_v1_1_20260804"
)
SPEC_PATH = (
    ROOT
    / "research/families/f09_campaign_action_uplift/docs/"
    "causal_v12_toxicity_outward_16tick_conditional_p3_reach_gate_"
    "owner_v1_1_spec_20260804.json"
)
PREDECESSOR_OUTPUT = predecessor.OUTPUT_ROOT
PREDECESSOR_SPEC = predecessor.SPEC_PATH
PREDECESSOR_REPORT = PREDECESSOR_OUTPUT / "cpp_screen_report.json"
PREDECESSOR_MANIFEST = PREDECESSOR_OUTPUT / "cpp_screen_manifest.json"
PREDECESSOR_SNAPSHOT = (
    PREDECESSOR_OUTPUT / "cpp_screen_implementation_snapshot.tar.gz"
)
PREDECESSOR_SNAPSHOT_SHA256 = (
    "4db18d45c7bded367450ef621c6b65c747f0ee729637e9015efb7ccfe18bf27b"
)


def _validate_predecessor() -> dict[str, Any]:
    predecessor.require_file(PREDECESSOR_SPEC)
    report = json.loads(
        predecessor.require_file(PREDECESSOR_REPORT).read_text(encoding="utf-8")
    )
    manifest = json.loads(
        predecessor.require_file(PREDECESSOR_MANIFEST).read_text(encoding="utf-8")
    )
    predecessor.require_file(
        PREDECESSOR_SNAPSHOT,
        PREDECESSOR_SNAPSHOT_SHA256,
    )
    if bool(report.get("all_cpp_screen_gates_passed")):
        raise ValueError("owner successor requires the predecessor failure to remain recorded")
    failures = {
        key for key, passed in dict(report.get("hard_gates") or {}).items() if not passed
    }
    expected = {
        "terminal_mtm_pnl_lcb_positive",
        "campaign_q10_noninferior",
    }
    if failures != expected:
        raise ValueError(f"predecessor failure set drift: {sorted(failures)}")
    if str(report.get("decision")) != "close_before_authoritative_python_and_do_not_deploy":
        raise ValueError("predecessor closure decision drift")
    if predecessor.sha256_file(PREDECESSOR_SPEC) != str(
        manifest["spec_sha256"]
    ):
        raise ValueError("predecessor Spec/manifest identity drift")
    if predecessor.sha256_file(PREDECESSOR_REPORT) != str(
        manifest["report"]["sha256"]
    ):
        raise ValueError("predecessor report/manifest identity drift")
    return report


def _owner_implementation_paths() -> tuple[Path, ...]:
    return (
        Path(__file__).resolve(),
        ROOT / "models/backtest_tick.py",
        ROOT / "strategy/conditional_p3_reach_gate.py",
        ROOT / "tests/test_conditional_p3_reach_gate_policy.py",
        ROOT / "tests/test_conditional_p3_reach_gate_cpp.py",
        ROOT / "models/audit/action_bound_full_path_promotion.py",
    )


def freeze_owner_spec() -> None:
    from models.audit.experiment_scorecard import score_profile_contract

    if SPEC_PATH.exists():
        raise FileExistsError(f"frozen owner Spec already exists: {SPEC_PATH}")
    if (OUTPUT_ROOT / "python_full_path_report.json").exists():
        raise RuntimeError("Python economic outcomes already exist")
    cpp_report = _validate_predecessor()
    baseline = predecessor.validate_current_baseline()
    days, grade_a, grade_b = predecessor.load_panel()
    predecessor_spec = json.loads(PREDECESSOR_SPEC.read_text(encoding="utf-8"))
    required_artifacts = {
        "predecessor_spec": PREDECESSOR_SPEC,
        "predecessor_cpp_report": PREDECESSOR_REPORT,
        "predecessor_cpp_manifest": PREDECESSOR_MANIFEST,
        "predecessor_implementation_snapshot": PREDECESSOR_SNAPSHOT,
        "gate_matrix_manifest": PREDECESSOR_OUTPUT / "gate_matrix_manifest.parquet",
        "toxicity_p90_schedule": PREDECESSOR_OUTPUT / "toxicity_p90_schedule.parquet",
        "p3_simultaneous_band": PREDECESSOR_OUTPUT / "p3_reach_simultaneous_band.json",
    }
    payload: dict[str, Any] = {
        "schema_version": f"{SCHEMA_VERSION}.spec",
        "identity": IDENTITY,
        "created_date": CREATED_DATE,
        "frozen_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "frozen_before_authoritative_python_economic_outcomes",
        "research_family": "F09_campaign_action_uplift",
        "promotion_route": "owner_risk_accepted_promotion",
        "owner_label_permanent": True,
        "owner_continuation_reason": (
            "the frozen C++ full-path screen had a positive point estimate; the owner "
            "requested an authoritative Python full-path continuation without tuning"
        ),
        "accepted_predecessor_failures": {
            "terminal_mtm_pnl_lcb_positive": False,
            "campaign_q10_noninferior": False,
            "cpp_sum_delta_usdc": float(
                cpp_report["paired_terminal_mtm_pnl"]["sum_delta_usdc"]
            ),
            "cpp_ci95_usdc_per_day": list(
                cpp_report["paired_terminal_mtm_pnl"]
                ["ci95_day_cluster_bootstrap_usdc_per_day"]
            ),
            "original_closure_preserved": True,
        },
        "baseline": {
            "pointer": predecessor._artifact(predecessor.BASELINE_POINTER),
            "identity": predecessor._artifact(baseline["identity_path"]),
            "config": predecessor._artifact(baseline["config_path"]),
            "baseline_id": str(baseline["pointer"]["baseline_id"]),
            "ml_enabled": True,
            "q90_action_enabled": False,
            "buy_fill_selection_action_enabled": False,
        },
        "action": {
            "predecessor_identity": str(predecessor_spec["identity"]),
            "candidate_source_identity": (
                "causal_v12_side_specific_toxicity_past_only_p90"
            ),
            "candidate_action": "exposure_quote_outward_16_ticks",
            "intervention_axis": "quote_price",
            "outward_ticks": predecessor.OUTWARD_TICKS,
            "toxicity_quantile": predecessor.TOXICITY_QUANTILE,
            "reach_change_min": predecessor.REACH_CHANGE_MIN,
            "reach_change_max": predecessor.REACH_CHANGE_MAX,
            "sides": list(predecessor.SIDES),
            "roles": list(predecessor.ROLES),
            "reducing_quote_changed": False,
            "lifecycle_ownership_changed": False,
            "requote_schedule_changed": False,
            "cancel_policy_changed": False,
            "cooldown_changed": False,
            "parameter_search_after_cpp_outcome": False,
        },
        "development_panel": {
            "days": days,
            "grade_a_days": sorted(grade_a),
            "grade_b_days": sorted(grade_b),
            "day_count": len(days),
            "validation_read": False,
            "sealed_holdout_read": False,
        },
        "authoritative_engine": "python_full_order_lifecycle",
        "economic_gates": {
            "terminal_mtm_pnl_day_cluster_lcb_strictly_positive": True,
            "closed_campaign_value_day_cluster_lcb_strictly_positive": True,
            "fill_retention_range": [0.80, 1.20],
            "candidate_price_change_rate_range": [0.02, 0.20],
            "campaign_q10_noninferior": True,
            "campaign_cvar10_noninferior": True,
            "campaign_mae_noninferior": True,
            "maximum_inventory_noninferior": True,
            "inventory_time_noninferior": True,
            "maximum_drawdown_noninferior": True,
            "minimum_buy_price_changes": 100,
            "minimum_sell_price_changes": 100,
            "bootstrap_draws": predecessor.ECONOMIC_BOOTSTRAP_DRAWS,
            "bootstrap_seed": predecessor.ECONOMIC_BOOTSTRAP_SEED,
        },
        "production_gates_after_economic_pass": {
            "python_cpp_policy_semantics_parity": True,
            "live_and_replay_share_policy_function": True,
            "config_model_and_artifact_hash_match": True,
            "live_preflight": True,
            "automatic_rollback": True,
        },
        "score_profile": score_profile_contract("action_execution_selective_v2"),
        "artifact_identities": {
            name: predecessor._artifact(path)
            for name, path in required_artifacts.items()
        },
        "implementation_identities": {
            str(path.relative_to(ROOT)): predecessor._artifact(path)
            for path in _owner_implementation_paths()
        },
        "permissions_at_freeze": {
            "authoritative_economic_authority": False,
            "action_authority": False,
            "live_authority": False,
        },
    }
    payload["canonical_spec_identity_sha256"] = predecessor.canonical_sha256(payload)
    predecessor.atomic_json(SPEC_PATH, payload)


def load_owner_spec() -> dict[str, Any]:
    spec = json.loads(predecessor.require_file(SPEC_PATH).read_text(encoding="utf-8"))
    expected = str(spec.get("canonical_spec_identity_sha256", ""))
    content = dict(spec)
    content.pop("canonical_spec_identity_sha256", None)
    if expected != predecessor.canonical_sha256(content):
        raise ValueError("owner Spec canonical identity drift")
    for section in ("artifact_identities", "implementation_identities"):
        for artifact in spec[section].values():
            predecessor.require_file(Path(str(artifact["path"])), str(artifact["sha256"]))
    _validate_predecessor()
    return spec


def _python_day_task(payload: Mapping[str, Any]) -> dict[str, Any]:
    from models import backtest_tick as bt
    from research.families.f03_causal_13_head.audit.full_path_ml_ab import (
        _campaign_day_metrics,
        _side_trace_metrics,
        reconstruct_campaigns,
    )

    day = str(payload["day"])
    panel_role = str(payload["panel_role"])
    output_dir = Path(str(payload["output_dir"])).resolve()
    config_path = Path(str(payload["config_path"])).resolve()
    params_base = predecessor.build_params(day, config_path)
    params_base.update(
        {
            "trace_fills_max": predecessor.TRACE_FILLS_MAX,
            "trace_fills_window_s": 30.0,
            "trace_p3_reach_decisions_max": 0,
            "replay_purpose": "conditional_p3_reach_gate_owner_python_full_path",
            "replay_promotion_eligible": False,
        }
    )
    window = predecessor.load_window(day, params_base)
    if str(window.book_source_authority) != "native_formal_lifecycle":
        raise ValueError(f"{day} is not native_formal_lifecycle")
    ml_ts = np.ascontiguousarray(window.ml_data[0], dtype=np.int64)
    gate_status = predecessor._load_gate_matrix(
        day=day,
        expected_ts_ms=ml_ts,
        manifest_row=dict(payload["gate_manifest_row"]),
    )
    thresholds = {
        str(row["side"]): dict(row) for row in payload["threshold_rows"]
    }
    if set(thresholds) != set(predecessor.SIDES):
        raise ValueError(f"{day} threshold schedule lacks BUY/SELL")

    daily_rows: list[dict[str, Any]] = []
    campaign_rows: list[dict[str, Any]] = []
    for arm in predecessor.ARMS:
        params = dict(params_base)
        if arm == "candidate":
            params.update(
                {
                    "conditional_p3_reach_gate_enabled": True,
                    "conditional_p3_reach_gate_outward_ticks": predecessor.OUTWARD_TICKS,
                    "conditional_p3_reach_gate_grid_min_ticks": (
                        predecessor.P3_DISTANCE_MIN_TICKS
                    ),
                    "conditional_p3_reach_gate_buy_toxicity_threshold": (
                        float(thresholds["BUY"]["threshold"])
                        if bool(thresholds["BUY"]["ready"])
                        else 1.0
                    ),
                    "conditional_p3_reach_gate_sell_toxicity_threshold": (
                        float(thresholds["SELL"]["threshold"])
                        if bool(thresholds["SELL"]["ready"])
                        else 1.0
                    ),
                    "_conditional_p3_reach_gate_ts_ms": ml_ts,
                    "_conditional_p3_reach_gate_status": gate_status,
                }
            )
        else:
            params["conditional_p3_reach_gate_enabled"] = False
            params.pop("_conditional_p3_reach_gate_ts_ms", None)
            params.pop("_conditional_p3_reach_gate_status", None)

        started = time.perf_counter()
        result = bt._simulate_tick_with_engine(
            "python",
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            params,
            ml_data=window.ml_data,
            bbo_data=window.bbo_data,
            l2_data=window.l2_data,
            var_ti=window.var_ti,
            var_retsq=window.var_retsq,
        )
        fill_trace = list(result.get("_fill_trace") or [])
        if len(fill_trace) != int(result["fills_total"]):
            raise RuntimeError(
                f"{day} {arm} fill trace truncated: "
                f"{len(fill_trace)} != {result['fills_total']}"
            )
        campaigns = reconstruct_campaigns(
            fill_trace,
            day=day,
            panel_role=panel_role,
            arm=arm,
            terminal_mark_price=float(result["terminal_mark_price"]),
            order_size=float(params["order_size"]),
        )
        campaign_frame = pd.DataFrame(campaigns)
        campaign_metrics = _campaign_day_metrics(campaign_frame)
        accounting_error = (
            float(campaign_metrics["campaign_terminal_value_usdc"])
            - float(result["terminal_mtm_pnl"])
        )
        if abs(accounting_error) > 1e-6:
            raise RuntimeError(f"{day} {arm} campaign accounting mismatch: {accounting_error}")
        buy = _side_trace_metrics(fill_trace, "BUY")
        sell = _side_trace_metrics(fill_trace, "SELL")
        daily_rows.append(
            {
                "day": day,
                "panel_role": panel_role,
                "arm": arm,
                "source_authority": str(window.book_source_authority),
                "terminal_mtm_pnl_usdc": float(result["terminal_mtm_pnl"]),
                "fills_bid": int(result["fills_bid"]),
                "fills_ask": int(result["fills_ask"]),
                "fills_total": int(result["fills_total"]),
                "abs_inventory_time_btc_s": float(result["abs_inventory_time_s"]),
                "max_inventory_btc": float(result["max_inventory"]),
                "final_inventory_btc": float(result["final_inventory"]),
                "max_drawdown_usdc": float(result["max_drawdown"]),
                "campaign_mae_usdc": float(result["campaign_max_adverse_excursion"]),
                "buy_maker_value_30s_bps": float(buy["maker_value_30s_bps"]),
                "sell_maker_value_30s_bps": float(sell["maker_value_30s_bps"]),
                "campaign_accounting_error_usdc": float(accounting_error),
                "p3_eval_count": int(result["conditional_p3_reach_gate_eval_count"]),
                "p3_toxicity_trigger_count": int(
                    result["conditional_p3_reach_gate_toxicity_trigger_count"]
                ),
                "p3_supported_count": int(
                    result["conditional_p3_reach_gate_supported_count"]
                ),
                "p3_gate_pass_count": int(
                    result["conditional_p3_reach_gate_pass_count"]
                ),
                "p3_price_change_count": int(
                    result["conditional_p3_reach_gate_price_change_count"]
                ),
                "p3_buy_price_change_count": int(
                    result["conditional_p3_reach_gate_buy_price_change_count"]
                ),
                "p3_sell_price_change_count": int(
                    result["conditional_p3_reach_gate_sell_price_change_count"]
                ),
                "p3_spread_cap_noop_count": int(
                    result["conditional_p3_reach_gate_spread_cap_noop_count"]
                ),
                "runtime_s": time.perf_counter() - started,
                **campaign_metrics,
            }
        )
        campaign_rows.extend(campaigns)

    daily = pd.DataFrame(daily_rows)
    if int(daily.loc[daily["arm"].eq("control"), "p3_price_change_count"].iloc[0]) != 0:
        raise RuntimeError(f"{day} control unexpectedly changed a P3-gated quote")
    campaigns_frame = pd.DataFrame(campaign_rows)
    predecessor.atomic_parquet(output_dir / f"day={day}.daily.parquet", daily)
    predecessor.atomic_parquet(
        output_dir / f"day={day}.campaigns.parquet", campaigns_frame
    )
    return {"day": day, "daily": daily_rows}


def _closed_campaign_daily_bootstrap(campaigns: pd.DataFrame) -> dict[str, Any]:
    closed = campaigns.loc[campaigns["closed"].astype(bool)].copy()
    grouped = (
        closed.groupby(["day", "arm"], sort=True)["terminal_value_usdc"]
        .sum()
        .unstack(fill_value=0.0)
        .reindex(columns=list(predecessor.ARMS), fill_value=0.0)
    )
    days, _, _ = predecessor.load_panel()
    grouped = grouped.reindex(days, fill_value=0.0)
    delta = (
        grouped["candidate"].to_numpy(dtype=float)
        - grouped["control"].to_numpy(dtype=float)
    )
    return predecessor._bootstrap_paired_daily(delta)


def run_python(*, workers: int, only_days: list[str] | None = None) -> None:
    if workers not in {1, 2}:
        raise ValueError("workers must be 1 or 2")
    spec = load_owner_spec()
    baseline = predecessor.validate_current_baseline()
    days, grade_a, grade_b = predecessor.load_panel()
    selected = list(only_days or days)
    if any(day not in days for day in selected):
        raise ValueError("requested day is outside the frozen 40-day panel")
    free_gib = shutil.disk_usage(OUTPUT_ROOT.parent).free / (1024**3)
    if free_gib < 62.5:
        raise OSError(f"storage safety gate failed: only {free_gib:.2f} GiB free")

    matrix_manifest = pd.read_parquet(
        PREDECESSOR_OUTPUT / "gate_matrix_manifest.parquet"
    )
    matrix_rows = {
        str(row["day"]): dict(row) for row in matrix_manifest.to_dict("records")
    }
    schedule = pd.read_parquet(PREDECESSOR_OUTPUT / "toxicity_p90_schedule.parquet")
    threshold_rows = {
        day: schedule.loc[schedule["day"].astype(str).eq(day)].to_dict("records")
        for day in days
    }
    day_dir = OUTPUT_ROOT / "python_full_path_days"
    day_dir.mkdir(parents=True, exist_ok=True)
    tasks = [
        {
            "day": day,
            "panel_role": "grade_a" if day in grade_a else "grade_b",
            "output_dir": str(day_dir),
            "config_path": str(baseline["config_path"]),
            "gate_manifest_row": matrix_rows[day],
            "threshold_rows": threshold_rows[day],
        }
        for day in selected
        if not (day_dir / f"day={day}.daily.parquet").exists()
    ]
    completed = len(selected) - len(tasks)
    if workers == 1:
        for task in tasks:
            row = _python_day_task(task)
            completed += 1
            print(f"Python full path {completed}/{len(selected)} {row['day']}", flush=True)
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_python_day_task, task): task["day"] for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                row = future.result()
                completed += 1
                daily = {item["arm"]: item for item in row["daily"]}
                delta = (
                    daily["candidate"]["terminal_mtm_pnl_usdc"]
                    - daily["control"]["terminal_mtm_pnl_usdc"]
                )
                print(
                    f"Python full path {completed}/{len(selected)} {row['day']} "
                    f"delta={delta:+.6f}",
                    flush=True,
                )
    if selected != days:
        return

    daily = pd.concat(
        [pd.read_parquet(day_dir / f"day={day}.daily.parquet") for day in days],
        ignore_index=True,
    ).sort_values(["day", "arm"])
    campaigns = pd.concat(
        [pd.read_parquet(day_dir / f"day={day}.campaigns.parquet") for day in days],
        ignore_index=True,
    ).sort_values(["day", "arm", "campaign_index"])
    if len(daily) != 80 or daily["day"].nunique() != 40:
        raise RuntimeError("paired Python full-path denominator mismatch")
    wide = daily.pivot(index="day", columns="arm", values="terminal_mtm_pnl_usdc")
    delta = wide["candidate"].to_numpy(dtype=float) - wide["control"].to_numpy(dtype=float)
    pnl = predecessor._bootstrap_paired_daily(delta)
    closed_campaign_value = _closed_campaign_daily_bootstrap(campaigns)
    totals = daily.groupby("arm", sort=True).sum(numeric_only=True)
    fill_retention = float(
        totals.loc["candidate", "fills_total"]
        / max(float(totals.loc["control", "fills_total"]), 1.0)
    )
    inventory_time_ratio = float(
        totals.loc["candidate", "abs_inventory_time_btc_s"]
        / max(float(totals.loc["control", "abs_inventory_time_btc_s"]), 1e-12)
    )
    campaign_summary = {
        arm: predecessor._campaign_tail_summary(campaigns.loc[campaigns["arm"].eq(arm)])
        for arm in predecessor.ARMS
    }
    candidate = daily.loc[daily["arm"].eq("candidate")]
    control = daily.loc[daily["arm"].eq("control")]
    evaluations = int(candidate["p3_eval_count"].sum())
    changes = int(candidate["p3_price_change_count"].sum())
    action_rate = float(changes / max(evaluations, 1))
    gates = dict(spec["economic_gates"])
    fill_bounds = [float(value) for value in gates["fill_retention_range"]]
    rate_bounds = [
        float(value) for value in gates["candidate_price_change_rate_range"]
    ]
    hard_gates = {
        "terminal_mtm_pnl_lcb_positive": bool(
            pnl["ci95_day_cluster_bootstrap_usdc_per_day"][0] > 0.0
        ),
        "closed_campaign_value_lcb_positive": bool(
            closed_campaign_value["ci95_day_cluster_bootstrap_usdc_per_day"][0] > 0.0
        ),
        "fill_retention_within_bounds": bool(
            fill_bounds[0] <= fill_retention <= fill_bounds[1]
        ),
        "candidate_price_change_rate_within_bounds": bool(
            rate_bounds[0] <= action_rate <= rate_bounds[1]
        ),
        "campaign_q10_noninferior": bool(
            campaign_summary["candidate"]["q10_usdc"]
            >= campaign_summary["control"]["q10_usdc"]
        ),
        "campaign_cvar10_noninferior": bool(
            campaign_summary["candidate"]["cvar10_usdc"]
            >= campaign_summary["control"]["cvar10_usdc"]
        ),
        "campaign_mae_noninferior": bool(
            candidate["campaign_mae_usdc"].min()
            >= control["campaign_mae_usdc"].min()
        ),
        "maximum_inventory_noninferior": bool(
            candidate["max_inventory_btc"].max()
            <= control["max_inventory_btc"].max()
        ),
        "inventory_time_noninferior": bool(inventory_time_ratio <= 1.0),
        "maximum_drawdown_noninferior": bool(
            candidate["max_drawdown_usdc"].min()
            >= control["max_drawdown_usdc"].min()
        ),
        "buy_action_support": bool(
            int(candidate["p3_buy_price_change_count"].sum())
            >= int(gates["minimum_buy_price_changes"])
        ),
        "sell_action_support": bool(
            int(candidate["p3_sell_price_change_count"].sum())
            >= int(gates["minimum_sell_price_changes"])
        ),
        "campaign_accounting_identity": bool(
            daily["campaign_accounting_error_usdc"].abs().max() <= 1e-6
        ),
    }
    all_passed = all(hard_gates.values())
    predecessor.atomic_parquet(OUTPUT_ROOT / "python_full_path_daily.parquet", daily)
    predecessor.atomic_parquet(
        OUTPUT_ROOT / "python_full_path_campaigns.parquet", campaigns
    )
    report = {
        "schema_version": f"{SCHEMA_VERSION}.python_full_path",
        "identity": IDENTITY,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec": predecessor._artifact(SPEC_PATH),
        "promotion_route": "owner_risk_accepted_promotion",
        "predecessor_failure_preserved": True,
        "comparison": "candidate_minus_current_v9_control",
        "control_totals": {
            "terminal_mtm_pnl_usdc": float(
                totals.loc["control", "terminal_mtm_pnl_usdc"]
            ),
            "fills_total": int(totals.loc["control", "fills_total"]),
        },
        "candidate_totals": {
            "terminal_mtm_pnl_usdc": float(
                totals.loc["candidate", "terminal_mtm_pnl_usdc"]
            ),
            "fills_total": int(totals.loc["candidate", "fills_total"]),
        },
        "paired_terminal_mtm_pnl": pnl,
        "paired_closed_campaign_value": closed_campaign_value,
        "fill_retention": fill_retention,
        "inventory_time_ratio": inventory_time_ratio,
        "campaign_summary": campaign_summary,
        "mechanics": {
            "evaluations": evaluations,
            "toxicity_triggers": int(candidate["p3_toxicity_trigger_count"].sum()),
            "p3_supported": int(candidate["p3_supported_count"].sum()),
            "p3_gate_pass": int(candidate["p3_gate_pass_count"].sum()),
            "price_changes": changes,
            "price_change_rate": action_rate,
            "buy_price_changes": int(candidate["p3_buy_price_change_count"].sum()),
            "sell_price_changes": int(candidate["p3_sell_price_change_count"].sum()),
            "spread_cap_noops": int(candidate["p3_spread_cap_noop_count"].sum()),
        },
        "hard_gates": hard_gates,
        "all_authoritative_economic_gates_passed": all_passed,
        "decision": (
            "economic_pass_production_gates_still_locked"
            if all_passed
            else "owner_full_path_failed_no_live"
        ),
        "permissions": {
            "authoritative_economic_evidence_read": True,
            "production_engineering_eligible": all_passed,
            "action_authority": False,
            "live_authority": False,
        },
    }
    predecessor.atomic_json(OUTPUT_ROOT / "python_full_path_report.json", report)
    manifest = {
        "schema_version": f"{SCHEMA_VERSION}.python_full_path.manifest",
        "identity": IDENTITY,
        "spec_sha256": predecessor.sha256_file(SPEC_PATH),
        "report": predecessor._artifact(OUTPUT_ROOT / "python_full_path_report.json"),
        "daily": predecessor._artifact(OUTPUT_ROOT / "python_full_path_daily.parquet"),
        "campaigns": predecessor._artifact(
            OUTPUT_ROOT / "python_full_path_campaigns.parquet"
        ),
    }
    manifest["canonical_identity_sha256"] = predecessor.canonical_sha256(manifest)
    predecessor.atomic_json(OUTPUT_ROOT / "python_full_path_manifest.json", manifest)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("freeze-spec")
    run = subparsers.add_parser("run-python")
    run.add_argument("--workers", type=int, default=2)
    run.add_argument("--day", action="append", default=[])
    args = parser.parse_args()
    if args.command == "freeze-spec":
        freeze_owner_spec()
        return 0
    if args.command == "run-python":
        run_python(workers=int(args.workers), only_days=list(args.day) or None)
        return 0
    raise ValueError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
