#!/usr/bin/env python3
"""Execution-estimand successor for the frozen role-safe BER policy."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from data_paths import data_root
from models.audit import experiment_scorecard_v2
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_native_40day_full_path_ml_ab as native_runner,
)
from research.families.f09_campaign_action_uplift.audit import (
    ber_guard_role_safe_add_only_current_stack_owner as base,
)

ROOT = Path(__file__).resolve().parents[4]
DATA_ROOT = data_root(ROOT)
EXECUTION_ERRATA = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_"
    "execution_estimand_errata_v1_20260808.json"
)
DEFAULT_EXECUTION_AMENDMENT = ROOT / (
    "research/families/f09_campaign_action_uplift/docs/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_"
    "execution_amendment_v2_20260808.json"
)
DEFAULT_OUTPUT = DATA_ROOT / (
    "reports/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_20260808/"
    "development_execution_v2"
)


def _validate_successor(
    *,
    execution_amendment_path: Path,
    requested_days: Sequence[str] | None,
    verify_all_windows: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    spec, plan, amendment = base.validate_spec(
        execution_amendment_path=execution_amendment_path,
        requested_days=requested_days,
        verify_all_windows=verify_all_windows,
    )
    errata = base._load_json(EXECUTION_ERRATA, role="BER execution estimand errata")
    if (
        errata.get("schema_version")
        != "ber_guard_role_safe_add_only_current_stack_owner.v1."
        "execution_estimand_errata_v1"
        or errata.get("identity") != base.IDENTITY
        or errata.get("status")
        != "mechanics_informed_execution_successor_before_full_development_read"
    ):
        raise base.BerRoleSafeError("execution estimand errata drifted")
    binding = amendment.get("execution_estimand_errata") or {}
    base._validate_artifact(
        EXECUTION_ERRATA,
        str(binding.get("sha256", "")),
        role="BER execution estimand errata",
    )
    if amendment.get("status") != "execution_bound_full_development_read_after_mechanics":
        raise base.BerRoleSafeError("full Development amendment is not active")
    return spec, plan, amendment


def mechanics_summary(
    days: Sequence[str],
    *,
    output: Path,
    execution_amendment_path: Path,
) -> dict[str, Any]:
    summaries: list[dict[str, Any]] = []
    for day in days:
        manifest = base._admitted_day(
            output, day, execution_amendment_path=execution_amendment_path
        )
        if manifest is None:
            raise base.BerRoleSafeError(f"missing mechanics day: {day}")
        payload = base._load_json(
            Path(manifest["summary"]["path"]), role=f"{day} summary"
        )
        summaries.extend(payload["arms"])
    daily = pd.DataFrame(summaries)
    candidate = daily.loc[daily["arm"].eq(base.ARMS[1])]
    control = daily.loc[daily["arm"].eq(base.ARMS[0])]
    candidate_requotes = int(candidate["n_requotes"].sum())
    bid_changes = int(candidate["ber_role_safe_bid_change_count"].sum())
    ask_changes = int(candidate["ber_role_safe_ask_change_count"].sum())
    zero_invariants = {
        "python_cpp_fill_path_mismatch_count": int(
            daily["campaign_mae_cpp_python_fill_path_mismatch_count"].sum()
        ),
        "python_cpp_ber_state_mismatch_count": int(
            daily["cpp_python_ber_state_mismatch_count"].sum()
        ),
        "candidate_source_mismatch_count": int(
            candidate["ber_role_safe_source_mismatch_count"].sum()
        ),
        "candidate_cap_infeasible_count": int(
            candidate["ber_role_safe_cap_infeasible_count"].sum()
        ),
    }
    return {
        "identity": base.IDENTITY,
        "execution_estimand": "candidate_arm_canonical_side_decisions",
        "days": list(days),
        "control_requotes": int(control["n_requotes"].sum()),
        "candidate_requotes": candidate_requotes,
        "requote_count_delta": candidate_requotes - int(control["n_requotes"].sum()),
        "candidate_bid_change_count": bid_changes,
        "candidate_ask_change_count": ask_changes,
        "candidate_pair_change_count": int(
            candidate["ber_role_safe_pair_change_count"].sum()
        ),
        "candidate_effective_side_change_rate": (bid_changes + ask_changes)
        / max(2 * candidate_requotes, 1),
        "zero_invariants": zero_invariants,
        "structural_mechanics_passed": all(value == 0 for value in zero_invariants.values())
        and bid_changes > 0
        and ask_changes > 0,
        "action_authorized": False,
        "live_authorized": False,
    }


def _add_multi_level_columns(daily: pd.DataFrame, campaigns: pd.DataFrame) -> None:
    for side in ("LONG", "SHORT"):
        name = f"multi_level_{side.lower()}_terminal_value_usdc"
        values = (
            campaigns.loc[
                campaigns["multi_level"].astype(bool)
                & campaigns["inventory_side"].eq(side)
            ]
            .groupby(["day", "arm"])["terminal_value_usdc"]
            .sum()
        )
        daily[name] = [
            float(values.get((row.day, row.arm), 0.0)) for row in daily.itertuples()
        ]


def finalize(
    *,
    execution_amendment_path: Path = DEFAULT_EXECUTION_AMENDMENT,
    output: Path = DEFAULT_OUTPUT,
) -> dict[str, Any]:
    spec, _, _ = _validate_successor(
        execution_amendment_path=execution_amendment_path,
        requested_days=None,
    )
    days = list(spec["development_panel"]["days"])
    manifests = [
        base._admitted_day(
            output, day, execution_amendment_path=execution_amendment_path
        )
        for day in days
    ]
    if any(row is None for row in manifests):
        missing = [day for day, row in zip(days, manifests, strict=True) if row is None]
        raise base.BerRoleSafeError(f"cannot finalize; missing days: {missing}")
    summaries: list[dict[str, Any]] = []
    campaigns: list[pd.DataFrame] = []
    fills: list[pd.DataFrame] = []
    for manifest in manifests:
        assert manifest is not None
        payload = base._load_json(Path(manifest["summary"]["path"]), role="day summary")
        summaries.extend(payload["arms"])
        campaigns.append(pd.read_parquet(manifest["campaigns"]["path"]))
        fills.append(pd.read_parquet(manifest["fills"]["path"]))
    daily = pd.DataFrame(summaries)
    campaign_frame = pd.concat(campaigns, ignore_index=True)
    fill_frame = pd.concat(fills, ignore_index=True)
    _add_multi_level_columns(daily, campaign_frame)
    draws = int(spec["comparison"]["bootstrap_draws"])
    seed = int(spec["comparison"]["bootstrap_seed"])
    metric_defs = (
        ("closed_campaign_value", "closed_campaign_value_usdc", True),
        ("conditional_net_value", "terminal_mtm_pnl_usdc", True),
        ("full_panel_continuous_mtm", "terminal_mtm_pnl_usdc", True),
        ("negative_terminal_protection", "negative_campaign_terminal_value_usdc", True),
        ("q10_shortfall_protection", "campaign_q10_usdc", True),
        ("campaign_cvar10_protection", "campaign_cvar10_usdc", True),
        ("campaign_mae_avoidance", "campaign_mae_usdc", True),
        ("maximum_inventory_avoidance", "max_inventory_btc", False),
        ("inventory_time_avoidance", "abs_inventory_time_btc_s", False),
        ("buy_maker_value_protection_bps", "buy_maker_value_30s_bps", True),
        ("sell_maker_value_protection_bps", "sell_maker_value_30s_bps", True),
        ("repair_event", "repair_event_rate", True),
        ("repair_time_avoidance_s", "mean_closed_repair_time_s", False),
        ("multi_level_long_protection", "multi_level_long_terminal_value_usdc", True),
        ("multi_level_short_protection", "multi_level_short_terminal_value_usdc", True),
    )
    metrics = {
        name: base._paired(
            daily,
            column,
            candidate_minus_control=direction,
            seed=seed + index,
            draws=draws,
        )
        for index, (name, column, direction) in enumerate(metric_defs)
    }
    totals = daily.groupby("arm", sort=False).sum(numeric_only=True)
    fill_retention = float(
        totals.loc[base.ARMS[1], "fills_total"]
        / max(totals.loc[base.ARMS[0], "fills_total"], 1.0)
    )
    metrics["fills_retention"] = {"estimate": fill_retention}
    candidate = daily.loc[daily["arm"].eq(base.ARMS[1])]
    control = daily.loc[daily["arm"].eq(base.ARMS[0])]
    candidate_requotes = int(candidate["n_requotes"].sum())
    control_requotes = int(control["n_requotes"].sum())
    bid_changes = int(candidate["ber_role_safe_bid_change_count"].sum())
    ask_changes = int(candidate["ber_role_safe_ask_change_count"].sum())
    effective_change_rate = (bid_changes + ask_changes) / max(
        2 * candidate_requotes, 1
    )
    bid_days = int((candidate["ber_role_safe_bid_change_count"] > 0).sum())
    ask_days = int((candidate["ber_role_safe_ask_change_count"] > 0).sum())
    mechanics_spec = spec["mechanics_gates"]
    mechanics_gates = {
        "python_cpp_fill_path_parity": int(
            daily["campaign_mae_cpp_python_fill_path_mismatch_count"].sum()
        )
        == 0,
        "python_cpp_ber_state_parity": int(
            daily["cpp_python_ber_state_mismatch_count"].sum()
        )
        == 0,
        "role_source_parity": int(
            candidate["ber_role_safe_source_mismatch_count"].sum()
        )
        == 0,
        "cap_infeasible_zero": int(
            candidate["ber_role_safe_cap_infeasible_count"].sum()
        )
        == 0,
        "effective_change_rate_supported": float(
            mechanics_spec["candidate_pair_change_rate_minimum"]
        )
        <= effective_change_rate
        <= float(mechanics_spec["candidate_pair_change_rate_maximum"]),
        "buy_effective_change_count_supported": bid_changes
        >= int(mechanics_spec["minimum_effective_changes_per_side"]),
        "sell_effective_change_count_supported": ask_changes
        >= int(mechanics_spec["minimum_effective_changes_per_side"]),
        "buy_effective_change_days_supported": bid_days
        >= int(mechanics_spec["minimum_effective_change_days_per_side"]),
        "sell_effective_change_days_supported": ask_days
        >= int(mechanics_spec["minimum_effective_change_days_per_side"]),
    }
    effective_rows = int(campaign_frame.groupby("arm").size().min())
    last_candidate = candidate.sort_values("day").iloc[-1]
    final_inventory = float(last_candidate["final_inventory_btc"])
    final_mark = float(last_candidate["terminal_mark_price_usdc_per_btc"])
    evidence = {
        "schema_version": experiment_scorecard_v2.CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": base.IDENTITY,
        "family_id": "F09_campaign_action_uplift",
        "panel_role": "development",
        "input_identity": {
            "spec_sha256": native_runner._sha256_file(base.SPEC),
            "execution_estimand_errata_sha256": native_runner._sha256_file(
                EXECUTION_ERRATA
            ),
            "execution_amendment_sha256": native_runner._sha256_file(
                execution_amendment_path
            ),
        },
        "score_profile_contract": experiment_scorecard_v2.score_profile_contract(
            "action_defense_v2"
        ),
        "validity_failures": [
            "daily_fresh_start_is_not_continuous_live_promotion_authority"
        ],
        "family_gate_failures": []
        if all(mechanics_gates.values())
        else ["role_safe_ber_mechanics_gate_failed"],
        "metrics": metrics,
        "n_rows": effective_rows,
        "n_days": len(days),
        "effective_sample_size": float(effective_rows),
        "minimum_behavior_propensity": 0.5,
        "unsupported_mass": 0.0,
        "overlap_violations": 0,
        "candidate_rate": effective_change_rate,
        "invariant_violations": [],
        "continuous_path_accounting": {
            "schema_version": experiment_scorecard_v2.CONTINUOUS_PATH_SCHEMA_VERSION,
            "utc_day_role": "bootstrap_cluster_only",
            "cash_carried_across_utc_days": False,
            "inventory_carried_across_utc_days": False,
            "campaign_state_carried_across_utc_days": False,
            "panel_final_inventory_mtm_included": True,
            "forced_day_end_liquidations": 0,
            "day_end_state_resets": len(days) - 1,
            "day_end_campaign_terminals": 0,
            "daily_pnl_sum_usdc": metrics["conditional_net_value"]["sum_delta"],
            "continuous_panel_pnl_usdc": metrics["conditional_net_value"][
                "sum_delta"
            ],
            "daily_accounting_identity_max_abs_error_usdc": float(
                daily["campaign_accounting_error_usdc"].abs().max()
            ),
            "panel_final_inventory_btc": final_inventory,
            "panel_final_mark_price_usdc_per_btc": final_mark,
            "panel_final_inventory_mtm_usdc": final_inventory * final_mark,
        },
    }
    raw_scorecard = experiment_scorecard_v2.score_canonical_evidence(
        evidence, profile_id="action_defense_v2"
    )
    gates = spec["noncompensable_economic_gates"]
    fill_range = list(gates["fill_retention_range"])
    economic_gates = {
        "terminal_mtm_lcb_positive": metrics["conditional_net_value"][
            "lower_bound"
        ]
        > 0.0,
        "closed_campaign_lcb_positive": metrics["closed_campaign_value"][
            "lower_bound"
        ]
        > 0.0,
        "daily_positive_rate_pass": metrics["conditional_net_value"][
            "daily_positive_rate"
        ]
        >= float(gates["daily_positive_rate_minimum"]),
        "negative_terminal_lcb_nonnegative": metrics[
            "negative_terminal_protection"
        ]["lower_bound"]
        >= float(gates["negative_terminal_protection_lcb_minimum"]),
        "campaign_q10_lcb_nonnegative": metrics["q10_shortfall_protection"][
            "lower_bound"
        ]
        >= float(gates["campaign_q10_delta_lcb_minimum"]),
        "campaign_cvar10_lcb_nonnegative": metrics[
            "campaign_cvar10_protection"
        ]["lower_bound"]
        >= float(gates["campaign_cvar10_delta_lcb_minimum"]),
        "campaign_mae_lcb_nonnegative": metrics["campaign_mae_avoidance"][
            "lower_bound"
        ]
        >= float(gates["campaign_mae_avoidance_lcb_minimum"]),
        "maximum_inventory_lcb_nonnegative": metrics[
            "maximum_inventory_avoidance"
        ]["lower_bound"]
        >= float(gates["maximum_inventory_avoidance_lcb_minimum"]),
        "inventory_time_lcb_nonnegative": metrics["inventory_time_avoidance"][
            "lower_bound"
        ]
        >= float(gates["inventory_time_avoidance_lcb_minimum"]),
        "buy_maker_value_lcb_pass": metrics["buy_maker_value_protection_bps"][
            "lower_bound"
        ]
        >= float(gates["buy_maker_value_delta_lcb_minimum_bps"]),
        "sell_maker_value_lcb_pass": metrics[
            "sell_maker_value_protection_bps"
        ]["lower_bound"]
        >= float(gates["sell_maker_value_delta_lcb_minimum_bps"]),
        "fill_retention_owner_range": float(fill_range[0])
        <= fill_retention
        <= float(fill_range[1]),
        "campaign_accounting_parity": float(
            daily["campaign_accounting_error_usdc"].abs().max()
        )
        <= float(gates["campaign_accounting_max_abs_error_usdc"]),
    }
    screen_passed = all(mechanics_gates.values()) and all(economic_gates.values())
    decision = (
        "advance_to_restart_aware_continuous_owner_confirmation"
        if screen_passed
        else "close_role_safe_ber_candidate_on_development"
    )
    report = {
        "schema_version": f"{base.SCHEMA_VERSION}.execution_estimand_v1_1.report",
        "identity": base.IDENTITY,
        "execution_estimand_errata_sha256": native_runner._sha256_file(
            EXECUTION_ERRATA
        ),
        "decision": decision,
        "comparison": "role_safe_add_only_minus_global_all_roles_ber",
        "days": days,
        "totals": {
            arm: {
                "terminal_mtm_pnl_usdc": float(
                    totals.loc[arm, "terminal_mtm_pnl_usdc"]
                ),
                "closed_campaign_value_usdc": float(
                    totals.loc[arm, "closed_campaign_value_usdc"]
                ),
                "fills_bid": int(totals.loc[arm, "fills_bid"]),
                "fills_ask": int(totals.loc[arm, "fills_ask"]),
                "fills_total": int(totals.loc[arm, "fills_total"]),
                "multi_level_long_terminal_value_usdc": float(
                    totals.loc[arm, "multi_level_long_terminal_value_usdc"]
                ),
                "multi_level_short_terminal_value_usdc": float(
                    totals.loc[arm, "multi_level_short_terminal_value_usdc"]
                ),
            }
            for arm in base.ARMS
        },
        "mechanics": {
            "control_requotes": control_requotes,
            "candidate_requotes": candidate_requotes,
            "requote_count_delta": candidate_requotes - control_requotes,
            "candidate_effective_side_change_rate": effective_change_rate,
            "candidate_bid_change_count": bid_changes,
            "candidate_ask_change_count": ask_changes,
            "candidate_bid_change_days": bid_days,
            "candidate_ask_change_days": ask_days,
            "candidate_pair_change_count": int(
                candidate["ber_role_safe_pair_change_count"].sum()
            ),
            "candidate_role_safe_decision_count": int(
                candidate["ber_role_safe_decision_count"].sum()
            ),
            "candidate_buy_add_count": int(
                candidate["ber_role_safe_buy_add_count"].sum()
            ),
            "candidate_sell_add_count": int(
                candidate["ber_role_safe_sell_add_count"].sum()
            ),
            "candidate_flat_bypass_count": int(
                candidate["ber_role_safe_flat_bypass_count"].sum()
            ),
            "candidate_mixed_fail_closed_count": int(
                candidate["ber_role_safe_mixed_fail_closed_count"].sum()
            ),
            "candidate_cap_collision_count": int(
                candidate["ber_role_safe_cap_collision_count"].sum()
            ),
        },
        "metrics": metrics,
        "mechanics_gates": mechanics_gates,
        "economic_gates": economic_gates,
        "development_screen_passed": screen_passed,
        "raw_action_defense_v2_scorecard": raw_scorecard,
        "daily_fresh_start_not_live_authority": True,
        "continuous_confirmation_required_before_config_change": True,
        "owner_risk_accepted_route": True,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
        "ranking_score": None,
    }
    output.mkdir(parents=True, exist_ok=True)
    artifacts = {
        "daily": output / "daily.parquet",
        "campaigns": output / "campaigns.parquet",
        "fills": output / "fills.parquet",
        "scorecard": output / "action-defense-v2-scorecard.json",
        "report": output / "report.json",
    }
    daily.to_parquet(artifacts["daily"], index=False, compression="zstd")
    campaign_frame.to_parquet(artifacts["campaigns"], index=False, compression="zstd")
    fill_frame.to_parquet(artifacts["fills"], index=False, compression="zstd")
    base._atomic_json(artifacts["scorecard"], raw_scorecard)
    base._atomic_json(artifacts["report"], report)
    manifest = {
        "schema_version": f"{base.SCHEMA_VERSION}.execution_estimand_v1_1.panel",
        "identity": base.IDENTITY,
        "spec_sha256": native_runner._sha256_file(base.SPEC),
        "execution_estimand_errata_sha256": native_runner._sha256_file(
            EXECUTION_ERRATA
        ),
        "execution_amendment_sha256": native_runner._sha256_file(
            execution_amendment_path
        ),
        "artifacts": {
            name: {"path": str(path), "sha256": native_runner._sha256_file(path)}
            for name, path in artifacts.items()
        },
    }
    manifest_path = output / "panel-manifest.json"
    base._atomic_json(manifest_path, manifest)
    base._atomic_text(
        output / base.PANEL_SUCCESS,
        native_runner._sha256_file(manifest_path) + "\n",
    )
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("preflight", "run", "mechanics", "finalize", "all")
    )
    parser.add_argument("--execution-amendment", type=Path, default=DEFAULT_EXECUTION_AMENDMENT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--days", nargs="*")
    args = parser.parse_args(argv)
    spec = base._load_json(base.SPEC, role="frozen role-safe BER spec")
    days = list(args.days or spec["development_panel"]["days"])
    _validate_successor(
        execution_amendment_path=args.execution_amendment,
        requested_days=days,
    )
    storage = base._storage_gate(args.output)
    if args.command == "preflight":
        result: Any = {
            "identity": base.IDENTITY,
            "execution_estimand": "candidate_arm_canonical_side_decisions",
            "status": "preflight_passed",
            "days": days,
            "storage": storage,
            "action_authorized": False,
            "live_authorized": False,
        }
    elif args.command == "mechanics":
        result = mechanics_summary(
            days,
            output=args.output,
            execution_amendment_path=args.execution_amendment,
        )
    else:
        result = {}
        if args.command in {"run", "all"}:
            workers = max(1, int(args.workers))
            if workers == 1:
                rows = [
                    base.execute_day(
                        day,
                        execution_amendment_path=args.execution_amendment,
                        output=args.output,
                    )
                    for day in days
                ]
            else:
                with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as pool:
                    futures = {
                        pool.submit(
                            base.execute_day,
                            day,
                            execution_amendment_path=args.execution_amendment,
                            output=args.output,
                        ): day
                        for day in days
                    }
                    rows = []
                    for future in concurrent.futures.as_completed(futures):
                        row = future.result()
                        rows.append(row)
                        print(f"completed {row['day']} reused={row['reused']}", flush=True)
            result = {
                "status": "run_complete",
                "days": sorted(rows, key=lambda row: row["day"]),
            }
        if args.command in {"finalize", "all"}:
            result = finalize(
                execution_amendment_path=args.execution_amendment,
                output=args.output,
            )
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
