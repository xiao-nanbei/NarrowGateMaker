#!/usr/bin/env python3
"""Repair the support envelope of the admitted role-safe BER scorecard."""

from __future__ import annotations

import json
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
OUTPUT = DATA_ROOT / (
    "reports/"
    "ber_guard_role_safe_add_only_current_stack_owner_v1_20260808/"
    "development_execution_v2"
)
ORIGINAL_REPORT_SHA256 = "b01c82a0cad3dcddc3113c5984dce481b12608b79b528e069b6c4da722a1388b"
ORIGINAL_SCORECARD_SHA256 = "18705eb8814509f936fbc94d6b61a355ba239369219a7f09040bf2c7f59e4a75"
ORIGINAL_PANEL_MANIFEST_SHA256 = "b85d34e5286455b730be2cb09c88c4426f0a361e1156534320fc244dd229d3db"
REPAIR_SUCCESS = "_SCORECARD_REPAIR_SUCCESS"
PREDECESSOR_REPAIR = {
    "scorecard": "d277afda95202d165432aa3a98398972d6cee4dd029178e72777c1079efc0395",
    "errata": "cee1e447135b6d5db9aef8151f3b83cfef24dacbd6be6efbfbcaaba93f1b1913",
    "manifest": "724a895a48289a8fa132e4f8332a687b303419e381b1843416a5c27b2c3ee6ce",
}
REPAIR_SUCCESS_V2 = "_SCORECARD_REPAIR_V2_SUCCESS"


def _load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise base.BerRoleSafeError(f"expected JSON object: {path}")
    return payload


def repair(*, output: Path = OUTPUT) -> dict[str, Any]:
    report_path = output / "report.json"
    scorecard_path = output / "action-defense-v2-scorecard.json"
    panel_manifest_path = output / "panel-manifest.json"
    for path, digest in (
        (report_path, ORIGINAL_REPORT_SHA256),
        (scorecard_path, ORIGINAL_SCORECARD_SHA256),
        (panel_manifest_path, ORIGINAL_PANEL_MANIFEST_SHA256),
    ):
        base._validate_artifact(path, digest, role="admitted predecessor artifact")
    panel_marker = output / base.PANEL_SUCCESS
    if not panel_marker.is_file() or panel_marker.read_text(encoding="ascii").strip() != (
        native_runner._sha256_file(panel_manifest_path)
    ):
        raise base.BerRoleSafeError("predecessor panel marker drifted")
    for name, digest in PREDECESSOR_REPAIR.items():
        predecessor = {
            "scorecard": output / "action-defense-v2-scorecard.support-repaired.json",
            "errata": output / "scorecard-support-errata.json",
            "manifest": output / "scorecard-repair-manifest.json",
        }[name]
        base._validate_artifact(
            predecessor, digest, role=f"incomplete scorecard repair {name}"
        )
    predecessor_marker = output / REPAIR_SUCCESS
    if not predecessor_marker.is_file() or predecessor_marker.read_text(
        encoding="ascii"
    ).strip() != PREDECESSOR_REPAIR["manifest"]:
        raise base.BerRoleSafeError("incomplete scorecard repair marker drifted")
    report = _load(report_path)
    panel = _load(panel_manifest_path)
    daily_path = Path(panel["artifacts"]["daily"]["path"])
    campaigns_path = Path(panel["artifacts"]["campaigns"]["path"])
    base._validate_artifact(
        daily_path, panel["artifacts"]["daily"]["sha256"], role="daily evidence"
    )
    base._validate_artifact(
        campaigns_path,
        panel["artifacts"]["campaigns"]["sha256"],
        role="campaign evidence",
    )
    daily = pd.read_parquet(daily_path)
    campaigns = pd.read_parquet(campaigns_path)
    effective_rows = int(campaigns.groupby("arm").size().min())
    candidate = daily.loc[daily["arm"].eq(base.ARMS[1])].sort_values("day")
    last = candidate.iloc[-1]
    final_inventory = float(last["final_inventory_btc"])
    final_mark = float(last["terminal_mark_price_usdc_per_btc"])
    metrics = report["metrics"]
    evidence = {
        "schema_version": experiment_scorecard_v2.CANONICAL_EVIDENCE_SCHEMA_VERSION,
        "experiment_id": base.IDENTITY,
        "family_id": "F09_campaign_action_uplift",
        "panel_role": "development",
        "input_identity": {
            "original_report_sha256": ORIGINAL_REPORT_SHA256,
            "original_scorecard_sha256": ORIGINAL_SCORECARD_SHA256,
            "original_panel_manifest_sha256": ORIGINAL_PANEL_MANIFEST_SHA256,
        },
        "score_profile_contract": experiment_scorecard_v2.score_profile_contract(
            "action_defense_v2"
        ),
        "validity_failures": [
            "daily_fresh_start_is_not_continuous_live_promotion_authority"
        ],
        "family_gate_failures": [],
        "metrics": metrics,
        "support": {
            "n_rows": effective_rows,
            "n_days": len(report["days"]),
            "effective_sample_size": float(effective_rows),
            "minimum_behavior_propensity": 0.5,
            "unsupported_mass": 0.0,
            "overlap_violations": 0,
            "candidate_rate": float(
                report["mechanics"]["candidate_effective_side_change_rate"]
            ),
            "failures": [],
        },
        "candidate_rate": float(
            report["mechanics"]["candidate_effective_side_change_rate"]
        ),
        "invariant_violations": [],
        "continuous_path_accounting": {
            "schema_version": experiment_scorecard_v2.CONTINUOUS_PATH_SCHEMA_VERSION,
            "utc_day_role": "bootstrap_cluster_only",
            "cash_carried_across_utc_days": False,
            "inventory_carried_across_utc_days": False,
            "campaign_state_carried_across_utc_days": False,
            "panel_final_inventory_mtm_included": True,
            "forced_day_end_liquidations": 0,
            "day_end_state_resets": len(report["days"]) - 1,
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
    corrected = experiment_scorecard_v2.score_canonical_evidence(
        evidence, profile_id="action_defense_v2"
    )
    if corrected["support"]["passed"] is not True:
        raise base.BerRoleSafeError("corrected support envelope still fails")
    if "candidate_rate_outside_profile_budget" in corrected["hard_gates"][
        "failures"
    ]:
        raise base.BerRoleSafeError("corrected candidate-rate envelope still fails")
    if corrected["hard_gates"]["passed"] is not False:
        raise base.BerRoleSafeError("scorecard repair changed the economic close decision")
    corrected_path = output / "action-defense-v2-scorecard.support-repaired-v2.json"
    errata_path = output / "scorecard-support-errata-v2.json"
    base._atomic_json(corrected_path, corrected)
    errata = {
        "schema_version": "ber_guard_role_safe_add_only_scorecard_support_errata.v2",
        "identity": base.IDENTITY,
        "status": "support_metadata_repaired_decision_unchanged",
        "original_report_sha256": ORIGINAL_REPORT_SHA256,
        "original_scorecard_sha256": ORIGINAL_SCORECARD_SHA256,
        "original_panel_manifest_sha256": ORIGINAL_PANEL_MANIFEST_SHA256,
        "predecessor_repair": PREDECESSOR_REPAIR,
        "failure": "the original scorecard omitted the canonical support object; repair v1 restored support but omitted the legacy top-level candidate_rate consumed by the frozen score engine",
        "corrected_scorecard_sha256": native_runner._sha256_file(corrected_path),
        "corrected_support": corrected["support"],
        "economic_hard_gates": corrected["hard_gates"],
        "decision_unchanged": report["decision"],
        "action_authorized": False,
        "live_authorized": False,
    }
    base._atomic_json(errata_path, errata)
    repair_manifest = {
        "schema_version": "ber_guard_role_safe_add_only_scorecard_repair.v2",
        "identity": base.IDENTITY,
        "artifacts": {
            "corrected_scorecard": {
                "path": str(corrected_path),
                "sha256": native_runner._sha256_file(corrected_path),
            },
            "errata": {
                "path": str(errata_path),
                "sha256": native_runner._sha256_file(errata_path),
            },
        },
    }
    repair_manifest_path = output / "scorecard-repair-manifest-v2.json"
    base._atomic_json(repair_manifest_path, repair_manifest)
    base._atomic_text(
        output / REPAIR_SUCCESS_V2,
        native_runner._sha256_file(repair_manifest_path) + "\n",
    )
    return errata


if __name__ == "__main__":
    print(json.dumps(repair(), indent=2, sort_keys=True, allow_nan=False))
