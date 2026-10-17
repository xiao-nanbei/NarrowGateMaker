"""Public aggregates keep complete coverage, units and archival status distinct."""
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_complete_common_panel_and_no_new_control_or_continuous():
    report = json.loads((ROOT / "research/final107_comparison.json").read_text())
    assert report["calendar_utc"] == {
        "start_inclusive": "2026-05-28", "end_exclusive": "2026-09-12",
        "days": 107, "independent_accounts": 54,
        "two_day_accounts": 53, "one_day_accounts": 1,
    }
    assert report["new_candidate_accounts"] == 270
    assert report["new_control_accounts"] == report["new_continuous_accounts"] == 0
    assert report["fresh_holdout"] is False
    assert report["underlying_evidence_availability"] == "private_not_distributed"
    assert report["closure"]["archive_status"] == "verified_minimal"
    assert report["closure"]["inspection_automation"] == "deleted"
    assert report["closure"]["frozen_economic_reports_rewritten"] is False
    assert report["closure"]["economic_reruns_for_archival"] == 0
    assert [c["id"] for c in report["candidates"]] == list("ABCDE")
    control = report["baseline"]
    for row in [control, *report["candidates"]]:
        assert row["pnl_usdc_per_0001_btc"] == pytest.approx(
            row["net_pnl_usdc"] / row["volume_btc"] * 0.001, abs=1e-12)
        assert row["pnl_usdc_per_10000_usdc_notional"] == pytest.approx(
            row["net_pnl_usdc"] / row["notional_usdc"] * 10000, abs=1e-12)
    for row in report["candidates"]:
        assert row["accepted_accounts"] == 54 and row["market_days"] == 107
        assert row["better_accounts"] + row["worse_accounts"] + row["tied_accounts"] == 54
        assert row["delta_pnl_usdc"] == pytest.approx(
            row["net_pnl_usdc"] - control["net_pnl_usdc"], abs=1e-10)
        assert row["delta_pnl_usdc"] < 0
        assert row["pnl_usdc_per_0001_btc"] < control["pnl_usdc_per_0001_btc"]
        assert row["pnl_usdc_per_10000_usdc_notional"] < control["pnl_usdc_per_10000_usdc_notional"]
        assert row["model_promoted"] is False
        assert row["economic_status"] == "closed_complete"
        assert row["archive_status"] == "verified_minimal"


def test_current_work_index_binds_completed_summary_without_changing_old_study():
    index = json.loads((ROOT / "research/recompute_407.json").read_text())
    closure = index["final107_five_candidate_completion"]
    assert closure["summary"] == "final107_comparison.json"
    assert closure["archive_status"] == "verified_minimal"
    packages = {p["id"]: p for p in index["packages"]}
    assert packages[4]["new_b0_final107_20261013"]["completed_M0_accounts"] == 8
    for package, key in (
        (4, "full_support_direction_20261014"),
        (4, "equal_label_all13_joint_dir10_20261015"),
        (5, "development300_extension_20261014"),
        (5, "valid_label_equal_extension_20261015"),
        (5, "original100_valid_label_equal_20261015"),
    ):
        assert packages[package][key]["status"] == (
            "54_accounts_107_days_economically_accepted_archive_verified_minimal_no_promotion")
