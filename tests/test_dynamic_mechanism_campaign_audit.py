from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from research.families.f10_live_replay_attribution.audit.dynamic_mechanism_campaign_audit import (
    classify_add_failure,
    defense_pause_intervals,
    evaluate_reducing_support,
    parse_live_mechanism_logs,
    summarize_campaign_decisions,
)


def test_fill_cooldown_log_parses_side_after_marker(tmp_path: Path) -> None:
    log = tmp_path / "maker.log"
    log.write_text(
        "2026-07-20 01:02:03 INFO FILL_CD: SELL kind=add consec=2 "
        "base=85s effective_base=85s vol_mult=1 add_mult=1 cooldown=170s\n",
        encoding="utf-8",
    )

    _, _, cooldown = parse_live_mechanism_logs((log,), start_ts=0.0, end_ts=math.inf)

    assert cooldown.iloc[0]["side"] == "SELL"
    assert cooldown.iloc[0]["cooldown_s"] == 170.0


def test_defense_interval_uses_epoch_seconds_and_individual_trade_through() -> None:
    campaigns = pd.DataFrame([{"campaign_id": 1, "start_ts": 1000.0, "end_ts": 1010.0}])
    decisions = pd.DataFrame(
        [
            {
                "timestamp": 1001.0,
                "side": "SELL",
                "campaign_id": 1,
                "defense_pause": 1,
                "base_price": 101.0,
            },
            {
                "timestamp": 1006.0,
                "side": "SELL",
                "campaign_id": 1,
                "defense_pause": 0,
                "base_price": 102.0,
            },
        ]
    )
    trades = pd.DataFrame(
        [
            {
                "time": 1_003_000,
                "price": 102.0,
                "qty": 0.001,
                "is_buyer_maker": False,
            }
        ]
    )

    intervals = defense_pause_intervals(decisions, campaigns, trades)

    assert intervals.iloc[0]["pause_duration_s"] == 5.0
    assert intervals.iloc[0]["touch"] == 1
    assert intervals.iloc[0]["strict_trade_through"] == 1


def test_reducing_support_uses_day_and_campaign_id() -> None:
    campaigns = pd.DataFrame(
        [
            {
                "day": "2026-01-01",
                "campaign_id": 1,
                "side": "LONG",
                "add_fills": 1,
                "terminal_pnl": -1.0,
            },
            {
                "day": "2026-01-02",
                "campaign_id": 1,
                "side": "LONG",
                "add_fills": 1,
                "terminal_pnl": -1.0,
            },
        ]
    )
    defense = pd.DataFrame(
        [
            {
                "day": "2026-01-01",
                "campaign_id": 1,
                "side": "SELL",
                "strict_trade_through": 1,
                "touch": 1,
                "pause_duration_s": 5.0,
            }
        ]
    )

    support = evaluate_reducing_support(campaigns, defense)

    assert support["sides"]["LONG"]["campaigns"] == 1
    assert support["sides"]["LONG"]["affected_add_negative_campaigns"] == 1


def test_missing_live_dynamic_fields_remain_unobserved() -> None:
    campaigns = pd.DataFrame([{"day": "2026-01-01", "campaign_id": 1, "side": "LONG"}])
    decisions = pd.DataFrame(
        [
            {
                "day": "2026-01-01",
                "campaign_id": 1,
                "inventory_role": "reducing",
                "base_distance_bps": 2.0,
                "final_distance_bps": 3.0,
                "spread_mult": 1.5,
                "defense_pause": 1,
                "inventory_emergency_eligible": 0,
                "loss_emergency_eligible": 0,
                "paused": 1,
                "widened": 1,
                "kept": 0,
            }
        ]
    )

    summarized = summarize_campaign_decisions(
        campaigns, decisions, p3_delta_star=14.0, p3_kappa_eff=0.067
    )

    assert math.isnan(summarized.iloc[0]["p3_floor_bound_rate"])
    assert math.isnan(summarized.iloc[0]["cap_hit_rate"])
    assert summarized.iloc[0]["defense_pause_decisions"] == 1


def test_add_failure_separates_immediate_toxicity_and_repair() -> None:
    campaigns = pd.DataFrame(
        [
            {
                "campaign_id": 1,
                "add_fills": 1,
                "first_add_markout_30s_bps": -1.0,
                "add_to_first_reducing_s": 20.0,
            },
            {
                "campaign_id": 2,
                "add_fills": 1,
                "first_add_markout_30s_bps": 0.2,
                "add_to_first_reducing_s": 600.0,
            },
        ]
    )

    classified = classify_add_failure(campaigns)

    assert classified.loc[0, "add_failure_class"] == "immediate_add_toxicity"
    assert classified.loc[1, "add_failure_class"] == "repair_failure"
