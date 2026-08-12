import pandas as pd
import pytest

from research.families.f09_campaign_action_uplift.audit.post_fill_stop_add_paired import summarize_panel


def _row(day: str, arm: str, *, pnl: float, terminal: float, fills: int):
    return {
        "day": day,
        "arm": arm,
        "replay_pnl": pnl,
        "terminal_pnl_sum": terminal,
        "fills_total": fills,
        "campaigns": 10,
        "loss_tail": 1,
        "bad_campaigns": 4,
        "repaired_campaigns": 6,
        "replay_abs_inventory_time_s": 100.0,
        "replay_max_inventory": 0.004,
        "duration_mean_s": 60.0,
        "early_20m_drawdown_mean": 0.2,
        "replay_campaign_max_adverse_excursion": -1.0,
        "buy_fill_share": 0.5,
        "fills_bid_buy": fills // 2,
        "fills_ask_sell": fills - fills // 2,
        "avg_markout_bid": -1.0,
        "avg_markout_ask": -2.0,
        "multi_market_policy_eval_count": 100 if arm != "baseline" else 0,
        "multi_market_policy_hit_count": 5 if arm != "baseline" else 0,
        "multi_market_policy_effective_block_count": 4 if arm != "baseline" else 0,
        "bid_multi_market_policy_effective_block_count": 3 if arm != "baseline" else 0,
        "ask_multi_market_policy_effective_block_count": 1 if arm != "baseline" else 0,
    }


def test_paired_report_uses_utc_day_pairs_and_campaign_normalization(tmp_path):
    rows = [
        _row("2026-01-01", "baseline", pnl=-2.0, terminal=-1.5, fills=100),
        _row("2026-01-01", "candidate", pnl=-1.0, terminal=-0.5, fills=99),
        _row("2026-01-02", "baseline", pnl=1.0, terminal=0.5, fills=100),
        _row("2026-01-02", "candidate", pnl=1.0, terminal=0.5, fills=100),
    ]
    path = tmp_path / "daily.csv"
    pd.DataFrame(rows).to_csv(path, index=False)

    result = summarize_panel("unit", path)

    assert result["days"] == 2
    assert result["changed_days"] == 1
    assert result["raw"]["delta"] == pytest.approx(1.0)
    assert result["campaign_terminal"]["delta"] == pytest.approx(1.0)
    assert result["activity"]["fills_retention"] == pytest.approx(199 / 200)
    assert result["campaign_quality"]["tail_delta"] == 0
    assert result["policy_occupancy"]["effective_blocks"] == 8
    assert result["hard_gates"]["all_pass"] is True
