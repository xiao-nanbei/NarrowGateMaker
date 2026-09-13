from research.families.f04_external_market_alpha.audit.external_venue_shadow_panel import (
    _local_absorption_state,
    _rebuild_fill_markouts,
    aggregate_cross_instrument,
    aggregate_interactions,
)
from research.families.f10_live_replay_attribution.audit.metrics import BboMidSeries


def test_local_absorption_proxy_uses_quote_time_fields_only():
    strong, score = _local_absorption_state({
        "near_depth_total": "1.2",
        "l2_book_refresh_ratio": "1.1",
        "l2_book_cancel_ratio": "0.5",
        "micro_reversion_score": "0.4",
        # Terminal labels must not affect the state.
        "terminal_final_total_pnl_delta": "-999",
    })
    assert (strong, score) == ("strong", 3)
    weak, score = _local_absorption_state({
        "near_depth_total": "0.5",
        "l2_book_refresh_ratio": "0.2",
        "l2_book_cancel_ratio": "0.9",
        "micro_reversion_score": "0.1",
    })
    assert (weak, score) == ("weak", 0)


def test_interaction_aggregate_is_fill_weighted():
    rows = [
        {
            "side": "BUY", "horizon_ms": 1000, "sample_time": "submit",
            "local_absorption_state": "strong", "external_side_bucket": "favorable",
            "orders": 100, "fills": 10, "terminal_labeled": 5, "tail_m50_30s": 1,
            "avg_markout_1s_bps": 1, "avg_markout_5s_bps": 1,
            "avg_markout_20s_bps": 1, "avg_markout_30s_bps": 1,
            "avg_campaign_terminal_pnl": 2, "campaign_repair_rate": 0.8,
        },
        {
            "side": "BUY", "horizon_ms": 1000, "sample_time": "submit",
            "local_absorption_state": "strong", "external_side_bucket": "favorable",
            "orders": 200, "fills": 30, "terminal_labeled": 15, "tail_m50_30s": 2,
            "avg_markout_1s_bps": 3, "avg_markout_5s_bps": 3,
            "avg_markout_20s_bps": 3, "avg_markout_30s_bps": 3,
            "avg_campaign_terminal_pnl": 4, "campaign_repair_rate": 0.4,
        },
    ]
    result = aggregate_interactions(rows)[0]
    assert result["orders"] == 300
    assert result["fills"] == 40
    assert result["avg_markout_30s_bps"] == "2.500000"
    assert result["avg_campaign_terminal_pnl"] == "3.500000"


def test_rebuild_fill_markouts_uses_independent_local_bbo_horizons():
    orders = [{
        "filled": "1",
        "side": "BUY",
        "fill_ts": "10",
        "avg_fill_price": "100",
        "markout_20s_bps": "999",
        "markout_30s_bps": "999",
    }]
    bbo = BboMidSeries(
        ts=(11.0, 15.0, 30.0, 40.0),
        mid=(100.1, 100.2, 101.0, 99.0),
        resolution="synthetic",
    )

    assert _rebuild_fill_markouts(orders, bbo) == 1
    assert orders[0]["markout_1s_bps"] == "10.000000"
    assert orders[0]["markout_5s_bps"] == "20.000000"
    assert orders[0]["markout_20s_bps"] == "100.000000"
    assert orders[0]["markout_30s_bps"] == "-100.000000"


def test_cross_instrument_aggregate_reports_daily_support_and_campaign_outcome():
    rows = [
        {
            "side": "SELL", "sample_time": "fill", "cross_instrument_state": "spot_leading_up",
            "orders": 10, "fills": 4, "terminal_labeled": 3, "tail_m50_30s": 1,
            "avg_markout_1s_bps": -2, "avg_markout_5s_bps": -3,
            "avg_markout_20s_bps": -4, "avg_markout_30s_bps": -5,
            "avg_campaign_terminal_pnl": -1, "campaign_repair_rate": 0.3,
        },
        {
            "side": "SELL", "sample_time": "fill", "cross_instrument_state": "spot_leading_up",
            "orders": 20, "fills": 6, "terminal_labeled": 2, "tail_m50_30s": 0,
            "avg_markout_1s_bps": 1, "avg_markout_5s_bps": 1,
            "avg_markout_20s_bps": 1, "avg_markout_30s_bps": 1,
            "avg_campaign_terminal_pnl": 2, "campaign_repair_rate": 0.8,
        },
    ]
    result = aggregate_cross_instrument(rows)[0]
    assert result["days"] == 2
    assert result["fills"] == 10
    assert result["avg_markout_30s_bps"] == "-1.400000"
    assert result["avg_campaign_terminal_pnl"] == "0.200000"
    assert result["positive_markout_days_30s"] == 1


def test_cross_instrument_aggregate_pairs_each_state_with_same_day_neutral():
    common = {
        "side": "BUY", "sample_time": "fill", "orders": 10, "fills": 5,
        "terminal_labeled": 4, "tail_m50_30s": 0,
        "avg_markout_1s_bps": 0, "avg_markout_5s_bps": 0,
        "avg_markout_20s_bps": 0, "avg_markout_30s_bps": 0,
        "avg_campaign_terminal_pnl": 0, "campaign_repair_rate": 0.5,
    }
    rows = [
        {**common, "day": "2026-01-01", "cross_instrument_state": "neutral"},
        {
            **common, "day": "2026-01-01", "cross_instrument_state": "spot_leading_up",
            "avg_markout_30s_bps": 2, "avg_campaign_terminal_pnl": 1,
            "campaign_repair_rate": 0.75,
        },
    ]
    result = next(
        row for row in aggregate_cross_instrument(rows)
        if row["cross_instrument_state"] == "spot_leading_up"
    )
    assert result["vs_neutral_markout_30s_bps"] == "2.000000"
    assert result["vs_neutral_positive_days_30s"] == 1
    assert result["vs_neutral_campaign_terminal_pnl"] == "1.000000"
