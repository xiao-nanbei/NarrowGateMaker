import pandas as pd

from models.alpha_evidence_ledger import _fill_evidence


def test_fill_evidence_reports_quantity_weighted_ev_and_usdc_sum() -> None:
    fills = pd.DataFrame(
        [
            {
                "day": "2026-07-01",
                "side": "BUY",
                "order_id": "a",
                "fill_qty": 0.001,
                "markout_1s": 10.0,
                "markout_5s": 10.0,
                "markout_30s": 10.0,
                "ev_30s": 10.0,
            },
            {
                "day": "2026-07-01",
                "side": "BUY",
                "order_id": "b",
                "fill_qty": 0.003,
                "markout_1s": -2.0,
                "markout_5s": -2.0,
                "markout_30s": -2.0,
                "ev_30s": -2.0,
            },
        ]
    )
    for column in (
        "age_ms",
        "quote_dist",
        "final_quote_delta_to_bbo",
        "near_depth_total",
        "queue_local_rank",
        "toxic_30s",
    ):
        fills[column] = 0.0

    _daily, rollup, _positive = _fill_evidence(fills, min_fills=1)
    overall = rollup.iloc[0]

    assert overall["filled_qty_btc"] == 0.004
    assert overall["avg_ev_30s_usdc_per_btc"] == 1.0
    assert overall["avg_markout_30s"] == 1.0
    assert overall["sum_ev_30s_usdc"] == 0.004
    assert "sum_ev_30s" not in rollup.columns
