from research.families.f10_live_replay_attribution.audit.metrics import (
    BboMidSeries,
    TradeRow,
    _order_group_stats,
    attach_campaign_labels_to_orders,
    build_campaigns,
    campaign_label_rows,
    campaign_policy_blocked_fill_rows,
    campaign_policy_replay_rows,
    fill_summary,
    inventory_role,
    live_order_daily_rows,
    live_replay_baseline_compare_rows,
    local_liquidity_mechanism_summary,
    local_liquidity_mechanism_tables,
    order_level_knob_shadow_rows,
    order_level_rows,
    order_level_score_daily_rows,
    order_level_score_sanity_rows,
    order_level_score_summary,
    order_level_summary,
    reducing_cooldown_replay_rows,
    replay_order_level_rows,
    spot_pending_shadow_tables,
    xmarket_ref_shadow_tables,
)
from research.families.f05_fill_quality_quote_ev.audit.order_score_fast import (
    ReducingFillBurstTracker,
    _order_inventory_reducing,
)


def test_order_level_rows_attach_fill_markout_and_campaign_state() -> None:
    order_rows = [
        {
            "_ts": 1000.0,
            "timestamp": "1000.000",
            "event_type": "placed",
            "client_order_id": "mm_S_1",
            "side": "SELL",
            "price": "101.0",
            "quantity": "0.001",
            "mid": "100.0",
            "mode": "normal",
            "reason_mask": "0",
            "reason_text": "none",
            "spread_mult": "1.0",
            "size_mult": "1.0",
            "toxicity": "0.2",
            "markout_ema": "0.0",
            "l2_near_depth_total": "8.0",
            "l2_book_refresh_ratio": "0.6",
            "l2_book_cancel_ratio": "0.1",
            "l2_quote_flip_rate": "0.0",
        },
        {
            "_ts": 1002.0,
            "timestamp": "1002.000",
            "event_type": "filled",
            "client_order_id": "mm_S_1",
            "side": "SELL",
            "price": "101.0",
            "filled_qty": "0.001",
            "avg_fill_price": "101.0",
            "age_ms": "2000",
            "inventory_before_fill": "-0.002",
        },
    ]
    quote_rows = [
        {
            "_ts": 1000.0,
            "timestamp": "1000.000",
            "side": "SELL",
            "mid": "100.0",
            "final_price": "101.0",
            "base_price": "101.0",
            "allow_post": "1",
            "allow_exposure_increase": "1",
            "action": "place",
        },
        {"_ts": 1003.0, "timestamp": "1003.000", "side": "BUY", "mid": "100.5"},
        {"_ts": 1032.0, "timestamp": "1032.000", "side": "BUY", "mid": "99.0"},
    ]
    inventory_shadow_rows = [
        {
            "_ts": 999.0,
            "timestamp": "999.000",
            "q": "-0.002",
            "active": "1",
            "campaign_id": "7",
            "side": "SHORT",
            "age_s": "3700",
            "max_abs_qty": "0.007",
            "total_pnl": "-0.4",
            "adverse_excursion": "-0.8",
            "exposure_increasing_fills": "3",
            "reducing_fills": "1",
            "ask_block_if_inv_006": "1",
            "ask_block_if_age_60m": "1",
            "ask_block_if_reducing_only": "1",
        }
    ]
    sell_resiliency_rows = [
        {
            "_ts": 1000.0,
            "timestamp": "1000.000",
            "side": "SELL",
            "hit": "1",
            "flow_decel": "0.35",
            "rank": "0.25",
            "refill_edge": "0.04",
            "ref_adv": "0.0",
            "spot_adv": "0.0",
            "spot_available": "1",
        }
    ]

    rows = order_level_rows(
        order_rows=order_rows,
        quote_rows=quote_rows,
        inventory_shadow_rows=inventory_shadow_rows,
        sell_resiliency_rows=sell_resiliency_rows,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["filled"] == 1
    assert row["side"] == "SELL"
    assert row["order_exposure_increasing"] == 1
    assert row["inventory_role"] == "add"
    assert row["order_add_on"] == 1
    assert row["fill_inventory_role"] == "add"
    assert row["fill_role_source"] == "exact_trace"
    assert row["inventory_role_drift"] == 0
    assert row["shadow_block_inv006"] == 1
    assert row["shadow_block_age60m"] == 1
    assert row["sell_resil_hit"] == 1
    # SELL maker-signed markout is fill_price - future_mid.
    assert abs(float(row["markout_30s_bps"]) - ((101.0 - 99.0) / 101.0 * 10000.0)) < 1e-6
    assert row["score_hint"] in {
        "stop_add_or_widen",
        "resilient_watch",
        "neutral",
        "quote_eligible",
    }

    summary = order_level_summary(rows)
    assert summary["order_rows"] == 1
    assert summary["filled_orders"] == 1
    assert summary["sell_fill_rate"] == 1.0
    assert summary["add_orders"] == 1

    score_rows = order_level_score_summary(rows)
    assert any(r["score"] == "campaign_risk_score" for r in score_rows)
    assert any(r["score"] == "campaign_outcome_risk_score" for r in score_rows)
    assert any(r["score"] == "reducing_burst_risk_score" for r in score_rows)
    assert any(r["score"] == "lifecycle_risk_score" for r in score_rows)

    daily_rows = order_level_score_daily_rows(rows)
    assert any(r["score"] == "campaign_risk_score" for r in daily_rows)

    sanity_rows = order_level_score_sanity_rows(rows)
    assert any(r["score"] == "campaign_risk_score" for r in sanity_rows)

    knob_rows = order_level_knob_shadow_rows(rows)
    assert knob_rows
    assert knob_rows[0]["shadow_rule"] in {
        "campaign_outcome_high_exposure_increasing",
        "campaign_state_high_exposure_increasing",
    }
    assert knob_rows[0]["knob"] == "soft_spread_widen_or_reducing_skew"


def test_inventory_role_separates_openers_adds_and_reducing_orders() -> None:
    assert inventory_role("BUY", 0.0) == "opener"
    assert inventory_role("SELL", 0.0) == "opener"
    assert inventory_role("BUY", 0.003) == "add"
    assert inventory_role("SELL", -0.003) == "add"
    assert inventory_role("SELL", 0.003) == "reducing"
    assert inventory_role("BUY", -0.003) == "reducing"


def test_order_level_markout_is_weighted_by_partial_fill_quantity() -> None:
    rows = [
        {
            "side": "BUY",
            "filled": 1,
            "filled_qty": "0.001",
            "markout_5s_bps": "10.0",
            "markout_20s_bps": "10.0",
            "markout_30s_bps": "10.0",
        },
        {
            "side": "BUY",
            "filled": 1,
            "filled_qty": "0.003",
            "markout_5s_bps": "-2.0",
            "markout_20s_bps": "-2.0",
            "markout_30s_bps": "-2.0",
        },
    ]

    summary = order_level_summary(rows)
    assert summary["buy_avg_markout_30s_bps"] == 1.0

    score_rows = order_level_score_summary(rows)
    grouped = [row for row in score_rows if row["side"] == "BUY"]
    assert grouped
    assert all(float(row["avg_markout_30s_bps"]) == 1.0 for row in grouped)


def test_fill_summary_reports_side_vwap() -> None:
    trades = [
        TradeRow(
            ts=1.0,
            side="BUY",
            trade_type="FILL",
            qty=0.001,
            price=100.0,
            position=0.001,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
        ),
        TradeRow(
            ts=2.0,
            side="BUY",
            trade_type="FILL",
            qty=0.003,
            price=104.0,
            position=0.004,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
        ),
        TradeRow(
            ts=3.0,
            side="SELL",
            trade_type="FILL",
            qty=0.002,
            price=110.0,
            position=0.002,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
        ),
        TradeRow(
            ts=4.0,
            side="SELL",
            trade_type="FILL",
            qty=0.001,
            price=107.0,
            position=0.001,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
        ),
    ]
    summary = fill_summary(trades, order_rows=[])

    assert summary["buy_fills"] == 2
    assert summary["sell_fills"] == 2
    assert abs(summary["buy_avg_fill_price"] - 103.0) < 1e-12
    assert abs(summary["sell_avg_fill_price"] - 109.0) < 1e-12


def test_order_level_rows_uses_terminal_age_ms_for_canceled_orders() -> None:
    order_rows = [
        {
            "_ts": 1000.0,
            "timestamp": "1000.000",
            "event_type": "placed",
            "client_order_id": "mm_B_cancel",
            "side": "BUY",
            "price": "99.0",
            "quantity": "0.001",
            "mid": "100.0",
        },
        {
            "_ts": 1005.0,
            "timestamp": "1005.000",
            "event_type": "canceled",
            "client_order_id": "mm_B_cancel",
            "side": "BUY",
            "price": "99.0",
            "age_ms": "5000",
        },
    ]
    rows = order_level_rows(
        order_rows=order_rows,
        quote_rows=[],
        inventory_shadow_rows=[],
        sell_resiliency_rows=[],
    )

    assert len(rows) == 1
    assert rows[0]["filled"] == 0
    assert rows[0]["outcome_event"] == "canceled"
    assert rows[0]["observed_lifetime_ms"] == "5000.000"


def test_live_replay_baseline_compare_reports_vwap_edge() -> None:
    live_daily = live_order_daily_rows(
        order_rows=[
            {"timestamp": "1782864001", "event_type": "placed", "side": "BUY", "quantity": "0.001"},
            {
                "timestamp": "1782864002",
                "event_type": "filled",
                "side": "BUY",
                "filled_qty": "0.001",
                "avg_fill_price": "100.0",
            },
            {
                "timestamp": "1782864003",
                "event_type": "filled",
                "side": "SELL",
                "filled_qty": "0.001",
                "avg_fill_price": "102.0",
            },
        ],
        quote_rows=[
            {"timestamp": "1782864001", "action": "replace"},
            {"timestamp": "1782864002", "action": "keep"},
            {"timestamp": "1782864003", "action": "pause"},
        ],
    )
    replay_daily = [
        {
            "day": "2026-07-01",
            "decision_place_count": "1",
            "decision_replace_count": "1",
            "decision_total": "4",
            "decision_keep_rate": "0.25",
            "decision_pause_rate": "0.25",
            "fills_total": "2",
            "fills_bid_buy": "1",
            "fills_ask_sell": "1",
            "buy_avg_fill_price": "99.0",
            "sell_avg_fill_price": "103.0",
        }
    ]

    rows = live_replay_baseline_compare_rows(
        live_daily_rows=live_daily, replay_daily_rows=replay_daily
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["live_fills"] == 2
    assert row["replay_fills"] == 2
    assert float(row["live_side_vwap_edge"]) == 2.0
    assert float(row["replay_side_vwap_edge"]) == 4.0
    assert row["live_action_keep_rate"] == "0.333333"


def test_xmarket_ref_shadow_tags_side_adverse_and_cancel_counterfactual() -> None:
    rows = [
        {
            "timestamp": "100.000",
            "day": "1970-01-01",
            "client_order_id": "sell_1",
            "side": "SELL",
            "filled": "1",
            "fill_ts": "101.000",
            "observed_lifetime_ms": "1000",
            "markout_30s_bps": "-60.0",
        }
    ]
    ref = BboMidSeries(
        ts=(98.0, 99.0, 100.0, 100.5, 101.0),
        mid=(100.0, 100.0, 101.0, 102.0, 102.0),
        resolution="test_event",
    )
    local = BboMidSeries(
        ts=(98.0, 99.0, 100.0, 100.5, 101.0),
        mid=(100.0, 100.0, 100.0, 100.0, 100.0),
        resolution="test_event",
    )

    tables = xmarket_ref_shadow_tables(
        rows,
        ref_bbo=ref,
        local_bbo=local,
        threshold_bps=10.0,
        cancel_threshold_bps=10.0,
        cancel_latencies_ms=(50,),
        include_orders=True,
    )

    enriched = tables["orders"][0]
    assert enriched["xmarket_state"] == "adverse_leading"
    assert enriched["ref_adverse_for_side"] == 1
    assert enriched["ref_leads_local"] == 1
    cancel = tables["event_cancel"][0]
    assert cancel["saved_toxic_fills_m50_30s"] == 1
    assert cancel["false_cancel_positive_fills"] == 0


def test_shadow_tables_accept_submit_ts_without_timestamp() -> None:
    submit_s = 1_782_576_000.0
    rows = [
        {
            "submit_ts": str(int(submit_s * 1000)),
            "day": "2026-06-27",
            "client_order_id": "buy_1",
            "side": "BUY",
            "filled": "0",
        }
    ]
    local = BboMidSeries(
        ts=(submit_s - 1.0, submit_s, submit_s + 1.0),
        mid=(100.0, 100.0, 100.0),
        resolution="test_event",
    )
    reference = BboMidSeries(
        ts=(submit_s - 1.0, submit_s, submit_s + 1.0),
        mid=(100.0, 100.1, 100.2),
        resolution="test_event",
    )

    xmarket = xmarket_ref_shadow_tables(
        rows,
        ref_bbo=reference,
        local_bbo=local,
        include_orders=True,
    )
    spot = spot_pending_shadow_tables(
        rows,
        local_bbo=local,
        exec_spot_bbo=reference,
        include_orders=True,
    )

    assert len(xmarket["orders"]) == 1
    assert len(spot["orders"]) == 1


def test_replay_order_level_rows_convert_trace_markout_to_bps() -> None:
    replay_orders = [
        {
            "day": "2026-06-27",
            "order_id": "42",
            "side": "BUY",
            "submit_ts": "1782576000000",
            "outcome": "fill",
            "outcome_ts": "1782576003000",
            "lifetime_ms": "3000",
            "fill_qty": "0.001",
            "price": "99.0",
            "quantity": "0.001",
            "final_price": "99.0",
            "mid": "100.0",
            "final_distance_to_mid": "1.0",
            "inventory": "0.006",
            "tox_bid": "0.7",
            "tox_ask": "0.2",
            "near_depth_total": "2.0",
            "l2_book_refresh_ratio": "0.1",
            "l2_book_cancel_ratio": "0.0",
            "l2_quote_flip_rate": "0.0",
            "microprice_shift_bps": "0.0",
            "side_adverse": "True",
            "adverse_markout": "True",
        }
    ]
    replay_fills = [
        {
            "order_id": "42",
            "fill_ts": "1782576003000",
            "fill_trade_px": "99.0",
            "markout_1s": "-0.5",
            "markout_5s": "-1.0",
            "markout_30s": "-2.0",
        }
    ]

    rows = replay_order_level_rows(
        replay_order_rows=replay_orders,
        replay_fill_rows=replay_fills,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["filled"] == 1
    assert row["reason_text"] == "adverse|markout"
    assert row["shadow_block_inv006"] == 1
    assert row["inventory_role"] == "add"
    assert row["fill_inventory_role"] == "opener"
    assert row["fill_role_source"] == "reconstructed_daily"
    assert row["inventory_role_drift"] == 1
    assert abs(float(row["markout_30s_bps"]) - (-2.0 / 99.0 * 10000.0)) < 1e-6


def test_local_liquidity_mechanism_tables_use_order_level_schema() -> None:
    rows = []
    for day in ("2026-06-27", "2026-06-28", "2026-06-29"):
        for i in range(3):
            rows.append(
                {
                    "day": day,
                    "side": "SELL",
                    "filled": 1,
                    "quote_distance_bps": "35.0",
                    "near_depth_total": "2.5",
                    "queue_local_rank": "0.80",
                    "queue_deplete_mult": "1.10",
                    "queue_mo_mult": "1.05",
                    "l2_book_refresh_ratio": "0.40",
                    "l2_book_cancel_ratio": "0.10",
                    "fill_age_ms": "5000",
                    "markout_1s_bps": "-2.0",
                    "markout_5s_bps": "1.0",
                    "markout_20s_bps": "5.0",
                    "markout_30s_bps": "8.0",
                }
            )
        rows.append(
            {
                "day": day,
                "side": "SELL",
                "filled": 0,
                "quote_distance_bps": "35.0",
                "near_depth_total": "2.5",
                "queue_local_rank": "0.80",
                "queue_deplete_mult": "1.10",
                "queue_mo_mult": "1.05",
                "l2_book_refresh_ratio": "0.40",
                "l2_book_cancel_ratio": "0.10",
            }
        )

    tables = local_liquidity_mechanism_tables(
        rows,
        min_fills=6,
        min_daily_fills=2,
        holding_budget_s=20.0,
    )
    summary = local_liquidity_mechanism_summary(tables)

    assert summary["rollup_rows"] == 1
    assert summary["candidate_rows"] == 1
    assert tables["order_capacity"][0]["placed_orders"] == 12
    assert tables["rollup"][0]["mechanism_candidate"] == 1


def test_replay_order_level_rows_reconstruct_campaign_state_from_prior_fills() -> None:
    replay_orders = [
        {
            "day": "2026-06-27",
            "order_id": "99",
            "side": "BUY",
            "submit_ts": str(1782576000000 + 3_700_000),
            "outcome": "cancel",
            "outcome_ts": str(1782576000000 + 3_705_000),
            "lifetime_ms": "5000",
            "fill_qty": "0.0",
            "price": "97.0",
            "quantity": "0.001",
            "final_price": "97.0",
            "mid": "98.0",
            "final_distance_to_mid": "1.0",
            "inventory": "0.006",
            "tox_bid": "0.5",
            "tox_ask": "0.5",
            "near_depth_total": "2.0",
            "l2_book_refresh_ratio": "0.1",
            "l2_book_cancel_ratio": "0.0",
            "l2_quote_flip_rate": "0.0",
            "microprice_shift_bps": "0.0",
            "side_adverse": "False",
            "adverse_markout": "False",
        }
    ]
    replay_fills = [
        {
            "day": "2026-06-27",
            "order_id": "starter",
            "side": "BUY",
            "fill_ts": "1782576000000",
            "fill_trade_px": "100.0",
            "fill_qty": "0.006",
            "inventory": "0.0",
        }
    ]

    rows = replay_order_level_rows(
        replay_order_rows=replay_orders,
        replay_fill_rows=replay_fills,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["campaign_active"] == 1
    assert abs(float(row["campaign_age_s"]) - 3700.0) < 1e-6
    assert abs(float(row["campaign_duration_s"]) - 3700.0) < 1e-6
    assert abs(float(row["campaign_max_abs_qty"]) - 0.006) < 1e-6
    assert float(row["campaign_adverse_excursion"]) < 0.0
    assert row["order_exposure_increasing"] == 1
    assert row["shadow_block_inv006"] == 1
    assert row["shadow_block_age60m"] == 1
    assert row["shadow_block_reducing_only"] == 1
    assert float(row["campaign_risk_score"]) >= 0.66


def test_campaign_labels_attach_terminal_outcome_to_order_rows() -> None:
    trades = [
        TradeRow(
            ts=1000.0,
            side="BUY",
            trade_type="FILL",
            qty=0.006,
            price=100.0,
            position=0.006,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
        ),
        TradeRow(
            ts=1010.0,
            side="BUY",
            trade_type="FILL",
            qty=0.001,
            price=99.0,
            position=0.007,
            realized_pnl=0.0,
            unrealized_pnl=-1.2,
        ),
        TradeRow(
            ts=1400.0,
            side="SELL",
            trade_type="FILL",
            qty=0.007,
            price=101.0,
            position=0.0,
            realized_pnl=0.4,
            unrealized_pnl=0.0,
        ),
    ]
    labels = campaign_label_rows(build_campaigns(trades))
    assert labels
    assert labels[0]["campaign_label"] == "repaired_after_drawdown"
    assert labels[0]["campaign_repaired"] == 1
    assert float(labels[0]["early_5m_min_pnl_delta"]) < 0.0

    order_rows = [
        {
            "day": labels[0]["start_day"],
            "campaign_id": labels[0]["campaign_id"],
            "side": "BUY",
            "filled": 1,
            "markout_30s_bps": "-5",
            "fill_probability_score": "0.8",
            "fill_quality_score": "0.2",
            "toxic_risk_score": "0.7",
            "campaign_risk_score": "0.9",
            "resiliency_score": "0.1",
        }
    ]
    attached = attach_campaign_labels_to_orders(order_rows, labels)
    assert attached[0]["terminal_campaign_label"] == "repaired_after_drawdown"
    assert attached[0]["terminal_campaign_repaired"] == 1
    assert attached[0]["terminal_campaign_bad"] == 0
    assert float(attached[0]["terminal_campaign_outcome_risk_target"]) == 0.25
    assert float(attached[0]["terminal_final_total_pnl_delta"]) > 0.0

    sanity_rows = order_level_score_sanity_rows(attached * 120)
    campaign_row = next(
        r for r in sanity_rows if r["side"] == "BUY" and r["score"] == "campaign_risk_score"
    )
    assert "delta_high_minus_low_terminal_campaign_pnl" in campaign_row


def test_order_group_terminal_metrics_deduplicate_campaign_rows() -> None:
    rows = [
        {
            "day": "2026-07-01",
            "arm": "baseline",
            "campaign_id": "1",
            "terminal_campaign_label": "loss_tail",
            "terminal_final_total_pnl_delta": "-6",
            "terminal_campaign_repaired": "0",
            "terminal_campaign_bad": "1",
            "terminal_campaign_tail_loss": "1",
        },
        {
            "day": "2026-07-01",
            "arm": "baseline",
            "campaign_id": "1",
            "terminal_campaign_label": "loss_tail",
            "terminal_final_total_pnl_delta": "-6",
            "terminal_campaign_repaired": "0",
            "terminal_campaign_bad": "1",
            "terminal_campaign_tail_loss": "1",
        },
        {
            "day": "2026-07-01",
            "arm": "baseline",
            "campaign_id": "2",
            "terminal_campaign_label": "positive_flat",
            "terminal_final_total_pnl_delta": "2",
            "terminal_campaign_repaired": "1",
            "terminal_campaign_bad": "0",
            "terminal_campaign_tail_loss": "0",
        },
    ]

    stats = _order_group_stats(rows)

    assert stats["terminal_labeled_orders"] == 3
    assert stats["terminal_labeled_campaigns"] == 2
    assert stats["avg_terminal_campaign_pnl"] == -2.0
    assert stats["terminal_repair_rate"] == 0.5
    assert stats["terminal_bad_rate"] == 0.5
    assert stats["terminal_tail_loss_rate"] == 0.5


def test_campaign_policy_blocked_fill_detail_reports_stop_add_fills() -> None:
    trades = [
        TradeRow(
            ts=1000.0,
            side="BUY",
            trade_type="FILL",
            qty=0.006,
            price=100.0,
            position=0.006,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
        ),
        TradeRow(
            ts=1010.0,
            side="BUY",
            trade_type="FILL",
            qty=0.001,
            price=99.0,
            position=0.007,
            realized_pnl=0.0,
            unrealized_pnl=-0.2,
        ),
        TradeRow(
            ts=1020.0,
            side="SELL",
            trade_type="FILL",
            qty=0.007,
            price=101.0,
            position=0.0,
            realized_pnl=0.4,
            unrealized_pnl=0.0,
        ),
    ]
    rows = campaign_policy_replay_rows(trades)
    inv006 = next(r for r in rows if r["policy"] == "stop_add_inv_006")
    assert inv006["blocked_fills"] == 1
    assert inv006["blocked_buy_fills"] == 1

    blocked = campaign_policy_blocked_fill_rows(trades)
    blocked_inv006 = [r for r in blocked if r["policy"] == "stop_add_inv_006"]
    assert len(blocked_inv006) == 1
    assert blocked_inv006[0]["side"] == "BUY"
    assert blocked_inv006[0]["shadow_exposure_increasing"] == 1


def test_reducing_cooldown_replay_blocks_only_fast_reducing_fills() -> None:
    trades = [
        TradeRow(
            ts=1000.0,
            side="BUY",
            trade_type="FILL",
            qty=0.003,
            price=100.0,
            position=0.003,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
        ),
        TradeRow(
            ts=1001.0,
            side="SELL",
            trade_type="FILL",
            qty=0.001,
            price=100.1,
            position=0.002,
            realized_pnl=0.1,
            unrealized_pnl=0.0,
        ),
        TradeRow(
            ts=1004.0,
            side="SELL",
            trade_type="FILL",
            qty=0.001,
            price=100.2,
            position=0.001,
            realized_pnl=0.2,
            unrealized_pnl=0.0,
        ),
        TradeRow(
            ts=1015.0,
            side="SELL",
            trade_type="FILL",
            qty=0.001,
            price=100.3,
            position=0.0,
            realized_pnl=0.3,
            unrealized_pnl=0.0,
        ),
    ]
    rows = reducing_cooldown_replay_rows(trades)
    cd5 = next(r for r in rows if r["policy"] == "reducing_cd_5s")
    assert cd5["blocked_reducing_fills"] == 1
    assert cd5["blocked_sell_fills"] == 1
    assert cd5["exposure_increasing_fills"] == 1


def test_reducing_fill_burst_tracker_uses_prior_reducing_fills_only() -> None:
    tracker = ReducingFillBurstTracker()
    first_reducer = {
        "side": "SELL",
        "quantity": "0.001",
        "filled_qty": "0.001",
        "q_before": "0.003",
    }
    second_reducer = {
        "side": "SELL",
        "quantity": "0.001",
        "filled_qty": "0.001",
        "q_before": "0.002",
    }
    exposure_increasing = {
        "side": "BUY",
        "quantity": "0.001",
        "filled_qty": "0.001",
        "q_before": "0.003",
    }

    assert _order_inventory_reducing(first_reducer)
    assert not _order_inventory_reducing(exposure_increasing)

    before_any = tracker.snapshot(ts=1000.0, side="SELL")
    assert before_any["reducing_burst_count_8s"] == "0"

    tracker.observe_fill_if_reducing(first_reducer, ts=1001.0)
    after_first = tracker.snapshot(ts=1004.0, side="SELL")
    assert after_first["reducing_burst_count_4s"] == "1"
    assert after_first["reducing_burst_bucket_8s"] == "burst_1"

    tracker.observe_fill_if_reducing(second_reducer, ts=1005.0)
    after_second = tracker.snapshot(ts=1006.0, side="SELL")
    assert after_second["reducing_burst_count_8s"] == "2"
    assert after_second["reducing_burst_bucket_8s"] == "burst_2"

    tracker.observe_fill_if_reducing(exposure_increasing, ts=1007.0)
    buy_snapshot = tracker.snapshot(ts=1008.0, side="BUY")
    assert buy_snapshot["reducing_burst_count_8s"] == "0"

    old_window = tracker.snapshot(ts=1015.0, side="SELL")
    assert old_window["reducing_burst_count_8s"] == "0"


def test_reducing_burst_lifecycle_shadow_requires_narrow_conditions() -> None:
    rows = [
        {
            "day": "2026-06-27",
            "side": "SELL",
            "filled": "0",
            "order_inventory_reducing": "1",
            "reducing_burst_count_8s": "1",
            "lifecycle_risk_score": "0.82",
            "trend_inventory_risk_score": "0.80",
            "campaign_outcome_risk_score": "0.78",
            "sell_resil_refill_edge": "-0.04",
            "fill_probability_score": "0.4",
            "fill_quality_score": "0.3",
            "toxic_risk_score": "0.4",
            "campaign_risk_score": "0.5",
            "resiliency_score": "0.2",
            "micro_reversion_score": "0.2",
            "markout_30s_bps": "0.0",
            "campaign_age_s": "1200",
            "campaign_max_abs_qty": "0.008",
        },
        {
            "day": "2026-06-27",
            "side": "SELL",
            "filled": "0",
            "order_inventory_reducing": "1",
            "reducing_burst_count_8s": "1",
            "lifecycle_risk_score": "0.82",
            "trend_inventory_risk_score": "0.80",
            "campaign_outcome_risk_score": "0.78",
            "sell_resil_refill_edge": "0.08",
            "fill_probability_score": "0.4",
            "fill_quality_score": "0.3",
            "toxic_risk_score": "0.4",
            "campaign_risk_score": "0.5",
            "resiliency_score": "0.2",
            "micro_reversion_score": "0.2",
            "markout_30s_bps": "0.0",
            "campaign_age_s": "1200",
            "campaign_max_abs_qty": "0.008",
        },
    ]

    knob_rows = order_level_knob_shadow_rows(rows)
    lifecycle_rows = [r for r in knob_rows if r["shadow_rule"] == "reducing_burst_lifecycle_narrow"]
    assert len(lifecycle_rows) == 1
    assert lifecycle_rows[0]["knob"] == "shorter_ttl_or_pacing_shadow"
