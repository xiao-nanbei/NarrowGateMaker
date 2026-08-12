from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit.fill_toxicity_incremental import (
    add_inventory_campaign_state,
    blocked_day_folds,
    build_external_consensus,
    chronological_folds,
    evaluate_incremental,
    match_order_outcomes,
)


def test_order_context_join_uses_exact_id_not_nearest_side_time(tmp_path) -> None:
    fills = pd.DataFrame(
        [
            {
                "day": "2026-01-01",
                "client_order_id": "order-a",
                "fill_ts": 10.00,
                "fill_price": 100.0,
                "side": "BUY",
            },
            {
                "day": "2026-01-01",
                "client_order_id": "order-b",
                "fill_ts": 10.01,
                "fill_price": 100.0,
                "side": "BUY",
            },
        ]
    )
    # Times and prices intentionally point toward the wrong row.  Exact order
    # identity must still control the association.
    outcomes = pd.DataFrame(
        [
            {
                "day": "2026-01-01",
                "client_order_id": "order-a",
                "timestamp": 10.02,
                "event_type": "filled",
                "side": "BUY",
                "avg_fill_price": 200.0,
                "mode": "context-a",
            },
            {
                "day": "2026-01-01",
                "client_order_id": "order-b",
                "timestamp": 9.99,
                "event_type": "filled",
                "side": "BUY",
                "avg_fill_price": 100.0,
                "mode": "context-b",
            },
        ]
    )
    path = tmp_path / "outcomes.csv"
    outcomes.to_csv(path, index=False)

    matched = match_order_outcomes(fills, path)

    assert matched["local_mode"].tolist() == ["context-a", "context-b"]
    assert matched["local_context_matched"].tolist() == [1, 1]
    assert set(matched["local_context_match_method"]) == {"exact_client_order_id"}


def test_order_context_join_fails_closed_without_shared_exact_id(tmp_path) -> None:
    fills = pd.DataFrame(
        [{"fill_ts": 10.0, "fill_price": 100.0, "side": "BUY"}]
    )
    outcomes = pd.DataFrame(
        [
            {
                "client_order_id": "nearby",
                "timestamp": 10.0,
                "event_type": "filled",
                "side": "BUY",
                "avg_fill_price": 100.0,
            }
        ]
    )
    path = tmp_path / "outcomes.csv"
    outcomes.to_csv(path, index=False)

    matched = match_order_outcomes(fills, path)

    assert matched.loc[0, "local_context_matched"] == 0
    assert matched.loc[0, "local_context_match_method"] == "missing_exact_identity"


def test_blocked_day_folds_exclude_late_panel() -> None:
    days = [f"2026-01-{day:02d}" for day in range(1, 11)]
    folds = blocked_day_folds(days, folds=3, late_days=2)

    assert len(folds) == 3
    development = set(days[:-2])
    seen: set[str] = set()
    for fold in folds:
        assert fold.panel == "blocked_day_crossfit"
        assert set(fold.test_days).isdisjoint(fold.train_days)
        assert set(fold.test_days) | set(fold.train_days) == development
        assert not (set(fold.test_days) & set(days[-2:]))
        seen.update(fold.test_days)
    assert seen == development


def test_inventory_roles_and_open_risk_are_causal() -> None:
    frame = pd.DataFrame(
        [
            {"fill_id": 1, "fill_ts": 1.0, "side": "BUY", "qty": 0.001, "position_after": 0.001, "realized_pnl": 0.0},
            {"fill_id": 2, "fill_ts": 2.0, "side": "BUY", "qty": 0.001, "position_after": 0.002, "realized_pnl": 0.0},
            {"fill_id": 3, "fill_ts": 3.0, "side": "SELL", "qty": 0.001, "position_after": 0.001, "realized_pnl": 1.0},
            {"fill_id": 4, "fill_ts": 4.0, "side": "SELL", "qty": 0.001, "position_after": 0.0, "realized_pnl": 2.0},
            {"fill_id": 5, "fill_ts": 5.0, "side": "SELL", "qty": 0.001, "position_after": -0.001, "realized_pnl": 0.0},
        ]
    )

    result = add_inventory_campaign_state(frame)

    assert result["inventory_role"].tolist() == ["opener", "add", "reducing", "reducing", "opener"]
    assert result.loc[:3, "campaign_flattened"].tolist() == [1, 1, 1, 1]
    assert result.loc[:3, "campaign_terminal_pnl"].tolist() == [3.0, 3.0, 3.0, 3.0]
    assert result.loc[4, "campaign_open_risk"] == 1


def test_leave_one_out_consensus_recomputes_without_outlier() -> None:
    row = {"execution_flow_pressure_100ms": 0.0}
    for venue, move in (("bitget", 1.0), ("bybit", 1.2), ("okx", 50.0)):
        for factor in ("spot", "perp"):
            row[f"{venue}_{factor}_book_fresh_100ms"] = 1
            row[f"{venue}_{factor}_mid_move_bps_100ms"] = move
            row[f"{venue}_{factor}_flow_pressure_100ms"] = move / 10.0
            row[f"{venue}_{factor}_trade_imbalance_100ms"] = 0.1
            row[f"{venue}_{factor}_l1_ofi_normalized_100ms"] = 0.1
            for name in ("bid_depletion", "bid_refill", "ask_depletion", "ask_refill"):
                row[f"{venue}_{factor}_{name}_100ms"] = 0.0
    frame = pd.DataFrame([row])

    full = build_external_consensus(frame, included_venues=("bitget", "bybit", "okx"))
    no_okx = build_external_consensus(frame, included_venues=("bitget", "bybit"))

    assert full.loc[0, "xv_spot_mid_move_bps_100ms"] == 1.2
    assert no_okx.loc[0, "xv_spot_mid_move_bps_100ms"] == 1.1


def test_chronological_folds_keep_embargo_and_late_untouched() -> None:
    days = [f"2026-01-{day:02d}" for day in range(1, 21)]
    folds = chronological_folds(
        days,
        min_train_days=8,
        test_days=3,
        embargo_days=1,
        late_days=4,
    )

    assert folds[0].train_days[-1] == "2026-01-08"
    assert folds[0].embargo_days == ("2026-01-09",)
    assert folds[0].test_days == ("2026-01-10", "2026-01-11", "2026-01-12")
    late = folds[-1]
    assert late.panel == "late_holdout"
    assert late.embargo_days == ("2026-01-16",)
    assert late.test_days == tuple(days[-4:])


def test_m1_detects_incremental_external_signal_on_future_days() -> None:
    rng = np.random.default_rng(7)
    rows = []
    for day_index in range(24):
        day = f"2026-01-{day_index + 1:02d}"
        for row_index in range(20):
            external = rng.normal()
            row = {
                "day": day,
                "fill_id": day_index * 20 + row_index,
                "fill_ts": day_index * 1000 + row_index,
                "side": "BUY" if row_index % 2 == 0 else "SELL",
                "qty": 0.001,
                "position_after": 0.001 if row_index % 2 == 0 else -0.001,
                "inventory_role": "opener",
                "market_data_latency_mode": "captured",
                "execution_flow_pressure_100ms": rng.normal(),
                "execution_mid_move_bps_100ms": rng.normal(),
                "bridge_flow_pressure_100ms": rng.normal(),
                "bridge_mid_move_bps_100ms": rng.normal(),
                "markout_100ms_bps": 2.0 * external + rng.normal(scale=0.15),
            }
            for venue in ("bitget", "bybit", "okx"):
                for factor in ("spot", "perp"):
                    row[f"{venue}_{factor}_book_fresh_100ms"] = 1
                    row[f"{venue}_{factor}_flow_pressure_100ms"] = external + rng.normal(scale=0.05)
                    row[f"{venue}_{factor}_mid_move_bps_100ms"] = external + rng.normal(scale=0.05)
                    row[f"{venue}_{factor}_trade_imbalance_100ms"] = external
                    row[f"{venue}_{factor}_l1_ofi_normalized_100ms"] = external
                    for name in ("bid_depletion", "bid_refill", "ask_depletion", "ask_refill"):
                        row[f"{venue}_{factor}_{name}_100ms"] = 0.0
            for name, value in {
                "inventory_before": 0.0,
                "inventory_after": row["position_after"],
                "abs_inventory_before": 0.0,
                "campaign_age_s_before": 0.0,
                "campaign_max_abs_inventory_before": 0.0,
                "campaign_add_count_before": 0.0,
                "campaign_fill_count_before": 0.0,
            }.items():
                row[name] = value
            rows.append(row)
    frame = pd.DataFrame(rows)
    folds = chronological_folds(
        frame["day"].tolist(),
        min_train_days=10,
        test_days=4,
        embargo_days=1,
        late_days=4,
    )

    result = pd.DataFrame(
        evaluate_incremental(
            frame,
            folds=folds,
            extreme_adverse_bps=-1.0,
            min_train_rows=100,
            min_test_rows=20,
        )
    )
    selected = result[
        (result["venue_set"] == "full")
        & (result["side"] == "ALL")
        & (result["inventory_role"] == "ALL")
        & (result["target"] == "markout_100ms_bps")
    ]

    assert len(selected) >= 2
    assert selected["mae_improvement_m1_vs_m0"].median() > 0.5
