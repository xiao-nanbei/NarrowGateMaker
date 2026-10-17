import pandas as pd
import pytest

from models.alpha_evidence_ledger import _bucket_stability, _fill_evidence, _order_evidence, _side_regime_evidence, _summary_evidence
from models.quote_decomposition_tick import _mean_bool


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


def _censored_fills() -> pd.DataFrame:
    rows = []
    for index, (qty, value, status, toxic) in enumerate([
        (0.001, 10.0, "exact", "False"),
        (0.009, float("nan"), "censored", None),
    ]):
        rows.append({"day": "2026-07-01", "side": "BUY", "order_id": str(index),
                     "fill_qty": qty, "ev_30s": value, "toxic_30s": toxic,
                     "age_ms": 0, "quote_dist": 0, "final_quote_delta_to_bbo": 0,
                     "near_depth_total": 1, "queue_local_rank": 1,
                     **{f"markout_{h}": value for h in ("1s", "5s", "30s")},
                     **{f"markout_{h}_status": status for h in ("1s", "5s", "30s")}})
    return pd.DataFrame(rows)


def test_censoring_retains_fill_denominator_and_uses_known_quantity(tmp_path) -> None:
    path = tmp_path / "fills.csv"
    _censored_fills().to_csv(path, index=False)
    fills = pd.read_csv(path)
    daily, rollup, _ = _fill_evidence(fills, min_fills=1)
    for frame in (daily, rollup, _side_regime_evidence(pd.DataFrame(), fills, min_fills=1)):
        row = frame.iloc[0]
        assert row["fills"] == 2
        assert row["filled_qty_btc"] == pytest.approx(0.01)
        assert row["ev_30s_valid_count"] == 1
        assert row["ev_30s_valid_qty_btc"] == 0.001
        assert row["markout_30s_censored_count"] == 1
        assert row["avg_ev_30s_usdc_per_btc"] == 10.0
        assert row["sum_ev_30s_usdc"] == 0.01
        assert row["positive_30s_rate"] == 1.0
    assert rollup.iloc[0]["toxic_30s_rate"] == 0.0
    summary = _summary_evidence(pd.DataFrame([{"day": "2026-07-01"}]), pd.DataFrame(), fills).iloc[0]
    assert summary["fills"] == 2
    assert summary["avg_ev_30s"] == 10.0
    assert summary["markout_30s_censored_count"] == 1


def test_all_unknown_remains_unknown_in_rollup_stability_and_summary() -> None:
    fills = _censored_fills().iloc[[1]].copy()
    daily, rollup, positive = _fill_evidence(fills, min_fills=1)
    row = rollup.iloc[0]
    assert row["fills"] == 1
    assert pd.isna(row["avg_ev_30s"])
    assert pd.isna(row["positive_30s_rate"])
    assert pd.isna(row["toxic_30s_rate"])
    assert pd.isna(row["sum_ev_30s_usdc"])
    assert row["alpha_evidence"] == "unknown"
    assert positive.empty
    stable = _bucket_stability(daily, min_fills=1).iloc[0]
    assert stable["verdict"] == "unknown"
    assert stable["unknown_days"] == 1
    assert pd.isna(stable["weighted_avg_ev_30s"])
    summary = _summary_evidence(pd.DataFrame([{"day": "2026-07-01"}]), pd.DataFrame(), fills).iloc[0]
    assert pd.isna(summary["avg_ev_30s"])
    assert pd.isna(summary["toxic_30s_rate"])


def test_nullable_boolean_strings_do_not_use_python_truthiness() -> None:
    assert _mean_bool(pd.Series(["False", "True", None, "unknown", float("nan")])) == 0.5
    assert pd.isna(_mean_bool(pd.Series([None, "unknown", float("nan")])))


def test_order_report_uses_joined_fill_diagnostics_and_preserves_unknown() -> None:
    fills = _censored_fills()
    orders = fills.drop(columns=[c for c in fills if c.startswith(("markout_", "ev_", "toxic_"))]).copy()
    orders["outcome"] = "fill"
    orders["lifetime_ms"] = 5
    _, report = _order_evidence(orders, fills)
    assert report.iloc[0]["orders"] == 2
    assert report.iloc[0]["filled_orders"] == 2
    assert report.iloc[0]["avg_fill_ev_30s"] == 10.0


def test_delayed_estimates_remain_explicit_not_fixed_horizon() -> None:
    fills = _censored_fills().iloc[[0]].copy()
    fills["markout_30s_status"] = "delayed"
    _, report, _ = _fill_evidence(fills, min_fills=1)
    assert report.iloc[0]["markout_30s_delayed_count"] == 1
    assert report.iloc[0]["markout_30s_exact_count"] == 0
    assert report.iloc[0]["avg_markout_30s"] == 10.0
