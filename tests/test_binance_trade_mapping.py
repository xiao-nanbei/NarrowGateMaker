import pandas as pd
import pytest

from research.families.f08_side_taker_lifecycle.audit.binance_trade_mapping import (
    SOURCE_CONTRACT_ID,
    build_individual_aggtrade_mapping,
)


def _individual() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [100, 101, 102],
            "price": [60_000.0, 60_000.0, 60_000.1],
            "qty": [0.1, 0.2, 0.4],
            "time": [1_000, 1_000, 1_001],
            "is_buyer_maker": [True, True, False],
        }
    )


def _aggregate() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "agg_trade_id": [7, 8],
            "price": [60_000.0, 60_000.1],
            "quantity": [0.3, 0.5],
            "normal_quantity": [0.3, 0.4],
            "first_trade_id": [100, 102],
            "last_trade_id": [101, 102],
            "transact_time": [1_000, 1_001],
            "is_buyer_maker": [True, False],
            "feature_ready_ts_ns": [1_005_000_000, 1_006_000_000],
        }
    )


def test_exact_mapping_uses_aggregate_visibility_clock() -> None:
    mapped, summary = build_individual_aggtrade_mapping(
        _individual(),
        _aggregate(),
    )

    assert summary["status"] == "passed"
    assert summary["source_contract_id"] == SOURCE_CONTRACT_ID
    assert summary["quantity_identity_counts"] == {
        "q_and_nq": 1,
        "nq_normal_only": 1,
    }
    assert mapped["agg_trade_id"].tolist() == [7, 7, 8]
    assert mapped["feature_ready_ts_ns"].tolist() == [
        1_005_000_000,
        1_005_000_000,
        1_006_000_000,
    ]
    assert mapped["exchange_ts_ms"].tolist() == [1_000, 1_000, 1_001]


def test_mapping_fails_closed_on_missing_trade_id() -> None:
    individual = _individual().query("id != 101")

    with pytest.raises(ValueError, match="strict individual↔aggTrade mapping"):
        build_individual_aggtrade_mapping(individual, _aggregate())


def test_mapping_fails_closed_on_side_mismatch() -> None:
    individual = _individual()
    individual.loc[individual["id"] == 101, "is_buyer_maker"] = False

    with pytest.raises(ValueError, match="strict individual↔aggTrade mapping"):
        build_individual_aggtrade_mapping(individual, _aggregate())


def test_historical_aggregate_timestamp_uses_last_child_visibility_floor() -> None:
    individual = _individual().iloc[:2].copy()
    individual.loc[individual.index[1], "time"] = 1_005
    aggregate = _aggregate().iloc[:1].drop(
        columns=["normal_quantity", "feature_ready_ts_ns"]
    )

    mapped, summary = build_individual_aggtrade_mapping(
        individual,
        aggregate,
        feature_ready_latency_ms=7.0,
        feature_ready_latency_profile_id="aws_tokyo_2v4g.test.v1",
    )

    assert mapped["feature_ready_ts_ns"].eq(1_012_000_000).all()
    assert set(mapped["feature_ready_source"]) == {
        "last_child_plus_frozen_latency"
    }
    assert summary["policy_feature_timing_eligible"] is True


def test_internal_trade_id_gap_is_excluded_from_exact_queue_outcomes() -> None:
    individual = pd.DataFrame(
        {
            "id": [100, 102],
            "price": [60_000.0, 60_000.0],
            "qty": [0.2, 0.3],
            "time": [1_000, 1_001],
            "is_buyer_maker": [True, True],
        }
    )
    aggregate = pd.DataFrame(
        {
            "agg_trade_id": [7],
            "price": [60_000.0],
            "quantity": [0.5],
            "first_trade_id": [100],
            "last_trade_id": [102],
            "transact_time": [1_000],
            "is_buyer_maker": [True],
        }
    )

    mapped, summary = build_individual_aggtrade_mapping(individual, aggregate)

    assert mapped["queue_outcome_exact"].eq(False).all()
    assert summary["nonexact_trade_id_range_aggregate_rows"] == 1
    assert summary["queue_outcome_day_strict_eligible"] is False
    with pytest.raises(ValueError, match="trade_ids_contiguous"):
        build_individual_aggtrade_mapping(
            individual,
            aggregate,
            require_exact_trade_id_coverage=True,
        )
