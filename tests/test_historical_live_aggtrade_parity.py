import pandas as pd

from research.families.f08_side_taker_lifecycle.audit.historical_live_aggtrade_parity import (
    LIVE_SOURCE_CONTRACT_ID,
    REST_PARENT_CONTRACT_ID,
    audit_historical_live_aggtrade_parity,
)


def _historical() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "agg_trade_id": [7, 8],
            "price": [60_000.0, 60_000.1],
            "quantity": [0.3, 0.4],
            "first_trade_id": [100, 102],
            "last_trade_id": [101, 102],
            "transact_time": [1_000, 1_001],
            "is_buyer_maker": [True, False],
        }
    )


def _live_row(aggregate_id: int, **updates):
    first_id = 100 if aggregate_id == 7 else 102
    last_id = 101 if aggregate_id == 7 else 102
    row = {
        "market_id": "binance:perp:BTCUSDC",
        "event_type": "trade",
        "trade_stream_type": "aggregate",
        "trade_payload_schema_version": "binance_usdm_aggtrade.v2",
        "trade_source_contract_id": LIVE_SOURCE_CONTRACT_ID,
        "aggregate_trade_id": aggregate_id,
        "first_trade_id": first_id,
        "last_trade_id": last_id,
        "individual_trade_count_from_id_range": last_id - first_id + 1,
        "sequence_number": aggregate_id,
        "price": 60_000.0 if aggregate_id == 7 else 60_000.1,
        "size": 0.3 if aggregate_id == 7 else 0.4,
        "aggressor_side": "sell" if aggregate_id == 7 else "buy",
        "exchange_event_ts_ns": (1_000 if aggregate_id == 7 else 1_001)
        * 1_000_000,
        "local_receive_ts_ns": (1_005 if aggregate_id == 7 else 1_006)
        * 1_000_000,
        "feature_ready_ts_ns": (1_006 if aggregate_id == 7 else 1_007)
        * 1_000_000,
    }
    row.update(updates)
    return row


def test_supported_aggregate_two_clock_contract_passes() -> None:
    rows, summary = audit_historical_live_aggtrade_parity(
        _historical(),
        [_live_row(7), _live_row(8)],
        min_matched_aggregates=2,
    )

    assert summary["status"] == "passed"
    assert summary["source_contract"]["individual_receive_stream_required"] is False
    assert summary["parity"]["lineage_match_rate"] == 1.0
    assert summary["historical_parent_exact_payload_replay_passed"] is True
    assert summary["dynamic_fill_hazard_allowed"] is False
    assert rows["causal_live_clock"].all()


def test_same_day_rest_parent_contract_is_recorded() -> None:
    _, summary = audit_historical_live_aggtrade_parity(
        _historical(),
        [_live_row(7), _live_row(8)],
        min_matched_aggregates=2,
        historical_parent_contract_id=REST_PARENT_CONTRACT_ID,
    )

    assert summary["status"] == "passed"
    assert summary["source_contract"]["historical_parent"] == REST_PARENT_CONTRACT_ID


def test_legacy_capture_is_diagnostic_but_not_formal_parity() -> None:
    legacy = _live_row(7)
    for name in (
        "trade_stream_type",
        "trade_payload_schema_version",
        "trade_source_contract_id",
        "aggregate_trade_id",
        "first_trade_id",
        "last_trade_id",
        "individual_trade_count_from_id_range",
    ):
        legacy.pop(name)
    legacy["trade_id"] = "7"

    _, summary = audit_historical_live_aggtrade_parity(
        _historical(),
        [legacy],
        min_matched_aggregates=1,
    )

    assert summary["status"] == "blocked"
    assert summary["parity"]["price_match_rate"] == 1.0
    assert "canonical_parent_prefix_compatibility" in summary["failed_gates"]
    assert "supported_live_source_contract" in summary["failed_gates"]
    assert "complete_f_l_lineage" in summary[
        "failed_historical_exact_replay_gates"
    ]


def test_quantity_mismatch_fails_closed() -> None:
    _, summary = audit_historical_live_aggtrade_parity(
        _historical(),
        [_live_row(7, size=0.31), _live_row(8)],
        min_matched_aggregates=2,
    )

    assert summary["status"] == "blocked"
    assert "canonical_parent_prefix_compatibility" in summary["failed_gates"]
    assert "quantity_parity" in summary["failed_historical_exact_replay_gates"]


def test_live_prefix_passes_recorder_but_blocks_exact_historical_replay() -> None:
    historical = _historical()
    historical.loc[0, "quantity"] = 0.6
    historical.loc[0, "last_trade_id"] = 103

    _, summary = audit_historical_live_aggtrade_parity(
        historical.iloc[[0]],
        [_live_row(7)],
        min_matched_aggregates=1,
    )

    assert summary["status"] == "passed"
    assert summary["recorder_contract_parity_passed"] is True
    assert summary["historical_parent_exact_payload_replay_passed"] is False
    assert summary["parity"]["canonical_parent_extension_count"] == 1
    assert summary["failed_historical_exact_replay_gates"] == [
        "complete_f_l_lineage",
        "quantity_parity",
    ]
