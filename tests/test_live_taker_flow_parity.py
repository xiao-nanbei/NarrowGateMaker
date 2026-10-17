from research.families.f08_side_taker_lifecycle.audit.live_taker_flow_parity import audit_trade_rows


def _trade_row(stream_type: str, **updates):
    row = {
        "market_id": "binance:perp:BTCUSDC",
        "event_type": "trade",
        "trade_stream_type": stream_type,
        "exchange_event_ts_ns": 1_000_000_000,
        "local_receive_ts_ns": 1_001_000_000,
        "feature_ready_ts_ns": 1_002_000_000,
        "price": 60_000.0,
        "size": 0.1,
        "aggressor_side": "sell",
    }
    row.update(updates)
    return row


def test_exact_individual_aggregate_receive_parity_passes() -> None:
    rows = [
        _trade_row("individual", trade_id=100, size=0.1),
        _trade_row(
            "individual",
            trade_id=101,
            size=0.2,
            exchange_event_ts_ns=1_000_100_000,
            local_receive_ts_ns=1_001_100_000,
            feature_ready_ts_ns=1_002_100_000,
        ),
        _trade_row(
            "aggregate",
            aggregate_trade_id=7,
            first_trade_id=100,
            last_trade_id=101,
            size=0.3,
            exchange_event_ts_ns=1_000_200_000,
            local_receive_ts_ns=1_001_200_000,
            feature_ready_ts_ns=1_002_200_000,
        ),
    ]

    summary = audit_trade_rows(
        rows,
        market_id="binance:perp:BTCUSDC",
        individual_source_identity="licensed_individual_receive_feed.test.v1",
        min_matched_aggregates=1,
    )

    assert summary["status"] == "passed"
    assert summary["live_taker_flow_parity_passed"] is True
    assert summary["parity"]["child_id_coverage"] == 1.0
    assert summary["parity"]["quantity_match_rate"] == 1.0
    assert summary["dynamic_fill_hazard_allowed"] is False


def test_legacy_aggtrade_only_tape_fails_closed() -> None:
    rows = [_trade_row("", trade_id="7")]

    summary = audit_trade_rows(
        rows,
        market_id="binance:perp:BTCUSDC",
        min_matched_aggregates=1,
    )

    assert summary["status"] == "blocked"
    assert "individual_rows_present" in summary["failed_gates"]
    assert "no_untyped_trade_rows" in summary["failed_gates"]
    assert "explicit_individual_source_identity" in summary["failed_gates"]
