from decimal import Decimal

import pytest

from live.feature_protocol import (
    LIVE_INPUT_CONTRACT, LiveExecutionFeatures, validate_live_feature_support,
)

NS = 1_000_000_000


def adapter():
    value = LiveExecutionFeatures(symbol="BTCUSDC", start_ns=0,
                                  allowed_lateness_ns=0, max_book_age_ns=NS)
    value.connection(connected=True, now_ns=0)
    return value


def trade(i, *, first=None, last=None):
    return dict(e="aggTrade", s="BTCUSDC", a=i, T=i*1000+100, E=i*1000+101,
                p="100", q="9", m=False, f=i*9 if first is None else first,
                l=i*9+8 if last is None else last)


def depth():
    return dict(e="depthUpdate", s="BTCUSDC", T=0, E=0, u=1,
                b=[[str(100-i), "1"] for i in range(20)],
                a=[[str(101+i), "2"] for i in range(20)])


def individual(i):
    return {k: v for k, v in {**trade(i), "e": "trade", "t": i}.items()
            if k not in {"a", "f", "l"}}


def test_individual_identity_gap_and_source_mixing():
    value = adapter()
    assert value.individual_trade(individual(0), receive_ns=102_000_000, ready_ns=103_000_000)
    assert not value.individual_trade(individual(0), receive_ns=104_000_000, ready_ns=105_000_000)
    with pytest.raises(ValueError, match="conflicting"):
        value.individual_trade({**individual(0), "q": "8"}, receive_ns=106_000_000, ready_ns=107_000_000)
    with pytest.raises(ValueError, match="mix"):
        value.aggregate_trade(trade(1), receive_ns=NS+102_000_000, ready_ns=NS+103_000_000)
    generation = value.generation
    value.individual_trade(individual(2), receive_ns=2*NS+102_000_000, ready_ns=2*NS+103_000_000)
    assert value.generation == generation + 1
    assert dict(value.frame(3*NS).values)["volume_10s"] is None


@pytest.mark.parametrize("mutation", [{"T": 100.5}, {"f": 0}, {"t": True}, {"q": "-1"}])
def test_bad_individual_does_not_commit_identity(mutation):
    value = adapter()
    with pytest.raises(ValueError):
        value.individual_trade({**individual(0), **mutation}, receive_ns=102_000_000, ready_ns=103_000_000)
    assert value.last_individual_id is None


def test_individual_counts_and_explicit_compatible_source():
    value = adapter()
    for i in range(10):
        value.individual_trade(individual(i), receive_ns=i*NS+102_000_000, ready_ns=i*NS+103_000_000)
    row = dict(value.frame(10*NS).values)
    assert row["individual_count_10s"] == row["observed_trade_count_10s"] == 10
    assert row["volume_10s"] == 90
    from data.tardis_input import CONTRACT
    validate_live_feature_support({"input_contract_id": CONTRACT, "heads": {
        "x": {"feature_cols": ["observed_trade_count_10s"]}}}, trade_source="individual")


def test_same_shared_calculator_preserves_native_packet_and_derived_counts():
    value = adapter()
    for i in range(10):
        value.aggregate_trade(trade(i), receive_ns=i*NS+102_000_000,
                              ready_ns=i*NS+103_000_000)
    frame = value.frame(10*NS)
    values = dict(frame.values)
    assert frame.input_contract_id == LIVE_INPUT_CONTRACT
    assert values["individual_count_10s"] == 90
    assert values["observed_trade_count_10s"] == 10
    assert values["native_packet_count_10s"] == 10
    assert values["volume_10s"] == 90
    assert frame.max_dependency_ready_ns <= frame.cutoff_ns


def test_duplicate_does_not_add_volume():
    value = adapter()
    event = trade(0)
    assert value.aggregate_trade(event, receive_ns=102_000_000, ready_ns=103_000_000)
    assert not value.aggregate_trade(event, receive_ns=104_000_000, ready_ns=105_000_000)
    value.frame(NS)
    assert value.features.bars[value.market_id][0].volume == 9


def test_future_packet_rejected_without_consuming_identity():
    value = adapter()
    with pytest.raises(ValueError, match="causal"):
        value.aggregate_trade(trade(0), receive_ns=99_000_000, ready_ns=100_000_000)
    assert value.trades.last_packet is None


def test_gap_resets_rolling_support():
    value = adapter()
    value.aggregate_trade(trade(0), receive_ns=102_000_000, ready_ns=103_000_000)
    value.frame(NS)
    value.aggregate_trade(trade(2), receive_ns=2*NS+102_000_000, ready_ns=2*NS+103_000_000)
    assert value.trades.id_gaps == 1
    assert dict(value.frame(3*NS).values)["volume_10s"] is None
    assert not value.features.bars


def test_disconnect_invalidates_book_and_support():
    value = adapter()
    value.partial_depth(depth(), receive_ns=0, ready_ns=0)
    assert dict(value.frame(0).values)["mid"] == Decimal("100.5")
    value.connection(connected=False, now_ns=1)
    assert dict(value.frame(1).values)["mid"] is None
    with pytest.raises(ValueError, match="disconnected"):
        value.aggregate_trade(trade(0), receive_ns=102_000_000, ready_ns=103_000_000)


def test_book_identity_staleness_and_incomplete_depth():
    value = adapter()
    event = depth()
    event["b"] = event["b"][:5]
    event["a"] = event["a"][:5]
    assert value.partial_depth(event, receive_ns=0, ready_ns=0)
    assert not value.partial_depth(event, receive_ns=1, ready_ns=1)
    assert dict(value.frame(1).values)["depth_bid_20"] is None
    assert dict(value.frame(2*NS).values)["mid"] is None


def test_clock_gap_requires_explicit_coverage_reset():
    with pytest.raises(ValueError, match="coverage reset"):
        adapter().frame(121*NS)


def test_reject_model_packet_semantic_substitution():
    with pytest.raises(ValueError, match="individual-source event counts"):
        validate_live_feature_support(dict(heads={"x": dict(
            feature_cols=["observed_trade_count_10s", "trade_intensity_burst_guard"])}))
    with pytest.raises(ValueError, match="compatibility"):
        validate_live_feature_support(dict(heads={"x": dict(feature_cols=["mid"])}))


@pytest.mark.parametrize("individual_source", [False, True])
def test_shared_historical_calculator_matches_single_execution_packets(individual_source):
    from data.observation import ExecutionFeatures, Observation, TradeContribution, VisibleTradeWindows
    value = adapter()
    historical = ExecutionFeatures("historical_test", market_id=value.market_id, max_book_age_ns=NS)
    windows = VisibleTradeWindows(start_ns=0, allowed_lateness_ns=0,
                                 market_id=value.market_id, coverage="observed",
                                 input_contract_id="historical_test")
    for i in range(60):
        event = trade(i, first=i, last=i)
        ready = i*NS+103_000_000
        if individual_source:
            value.individual_trade(individual(i), receive_ns=ready-1_000_000, ready_ns=ready)
        else:
            value.aggregate_trade(event, receive_ns=ready-1_000_000, ready_ns=ready)
        contribution = TradeContribution(str(i), i*NS+100_000_000, Decimal(100),
                                         Decimal(9), "buy", 1, None, "source_trade_event", i)
        observation = Observation(str(i), value.market_id, "historical_test", "source_trade_event",
                                  i*NS+100_000_000, i*NS+101_000_000,
                                  ready-1_000_000, ready, contribution, "observed")
        bars = windows.advance(ready, (observation,))
        historical.advance(ready, (), ((value.market_id, b) for b in bars))
    bars = windows.advance(60*NS)
    historical.advance(60*NS, (), ((value.market_id, b) for b in bars))
    left, right = dict(value.frame(60*NS).values), dict(historical.frame(60*NS).values)
    # Source packet observability differs, and is not used by the frozen model.
    left.pop("native_packet_count_10s")
    right.pop("native_packet_count_10s")
    assert left == right


def test_invalid_depth_and_bad_clock_preserve_committed_identity():
    value = adapter()
    event = depth()
    event["a"][0][0] = "99"
    with pytest.raises(ValueError, match="crossed"):
        value.partial_depth(event, receive_ns=0, ready_ns=0)
    assert value.depth_id is None
    event = trade(0)
    event["T"] = 100.5
    with pytest.raises(ValueError, match="integer"):
        value.aggregate_trade(event, receive_ns=102_000_000, ready_ns=103_000_000)
    assert value.trades.last_packet is None
