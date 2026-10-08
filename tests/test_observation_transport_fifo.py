"""Ordered transport tests using synthetic messages, not private samples."""

from dataclasses import replace
from decimal import Decimal
import pickle

import pytest

from data.observation import DeliveryQueue, LatencyProfile, TradeContribution
from data.runtime import ObservationProfile


def publish(queue, identity, sent, delay, connection="ws:epoch0", service=0):
    payload = TradeContribution(str(identity), sent, Decimal(100), Decimal(1),
                                "buy", 1, None, "synthetic", identity)
    queue.publish(event_id=str(identity), market_id="market", input_contract_id="input",
                  origin="synthetic", source_asof_ns=sent, publish_ns=sent, payload=payload,
                  market_delay_ns=delay, processing_ns=service, connection_id=connection)


def queue():
    return DeliveryQueue(LatencyProfile(0, 0, "synthetic"))


def test_later_fast_message_waits_for_earlier_slow_message_without_epsilon():
    q = queue()
    publish(q, 1, 0, 209)
    publish(q, 2, 100, 10)
    assert q.advance(208) == ()
    observations = q.advance(209)
    assert [o.event_id for o in observations] == ["1", "2"]
    assert [o.receive_ns for o in observations] == [209, 209]
    assert [o.ready_ns for o in observations] == [209, 209]
    assert q.transport_delayed_messages == 1
    assert q.transport_wait_ns == q.max_transport_wait_ns == 99


def test_service_is_charged_once_after_ordered_transport():
    q = queue()
    publish(q, 1, 0, 209, service=2)
    publish(q, 2, 100, 10, service=3)
    assert q.advance(210) == ()
    assert q.advance(211)[0].event_id == "1"
    assert q.advance(213) == ()
    second = q.advance(214)[0]
    assert second.receive_ns == 209 and second.ready_ns == 214


@pytest.mark.parametrize("connection", [None, "another:epoch0", "ws:epoch1"])
def test_other_connection_or_epoch_and_legacy_mode_do_not_share_order(connection):
    q = queue()
    publish(q, 1, 0, 209)
    publish(q, 2, 100, 10, connection)
    assert q.advance(110)[0].event_id == "2"
    assert q.advance(209)[0].event_id == "1"
    assert q.transport_delayed_messages == 0


def test_send_order_is_not_source_version_sorting():
    q = queue()
    publish(q, 7, 100, 0)
    publish(q, 3, 100, 0)
    assert [o.event_id for o in q.advance(100)] == ["7", "3"]


def test_backwards_send_rejected_not_silently_reordered():
    q = queue()
    publish(q, 1, 100, 50)
    with pytest.raises(ValueError, match="send order"):
        publish(q, 2, 99, 0)


def test_invalid_observation_does_not_commit_connection_watermark():
    q = queue()
    payload = TradeContribution("valid", 0, Decimal(100), Decimal(1), "buy", 1, None, "synthetic", 0)
    with pytest.raises(ValueError, match="identity"):
        q.publish(event_id="", market_id="market", input_contract_id="input", origin="synthetic",
                  source_asof_ns=0, publish_ns=0, payload=payload, market_delay_ns=999,
                  connection_id="ws:epoch0")
    publish(q, 1, 0, 0)
    assert q.advance(0)[0].receive_ns == 0
    assert q.transport_delayed_messages == 0


def test_queue_pickle_restores_order_and_forks_independently():
    q = queue()
    publish(q, 1, 0, 209)
    saved = pickle.dumps(q)
    left, right = pickle.loads(saved), pickle.loads(saved)
    publish(left, 2, 100, 10)
    publish(right, 2, 100, 10)
    assert left.advance(209) == right.advance(209)
    assert len(q.advance(209)) == 1
    assert left._connection_watermarks is not right._connection_watermarks


def test_chunked_advance_and_one_advance_agree():
    a, b = queue(), queue()
    for q in (a, b):
        publish(q, 1, 0, 209, service=2)
        publish(q, 2, 100, 10, service=3)
        publish(q, 3, 150, 100, service=1)
    chunked = tuple(x for t in (100, 209, 211, 214, 300) for x in a.advance(t))
    assert chunked == b.advance(300)


def test_profile_requires_explicit_complete_connection_mapping():
    p = ObservationProfile("synthetic", "source_timestamp_proxy", 0, 0, 0, 1000)
    assert p.depth_connection_id is p.trade_connection_id is None
    with pytest.raises(ValueError, match="both depth and trade"):
        replace(p, depth_connection_id="depth")
    assert replace(p, depth_connection_id="market", trade_connection_id="market")
    assert replace(p, depth_connection_id="depth", trade_connection_id="trade")


def test_real_input_scheduler_uses_connection_mapping_without_resampling(monkeypatch):
    import data.runtime as runtime
    from data.tardis_input import BookMessage, BookView

    calls = []

    class Samples:
        def __init__(self, profile):
            pass

        def draw(self, channel, event_id):
            calls.append((channel, event_id))
            return {"market_delay_ns": 200_000_000 if event_id == "depth:0" else 0,
                    "processing_ns": 0}

    monkeypatch.setattr(runtime, "MeasuredDelivery", Samples)
    market = "binance_futures:perpetual:BTCUSDC"
    book = BookMessage(market, "synthetic", 1, 2, 0, "group", "snapshot", 0,
                       "unknown", None, 0, "complete", "unknown",
                       (("bid", Decimal(99), Decimal(1)), ("ask", Decimal(101), Decimal(1))))
    profile = ObservationProfile("synthetic", "source_timestamp_proxy", 0, 0, 0,
        1_000_000_000, measured_latency_path="synthetic", measured_latency_sha256="synthetic",
        measured_latency_market_id="binance:perp:BTCUSDC")

    def run(selected):
        stream = runtime.PublicInputStream([], profile=selected, start_ns=0,
            end_ns=300_000_000, market_id=market, input_contract_id="synthetic")
        stream._channel = lambda channel: iter([(0, book)] if channel == "incremental_book_L2" else [])
        observations = [o for t in stream for o in t.observations if isinstance(o.payload, BookView)]
        return observations, stream.stats

    old, _ = run(profile)
    old_calls = list(calls)
    calls.clear()
    ordered, stats = run(replace(profile, depth_connection_id="depth", trade_connection_id="trade"))
    assert [o.publish_ns for o in old] == [100_000_000, 0, 200_000_000]
    assert [o.publish_ns for o in ordered] == [0, 100_000_000, 200_000_000]
    assert [o.receive_ns for o in ordered] == [200_000_000] * 3
    assert calls == old_calls  # exact same draw count, channel and event identities
    assert stats["transport_delayed_messages"] == 1
    assert stats["transport_wait_ns"] == 100_000_000
