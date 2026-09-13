import hashlib
import json
from dataclasses import replace
from decimal import Decimal

import pytest

from data.observation import DeliveryQueue, LatencyProfile
from data.runtime import MeasuredDelivery, ObservationProfile, _require_observation_scenario
from data.tardis_input import BookView


def profile(tmp_path):
    groups = [dict(market_id="measured", event_type=c, transport="websocket", rows=2,
        simulation_clock_pair_columns=["transport_lag_ms", "feature_latency_ms"],
        simulation_clock_pair_semantics="all_observed_same_message_pairs",
        simulation_clock_pair_samples_ms=[[2., 3.], [5., 7.]]) for c in ("depth", "trade")]
    path = tmp_path / "timing.json"
    path.write_text(json.dumps(dict(schema="market_data_latency_profile.v1", groups=groups)))
    return ObservationProfile("paired", "source_timestamp_proxy", 0, 0, 100, 100,
        measured_latency_path=str(path), measured_latency_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        measured_latency_market_id="measured")


def test_paired_draw_identity_and_no_fallback(tmp_path):
    p = profile(tmp_path)
    a, b = MeasuredDelivery(p), MeasuredDelivery(p)
    expected = [a.draw("depth", str(i)) for i in range(30)]
    assert expected == [b.draw("depth", str(i)) for i in range(30)]
    assert expected[20:] == [MeasuredDelivery(p).draw("depth", str(i)) for i in range(20, 30)]
    assert all((x["market_delay_ns"], x["processing_ns"]) in
               {(2_000_000, 3_000_000), (5_000_000, 7_000_000)} for x in expected)
    with pytest.raises(ValueError, match="double count"):
        replace(p, processing_ns=1)
    with pytest.raises(ValueError, match="binding changed"):
        MeasuredDelivery(replace(p, measured_latency_sha256="wrong"))


def test_measured_profile_cannot_cross_market(tmp_path):
    measured = replace(profile(tmp_path), measured_latency_market_id="binance:perp:BTCUSDC")
    measured.require_market("binance_futures:perpetual:BTCUSDC")
    with pytest.raises(ValueError, match="market identity"):
        measured.require_market("binance_futures:perpetual:BTCUSDT")
    with pytest.raises(ValueError, match="market identity"):
        measured.require_market("binance_futures:spot:BTCUSDC")


def test_declared_reference_simulation_is_not_measured(tmp_path):
    market = "binance_futures:perpetual:BTCUSDT"
    simulated = ObservationProfile("f04_btcusdt_simulated_v1", "source_timestamp_proxy",
                                   250_000_000, 1_000_000, 100_000_000, 1_000_000_000)
    scenario = {"schema": "data.observation_scenario.v1", "market_id": market,
                "classification": "simulated_not_measured", "parameter_basis": "frozen before outcomes",
                "provider_local_timestamp_as_receive": False,
                "native_observation_parity": "not_proven"}
    plan = {"market_id": market, "observation_scenario": scenario}
    _require_observation_scenario(plan, simulated)
    with pytest.raises(ValueError, match="identity/market"):
        _require_observation_scenario({**plan, "market_id": "binance_futures:perpetual:BTCUSDC"}, simulated)
    with pytest.raises(ValueError, match="positive delay"):
        _require_observation_scenario(plan, replace(simulated, market_delay_ns=0))
    with pytest.raises(ValueError, match="positive delay"):
        _require_observation_scenario(plan, replace(simulated, measured_latency_path=str(tmp_path / "measured.json"),
            measured_latency_sha256="x", measured_latency_market_id="binance:perp:BTCUSDT",
            market_delay_ns=0, processing_ns=0))


def test_per_message_service_retains_queue_contention():
    q = DeliveryQueue(LatencyProfile(999, 999, "overridden"))
    view = BookView(1, ((Decimal(1), Decimal(1)),), ((Decimal(2), Decimal(1)),), 0, 0, "unknown", True)
    for event, lag, service in (("a", 2, 10), ("b", 3, 4)):
        q.publish(event_id=event, market_id="m", input_contract_id="i", origin="synthetic",
            source_asof_ns=0, publish_ns=0, payload=view, market_delay_ns=lag, processing_ns=service)
    assert not q.advance(11)
    assert [(o.event_id, o.receive_ns, o.ready_ns) for o in q.advance(20)] == [("a", 2, 12), ("b", 3, 16)]
