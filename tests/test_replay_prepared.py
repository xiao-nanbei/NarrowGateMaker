"""Prepared replay isolation and numeric tape equivalence, synthetic inputs."""

from dataclasses import asdict

import numpy as np
import pytest

from test_research_public_inputs import bundle as input_bundle
from models import backtest_tick as replay

bundle = input_bundle


def parameters():
    return dict(
        gamma=0.01,
        kappa=1.0,
        order_size=0.001,
        max_inventory=0.01,
        requote_interval=1.0,
        rq_min=1.0,
        rq_max=1.0,
        requote_clock="fixed",
        maker_fee=0.0,
        taker_fee=0.0,
        tick_size=0.1,
        lot_size=0.001,
        queue_base=0.0,
        queue_decay=0.0,
        maker_fill_prob=1.0,
        use_bar_pricing=True,
        replay_event_clock="merged",
        replay_clock_interval_ms=100,
        exchange_book_queue_mode="diagnostic",
        public_fill_volume_policy="all_public_volume_eligible",
        max_exec_book_age_s=1.0,
        collect_curves=False,
        position_timeout=0.0,
        markout_ema_span_fills=0,
        account_start_ns=1_200_000_000,
    )


def test_prepared_a_b_a_and_readonly(bundle):
    p = parameters()
    prepared = replay.prepare_public_inputs(bundle, tick_size=0.1)
    first = replay.simulate_prepared_inputs(prepared, p)
    replay.simulate_prepared_inputs(prepared, {**p, "gamma": 0.02})
    repeat = replay.simulate_prepared_inputs(prepared, p)
    assert first == repeat == replay.simulate_public_inputs(bundle, p)
    with pytest.raises(ValueError):
        prepared.inputs["l2"].bid_px[0, 0] = 0
    with pytest.raises(ValueError, match="tick contract"):
        replay.simulate_prepared_inputs(prepared, {**p, "tick_size": 1.0})


def test_numeric_tape_cursor_cache_and_corruption(bundle, tmp_path):
    uncached = replay.prepare_public_inputs(bundle, tick_size=0.1)
    cached = replay.prepare_public_inputs(bundle, tick_size=0.1, cache_dir=tmp_path / "cache")
    tape = cached.inputs["exchange_book_event_tape"]
    expected = [asdict(e) for e in uncached.inputs["exchange_book_event_tape"]]
    assert [asdict(e) for e in tape] == expected == [asdict(e) for e in tape]
    assert replay.simulate_prepared_inputs(cached, parameters()) == replay.simulate_prepared_inputs(
        uncached, parameters()
    )
    array = np.load(tape.root / "events-0.npy", mmap_mode="r", allow_pickle=False)
    assert not array.flags.writeable
    with (tape.root / "events-0.npy").open("ab") as f:
        f.write(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        replay.prepare_public_inputs(bundle, tick_size=0.1, cache_dir=tmp_path / "cache")


def test_f01_prepares_once(bundle, monkeypatch):
    from research.families.f01_fixed_parameter_racing.public_input import (
        replay_parameter_candidates,
    )

    original = replay.load_public_inputs
    calls = []

    def load(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(replay, "load_public_inputs", load)
    results = replay_parameter_candidates(
        bundle, {"a": {"gamma": 0.01}, "b": {"gamma": 0.02}}, common_params=parameters()
    )
    assert len(calls) == 1 and len(results) == 2


def test_f01_ml_candidates_load_independent_engines_and_match_direct_b0(bundle, monkeypatch, tmp_path):
    from types import SimpleNamespace
    from research.families.f01_fixed_parameter_racing.public_input import (
        replay_parameter_candidates,
    )
    from strategy.signal import SignalEngine

    created = []

    class Engine:
        def __init__(self):
            self.calls = 0

        def compute_feature_frames(self, frames):
            self.calls += 1
            return [SimpleNamespace(dir_10s=0.5, vol_10s=0.0, ret_10s=0.0,
                                    tox_bid_10s=0.5, tox_ask_10s=0.5)
                    for _ in frames]

    def load(cls, model_dir, *, symbol, ret_demean_halflife):
        assert model_dir == tmp_path / "frozen" and symbol == "BTCUSDC"
        assert ret_demean_halflife == 0
        engine = Engine()
        created.append(engine)
        return engine

    monkeypatch.setattr(SignalEngine, "from_public_models", classmethod(load))
    params = {**parameters(), "ml_enabled": True, "vol_blend": 0.5,
              "ret_demean_halflife": 0}
    with pytest.raises(ValueError, match="explicit frozen model_dir"):
        replay_parameter_candidates(bundle, {"b0": {"gamma": params["gamma"]}},
                                    common_params=params)
    with pytest.raises(ValueError, match="loaded frozen P3 identity"):
        replay_parameter_candidates(bundle, {"b0": {"gamma": params["gamma"]}},
                                    common_params=params, model_dir=tmp_path / "frozen")
    params.update(fill_probability_calibrated=True, p3_identity_required=True,
                  p3_delta_star=1.0, p3_kappa_eff=0.05,
                  fill_probability_event_type="touch", fill_probability_horizon_s=10.0,
                  fill_probability_distance_origin="same_side_best_bid_or_ask_at_window_start",
                  fill_probability_distance_unit="USDC_per_BTC",
                  fill_probability_side="pooled_buy_sell",
                  fill_probability_queue_included=False,
                  fill_probability_artifact_sha256="a" * 64)
    arms = replay_parameter_candidates(
        bundle, {"b0": {"gamma": params["gamma"]},
                 "candidate": {"gamma": params["gamma"] * 1.2}},
        common_params=params, model_dir=tmp_path / "frozen",
    )
    assert len(created) == 2 and created[0] is not created[1]
    assert [engine.calls for engine in created] == [1, 1]
    direct = replay.simulate_public_inputs(bundle, params, signal_engine=Engine())
    assert arms["b0"] == direct


@pytest.mark.parametrize("change, mask, match", [
    ({"gamma": 0.02}, {"a_spread": 0.01}, "gamma is masked"),
    ({"gamma": 0.02}, {"quote_math_mode": "quantity_aware_v1"}, "gamma is masked"),
    ({"kappa": 2.0}, {"execution_intensity_slope": 1.0}, "kappa is masked"),
    ({"kappa": 2.0}, {"p3_kappa_eff": 3.0}, "kappa is masked"),
    ({"max_spread_bps": 25.0}, {"dynamic_cap_enabled": True,
                                  "dynamic_cap_base_bps": 20.0}, "max_spread_bps is masked"),
])
def test_f01_rejects_masked_parameter_aliases(bundle, change, mask, match):
    from research.families.f01_fixed_parameter_racing.public_input import (
        replay_parameter_candidates,
    )
    params = {**parameters(), "max_spread_bps": 20.0, **mask}
    with pytest.raises(ValueError, match=match):
        replay_parameter_candidates(bundle, {"candidate": change}, common_params=params)


def test_f01_unmasked_aliases_reach_effective_quote_coefficients():
    from research.families.f01_fixed_parameter_racing.public_input import (
        _validate_effective_quote_change,
    )
    from strategy.quote_core import quote_core_config_from_params

    base = {**parameters(), "max_spread_bps": 20.0,
            "dynamic_cap_enabled": True}

    def effective(params):
        return quote_core_config_from_params(
            params, tick_size=params["tick_size"], lot_size=params["lot_size"],
            use_ml=True, use_depth_microprice=False, use_depth_kappa=False,
        )

    b0 = effective(base)
    for change, fields in (
        ({"gamma": 0.02}, ("eta_inventory", "a_spread", "risk_per_order")),
        ({"kappa": 2.0}, ("execution_intensity_slope",)),
        ({"max_spread_bps": 25.0}, ("max_spread_bps", "dynamic_cap_base_bps")),
    ):
        _validate_effective_quote_change(base, change)
        candidate = effective({**base, **change})
        assert all(getattr(candidate, field) != getattr(b0, field) for field in fields)


def test_batched_prediction_preserves_all_heads_and_ema(bundle):
    from data.runtime import ConsumerBundle
    from strategy.signal import SignalEngine, REQUIRED_MODEL_HEADS

    frames = tuple(ConsumerBundle(bundle).frames())

    class Model:
        def __init__(self, offset):
            self.offset = offset
            self.calls = 0

        def predict(self, matrix, **kwargs):
            self.calls += 1
            return np.nan_to_num(matrix).sum(axis=1) * 0.001 + self.offset

    def engine():
        value = SignalEngine(enable_ml=False, ret_demean_halflife=3)
        names = [name for name, _ in frames[0].values]
        value._model_metadata = {
            head: dict(
                input_contract_id=frames[0].input_contract_id,
                observation_contract_id=frames[0].observation_contract_id,
                feature_contract_id=frames[0].feature_contract_id,
                feature_cols=names,
                missing_policy="native_nan",
            )
            for head in REQUIRED_MODEL_HEADS
        }
        value._model_feature_cols = {head: names for head in REQUIRED_MODEL_HEADS}
        value._model_feature_schema = tuple(names)
        value._models = {head: Model(i - 7.0) for i, head in enumerate(REQUIRED_MODEL_HEADS)}
        value._enable_ml = True
        return value

    one, batch = engine(), engine()
    expected = [one.compute_signal(feature_frame=f, decision_ns=f.cutoff_ns) for f in frames]
    actual = batch.compute_feature_frames(frames, batch_size=2)
    for left, right in zip(expected, actual, strict=True):
        for head in REQUIRED_MODEL_HEADS:
            assert getattr(left, head) == getattr(right, head)
        assert left.ts == right.ts
        np.testing.assert_array_equal(left.features, right.features)
    np.testing.assert_array_equal(one._pred_ret_ema, batch._pred_ret_ema)
    assert all(m.calls == (len(frames) + 1) // 2 for m in batch._models.values())
