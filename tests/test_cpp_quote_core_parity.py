import numpy as np
import pytest

narrowgate_cpp = pytest.importorskip("narrowgate_cpp")

from strategy import quote_core as qc
from strategy.policy_guards import CommonSidePolicyInput, evaluate_common_side_policy


def _cfg(**overrides):
    values = dict(
        gamma=0.01,
        kappa=1.0,
        tick_size=0.1,
        lot_size=0.001,
        maker_fee=0.0,
        order_size=0.001,
        max_inventory=0.02,
        ml_enabled=True,
        vol_blend=0.2,
        skew_strength=0.15,
        asym_strength=0.20,
        ret_skew=0.05,
        dynamic_cap_enabled=True,
        dynamic_cap_base_bps=20.0,
        dynamic_cap_alpha=0.5,
        dynamic_cap_var_baseline=1.0,
        max_spread_bps=20.0,
    )
    values.update(overrides)
    values.setdefault(
        "f03_ret_action_horizon_s", values.get("quote_horizon_s", 1.0)
    )
    values.setdefault("f03_ret_action_compatible", True)
    return qc.QuoteCoreConfig(**values)


def test_markout_asymmetry_default_uses_maker_signed_direction():
    assert _cfg().markout_side_asymmetry_sign == 1.0
    assert narrowgate_cpp.QuoteCoreConfig().markout_side_asymmetry_sign == 1.0


def test_spread_cap_missing_field_defaults_fail_closed_in_python_and_cpp():
    assert _cfg().spread_cap_mode == qc.SPREAD_CAP_PAUSE_EXPOSURE
    assert narrowgate_cpp.QuoteCoreConfig().spread_cap_mode == (
        qc.SPREAD_CAP_PAUSE_EXPOSURE
    )
    assert qc.quote_core_config_from_params(
        {
            "gamma": 0.01,
            "kappa": 1.0,
            "maker_fee": 0.0,
            "order_size": 0.001,
            "max_inventory": 0.01,
        },
        tick_size=0.1,
        lot_size=0.001,
        use_ml=False,
        use_depth_microprice=False,
        use_depth_kappa=False,
    ).spread_cap_mode == (
        qc.SPREAD_CAP_PAUSE_EXPOSURE
    )


@pytest.mark.parametrize(
    ("side", "inventory", "quantity", "expected"),
    (
        ("BUY", 0.0, 0.001, True),
        ("BUY", -0.002, 0.001, False),
        ("BUY", -0.001, 0.001, False),
        ("BUY", -0.0005, 0.001, True),
        ("SELL", 0.0, 0.001, True),
        ("SELL", 0.002, 0.001, False),
        ("SELL", 0.001, 0.001, False),
        ("SELL", 0.0005, 0.001, True),
    ),
)
def test_exposure_role_is_quantity_aware_and_cross_zero_fails_closed(
    side, inventory, quantity, expected
):
    assert qc._exposure_increasing(side, inventory, quantity, 0.001) is expected


@pytest.mark.parametrize("lot_size", (0.0, float("nan"), float("inf")))
def test_exposure_role_fails_closed_when_lot_size_is_invalid(lot_size):
    assert qc._exposure_increasing("BUY", -0.001, 0.001, lot_size) is True
    assert qc._exposure_increasing("SELL", 0.001, 0.001, lot_size) is True


def test_exact_one_lot_close_avoids_adverse_price_size_and_reason_with_cpp_parity(
    monkeypatch,
):
    cfg = _cfg(
        ml_enabled=False,
        dynamic_cap_enabled=False,
        max_spread_bps=0.0,
        adverse_guard_enabled=True,
        adverse_toxicity_threshold=0.7,
        adverse_spread_mult=2.0,
        adverse_pause=False,
        markout_spread_scale=0.0,
    )
    pred = qc.QuotePrediction(tox_bid=1.0, tox_ask=0.0)
    exact_close = _state(inventory=-0.001, mo_ema_bid=0.0, mo_ema_ask=0.0)
    cross_zero = _state(inventory=-0.0005, mo_ema_bid=0.0, mo_ema_ask=0.0)

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    exact_py = qc.compute_quote_core(exact_close, cfg, pred, qc.DepthSnapshot())
    cross_py = qc.compute_quote_core(cross_zero, cfg, pred, qc.DepthSnapshot())
    exact_context = exact_py.quote_context["BUY"]
    cross_context = cross_py.quote_context["BUY"]
    exact_policy = evaluate_common_side_policy(
        CommonSidePolicyInput(
            exposure_increasing=False,
            side_adverse=exact_context["side_adverse"],
            side_adverse_pause=exact_context["side_adverse_pause"],
        )
    )
    cross_policy = evaluate_common_side_policy(
        CommonSidePolicyInput(
            exposure_increasing=True,
            side_adverse=cross_context["side_adverse"],
            side_adverse_pause=cross_context["side_adverse_pause"],
        )
    )

    assert exact_context["side_adverse"] is False
    assert exact_policy.size_mult == 1.0
    assert exact_policy.reason_mask == 0
    assert cross_context["side_adverse"] is True
    assert cross_policy.size_mult == pytest.approx(0.7)
    assert cross_policy.reason_mask != 0
    assert cross_py.bid_price < exact_py.bid_price

    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    exact_cpp = qc.compute_quote_core(exact_close, cfg, pred, qc.DepthSnapshot())
    cross_cpp = qc.compute_quote_core(cross_zero, cfg, pred, qc.DepthSnapshot())
    assert exact_cpp.quote_context["BUY"]["side_adverse"] is False
    assert cross_cpp.quote_context["BUY"]["side_adverse"] is True
    assert exact_cpp.bid_price == pytest.approx(exact_py.bid_price, abs=cfg.tick_size * 0.51)
    assert cross_cpp.bid_price == pytest.approx(cross_py.bid_price, abs=cfg.tick_size * 0.51)


def _state(i=0, **overrides):
    values = dict(
        mid=100.0 + i,
        inventory=(i - 2) * 0.001,
        sigma_sq=1.0 + i * 0.1,
        trade_intensity=100.0,
        best_bid=99.9 + i,
        best_ask=100.1 + i,
        mo_ema_bid=-1.0,
        mo_ema_ask=-0.5,
    )
    values.update(overrides)
    return qc.QuoteState(**values)


def _pred(i=0):
    return qc.QuotePrediction(
        dir_10s=0.45 + i * 0.02,
        vol_10s=1.5,
        ret_10s=(i - 2) * 1e-5,
        tox_bid=0.55,
        tox_ask=0.52,
    )


def _depth():
    return qc.DepthSnapshot(
        bids=((99.9, 2.0), (99.8, 3.0), (99.7, 4.0)),
        asks=((100.1, 2.5), (100.2, 2.0), (100.3, 5.0)),
    )


def test_cpp_quote_core_scalar_parity(monkeypatch):
    cases = [
        (_state(0), _cfg(), _pred(0), qc.DepthSnapshot()),
        (_state(1), _cfg(use_depth_microprice=True, use_depth_kappa=True), _pred(1), _depth()),
        (
            _state(1),
            _cfg(
                eta_inventory=0.02,
                a_spread=0.03,
                quote_horizon_s=5.0,
            ),
            _pred(1),
            qc.DepthSnapshot(),
        ),
        (
            _state(2, inventory=0.004, mo_ema_bid=-6.0, mo_ema_ask=-7.0),
            _cfg(adverse_guard_enabled=True, adverse_markout_threshold=5.0, adverse_pause=False),
            _pred(2),
            _depth(),
        ),
        (
            _state(1, inventory=0.004),
            _cfg(
                p3_delta_star=0.5,
                p3_kappa_eff=0.1,
                p3_side_bbo_floor_enabled=True,
                p3_event_type="touch",
                p3_horizon_s=10.0,
                p3_distance_origin=(
                    "same_side_best_bid_or_ask_at_window_start"
                ),
                p3_distance_unit="USDC_per_BTC",
                p3_side="pooled_buy_sell",
                p3_queue_included=False,
                p3_artifact_sha256="c" * 64,
            ),
            _pred(1),
            qc.DepthSnapshot(),
        ),
    ]
    for state, cfg, pred, depth in cases:
        monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
        py = qc.compute_quote_core(state, cfg, pred, depth)
        monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
        monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
        cpp = qc.compute_quote_core(state, cfg, pred, depth)

        assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
        assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
        assert cpp.spread == pytest.approx(py.spread, abs=cfg.tick_size * 1.01)


def test_cpp_p3_side_floor_constraint_flags_are_side_specific(monkeypatch):
    cfg = _cfg(
        kappa=0.1,
        ml_enabled=False,
        dynamic_cap_enabled=False,
        max_spread_bps=0.0,
        inventory_skew_strength=2.0,
        p3_delta_star=0.1,
        p3_side_bbo_floor_enabled=True,
        p3_event_type="touch",
        p3_horizon_s=10.0,
        p3_distance_origin="same_side_best_bid_or_ask_at_window_start",
        p3_distance_unit="USDC_per_BTC",
        p3_side="pooled_buy_sell",
        p3_queue_included=False,
        p3_artifact_sha256="c" * 64,
    )
    state = _state(0, inventory=-0.02, sigma_sq=1.0)
    pred = qc.QuotePrediction()

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())

    assert py.bid_price == pytest.approx(cpp.bid_price, abs=cfg.tick_size * 0.51)
    assert py.ask_price == pytest.approx(cpp.ask_price, abs=cfg.tick_size * 0.51)
    assert py.quote_context["BUY"]["any_constraint_changed"] is True
    assert py.quote_context["SELL"]["any_constraint_changed"] is False
    assert cpp.quote_context["BUY"]["any_constraint_changed"] is True
    assert cpp.quote_context["SELL"]["any_constraint_changed"] is False


def test_cpp_direct_legacy_gamma_fallback_preserves_old_callers():
    cfg = _cfg(gamma=0.02)
    state = _state(1)
    pred = _pred(1)
    depth = qc.DepthSnapshot()
    expected = qc.compute_quote_core(state, cfg, pred, depth)

    cpp_cfg = narrowgate_cpp.QuoteCoreConfig()
    assert np.isnan(cpp_cfg.eta_inventory)
    assert np.isnan(cpp_cfg.a_spread)
    for name in qc._CPP_CFG_FIELDS:
        if name not in {"eta_inventory", "a_spread"}:
            setattr(cpp_cfg, name, getattr(cfg, name))
    cpp_state = qc._copy_attrs(state, narrowgate_cpp.QuoteState(), qc._CPP_STATE_FIELDS)
    cpp_pred = qc._copy_attrs(pred, narrowgate_cpp.QuotePrediction(), qc._CPP_PRED_FIELDS)
    actual = narrowgate_cpp.compute_quote_core(
        cpp_state,
        cpp_cfg,
        cpp_pred,
        qc._to_cpp_depth(narrowgate_cpp, depth),
    )

    assert actual.bid_price == pytest.approx(expected.bid_price, abs=cfg.tick_size * 0.51)
    assert actual.ask_price == pytest.approx(expected.ask_price, abs=cfg.tick_size * 0.51)
    cpp_cfg.a_spread = 0.0
    with pytest.raises(ValueError, match="a_spread"):
        narrowgate_cpp.compute_quote_core(
            cpp_state,
            cpp_cfg,
            cpp_pred,
            qc._to_cpp_depth(narrowgate_cpp, qc.DepthSnapshot()),
        )


def test_cpp_f03_ret_action_requires_explicit_consumer_compatibility():
    cfg = _cfg(
        ml_enabled=True,
        ret_skew=0.1,
        quote_horizon_s=10.0,
        f03_ret_action_horizon_s=10.0,
        f03_ret_action_compatible=True,
    )
    cpp_cfg = qc._copy_attrs(
        cfg,
        narrowgate_cpp.QuoteCoreConfig(),
        qc._CPP_CFG_FIELDS,
    )
    cpp_cfg.f03_ret_action_compatible = False
    with pytest.raises(ValueError, match="F03 ret action horizon"):
        narrowgate_cpp.compute_quote_core(
            qc._copy_attrs(
                _state(4),
                narrowgate_cpp.QuoteState(),
                qc._CPP_STATE_FIELDS,
            ),
            cpp_cfg,
            qc._copy_attrs(
                _pred(4),
                narrowgate_cpp.QuotePrediction(),
                qc._CPP_PRED_FIELDS,
            ),
            qc._to_cpp_depth(narrowgate_cpp, qc.DepthSnapshot()),
        )


def test_cpp_quote_core_horizon_and_absolute_price_risk_contract(monkeypatch):
    state = _state(
        mid=100.0,
        inventory=0.01,
        sigma_sq=4.0,
        position_open=True,
        unrealized_pnl=-0.1,
    )
    cfg = _cfg(
        quote_horizon_s=5.0,
        pnl_volatility_horizon_s=25.0,
        exit_urgency_strength=1.0,
        urgency_time_weight=0.0,
        urgency_pnl_weight=1.0,
        urgency_signal_weight=0.0,
        ml_enabled=False,
    )
    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, qc.QuotePrediction())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, qc.QuotePrediction())
    assert cpp.diagnostics["sigma_sq_horizon"] == pytest.approx(20.0)
    assert cpp.diagnostics["reservation_price"] == pytest.approx(
        py.diagnostics["reservation_price"]
    )
    assert cpp.diagnostics["delta_raw"] == pytest.approx(py.diagnostics["delta_raw"])
    assert cpp.diagnostics["asym"] == pytest.approx(-0.9)
    assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
    assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)


def test_markout_asymmetry_three_arm_direction_and_cpp_parity(monkeypatch):
    state = _state(
        mid=100.0,
        best_bid=99.9,
        best_ask=100.1,
        inventory=0.0,
        mo_ema_bid=10.0,
        mo_ema_ask=-10.0,
    )
    pred = qc.QuotePrediction()
    configs = {
        "historical": _cfg(
            ml_enabled=False,
            dynamic_cap_enabled=False,
            max_spread_bps=0.0,
            markout_spread_scale=0.4,
            markout_side_asymmetry_sign=-1.0,
        ),
        "off": _cfg(
            ml_enabled=False,
            dynamic_cap_enabled=False,
            max_spread_bps=0.0,
            markout_spread_scale=0.0,
            markout_side_asymmetry_sign=-1.0,
        ),
        "corrected": _cfg(
            ml_enabled=False,
            dynamic_cap_enabled=False,
            max_spread_bps=0.0,
            markout_spread_scale=0.4,
            markout_side_asymmetry_sign=1.0,
        ),
    }
    results = {}
    for name, cfg in configs.items():
        monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
        py = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())
        monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
        monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
        cpp = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())
        assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
        assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
        results[name] = py

    historical_bid_dist = state.mid - results["historical"].bid_price
    off_bid_dist = state.mid - results["off"].bid_price
    corrected_bid_dist = state.mid - results["corrected"].bid_price
    assert historical_bid_dist > off_bid_dist > corrected_bid_dist


@pytest.mark.parametrize(
    ("mode", "capped", "blocked"),
    [
        (qc.SPREAD_CAP_COMPRESS, True, False),
        (qc.SPREAD_CAP_PAUSE_EXPOSURE, False, True),
        (qc.SPREAD_CAP_OBSERVE, False, False),
    ],
)
def test_spread_cap_action_three_arm_cpp_parity(monkeypatch, mode, capped, blocked):
    state = _state(
        mid=100.0,
        best_bid=99.9,
        best_ask=100.1,
        inventory=0.0,
        sigma_sq=25.0,
        mo_ema_bid=0.0,
        mo_ema_ask=0.0,
    )
    cfg = _cfg(
        ml_enabled=False,
        dynamic_cap_enabled=False,
        max_spread_bps=2.0,
        spread_cap_mode=mode,
        markout_spread_scale=0.0,
    )
    pred = qc.QuotePrediction()
    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, pred, qc.DepthSnapshot())

    assert cpp.bid_price == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
    assert cpp.ask_price == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
    max_spread = state.mid * cfg.max_spread_bps / 10000.0
    if capped:
        assert cpp.spread <= max_spread + 2.0 * cfg.tick_size
    else:
        assert cpp.spread > max_spread
    assert cpp.quote_flags["cap_exposure_block"] is blocked
    assert cpp.quote_context["BUY"]["cap_exposure_block"] is blocked
    assert cpp.quote_context["SELL"]["cap_exposure_block"] is blocked


def test_tick_rounding_snaps_numerical_noise_at_boundary():
    tick = 0.1
    boundary = 79_196.7
    below_by_float_noise = boundary - 1e-11
    above_by_float_noise = boundary + 1e-11

    assert qc._floor_tick(below_by_float_noise, tick) == pytest.approx(boundary)
    assert qc._ceil_tick(above_by_float_noise, tick) == pytest.approx(boundary)


def test_cpp_live_quote_binding_matches_object_binding():
    cfg = _cfg(use_depth_microprice=True, use_depth_kappa=True)
    state = _state(1)
    pred = _pred(1)
    depth = _depth()
    cpp_cfg = qc._copy_attrs(cfg, narrowgate_cpp.QuoteCoreConfig(), qc._CPP_CFG_FIELDS)
    cpp_state = qc._copy_attrs(state, narrowgate_cpp.QuoteState(), qc._CPP_STATE_FIELDS)
    cpp_pred = qc._copy_attrs(pred, narrowgate_cpp.QuotePrediction(), qc._CPP_PRED_FIELDS)
    cpp_depth = qc._to_cpp_depth(narrowgate_cpp, depth)
    expected = narrowgate_cpp.compute_quote_core(cpp_state, cpp_cfg, cpp_pred, cpp_depth)
    actual = narrowgate_cpp.compute_quote_core_live(
        tuple(getattr(state, name) for name in qc._CPP_STATE_FIELDS),
        cpp_cfg,
        tuple(getattr(pred, name) for name in qc._CPP_PRED_FIELDS),
        depth.bids,
        depth.asks,
    )
    assert actual.bid_price == pytest.approx(expected.bid_price)
    assert actual.ask_price == pytest.approx(expected.ask_price)
    assert actual.book_imb == pytest.approx(expected.book_imb)
    assert actual.near_depth_total == pytest.approx(expected.near_depth_total)


def test_cpp_live_routing_compact_tuple_contract():
    input_values = (
        100.0, 0.0, 99.9, 100.1, 99.9, 100.1,
        0.1, 0.001, 0.001, 0.0, 0.001, 0.01,
        0.0, False, 1.0, 0.5,
        True, 99.9, 500.0, True, 100.1, 500.0,
    )
    bid_policy = (True, False, 1.0, 1.0, 1_000.0)
    ask_policy = (True, True, 1.0, 1.0, 1_000.0)

    result = narrowgate_cpp.compute_live_routing_decision(
        input_values, bid_policy, ask_policy
    )

    assert isinstance(result, tuple)
    assert len(result) == 11
    assert result[0] == pytest.approx(99.9)
    assert result[1] == pytest.approx(100.1)
    assert result[2] is False
    assert result[3:7] == (True, True, False, True)
    assert result[7:9] == (False, False)
    assert result[9] == pytest.approx(0.001)
    assert result[10] == pytest.approx(0.001)

    expired = list(input_values)
    expired[18] = 1_000.0
    expired_result = narrowgate_cpp.compute_live_routing_decision(
        tuple(expired), bid_policy, ask_policy
    )
    assert expired_result[7] is True


def test_cpp_live_routing_does_not_enlarge_invalid_base_order():
    input_values = (
        100.0, 0.0, 99.9, 100.1, 99.9, 100.1,
        0.1, 0.001, 0.001, 10.0, 0.001, 0.01,
        0.0, False, 1.0, 0.5,
        False, 0.0, 0.0, False, 0.0, 0.0,
    )
    policy = (True, True, 1.0, 1.0, 1_000.0)

    result = narrowgate_cpp.compute_live_routing_decision(
        input_values, policy, policy
    )

    # 0.001 BTC at 100 USDC is below the 10 USDC notional minimum.  Python
    # leaves this invalid so the final exchange filter skips it; C++ must not
    # silently turn it into a 0.101 BTC order.
    assert result[9] == pytest.approx(0.001)
    assert result[10] == pytest.approx(0.001)


def test_cpp_live_routing_rejects_wrong_compact_shape():
    with pytest.raises(ValueError, match="compact input length mismatch"):
        narrowgate_cpp.compute_live_routing_decision((1.0,), (True,) * 5, (True,) * 5)


def test_cpp_live_compact_context_preserves_policy_fields(monkeypatch):
    state = _state(1, inventory=0.006, mo_ema_bid=-6.0, mo_ema_ask=-4.0)
    cfg = _cfg(
        use_depth_microprice=True,
        use_depth_kappa=True,
        adverse_guard_enabled=True,
        adverse_markout_threshold=5.0,
        adverse_pause=True,
        defense_guard_enabled=True,
        defense_markout_threshold=2.0,
    )
    pred = _pred(1)
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    full = qc.compute_quote_core(state, cfg, pred, _depth())
    compact = qc.compute_quote_core_live(
        state, cfg, pred, _depth(), require_full_context=False
    )
    assert compact.bid_price == pytest.approx(full.bid_price)
    assert compact.ask_price == pytest.approx(full.ask_price)
    for side in ("BUY", "SELL"):
        for key in (
            "side_adverse", "side_adverse_pause", "defense_guard",
            "defense_pause", "defense_spread_mult", "near_depth_total",
            "final_quote_delta_to_bbo",
        ):
            assert compact.quote_context[side][key] == pytest.approx(
                full.quote_context[side][key]
            )
    for key in ("max_spread", "kappa_used", "asym", "delta_after_cap"):
        assert compact.diagnostics[key] == pytest.approx(full.diagnostics[key])

    requested_full = qc.compute_quote_core_live(
        state, cfg, pred, _depth(), require_full_context=True
    )
    assert "raw_asym_shift" in requested_full.quote_context["BUY"]
    assert requested_full.quote_context["BUY"]["raw_asym_shift"] == pytest.approx(
        full.quote_context["BUY"]["raw_asym_shift"]
    )


def test_cpp_quote_core_diagnostics_and_defense_context_parity(monkeypatch):
    state = _state(
        1,
        mid=100.0,
        best_bid=99.9,
        best_ask=100.1,
        inventory=0.006,
        mo_ema_bid=-3.0,
        mo_ema_ask=-4.0,
        unrealized_pnl=-2.0,
    )
    cfg = _cfg(
        use_depth_microprice=True,
        use_depth_kappa=True,
        book_imb_strength=0.3,
        trace_book_imb_levels=3,
        depth_tox_enabled=True,
        depth_tox_levels=3,
        depth_tox_imbalance_threshold=0.01,
        depth_tox_spread_mult=1.5,
        dynamic_cap_liq_beta=0.3,
        dynamic_cap_liq_baseline=10.0,
        adverse_guard_enabled=True,
        adverse_markout_threshold=2.0,
        adverse_pause=False,
        defense_guard_enabled=True,
        defense_markout_threshold=2.0,
        defense_dir_threshold=0.01,
        defense_ret_bps_threshold=0.01,
        defense_microprice_shift_bps=0.01,
        defense_pause=True,
        defense_spread_mult=1.4,
        defense_emergency_inventory_ratio=0.9,
        defense_emergency_loss=20.0,
    )
    pred = qc.QuotePrediction(
        dir_10s=0.56,
        vol_10s=1.8,
        ret_10s=2e-5,
        tox_bid=0.8,
        tox_ask=0.4,
    )

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    py = qc.compute_quote_core(state, cfg, pred, _depth())
    monkeypatch.setenv("NARROWGATE_CPP_QUOTE_CORE", "1")
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    cpp = qc.compute_quote_core(state, cfg, pred, _depth())

    common_fields = [
        "raw_half_spread",
        "capped_half_spread",
        "raw_mid_shift",
        "raw_reservation_shift",
        "raw_asym_shift",
        "asym",
        "book_imb",
        "microprice_shift_bps",
        "near_depth_total",
        "raw_quote_skew",
    ]
    for field in common_fields:
        assert cpp.quote_context["BUY"][field] == pytest.approx(py.quote_context["BUY"][field])
        assert cpp.quote_context["SELL"][field] == pytest.approx(py.quote_context["SELL"][field])

    diagnostic_fields = [
        "reservation_price",
        "sigma_sq_raw",
        "sigma_sq_blended",
        "delta_raw",
        "delta_after_regime",
        "delta_pre_cap",
        "delta_after_cap",
        "half_d",
        "asym",
        "kappa_before_depth",
        "kappa_used",
        "depth_tox_mult",
        "final_cap_excess",
        "mid_guard_bid",
        "mid_guard_ask",
        "post_only_bid",
        "post_only_ask",
        "final_cap_rounding",
        "final_cap_mid_guard",
        "final_cap_post_only",
        "final_cap_delta",
    ]
    for field in diagnostic_fields:
        if isinstance(py.diagnostics[field], bool):
            assert cpp.diagnostics[field] is py.diagnostics[field]
        else:
            assert cpp.diagnostics[field] == pytest.approx(py.diagnostics[field])

    defense_fields = [
        "defense_guard",
        "defense_pause",
        "defense_reducing",
        "defense_emergency",
        "defense_markout",
        "defense_direction",
        "defense_ret",
        "defense_microprice",
        "defense_spread_mult",
    ]
    for side in ("BUY", "SELL"):
        for field in defense_fields:
            if isinstance(py.quote_context[side][field], bool):
                assert cpp.quote_context[side][field] is py.quote_context[side][field]
            else:
                assert cpp.quote_context[side][field] == pytest.approx(py.quote_context[side][field])


def test_cpp_quote_core_strict_module_token_guard(monkeypatch):
    monkeypatch.setenv("NARROWGATE_CPP_STRICT", "1")
    monkeypatch.setenv("NARROWGATE_CPP_EXPECT_MODULE_TOKEN", "__definitely_wrong_token__")
    qc._CPP_QUOTE_CORE = None
    qc._CPP_QUOTE_CORE_IMPORT_FAILED = False
    with pytest.raises(RuntimeError, match="different build"):
        qc._load_cpp_quote_core()
    monkeypatch.delenv("NARROWGATE_CPP_EXPECT_MODULE_TOKEN", raising=False)
    qc._CPP_QUOTE_CORE = None
    qc._CPP_QUOTE_CORE_IMPORT_FAILED = False


def test_direct_cpp_p3_projection_requires_complete_touch_identity() -> None:
    cpp_cfg = narrowgate_cpp.QuoteCoreConfig()
    cpp_cfg.p3_delta_star = 0.5
    cpp_cfg.p3_side_bbo_floor_enabled = True
    cpp_state = qc._copy_attrs(
        _state(1), narrowgate_cpp.QuoteState(), qc._CPP_STATE_FIELDS
    )
    cpp_pred = qc._copy_attrs(
        _pred(1), narrowgate_cpp.QuotePrediction(), qc._CPP_PRED_FIELDS
    )
    cpp_depth = qc._to_cpp_depth(narrowgate_cpp, qc.DepthSnapshot())

    with pytest.raises(ValueError, match="complete touch identity"):
        narrowgate_cpp.compute_quote_core(
            cpp_state,
            cpp_cfg,
            cpp_pred,
            cpp_depth,
        )

    cpp_cfg.p3_identity_required = True
    cpp_cfg.p3_event_type = "touch"
    cpp_cfg.p3_horizon_s = 10.0
    cpp_cfg.p3_distance_origin = "same_side_best_bid_or_ask_at_window_start"
    cpp_cfg.p3_distance_unit = "USDC_per_BTC"
    cpp_cfg.p3_side = "pooled_buy_sell"
    cpp_cfg.p3_queue_included = False
    cpp_cfg.p3_artifact_sha256 = "d" * 64
    result = narrowgate_cpp.compute_quote_core(
        cpp_state,
        cpp_cfg,
        cpp_pred,
        cpp_depth,
    )
    assert result.bid_price > 0.0
    assert result.ask_price > result.bid_price


def test_scalar_cpp_route_rejects_stale_quote_config_abi() -> None:
    class StaleCppModule:
        class QuoteCoreConfig:
            gamma = 0.01

    qc._CPP_CFG_CACHE_KEY = None
    qc._CPP_CFG_CACHE_REF = None
    qc._CPP_CFG_CACHE_VALUE = None
    with pytest.raises(RuntimeError, match="p3_identity_required"):
        qc._cached_cpp_config(StaleCppModule(), _cfg())


def test_cpp_quote_core_batch_parity(monkeypatch):
    cfg = _cfg()
    cpp_cfg = narrowgate_cpp.QuoteCoreConfig()
    for name in qc._CPP_CFG_FIELDS:
        if hasattr(cpp_cfg, name):
            setattr(cpp_cfg, name, getattr(cfg, name))

    n = 256
    mid = np.linspace(100.0, 101.0, n, dtype=np.float64)
    inventory = np.linspace(-0.004, 0.004, n, dtype=np.float64)
    sigma_sq = np.linspace(0.5, 2.0, n, dtype=np.float64)
    trade_intensity = np.full(n, 100.0, dtype=np.float64)
    best_bid = mid - 0.1
    best_ask = mid + 0.1
    dir_10s = np.linspace(0.45, 0.55, n, dtype=np.float64)
    vol_10s = np.full(n, 1.0, dtype=np.float64)
    ret_10s = np.linspace(-2e-5, 2e-5, n, dtype=np.float64)
    tox_bid = np.full(n, 0.5, dtype=np.float64)
    tox_ask = np.full(n, 0.5, dtype=np.float64)

    out = narrowgate_cpp.compute_quote_core_batch(
        mid,
        inventory,
        sigma_sq,
        trade_intensity,
        best_bid,
        best_ask,
        dir_10s,
        vol_10s,
        ret_10s,
        tox_bid,
        tox_ask,
        cpp_cfg,
    )

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    for i in range(0, n, 17):
        py = qc.compute_quote_core(
            qc.QuoteState(
                mid=float(mid[i]),
                inventory=float(inventory[i]),
                sigma_sq=float(sigma_sq[i]),
                trade_intensity=float(trade_intensity[i]),
                best_bid=float(best_bid[i]),
                best_ask=float(best_ask[i]),
            ),
            cfg,
            qc.QuotePrediction(
                dir_10s=float(dir_10s[i]),
                vol_10s=float(vol_10s[i]),
                ret_10s=float(ret_10s[i]),
                tox_bid=float(tox_bid[i]),
                tox_ask=float(tox_ask[i]),
            ),
        )
        assert out["bid_price"][i] == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
        assert out["ask_price"][i] == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)


def test_cpp_quote_core_batch_depth_parity(monkeypatch):
    cfg = _cfg(
        use_depth_microprice=True,
        use_depth_kappa=True,
        book_imb_strength=0.05,
        trace_book_imb_levels=3,
        depth_tox_enabled=True,
        depth_tox_levels=3,
        depth_tox_imbalance_threshold=0.55,
        depth_tox_microprice_shift_bps=0.01,
    )
    n = 4_097
    mid = np.linspace(100.0, 100.6, n, dtype=np.float64)
    inventory = np.linspace(-0.004, 0.004, n, dtype=np.float64)
    sigma_sq = np.linspace(0.5, 2.0, n, dtype=np.float64)
    trade_intensity = np.full(n, 100.0, dtype=np.float64)
    best_bid = mid - 0.1
    best_ask = mid + 0.1
    dir_10s = np.linspace(0.45, 0.55, n, dtype=np.float64)
    vol_10s = np.full(n, 1.0, dtype=np.float64)
    ret_10s = np.linspace(-2e-5, 2e-5, n, dtype=np.float64)
    tox_bid = np.full(n, 0.55, dtype=np.float64)
    tox_ask = np.full(n, 0.52, dtype=np.float64)

    bid_offsets = np.array([0.1, 0.2, 0.3], dtype=np.float64)
    ask_offsets = np.array([0.1, 0.2, 0.3], dtype=np.float64)
    l2_bid_px = mid[:, None] - bid_offsets[None, :]
    l2_ask_px = mid[:, None] + ask_offsets[None, :]
    l2_bid_qty = np.column_stack([
        np.full(n, 2.0),
        np.linspace(2.0, 4.0, n),
        np.full(n, 5.0),
    ])
    l2_ask_qty = np.column_stack([
        np.full(n, 2.5),
        np.linspace(4.0, 2.0, n),
        np.full(n, 4.0),
    ])

    out = qc.compute_quote_core_batch_depth_cpp(
        mid=mid,
        inventory=inventory,
        sigma_sq=sigma_sq,
        trade_intensity=trade_intensity,
        best_bid=best_bid,
        best_ask=best_ask,
        dir_10s=dir_10s,
        vol_10s=vol_10s,
        ret_10s=ret_10s,
        tox_bid=tox_bid,
        tox_ask=tox_ask,
        cfg=cfg,
        mo_ema_bid=np.full(n, -1.0),
        mo_ema_ask=np.full(n, -0.5),
        l2_bid_px=l2_bid_px,
        l2_bid_qty=l2_bid_qty,
        l2_ask_px=l2_ask_px,
        l2_ask_qty=l2_ask_qty,
        strict=True,
    )
    out_parallel = qc.compute_quote_core_batch_depth_cpp(
        mid=mid,
        inventory=inventory,
        sigma_sq=sigma_sq,
        trade_intensity=trade_intensity,
        best_bid=best_bid,
        best_ask=best_ask,
        dir_10s=dir_10s,
        vol_10s=vol_10s,
        ret_10s=ret_10s,
        tox_bid=tox_bid,
        tox_ask=tox_ask,
        cfg=cfg,
        mo_ema_bid=np.full(n, -1.0),
        mo_ema_ask=np.full(n, -0.5),
        l2_bid_px=l2_bid_px,
        l2_bid_qty=l2_bid_qty,
        l2_ask_px=l2_ask_px,
        l2_ask_qty=l2_ask_qty,
        strict=True,
        workers=4,
    )
    for key in ("bid_price", "ask_price", "near_depth_total", "book_imb"):
        assert out_parallel[key] == pytest.approx(out[key], abs=1e-12), key

    monkeypatch.delenv("NARROWGATE_CPP_QUOTE_CORE", raising=False)
    for i in range(0, n, max(11, n // 6)):
        depth = qc.quote_depth_from_l2_rows(l2_bid_px[i], l2_bid_qty[i], l2_ask_px[i], l2_ask_qty[i])
        py = qc.compute_quote_core(
            qc.QuoteState(
                mid=float(mid[i]),
                inventory=float(inventory[i]),
                sigma_sq=float(sigma_sq[i]),
                trade_intensity=float(trade_intensity[i]),
                best_bid=float(best_bid[i]),
                best_ask=float(best_ask[i]),
                mo_ema_bid=-1.0,
                mo_ema_ask=-0.5,
            ),
            cfg,
            qc.QuotePrediction(
                dir_10s=float(dir_10s[i]),
                vol_10s=float(vol_10s[i]),
                ret_10s=float(ret_10s[i]),
                tox_bid=float(tox_bid[i]),
                tox_ask=float(tox_ask[i]),
            ),
            depth,
        )
        assert out["bid_price"][i] == pytest.approx(py.bid_price, abs=cfg.tick_size * 0.51)
        assert out["ask_price"][i] == pytest.approx(py.ask_price, abs=cfg.tick_size * 0.51)
        assert out["near_depth_total"][i] == pytest.approx(py.quote_context["BUY"]["near_depth_total"], rel=1e-12)
        assert out["book_imb"][i] == pytest.approx(py.quote_context["BUY"]["book_imb"], abs=1e-12)
        assert "bid_defense_guard" in out
