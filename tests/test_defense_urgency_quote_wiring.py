from dataclasses import replace

import pytest

from strategy import quote_core as qc
from strategy.defense_urgency import MODES, resolve_quote_config


def fixture(mode='B0', q=.004, pnl=-.1, probability=.40):
    cfg = qc.QuoteCoreConfig(
        tick_size=.1, lot_size=.001, order_size=.001, maker_fee=0.,
        max_inventory=.026, inventory_reference_qty=.001,
        eta_inventory=.05, a_spread=.05, risk_per_order=.05,
        execution_intensity_slope=1., risk_horizon_s=1.,
        trade_intensity_acceleration_spread_mult=2.,
        defense_guard_enabled=True, defense_pause=True,
        defense_spread_mult=1.5, defense_urgency_mode=mode,
        exit_urgency_strength=.5, pnl_volatility_horizon_s=300.)
    state = qc.QuoteState(mid=100_000., inventory=q, sigma_sq=4.,
        best_bid=99_999.9, best_ask=100_000.1, position_open=True,
        mo_ema_bid=-3., mo_ema_ask=-3., unrealized_pnl=pnl,
        defense_urgency_visible_state_consistent=True)
    pred = qc.QuotePrediction(touch_conditioned_up_probability_10000ms=probability)
    return state, cfg, pred, qc.DepthSnapshot()


@pytest.mark.parametrize('q,p', [(.004,.4),(-.004,.6)])
@pytest.mark.parametrize('mode,patch', [
    (MODES[1], {'defense_pause':False}),
    (MODES[2], {'defense_guard_enabled':False}),
    (MODES[3], {'exit_urgency_strength':0.}),
])
def test_same_contribution_patch_reaches_python_and_native(monkeypatch, q, p, mode, patch):
    state, cfg, pred, depth = fixture(mode, q=q, probability=p)
    resolved = resolve_quote_config(state,cfg,pred,depth)
    assert resolved == replace(cfg, **patch)
    for native in (False, True):
        if native:
            pytest.importorskip('narrowgate_cpp')
        monkeypatch.setenv('NARROWGATE_CPP_QUOTE_CORE', str(int(native)))
        monkeypatch.setenv('NARROWGATE_CPP_STRICT', str(int(native)))
        actual = qc.compute_quote_core(state,cfg,pred,depth)
        expected = qc.compute_quote_core(state,replace(cfg,defense_urgency_mode='B0',**patch),pred,depth)
        assert actual == expected


@pytest.mark.parametrize('mode',MODES)
def test_baseline_and_unknown_state_do_not_change_quotes(monkeypatch, mode):
    monkeypatch.setenv('NARROWGATE_CPP_QUOTE_CORE','0')
    state,cfg,pred,depth=fixture(mode)
    state=replace(state,defense_urgency_visible_state_consistent=False)
    assert resolve_quote_config(state,cfg,pred,depth) is cfg
    assert qc.compute_quote_core(state,cfg,pred,depth)==qc.compute_quote_core(
        state,replace(cfg,defense_urgency_mode='B0'),pred,depth)


@pytest.mark.parametrize('mode',MODES[1:])
@pytest.mark.parametrize('changes',[
    {'inventory':0.}, {'inventory':.0005}, {'inventory':.013},
    {'unrealized_pnl':-5.}, {'unrealized_pnl':float('nan')},
    {'mo_ema_ask':-2.}, {'defense_urgency_visible_state_consistent':False},
])
def test_cross_zero_emergency_unknown_and_threshold_preserve_baseline(mode,changes):
    state,cfg,pred,depth=fixture(mode)
    state=replace(state,**changes)
    assert resolve_quote_config(state,cfg,pred,depth) is cfg


@pytest.mark.parametrize('mode',MODES[1:3])
@pytest.mark.parametrize('pnl',[0.,.001])
def test_loss_strict_boundary(mode,pnl):
    state,cfg,pred,depth=fixture(mode,pnl=pnl)
    assert resolve_quote_config(state,cfg,pred,depth) is cfg


def test_c3_removes_common_asym_not_only_reducing_side(monkeypatch):
    monkeypatch.setenv('NARROWGATE_CPP_QUOTE_CORE','0')
    state,cfg,pred,depth=fixture(MODES[3])
    before=qc.compute_quote_core(state,replace(cfg,defense_urgency_mode='B0'),pred,depth)
    after=qc.compute_quote_core(state,cfg,pred,depth)
    assert before.bid_price != after.bid_price
    assert before.ask_price != after.ask_price
    assert after.quote_context['SELL']['defense_pause'] is True


def test_reject_native_fused_or_live_mode_instead_of_silent_fallback():
    state,cfg,pred,depth=fixture(MODES[1])
    with pytest.raises(NotImplementedError):
        qc.make_native_quote_policy_stage(cfg)
    with pytest.raises(NotImplementedError):
        qc.compute_quote_core_live(state,cfg,pred,depth)


@pytest.mark.parametrize('q,p,active',[
    (.004,.449999,True), (.004,.45,False), (.004,.450001,False),
    (-.004,.549999,False), (-.004,.55,True), (-.004,.550001,True),
    (.012999,.4,True), (.013,.4,False), (.013001,.4,False),
    (.001,.4,True), (.000999,.4,False),
])
def test_original_float_direction_and_inventory_boundaries(q,p,active):
    # Preserve the original floating-point comparisons, including .45/.55;
    # do not add an epsilon to rewrite the historical baseline.
    state,cfg,pred,depth=fixture(MODES[1],q=q,probability=p)
    assert (resolve_quote_config(state,cfg,pred,depth) is not cfg)==active


@pytest.mark.parametrize('pnl,active',[(-5.000001,False),(-5.,False),(-4.999999,True)])
def test_original_loss_emergency_boundary(pnl,active):
    state,cfg,pred,depth=fixture(MODES[1],pnl=pnl)
    assert (resolve_quote_config(state,cfg,pred,depth) is not cfg)==active


def test_release_does_not_clear_adding_side_adverse_pause(monkeypatch):
    monkeypatch.setenv('NARROWGATE_CPP_QUOTE_CORE','0')
    state,cfg,pred,depth=fixture(MODES[1])
    state=replace(state,bid_adverse_markout_pause_latch=True,mo_ema_bid=-6.)
    cfg=replace(cfg,adverse_guard_enabled=True,adverse_pause=True)
    result=qc.compute_quote_core(state,cfg,pred,depth)
    assert result.quote_context['SELL']['defense_pause'] is False
    assert result.quote_context['BUY']['side_adverse_pause'] is True


def test_non_boolean_visible_state_is_unknown():
    state,cfg,pred,depth=fixture(MODES[1])
    assert resolve_quote_config(replace(state,defense_urgency_visible_state_consistent=1),cfg,pred,depth) is cfg


def test_invalid_mode_rejected_at_configuration():
    with pytest.raises(ValueError,match='unknown defense'):
        fixture('TYPO')
