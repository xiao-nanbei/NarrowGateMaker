"""E/C through MakerEngine and OrderManager; network boundary is a recorder."""
from types import SimpleNamespace
import time

import pytest

from strategy.prefill_live import LivePrefillSelection
from strategy.risk_selection import LinearValueModel, RiskSelectionPolicy
from tests.test_live_maker_close_ioc import _engine as transport_engine, _RestClient
from tests.test_live_replace_throttle import _routable_update_orders_engine


class Recorder(_RestClient):
    def new_order(self, **params):
        response = super().new_order(**params)
        response['orderId'] = len(self.calls)
        return response

    def cancel_order(self, **params):
        self.cancel_calls.append(params)
        return {}  # Request transport completion is not a private terminal notification.


def engine(mode, value):
    gateway = Recorder()
    live = transport_engine(gateway)
    routing = _routable_update_orders_engine()
    live.__dict__.update(routing.__dict__)
    live.cfg.strategy.order_size = .001
    live._prefill_ec = LivePrefillSelection(mode=mode, scope='EC',
        policy=RiskSelectionPolicy('controlled-test', {}, {
            f'{k}:{s}': LinearValueModel(value, {}) for k in 'EC' for s in ('BUY', 'SELL')
        }, 'visible_inventory'))
    live._last_quote_diagnostics_cache = {'sigma_sq_raw': 1.}
    return live, gateway


def update(live):
    live._update_orders(mid=100000., bid_price=99000., ask_price=101000., q=0.,
        pred=SimpleNamespace(), quote_snapshot=SimpleNamespace(capture_ts_ns=time.time_ns(),
            best_bid=99000., best_ask=101000., bids=((99000., 2.),), asks=((101000., 1.),)),
        post_only_guard=SimpleNamespace(best_bid=99000., best_ask=101000., source='test'))


@pytest.mark.parametrize('mode,value,requests', [('off', -1., 2), ('shadow', -1., 2),
                                                ('enforce', -1., 0), ('enforce', 1., 2)])
@pytest.mark.parametrize('backend', ['python', 'native', 'native_final'])
def test_live_post_wait_uses_real_order_manager(mode, value, requests, monkeypatch, backend):
    monkeypatch.setenv('NARROWGATE_CPP_ORDER_ACTION_PLAN', '1' if backend == 'native' else '0')
    monkeypatch.setenv('NARROWGATE_CPP_FINAL_ORDER_PLAN', '1' if backend == 'native_final' else '0')
    live, gateway = engine(mode, value)
    if backend != 'python':
        live._freeze_native_order_action_planner()
    update(live)
    assert len(gateway.calls) == requests
    assert len(live.orders.get_active_orders()) == requests
    if requests:
        assert [float(r['price']) for r in gateway.calls] == [99000., 101000.]
        assert all(float(r['quantity']) == .001 for r in gateway.calls)
    else:
        # WAIT consumes no order ownership or sticky denial; next normal opportunity works.
        live._prefill_ec = engine('enforce', 1.)[0]._prefill_ec
        update(live)
        assert len(gateway.calls) == 2


def test_cancel_retains_ownership_and_pending_exposure_until_private_terminal(monkeypatch):
    from strategy.order_manager import OrderState

    monkeypatch.setenv('NARROWGATE_CPP_ORDER_ACTION_PLAN', '0')
    monkeypatch.setenv('NARROWGATE_CPP_FINAL_ORDER_PLAN', '0')
    live, gateway = engine('enforce', 1.)
    update(live)
    ids = {o.client_order_id for o in live.orders.get_active_orders()}
    update(live)  # KEEP: no replacement or cancellation.
    assert len(gateway.calls) == 2 and not gateway.cancel_calls
    live._prefill_ec = engine('enforce', -1.)[0]._prefill_ec
    update(live)
    assert {r['origClientOrderId'] for r in gateway.cancel_calls} == ids
    assert len(gateway.cancel_calls) == 2 and len(gateway.calls) == 2
    assert all(o.state == OrderState.PENDING_CANCEL and o.remaining_qty == .001
               for o in live.orders.get_active_orders())
    update(live)
    assert len(gateway.cancel_calls) == 2  # No duplicate cancel during unknown outcome.
    order = live.orders.get_active_orders()[0]
    event = dict(c=order.client_order_id, i=order.order_id, s='BTCUSDC', S=order.side.value,
        X='PARTIALLY_FILLED', o='LIMIT', p=str(order.price), q='0.001', z='0.0005',
        l='0.0005', L=str(order.price), n='0', N='USDC', t=9001,
        T=int(time.time()*1000), _local_receive_ts_ns=time.time_ns())
    live.orders.on_order_update(event)
    assert order.filled_qty == .0005 and order.remaining_qty == .0005
    live.orders.on_order_update(event)
    assert order.filled_qty == .0005  # Duplicate notification is not another fill.
    terminal = {**event, 'X': 'CANCELED', 'l': '0', 't': None,
                '_local_receive_ts_ns': time.time_ns()}
    live.orders.on_order_update(terminal)
    assert order.state == OrderState.CANCELED
    live.orders.on_order_update(event)
    assert order.state == OrderState.CANCELED  # Late nonterminal cannot resurrect ownership.


def test_live_required_surface_and_unsupported_feature_reject():
    with pytest.raises(ValueError, match='surface'):
        LivePrefillSelection(mode='enforce', scope='EC', policy=RiskSelectionPolicy(
            'missing', {}, {}, 'visible_inventory'))
    with pytest.raises(ValueError, match='5000ms'):
        LivePrefillSelection(mode='shadow', scope='E', policy=RiskSelectionPolicy(
            'unsupported', {'side_trade_imbalance_5000ms': ('1', 0., 1.)},
            {f'E:{s}': LinearValueModel(0., {}) for s in ('BUY', 'SELL')}, 'visible_inventory'))


def test_live_config_loads_current_policy_and_checks_baseline(tmp_path):
    import json
    from strategy.risk_selection import SCHEMA_VERSION, VALUE_UNIT

    live, _ = engine('off', 0.)
    cfg = live.cfg
    cfg.strategy.prefill_ec_mode = 'enforce'
    cfg.strategy.prefill_ec_scope = 'EC'
    cfg.strategy.prefill_ec_policy_path = str(tmp_path/'policy.json')
    coefficients = {k: getattr(cfg.strategy, k) for k in (
        'eta_inventory', 'a_spread', 'risk_per_order', 'inventory_reference_qty')}
    coefficients['asym_strength'] = cfg.ml.asym_strength
    payload = dict(schema_version=SCHEMA_VERSION, value_unit=VALUE_UNIT,
        policy_id='controlled-config', selection_scope='visible_inventory', features={},
        training_label_contract='prefill_ec_accounting_30000ms.v1',
        baseline_coefficients=coefficients,
        models={f'{k}:{s}': dict(intercept_usdc=1., coefficients={})
                for k in 'EC' for s in ('BUY', 'SELL')})
    (tmp_path/'policy.json').write_text(json.dumps(payload))
    assert LivePrefillSelection.from_config(cfg).mode == 'enforce'
    cfg.ml.asym_strength += .1
    with pytest.raises(ValueError, match='baseline'):
        LivePrefillSelection.from_config(cfg)


def test_first_fill_not_required_and_sides_share_visible_book(monkeypatch):
    monkeypatch.setenv('NARROWGATE_CPP_ORDER_ACTION_PLAN', '0')
    monkeypatch.setenv('NARROWGATE_CPP_FINAL_ORDER_PLAN', '0')
    live, gateway = engine('off', 0.)
    live._prefill_ec = LivePrefillSelection(mode='enforce', scope='E',
        policy=RiskSelectionPolicy('visible-book-test', {'side_l1_imbalance': ('1', 0., 1.)},
            {f'E:{s}': LinearValueModel(0., {'side_l1_imbalance': 1.}) for s in ('BUY', 'SELL')},
            'visible_inventory'))
    update(live)
    buy, sell = live._prefill_ec.last_decisions
    assert buy.value_delta_usdc == pytest.approx(1/3)
    assert sell.value_delta_usdc == pytest.approx(-1/3)
    assert buy.decision_ts_ns == sell.decision_ts_ns
    assert buy.feature_ready_ts_ns == sell.feature_ready_ts_ns <= buy.decision_ts_ns
    assert [r['side'] for r in gateway.calls] == ['BUY']


def test_prefill_mode_is_restart_only():
    from copy import deepcopy

    live, _ = engine('off', 0.)
    candidate = deepcopy(live.cfg)
    candidate.strategy.prefill_ec_mode = 'shadow'
    with pytest.raises(ValueError, match='restart-only'):
        live.on_config_reload(candidate)
