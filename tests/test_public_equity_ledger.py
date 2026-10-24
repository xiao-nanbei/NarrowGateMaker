from copy import deepcopy

import pytest

from tests.test_optional_update_branch import synthetic_parent  # noqa: F401
from models.replay.public_accounting import public_equity_ledger


def example(prepared):
    plan = prepared.inputs['bundle'].manifest['plan']
    fills = [dict(fill_sequence=0, fill_ts=2000, side='BUY', fill_qty=1.,
                  quote_px=100., fill_fee_usdc=.1),
             dict(fill_sequence=1, fill_ts=10000, side='SELL', fill_qty=1.,
                  quote_px=105., fill_fee_usdc=.2)]
    result = dict(public_input_contract=dict(input_manifest_id=prepared.manifest_id,
        account_start_ns=plan['start_ns']), _fill_trace=fills, fills_total=2,
        final_inventory=0., cash_before_terminal=4.7)
    funding = dict(market_id=plan['market_id'], source_identity='synthetic-only',
        coverage_start_ns=plan['start_ns'], coverage_end_ns=plan['end_ns'],
        events=[dict(settlement_ns=10_000_000_000, mark_price=100., rate=.01)],
        expected_settlements_ns=[10_000_000_000])
    return result, dict(initial_capital=10000., max_mark_age_ns=100_000_000_000,
                        funding=funding)


def test_equity_carries_inventory_and_funding_before_boundary_fill(synthetic_parent):  # noqa: F811
    prepared, _ = synthetic_parent
    result, kwargs = example(prepared)
    before = deepcopy(result)
    ledger = public_equity_ledger(prepared.inputs['bundle'].root, result,
                                 [10_000_000_000, 21_000_000_000], **kwargs)
    first, last = ledger['rows']
    assert ledger['account_reset'] is False
    assert first['terminal_inventory'] == 1.
    assert first['funding_cashflow'] == -1.
    assert first['volume_btc'] == last['volume_btc'] == 1.
    assert last['initial_equity'] == first['terminal_equity']
    assert last['terminal_inventory'] == 0.
    assert last['funding_cashflow'] == 0.
    assert sum(r['net_pnl'] for r in ledger['rows']) == pytest.approx(3.7)
    assert result == before


def test_equity_ledger_rejects_incomplete_trace_or_cut(synthetic_parent):  # noqa: F811
    prepared, _ = synthetic_parent
    result, kwargs = example(prepared)
    with pytest.raises(ValueError, match='terminal cut'):
        public_equity_ledger(prepared.inputs['bundle'].root, result, [10_000_000_000], **kwargs)
    result['fills_total'] = 3
    with pytest.raises(ValueError, match='incomplete fill trace'):
        public_equity_ledger(prepared.inputs['bundle'].root, result, [21_000_000_000], **kwargs)
