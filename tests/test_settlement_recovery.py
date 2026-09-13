from copy import deepcopy
from hashlib import sha256
import json

import pytest

from models.replay.l2_journal import ReplayL2Journal
from models.replay.settlement_recovery import verify_manifest_relocation, settle_closed_journal
from tests.test_research_public_inputs import bundle, binding, SECOND, MARKET  # noqa: F401


@pytest.mark.parametrize('change', [None, 'range', 'content', 'observation', 'identity'])
def test_explicit_relocation_never_relaxes_content_identity(tmp_path, change):
    original = dict(plan=dict(facts_root='/original', end_ns=100,
        observation_profile=dict(measured_latency_path='/old', profile_id='fixed')),
        source_bundles=[dict(path='/old/day', sha256='unchanged')])
    a = tmp_path / 'original.json'
    a.write_text(json.dumps(original))
    ah = sha256(a.read_bytes()).hexdigest()
    relocated = deepcopy(original)
    relocated['relocated_from_manifest_sha256'] = ah
    relocated['plan']['facts_root'] = '/new'
    relocated['plan']['observation_profile']['measured_latency_path'] = '/new/latency'
    relocated['source_bundles'][0]['path'] = '/new/day'
    if change == 'range':
        relocated['plan']['end_ns'] += 1
    elif change == 'content':
        relocated['source_bundles'][0]['sha256'] = 'changed'
    elif change == 'observation':
        relocated['plan']['observation_profile']['profile_id'] = 'changed'
    b = tmp_path / 'relocated.json'
    b.write_text(json.dumps(relocated))
    bh = sha256(b.read_bytes()).hexdigest()
    kwargs = dict(original_sha256=ah, relocated_sha256=bh if change != 'identity' else 'wrong')
    if change is None:
        assert verify_manifest_relocation(a, b, **kwargs)['original_manifest_sha256'] == ah
    else:
        with pytest.raises(ValueError, match='non-locator|identity mismatch'):
            verify_manifest_relocation(a, b, **kwargs)


def test_second_locator_relocation_preserves_first_parent(tmp_path):
    original = dict(plan=dict(facts_root='/original', end_ns=100,
        observation_profile=dict(measured_latency_path='/old', profile_id='fixed')),
        source_bundles=[dict(path='/old/day', sha256='same-content')])
    paths = [tmp_path / name for name in ('original.json', 'first.json', 'second.json')]
    paths[0].write_text(json.dumps(original))
    identities = [sha256(paths[0].read_bytes()).hexdigest()]
    first = deepcopy(original)
    first['relocated_from_manifest_sha256'] = identities[0]
    first['plan']['facts_root'] = '/first'
    first['source_bundles'][0]['path'] = '/first/day'
    paths[1].write_text(json.dumps(first))
    identities.append(sha256(paths[1].read_bytes()).hexdigest())
    second = deepcopy(first)
    second['relocated_from_manifest_sha256'] = identities[1]
    second['relocation_previous_parent_sha256'] = identities[0]
    second['plan']['facts_root'] = '/second'
    second['source_bundles'][0]['path'] = '/second/day'
    paths[2].write_text(json.dumps(second))
    identity = sha256(paths[2].read_bytes()).hexdigest()
    verify_manifest_relocation(paths[1], paths[2], original_sha256=identities[1],
                               relocated_sha256=identity)
    second['relocation_previous_parent_sha256'] = 'wrong-parent'
    paths[2].write_text(json.dumps(second))
    with pytest.raises(ValueError, match='ancestry mismatch'):
        verify_manifest_relocation(paths[1], paths[2], original_sha256=identities[1],
                                   relocated_sha256=sha256(paths[2].read_bytes()).hexdigest())


def test_settlement_retry_is_create_once_and_never_reenters_replay(bundle, tmp_path, monkeypatch):  # noqa: F811
    import models.backtest_tick as replay

    def forbidden(*args, **kwargs):
        pytest.fail('postprocessing must never enter replay')

    for name in ('simulate_tick', 'simulate_public_inputs', 'simulate_prepared_inputs'):
        monkeypatch.setattr(replay, name, forbidden)
    journal = ReplayL2Journal(tmp_path / 'journal', identity={})
    for i, t, side, price in [(0, 2000, 'BUY', 101.), (1, 3000, 'SELL', 100.)]:
        journal.emit('fill', t, side=side, payload=dict(fill_sequence=i, fill_ts=t, side=side,
                     fill_qty=1., quote_px=price, fill_fee_usdc=1.))
    journal.emit('account_end', 4000, payload=dict(final_inventory=0., cash_before_terminal=-3.))
    journal.close()
    funding = dict(market_id=MARKET, source_identity='fixture', coverage_start_ns=SECOND,
                   coverage_end_ns=4*SECOND, expected_settlements_ns=[2500000000],
                   events=[dict(settlement_ns=2500000000, mark_price=100., rate=.01)])
    kwargs = dict(input_contract={'account_start_ns':SECOND, **binding(bundle)}, funding=funding,
                  output=tmp_path/'settlement.json', initial_capital=100., max_mark_age_ns=SECOND)
    first = settle_closed_journal(bundle, journal.writer.manifest_path, **kwargs)
    before = kwargs['output'].stat().st_mtime_ns
    second = settle_closed_journal(bundle, journal.writer.manifest_path, **kwargs)
    assert first == second
    assert second['accounting']['all_in_net_pnl'] == -4.
    assert kwargs['output'].stat().st_mtime_ns == before
    with pytest.raises(ValueError, match='conflicting settlement'):
        settle_closed_journal(bundle, journal.writer.manifest_path, **{**kwargs, 'initial_capital':101.})
