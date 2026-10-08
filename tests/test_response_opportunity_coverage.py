"""Joint coverage is descriptive local evidence, not an executable action."""
from decimal import Decimal as D
from types import SimpleNamespace
import pytest

from data.tardis_input import BookView
from features.trade_book_response import ResponseState
from strategy.update_opportunity import UpdateOpportunityObserver, old_price_support


class Cursor:
    def __init__(self):
        self.state = ResponseState(trade_coverage='observed', fast_response_state=True)
        view = BookView(1, ((D(100), D(2)), (D(98), D(3))), ((D(102), D(3)),),
                        None, None, 'synthetic', True)
        self.state.observe_book(0, 0, view)
        self.state.observe_book(11_000_000_000, 11_000_000_000, view)
        self.sequence = 2
        self.last_ready_ns = self.read_ns = 11_000_000_000

    def advance(self, now):
        self.read_ns = now
        self.state.advance(now)

    def frame(self, side, *, order_price):
        return self.state.order_frame(side, order_price)


def test_joint_denominator_excludes_first_placement_and_preserves_unknown():
    observer = UpdateOpportunityObserver(Cursor(), sample_limit=1)
    args = dict(side='BUY', now_ms=11000, call_sequence=1, inventory=0,
                target_price=99, order_price=100, order_id='local', order_active=True,
                needs_update=True, force_update=False, quantity_changed=False, pending=False,
                age_ms=2000, interval_ms=1000, price_delta_ticks=10, fixed_ticks=15,
                route_allowed=True, queue_keep=False, compute={}, emit_row=False)
    first = observer.observe(**args)
    assert first['eligibility_status'] == 'unknown'
    assert first['counterfactual_effective_ns'] is None
    assert observer.observe(**dict(args, pending=True)) is None
    observer.observe(**dict(args, order_active=False, order_price=None))
    c = observer.snapshot()['coverage']
    assert c['BUY:opener:observed'] == 3
    assert c['BUY:opener:valid_active_old_order'] == 2
    assert c['BUY:opener:local_micro:1:1'] == 1
    assert c['BUY:opener:local_micro:0:1'] == 1
    assert observer.snapshot()['complete_eligibility_coverage'] is None


@pytest.mark.parametrize('price,position', [(101, 'better_than_touch'), (97, 'worse_than_last_visible')])
def test_outside_interval_geometry_is_not_always_beyond_depth(price, position):
    reasons, geometry = old_price_support(Cursor(), 'BUY', price, True, None)
    assert 'outside_visible_price_interval' in reasons
    assert geometry['position'] == position


def test_no_order_and_invalid_book_are_different():
    cursor = Cursor()
    assert old_price_support(cursor, 'BUY', None, False, None)[0] == ['no_active_old_order']
    assert old_price_support(cursor, 'BUY', None, True, None)[0] == ['missing_or_invalid_old_price']
    cursor.state.book = None
    assert old_price_support(cursor, 'BUY', 100, True, None)[0] == ['no_visible_book']


def test_timing_wrappers_restore_after_failure():
    from models.replay.phase_timing import ReplayPhaseTiming
    owner = SimpleNamespace(f=lambda: 7)
    original = owner.f
    timing = ReplayPhaseTiming()
    timing.mark('init')
    with pytest.raises(RuntimeError):
        with timing.measure_methods([(owner, 'f', 'f')]):
            assert owner.f() == 7
            raise RuntimeError('synthetic')
    timing.mark(None)
    assert owner.f is original
    assert timing.report()['nested_calls']['f'] == 1
    assert timing.report()['exclusive_wall']['init'] >= 0


def test_downstream_identity_reservation_submission_and_fork_are_distinct():
    import pickle
    observer = UpdateOpportunityObserver(Cursor())
    row = observer.observe(side='BUY', now_ms=11000, call_sequence=1, inventory=0,
        target_price=99, order_price=100, order_id='old', order_active=True,
        needs_update=True, force_update=False, quantity_changed=False, pending=False,
        age_ms=2000, interval_ms=1000, price_delta_ticks=10, fixed_ticks=15,
        route_allowed=True, queue_keep=False, compute={})
    token = observer.token('BUY', 1)
    assert observer.token('SELL', 1) is observer.token('BUY', 2) is None
    denominator = dict(observer.coverage)
    observer.resolved(token, action='replace', final_quantity=.01)
    observer.checked(token, 'replace_throttle', price_blocked=False, age_blocked=False)
    observer.bind_request(token, 'cancel', 'old', 11005)
    assert 'submitted_requests' not in row
    restored = pickle.loads(pickle.dumps(observer))
    restored.submitted('cancel', 'old', 11005)
    restored.submitted('cancel', 'old', 11005)  # duplicate consumer is not a new request
    assert restored.downstream['BUY:opener:request_submitted:cancel'] == 1
    assert 'submitted_requests' not in row
    sample = restored.samples[0]
    assert sample['submitted_requests'][0]['observation_sequence'] == row['sequence']
    assert sample['eligibility_status'] == 'unknown'
    assert sample['counterfactual_effective_ns'] is None
    assert not sample['complete_action_eligibility_verified']
    assert observer.coverage == restored.coverage == denominator
    restored.blocked(restored.token('BUY', 1), 'remaining_same_side_order', 11006)
    assert restored.samples[0]['execution_blocks'][0]['reason'] == 'remaining_same_side_order'
