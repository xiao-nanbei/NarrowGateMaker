import copy
import pickle
import threading

import pytest

from strategy.quote_schedule import QuoteSchedule, QuoteScheduleWakeup, SIDES


def test_capture_absorbs_only_actual_reads_and_tail_blocks_reentry():
    s = QuoteSchedule()
    s.publish("book", 7)
    s.publish("prediction", 7)
    q = s.claim(1000)
    s.read(q.sequence, "prediction", 7, SIDES)
    s.publish("book", 8)
    s.publish("book", 9)
    s.read(q.sequence, "book", 9, SIDES)
    s.publish("prediction", 8)
    s.publish("book", 10)
    assert s.claim(1006) is None
    s.finish(q.sequence, busy_until_ns=1010)
    assert s.handled["BUY"] == {"book": 9, "prediction": 7}
    assert s.claim(1009) is None
    q2 = s.claim(1010)
    assert q2.claimed == {"book": 10, "prediction": 8}
    s.finish(q2.sequence, busy_until_ns=1010)
    assert s.claim(1010) is None  # zero-duration work cannot self-excite


def test_single_side_does_not_clear_opposite_side():
    s = QuoteSchedule()
    s.publish("book", 1)
    s.terminal("BUY", "cancel-1")
    q = s.claim(0)
    assert q.sides == ("BUY",)
    s.finish(q.sequence, busy_until_ns=0)
    assert not s.unhandled(("BUY",))
    assert s.unhandled(("SELL",))
    q = s.claim(0)
    assert q.sides == SIDES
    s.finish(q.sequence, busy_until_ns=0)
    assert s.claim(0) is None


def test_new_ticket_during_call_is_not_consumed():
    s = QuoteSchedule()
    s.terminal("BUY", "old")
    q = s.claim(0)
    s.terminal("BUY", "new")
    s.finish(q.sequence, busy_until_ns=0)
    assert s.claim(0).ticket == ("BUY", "new")


def test_deadline_once_and_stale_order_invalidated():
    s = QuoteSchedule()
    s.deadline("age", 1000, "BUY", "order-1")
    s.expire(999, [("BUY", "order-1")])
    assert s.claim(999) is None
    s.expire(1000, [("BUY", "order-1")])
    q = s.claim(1000)
    s.finish(q.sequence, busy_until_ns=1000)
    s.expire(1001, [("BUY", "order-1")])
    assert s.claim(1001) is None
    s.deadline("age", 2000, "BUY", "order-1")
    s.expire(2000, [("BUY", "order-2")])
    assert s.claim(2000) is None


def test_duplicate_version_and_keep_do_not_generate_work():
    s = QuoteSchedule()
    assert s.publish("book", 1)
    assert not s.publish("book", 1)
    q = s.claim(0)
    s.finish(q.sequence, busy_until_ns=0, blocked=True)
    assert s.claim(100) is None
    assert s.publish("book", 2)  # new same-price view is still a publication
    assert s.claim(100) is not None


def test_checkpoint_inflight_and_independent_branch():
    s = QuoteSchedule()
    s.publish("book", 9)
    q = s.claim(0)
    s.read(q.sequence, "book", 9, SIDES)
    s.publish("book", 10)
    restored = pickle.loads(pickle.dumps(s))
    fork = copy.deepcopy(restored)
    restored.finish(q.sequence, busy_until_ns=10)
    assert restored.claim(10).claimed["book"] == 10
    assert fork.inflight.sequence == q.sequence
    assert fork.handled["BUY"] == {}


def test_reject_future_reads_and_wrong_call():
    s = QuoteSchedule()
    s.publish("book", 1)
    q = s.claim(0)
    with pytest.raises(ValueError, match="not published"):
        s.read(q.sequence, "book", 2, SIDES)
    with pytest.raises(ValueError, match="current quote"):
        s.finish(q.sequence + 1, busy_until_ns=0)


def test_entry_frozen_read_does_not_acknowledge_newer_claimed_prediction():
    s = QuoteSchedule()
    s.publish("prediction", 8)
    q = s.claim(100)
    # The adapter's entry-frozen prediction can lag the latest committed input.
    s.read(q.sequence, "prediction", 7, SIDES)
    s.finish(q.sequence, busy_until_ns=100)
    assert s.handled["BUY"]["prediction"] == 7
    assert s.handled["SELL"]["prediction"] == 7
    q = s.claim(100)
    assert q is not None
    s.read(q.sequence, "prediction", 8, SIDES)
    s.finish(q.sequence, busy_until_ns=100)
    assert s.claim(100) is None


def test_invalid_read_side_does_not_partially_change_watermarks():
    s = QuoteSchedule()
    s.publish("book", 1)
    s.terminal("BUY", "cancel")
    q = s.claim(0)
    with pytest.raises(ValueError, match="outside original route"):
        s.read(q.sequence, "book", 1, ("BUY", "SELL"))
    assert q.reads == {}


@pytest.mark.parametrize("when", ["before_check", "after_clear", "during_wait"])
def test_visible_publication_cannot_be_lost_by_wait(when):
    state = QuoteSchedule()
    event = threading.Event()
    wake = QuoteScheduleWakeup(state, event)
    if when == "before_check":
        wake.publish("book", 1)
        assert not wake.prepare_wait(0)
    else:
        assert wake.prepare_wait(0)
        if when == "after_clear":
            wake.publish("book", 1)
            assert event.wait(0)
        else:
            waiting = threading.Event()
            observed = []

            def waiter():
                waiting.set()
                observed.append(event.wait(1))

            thread = threading.Thread(target=waiter)
            thread.start()
            assert waiting.wait(1)
            wake.publish("book", 1)
            thread.join(1)
            assert observed == [True]
    assert wake.claim(0).claimed == {"book": 1}
