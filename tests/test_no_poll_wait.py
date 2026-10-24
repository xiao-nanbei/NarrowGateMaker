from models.backtest_tick import next_no_poll_wait_ms


def test_no_mandatory_delay_after_work():
    assert next_no_poll_wait_ms(1000, [1002, 1040]) == 1002
    assert next_no_poll_wait_ms(1000, [1000.25]) == 1001


def test_overdue_or_absent_deadline_does_not_busy_loop():
    assert next_no_poll_wait_ms(1000, [None, 999, 1000]) == 1100
    assert next_no_poll_wait_ms(1000, []) == 1100


def test_future_strategy_deadline_is_not_advanced():
    assert next_no_poll_wait_ms(1000, [1051]) == 1051
    assert next_no_poll_wait_ms(1000, [2000]) == 1100
