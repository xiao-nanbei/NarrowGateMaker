"""The event clock has one owner without loading the economic executor."""
import subprocess
import sys


def test_executor_uses_the_single_clock_owner():
    from models import backtest_tick
    from narrowgate.replay.event_clock import build_replay_event_clock

    assert backtest_tick.build_replay_event_clock is build_replay_event_clock
    assert build_replay_event_clock.__module__ == 'narrowgate.replay.event_clock'


def test_clock_import_does_not_load_executor_live_or_research():
    result = subprocess.run(
        [sys.executable, '-c', '''
import sys
import pandas as pd
from narrowgate.replay.event_clock import build_replay_event_clock
frame = pd.DataFrame(dict(transact_time=[100, 100], price=[10., 11.],
                          quantity=[.1, .2], is_buyer_maker=[0, 1]))
events, count = build_replay_event_clock(frame, mode='merged', interval_ms=100,
                                        system_event_ts_ms=[100])
assert count == 2
assert events['price'].tolist() == [10., 11., 11.]
assert events['quantity'].tolist() == [.1, .2, 0.]
assert events['is_buyer_maker'].tolist() == [0, 1, 4]
assert events['_is_execution_trade'].tolist() == [True, True, False]
assert 'models.backtest_tick' not in sys.modules
assert not any(n == 'research' or n.startswith(('research.', 'live.')) for n in sys.modules)
'''], capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
