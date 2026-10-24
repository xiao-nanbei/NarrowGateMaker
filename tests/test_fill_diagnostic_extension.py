import math

import numpy as np
import pytest

from models.replay.fill_diagnostics import extend_fill_diagnostics, fill_markout_diagnostics


def row():
    value = dict(fill_ts=1708, quote_px=100., side='SELL', fill_fee_rate=.001)
    value.update(fill_markout_diagnostics(
        np.array([1708, 2400]), np.array([100., 101.]), fill_ts_ms=1708,
        quote_px=100., side='SELL', fee_rate=.001, outcome_end_ts_ms=2499))
    return value


def test_temporary_window_censor_resolves_without_changing_account_fields():
    value = row()
    assert math.isnan(value['markout_1s'])
    extend_fill_diagnostics([value], np.array([2000, 2708, 3000]),
                            np.array([101., 99., 100.]), outcome_end_ts_ms=3000)
    assert value['markout_1s'] == 1.
    assert value['ev_1s'] == .9
    assert value['markout_1s_status'] == 'exact'
    assert value['markout_5s_status'] == 'censored'
    # Measured horizons must not change on subsequent windows.
    extend_fill_diagnostics([value], np.array([3000, 6708]),
                            np.array([900., 102.]), outcome_end_ts_ms=6708)
    assert value['markout_1s'] == 1.
    assert value['markout_5s'] == -2.
    assert value['fill_ts'] == 1708 and value['quote_px'] == 100.


def test_extension_rejects_discarded_unknown_endpoint():
    with pytest.raises(ValueError, match='unresolved'):
        extend_fill_diagnostics([row()], np.array([3000]), np.array([100.]),
                                outcome_end_ts_ms=3000)
