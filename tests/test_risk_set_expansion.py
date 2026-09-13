from __future__ import annotations

import pandas as pd

from research.families.f06_placement_fill_cif.audit.risk_set_expansion import (
    EVENT_ACK,
    EVENT_CENSOR,
    EVENT_FILL,
    expand_competing_risk_intervals_native,
    expand_competing_risk_intervals_python,
)


def test_native_risk_set_matches_reference_at_edges_and_censoring() -> None:
    durations = [0.0, 5.0, 7.5, 25.0]
    events = [EVENT_FILL, EVENT_ACK, EVENT_CENSOR, EVENT_FILL]
    edges = [0.0, 5.0, 10.0, 20.0]

    native = expand_competing_risk_intervals_native(durations, events, edges)
    reference = expand_competing_risk_intervals_python(durations, events, edges)

    pd.testing.assert_frame_equal(native, reference, check_dtype=False)
    first = native.loc[native["row_index"] == 0].iloc[0]
    assert first["fill_target"] == 1
    boundary = native.loc[native["row_index"] == 1]
    assert len(boundary) == 1
    assert boundary.iloc[0]["ack_target"] == 1
    censored = native.loc[native["row_index"] == 2]
    assert censored.iloc[-1]["exposure_fraction"] == 0.5
