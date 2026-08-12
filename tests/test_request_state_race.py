from __future__ import annotations

import numpy as np
import pandas as pd

from research.families.f06_placement_fill_cif.audit.request_state_race import (
    CauseSpecificRateModel,
    empirical_bin_edges,
    pending_event_kind,
)


def test_empirical_edges_are_train_derived_and_strict() -> None:
    edges = empirical_bin_edges(np.asarray([1.0, 2.0, 3.0, 100.0]), maximum_bins=3)
    assert edges[0] == 0.0
    assert edges[-1] == 100.0
    assert np.all(edges[1:] > edges[:-1])


def test_pending_event_kind_uses_first_fill_ack_race() -> None:
    frame = pd.DataFrame(
        {
            "first_pending_cancel_fill_ts_ns": [10, 30, 0],
            "actual_cancel_ack_ts_ns": [20, 20, 40],
            "pending_cancel_fill": [1, 1, 0],
            "cancel_ack_observed": [1, 1, 1],
        }
    )
    assert pending_event_kind(frame).tolist() == [1, 2, 2]


def test_cause_rate_cif_is_monotone_and_respects_simplex() -> None:
    rows = 240
    frame = pd.DataFrame(
        {
            "x": np.tile([0.0, 1.0], rows // 2),
            "inventory_role": np.tile(["opener", "add", "reducing"], rows // 3),
            "duration": np.tile([5.0, 10.0, 20.0, 40.0], rows // 4),
        }
    )
    event = np.zeros(rows, dtype=np.uint8)
    event[::7] = 1
    event[3::5] = 2
    model = CauseSpecificRateModel.fit(
        frame,
        duration_column="duration",
        event_kind=event,
        numeric_features=("x",),
        categorical_features=("inventory_role",),
        maximum_bins=6,
        include_ack=True,
    )
    cif = model.predict_cif(frame.iloc[:5])
    for _, group in cif.groupby("row_index"):
        assert group["fill_cif"].is_monotonic_increasing
        assert group["ack_cif"].is_monotonic_increasing
        np.testing.assert_allclose(
            group["fill_cif"] + group["ack_cif"] + group["survival"],
            1.0,
            atol=1e-10,
        )
