from __future__ import annotations

import pandas as pd

from research.families.f06_placement_fill_cif.audit.evaluate_direct_fill_cif import _action_resolution


def test_action_resolution_reports_one_tick_discrimination() -> None:
    rows = []
    for side in ("BUY", "SELL"):
        for role in ("opener", "add", "reducing"):
            for horizon_ms in (1_000, 5_000, 10_000):
                for cohort, values in (
                    ("flat", (0.1, 0.1, 0.1)),
                    ("resolved", (0.2, 0.15, 0.1)),
                ):
                    for action, probability in zip(  # noqa: B905 - py3.9 audit runtime
                        ("closer_1tick", "current", "farther_1tick"), values
                    ):
                        rows.append(
                            {
                                "cohort_id": f"{side}:{role}:{horizon_ms}:{cohort}",
                                "day": "2026-01-01",
                                "side": side,
                                "inventory_role": role,
                                "horizon_ms": horizon_ms,
                                "action": action,
                                "probability": probability,
                            }
                        )
    result = _action_resolution(pd.DataFrame(rows))

    assert len(result) == 18
    assert set(result["nonzero_action_delta_fraction"]) == {0.5}
    assert set(result["median_closer_minus_farther_probability"]) == {0.05}
