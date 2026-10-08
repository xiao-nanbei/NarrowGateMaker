from __future__ import annotations

from research.families.f07_active_order_continuation.audit.active_order_lifecycle_cif_100ms_training_v1_5 import (
    IDENTITY,
    _canonical_sha256,
)
from research.families.f07_active_order_continuation.audit.active_order_lifecycle_cif_cpp_parity_v1_5 import (
    ArtifactRateTable,
)


def _artifact() -> dict[str, object]:
    payload: dict[str, object] = {
        "identity": IDENTITY,
        "cells": [
            {
                "side": "BUY",
                "phase": "ACTIVE",
                "risk_age_bin": 0,
                "remaining_class": "full",
                "utc_hour_bin": 0,
                "rates_per_s": {
                    "full_fill": 0.2,
                    "cancel_ack": 0.1,
                    "other_terminal": 0.01,
                },
            }
        ],
        "parent_rates": [
            {
                "side": "BUY",
                "phase": "ACTIVE",
                "risk_age_bin": 0,
                "rates_per_s": {
                    "full_fill": 0.15,
                    "cancel_ack": 0.05,
                    "other_terminal": 0.0,
                },
            }
        ],
    }
    payload["canonical_artifact_sha256"] = _canonical_sha256(payload)
    return payload


def test_artifact_rate_lookup_maps_aggregate_fill_to_first_kernel_channel() -> None:
    table = ArtifactRateTable(_artifact())
    exact = table.rates(
        side="BUY",
        phase="ACTIVE",
        age_s=0.1,
        remaining_class="full",
        utc_hour_bin=0,
    )
    assert exact.tolist() == [0.2, 0.0, 0.1, 0.01]
    parent = table.rates(
        side="BUY",
        phase="ACTIVE",
        age_s=0.1,
        remaining_class="partial",
        utc_hour_bin=3,
    )
    assert parent.tolist() == [0.15, 0.0, 0.05, 0.0]
