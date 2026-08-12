from __future__ import annotations

from pathlib import Path

import pandas as pd

from research.families.f07_active_order_continuation.audit.active_order_queue_closure import (
    build_closure_report,
    load_trajectory,
)


def _write_generation(
    root: Path,
    label: str,
    identities: list[tuple[str, int, int]],
    *,
    missing: int = 0,
    unusable: int = 0,
) -> dict:
    manifest_path = root / f"{label}.parquet"
    daily_path = root / f"{label}.csv"
    pd.DataFrame(
        {
            "trajectory_id": [label] * len(identities),
            "side": [row[0] for row in identities],
            "price_tick": [row[1] for row in identities],
            "activate_ts_ms": [row[2] for row in identities],
            "order_id": range(len(identities)),
        }
    ).to_parquet(manifest_path, index=False)
    pd.DataFrame(
        {
            "day": ["2026-06-05"],
            "active_order_queue_mode": ["diagnostic"],
            "active_order_queue_lookup_count": [len(identities)],
            "active_order_queue_missing_count": [missing],
            "active_order_queue_unusable_count": [unusable],
        }
    ).to_csv(daily_path, index=False)
    return load_trajectory(label, manifest_path, daily_path)


def test_closure_uses_market_identity_not_order_id(tmp_path: Path) -> None:
    previous = _write_generation(
        tmp_path,
        "g0",
        [("BUY", 1000, 10), ("SELL", 1001, 20)],
    )
    current = _write_generation(
        tmp_path,
        "g1",
        [("bid", 1000, 10), ("ask", 1001, 20)],
    )

    report = build_closure_report([previous, current])

    assert report["adjacent_comparisons"][0]["overlap_count"] == 2
    assert report["closed"] is True
    assert report["order_id_is_identity"] is False


def test_missing_sparse_seed_keeps_closure_diagnostic(
    tmp_path: Path,
) -> None:
    previous = _write_generation(
        tmp_path,
        "g0",
        [("BUY", 1000, 10)],
    )
    current = _write_generation(
        tmp_path,
        "g1",
        [("BUY", 1000, 10)],
        missing=1,
    )

    report = build_closure_report([previous, current])

    assert report["closed"] is False
    assert report["promotion_status"] == "diagnostic_only"
