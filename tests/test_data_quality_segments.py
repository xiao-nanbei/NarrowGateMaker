from pathlib import Path

import pandas as pd

import data_quality
from data_quality import (
    COMPLETE_DATA_POLICY,
    DataCompletenessPolicy,
    continuous_segment_ids,
    excluded_orderbook_days,
    filter_paths_for_orderbook_quality,
    mask_valid_horizon,
)


def test_mask_valid_horizon_respects_monotonic_segments():
    idx = pd.to_datetime(
        [
            "2026-06-01T00:00:00Z",
            "2026-06-01T00:00:10Z",
            "2026-06-01T00:00:20Z",
            "2026-06-01T00:01:00Z",
        ],
        utc=True,
    )

    assert continuous_segment_ids(idx, max_gap_s=15).tolist() == [0, 0, 0, 1]
    assert mask_valid_horizon(idx, horizon_s=15, max_gap_s=15).tolist() == [
        True,
        False,
        False,
        False,
    ]


def test_mask_valid_horizon_handles_non_monotonic_index_without_cross_row_leakage():
    # The function is usually called on sorted UTC indexes, but callers may pass
    # unsorted rows after merges.  The fallback must search inside each segment,
    # not the globally sorted timestamp array, or it can validate the wrong row.
    idx = pd.to_datetime(
        [
            "2026-06-01T00:00:20Z",
            "2026-06-01T00:00:00Z",
            "2026-06-01T00:00:10Z",
        ],
        utc=True,
    )

    assert continuous_segment_ids(idx, max_gap_s=15).tolist() == [0, 1, 1]
    assert mask_valid_horizon(idx, horizon_s=5, max_gap_s=15).tolist() == [
        False,
        True,
        False,
    ]


def test_cross_day_snapshot_recovery_removes_obsolete_hard_exclusion():
    assert "2026-07-13" not in excluded_orderbook_days("BTCUSDC")
    assert "strict_reconstruction_missing_opening_snapshot" not in (
        COMPLETE_DATA_POLICY.reasons_for_day("BTCUSDC", "2026-07-13")
    )


def test_retired_btcusdt_book_gap_does_not_exclude_btcusdc_execution():
    day = "2026-04-01"

    assert day in excluded_orderbook_days("BTCUSDT")
    assert day not in excluded_orderbook_days("BTCUSDC")
    legacy = DataCompletenessPolicy(cross_symbol_orderbook_exclusions=True)
    assert day in legacy.excluded_orderbook_days("BTCUSDC")


def test_explicit_formal_day_supersedes_only_its_legacy_audit_exclusion(monkeypatch):
    allowed_day = "2026-05-07"
    still_excluded_day = "2026-05-08"
    monkeypatch.setattr(
        data_quality,
        "AUDIT_BAD_ORDERBOOK_DAYS",
        frozenset(
            {
                ("BTCUSDC", allowed_day),
                ("BTCUSDC", still_excluded_day),
            }
        ),
    )
    assert allowed_day in excluded_orderbook_days("BTCUSDC")
    assert still_excluded_day in excluded_orderbook_days("BTCUSDC")
    paths = [
        Path(f"BTCUSDC-trades-{allowed_day}.csv"),
        Path(f"BTCUSDC-trades-{still_excluded_day}.csv"),
    ]

    assert filter_paths_for_orderbook_quality(paths, "BTCUSDC") == []
    assert filter_paths_for_orderbook_quality(
        paths,
        "BTCUSDC",
        explicitly_allowed_days=(allowed_day,),
    ) == [paths[0]]
