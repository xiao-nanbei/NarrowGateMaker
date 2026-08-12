from __future__ import annotations

from models.audit.native_gap_segments import assign_segment, build_day_segments


def test_gap_segments_censor_after_maximum_source_age() -> None:
    start = 1_785_196_800_000  # 2026-07-28 UTC
    timestamps = [start + 100, start + 1_000, start + 9_000, start + 9_100]
    segments, gaps = build_day_segments(
        "2026-07-28", timestamps, maximum_gap_ms=5_000
    )

    assert len(gaps) == 1
    assert gaps.iloc[0]["censor_ts_ms"] == start + 6_000
    assert segments.iloc[0]["end_ts_ms_exclusive"] == start + 6_000
    assert segments.iloc[1]["start_ts_ms"] == start + 9_000
    assert assign_segment(
        [start + 5_999, start + 6_000, start + 9_000], segments
    ).tolist() == [1, 0, 2]
