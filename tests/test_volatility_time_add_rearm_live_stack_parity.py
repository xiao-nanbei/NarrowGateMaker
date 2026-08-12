from __future__ import annotations

from pathlib import Path

import pandas as pd

from research.families.f09_campaign_action_uplift.audit.volatility_time_add_rearm_live_stack_parity import (
    annotate_reconstructible_lineage,
    build_bbo_source_manifest,
)


def _events(sides: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day": ["2026-01-02"] * len(sides),
            "side": sides,
            "fill_ts": [1_000 * (index + 1) for index in range(len(sides))],
            "order_id": list(range(len(sides))),
        }
    )


def test_lineage_requires_two_within_day_side_transitions() -> None:
    annotated, daily = annotate_reconstructible_lineage(
        _events(["BUY", "BUY", "SELL", "SELL", "BUY", "BUY"])
    )
    assert annotated["cooldown_lineage_reconstructible"].tolist() == [
        False,
        False,
        False,
        False,
        True,
        True,
    ]
    assert daily.loc[0, "side_transitions"] == 2
    assert daily.loc[0, "reconstructible_fill_rows"] == 2
    assert bool(daily.loc[0, "lineage_supported"])


def test_lineage_does_not_invent_state_with_one_transition() -> None:
    annotated, daily = annotate_reconstructible_lineage(
        _events(["SELL", "SELL", "BUY", "BUY"])
    )
    assert not annotated["cooldown_lineage_reconstructible"].any()
    assert not bool(daily.loc[0, "lineage_supported"])


def test_bbo_manifest_requires_and_hashes_dminus1(tmp_path: Path) -> None:
    root = tmp_path / "normalized"
    bbo = root / "bbo"
    bbo.mkdir(parents=True)
    previous = bbo / "BTCUSDC-bbo-2026-01-01.parquet"
    target = bbo / "BTCUSDC-bbo-2026-01-02.parquet"
    previous.write_bytes(b"warmup")
    target.write_bytes(b"target")

    import hashlib

    quality = pd.DataFrame(
        [
            {
                "day": "2026-01-01",
                "formal_eligible": True,
                "bbo_sha256": hashlib.sha256(b"warmup").hexdigest(),
                "bbo_coverage": 1.0,
                "bbo_p99_gap_s": 0.1,
            },
            {
                "day": "2026-01-02",
                "formal_eligible": True,
                "bbo_sha256": hashlib.sha256(b"target").hexdigest(),
                "bbo_coverage": 1.0,
                "bbo_p99_gap_s": 0.1,
            },
        ]
    )
    quality_path = root / "daily_quality.csv"
    quality.to_csv(quality_path, index=False)
    rows = build_bbo_source_manifest(root, quality_path, ["2026-01-02"])
    assert [row["role"] for row in rows] == [
        "warmup_d_minus_1",
        "target_day",
    ]
    assert rows[0]["used_by_days"] == ["2026-01-02"]
