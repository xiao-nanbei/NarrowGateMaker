from pathlib import Path

import pandas as pd

from data.audit_raw_trades import buyer_maker_counts, scan_raw_trade_file


def test_buyer_maker_counts_accepts_bool_and_text() -> None:
    assert buyer_maker_counts(pd.Series([True, False, True])) == (2, 1, 0)
    assert buyer_maker_counts(
        pd.Series(["true", "FALSE", "1", "0", "bad"])
    ) == (2, 2, 1)


def test_raw_trade_scan_requires_both_sides(tmp_path: Path) -> None:
    path = tmp_path / "BTCUSDC-trades-2026-01-01.csv"
    pd.DataFrame(
        {
            "id": [1, 2],
            "time": [1_767_225_600_001, 1_767_225_600_002],
            "is_buyer_maker": [True, False],
        }
    ).to_csv(path, index=False)

    stats = scan_raw_trade_file(path, "BTCUSDC", chunk_size=1)

    assert stats.side_complete is True
    assert stats.buyer_maker_true_count == 1
    assert stats.buyer_maker_false_count == 1
