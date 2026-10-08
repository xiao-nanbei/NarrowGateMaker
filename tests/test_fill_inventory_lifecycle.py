from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from research.families.f10_live_replay_attribution.audit.fill_inventory_lifecycle import (
    _load_input_paths,
    reconstruct_lifetimes,
)


def test_fifo_and_lifo_expose_attribution_sensitivity() -> None:
    fills = pd.DataFrame(
        {
            "day": ["2026-01-01"] * 3,
            "client_order_id": ["a", "b", "c"],
            "side": ["BUY", "BUY", "SELL"],
            "filled": [1, 1, 1],
            "fill_ts": [10.0, 20.0, 30.0],
            "filled_qty": [0.001, 0.001, 0.001],
            "avg_fill_price": [100.0, 101.0, 102.0],
        }
    )
    fifo = reconstruct_lifetimes(fills, matching="fifo")
    lifo = reconstruct_lifetimes(fills, matching="lifo")
    assert fifo.loc[fifo["observed_close"].eq(1), "duration_s"].tolist() == [20.0]
    assert lifo.loc[lifo["observed_close"].eq(1), "duration_s"].tolist() == [10.0]
    assert int(fifo["observed_close"].eq(0).sum()) == 1
    assert fifo.loc[fifo["observed_close"].eq(1), "lot_pnl"].tolist() == [0.002]


def test_input_filelist_is_strict_and_rejects_duplicates(tmp_path: Path) -> None:
    order_file = tmp_path / "orders.csv"
    order_file.write_text("day\n2026-01-01\n", encoding="utf-8")
    filelist = tmp_path / "filelist.csv"
    filelist.write_text(
        f"day,order_level_csv\n2026-01-01,{order_file}\n",
        encoding="utf-8",
    )
    assert _load_input_paths(
        data_dir=None,
        input_glob=None,
        input_filelist=filelist,
    ) == [order_file.resolve()]

    filelist.write_text(
        "day,order_level_csv\n"
        f"2026-01-01,{order_file}\n"
        f"2026-01-02,{order_file}\n",
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="duplicate"):
        _load_input_paths(
            data_dir=None,
            input_glob=None,
            input_filelist=filelist,
        )
