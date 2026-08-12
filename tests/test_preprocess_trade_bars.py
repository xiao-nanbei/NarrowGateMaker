from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from features.preprocess import process_file


def test_individual_trade_bar_writes_atomic_provenance(tmp_path: Path) -> None:
    source = tmp_path / "BTCUSDT-trades-2026-01-01.csv"
    source.write_text(
        "id,price,qty,quote_qty,time,is_buyer_maker\n"
        "1,100.0,0.1,10.0,1767225600100,false\n"
        "2,101.0,0.2,20.2,1767225600900,true\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "bars"
    out_dir.mkdir()

    output, status, rows, source_rows = process_file(
        source,
        "BTCUSDT",
        out_dir,
        data_type="trades",
    )

    assert status == "ok"
    assert rows == 1
    assert source_rows == 2
    frame = pd.read_parquet(output)
    assert frame.iloc[0]["close"] == 101.0
    assert frame.iloc[0]["trade_count"] == 2
    assert frame.iloc[0]["last_event_ts_ms"] == 1_767_225_600_900
    metadata = json.loads(
        output.with_suffix(output.suffix + ".meta.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["complete"] is True
    assert metadata["schema_version"] == "binance_individual_trade_bar_1s.v1"
    assert metadata["source_data_type"] == "trades"
    assert metadata["utc_day"] == "2026-01-01"
    assert metadata["causal_visible_at"] == "t+1s"
    assert not list(out_dir.glob("*.tmp"))
