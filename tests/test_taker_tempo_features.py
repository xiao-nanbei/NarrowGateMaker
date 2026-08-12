import json
from pathlib import Path

import pandas as pd

from research.families.f03_causal_13_head.taker_tempo_features import write_manifest


def test_write_manifest_binds_raw_and_both_taker_sides(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw" / "BTCUSDC"
    sidecar_root = tmp_path / "sidecars"
    sidecar_dir = sidecar_root / "BTCUSDC"
    raw_dir.mkdir(parents=True)
    sidecar_dir.mkdir(parents=True)

    day = "2026-07-04"
    raw_path = raw_dir / f"BTCUSDC-trades-{day}.csv"
    raw_path.write_text("id,price,qty,quote_qty,time,is_buyer_maker\n", encoding="utf-8")
    sidecar_path = sidecar_dir / f"BTCUSDC-trade-tempo-{day}.parquet"
    pd.DataFrame(
        {
            "buy_trade_count": [2, 0],
            "sell_trade_count": [0, 3],
        }
    ).to_parquet(sidecar_path)

    manifest_path = tmp_path / "manifest.json"
    write_manifest([raw_path], "BTCUSDC", sidecar_root, manifest_path)

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["schema"] == "narrowgate.taker_tempo_manifest.v1"
    assert payload["daily_file_count"] == 1
    assert payload["daily_files"][0]["buy_taker_trades"] == 2
    assert payload["daily_files"][0]["sell_taker_trades"] == 3
    assert len(payload["daily_files"][0]["raw_sha256"]) == 64
    assert len(payload["daily_files"][0]["sidecar_sha256"]) == 64
