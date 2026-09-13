import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f03_causal_13_head.taker_tempo_features import (
    contiguous_warmup_paths, process_file, write_manifest,
)


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


def test_canonical_individual_tempo_preserves_midnight_context(tmp_path: Path) -> None:
    source = tmp_path / "raw"
    paths = {}
    for day, times, ids in (
        ("2026-01-01", ["23:59:59.000", "23:59:59.500"], [1, 2]),
        ("2026-01-02", ["00:00:00.100", "00:00:00.500"], [3, 4]),
    ):
        path = source / day / "trades.parquet"
        path.parent.mkdir(parents=True)
        times_ms = [pd.Timestamp(day + " " + time, tz="UTC").value // 1_000_000 for time in times]
        pd.DataFrame({
            "id": ids, "price": ["100", "100"], "qty": ["1", "2"],
            "quote_qty": ["100", "200"], "timestamp": [time * 1000 for time in times_ms],
            "is_buyer_maker": [False, False],
        }).to_parquet(path)
        paths[day] = path
    target = paths["2026-01-02"]
    warmup = contiguous_warmup_paths(target, paths, 7)
    assert warmup == [paths["2026-01-01"]]
    status, rows, output = process_file(
        target, "BTCUSDC", tmp_path / "out", chunk_size=1, verbose=False,
        overwrite=True, dense=True, warmup_paths=tuple(warmup),
    )
    result = pd.read_parquet(output)
    assert status == "ok" and rows == 2
    assert result["trade_count"].sum() == 2  # Context is not counted twice.
    assert result.iloc[0]["max_same_side_run"] == 4
    assert result.iloc[0]["trade_count_sum_5s"] == 4
    assert result.iloc[0]["interarrival_ms_sum"] == 1000
    assert result.index.min() == pd.Timestamp("2026-01-02", tz="UTC").value // 1_000_000
    with pytest.raises(ValueError, match="adjacent UTC"):
        process_file(target, "BTCUSDC", tmp_path / "bad", chunk_size=1, verbose=False,
                     overwrite=True, dense=True, warmup_paths=(target,))


def test_tempo_context_does_not_skip_calendar_gap(tmp_path: Path) -> None:
    paths = {day: tmp_path / day / "trades.parquet" for day in ("2026-01-01", "2026-01-03")}
    assert contiguous_warmup_paths(paths["2026-01-03"], paths, 7) == []
