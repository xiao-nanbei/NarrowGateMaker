import csv
import gzip
import json
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

from data.import_bitget_archive import (
    BitgetArchiveImporter,
    required_source_days,
)

HEADER = "trade_id,timestamp,price,side,volume(quote),size(base)\n"


def _write_archive_part(path, member, rows):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member, HEADER + "".join(rows))


def test_archive_utc8_days_are_resliced_to_utc(tmp_path):
    archive_dir = tmp_path / "archive"
    out_dir = tmp_path / "daily"
    archive_dir.mkdir()
    target = date(2026, 1, 1)
    start_ms = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)

    # Filename day 20260101 starts at 2025-12-31 16:00Z.  Only its final
    # eight hours belong to target UTC day; the remaining sixteen hours come
    # from filename day 20260102.
    _write_archive_part(
        archive_dir / "20260101_001.zip",
        "BTCUSDT_UMCBL_20260101_001.csv",
        [
            f"1,{start_ms - 1},60000,buy,6,0.0001\n",
            f"2,{start_ms},60001,sell,6,0.0001\n",
        ],
    )
    _write_archive_part(
        archive_dir / "20260102_001.zip",
        "BTCUSDT_UMCBL_20260102_001.csv",
        [
            f"3,{start_ms + 57_600_000},60002,buy,6,0.0001\n",
            f"4,{start_ms + 86_400_000},60003,sell,6,0.0001\n",
        ],
    )

    importer = BitgetArchiveImporter(
        archive_dir=archive_dir,
        out_dir=out_dir,
        symbol="BTCUSDT",
        product_type="USDT-FUTURES",
    )
    result = importer.import_day(target)
    assert result.status == "imported"
    assert result.rows == 2
    with gzip.open(result.path, "rt", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert [row["trade_id"] for row in rows] == ["2", "3"]
    assert [int(row["exchange_event_ts_ms"]) for row in rows] == [start_ms, start_ms + 57_600_000]

    meta = json.loads((out_dir / "bitget_BTCUSDT_trades_2026-01-01.csv.gz.meta.json").read_text())
    assert meta["source_kind"] == "bitget_history_data_download"
    assert meta["archive_parts"] == 2


def test_archive_missing_next_utc8_day_fails_closed(tmp_path):
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    _write_archive_part(
        archive_dir / "20260101_001.zip",
        "BTCUSDT_UMCBL_20260101_001.csv",
        ["1,1767225600000,60000,buy,6,0.0001\n"],
    )
    importer = BitgetArchiveImporter(
        archive_dir=archive_dir,
        out_dir=tmp_path / "daily",
        symbol="BTCUSDT",
        product_type="USDT-FUTURES",
    )
    result = importer.import_day(date(2026, 1, 1))
    assert result.status == "archive_missing"
    assert "2026-01-02" in result.message


def test_bitget_downloader_derives_source_union_from_missing_targets(tmp_path):
    out_dir = tmp_path / "daily"
    out_dir.mkdir()
    complete_day = date(2026, 1, 1)
    target = out_dir / "bitget_BTCUSDT_trades_2026-01-01.csv.gz"
    target.touch()
    Path(str(target) + ".meta.json").write_text(
        json.dumps({"complete": True, "utc_day": complete_day.isoformat()}),
        encoding="utf-8",
    )

    required = required_source_days(
        [complete_day, date(2026, 1, 3), date(2026, 1, 4)],
        out_dir=out_dir,
        symbol="BTCUSDT",
    )

    assert required == [date(2026, 1, 3), date(2026, 1, 4), date(2026, 1, 5)]


def test_bitget_downloader_redownloads_target_with_invalid_meta(tmp_path):
    out_dir = tmp_path / "daily"
    out_dir.mkdir()
    target = out_dir / "bitget_BTCUSDT_trades_2026-01-01.csv.gz"
    target.touch()
    Path(str(target) + ".meta.json").write_text("{}", encoding="utf-8")

    required = required_source_days(
        [date(2026, 1, 1)], out_dir=out_dir, symbol="BTCUSDT"
    )

    assert required == [date(2026, 1, 1), date(2026, 1, 2)]


def test_bitget_downloader_skips_all_complete_targets(tmp_path):
    out_dir = tmp_path / "daily"
    out_dir.mkdir()
    complete_day = date(2026, 1, 1)
    target = out_dir / "bitget_BTCUSDT_trades_2026-01-01.csv.gz"
    target.touch()
    Path(str(target) + ".meta.json").write_text(
        json.dumps({"complete": True, "utc_day": complete_day.isoformat()}),
        encoding="utf-8",
    )

    assert required_source_days(
        [complete_day], out_dir=out_dir, symbol="BTCUSDT"
    ) == []
