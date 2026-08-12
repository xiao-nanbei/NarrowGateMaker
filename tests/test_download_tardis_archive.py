from datetime import date
from pathlib import Path

import pytest
import zstandard

from data.download_tardis_archive import (
    Contract,
    RemoteTarget,
    _days,
    _parse_contract,
    _space_preflight,
    _target,
    _validate_zstd,
    resolve_tardis_artifact_path,
)


def test_contract_and_target_identity() -> None:
    contract = _parse_contract("binance-futures,incremental_book_L2,BTCUSDC")
    assert contract == Contract("binance-futures", "incremental_book_L2", "BTCUSDC")
    url, relative = _target("https://example.test/tardis/", contract, date(2026, 1, 2))
    assert relative == (
        "binance-futures/incremental_book_L2/2026/01/02/BTCUSDC.csv.zst"
    )
    assert url == f"https://example.test/tardis/{relative}"


def test_day_range_is_closed_and_chronological() -> None:
    assert _days(date(2026, 1, 30), date(2026, 2, 1)) == [
        date(2026, 1, 30),
        date(2026, 1, 31),
        date(2026, 2, 1),
    ]


def test_space_preflight_fails_closed(monkeypatch, tmp_path: Path) -> None:
    target = RemoteTarget(
        "binance-futures",
        "book_ticker",
        "BTCUSDC",
        "2026-01-01",
        "https://example.test/file",
        "binance-futures/book_ticker/2026/01/01/BTCUSDC.csv.zst",
        True,
        100,
        "etag",
        "",
        "bytes",
        "",
    )
    usage = type("Usage", (), {"free": 50})()
    monkeypatch.setattr(
        "data.download_tardis_archive.shutil.disk_usage", lambda _path: usage
    )
    with pytest.raises(RuntimeError, match="insufficient space"):
        _space_preflight(
            [target],
            tmp_path,
            reserve_gib=0.0,
            factor=2.5,
            min_free_gib=0.0,
        )


def test_zstd_validation_records_csv_boundaries(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv.zst"
    payload = b"a,b\n1,2\n3,4\n"
    path.write_bytes(zstandard.ZstdCompressor().compress(payload))
    result = _validate_zstd(path)
    assert result == {
        "decompressed_bytes": len(payload),
        "csv_rows": 2,
        "header": "a,b",
        "first_data_row": "1,2",
        "last_data_row": "3,4",
    }


def test_relocated_tardis_path_resolves_only_when_target_exists(
    monkeypatch, tmp_path: Path
) -> None:
    direct = tmp_path / "tardis"
    legacy = direct / "0730-beinan"
    relocated = direct / "binance-futures/book_ticker/file.csv.zst"
    relocated.parent.mkdir(parents=True)
    relocated.write_bytes(b"payload")
    monkeypatch.setattr("data.download_tardis_archive.DEFAULT_OUTPUT_ROOT", direct)
    monkeypatch.setattr("data.download_tardis_archive.LEGACY_OUTPUT_ROOT", legacy)

    assert resolve_tardis_artifact_path(
        legacy / "binance-futures/book_ticker/file.csv.zst"
    ) == relocated
    missing = legacy / "binance-futures/book_ticker/missing.csv.zst"
    assert resolve_tardis_artifact_path(missing) == missing
