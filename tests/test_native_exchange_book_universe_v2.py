from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from models.audit.native_exchange_book_universe_v2 import (
    freeze_complete_warmup_universe,
    scan_complete_warmup_universe,
)


def _write_day(root: Path, day: str) -> None:
    start = datetime.strptime(day, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    for hour in range(24):
        timestamp = start + timedelta(hours=hour)
        path = (
            root
            / "binance_futures"
            / timestamp.strftime("%Y-%m-%d")
            / timestamp.strftime("%H")
            / "BTCUSDC_orderbook.parquet.zst"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(timestamp.isoformat().encode("ascii"))


def test_complete_warmup_universe_excludes_only_missing_source_days(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    for day in ("2026-01-01", "2026-01-02", "2026-01-04"):
        _write_day(raw_root, day)
    good_days = tmp_path / "good_days.csv"
    pd.DataFrame(
        {"day": ["2026-01-02", "2026-01-04"]}
    ).to_csv(good_days, index=False)

    availability, raw_paths = scan_complete_warmup_universe(
        raw_root=raw_root,
        good_days_path=good_days,
    )

    assert availability["candidate"].tolist() == [True, False]
    assert availability.loc[1, "exclusion_reason"] == (
        "missing_prior_day_warmup"
    )
    assert len(raw_paths) == 48


def test_freeze_complete_warmup_universe_records_source_identity(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    _write_day(raw_root, "2026-01-01")
    _write_day(raw_root, "2026-01-02")
    good_days = tmp_path / "good_days.csv"
    pd.DataFrame({"day": ["2026-01-02"]}).to_csv(
        good_days,
        index=False,
    )
    output = tmp_path / "universe"

    payload = freeze_complete_warmup_universe(
        raw_root=raw_root,
        good_days_path=good_days,
        output_dir=output,
        hash_raw_files=True,
    )

    assert payload["candidate_days_count"] == 1
    assert payload["excluded_days_count"] == 0
    assert payload["raw_unique_file_count"] == 48
    assert payload["raw_files_hashed_by_content"] is True
    assert (output / "candidate_days.csv").is_file()
    assert len(
        (output / "raw_files.manifest")
        .read_text(encoding="utf-8")
        .splitlines()
    ) == 48
