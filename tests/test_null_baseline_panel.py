from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

from research.families.f05_fill_quality_quote_ev.audit.null_baseline_panel import _order_level_filelist


def _write_filelist(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["day", "order_level_csv", "size_bytes", "sha256"],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_order_level_filelist_binds_size_and_sha256(tmp_path: Path) -> None:
    panel = tmp_path / "orders.csv"
    panel.write_text("day,filled\n2026-07-01,0\n", encoding="utf-8")
    digest = hashlib.sha256(panel.read_bytes()).hexdigest()
    filelist = tmp_path / "filelist.csv"
    _write_filelist(
        filelist,
        [
            {
                "day": "2026-07-01",
                "order_level_csv": str(panel),
                "size_bytes": str(panel.stat().st_size),
                "sha256": digest,
            }
        ],
    )

    result = _order_level_filelist(filelist, verify_hashes=True)

    assert result == {"2026-07-01": panel.resolve()}


def test_order_level_filelist_rejects_hash_mismatch(tmp_path: Path) -> None:
    panel = tmp_path / "orders.csv"
    panel.write_text("day,filled\n2026-07-01,0\n", encoding="utf-8")
    filelist = tmp_path / "filelist.csv"
    _write_filelist(
        filelist,
        [
            {
                "day": "2026-07-01",
                "order_level_csv": str(panel),
                "size_bytes": str(panel.stat().st_size),
                "sha256": "0" * 64,
            }
        ],
    )

    with pytest.raises(RuntimeError, match="SHA256 mismatch"):
        _order_level_filelist(filelist, verify_hashes=True)
