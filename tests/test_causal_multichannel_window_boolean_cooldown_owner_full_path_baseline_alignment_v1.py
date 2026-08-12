from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_full_path_baseline_alignment_v1 as audit,
)


def _row(day: str, terminal: float = 1.25) -> dict[str, object]:
    row: dict[str, object] = {"day": day, "arm": audit.CONTROL_ARM}
    for metric in audit.EXACT_METRICS:
        row[metric] = 2
    for metric in audit.FLOAT_METRICS:
        row[metric] = terminal if metric == "terminal_mtm_pnl_usdc" else 0.25
    return row


def _write_owner(root: Path, row: dict[str, object]) -> None:
    day_root = root / "days" / str(row["day"])
    day_root.mkdir(parents=True)
    (day_root / "summary.json").write_text(
        json.dumps({"arms": [row]}), encoding="utf-8"
    )
    (day_root / "_SUCCESS").write_text("ok\n", encoding="utf-8")


def test_partial_alignment_passes_without_economic_authority(tmp_path: Path) -> None:
    owner_root = tmp_path / "owner"
    reference = tmp_path / "reference.parquet"
    rows = [_row("2026-01-01"), _row("2026-01-02")]
    pd.DataFrame(rows).to_parquet(reference, index=False)
    _write_owner(owner_root, rows[0])

    report = audit.validate_alignment(
        owner_root=owner_root, reference_path=reference, require_complete=False
    )

    assert report["status"] == "passed"
    assert report["aligned_day_count"] == 1
    assert report["economic_interpretation_allowed"] is False


def test_complete_alignment_fails_when_a_day_is_missing(tmp_path: Path) -> None:
    owner_root = tmp_path / "owner"
    reference = tmp_path / "reference.parquet"
    rows = [_row("2026-01-01"), _row("2026-01-02")]
    pd.DataFrame(rows).to_parquet(reference, index=False)
    _write_owner(owner_root, rows[0])

    with pytest.raises(audit.BaselineAlignmentError, match="missing"):
        audit.validate_alignment(
            owner_root=owner_root, reference_path=reference, require_complete=True
        )


def test_numeric_mismatch_fails_alignment(tmp_path: Path) -> None:
    owner_root = tmp_path / "owner"
    reference = tmp_path / "reference.parquet"
    pd.DataFrame([_row("2026-01-01")]).to_parquet(reference, index=False)
    _write_owner(owner_root, _row("2026-01-01", terminal=1.2501))

    report = audit.validate_alignment(
        owner_root=owner_root,
        reference_path=reference,
        tolerance=1e-9,
        require_complete=True,
    )

    assert report["status"] == "failed"
    assert report["economic_interpretation_allowed"] is False
    assert report["failures"][0]["metric"] == "terminal_mtm_pnl_usdc"
