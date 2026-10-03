from __future__ import annotations

from pathlib import Path

import pandas as pd

from models.audit.strict_universe_split import (
    build_expanding_chronological_folds,
    build_strict_evidence_split,
    freeze_strict_evidence_split,
)


def _days(count: int) -> list[str]:
    return (
        pd.date_range("2026-01-01", periods=count, freq="D")
        .strftime("%Y-%m-%d")
        .tolist()
    )


def test_expanding_folds_only_train_on_dates_before_test() -> None:
    folds = build_expanding_chronological_folds(
        _days(35),
        min_train_days=20,
        test_days=5,
        embargo_days=1,
    )

    assert len(folds) == 3
    assert [
        day for fold in folds for day in fold["test_days"]
    ] == _days(35)[21:]
    for fold in folds:
        assert max(fold["train_days"]) < min(fold["test_days"])
        assert max(fold["embargo_days"]) < min(fold["test_days"])


def test_strict_split_keeps_validation_and_holdout_untrainable() -> None:
    payload = build_strict_evidence_split(
        _days(80),
        family_id="dynamic_fill_hazard_m0_v3",
        validation_days=10,
        holdout_days=10,
        panel_embargo_days=1,
        min_train_days=20,
        fold_test_days=5,
        fold_embargo_days=1,
    )

    assert payload["panels"]["development"]["day_count"] == 58
    assert payload["panels"]["validation"]["trainable"] is False
    assert payload["panels"]["sealed_holdout"]["trainable"] is False
    assert payload["panels"]["sealed_holdout"]["sealed"] is True
    assert set(payload["development_oof_days"]).isdisjoint(
        payload["panels"]["validation"]["days"]
    )


def test_freeze_strict_split_binds_source_manifest(tmp_path: Path) -> None:
    source = tmp_path / "strict.csv"
    pd.DataFrame({"day": _days(50)}).to_csv(source, index=False)
    output = tmp_path / "split.json"

    payload = freeze_strict_evidence_split(
        strict_days_path=source,
        output_path=output,
        family_id="family",
        validation_days=5,
        holdout_days=5,
        panel_embargo_days=1,
        min_train_days=15,
        fold_test_days=5,
        fold_embargo_days=1,
    )

    assert output.is_file()
    assert payload["strict_days_path"] == str(source.resolve())
    assert payload["strict_days_sha256"]
