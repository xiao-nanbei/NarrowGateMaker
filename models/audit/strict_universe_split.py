#!/usr/bin/env python3
"""Freeze chronological evidence panels and expanding Development folds."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

SCHEMA_VERSION = "strict_native_evidence_split.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame:
        raise ValueError("strict-day manifest must contain a day column")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError("strict-day manifest contains invalid UTC dates")
    days = sorted(set(parsed.dt.strftime("%Y-%m-%d")))
    if not days:
        raise ValueError("strict-day manifest is empty")
    return days


def build_expanding_chronological_folds(
    development_days: list[str],
    *,
    min_train_days: int,
    test_days: int,
    embargo_days: int,
) -> list[dict[str, Any]]:
    """Use only earlier strict days to produce later Development OOF rows."""

    days = sorted(set(development_days))
    if days != development_days:
        raise ValueError("development_days must be unique and chronological")
    if min_train_days <= 0 or test_days <= 0 or embargo_days < 0:
        raise ValueError("invalid expanding-fold sizes")
    folds: list[dict[str, Any]] = []
    test_start = min_train_days + embargo_days
    fold_index = 1
    while test_start < len(days):
        train_end = test_start - embargo_days
        test_end = min(len(days), test_start + test_days)
        train = days[:train_end]
        embargo = days[train_end:test_start]
        test = days[test_start:test_end]
        if len(train) < min_train_days or not test:
            break
        folds.append(
            {
                "fold": fold_index,
                "train_days": train,
                "embargo_days": embargo,
                "test_days": test,
                "train_start": train[0],
                "train_end": train[-1],
                "test_start": test[0],
                "test_end": test[-1],
            }
        )
        fold_index += 1
        # The next fold receives its own trailing train embargo. Skipping
        # another block here would double-count the embargo and discard valid
        # Development OOF days.
        test_start = test_end
    if not folds:
        raise ValueError("strict Development panel is too small for one OOF fold")
    return folds


def build_strict_evidence_split(
    strict_days: list[str],
    *,
    family_id: str,
    validation_days: int = 10,
    holdout_days: int = 10,
    panel_embargo_days: int = 1,
    min_train_days: int = 20,
    fold_test_days: int = 5,
    fold_embargo_days: int = 1,
) -> dict[str, Any]:
    days = sorted(set(strict_days))
    if days != strict_days:
        raise ValueError("strict_days must be unique and chronological")
    reserved = (
        int(validation_days)
        + int(holdout_days)
        + 2 * int(panel_embargo_days)
    )
    if min(
        validation_days,
        holdout_days,
        panel_embargo_days,
        min_train_days,
        fold_test_days,
    ) <= 0:
        raise ValueError("panel and fold sizes must be positive")
    if len(days) <= reserved + min_train_days + fold_embargo_days:
        raise ValueError("strict universe is too small for requested split")

    holdout_start = len(days) - holdout_days
    embargo_2_start = holdout_start - panel_embargo_days
    validation_start = embargo_2_start - validation_days
    embargo_1_start = validation_start - panel_embargo_days
    panels = {
        "development": days[:embargo_1_start],
        "embargo_1": days[embargo_1_start:validation_start],
        "validation": days[validation_start:embargo_2_start],
        "embargo_2": days[embargo_2_start:holdout_start],
        "sealed_holdout": days[holdout_start:],
    }
    folds = build_expanding_chronological_folds(
        panels["development"],
        min_train_days=min_train_days,
        test_days=fold_test_days,
        embargo_days=fold_embargo_days,
    )
    oof_days = [
        day for fold in folds for day in fold["test_days"]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "family_id": str(family_id),
        "split_mode": "maximum_strict_universe_chronological",
        "strict_days_count": int(len(days)),
        "panels": {
            name: {
                "days": values,
                "day_count": int(len(values)),
                "trainable": name == "development",
                "sealed": name == "sealed_holdout",
            }
            for name, values in panels.items()
        },
        "development_folds": folds,
        "development_oof_days": oof_days,
        "development_oof_day_count": int(len(oof_days)),
        "fold_contract": {
            "mode": "expanding_chronological",
            "past_only": True,
            "min_train_days": int(min_train_days),
            "test_days": int(fold_test_days),
            "embargo_days": int(fold_embargo_days),
        },
        "access_contract": {
            "validation": (
                "not trainable; remains unread until the frozen Development "
                "prediction gate passes"
            ),
            "sealed_holdout": (
                "not trainable; remains unread until frozen Validation passes"
            ),
        },
    }


def freeze_strict_evidence_split(
    *,
    strict_days_path: Path,
    output_path: Path,
    family_id: str,
    validation_days: int = 10,
    holdout_days: int = 10,
    panel_embargo_days: int = 1,
    min_train_days: int = 20,
    fold_test_days: int = 5,
    fold_embargo_days: int = 1,
) -> dict[str, Any]:
    source = strict_days_path.expanduser().resolve()
    output = output_path.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"refusing to overwrite evidence split: {output}")
    payload = build_strict_evidence_split(
        _read_days(source),
        family_id=family_id,
        validation_days=validation_days,
        holdout_days=holdout_days,
        panel_embargo_days=panel_embargo_days,
        min_train_days=min_train_days,
        fold_test_days=fold_test_days,
        fold_embargo_days=fold_embargo_days,
    )
    payload["strict_days_path"] = str(source)
    payload["strict_days_sha256"] = _sha256(source)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict-days", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--family-id", required=True)
    parser.add_argument("--validation-days", type=int, default=10)
    parser.add_argument("--holdout-days", type=int, default=10)
    parser.add_argument("--panel-embargo-days", type=int, default=1)
    parser.add_argument("--min-train-days", type=int, default=20)
    parser.add_argument("--fold-test-days", type=int, default=5)
    parser.add_argument("--fold-embargo-days", type=int, default=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = freeze_strict_evidence_split(
        strict_days_path=args.strict_days,
        output_path=args.output,
        family_id=args.family_id,
        validation_days=args.validation_days,
        holdout_days=args.holdout_days,
        panel_embargo_days=args.panel_embargo_days,
        min_train_days=args.min_train_days,
        fold_test_days=args.fold_test_days,
        fold_embargo_days=args.fold_embargo_days,
    )
    print(
        json.dumps(
            {
                "strict_days": payload["strict_days_count"],
                "development_days": payload["panels"]["development"][
                    "day_count"
                ],
                "development_oof_days": payload[
                    "development_oof_day_count"
                ],
                "validation_days": payload["panels"]["validation"][
                    "day_count"
                ],
                "sealed_holdout_days": payload["panels"][
                    "sealed_holdout"
                ]["day_count"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
