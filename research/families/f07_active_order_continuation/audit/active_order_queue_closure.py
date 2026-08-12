#!/usr/bin/env python3
"""Audit fixed-point closure for sparse active-order queue trajectories."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

SCHEMA_VERSION = "active_order_queue_closure.v1"
IDENTITY_COLUMNS = ("side", "price_tick", "activate_ts_ms")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalize_side(series: pd.Series) -> pd.Series:
    side = series.astype(str).str.strip().str.upper()
    return side.replace({"BID": "BUY", "ASK": "SELL"})


def load_trajectory(
    label: str,
    manifest_path: Path,
    daily_path: Path,
) -> dict[str, Any]:
    manifest_path = manifest_path.expanduser().resolve()
    daily_path = daily_path.expanduser().resolve()
    manifest = pd.read_parquet(manifest_path)
    required = {"trajectory_id", *IDENTITY_COLUMNS}
    missing = sorted(required - set(manifest.columns))
    if missing:
        raise ValueError(f"{label} manifest missing columns: {missing}")
    if manifest.empty:
        raise ValueError(f"{label} manifest is empty")

    identity = pd.DataFrame(
        {
            "side": _normalize_side(manifest["side"]),
            "price_tick": pd.to_numeric(
                manifest["price_tick"],
                errors="raise",
            ).astype("int64"),
            "activate_ts_ms": pd.to_numeric(
                manifest["activate_ts_ms"],
                errors="raise",
            ).astype("int64"),
        }
    )
    if not identity["side"].isin(("BUY", "SELL")).all():
        raise ValueError(f"{label} manifest has unsupported sides")
    duplicate = identity.duplicated(keep=False)
    if duplicate.any():
        raise ValueError(
            f"{label} manifest has duplicate market identities: "
            f"{identity.loc[duplicate].head(3).to_dict('records')}"
        )

    daily = pd.read_csv(daily_path)
    if len(daily) != 1:
        raise ValueError(f"{label} daily summary must contain exactly one row")
    daily_row = daily.iloc[0].to_dict()
    mechanics = {}
    for column in (
        "rows",
        "quote_rows",
        "fills",
        "placed",
        "campaigns",
        "active_order_queue_mode",
        "active_order_queue_scope",
        "active_order_queue_lookup_count",
        "active_order_queue_exact_count",
        "active_order_queue_known_zero_count",
        "active_order_queue_missing_count",
        "active_order_queue_unusable_count",
    ):
        if column in daily_row and pd.notna(daily_row[column]):
            value = daily_row[column]
            mechanics[column] = (
                int(value)
                if column
                not in {
                    "active_order_queue_mode",
                    "active_order_queue_scope",
                }
                else str(value)
            )

    identities = {
        (str(row.side), int(row.price_tick), int(row.activate_ts_ms))
        for row in identity.itertuples(index=False)
    }
    return {
        "label": str(label),
        "manifest_path": str(manifest_path),
        "manifest_sha256": _sha256(manifest_path),
        "daily_path": str(daily_path),
        "daily_sha256": _sha256(daily_path),
        "watch_count": int(len(manifest)),
        "trajectory_ids": sorted(
            manifest["trajectory_id"].astype(str).unique().tolist()
        ),
        "mechanics": mechanics,
        "_identities": identities,
    }


def build_closure_report(
    trajectories: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    if len(trajectories) < 2:
        raise ValueError("closure audit requires at least two trajectories")

    comparisons = []
    for index in range(len(trajectories) - 1):
        previous = trajectories[index]
        current = trajectories[index + 1]
        previous_ids = previous["_identities"]
        current_ids = current["_identities"]
        overlap = previous_ids & current_ids
        comparisons.append(
            {
                "previous": previous["label"],
                "current": current["label"],
                "overlap_count": int(len(overlap)),
                "previous_only_count": int(len(previous_ids - current_ids)),
                "current_only_count": int(len(current_ids - previous_ids)),
                "previous_retention": float(
                    len(overlap) / len(previous_ids)
                ),
                "current_retention": float(
                    len(overlap) / len(current_ids)
                ),
            }
        )

    latest = trajectories[-1]
    latest_mechanics = latest["mechanics"]
    latest_comparison = comparisons[-1]
    closed = bool(
        latest_comparison["previous_only_count"] == 0
        and latest_comparison["current_only_count"] == 0
        and int(latest_mechanics.get("active_order_queue_missing_count", 0))
        == 0
        and int(latest_mechanics.get("active_order_queue_unusable_count", 0))
        == 0
    )
    public_trajectories = []
    for trajectory in trajectories:
        public_trajectories.append(
            {
                key: value
                for key, value in trajectory.items()
                if not key.startswith("_")
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "identity_contract": list(IDENTITY_COLUMNS),
        "order_id_is_identity": False,
        "trajectories": public_trajectories,
        "adjacent_comparisons": comparisons,
        "closed": closed,
        "promotion_status": (
            "data_layer_closed" if closed else "diagnostic_only"
        ),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--generation",
        nargs=3,
        action="append",
        metavar=("LABEL", "MANIFEST", "DAILY"),
        required=True,
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    trajectories = [
        load_trajectory(label, Path(manifest), Path(daily))
        for label, manifest, daily in args.generation
    ]
    report = build_closure_report(trajectories)
    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
