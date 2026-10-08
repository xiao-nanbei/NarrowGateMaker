#!/usr/bin/env python3
"""Build order-level right-censor segments from normalized native L2."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "native_l2_gap_segments.v1"
DAY_MS = 86_400_000


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_day_segments(
    day: str,
    timestamps_ms: Sequence[int] | np.ndarray,
    *,
    maximum_gap_ms: int = 5_000,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    timestamps = np.asarray(timestamps_ms, dtype=np.int64)
    if timestamps.size == 0:
        raise ValueError(f"{day} has no L2 timestamps")
    if np.any(timestamps[1:] < timestamps[:-1]):
        raise ValueError(f"{day} L2 timestamps are not sorted")
    day_start = int(pd.Timestamp(day, tz="UTC").timestamp() * 1000)
    day_end = day_start + DAY_MS
    timestamps = timestamps[(timestamps >= day_start) & (timestamps < day_end)]
    if timestamps.size == 0:
        raise ValueError(f"{day} has no in-day L2 timestamps")
    differences = np.diff(timestamps)
    positions = np.flatnonzero(differences > int(maximum_gap_ms))
    gaps: list[dict[str, Any]] = []
    segments: list[dict[str, Any]] = []
    segment_start = day_start
    segment_id = 1
    for position in positions:
        last_visible = int(timestamps[position])
        next_visible = int(timestamps[position + 1])
        censor_ts = min(day_end, last_visible + int(maximum_gap_ms))
        if censor_ts > segment_start:
            segments.append(
                {
                    "day": day,
                    "segment_id": segment_id,
                    "start_ts_ms": segment_start,
                    "end_ts_ms_exclusive": censor_ts,
                    "right_censor_reason": "native_l2_gap",
                }
            )
            segment_id += 1
        gaps.append(
            {
                "day": day,
                "left_event_ts_ms": last_visible,
                "right_event_ts_ms": next_visible,
                "gap_ms": next_visible - last_visible,
                "censor_ts_ms": censor_ts,
                "resume_ts_ms": next_visible,
            }
        )
        segment_start = next_visible
    if segment_start < day_end:
        segments.append(
            {
                "day": day,
                "segment_id": segment_id,
                "start_ts_ms": segment_start,
                "end_ts_ms_exclusive": day_end,
                "right_censor_reason": "utc_day_end",
            }
        )
    return pd.DataFrame(segments), pd.DataFrame(gaps)


def assign_segment(
    timestamps_ms: Sequence[int] | np.ndarray,
    segments: pd.DataFrame,
) -> np.ndarray:
    values = np.asarray(timestamps_ms, dtype=np.int64)
    result = np.zeros(len(values), dtype=np.int64)
    for row in segments.itertuples(index=False):
        mask = (
            (values >= int(row.start_ts_ms))
            & (values < int(row.end_ts_ms_exclusive))
        )
        result[mask] = int(row.segment_id)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--normalized-root", type=Path, required=True)
    parser.add_argument("--strict-days", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--maximum-gap-ms", type=int, default=5_000)
    args = parser.parse_args(argv)
    normalized_root = args.normalized_root.expanduser().resolve()
    strict_days = args.strict_days.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    days = pd.read_csv(strict_days)["day"].astype(str).tolist()
    segment_frames: list[pd.DataFrame] = []
    gap_frames: list[pd.DataFrame] = []
    source_files: list[dict[str, Any]] = []
    for day in days:
        path = normalized_root / "l2" / f"BTCUSDC-l2-{day}.parquet"
        timestamps = pd.read_parquet(path, columns=["timestamp"])[
            "timestamp"
        ].to_numpy(np.int64)
        segments, gaps = build_day_segments(
            day, timestamps, maximum_gap_ms=int(args.maximum_gap_ms)
        )
        segment_frames.append(segments)
        if not gaps.empty:
            gap_frames.append(gaps)
        source_files.append(
            {
                "day": day,
                "path": str(path),
                "sha256": _sha256(path),
                "rows": int(len(timestamps)),
            }
        )
    all_segments = pd.concat(segment_frames, ignore_index=True)
    all_gaps = (
        pd.concat(gap_frames, ignore_index=True)
        if gap_frames
        else pd.DataFrame(
            columns=(
                "day",
                "left_event_ts_ms",
                "right_event_ts_ms",
                "gap_ms",
                "censor_ts_ms",
                "resume_ts_ms",
            )
        )
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    segments_path = output_dir / "native_l2_segments.csv"
    gaps_path = output_dir / "native_l2_gaps.csv"
    all_segments.to_csv(segments_path, index=False)
    all_gaps.to_csv(gaps_path, index=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "maximum_gap_ms": int(args.maximum_gap_ms),
        "strict_days_path": str(strict_days),
        "strict_days_sha256": _sha256(strict_days),
        "day_count": int(len(days)),
        "continuous_day_count": int((all_segments.groupby("day").size() == 1).sum()),
        "segmented_day_count": int((all_segments.groupby("day").size() > 1).sum()),
        "segment_count": int(len(all_segments)),
        "gap_count": int(len(all_gaps)),
        "segments_path": str(segments_path),
        "segments_sha256": _sha256(segments_path),
        "gaps_path": str(gaps_path),
        "gaps_sha256": _sha256(gaps_path),
        "sources": source_files,
        "research_contract": "no order risk interval may cross segment boundaries",
    }
    temporary = output_dir / "native_l2_gap_manifest.json.tmp"
    temporary.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output_dir / "native_l2_gap_manifest.json")
    print(json.dumps({key: manifest[key] for key in ("day_count", "continuous_day_count", "segmented_day_count", "gap_count")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
