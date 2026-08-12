#!/usr/bin/env python3
"""Freeze one strict per-day BBO/L2 view from versioned normalized roots."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from data.download_cryptohft_orderbook import _normalized_day_summary

SCHEMA_VERSION = "native_normalized_book_manifest.v1"
AUDIT_SCHEMA_VERSION = "native_normalized_book_audit.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Any) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _read_days(path: Path) -> list[str]:
    frame = pd.read_csv(path)
    if "day" not in frame:
        raise ValueError("eligible-day manifest must contain a day column")
    parsed = pd.to_datetime(frame["day"], utc=True, errors="coerce")
    if parsed.isna().any():
        raise ValueError("eligible-day manifest contains invalid UTC dates")
    days = sorted(set(parsed.dt.strftime("%Y-%m-%d")))
    if not days:
        raise ValueError("eligible-day manifest is empty")
    return days


def normalized_summary_is_strict(
    summary: dict[str, object],
    *,
    min_coverage: float,
    min_valid_spread_ratio: float,
    max_p99_gap_s: float,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not bool(summary.get("bbo_readable", False)):
        reasons.append("bbo_unreadable")
    if not bool(summary.get("l2_readable", False)):
        reasons.append("l2_unreadable")
    if bool(summary.get("tmp_exists", False)):
        reasons.append("partial_file_present")
    if not bool(summary.get("l2_schema_complete", False)):
        reasons.append("missing_top20_schema")
    if float(summary.get("bbo_coverage") or 0.0) < float(min_coverage):
        reasons.append("bbo_coverage")
    if float(summary.get("l2_coverage") or 0.0) < float(min_coverage):
        reasons.append("l2_coverage")
    if float(summary.get("l2_valid_spread_ratio") or 0.0) < float(
        min_valid_spread_ratio
    ):
        reasons.append("invalid_spread")
    if float(summary.get("bbo_p99_gap_s") or float("inf")) > float(
        max_p99_gap_s
    ):
        reasons.append("bbo_cadence")
    if float(summary.get("l2_p99_gap_s") or float("inf")) > float(
        max_p99_gap_s
    ):
        reasons.append("l2_cadence")
    return not reasons, reasons


def select_normalized_source(
    summaries: list[dict[str, object]],
    *,
    min_coverage: float,
    min_valid_spread_ratio: float,
    max_p99_gap_s: float,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    attempts: list[dict[str, object]] = []
    for summary in summaries:
        eligible, reasons = normalized_summary_is_strict(
            summary,
            min_coverage=min_coverage,
            min_valid_spread_ratio=min_valid_spread_ratio,
            max_p99_gap_s=max_p99_gap_s,
        )
        attempts.append(
            {
                "root": str(summary.get("root", "")),
                "eligible": bool(eligible),
                "reasons": reasons,
            }
        )
        if eligible:
            return summary, attempts
    raise ValueError(f"no strict normalized source: {attempts}")


def _end_age_reasons(
    summary: dict[str, object],
    *,
    day_start: datetime,
    max_end_age_s: float | None,
) -> tuple[list[str], float | None, float | None]:
    if max_end_age_s is None:
        return [], None, None
    day_end_ms = int(
        (pd.Timestamp(day_start) + pd.Timedelta(days=1)).value // 1_000_000
    )
    ages: dict[str, float | None] = {}
    reasons: list[str] = []
    for prefix in ("bbo", "l2"):
        raw = summary.get(f"{prefix}_last_ts")
        age = (
            float((day_end_ms - int(raw)) / 1000.0)
            if raw is not None
            else None
        )
        ages[prefix] = age
        if age is None or age < 0.0 or age > float(max_end_age_s):
            reasons.append(f"{prefix}_end_stale")
    return reasons, ages["bbo"], ages["l2"]


def audit_normalized_book_universe(
    *,
    candidate_days_path: Path,
    source_roots: list[Path],
    audit_output_path: Path,
    strict_days_output_path: Path,
    symbol: str = "BTCUSDC",
    levels: int = 20,
    freshness_s: float = 5.0,
    min_coverage: float = 0.99,
    min_valid_spread_ratio: float = 0.999,
    max_p99_gap_s: float = 0.5,
    max_end_age_s: float | None = None,
    summary_loader: Callable[..., dict[str, object]] = (
        _normalized_day_summary
    ),
) -> dict[str, object]:
    """Audit every candidate without weakening gates after a failed day."""

    source = candidate_days_path.expanduser().resolve()
    roots = [path.expanduser().resolve() for path in source_roots]
    audit_output = audit_output_path.expanduser().resolve()
    strict_output = strict_days_output_path.expanduser().resolve()
    audit_manifest = audit_output.with_suffix(".manifest.json")
    if not roots:
        raise ValueError("at least one normalized source root is required")
    for output in (audit_output, strict_output, audit_manifest):
        if output.exists():
            raise FileExistsError(f"refusing to overwrite audit output: {output}")

    rows: list[dict[str, object]] = []
    strict_days: list[str] = []
    for day in _read_days(source):
        day_start = pd.Timestamp(day, tz="UTC").to_pydatetime()
        summaries = [
            summary_loader(
                root,
                str(symbol).upper(),
                day_start,
                int(float(freshness_s) * 1000),
                levels=int(levels),
            )
            for root in roots
        ]
        selected: dict[str, object] | None = None
        attempts: list[dict[str, object]] = []
        for summary in summaries:
            eligible, reasons = normalized_summary_is_strict(
                summary,
                min_coverage=min_coverage,
                min_valid_spread_ratio=min_valid_spread_ratio,
                max_p99_gap_s=max_p99_gap_s,
            )
            end_reasons, bbo_end_age_s, l2_end_age_s = _end_age_reasons(
                summary,
                day_start=day_start,
                max_end_age_s=max_end_age_s,
            )
            reasons.extend(end_reasons)
            eligible = not reasons
            attempts.append(
                {
                    "root": str(summary.get("root", "")),
                    "eligible": bool(eligible),
                    "reasons": reasons,
                    "bbo_coverage": float(
                        summary.get("bbo_coverage") or 0.0
                    ),
                    "l2_coverage": float(
                        summary.get("l2_coverage") or 0.0
                    ),
                    "bbo_p99_gap_s": summary.get("bbo_p99_gap_s"),
                    "l2_p99_gap_s": summary.get("l2_p99_gap_s"),
                    "bbo_end_age_s": bbo_end_age_s,
                    "l2_end_age_s": l2_end_age_s,
                }
            )
            if eligible and selected is None:
                selected = summary

        if selected is not None:
            strict_days.append(day)
        rows.append(
            {
                "day": day,
                "eligible": selected is not None,
                "selected_root": (
                    str(selected.get("root", ""))
                    if selected is not None
                    else ""
                ),
                "bbo_coverage": (
                    float(selected.get("bbo_coverage") or 0.0)
                    if selected is not None
                    else 0.0
                ),
                "l2_coverage": (
                    float(selected.get("l2_coverage") or 0.0)
                    if selected is not None
                    else 0.0
                ),
                "bbo_p99_gap_s": (
                    selected.get("bbo_p99_gap_s")
                    if selected is not None
                    else None
                ),
                "l2_p99_gap_s": (
                    selected.get("l2_p99_gap_s")
                    if selected is not None
                    else None
                ),
                "source_attempts": json.dumps(
                    attempts,
                    sort_keys=True,
                ),
            }
        )

    audit_output.parent.mkdir(parents=True, exist_ok=True)
    strict_output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(audit_output, index=False)
    pd.DataFrame({"day": strict_days}).to_csv(strict_output, index=False)
    payload: dict[str, object] = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": str(symbol).upper(),
        "candidate_days_path": str(source),
        "candidate_days_sha256": _sha256(source),
        "candidate_days_count": len(rows),
        "strict_days_count": len(strict_days),
        "excluded_days_count": len(rows) - len(strict_days),
        "source_roots": [str(root) for root in roots],
        "levels": int(levels),
        "freshness_s": float(freshness_s),
        "min_coverage": float(min_coverage),
        "min_valid_spread_ratio": float(min_valid_spread_ratio),
        "max_p99_gap_s": float(max_p99_gap_s),
        "max_end_age_s": (
            float(max_end_age_s) if max_end_age_s is not None else None
        ),
        "audit_output_path": str(audit_output),
        "audit_output_sha256": _sha256(audit_output),
        "strict_days_output_path": str(strict_output),
        "strict_days_output_sha256": _sha256(strict_output),
        "audit_manifest_path": str(audit_manifest),
    }
    payload["identity_sha256"] = _canonical_sha256(payload)
    audit_manifest.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def freeze_normalized_book_manifest(
    *,
    eligible_days_path: Path,
    warmup_days_path: Path | None = None,
    source_roots: list[Path],
    output_root: Path,
    symbol: str = "BTCUSDC",
    levels: int = 20,
    freshness_s: float = 5.0,
    min_coverage: float = 0.99,
    min_valid_spread_ratio: float = 0.999,
    max_p99_gap_s: float = 0.5,
    warmup_min_coverage: float = 0.0,
    warmup_max_end_age_s: float = 0.5,
    summary_loader: Callable[..., dict[str, object]] = (
        _normalized_day_summary
    ),
) -> dict[str, object]:
    source = eligible_days_path.expanduser().resolve()
    warmup_source = (
        warmup_days_path.expanduser().resolve()
        if warmup_days_path is not None
        else None
    )
    roots = [path.expanduser().resolve() for path in source_roots]
    output = output_root.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"refusing to overwrite normalized root: {output}")
    if not roots:
        raise ValueError("at least one normalized source root is required")
    target_days = _read_days(source)
    target_set = set(target_days)
    warmup_days = (
        [
            day
            for day in _read_days(warmup_source)
            if day not in target_set
        ]
        if warmup_source is not None
        else []
    )
    day_roles = [
        (day, "target") for day in target_days
    ] + [
        (day, "warmup_only") for day in warmup_days
    ]
    day_roles.sort()
    bbo_output = output / "bbo"
    l2_output = output / "l2"
    bbo_output.mkdir(parents=True)
    l2_output.mkdir(parents=True)

    rows: list[dict[str, object]] = []
    try:
        for day, role in day_roles:
            day_start = pd.Timestamp(day, tz="UTC").to_pydatetime()
            summaries = [
                summary_loader(
                    root,
                    str(symbol).upper(),
                    day_start,
                    int(float(freshness_s) * 1000),
                    levels=int(levels),
                )
                for root in roots
            ]
            role_min_coverage = (
                min_coverage
                if role == "target"
                else warmup_min_coverage
            )
            role_max_end_age_s = (
                None
                if role == "target"
                else warmup_max_end_age_s
            )
            selected = None
            attempts: list[dict[str, object]] = []
            for summary in summaries:
                eligible, reasons = normalized_summary_is_strict(
                    summary,
                    min_coverage=role_min_coverage,
                    min_valid_spread_ratio=min_valid_spread_ratio,
                    max_p99_gap_s=max_p99_gap_s,
                )
                end_reasons, bbo_end_age_s, l2_end_age_s = (
                    _end_age_reasons(
                        summary,
                        day_start=day_start,
                        max_end_age_s=role_max_end_age_s,
                    )
                )
                reasons.extend(end_reasons)
                eligible = not reasons
                attempts.append(
                    {
                        "root": str(summary.get("root", "")),
                        "eligible": bool(eligible),
                        "reasons": reasons,
                        "bbo_end_age_s": bbo_end_age_s,
                        "l2_end_age_s": l2_end_age_s,
                    }
                )
                if eligible and selected is None:
                    selected = summary
            if selected is None:
                raise ValueError(
                    f"no normalized source for {day} ({role}): {attempts}"
                )
            source_bbo = Path(str(selected["bbo_path"])).resolve()
            source_l2 = Path(str(selected["l2_path"])).resolve()
            target_bbo = bbo_output / source_bbo.name
            target_l2 = l2_output / source_l2.name
            os.symlink(source_bbo, target_bbo)
            os.symlink(source_l2, target_l2)
            rows.append(
                {
                    "day": day,
                    "role": role,
                    "source_root": str(selected["root"]),
                    "source_bbo": str(source_bbo),
                    "source_l2": str(source_l2),
                    "bbo_sha256": _sha256(source_bbo),
                    "l2_sha256": _sha256(source_l2),
                    "bbo_rows": int(selected["bbo_rows"]),
                    "l2_rows": int(selected["l2_rows"]),
                    "bbo_coverage": float(selected["bbo_coverage"]),
                    "l2_coverage": float(selected["l2_coverage"]),
                    "bbo_p99_gap_s": float(
                        selected["bbo_p99_gap_s"] or 0.0
                    ),
                    "l2_p99_gap_s": float(
                        selected["l2_p99_gap_s"] or 0.0
                    ),
                    "l2_valid_spread_ratio": float(
                        selected["l2_valid_spread_ratio"]
                    ),
                    "source_attempts": json.dumps(
                        attempts,
                        sort_keys=True,
                    ),
                }
            )
    except Exception:
        for path in sorted(output.rglob("*"), reverse=True):
            if path.is_symlink() or path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        output.rmdir()
        raise

    daily = pd.DataFrame(rows)
    daily_path = output / "daily_sources.csv"
    daily.to_csv(daily_path, index=False)
    payload: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbol": str(symbol).upper(),
        "levels": int(levels),
        "freshness_s": float(freshness_s),
        "min_coverage": float(min_coverage),
        "min_valid_spread_ratio": float(min_valid_spread_ratio),
        "max_p99_gap_s": float(max_p99_gap_s),
        "eligible_days_path": str(source),
        "eligible_days_sha256": _sha256(source),
        "eligible_days_count": len(target_days),
        "warmup_days_path": (
            str(warmup_source) if warmup_source is not None else ""
        ),
        "warmup_days_sha256": (
            _sha256(warmup_source)
            if warmup_source is not None
            else ""
        ),
        "warmup_days_count": len(warmup_days),
        "linked_days_count": len(day_roles),
        "warmup_min_coverage": float(warmup_min_coverage),
        "warmup_max_end_age_s": float(warmup_max_end_age_s),
        "source_roots": [str(root) for root in roots],
        "daily_sources_path": str(daily_path),
        "daily_sources_sha256": _sha256(daily_path),
        "output_root": str(output),
        "storage": "absolute symlink view over immutable hashed daily files",
    }
    payload["identity_sha256"] = _canonical_sha256(payload)
    manifest_path = output / "manifest.json"
    manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--eligible-days", type=Path, required=True)
    parser.add_argument(
        "--warmup-days",
        type=Path,
        help="Optional context-only days linked but never counted as targets",
    )
    parser.add_argument(
        "--source-root",
        action="append",
        type=Path,
        required=True,
        help="Priority-ordered normalized root; may be repeated",
    )
    parser.add_argument("--output-root", type=Path)
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Audit every candidate and write the surviving strict-day set",
    )
    parser.add_argument("--audit-output", type=Path)
    parser.add_argument("--strict-days-output", type=Path)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--levels", type=int, default=20)
    parser.add_argument("--freshness-s", type=float, default=5.0)
    parser.add_argument("--min-coverage", type=float, default=0.99)
    parser.add_argument("--min-valid-spread-ratio", type=float, default=0.999)
    parser.add_argument("--max-p99-gap-s", type=float, default=0.5)
    parser.add_argument(
        "--max-end-age-s",
        type=float,
        default=None,
        help="Audit-only maximum age of the final state before UTC midnight",
    )
    parser.add_argument("--warmup-min-coverage", type=float, default=0.0)
    parser.add_argument("--warmup-max-end-age-s", type=float, default=0.5)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.audit_only:
        if args.audit_output is None or args.strict_days_output is None:
            raise SystemExit(
                "--audit-only requires --audit-output and "
                "--strict-days-output"
            )
        payload = audit_normalized_book_universe(
            candidate_days_path=args.eligible_days,
            source_roots=args.source_root,
            audit_output_path=args.audit_output,
            strict_days_output_path=args.strict_days_output,
            symbol=args.symbol,
            levels=args.levels,
            freshness_s=args.freshness_s,
            min_coverage=args.min_coverage,
            min_valid_spread_ratio=args.min_valid_spread_ratio,
            max_p99_gap_s=args.max_p99_gap_s,
            max_end_age_s=args.max_end_age_s,
        )
        print(
            json.dumps(
                {
                    "candidate_days": payload["candidate_days_count"],
                    "strict_days": payload["strict_days_count"],
                    "excluded_days": payload["excluded_days_count"],
                    "identity_sha256": payload["identity_sha256"],
                },
                sort_keys=True,
            )
        )
        return
    if args.output_root is None:
        raise SystemExit("--output-root is required unless --audit-only is used")
    payload = freeze_normalized_book_manifest(
        eligible_days_path=args.eligible_days,
        warmup_days_path=args.warmup_days,
        source_roots=args.source_root,
        output_root=args.output_root,
        symbol=args.symbol,
        levels=args.levels,
        freshness_s=args.freshness_s,
        min_coverage=args.min_coverage,
        min_valid_spread_ratio=args.min_valid_spread_ratio,
        max_p99_gap_s=args.max_p99_gap_s,
        warmup_min_coverage=args.warmup_min_coverage,
        warmup_max_end_age_s=args.warmup_max_end_age_s,
    )
    print(
        json.dumps(
            {
                "days": payload["eligible_days_count"],
                "output_root": payload["output_root"],
                "identity_sha256": payload["identity_sha256"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
