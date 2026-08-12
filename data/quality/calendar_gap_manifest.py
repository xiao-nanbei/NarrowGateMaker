#!/usr/bin/env python3
"""Build an outcome-blind calendar and gap identity for continuous replay."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from data_paths import relocate_marketdata_path

SCHEMA_VERSION = "calendar_continuity_manifest.v1"
DAY_MS = 86_400_000
ALLOWED_GRADES = frozenset({"A", "B", "C", "D", "F"})
ACTIVE_CAPABLE_GRADES = frozenset({"A", "B", "C"})


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_manifest_sha256", None)
    raw = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _bool(value: Any) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _calendar_days(start_day: str, end_day: str) -> list[str]:
    start = date.fromisoformat(start_day)
    end = date.fromisoformat(end_day)
    if end < start:
        raise ValueError("calendar end precedes calendar start")
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def _nested_value(payload: Mapping[str, Any], dotted_field: str) -> Any:
    value: Any = payload
    for token in dotted_field.split("."):
        if not isinstance(value, Mapping) or token not in value:
            raise ValueError(f"anchor field is absent: {dotted_field}")
        value = value[token]
    return value


def load_anchor_identity(path: Path, *, day_field: str) -> dict[str, Any]:
    path = path.expanduser().resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_days = _nested_value(payload, day_field)
    if not isinstance(raw_days, list):
        raise ValueError("anchor day field must be a JSON list")
    days = [str(day) for day in raw_days]
    if not days or len(days) != len(set(days)) or days != sorted(days):
        raise ValueError("anchor target days must be non-empty, unique, and ordered")
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "day_field": day_field,
        "target_days": days,
        "target_day_count": len(days),
    }


@dataclass(frozen=True)
class DaySource:
    day: str
    quality_grade: str
    quality_reason: str
    formal_eligible: bool
    sequence_valid: bool
    coverage_99_valid: bool
    l2_path: str
    expected_sha256: str
    source_quality_path: str
    source_quality_sha256: str

    @property
    def l2_identity_present(self) -> bool:
        return bool(self.l2_path and self.expected_sha256)

    @property
    def strategy_tape_usable(self) -> bool:
        return bool(
            self.quality_grade in ACTIVE_CAPABLE_GRADES
            and self.sequence_valid
            and self.l2_identity_present
        )


def _derived_grade(row: Any | None) -> tuple[str, str]:
    if row is None:
        return "F", "normalized_l2_not_built"
    if _bool(getattr(row, "formal_eligible", False)):
        return "A", "postfit_formal_eligible"
    if _bool(getattr(row, "sequence_valid", False)) and _bool(
        getattr(row, "coverage_99_valid", False)
    ):
        return "B", "postfit_gap_censored"
    if _bool(getattr(row, "sequence_valid", False)):
        return "C", "postfit_sequence_valid_below_coverage"
    return "D", _clean_text(
        getattr(row, "formal_exclusion_reason", "postfit_sequence_invalid")
    ) or "postfit_sequence_invalid"


def load_day_sources(
    *,
    start_day: str,
    end_day: str,
    grade_ledger_path: Path,
    source_quality_paths: Sequence[Path],
) -> list[DaySource]:
    calendar = _calendar_days(start_day, end_day)
    grade_ledger_path = grade_ledger_path.expanduser().resolve()
    grades = pd.read_csv(grade_ledger_path, dtype={"day": str})
    grade_rows = {
        str(row.day): row
        for row in grades.itertuples(index=False)
        if start_day <= str(row.day) <= end_day
    }

    source_rows: dict[str, tuple[Any, Path, str]] = {}
    for raw_path in source_quality_paths:
        path = raw_path.expanduser().resolve()
        identity = sha256_file(path)
        frame = pd.read_csv(path, dtype={"day": str})
        for row in frame.itertuples(index=False):
            day = str(row.day)
            if start_day <= day <= end_day:
                source_rows[day] = (row, path, identity)

    result: list[DaySource] = []
    for day in calendar:
        source = source_rows.get(day)
        row = source[0] if source else None
        source_path = source[1] if source else None
        source_identity = source[2] if source else ""
        grade_row = grade_rows.get(day)
        if grade_row is not None:
            grade = _clean_text(getattr(grade_row, "quality_grade", "")).upper()
            reason = _clean_text(getattr(grade_row, "quality_reasons", ""))
        else:
            grade, reason = _derived_grade(row)
        if grade not in ALLOWED_GRADES:
            raise ValueError(f"calendar replay does not recognize Grade {grade}: {day}")

        l2_raw = _clean_text(getattr(row, "l2_source_path", "")) if row else ""
        l2_path = str(relocate_marketdata_path(l2_raw)) if l2_raw else ""
        result.append(
            DaySource(
                day=day,
                quality_grade=grade,
                quality_reason=reason,
                formal_eligible=_bool(getattr(row, "formal_eligible", False)) if row else False,
                sequence_valid=_bool(getattr(row, "sequence_valid", False)) if row else False,
                coverage_99_valid=_bool(getattr(row, "coverage_99_valid", False)) if row else False,
                l2_path=l2_path,
                expected_sha256=(
                    _clean_text(getattr(row, "l2_sha256", "")) if row else ""
                ),
                source_quality_path=str(source_path) if source_path else "",
                source_quality_sha256=source_identity,
            )
        )
    return result


def _load_timestamps(path: Path) -> np.ndarray:
    import pyarrow.parquet as pq

    names = pq.read_schema(path).names
    timestamp_column = next(
        (name for name in ("timestamp", "timestamp_ms", "transact_time") if name in names),
        None,
    )
    if timestamp_column is None:
        raise ValueError(f"normalized L2 file has no timestamp column: {path}")
    values = pd.read_parquet(path, columns=[timestamp_column])[timestamp_column].to_numpy(
        dtype=np.int64,
        copy=False,
    )
    if values.size == 0:
        raise ValueError(f"normalized L2 file is empty: {path}")
    if np.any(values[1:] < values[:-1]):
        raise ValueError(f"normalized L2 timestamps moved backward: {path}")
    return values


def detect_internal_gaps(
    timestamps_ms: Sequence[int] | np.ndarray,
    *,
    maximum_contiguous_gap_ms: int,
) -> list[tuple[int, int]]:
    values = np.asarray(timestamps_ms, dtype=np.int64)
    if values.size == 0:
        raise ValueError("cannot detect gaps in an empty timestamp sequence")
    if np.any(values[1:] < values[:-1]):
        raise ValueError("gap timestamps are not sorted")
    positions = np.flatnonzero(np.diff(values) > int(maximum_contiguous_gap_ms))
    return [(int(values[index]), int(values[index + 1])) for index in positions]


def _observed_gaps(
    *,
    day: str,
    timestamps_ms: np.ndarray,
    maximum_contiguous_gap_ms: int,
) -> list[dict[str, Any]]:
    day_start_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000)
    day_end_ms = day_start_ms + DAY_MS
    in_day = timestamps_ms[
        (timestamps_ms >= day_start_ms) & (timestamps_ms < day_end_ms)
    ]
    if in_day.size == 0:
        raise ValueError(f"normalized L2 has no in-day timestamps: {day}")
    gaps: list[dict[str, Any]] = []
    if int(in_day[0]) - day_start_ms > maximum_contiguous_gap_ms:
        gaps.append(
            {
                "kind": "leading_edge",
                "offline_start_ts_ms": day_start_ms,
                "resume_ts_ms": int(in_day[0]),
            }
        )
    for left, right in detect_internal_gaps(
        in_day,
        maximum_contiguous_gap_ms=maximum_contiguous_gap_ms,
    ):
        gaps.append(
            {
                "kind": "internal",
                "left_visible_ts_ms": left,
                "offline_start_ts_ms": left + 1,
                "resume_ts_ms": right,
            }
        )
    if day_end_ms - int(in_day[-1]) > maximum_contiguous_gap_ms:
        gaps.append(
            {
                "kind": "trailing_edge",
                "left_visible_ts_ms": int(in_day[-1]),
                "offline_start_ts_ms": int(in_day[-1]) + 1,
                "resume_ts_ms": day_end_ms,
            }
        )
    for index, row in enumerate(gaps, start=1):
        row["observed_gap_id"] = f"{day}-OBS-{index:03d}"
        row["day"] = day
        row["duration_ms"] = int(row["resume_ts_ms"]) - int(
            row["offline_start_ts_ms"]
        )
    return gaps


def _official_trade_path(root: Path | None, day: str) -> str:
    if root is None:
        return ""
    for suffix in (".csv", ".csv.gz"):
        path = root / "BTCUSDC" / f"BTCUSDC-trades-{day}{suffix}"
        if path.is_file():
            return str(path.resolve())
    return ""


def _provider_normalized_identity(root: Path | None, day: str) -> dict[str, Any]:
    if root is None:
        return {}
    quality_path = root / "quality" / f"BTCUSDC-{day}.json"
    if not quality_path.is_file():
        return {}
    try:
        quality = json.loads(quality_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if str(quality.get("day", "")) != day or not bool(quality.get("complete_day")):
        return {}
    outputs: dict[str, dict[str, Any]] = {}
    for name in ("bbo", "l2", "clock"):
        output = quality.get(f"{name}_output") or {}
        path = Path(str(output.get("path", ""))).expanduser()
        expected_size = int(output.get("size_bytes", 0))
        expected_sha256 = str(output.get("sha256", ""))
        valid = bool(
            path.is_file()
            and expected_size > 0
            and path.stat().st_size == expected_size
            and len(expected_sha256) == 64
        )
        outputs[name] = {
            "path": str(path.resolve()) if path.is_file() else str(path),
            "sha256": expected_sha256,
            "size_bytes": expected_size,
            "admission_identity_valid": valid,
        }
    all_outputs_valid = all(
        output["admission_identity_valid"] for output in outputs.values()
    )
    day_end_ms = int(pd.Timestamp(day, tz="UTC").timestamp() * 1_000) + DAY_MS
    last_timestamp_ms = quality.get("last_timestamp_ms")
    end_gap_ms = (
        None
        if last_timestamp_ms is None
        else day_end_ms - int(last_timestamp_ms)
    )
    return {
        "quality_path": str(quality_path.resolve()),
        "quality_sha256": sha256_file(quality_path),
        "source_id": str(quality.get("source_id", "")),
        "clock_source": str(quality.get("clock_source", "")),
        "provider_normalized_replay_candidate": bool(
            quality.get("provider_normalized_replay_candidate")
        ),
        "policy_visible": bool(quality.get("policy_visible")),
        "native_sequence_ids_present": bool(
            quality.get("native_binance_sequence_ids_present")
        ),
        "exact_queue_policy_eligible": bool(
            quality.get("exact_queue_policy_eligible")
        ),
        "first_timestamp_ms": quality.get("first_timestamp_ms"),
        "last_timestamp_ms": last_timestamp_ms,
        "end_gap_ms": end_gap_ms,
        "outputs": outputs,
        "all_outputs_admitted": all_outputs_valid,
        "daily_mark_available": bool(
            all_outputs_valid and end_gap_ms is not None and 0 <= end_gap_ms <= 5_000
        ),
    }


def build_calendar_continuity_manifest(
    day_sources: Sequence[DaySource],
    *,
    start_day: str,
    end_day: str,
    anchor_identity: Mapping[str, Any],
    official_trade_root: Path | None = None,
    provider_normalized_root: Path | None = None,
    maximum_contiguous_gap_ms: int = 5_000,
    cancel_drain_ms: int = 2_000,
    feature_warmup_lookback_s: int = 3_000,
) -> dict[str, Any]:
    if maximum_contiguous_gap_ms <= 0 or cancel_drain_ms < 0:
        raise ValueError("gap and cancel-drain parameters are invalid")
    if feature_warmup_lookback_s <= 0:
        raise ValueError("feature warmup lookback must be positive")
    calendar = _calendar_days(start_day, end_day)
    if [source.day for source in day_sources] != calendar:
        raise ValueError("day sources must exactly match the ordered calendar")
    anchor_days = [str(day) for day in anchor_identity.get("target_days", ())]
    if not anchor_days or any(day not in calendar for day in anchor_days):
        raise ValueError("anchor target days are not contained in the calendar envelope")

    source_rows: list[dict[str, Any]] = []
    observed_gaps: list[dict[str, Any]] = []
    missing_native_normalized_l2_days: list[str] = []
    missing_any_normalized_l2_days: list[str] = []
    missing_mark_days: list[str] = []
    unusable_anchor_days: list[str] = []
    for source in day_sources:
        target = source.day in anchor_days
        row: dict[str, Any] = {
            **asdict(source),
            "anchor_target_day": target,
            "native_normalized_l2_file_available": False,
            "strategy_tape_usable": False,
            "actual_sha256": "",
            "timestamp_rows": 0,
            "first_timestamp_ms": None,
            "last_timestamp_ms": None,
            "observed_gap_count": 0,
        }
        if source.l2_identity_present:
            path = Path(source.l2_path).expanduser().resolve()
            if not path.is_file():
                missing_native_normalized_l2_days.append(source.day)
            else:
                actual_sha256 = sha256_file(path)
                if actual_sha256 != source.expected_sha256:
                    raise ValueError(f"normalized L2 SHA256 mismatch: {source.day}")
                row["native_normalized_l2_file_available"] = True
                row["actual_sha256"] = actual_sha256
                if source.strategy_tape_usable:
                    timestamps = _load_timestamps(path)
                    day_start_ms = int(
                        pd.Timestamp(source.day, tz="UTC").timestamp() * 1_000
                    )
                    day_end_ms = day_start_ms + DAY_MS
                    in_day = timestamps[
                        (timestamps >= day_start_ms) & (timestamps < day_end_ms)
                    ]
                    if in_day.size == 0:
                        raise ValueError(
                            f"normalized L2 has no in-day timestamps: {source.day}"
                        )
                    gaps = _observed_gaps(
                        day=source.day,
                        timestamps_ms=timestamps,
                        maximum_contiguous_gap_ms=maximum_contiguous_gap_ms,
                    )
                    row.update(
                        {
                            "strategy_tape_usable": True,
                            "timestamp_rows": int(in_day.size),
                            "first_timestamp_ms": int(in_day[0]),
                            "last_timestamp_ms": int(in_day[-1]),
                            "observed_gap_count": len(gaps),
                        }
                    )
                    observed_gaps.extend(gaps)
        else:
            missing_native_normalized_l2_days.append(source.day)
        if target and not row["strategy_tape_usable"]:
            unusable_anchor_days.append(source.day)
        trade_path = _official_trade_path(official_trade_root, source.day)
        provider = _provider_normalized_identity(provider_normalized_root, source.day)
        row["provider_normalized"] = provider
        row["provider_normalized_tape_usable"] = bool(
            provider.get("all_outputs_admitted")
            and provider.get("provider_normalized_replay_candidate")
        )
        if not (
            row["native_normalized_l2_file_available"]
            or provider.get("all_outputs_admitted")
        ):
            missing_any_normalized_l2_days.append(source.day)
        row["official_btcusdc_trade_path"] = trade_path
        row["official_trade_mark_available"] = bool(trade_path)
        row["daily_mark_source"] = (
            "official_btcusdc_individual_trades"
            if trade_path
            else "tardis_provider_normalized_bbo"
            if provider.get("daily_mark_available")
            else ""
        )
        row["daily_mark_available"] = bool(row["daily_mark_source"])
        if not row["daily_mark_available"]:
            missing_mark_days.append(source.day)
        source_rows.append(row)

    if unusable_anchor_days:
        raise ValueError(
            "frozen anchor panel contains unusable target days: "
            + ",".join(unusable_anchor_days)
        )

    grade_counts = {
        grade: sum(source.quality_grade == grade for source in day_sources)
        for grade in sorted(ALLOWED_GRADES)
    }
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": "versioned_continuous_replay_substrate_v1",
        "calendar_start_day": start_day,
        "calendar_end_day": end_day,
        "calendar_day_count": len(calendar),
        "calendar_days": calendar,
        "anchor_panel_identity": dict(anchor_identity),
        "anchor_target_days": anchor_days,
        "anchor_target_day_count": len(anchor_days),
        "maximum_contiguous_gap_ms": int(maximum_contiguous_gap_ms),
        "cancel_drain_ms": int(cancel_drain_ms),
        "feature_warmup_lookback_s": int(feature_warmup_lookback_s),
        "gap_interpretation": "planned_strategy_offline_maintenance_not_market_continuity",
        "utc_midnight_policy": "accounting_slice_only_no_flatten_no_state_reset",
        "observed_data_gaps": observed_gaps,
        "observed_data_gap_count": len(observed_gaps),
        "quality_grade_counts": grade_counts,
        "day_sources": source_rows,
        "data_readiness": {
            "anchor_panel_all_strategy_tapes_usable": not unusable_anchor_days,
            "calendar_all_native_normalized_l2_files_available": not missing_native_normalized_l2_days,
            "calendar_all_any_source_normalized_l2_files_available": not missing_any_normalized_l2_days,
            "calendar_daily_mark_bridge_complete": not missing_mark_days,
            "missing_native_normalized_l2_days": sorted(
                set(missing_native_normalized_l2_days)
            ),
            "missing_any_source_normalized_l2_days": sorted(
                set(missing_any_normalized_l2_days)
            ),
            "missing_daily_mark_days": sorted(set(missing_mark_days)),
        },
        "authority": {
            "anchor_panel_continuity_comparison": True,
            "continuous_pnl_inventory_campaign_sensitivity": True,
            "tail_governance_causal_attribution_without_on_off_control": False,
            "upgrade_non_a_to_grade_a": False,
            "gap_queue_lifecycle_q90_markout": False,
            "strategy_action_or_live_authority": False,
        },
    }
    manifest["canonical_manifest_sha256"] = canonical_sha256(manifest)
    validate_calendar_continuity_manifest(manifest)
    return manifest


def validate_calendar_continuity_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected calendar continuity manifest schema")
    if canonical_sha256(manifest) != str(manifest.get("canonical_manifest_sha256", "")):
        raise ValueError("calendar continuity manifest canonical hash mismatch")
    calendar = _calendar_days(
        str(manifest["calendar_start_day"]),
        str(manifest["calendar_end_day"]),
    )
    if list(manifest.get("calendar_days", ())) != calendar:
        raise ValueError("calendar continuity day list is incomplete or unordered")
    sources = list(manifest.get("day_sources", ()))
    if [str(row.get("day", "")) for row in sources] != calendar:
        raise ValueError("calendar continuity source days do not match the calendar")
    anchor = [str(day) for day in manifest.get("anchor_target_days", ())]
    if len(anchor) != int(manifest.get("anchor_target_day_count", -1)):
        raise ValueError("anchor target day count does not match its list")
    if not anchor or anchor != sorted(set(anchor)):
        raise ValueError("anchor target days must be unique and ordered")
    if any(day not in calendar for day in anchor):
        raise ValueError("anchor target day lies outside the calendar")
    target_rows = {str(row["day"]): row for row in sources if row.get("anchor_target_day")}
    if list(target_rows) != anchor:
        raise ValueError("anchor flags do not match the frozen target panel")
    if any(not bool(target_rows[day].get("strategy_tape_usable")) for day in anchor):
        raise ValueError("anchor target day lacks a usable strategy tape")
    gaps = list(manifest.get("observed_data_gaps", ()))
    if len(gaps) != int(manifest.get("observed_data_gap_count", -1)):
        raise ValueError("observed data gap count does not match its rows")
    previous_start = -1
    ids: set[str] = set()
    for row in gaps:
        gap_id = str(row.get("observed_gap_id", ""))
        start = int(row["offline_start_ts_ms"])
        end = int(row["resume_ts_ms"])
        if not gap_id or gap_id in ids or start >= end or start < previous_start:
            raise ValueError("observed data gaps are invalid or unordered")
        ids.add(gap_id)
        previous_start = start
    authority = manifest.get("authority") or {}
    if authority.get("tail_governance_causal_attribution_without_on_off_control") is not False:
        raise ValueError("continuity alone cannot identify tail-governance effects")
    if authority.get("upgrade_non_a_to_grade_a") is not False:
        raise ValueError("restart-aware replay cannot upgrade non-A days")
    if authority.get("strategy_action_or_live_authority") is not False:
        raise ValueError("calendar substrate cannot grant strategy authority")


def write_manifest(path: Path, manifest: Mapping[str, Any]) -> None:
    validate_calendar_continuity_manifest(manifest)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial")
    temporary.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day", required=True)
    parser.add_argument("--anchor-spec", type=Path, required=True)
    parser.add_argument("--anchor-day-field", default="panels.development_days")
    parser.add_argument("--grade-ledger", type=Path, required=True)
    parser.add_argument("--source-quality", type=Path, action="append", required=True)
    parser.add_argument("--official-trade-root", type=Path)
    parser.add_argument("--provider-normalized-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--maximum-contiguous-gap-ms", type=int, default=5_000)
    parser.add_argument("--cancel-drain-ms", type=int, default=2_000)
    parser.add_argument("--feature-warmup-lookback-s", type=int, default=3_000)
    args = parser.parse_args(argv)
    anchor_identity = load_anchor_identity(
        args.anchor_spec,
        day_field=args.anchor_day_field,
    )
    sources = load_day_sources(
        start_day=args.start_day,
        end_day=args.end_day,
        grade_ledger_path=args.grade_ledger,
        source_quality_paths=args.source_quality,
    )
    manifest = build_calendar_continuity_manifest(
        sources,
        start_day=args.start_day,
        end_day=args.end_day,
        anchor_identity=anchor_identity,
        official_trade_root=(
            args.official_trade_root.expanduser().resolve()
            if args.official_trade_root
            else None
        ),
        provider_normalized_root=(
            args.provider_normalized_root.expanduser().resolve()
            if args.provider_normalized_root
            else None
        ),
        maximum_contiguous_gap_ms=args.maximum_contiguous_gap_ms,
        cancel_drain_ms=args.cancel_drain_ms,
        feature_warmup_lookback_s=args.feature_warmup_lookback_s,
    )
    write_manifest(args.output.expanduser().resolve(), manifest)
    print(
        json.dumps(
            {
                "calendar_day_count": manifest["calendar_day_count"],
                "anchor_target_day_count": manifest["anchor_target_day_count"],
                "quality_grade_counts": manifest["quality_grade_counts"],
                "observed_data_gap_count": manifest["observed_data_gap_count"],
                "data_readiness": manifest["data_readiness"],
                "canonical_manifest_sha256": manifest["canonical_manifest_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
