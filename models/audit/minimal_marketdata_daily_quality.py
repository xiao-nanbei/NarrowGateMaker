#!/usr/bin/env python3
"""Grade each UTC day against the current minimal local replay data contract.

The contract intentionally excludes CryptoHFT BTCUSDT order-book data.  The
historical BTCUSDT bridge is supplied by official Binance trades; only the
BTCUSDC execution market requires native snapshot/delta order-book data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pyarrow.parquet as pq


SCHEMA_VERSION = "minimal_marketdata_daily_quality.v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _present(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0


def _read_optional_daily_csv(path: Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    source = path.expanduser().resolve()
    frame = pd.read_csv(source)
    if "day" not in frame:
        raise ValueError(f"daily audit must contain a day column: {source}")
    frame = frame.copy()
    frame["day"] = pd.to_datetime(
        frame["day"], utc=True, errors="raise"
    ).dt.strftime("%Y-%m-%d")
    if frame["day"].duplicated().any():
        raise ValueError(f"daily audit contains duplicate days: {source}")
    return frame.set_index("day", drop=False)


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _float_or_zero(value: Any) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(value)


def _max_internal_gap_s(path_value: Any) -> float | None:
    if path_value is None or pd.isna(path_value):
        return None
    path = Path(str(path_value)).expanduser()
    if not path.is_file():
        return None
    parquet = pq.ParquetFile(path)
    if "timestamp" not in parquet.schema.names:
        return None
    values = parquet.read(columns=["timestamp"])["timestamp"].to_numpy()
    if len(values) < 2:
        return None
    timestamps = pd.to_numeric(values, errors="coerce")
    timestamps = timestamps[pd.notna(timestamps)]
    if len(timestamps) < 2:
        return None
    gaps = timestamps[1:] - timestamps[:-1]
    return float(max(float(gaps.max()), 0.0) / 1000.0)


def _official_paths(
    data_root: Path,
) -> dict[str, Callable[[str], Path]]:
    return {
        "btcusdc_perp_aggtrades": lambda day: (
            data_root / "raw" / f"BTCUSDC-aggTrades-{day}.csv"
        ),
        "btcusdt_perp_aggtrades": lambda day: (
            data_root / "raw" / f"BTCUSDT-aggTrades-{day}.csv"
        ),
        "btcusdc_perp_trades": lambda day: (
            data_root
            / "raw_trades"
            / "BTCUSDC"
            / f"BTCUSDC-trades-{day}.csv"
        ),
        "btcusdt_perp_trades": lambda day: (
            data_root
            / "raw_trades"
            / "BTCUSDT"
            / f"BTCUSDT-trades-{day}.csv"
        ),
        "btcusdc_spot_aggtrades": lambda day: (
            data_root / "raw_spot" / f"BTCUSDC-aggTrades-{day}.csv"
        ),
        "btcusdt_spot_aggtrades": lambda day: (
            data_root / "raw_spot" / f"BTCUSDT-aggTrades-{day}.csv"
        ),
        "btcusdc_perp_metrics": lambda day: (
            data_root / "raw_metrics" / f"BTCUSDC-metrics-{day}.csv"
        ),
        "btcusdt_perp_metrics": lambda day: (
            data_root / "raw_metrics" / f"BTCUSDT-metrics-{day}.csv"
        ),
    }


def _cryptohft_hour_path(
    raw_root: Path,
    timestamp: datetime,
) -> Path:
    return (
        raw_root
        / "binance_futures"
        / timestamp.strftime("%Y-%m-%d")
        / timestamp.strftime("%H")
        / "BTCUSDC_orderbook.parquet.zst"
    )


def classify_day(
    *,
    official_complete: bool,
    cryptohft_complete: bool,
    sequence_eligible: bool,
    normalized_present: bool,
    normalized_formal: bool,
    normalized_coverage: float,
    normalized_max_internal_gap_s: float | None = None,
    maximum_formal_gap_s: float = 5.0,
) -> tuple[str, str, bool]:
    """Return grade, allowed-use identity, and formal eligibility."""

    formal = bool(
        official_complete
        and cryptohft_complete
        and sequence_eligible
        and normalized_present
        and normalized_formal
        and normalized_max_internal_gap_s is not None
        and normalized_max_internal_gap_s <= maximum_formal_gap_s
    )
    if formal:
        return "A", "formal_training_and_replay", True
    if not official_complete or not cryptohft_complete:
        return "F", "excluded_missing_required_raw", False
    if not sequence_eligible:
        return "D", "official_trade_bar_only_no_exact_l2", False
    if normalized_present and normalized_coverage >= 0.99:
        return "B", "gap_censored_l2_replay_only", False
    if not normalized_present:
        return "C", "source_valid_normalization_pending", False
    if normalized_coverage >= 0.95:
        return "C", "segmented_l2_diagnostic_only", False
    return "D", "official_trade_bar_only_no_exact_l2", False


def build_daily_quality(
    *,
    start_day: str,
    end_day: str,
    data_root: Path,
    cryptohft_raw_root: Path,
    sequence_audit_csv: Path | None,
    normalized_quality_csv: Path | None,
    normalized_gap_csv: Path | None,
    maximum_formal_gap_s: float,
) -> pd.DataFrame:
    root = data_root.expanduser().resolve()
    crypto_root = cryptohft_raw_root.expanduser().resolve()
    sequence = _read_optional_daily_csv(sequence_audit_csv)
    normalized = _read_optional_daily_csv(normalized_quality_csv)
    gaps = _read_optional_daily_csv(normalized_gap_csv)
    official_paths = _official_paths(root)
    days = pd.date_range(start_day, end_day, freq="D", tz="UTC")
    rows: list[dict[str, Any]] = []

    for timestamp in days:
        day = timestamp.strftime("%Y-%m-%d")
        start = timestamp.to_pydatetime()
        warmup = start - timedelta(days=1)
        official_flags = {
            key: _present(builder(day))
            for key, builder in official_paths.items()
        }
        target_hours = sum(
            _present(_cryptohft_hour_path(crypto_root, start + timedelta(hours=h)))
            for h in range(24)
        )
        warmup_hours = sum(
            _present(_cryptohft_hour_path(crypto_root, warmup + timedelta(hours=h)))
            for h in range(24)
        )
        sequence_row = sequence.loc[day] if day in sequence.index else None
        normalized_row = normalized.loc[day] if day in normalized.index else None
        gap_row = gaps.loc[day] if day in gaps.index else None
        sequence_known = sequence_row is not None
        sequence_eligible = bool(
            sequence_known and _parse_bool(sequence_row.get("eligible"))
        )
        normalized_present = normalized_row is not None
        bbo_coverage = (
            _float_or_zero(normalized_row.get("bbo_coverage"))
            if normalized_present
            else 0.0
        )
        l2_coverage = (
            _float_or_zero(normalized_row.get("l2_coverage"))
            if normalized_present
            else 0.0
        )
        normalized_coverage = min(bbo_coverage, l2_coverage)
        normalized_formal = bool(
            normalized_present
            and _parse_bool(normalized_row.get("formal_eligible"))
        )
        if gap_row is not None:
            normalized_max_internal_gap_s = _float_or_zero(
                gap_row.get("max_internal_gap_s")
            )
        elif normalized_present:
            normalized_max_internal_gap_s = _max_internal_gap_s(
                normalized_row.get("bbo_source_path")
            )
        else:
            normalized_max_internal_gap_s = None
        official_count = sum(official_flags.values())
        official_complete = official_count == len(official_flags)
        cryptohft_complete = target_hours == 24 and warmup_hours == 24
        grade, allowed_use, formal = classify_day(
            official_complete=official_complete,
            cryptohft_complete=cryptohft_complete,
            sequence_eligible=sequence_eligible,
            normalized_present=normalized_present,
            normalized_formal=normalized_formal,
            normalized_coverage=normalized_coverage,
            normalized_max_internal_gap_s=normalized_max_internal_gap_s,
            maximum_formal_gap_s=maximum_formal_gap_s,
        )
        reasons: list[str] = []
        if not official_complete:
            reasons.append("official_binance_raw_incomplete")
        if target_hours != 24:
            reasons.append("cryptohft_target_hours_incomplete")
        if warmup_hours != 24:
            reasons.append("cryptohft_warmup_hours_incomplete")
        if not sequence_known:
            reasons.append("native_sequence_not_audited")
        elif not sequence_eligible:
            detail = str(sequence_row.get("exclusion_reasons", "")).strip()
            reasons.append(detail or "native_sequence_invalid")
        if not normalized_present:
            reasons.append("normalized_l2_not_built")
        elif not normalized_formal:
            detail = str(
                normalized_row.get("formal_exclusion_reason", "")
            ).strip()
            reasons.append(detail or "normalized_l2_not_formal")
        rows.append(
            {
                "day": day,
                "quality_grade": grade,
                "allowed_use": allowed_use,
                "formal_training_replay_eligible": formal,
                "official_required_files_present": official_count,
                "official_required_files_expected": len(official_flags),
                "official_complete": official_complete,
                **official_flags,
                "cryptohft_btcusdc_target_hours_present": target_hours,
                "cryptohft_btcusdc_warmup_hours_present": warmup_hours,
                "cryptohft_btcusdc_complete": cryptohft_complete,
                "cryptohft_btcusdt_required": False,
                "native_sequence_audited": sequence_known,
                "native_sequence_eligible": sequence_eligible,
                "native_sequence_exclusion_reasons": (
                    ""
                    if sequence_row is None
                    else str(sequence_row.get("exclusion_reasons", ""))
                ),
                "normalized_l2_present": normalized_present,
                "normalized_formal_eligible": normalized_formal,
                "normalized_bbo_coverage": bbo_coverage,
                "normalized_l2_coverage": l2_coverage,
                "normalized_min_coverage": normalized_coverage,
                "normalized_max_internal_gap_s": normalized_max_internal_gap_s,
                "maximum_formal_gap_s": maximum_formal_gap_s,
                "normalized_formal_exclusion_reason": (
                    ""
                    if normalized_row is None
                    else str(
                        normalized_row.get("formal_exclusion_reason", "")
                    )
                ),
                "quality_reasons": "|".join(reason for reason in reasons if reason),
            }
        )
    return pd.DataFrame(rows)


def write_daily_quality(
    *,
    frame: pd.DataFrame,
    output_csv: Path,
    output_json: Path,
    inputs: dict[str, Path | None],
) -> dict[str, Any]:
    csv_path = output_csv.expanduser().resolve()
    json_path = output_json.expanduser().resolve()
    for path in (csv_path, json_path):
        if path.exists():
            raise FileExistsError(f"refusing to overwrite quality artifact: {path}")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(csv_path, index=False)
    grades = Counter(frame["quality_grade"].astype(str))
    allowed = Counter(frame["allowed_use"].astype(str))
    payload = {
        "schema_version": SCHEMA_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "day_count": int(len(frame)),
        "first_day": str(frame["day"].iloc[0]),
        "last_day": str(frame["day"].iloc[-1]),
        "grade_counts": dict(sorted(grades.items())),
        "allowed_use_counts": dict(sorted(allowed.items())),
        "formal_training_replay_day_count": int(
            frame["formal_training_replay_eligible"].astype(bool).sum()
        ),
        "minimal_contract": {
            "official_binance": [
                "BTCUSDC/BTCUSDT futures aggTrades",
                "BTCUSDC/BTCUSDT futures individual trades",
                "BTCUSDC/BTCUSDT spot aggTrades",
                "BTCUSDC/BTCUSDT futures metrics",
            ],
            "native_orderbook": (
                "CryptoHFTData BTCUSDC target day plus previous natural "
                "UTC day snapshot/delta warmup"
            ),
            "explicitly_not_required": "CryptoHFTData BTCUSDT orderbook",
        },
        "grade_contract": {
            "A": "all required raw, native sequence, formal normalized L2, and maximum-gap gate pass",
            "B": "sequence-valid and >=99% normalized coverage, but a formal or maximum-gap gate fails",
            "C": "sequence-valid but normalization is pending or coverage is 95%-99%",
            "D": "raw is present but native sequence fails or normalized coverage is <95%",
            "F": "one or more required raw sources are missing",
        },
        "input_paths": {
            key: None if value is None else str(value.expanduser().resolve())
            for key, value in inputs.items()
        },
        "input_sha256": {
            key: (
                None
                if value is None
                else _sha256(value.expanduser().resolve())
            )
            for key, value in inputs.items()
        },
        "output_csv": str(csv_path),
        "output_csv_sha256": _sha256(csv_path),
    }
    json_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--cryptohft-raw-root", type=Path, required=True)
    parser.add_argument("--sequence-audit-csv", type=Path)
    parser.add_argument("--normalized-quality-csv", type=Path)
    parser.add_argument("--normalized-gap-csv", type=Path)
    parser.add_argument("--maximum-formal-gap-s", type=float, default=5.0)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    if end < start:
        raise SystemExit("--end must not precede --start")
    if args.maximum_formal_gap_s <= 0.0:
        raise SystemExit("--maximum-formal-gap-s must be positive")
    frame = build_daily_quality(
        start_day=start.isoformat(),
        end_day=end.isoformat(),
        data_root=args.data_root,
        cryptohft_raw_root=args.cryptohft_raw_root,
        sequence_audit_csv=args.sequence_audit_csv,
        normalized_quality_csv=args.normalized_quality_csv,
        normalized_gap_csv=args.normalized_gap_csv,
        maximum_formal_gap_s=args.maximum_formal_gap_s,
    )
    payload = write_daily_quality(
        frame=frame,
        output_csv=args.output_csv,
        output_json=args.output_json,
        inputs={
            "sequence_audit_csv": args.sequence_audit_csv,
            "normalized_quality_csv": args.normalized_quality_csv,
            "normalized_gap_csv": args.normalized_gap_csv,
        },
    )
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
