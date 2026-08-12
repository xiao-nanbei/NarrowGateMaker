#!/usr/bin/env python3
"""Build a source-aware research-day universe and normalized book view.

The output deliberately keeps native CryptoHFTData and Tardis provider-local
books under different authority labels.  A Tardis day may be admitted for
causal training, C++ calculation, and provider-normalized sensitivity replay,
but it can never acquire native sequence, exact-queue, action, or live
authority through this builder.

The dataset directory is an immutable hard-link view.  Source bytes are not
copied, and the canonical ``normalized_l2_100ms_v2`` registry is never edited.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from data_paths import data_root

SCHEMA_VERSION = "narrowgate.research_day_universe.v1"
DATASET_VERSION = "normalized_l2_research_union_v1"
PROVIDER_DATASET_ID = "normalized_tardis_l2_100ms_v1"
NATIVE_DATASET_ID = "normalized_l2_100ms_v2"
SYMBOL = "BTCUSDC"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _atomic_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write empty CSV: {path}")
    fieldnames = list(rows[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        return [dict(row) for row in reader]


def _day_range(start: date, end: date) -> list[str]:
    if start > end:
        raise ValueError("start is after end")
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def _previous_day(day: str) -> str:
    return (date.fromisoformat(day) - timedelta(days=1)).isoformat()


def _book_path(root: Path, kind: str, day: str) -> Path:
    return root / kind / f"{SYMBOL}-{kind}-{day}.parquet"


def _provider_quality_index(path: Path) -> dict[str, dict[str, str]]:
    rows = _read_csv(path)
    required = {"day", "provider_normalized_replay_candidate"}
    if rows and not required.issubset(rows[0]):
        raise ValueError(f"provider quality lacks {sorted(required)}: {path}")
    return {str(row["day"]): row for row in rows}


def _native_quality_index(path: Path) -> dict[str, dict[str, str]]:
    rows = _read_csv(path)
    required = {"day", "quality_grade", "formal_training_replay_eligible"}
    if rows and not required.issubset(rows[0]):
        raise ValueError(f"native quality lacks {sorted(required)}: {path}")
    return {str(row["day"]): row for row in rows}


def _non_cryptohft_validity(path: Path) -> tuple[dict[str, bool], int]:
    rows = _read_csv(path)
    required = {"day", "source_id", "status"}
    if rows and not required.issubset(rows[0]):
        raise ValueError(f"technical source audit lacks {sorted(required)}: {path}")
    expected_sources = len({str(row["source_id"]) for row in rows})
    by_day: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        by_day[str(row["day"])].append(row)
    valid = {
        day: (
            len({str(row["source_id"]) for row in day_rows}) == expected_sources
            and all(str(row["status"]) == "valid" for row in day_rows)
        )
        for day, day_rows in by_day.items()
    }
    return valid, expected_sources


def _target_input_state(
    root: Path,
    day: str,
    *,
    trade_feature_root: Path,
) -> dict[str, bool]:
    paths = {
        "btcusdc_individual_trades": (
            root / "raw_trades" / "BTCUSDC" / f"BTCUSDC-trades-{day}.csv"
        ),
        "btcusdc_aggtrades": root / "raw" / f"BTCUSDC-aggTrades-{day}.csv",
        "btcusdc_perp_bars": root / "bars_1s" / f"BTCUSDC-1s-{day}.parquet",
        "btcusdt_perp_bars": root / "bars_1s" / f"BTCUSDT-1s-{day}.parquet",
        "btcusdc_spot_bars": (
            root / "bars_1s_spot" / f"BTCUSDC-1s-{day}.parquet"
        ),
        "btcusdt_spot_bars": (
            root / "bars_1s_spot" / f"BTCUSDT-1s-{day}.parquet"
        ),
        "btcusdc_metrics": (
            root / "metrics_5m" / f"BTCUSDC-metrics-{day}.parquet"
        ),
        "btcusdt_metrics": (
            root / "metrics_5m" / f"BTCUSDT-metrics-{day}.parquet"
        ),
        "btcusdc_taker_tempo": (
            trade_feature_root
            / "BTCUSDC"
            / f"BTCUSDC-trade-tempo-{day}.parquet"
        ),
    }
    return {key: path.is_file() for key, path in paths.items()}


def _pair_exists(root: Path, day: str) -> bool:
    return all(
        _book_path(root, kind, day).is_file()
        for kind in ("bbo", "l2")
    )


def _source_choice(
    day: str,
    *,
    provider_quality: Mapping[str, Mapping[str, str]],
    native_quality: Mapping[str, Mapping[str, str]],
    provider_root: Path,
    native_root: Path,
) -> tuple[str, Path, str, bool, str]:
    native = native_quality.get(day)
    native_target = bool(
        native
        and str(native.get("quality_grade")) == "A"
        and _parse_bool(native.get("formal_training_replay_eligible"))
        and _pair_exists(native_root, day)
        and _pair_exists(native_root, _previous_day(day))
    )
    if native_target:
        return (
            "native_formal_lifecycle",
            native_root,
            NATIVE_DATASET_ID,
            True,
            "",
        )

    provider = provider_quality.get(day)
    prior = provider_quality.get(_previous_day(day))
    provider_target = bool(
        provider
        and prior
        and _parse_bool(provider.get("provider_normalized_replay_candidate"))
        and _parse_bool(prior.get("provider_normalized_replay_candidate"))
        and _pair_exists(provider_root, day)
        and _pair_exists(provider_root, _previous_day(day))
    )
    if provider_target:
        return (
            "provider_normalized_causal",
            provider_root,
            PROVIDER_DATASET_ID,
            False,
            "",
        )

    reasons: list[str] = []
    if native and str(native.get("quality_grade")) != "A":
        reasons.append(f"native_grade_{native.get('quality_grade')}")
    if provider and not _parse_bool(
        provider.get("provider_normalized_replay_candidate")
    ):
        reasons.append("provider_target_rejected")
    if not prior or not _parse_bool(
        prior.get("provider_normalized_replay_candidate") if prior else False
    ):
        reasons.append("provider_dminus1_rejected_or_missing")
    if not reasons:
        reasons.append("no_supported_book_source")
    return "none", provider_root, "", False, "|".join(reasons)


def _link_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if os.path.samefile(source, destination):
            return
        raise FileExistsError(f"conflicting research book source: {destination}")
    os.link(source, destination)


def _context_quality_row(
    *,
    day: str,
    source_authority: str,
    source_dataset_id: str,
    source_root: Path,
    target_days: set[str],
    provider_quality: Mapping[str, Mapping[str, str]],
    native_quality: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    bbo = _book_path(source_root, "bbo", day)
    l2 = _book_path(source_root, "l2", day)
    provider = provider_quality.get(day, {})
    native = native_quality.get(day, {})
    is_native = source_authority == "native_formal_lifecycle"
    provider_candidate = _parse_bool(
        provider.get("provider_normalized_replay_candidate")
    )
    native_formal = bool(
        str(native.get("quality_grade", "")) == "A"
        and _parse_bool(native.get("formal_training_replay_eligible"))
    )
    coverage = (
        provider.get("freshness_union_coverage", "")
        if not is_native
        else native.get("normalized_min_coverage", "")
    )
    return {
        "day": day,
        "dataset_version": DATASET_VERSION,
        "source_dataset_id": source_dataset_id,
        "source_authority": source_authority,
        "research_target_day": str(day in target_days).lower(),
        "rebuilt": "true",
        "sequence_valid": str(is_native and native_formal).lower(),
        "warmup_valid": "true",
        "target_source_valid": "true",
        "strict_listed": str(is_native and native_formal).lower(),
        "formal_eligible": str(is_native and native_formal and day in target_days).lower(),
        "formal_exclusion_reason": (
            "" if is_native and native_formal else "provider_has_no_binance_sequence_ids"
        ),
        "source_root": str(source_root),
        "source_label": source_dataset_id,
        "reconstruction_mode": (
            "native_snapshot_delta_normalized_100ms"
            if is_native
            else "tardis_provider_local_half_open_100ms"
        ),
        "source_formal_capable": str(is_native).lower(),
        "cadence_schema_valid": "true",
        "coverage_99_valid": str(
            native_formal if is_native else provider_candidate
        ).lower(),
        "normalized_min_coverage": coverage,
        "provider_normalized_replay_candidate": str(
            provider_candidate and not is_native
        ).lower(),
        "native_sequence_continuity_proven": str(is_native and native_formal).lower(),
        "exact_queue_policy_eligible": "false",
        "formal_lifecycle_replay_eligible": str(
            is_native and native_formal and day in target_days
        ).lower(),
        "provider_sensitivity_replay_eligible": str(
            (not is_native) and provider_candidate and day in target_days
        ).lower(),
        "bbo_source_path": str(bbo.resolve()),
        "bbo_sha256": _sha256(bbo),
        "bbo_size_bytes": bbo.stat().st_size,
        "l2_source_path": str(l2.resolve()),
        "l2_sha256": _sha256(l2),
        "l2_size_bytes": l2.stat().st_size,
    }


def build_universe(
    *,
    start: date,
    end: date,
    provider_quality_csv: Path,
    provider_root: Path,
    non_cryptohft_csv: Path,
    native_quality_csv: Path,
    native_root: Path,
    project_data_root: Path,
    trade_feature_root: Path,
    output_root: Path,
) -> dict[str, Any]:
    """Freeze the maximum currently supported source-separated day universe."""

    if output_root.exists():
        raise FileExistsError(
            f"versioned research dataset already exists: {output_root}"
        )
    provider_quality = _provider_quality_index(provider_quality_csv)
    native_quality = _native_quality_index(native_quality_csv)
    non_cryptohft_valid, expected_sources = _non_cryptohft_validity(
        non_cryptohft_csv
    )

    day_rows: list[dict[str, Any]] = []
    target_sources: dict[str, tuple[str, Path, str]] = {}
    for day in _day_range(start, end):
        authority, source_root, dataset_id, native_formal, source_reason = (
            _source_choice(
                day,
                provider_quality=provider_quality,
                native_quality=native_quality,
                provider_root=provider_root,
                native_root=native_root,
            )
        )
        input_state = _target_input_state(
            project_data_root,
            day,
            trade_feature_root=trade_feature_root,
        )
        is_2025_technical = day in non_cryptohft_valid
        official_source_valid = (
            bool(non_cryptohft_valid.get(day))
            if is_2025_technical
            else _parse_bool(
                native_quality.get(day, {}).get("official_complete")
            )
        )
        replay_inputs_ready = all(
            input_state[key]
            for key in (
                "btcusdc_individual_trades",
                "btcusdc_perp_bars",
                "btcusdt_perp_bars",
            )
        )
        feature_inputs_ready = all(input_state.values())
        research_good_day = bool(
            authority != "none"
            and official_source_valid
            and replay_inputs_ready
        )
        causal_training_eligible = bool(
            research_good_day and feature_inputs_ready
        )
        provider_replay = bool(
            research_good_day and authority == "provider_normalized_causal"
        )
        native_replay = bool(
            research_good_day and authority == "native_formal_lifecycle"
        )
        reasons: list[str] = []
        if authority == "none":
            reasons.append(source_reason)
        if not official_source_valid:
            reasons.append("official_or_declared_source_set_incomplete")
        if not replay_inputs_ready:
            reasons.append("replay_inputs_not_materialized")
        if research_good_day and not feature_inputs_ready:
            reasons.append("training_sidecars_not_materialized")
        row = {
            "day": day,
            "research_good_day": str(research_good_day).lower(),
            "causal_training_eligible": str(causal_training_eligible).lower(),
            "cpp_calculation_eligible": str(research_good_day).lower(),
            "provider_sensitivity_replay_eligible": str(provider_replay).lower(),
            "formal_lifecycle_replay_eligible": str(native_replay).lower(),
            "exact_queue_policy_eligible": "false",
            "source_authority": authority,
            "source_dataset_id": dataset_id,
            "warmup_day": _previous_day(day),
            "warmup_source_valid": str(authority != "none").lower(),
            "official_source_set_valid": str(official_source_valid).lower(),
            "non_cryptohft_expected_source_count": (
                expected_sources if is_2025_technical else ""
            ),
            **{
                key: str(value).lower()
                for key, value in input_state.items()
            },
            "allowed_use": (
                "causal_training|cpp_calculation|provider_sensitivity_replay"
                if provider_replay
                else (
                    "causal_training|cpp_calculation|formal_lifecycle_replay"
                    if native_replay
                    else ""
                )
            ),
            "exclusion_reasons": "|".join(reason for reason in reasons if reason),
        }
        day_rows.append(row)
        if research_good_day:
            target_sources[day] = (authority, source_root, dataset_id)

    if not target_sources:
        raise ValueError("no research good days passed the source-aware contract")

    context_sources: dict[str, tuple[str, Path, str]] = {}
    for day, source in target_sources.items():
        for needed in (_previous_day(day), day):
            current = context_sources.get(needed)
            if current is not None and current != source:
                raise ValueError(
                    f"{needed}: conflicting context sources {current[0]} and {source[0]}"
                )
            context_sources[needed] = source

    output_root.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{output_root.name}.staging.", dir=output_root.parent
        )
    )
    try:
        for day, (_, source_root, _) in sorted(context_sources.items()):
            for kind in ("bbo", "l2"):
                source = _book_path(source_root, kind, day)
                if not source.is_file():
                    raise FileNotFoundError(
                        f"{day}: missing {kind} source after admission: {source}"
                    )
                _link_file(source, _book_path(staging, kind, day))

        target_days = set(target_sources)
        quality_rows = [
            _context_quality_row(
                day=day,
                source_authority=source[0],
                source_dataset_id=source[2],
                source_root=source[1],
                target_days=target_days,
                provider_quality=provider_quality,
                native_quality=native_quality,
            )
            for day, source in sorted(context_sources.items())
        ]
        _atomic_csv(staging / "daily_quality.csv", quality_rows)
        _atomic_csv(staging / "research_day_universe.csv", day_rows)
        _atomic_csv(
            staging / "good_days.csv",
            [{"day": day} for day in sorted(target_sources)],
        )
        training_days = [
            row["day"]
            for row in day_rows
            if _parse_bool(row["causal_training_eligible"])
        ]
        if training_days:
            _atomic_csv(
                staging / "training_days.csv",
                [{"day": day} for day in training_days],
            )
        provider_days = [
            day
            for day, source in sorted(target_sources.items())
            if source[0] == "provider_normalized_causal"
        ]
        if provider_days:
            _atomic_csv(
                staging / "provider_replay_days.csv",
                [{"day": day} for day in provider_days],
            )
        native_days = [
            day
            for day, source in sorted(target_sources.items())
            if source[0] == "native_formal_lifecycle"
        ]
        if native_days:
            _atomic_csv(
                staging / "native_replay_days.csv",
                [{"day": day} for day in native_days],
            )

        output_names = [
            "daily_quality.csv",
            "research_day_universe.csv",
            "good_days.csv",
        ]
        if training_days:
            output_names.append("training_days.csv")
        if provider_days:
            output_names.append("provider_replay_days.csv")
        if native_days:
            output_names.append("native_replay_days.csv")

        manifest: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "dataset_version": DATASET_VERSION,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": SYMBOL,
            "start_day": start.isoformat(),
            "end_day": end.isoformat(),
            "day_count": len(target_sources),
            "training_day_count": len(training_days),
            "context_day_count": len(context_sources),
            "source_counts": {
                "provider_normalized_causal": len(provider_days),
                "native_formal_lifecycle": len(native_days),
            },
            "permission_boundary": {
                "canonical_good_day_modified": False,
                "canonical_normalized_l2_modified": False,
                "native_binance_sequence_ids_present_for_all_days": False,
                "exact_queue_policy_eligible": False,
                "provider_days_are_sensitivity_only": True,
                "action_authority": False,
                "live_authority": False,
            },
            "inputs": {
                "provider_quality_csv": {
                    "path": str(provider_quality_csv),
                    "sha256": _sha256(provider_quality_csv),
                },
                "provider_root": str(provider_root),
                "non_cryptohft_csv": {
                    "path": str(non_cryptohft_csv),
                    "sha256": _sha256(non_cryptohft_csv),
                },
                "native_quality_csv": {
                    "path": str(native_quality_csv),
                    "sha256": _sha256(native_quality_csv),
                },
                "native_root": str(native_root),
                "project_data_root": str(project_data_root),
                "trade_feature_root": str(trade_feature_root),
            },
            "outputs": {
                name: {
                    "path": str((output_root / name).resolve()),
                    "sha256": _sha256(staging / name),
                }
                for name in output_names
            },
            "source_files": [
                {
                    "day": row["day"],
                    "source_authority": row["source_authority"],
                    "bbo_sha256": row["bbo_sha256"],
                    "l2_sha256": row["l2_sha256"],
                }
                for row in quality_rows
            ],
        }
        _atomic_json(staging / "manifest.json", manifest)
        os.replace(staging, output_root)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return manifest


def _parser() -> argparse.ArgumentParser:
    root = data_root(Path(__file__).resolve().parent.parent)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--provider-quality-csv", type=Path, required=True)
    parser.add_argument(
        "--provider-root",
        type=Path,
        default=root / PROVIDER_DATASET_ID,
    )
    parser.add_argument("--non-cryptohft-csv", type=Path, required=True)
    parser.add_argument("--native-quality-csv", type=Path, required=True)
    parser.add_argument(
        "--native-root",
        type=Path,
        default=root / "normalized_l2_100ms_v2_minimal141_20260727",
    )
    parser.add_argument("--data-root", type=Path, default=root)
    parser.add_argument(
        "--trade-feature-root",
        type=Path,
        default=root / "trade_features",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=root / DATASET_VERSION,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    payload = build_universe(
        start=args.start,
        end=args.end,
        provider_quality_csv=args.provider_quality_csv.expanduser().resolve(),
        provider_root=args.provider_root.expanduser().resolve(),
        non_cryptohft_csv=args.non_cryptohft_csv.expanduser().resolve(),
        native_quality_csv=args.native_quality_csv.expanduser().resolve(),
        native_root=args.native_root.expanduser().resolve(),
        project_data_root=args.data_root.expanduser().resolve(),
        trade_feature_root=args.trade_feature_root.expanduser().resolve(),
        output_root=args.output_root.expanduser().resolve(),
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
