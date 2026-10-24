#!/usr/bin/env python3
"""Freeze the outcome-blind 2025 source intersection for cooldown v2.

The manifest binds provider-normalized BBO/L2 and official Binance Futures
trade files.  It may be used to estimate feature scales, missingness, and a
predeclared predicate universe.  It carries no queue, economic, action, or
live authority.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from data_paths import data_root
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as features,
)

IDENTITY = features.IDENTITY
SCHEMA_VERSION = f"{IDENTITY}.outcome_blind_source_manifest.v1"
SYMBOL = "BTCUSDC"
SOURCE_AUTHORITY = "provider_normalized_causal"
TRADE_AUTHORITY = "binance_futures_official_public"

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_DATA_ROOT = data_root(ROOT)
DEFAULT_UNION_ROOT = DEFAULT_DATA_ROOT / "normalized_l2_research_union_v1"
DEFAULT_OUTPUT = DEFAULT_DATA_ROOT / (
    "reports/causal_multichannel_window_boolean_cooldown_duration_v2_20260810/"
    "outcome_blind_2025_source_manifest.json"
)

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_SHA_RE = re.compile(r"[0-9a-f]{64}")
_AGGTRADE_HEADER = (
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
)
_INDIVIDUAL_TRADE_HEADER = (
    "id",
    "price",
    "qty",
    "quote_qty",
    "time",
    "is_buyer_maker",
)


class SourceManifestError(RuntimeError):
    """Raised when an outcome-blind source identity is incomplete or drifts."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_manifest_sha256(manifest: Mapping[str, Any]) -> str:
    payload = dict(manifest)
    payload.pop("canonical_manifest_sha256", None)
    return canonical_sha256(payload)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SourceManifestError(f"cannot load JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise SourceManifestError(f"JSON root must be an object: {path}")
    return payload


def _canonical_day(value: Any, *, label: str) -> str:
    raw = str(value)
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise SourceManifestError(f"{label} is not an ISO UTC date: {raw!r}") from exc
    if parsed.isoformat() != raw:
        raise SourceManifestError(f"{label} is not canonical YYYY-MM-DD: {raw!r}")
    return raw


def _previous_day(day: str) -> str:
    return (date.fromisoformat(day) - timedelta(days=1)).isoformat()


def _require_file(path: Path, *, label: str) -> Path:
    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise SourceManifestError(f"{label} is missing: {resolved}")
    return resolved


def _require_hash(value: Any, *, label: str) -> str:
    digest = str(value or "").lower()
    if _SHA_RE.fullmatch(digest) is None:
        raise SourceManifestError(f"{label} has no valid SHA256")
    return digest


def _read_exact_day_csv(path: Path) -> tuple[str, ...]:
    with _require_file(path, label="provider replay-day CSV").open(
        newline="", encoding="utf-8"
    ) as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["day"]:
            raise SourceManifestError("provider replay-day CSV schema drifted")
        days = tuple(_canonical_day(row["day"], label="provider day") for row in reader)
    if not days or list(days) != sorted(days) or len(days) != len(set(days)):
        raise SourceManifestError("provider days must be nonempty, unique, and chronological")
    if any(not day.startswith("2025-") for day in days):
        raise SourceManifestError("v2 outcome-blind provider panel must contain only 2025")
    return days


def _read_csv_header(path: Path) -> tuple[str, ...]:
    with path.open(newline="", encoding="utf-8") as handle:
        return tuple(next(csv.reader(handle), ()))


def _resolve_csv(root: Path, stem: str, *, label: str) -> Path:
    candidates = (root / f"{stem}.csv", root / f"{stem}.csv.gz")
    found = [path.expanduser().resolve() for path in candidates if path.is_file()]
    if len(found) != 1:
        raise SourceManifestError(
            f"{label} requires exactly one .csv/.csv.gz source: {found or candidates}"
        )
    return found[0]


def _csv_header(path: Path) -> tuple[str, ...]:
    if path.suffix != ".gz":
        return _read_csv_header(path)
    import gzip

    with gzip.open(path, "rt", newline="", encoding="utf-8") as handle:
        return tuple(next(csv.reader(handle), ()))


def _file_identity(
    path: Path,
    *,
    authority: str,
    clock: str,
    expected_sha256: str | None = None,
    expected_header: Sequence[str] | None = None,
) -> dict[str, Any]:
    required = _require_file(path, label=authority)
    observed = sha256_file(required)
    if expected_sha256 is not None and observed != expected_sha256:
        raise SourceManifestError(
            f"{authority} hash drifted for {required}: {observed} != {expected_sha256}"
        )
    if expected_header is not None:
        header = _csv_header(required)
        if tuple(expected_header) != header:
            raise SourceManifestError(
                f"{authority} schema drifted for {required}: {header}"
            )
    return {
        "path": str(required),
        "sha256": observed,
        "size_bytes": required.stat().st_size,
        "authority": authority,
        "source_clock": clock,
    }


def _source_index(union_manifest: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = union_manifest.get("source_files")
    if not isinstance(rows, list) or not rows:
        raise SourceManifestError("union source_files is empty")
    output: dict[str, dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise SourceManifestError("union source_files row is not an object")
        day = _canonical_day(raw.get("day"), label="union source day")
        if day in output:
            raise SourceManifestError(f"duplicate union source day: {day}")
        output[day] = dict(raw)
    return output


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def build_manifest(
    *,
    union_root: Path = DEFAULT_UNION_ROOT,
    aggtrades_root: Path = DEFAULT_DATA_ROOT / "raw",
    individual_trades_root: Path = DEFAULT_DATA_ROOT / "raw_trades" / SYMBOL,
) -> dict[str, Any]:
    union = union_root.expanduser().resolve()
    union_manifest_path = _require_file(union / "manifest.json", label="union manifest")
    union_manifest = _load_json(union_manifest_path)
    if union_manifest.get("dataset_version") != "normalized_l2_research_union_v1":
        raise SourceManifestError("normalized L2 union identity drifted")
    if union_manifest.get("symbol") != SYMBOL:
        raise SourceManifestError("normalized L2 union symbol drifted")

    replay_entry = (union_manifest.get("outputs") or {}).get("provider_replay_days.csv")
    if not isinstance(replay_entry, Mapping):
        raise SourceManifestError("union manifest lacks provider replay-day identity")
    replay_path = _require_file(Path(str(replay_entry.get("path"))), label="provider days")
    if sha256_file(replay_path) != _require_hash(
        replay_entry.get("sha256"), label="provider replay-day CSV"
    ):
        raise SourceManifestError("provider replay-day CSV hash drifted")
    target_days = _read_exact_day_csv(replay_path)
    source_index = _source_index(union_manifest)
    required_days = tuple(
        sorted({day for target in target_days for day in (_previous_day(target), target)})
    )

    source_days: list[dict[str, Any]] = []
    for day in required_days:
        source = source_index.get(day)
        if source is None or source.get("source_authority") != SOURCE_AUTHORITY:
            raise SourceManifestError(f"provider source identity missing for {day}")
        bbo = union / "bbo" / f"{SYMBOL}-bbo-{day}.parquet"
        l2 = union / "l2" / f"{SYMBOL}-l2-{day}.parquet"
        agg = _resolve_csv(
            aggtrades_root,
            f"{SYMBOL}-aggTrades-{day}",
            label=f"official aggTrades {day}",
        )
        individual = _resolve_csv(
            individual_trades_root,
            f"{SYMBOL}-trades-{day}",
            label=f"official individual trades {day}",
        )
        source_days.append(
            {
                "day": day,
                "bbo": _file_identity(
                    bbo,
                    authority=SOURCE_AUTHORITY,
                    clock="provider_local_receive_time_right_boundary_100ms",
                    expected_sha256=_require_hash(
                        source.get("bbo_sha256"), label=f"BBO {day}"
                    ),
                ),
                "l2": _file_identity(
                    l2,
                    authority=SOURCE_AUTHORITY,
                    clock="provider_local_receive_time_right_boundary_100ms",
                    expected_sha256=_require_hash(
                        source.get("l2_sha256"), label=f"L2 {day}"
                    ),
                ),
                "aggtrades": _file_identity(
                    agg,
                    authority=TRADE_AUTHORITY,
                    clock="exchange_trade_time",
                    expected_header=_AGGTRADE_HEADER,
                ),
                "individual_trades": _file_identity(
                    individual,
                    authority=TRADE_AUTHORITY,
                    clock="exchange_trade_time",
                    expected_header=_INDIVIDUAL_TRADE_HEADER,
                ),
            }
        )

    source_day_set = {row["day"] for row in source_days}
    target_windows = []
    for day in target_days:
        prior = _previous_day(day)
        if {prior, day} - source_day_set:
            raise SourceManifestError(f"D-1 source intersection is incomplete for {day}")
        target_windows.append(
            {
                "target_day": day,
                "warmup_day": prior,
                "warmup_duration_hours": 24,
                "target_source_authority": SOURCE_AUTHORITY,
            }
        )

    manifest: dict[str, Any] = {
        "identity": IDENTITY,
        "schema_version": SCHEMA_VERSION,
        "symbol": SYMBOL,
        "purpose": "outcome_blind_feature_scale_missingness_and_predicate_support",
        "source_union": {
            "path": str(union_manifest_path),
            "sha256": sha256_file(union_manifest_path),
            "dataset_version": union_manifest["dataset_version"],
        },
        "provider_replay_days": {
            "path": str(replay_path),
            "sha256": sha256_file(replay_path),
        },
        "target_days": list(target_days),
        "target_day_count": len(target_days),
        "unique_source_day_count": len(source_days),
        "target_windows": target_windows,
        "source_days": source_days,
        "clock_contract": {
            "bbo_l2_clock": "provider_local_receive_time_right_boundary_100ms",
            "trade_clock": "binance_exchange_trade_time",
            "exact_historical_receive_time_present": False,
            "feature_ready_clock": "provider_local_receive_bucket_right_boundary",
            "book_trade_joint_visibility_authority": False,
            "trade_derived_M2_action_grade_support": False,
            "trade_derived_M2_role": "exchange_time_channel_diagnostic_only",
            "live_transport_authority": False,
        },
        "permission_boundary": {
            "economic_outcomes_read": False,
            "queue_or_lifecycle_authority": False,
            "exact_queue_policy_eligible": False,
            "action_authorized": False,
            "live_authorized": False,
            "allowed_uses": [
                "outcome_blind_feature_scaling",
                "outcome_blind_missingness_support",
                "outcome_blind_predicate_candidate_freeze",
                "per_channel_clock_separated_support_only",
            ],
        },
    }
    manifest["canonical_manifest_sha256"] = canonical_manifest_sha256(manifest)
    return manifest


def validate_manifest(manifest: Mapping[str, Any], *, rehash_sources: bool = True) -> None:
    if manifest.get("identity") != IDENTITY or manifest.get("schema_version") != SCHEMA_VERSION:
        raise SourceManifestError("source manifest identity drifted")
    if manifest.get("canonical_manifest_sha256") != canonical_manifest_sha256(manifest):
        raise SourceManifestError("canonical source manifest hash drifted")
    permissions = manifest.get("permission_boundary") or {}
    forbidden_true = (
        "economic_outcomes_read",
        "queue_or_lifecycle_authority",
        "exact_queue_policy_eligible",
        "action_authorized",
        "live_authorized",
    )
    if any(permissions.get(name) is not False for name in forbidden_true):
        raise SourceManifestError("source manifest permission boundary drifted")
    clocks = manifest.get("clock_contract") or {}
    if clocks.get("bbo_l2_clock") != (
        "provider_local_receive_time_right_boundary_100ms"
    ):
        raise SourceManifestError("provider BBO/L2 clock identity drifted")
    if clocks.get("trade_clock") != "binance_exchange_trade_time":
        raise SourceManifestError("official trade clock identity drifted")
    for field in (
        "exact_historical_receive_time_present",
        "book_trade_joint_visibility_authority",
        "trade_derived_M2_action_grade_support",
        "live_transport_authority",
    ):
        if clocks.get(field) is not False:
            raise SourceManifestError(f"source clock authority overstated: {field}")
    targets = tuple(manifest.get("target_days") or ())
    if not targets or len(targets) != len(set(targets)) or list(targets) != sorted(targets):
        raise SourceManifestError("target days drifted")
    source_rows = manifest.get("source_days")
    if not isinstance(source_rows, list) or not source_rows:
        raise SourceManifestError("source-day records are missing")
    by_day = {row.get("day"): row for row in source_rows if isinstance(row, Mapping)}
    if len(by_day) != len(source_rows):
        raise SourceManifestError("source-day identities are duplicate or malformed")
    for target in targets:
        if target not in by_day or _previous_day(target) not in by_day:
            raise SourceManifestError(f"D-1 source support drifted for {target}")
    if not rehash_sources:
        return
    for day, row in by_day.items():
        _canonical_day(day, label="source record day")
        for key in ("bbo", "l2", "aggtrades", "individual_trades"):
            identity = row.get(key)
            if not isinstance(identity, Mapping):
                raise SourceManifestError(f"{day} lacks {key} identity")
            path = _require_file(Path(str(identity.get("path"))), label=f"{day} {key}")
            observed = sha256_file(path)
            expected = _require_hash(identity.get("sha256"), label=f"{day} {key}")
            if observed != expected:
                raise SourceManifestError(f"{day} {key} hash drifted")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--union-root", type=Path, default=DEFAULT_UNION_ROOT)
    build.add_argument("--aggtrades-root", type=Path, default=DEFAULT_DATA_ROOT / "raw")
    build.add_argument(
        "--individual-trades-root",
        type=Path,
        default=DEFAULT_DATA_ROOT / "raw_trades" / SYMBOL,
    )
    build.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    validate = subparsers.add_parser("validate")
    validate.add_argument("manifest", type=Path)
    validate.add_argument("--no-rehash", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "build":
        manifest = build_manifest(
            union_root=args.union_root,
            aggtrades_root=args.aggtrades_root,
            individual_trades_root=args.individual_trades_root,
        )
        validate_manifest(manifest, rehash_sources=False)
        _atomic_write_json(args.output.expanduser().resolve(), manifest)
        print(json.dumps({
            "output": str(args.output.expanduser().resolve()),
            "target_day_count": manifest["target_day_count"],
            "unique_source_day_count": manifest["unique_source_day_count"],
            "canonical_manifest_sha256": manifest["canonical_manifest_sha256"],
        }, sort_keys=True))
        return
    manifest = _load_json(args.manifest.expanduser().resolve())
    validate_manifest(manifest, rehash_sources=not args.no_rehash)
    print(json.dumps({
        "manifest": str(args.manifest.expanduser().resolve()),
        "valid": True,
        "rehash_sources": not args.no_rehash,
        "canonical_manifest_sha256": manifest["canonical_manifest_sha256"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
