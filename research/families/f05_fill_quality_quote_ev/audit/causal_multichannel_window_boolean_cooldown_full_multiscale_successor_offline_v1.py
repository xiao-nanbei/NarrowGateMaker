#!/usr/bin/env python3
"""Freeze the outcome-blind historical source gate for the F05 successor.

This module deliberately stops before opportunity construction, labels, PnL, or
model fitting.  It binds the family-specific unconsumed day universe to D-1/D/D+1
raw book, normalized book, public trade, and native sequence evidence.  A target
day is admitted only when every source is complete and internally consistent.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow.parquet as pq

from data_paths import data_root, marketdata_root

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_v1"
)
SCHEMA_VERSION = f"{IDENTITY}.canonical_source_manifest.v1"
DAY_RECEIPT_SCHEMA = f"{IDENTITY}.day_source_receipt.v1"
FOLD_SCHEMA = f"{IDENTITY}.four_by_three_chronological_folds.v1"
NESTED_FOLD_SCHEMA = f"{IDENTITY}.bound_four_by_three_chronological_folds.v1"
PANEL_ROLE = "family_specific_unconsumed_historical_development"
QUEUE_IDENTITY = "modeled_queue_with_same_millisecond_ambiguity_censoring"
SYMBOL = "BTCUSDC"
REQUIRED_DAYS = 30
ACTIVE_OWNER_POLICY_SHA256 = (
    "877a20033ff678bd7aa9b58069f37c3dc459b18db78c316b7e50023248f15a29"
)
ACTIVE_PREDICATE_BUNDLE_SHA256 = (
    "ba4c1bac2380564aa24d47d12796f3be5c0312cc88d28218ce84bd20e4170f37"
)
ACTIVE_PRIVATE_CONFIG_SHA256 = (
    "800f4c025663ce6b54cfcf16d02ce510ccaf52545332ca4c19b1fbdf37f0cf85"
)
RAW_HOURS = tuple(f"{hour:02d}" for hour in range(24))

CONSUMED_TARGET_DAYS = (
    "2026-06-29",
    "2026-07-03",
    "2026-07-04",
    "2026-07-05",
    "2026-07-06",
    "2026-07-07",
    "2026-07-08",
    "2026-07-09",
    "2026-07-10",
    "2026-07-16",
)
PRIMARY_TARGET_DAYS = (
    "2026-06-27",
    "2026-06-28",
    "2026-06-30",
    "2026-07-01",
    "2026-07-02",
    "2026-07-11",
    "2026-07-12",
    "2026-07-13",
    "2026-07-14",
    "2026-07-15",
    "2026-07-17",
    "2026-07-18",
    "2026-07-19",
    "2026-07-20",
    "2026-07-21",
    "2026-07-22",
    "2026-07-23",
    "2026-07-24",
    "2026-07-25",
    "2026-07-26",
    "2026-07-27",
    "2026-07-28",
    "2026-07-29",
    "2026-07-30",
    "2026-07-31",
    "2026-08-01",
    "2026-08-02",
    "2026-08-03",
    "2026-08-04",
    "2026-08-05",
)
BACKUP_TARGET_DAYS = tuple(f"2026-08-{day:02d}" for day in range(6, 12))
CANDIDATE_TARGET_DAYS = (*PRIMARY_TARGET_DAYS, *BACKUP_TARGET_DAYS)

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_ECONOMIC_PARTS = (
    "pnl",
    "profit",
    "reward",
    "terminal_value",
    "closed_campaign_value",
    "economic_outcome",
)
_BBO_COLUMNS = (
    "timestamp",
    "best_bid",
    "best_bid_qty",
    "best_ask",
    "best_ask_qty",
)
_L2_COLUMNS = tuple(
    ["timestamp"]
    + [
        name
        for level in range(1, 21)
        for name in (
            f"bid_px_{level}",
            f"bid_qty_{level}",
            f"ask_px_{level}",
            f"ask_qty_{level}",
        )
    ]
)
_AGG_COLUMNS = (
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
)
_TRADE_COLUMNS = (
    "id",
    "price",
    "qty",
    "quote_qty",
    "time",
    "is_buyer_maker",
)


class OfflineSourceGateError(RuntimeError):
    """Raised when a canonical historical source or identity drifts."""


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def canonical_document_sha256(value: Mapping[str, Any], field: str) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return canonical_sha256(payload)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _day(value: Any) -> str:
    raw = str(value)
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise OfflineSourceGateError(f"invalid UTC day: {raw!r}") from exc
    if parsed.isoformat() != raw:
        raise OfflineSourceGateError(f"noncanonical UTC day: {raw!r}")
    return raw


def _offset(day: str, amount: int) -> str:
    return (date.fromisoformat(day) + timedelta(days=amount)).isoformat()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflineSourceGateError(f"cannot load JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise OfflineSourceGateError(f"JSON root must be an object: {path}")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="ascii",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = handle.name
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)


def _csv_header(path: Path) -> tuple[str, ...]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", newline="", encoding="utf-8") as handle:
        return tuple(next(csv.reader(handle), ()))


def _resolve_csv(root: Path, stem: str) -> Path | None:
    found = [path for path in (root / f"{stem}.csv", root / f"{stem}.csv.gz") if path.is_file()]
    return found[0].resolve() if len(found) == 1 else None


def _portable_path(path: Path, *, project_data: Path, market_data: Path) -> str:
    resolved = path.resolve()
    for root, marker in (
        (project_data.resolve(), "${NARROWGATE_DATA_ROOT}"),
        (market_data.resolve(), "${NARROWGATE_MARKETDATA_ROOT}"),
    ):
        try:
            relative = resolved.relative_to(root)
        except ValueError:
            continue
        return marker if not relative.parts else f"{marker}/{relative.as_posix()}"
    raise OfflineSourceGateError(f"source lies outside portable data roots: {resolved}")


@dataclass(frozen=True, slots=True)
class OfflineSourceLayout:
    project_data_root: Path
    marketdata_root: Path
    raw_orderbook_root: Path
    normalized_roots: tuple[Path, ...]
    aggtrades_root: Path
    individual_trades_root: Path
    sequence_audit_paths: tuple[Path, ...]

    def __post_init__(self) -> None:
        if not self.normalized_roots:
            raise OfflineSourceGateError("at least one normalized root is required")


def default_layout() -> OfflineSourceLayout:
    project = data_root(Path(__file__).resolve().parents[4])
    market = marketdata_root()
    normalized_names = (
        "normalized_l2_100ms_v2",
        "normalized_l2_100ms_v2_20260727",
        "normalized_l2_postfit_20260726_31_100ms_v1",
        "normalized_l2_incremental_20260801_09_100ms_v1",
        "normalized_l2_f05_offline_20260802_05_100ms_v1",
        "normalized_l2_live_compare_20260806_100ms_v1",
        "normalized_l2_live_compare_20260807_100ms_v1",
    )
    audit_paths = (
        project / "reports/minimal_good_day_reaudit_20260727/calendar206_native_sequence.json",
        project / "reports/marketdata_update_20260802/cryptohft_btcusdc_20260726_31_sequence.json",
        project / "reports/f05_full_multiscale_offline_source_audit_v1/normalized_20260801_sequence.json",
        project / "reports/f05_full_multiscale_offline_source_audit_v1/normalized_20260802_05_sequence.json",
        project / "reports/marketdata_update_20260810/cryptohft_btcusdc_20260806_live_compare_sequence.json",
        project / "reports/marketdata_update_20260810/cryptohft_btcusdc_20260807_live_compare_sequence.json",
    )
    return OfflineSourceLayout(
        project_data_root=project,
        marketdata_root=market,
        raw_orderbook_root=market / "cryptohftdata/binance_futures",
        normalized_roots=tuple(project / name for name in normalized_names),
        aggtrades_root=project / "raw",
        individual_trades_root=project / "raw_trades" / SYMBOL,
        sequence_audit_paths=audit_paths,
    )


def _identity(path: Path, sha256: str, *, layout: OfflineSourceLayout) -> dict[str, Any]:
    return {
        "path": _portable_path(
            path,
            project_data=layout.project_data_root,
            market_data=layout.marketdata_root,
        ),
        "sha256": sha256,
        "size_bytes": path.stat().st_size,
    }


def _extract_sequence_days(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    output: dict[str, Mapping[str, Any]] = {}
    direct = payload.get("day_audits")
    if isinstance(direct, Mapping):
        for raw_day, row in direct.items():
            if isinstance(row, Mapping):
                output[_day(raw_day)] = row
    ranges = payload.get("range_audits")
    if isinstance(ranges, list):
        for raw_range in ranges:
            if not isinstance(raw_range, Mapping):
                continue
            sequence = raw_range.get("sequence_audit")
            if not isinstance(sequence, Mapping):
                continue
            rows = sequence.get("day_sequence_audits")
            if not isinstance(rows, Mapping):
                continue
            for raw_day, row in rows.items():
                if isinstance(row, Mapping):
                    output[_day(raw_day)] = row
    return output


def _sequence_valid(row: Mapping[str, Any]) -> bool:
    return bool(
        row.get("eligible", True) is True
        and row.get("target_initialized_at_start") is True
        and row.get("target_initialization_source_at_start") == "snapshot"
        and int(row.get("target_accepted_updates", 0)) > 0
        and int(row.get("target_sequence_gaps", -1)) == 0
        and int(row.get("target_invalid_sequence_messages", -1)) == 0
        and int(row.get("target_message_time_reversals", -1)) == 0
        and int(row.get("target_duplicate_messages", -1)) == 0
        and int(row.get("target_stale_updates", -1)) == 0
    )


def _sequence_index(layout: OfflineSourceLayout) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {}
    for path in layout.sequence_audit_paths:
        if not path.is_file():
            continue
        payload = _load_json(path)
        if payload.get("timestamp_source") not in (None, "transaction"):
            continue
        if payload.get("snapshot_ms") not in (None, 100):
            continue
        for day, row in _extract_sequence_days(payload).items():
            if not _sequence_valid(row):
                continue
            output.setdefault(day, []).append(
                {
                    "audit_path": _portable_path(
                        path,
                        project_data=layout.project_data_root,
                        market_data=layout.marketdata_root,
                    ),
                    "audit_sha256": file_sha256(path),
                    "target_accepted_updates": int(row["target_accepted_updates"]),
                    "snapshot_seeded": True,
                    "sequence_gaps": 0,
                    "invalid_sequence_messages": 0,
                    "time_reversals": 0,
                }
            )
    for records in output.values():
        records.sort(key=lambda row: (row["audit_path"], row["audit_sha256"]))
    return output


def _normalized_pair(day: str, layout: OfflineSourceLayout) -> tuple[Path, Path] | None:
    found: list[tuple[Path, Path]] = []
    for root in layout.normalized_roots:
        bbo = root / "bbo" / f"{SYMBOL}-bbo-{day}.parquet"
        l2 = root / "l2" / f"{SYMBOL}-l2-{day}.parquet"
        if bbo.is_file() and l2.is_file():
            found.append((bbo.resolve(), l2.resolve()))
        elif bbo.exists() or l2.exists():
            raise OfflineSourceGateError(f"normalized BBO/L2 pair is incomplete for {day}: {root}")
    return found[0] if found else None


def _parquet_clock_audit(bbo: Path, l2: Path, day: str) -> dict[str, Any]:
    bbo_file = pq.ParquetFile(bbo)
    l2_file = pq.ParquetFile(l2)
    if tuple(bbo_file.schema_arrow.names) != _BBO_COLUMNS:
        raise OfflineSourceGateError(f"BBO schema drifted for {day}")
    if tuple(l2_file.schema_arrow.names) != _L2_COLUMNS:
        raise OfflineSourceGateError(f"L2 schema drifted for {day}")
    if bbo_file.metadata.num_rows != l2_file.metadata.num_rows or bbo_file.metadata.num_rows <= 0:
        raise OfflineSourceGateError(f"BBO/L2 row denominator drifted for {day}")
    bbo_ts = pq.read_table(bbo, columns=["timestamp"]).column(0).to_numpy()
    l2_ts = pq.read_table(l2, columns=["timestamp"]).column(0).to_numpy()
    if not np.array_equal(bbo_ts, l2_ts):
        raise OfflineSourceGateError(f"BBO/L2 timestamps differ for {day}")
    if bbo_ts.size == 0 or np.any(np.diff(bbo_ts) <= 0):
        raise OfflineSourceGateError(f"normalized timestamps are not strictly increasing for {day}")
    start_ms = int(np.datetime64(day, "ms").astype(np.int64))
    end_ms = start_ms + 86_400_000
    if int(bbo_ts[0]) < start_ms or int(bbo_ts[-1]) >= end_ms:
        raise OfflineSourceGateError(f"normalized timestamps escape UTC day {day}")
    gaps = np.diff(bbo_ts)
    return {
        "rows": int(bbo_ts.size),
        "first_timestamp_ms": int(bbo_ts[0]),
        "last_timestamp_ms": int(bbo_ts[-1]),
        "strictly_increasing": True,
        "bbo_l2_timestamps_equal": True,
        "maximum_visible_gap_ms": int(gaps.max(initial=0)),
        "timestamp_source": "transaction",
        "snapshot_grid_ms": 100,
        "right_boundary_semantics": "causal_latest_visible_state_per_100ms_bucket",
    }


def _trade_clock_audit(path: Path, *, kind: str, day: str) -> dict[str, Any]:
    columns = _AGG_COLUMNS if kind == "aggtrades" else _TRADE_COLUMNS
    if _csv_header(path) != columns:
        raise OfflineSourceGateError(f"{kind} schema drifted for {day}")
    id_column = "agg_trade_id" if kind == "aggtrades" else "id"
    time_column = "transact_time" if kind == "aggtrades" else "time"
    first_id: int | None = None
    last_id: int | None = None
    first_time: int | None = None
    last_time: int | None = None
    first_constituent_trade_id: int | None = None
    last_constituent_trade_id: int | None = None
    rows = 0
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            current_id = int(row[id_column])
            current_time = int(row[time_column])
            if last_id is not None and current_id <= last_id:
                raise OfflineSourceGateError(f"{kind} IDs are not increasing for {day}")
            if last_time is not None and current_time < last_time:
                raise OfflineSourceGateError(f"{kind} clock reverses for {day}")
            if first_id is None:
                first_id, first_time = current_id, current_time
                if kind == "aggtrades":
                    first_constituent_trade_id = int(row["first_trade_id"])
            last_id, last_time = current_id, current_time
            if kind == "aggtrades":
                last_constituent_trade_id = int(row["last_trade_id"])
            rows += 1
    if rows == 0 or first_time is None or last_time is None:
        raise OfflineSourceGateError(f"{kind} has no rows for {day}")
    start_ms = int(np.datetime64(day, "ms").astype(np.int64))
    if first_time < start_ms or last_time >= start_ms + 86_400_000:
        raise OfflineSourceGateError(f"{kind} clock escapes UTC day {day}")
    return {
        "rows": rows,
        "first_id": first_id,
        "last_id": last_id,
        "first_timestamp_ms": first_time,
        "last_timestamp_ms": last_time,
        "id_strictly_increasing": True,
        "clock_nondecreasing": True,
        "clock": "exchange_trade_time",
        "first_constituent_trade_id": first_constituent_trade_id,
        "last_constituent_trade_id": last_constituent_trade_id,
    }


def _source_day_receipt(
    day: str,
    *,
    layout: OfflineSourceLayout,
    sequence_index: Mapping[str, Sequence[Mapping[str, Any]]],
    workers: int,
) -> tuple[dict[str, Any] | None, tuple[str, ...]]:
    reasons: list[str] = []
    raw_paths = tuple(
        layout.raw_orderbook_root / day / hour / f"{SYMBOL}_orderbook.parquet.zst"
        for hour in RAW_HOURS
    )
    missing_hours = [hour for hour, path in zip(RAW_HOURS, raw_paths, strict=True) if not path.is_file()]
    if missing_hours:
        reasons.append("raw_orderbook_missing_hours:" + ",".join(missing_hours))
    pair = _normalized_pair(day, layout)
    if pair is None:
        reasons.append("normalized_bbo_l2_missing")
    agg = _resolve_csv(layout.aggtrades_root, f"{SYMBOL}-aggTrades-{day}")
    individual = _resolve_csv(layout.individual_trades_root, f"{SYMBOL}-trades-{day}")
    if agg is None:
        reasons.append("official_aggtrades_missing_or_ambiguous")
    if individual is None:
        reasons.append("official_individual_trades_missing_or_ambiguous")
    sequence = tuple(sequence_index.get(day, ()))
    if not sequence:
        reasons.append("strict_native_sequence_audit_missing_or_invalid")
    if reasons:
        return None, tuple(reasons)

    assert pair is not None and agg is not None and individual is not None
    hash_paths = (*raw_paths, pair[0], pair[1], agg, individual)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        hashes = tuple(executor.map(file_sha256, hash_paths))
    hash_by_path = dict(zip(hash_paths, hashes, strict=True))
    normalized_audit = _parquet_clock_audit(pair[0], pair[1], day)
    agg_audit = _trade_clock_audit(agg, kind="aggtrades", day=day)
    individual_audit = _trade_clock_audit(individual, kind="individual_trades", day=day)
    if (
        agg_audit["first_constituent_trade_id"] != individual_audit["first_id"]
        or agg_audit["last_constituent_trade_id"] != individual_audit["last_id"]
    ):
        reasons.append("aggtrade_individual_trade_id_coverage_differs")
    if reasons:
        return None, tuple(reasons)
    receipt: dict[str, Any] = {
        "schema_version": f"{IDENTITY}.source_day.v1",
        "source_day": day,
        "raw_orderbook": {
            "hour_count": 24,
            "hours": [
                {
                    "hour": hour,
                    **_identity(path, hash_by_path[path], layout=layout),
                }
                for hour, path in zip(RAW_HOURS, raw_paths, strict=True)
            ],
        },
        "normalized": {
            "bbo": _identity(pair[0], hash_by_path[pair[0]], layout=layout),
            "l2": _identity(pair[1], hash_by_path[pair[1]], layout=layout),
            "clock_audit": normalized_audit,
        },
        "aggtrades": {
            **_identity(agg, hash_by_path[agg], layout=layout),
            "clock_audit": agg_audit,
        },
        "individual_trades": {
            **_identity(individual, hash_by_path[individual], layout=layout),
            "clock_audit": individual_audit,
        },
        "trade_source_alignment": {
            "constituent_id_coverage_equal": True,
            "first_timestamp_delta_ms": int(
                agg_audit["first_timestamp_ms"]
                - individual_audit["first_timestamp_ms"]
            ),
            "last_timestamp_delta_ms": int(
                agg_audit["last_timestamp_ms"]
                - individual_audit["last_timestamp_ms"]
            ),
            "timestamp_equality_required": False,
        },
        "sequence_evidence": sequence,
        "joint_book_trade_ordering": {
            "common_sequence_present": False,
            "same_millisecond_ambiguity_policy": "censor",
        },
        "permissions": {
            "economic_outcomes_read": False,
            "labels_generated": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    receipt["source_day_receipt_sha256"] = canonical_document_sha256(
        receipt, "source_day_receipt_sha256"
    )
    return receipt, ()


def _fold_manifest(days: Sequence[str], *, selection_sha256: str) -> dict[str, Any]:
    if len(days) != REQUIRED_DAYS:
        raise OfflineSourceGateError("four-by-three folds require exactly 30 admitted days")
    folds = []
    for index, train_count in enumerate((10, 15, 20, 25), start=1):
        test = tuple(days[train_count : train_count + 5])
        folds.append(
            {
                "fold": index,
                "train_days": list(days[:train_count]),
                "test_days": list(test),
                "purge_and_washout_required": True,
            }
        )
    manifest: dict[str, Any] = {
        "schema_version": FOLD_SCHEMA,
        "panel_role": PANEL_ROLE,
        "selection_sha256": selection_sha256,
        "active_days": list(days),
        "outer_folds": folds,
    }
    manifest["fold_manifest_sha256"] = canonical_document_sha256(
        manifest, "fold_manifest_sha256"
    )
    return manifest


def derive_bound_nested_fold_manifest(
    source_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Derive and hash the complete 4x3 fold contract before economics.

    The source admission freezes the four expanding outer folds. The three
    chronological inner folds for each outer train are a deterministic
    refinement of those bytes. Formal binding persists this refinement and its
    hash so the backend cannot choose a different inner split at runtime.
    """

    days = tuple(str(day) for day in source_manifest.get("selected_days", ()))
    if len(days) != REQUIRED_DAYS or tuple(sorted(set(days))) != days:
        raise OfflineSourceGateError(
            "bound four-by-three folds require the frozen 30 chronological days"
        )
    source_folds = source_manifest.get("fold_manifest")
    if not isinstance(source_folds, Mapping):
        raise OfflineSourceGateError("source manifest lacks its outer-fold contract")
    source_fold_sha = str(source_folds.get("fold_manifest_sha256", ""))
    if source_fold_sha != canonical_document_sha256(
        source_folds, "fold_manifest_sha256"
    ):
        raise OfflineSourceGateError("source outer-fold manifest hash drifted")
    if tuple(source_folds.get("active_days", ())) != days:
        raise OfflineSourceGateError("source outer-fold day order drifted")
    source_outer = source_folds.get("outer_folds")
    if not isinstance(source_outer, list) or len(source_outer) != 4:
        raise OfflineSourceGateError("source outer-fold census drifted")

    expected_ranges = ((10, 15), (15, 20), (20, 25), (25, 30))
    bound_outer: list[dict[str, Any]] = []
    for position, ((train_end, test_end), row) in enumerate(
        zip(expected_ranges, source_outer, strict=True), start=1
    ):
        if not isinstance(row, Mapping):
            raise OfflineSourceGateError("source outer-fold row is malformed")
        train = days[:train_end]
        test = days[train_end:test_end]
        if (
            tuple(row.get("train_days", ())) != train
            or tuple(row.get("test_days", ())) != test
            or row.get("purge_and_washout_required") is not True
        ):
            raise OfflineSourceGateError("source outer-fold boundaries drifted")

        positions = np.arange(5, len(train), dtype=np.int64)
        inner_folds: list[dict[str, Any]] = []
        for inner_position, test_positions in enumerate(
            np.array_split(positions, 3), start=1
        ):
            if len(test_positions) == 0:
                raise OfflineSourceGateError("derived inner fold is empty")
            start = int(test_positions[0])
            inner_folds.append(
                {
                    "fold_id": f"outer{position}.inner{inner_position}",
                    "train_days": list(train[:start]),
                    "test_days": [train[int(value)] for value in test_positions],
                    "purge_and_washout_required": True,
                }
            )
        bound_outer.append(
            {
                "fold_id": f"outer{position}",
                "train_days": list(train),
                "test_days": list(test),
                "purge_and_washout_required": True,
                "inner_folds": inner_folds,
            }
        )

    manifest: dict[str, Any] = {
        "schema_version": NESTED_FOLD_SCHEMA,
        "panel_role": PANEL_ROLE,
        "source_fold_manifest_sha256": source_fold_sha,
        "active_days": list(days),
        "outer_folds": bound_outer,
    }
    manifest["nested_fold_manifest_sha256"] = canonical_document_sha256(
        manifest, "nested_fold_manifest_sha256"
    )
    return manifest


def audit_historical_sources(
    *,
    layout: OfflineSourceLayout,
    output_dir: Path,
    workers: int = 4,
) -> dict[str, Any]:
    """Audit sources and atomically publish day receipts without reading outcomes."""

    if set(CONSUMED_TARGET_DAYS) & set(CANDIDATE_TARGET_DAYS):
        raise OfflineSourceGateError("consumed and candidate target days overlap")
    if tuple(sorted(CANDIDATE_TARGET_DAYS)) != CANDIDATE_TARGET_DAYS:
        raise OfflineSourceGateError("candidate replacement order is not chronological")
    output = output_dir.expanduser().resolve()
    receipts_dir = output / "day_receipts"
    sequence = _sequence_index(layout)
    required_source_days = tuple(
        sorted(
            {
                _offset(target, delta)
                for target in CANDIDATE_TARGET_DAYS
                for delta in (-1, 0, 1)
            }
        )
    )
    source_receipts: dict[str, dict[str, Any]] = {}
    source_receipt_files: dict[str, dict[str, Any]] = {}
    source_failures: dict[str, tuple[str, ...]] = {}
    for source_day in required_source_days:
        try:
            receipt, reasons = _source_day_receipt(
                source_day,
                layout=layout,
                sequence_index=sequence,
                workers=workers,
            )
        except (OfflineSourceGateError, OSError, ValueError) as exc:
            receipt, reasons = None, (f"source_audit_error:{type(exc).__name__}:{exc}",)
        if receipt is None:
            source_failures[source_day] = reasons
            continue
        source_receipts[source_day] = receipt
        receipt_path = receipts_dir / f"source-{source_day}.json"
        _atomic_json(receipt_path, receipt)
        source_receipt_files[source_day] = {
            "path": _portable_path(
                receipt_path,
                project_data=layout.project_data_root,
                market_data=layout.marketdata_root,
            ),
            "sha256": file_sha256(receipt_path),
            "canonical_sha256": receipt["source_day_receipt_sha256"],
        }

    target_receipts: list[dict[str, Any]] = []
    target_receipt_files: dict[str, dict[str, Any]] = {}
    selected: list[str] = []
    for target in CANDIDATE_TARGET_DAYS:
        context = (_offset(target, -1), target, _offset(target, 1))
        reasons = tuple(
            f"{day}:{reason}"
            for day in context
            for reason in source_failures.get(day, ())
        )
        eligible = not reasons
        if eligible and len(selected) < REQUIRED_DAYS:
            selected.append(target)
        receipt: dict[str, Any] = {
            "schema_version": DAY_RECEIPT_SCHEMA,
            "utc_day": target,
            "panel_role": PANEL_ROLE,
            "candidate_position": CANDIDATE_TARGET_DAYS.index(target) + 1,
            "primary_candidate": target in PRIMARY_TARGET_DAYS,
            "context_days": {
                "D_minus_1": context[0],
                "D": context[1],
                "D_plus_1": context[2],
            },
            "source_day_receipt_sha256": {
                day: source_receipts[day]["source_day_receipt_sha256"]
                for day in context
                if day in source_receipts
            },
            "source_gate_eligible": eligible,
            "exclusion_reasons": list(reasons),
            "economic_outcomes_read": False,
        }
        receipt["day_receipt_sha256"] = canonical_document_sha256(
            receipt, "day_receipt_sha256"
        )
        target_receipts.append(receipt)
        receipt_path = receipts_dir / f"target-{target}.json"
        _atomic_json(receipt_path, receipt)
        target_receipt_files[target] = {
            "path": _portable_path(
                receipt_path,
                project_data=layout.project_data_root,
                market_data=layout.marketdata_root,
            ),
            "sha256": file_sha256(receipt_path),
            "canonical_sha256": receipt["day_receipt_sha256"],
        }

    selection_body = {
        "identity": IDENTITY,
        "panel_role": PANEL_ROLE,
        "required_days": REQUIRED_DAYS,
        "candidate_order": list(CANDIDATE_TARGET_DAYS),
        "consumed_exclusions": list(CONSUMED_TARGET_DAYS),
        "selected_days": selected,
        "selected_day_receipts": [
            row["day_receipt_sha256"]
            for row in target_receipts
            if row["utc_day"] in selected
        ],
    }
    selection_sha = canonical_sha256(selection_body)
    folds = _fold_manifest(selected, selection_sha256=selection_sha) if len(selected) == REQUIRED_DAYS else None
    status = (
        "offline_canonical_source_gate_passed_panel_mechanics_required"
        if len(selected) == REQUIRED_DAYS
        else "blocked_missing_canonical_fields"
    )
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": status,
        "panel_role": PANEL_ROLE,
        "evidence_clock": "exchange_time",
        "queue_identity": QUEUE_IDENTITY,
        "execution_mode": "offline_only",
        "exact_current_owner_baseline": {
            "policy_sha256": ACTIVE_OWNER_POLICY_SHA256,
            "predicate_bundle_sha256": ACTIVE_PREDICATE_BUNDLE_SHA256,
            "private_live_config_sha256": ACTIVE_PRIVATE_CONFIG_SHA256,
            "buy_policy": "CONTROL_85N",
            "owner_policy_unchanged": True,
        },
        "candidate_target_days": list(CANDIDATE_TARGET_DAYS),
        "primary_target_days": list(PRIMARY_TARGET_DAYS),
        "backup_target_days": list(BACKUP_TARGET_DAYS),
        "consumed_target_days_excluded": list(CONSUMED_TARGET_DAYS),
        "required_active_days": REQUIRED_DAYS,
        "selected_days": selected,
        "selection_sha256": selection_sha,
        "target_day_receipts": target_receipts,
        "target_day_receipt_files": target_receipt_files,
        "source_day_receipt_files": source_receipt_files,
        "source_failures": {day: list(reasons) for day, reasons in sorted(source_failures.items())},
        "fold_manifest": folds,
        "same_millisecond_policy": "censor_without_joint_book_trade_sequence",
        "permissions": {
            "economic_outcomes_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "new_research_writer_allowed": False,
            "candidate_specific_capture_allowed": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    manifest["canonical_manifest_sha256"] = canonical_document_sha256(
        manifest, "canonical_manifest_sha256"
    )
    _atomic_json(output / "canonical_source_manifest.json", manifest)
    if folds is not None:
        _atomic_json(output / "fold_manifest.json", folds)
    return manifest


def _forbidden_economic_fields(value: Any) -> Iterable[str]:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).lower()
            allowed_false = normalized == "economic_outcomes_read" and item is False
            if not allowed_false and any(part in normalized for part in _FORBIDDEN_ECONOMIC_PARTS):
                yield normalized
            yield from _forbidden_economic_fields(item)
    elif isinstance(value, list):
        for item in value:
            yield from _forbidden_economic_fields(item)


def validate_canonical_manifest(
    manifest_path: Path,
    *,
    rehash_sources: bool = True,
    layout: OfflineSourceLayout | None = None,
) -> dict[str, Any]:
    manifest = _load_json(manifest_path.expanduser().resolve())
    if manifest.get("identity") != IDENTITY or manifest.get("schema_version") != SCHEMA_VERSION:
        raise OfflineSourceGateError("offline canonical manifest identity drifted")
    if manifest.get("canonical_manifest_sha256") != canonical_document_sha256(
        manifest, "canonical_manifest_sha256"
    ):
        raise OfflineSourceGateError("offline canonical manifest hash drifted")
    forbidden_fields = tuple(_forbidden_economic_fields(manifest))
    if forbidden_fields:
        raise OfflineSourceGateError("source manifest contains an economic field")
    permissions = manifest.get("permissions")
    if not isinstance(permissions, Mapping) or any(
        permissions.get(field) is not False
        for field in (
            "economic_outcomes_read",
            "validation_read",
            "sealed_holdout_read",
            "new_research_writer_allowed",
            "candidate_specific_capture_allowed",
            "action_authorized",
            "live_authorized",
        )
    ):
        raise OfflineSourceGateError("offline permission boundary drifted")
    if manifest.get("exact_current_owner_baseline") != {
        "policy_sha256": ACTIVE_OWNER_POLICY_SHA256,
        "predicate_bundle_sha256": ACTIVE_PREDICATE_BUNDLE_SHA256,
        "private_live_config_sha256": ACTIVE_PRIVATE_CONFIG_SHA256,
        "buy_policy": "CONTROL_85N",
        "owner_policy_unchanged": True,
    }:
        raise OfflineSourceGateError("exact current owner B0 identity drifted")
    selected = tuple(manifest.get("selected_days") or ())
    target_receipts = manifest.get("target_day_receipts")
    if not isinstance(target_receipts, list) or len(target_receipts) != len(
        CANDIDATE_TARGET_DAYS
    ):
        raise OfflineSourceGateError("target day receipt census drifted")
    by_target: dict[str, Mapping[str, Any]] = {}
    for receipt in target_receipts:
        if not isinstance(receipt, Mapping):
            raise OfflineSourceGateError("target day receipt is malformed")
        target = _day(receipt.get("utc_day"))
        if target in by_target:
            raise OfflineSourceGateError("target day receipt is duplicated")
        if receipt.get("day_receipt_sha256") != canonical_document_sha256(
            receipt, "day_receipt_sha256"
        ):
            raise OfflineSourceGateError(f"target day receipt hash drifted: {target}")
        by_target[target] = receipt
    if tuple(by_target) != CANDIDATE_TARGET_DAYS:
        raise OfflineSourceGateError("target day receipt order drifted")
    expected_selected = tuple(
        day
        for day in CANDIDATE_TARGET_DAYS
        if by_target[day].get("source_gate_eligible") is True
    )[:REQUIRED_DAYS]
    if selected != expected_selected:
        raise OfflineSourceGateError("selected day order drifted from frozen replacement rule")
    if len(selected) == REQUIRED_DAYS:
        folds = manifest.get("fold_manifest")
        if not isinstance(folds, Mapping):
            raise OfflineSourceGateError("admitted panel lacks fold manifest")
        if folds.get("fold_manifest_sha256") != canonical_document_sha256(
            folds, "fold_manifest_sha256"
        ):
            raise OfflineSourceGateError("fold manifest hash drifted")
        if tuple(folds.get("active_days") or ()) != selected:
            raise OfflineSourceGateError("fold days drifted from selected days")
    if not rehash_sources:
        return manifest
    roots = layout or default_layout()
    _validate_receipt_files(
        manifest,
        layout=roots,
        target_receipts=by_target,
    )
    return manifest


def _validate_file_identity(
    identity: Mapping[str, Any],
    *,
    layout: OfflineSourceLayout,
) -> None:
    path_value = identity.get("path")
    expected = identity.get("sha256")
    if not isinstance(path_value, str) or _SHA_RE.fullmatch(str(expected)) is None:
        raise OfflineSourceGateError("source file identity is malformed")
    path = _resolve_portable(path_value, layout=layout)
    if not path.is_file() or path.stat().st_size != int(identity.get("size_bytes", -1)):
        raise OfflineSourceGateError(f"bound source file is missing or resized: {path_value}")
    if file_sha256(path) != expected:
        raise OfflineSourceGateError(f"bound source hash drifted: {path_value}")


def _validate_source_receipt(
    receipt: Mapping[str, Any],
    *,
    layout: OfflineSourceLayout,
) -> None:
    if receipt.get("source_day_receipt_sha256") != canonical_document_sha256(
        receipt, "source_day_receipt_sha256"
    ):
        raise OfflineSourceGateError("source-day receipt canonical hash drifted")
    raw = receipt.get("raw_orderbook")
    if not isinstance(raw, Mapping) or raw.get("hour_count") != 24:
        raise OfflineSourceGateError("source-day raw-hour census drifted")
    hours = raw.get("hours")
    if not isinstance(hours, list) or tuple(row.get("hour") for row in hours) != RAW_HOURS:
        raise OfflineSourceGateError("source-day raw-hour order drifted")
    for row in hours:
        if not isinstance(row, Mapping):
            raise OfflineSourceGateError("raw-hour identity is malformed")
        _validate_file_identity(row, layout=layout)
    for parent, key in (
        ("normalized", "bbo"),
        ("normalized", "l2"),
        (None, "aggtrades"),
        (None, "individual_trades"),
    ):
        container = receipt if parent is None else receipt.get(parent)
        if not isinstance(container, Mapping) or not isinstance(container.get(key), Mapping):
            raise OfflineSourceGateError(f"source-day identity lacks {key}")
        _validate_file_identity(container[key], layout=layout)


def _validate_receipt_files(
    manifest: Mapping[str, Any],
    *,
    layout: OfflineSourceLayout,
    target_receipts: Mapping[str, Mapping[str, Any]],
) -> None:
    target_files = manifest.get("target_day_receipt_files")
    source_files = manifest.get("source_day_receipt_files")
    if not isinstance(target_files, Mapping) or not isinstance(source_files, Mapping):
        raise OfflineSourceGateError("receipt file bindings are missing")
    if tuple(target_files) != CANDIDATE_TARGET_DAYS:
        raise OfflineSourceGateError("target receipt file order drifted")
    for day, binding in target_files.items():
        if not isinstance(binding, Mapping):
            raise OfflineSourceGateError("target receipt file binding is malformed")
        path = _resolve_portable(str(binding.get("path")), layout=layout)
        if not path.is_file() or file_sha256(path) != binding.get("sha256"):
            raise OfflineSourceGateError(f"target receipt file hash drifted: {day}")
        receipt = _load_json(path)
        if receipt != target_receipts[day]:
            raise OfflineSourceGateError(f"embedded and file target receipts differ: {day}")
        if receipt.get("day_receipt_sha256") != binding.get("canonical_sha256"):
            raise OfflineSourceGateError(f"target receipt canonical binding drifted: {day}")
    referenced_source_days = {
        source_day
        for receipt in target_receipts.values()
        if receipt.get("source_gate_eligible") is True
        for source_day in receipt.get("context_days", {}).values()
    }
    if not referenced_source_days.issubset(source_files):
        raise OfflineSourceGateError("eligible targets reference unbound source days")
    for day, binding in source_files.items():
        if not isinstance(binding, Mapping):
            raise OfflineSourceGateError("source receipt file binding is malformed")
        path = _resolve_portable(str(binding.get("path")), layout=layout)
        if not path.is_file() or file_sha256(path) != binding.get("sha256"):
            raise OfflineSourceGateError(f"source receipt file hash drifted: {day}")
        receipt = _load_json(path)
        if receipt.get("source_day") != day:
            raise OfflineSourceGateError(f"source receipt day drifted: {day}")
        if receipt.get("source_day_receipt_sha256") != binding.get("canonical_sha256"):
            raise OfflineSourceGateError(f"source receipt canonical binding drifted: {day}")
        _validate_source_receipt(receipt, layout=layout)


def _resolve_portable(value: str, *, layout: OfflineSourceLayout) -> Path:
    for marker, root in (
        ("${NARROWGATE_DATA_ROOT}", layout.project_data_root),
        ("${NARROWGATE_MARKETDATA_ROOT}", layout.marketdata_root),
    ):
        if value == marker:
            return root.resolve()
        prefix = marker + "/"
        if value.startswith(prefix):
            return (root / value[len(prefix) :]).resolve()
    raise OfflineSourceGateError(f"unsupported portable source path: {value}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    audit = subparsers.add_parser("audit")
    audit.add_argument("--output-dir", type=Path, required=True)
    audit.add_argument("--workers", type=int, default=4)
    validate = subparsers.add_parser("validate")
    validate.add_argument("manifest", type=Path)
    validate.add_argument("--no-rehash", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "audit":
        manifest = audit_historical_sources(
            layout=default_layout(),
            output_dir=args.output_dir,
            workers=args.workers,
        )
    else:
        manifest = validate_canonical_manifest(
            args.manifest,
            rehash_sources=not args.no_rehash,
        )
    print(
        json.dumps(
            {
                "identity": manifest["identity"],
                "status": manifest["status"],
                "selected_day_count": len(manifest.get("selected_days", ())),
                "canonical_manifest_sha256": manifest["canonical_manifest_sha256"],
                "economic_outcomes_read": False,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
