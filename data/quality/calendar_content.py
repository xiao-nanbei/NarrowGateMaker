"""Streaming, zero-economic content audit of a calendar_readability manifest.

Decode every registered row/column; do not infer freshness from output density,
reconstruct missing events, grant research use, or filter calendar dates. Results
are exclusive per-day checkpoints. A completed scan may still report findings.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from data.quality.calendar_gap_manifest import _calendar_days, sha256_file
from data.quality.calendar_readability import UNKNOWN, validate_readability_manifest
from data.daily_raw import DAILY_BOOK_MARKER
from data_paths import resolve_portable_path

DAY_US = 86_400_000_000
DERIVED_AGGREGATE_ID = "btcusdc-derived-trade-aggregates-100ms"
BOOK_FUSION_SCHEMA = "reconstructed_fusion.v1"
BOOK_UNION_SCHEMA = "observed_union.v1"
BOOK_REPRESENTATIONS = {BOOK_FUSION_SCHEMA.encode(), BOOK_UNION_SCHEMA.encode(), DAILY_BOOK_MARKER.encode()}


def clock_content(day: str, path: Path, *, previous: dict | None = None) -> dict:
    """Bounded clock-only scan; price validity is not inferred from timestamps."""
    import numpy as np
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    names = set(parquet.schema_arrow.names)
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp())*1_000_000
    cadence, expected = 100_000, 864_000
    seen = np.zeros(expected, dtype=np.bool_)
    channels = {
        "l2": ("last_observation_timestamp_us", "observation_age_us", "observation_kind"),
        "bbo": ("bbo_last_observation_timestamp_us", "bbo_observation_age_us", "bbo_observation_kind"),
    }
    result = {"rows": 0, "calendar_grid_states": expected, "cadence_us": cadence,
              "duplicate_grid_rows": 0, "off_grid_rows": 0, "axis_regressions": 0,
              "bbo_newer_than_l2_rows": 0, "channels": {}}
    for name, columns in channels.items():
        if not set(columns) <= names:
            result["channels"][name] = {"status": "UNKNOWN", "missing_columns": sorted(set(columns)-names)}
            continue
        result["channels"][name] = {"status": "CHECKED", "max_age_us": None,
            "max_age_output_timestamp_us": None, "max_age_last_observation_timestamp_us": None,
            "over_500ms_rows": 0, "unknown_state_rows": 0, "source_observed_rows": 0,
            "carried_forward_rows": 0, "future_fill_violations": 0, "age_identity_violations": 0,
            "source_clock_regressions": 0, "observation_kind_counts": Counter(),
            "unusable_rows": 0 if name == "bbo" and "bbo_usable" in names else UNKNOWN,
            "invalid_quote_rows": UNKNOWN,
            "invalid_top_rows": 0 if name == "bbo" else UNKNOWN}
    selected = ["timestamp", *[column for columns in channels.values() for column in columns if column in names]]
    if "bbo_usable" in names:
        selected.append("bbo_usable")
    previous_axis, previous_observed = None, {}
    grid_axis, grid_observations = [], {name: [] for name in channels}

    def integers(batch, column):
        array = batch[column]
        return (pc.fill_null(array, -1).to_numpy(zero_copy_only=False),
                pc.is_valid(array).to_numpy(zero_copy_only=False))

    for batch in parquet.iter_batches(batch_size=65_536, columns=selected, use_threads=False):
        axis, valid_axis = integers(batch, "timestamp")
        axis = axis * 1000
        grid_axis.append(axis.copy())
        valid_axis &= axis >= start
        valid_axis &= axis < start + expected*cadence
        valid_axis &= (axis-start) % cadence == 0
        indices = ((axis[valid_axis]-start)//cadence).astype(np.int64)
        unique = np.unique(indices)
        new_count = int((~seen[unique]).sum())
        seen[unique] = True
        result["duplicate_grid_rows"] += len(indices)-new_count
        result["off_grid_rows"] += int((~valid_axis).sum())
        delta = np.diff(axis if previous_axis is None else np.r_[previous_axis, axis])
        result["axis_regressions"] += int((delta < 0).sum())
        previous_axis = int(axis[-1])
        result["rows"] += len(batch)
        observations = {}
        for name, (observed_column, age_column, kind_column) in channels.items():
            stats = result["channels"][name]
            if stats["status"] != "CHECKED":
                continue
            observed, valid_observed = integers(batch, observed_column)
            grid_observations[name].append(observed.copy())
            age, valid_age = integers(batch, age_column)
            valid_observed &= observed > 0
            known = valid_observed & valid_age
            kinds = batch[kind_column].to_pylist()
            counts = Counter(kinds)
            stats["observation_kind_counts"].update(str(k) if k is not None else "NULL" for k in kinds)
            stats["source_observed_rows"] += counts["source_observed"]
            stats["carried_forward_rows"] += counts["carried_forward"]
            kind_unknown = np.array([k in (None, "unknown") for k in kinds], dtype=bool)
            stats["unknown_state_rows"] += int((~known | kind_unknown).sum())
            stats["over_500ms_rows"] += int((known & (age > 500_000)).sum())
            stats["future_fill_violations"] += int((valid_observed & (observed > axis) | valid_age & (age < 0)).sum())
            stats["age_identity_violations"] += int((known & (age != axis-observed)).sum())
            valid_values = observed[valid_observed]
            last_observed = previous_observed.get(name)
            differences = np.diff(valid_values if last_observed is None else np.r_[last_observed, valid_values])
            stats["source_clock_regressions"] += int((differences < 0).sum())
            if len(valid_values):
                previous_observed[name] = int(valid_values[-1])
            if known.any():
                candidate = int(np.argmax(np.where(known, age, np.iinfo(np.int64).min)))
                if stats["max_age_us"] is None or age[candidate] > stats["max_age_us"]:
                    stats.update(max_age_us=int(age[candidate]), max_age_output_timestamp_us=int(axis[candidate]),
                                 max_age_last_observation_timestamp_us=int(observed[candidate]))
            if name == "bbo":
                stats["invalid_top_rows"] += counts["invalid_top"]
                if "bbo_usable" in names:
                    stats["unusable_rows"] += len(batch)-int(pc.sum(pc.fill_null(batch["bbo_usable"], False)).as_py())
            observations[name] = (observed, valid_observed)
        if len(observations) == 2:
            l2, lv = observations["l2"]
            bbo, bv = observations["bbo"]
            result["bbo_newer_than_l2_rows"] += int((lv & bv & (bbo > l2)).sum())
        else:
            result["bbo_newer_than_l2_rows"] = UNKNOWN
    result["missing_calendar_grid_states"] = expected-int(seen.sum())
    axis = np.concatenate(grid_axis) if grid_axis else np.empty(0, dtype=np.int64)
    result["boundary_output_ts_ms"] = [int(v//1000) for v in np.unique(np.r_[axis[:3], axis[-3:]])]
    for name, stats in result["channels"].items():
        if stats["status"] == "CHECKED":
            stats["observation_kind_counts"] = dict(stats["observation_kind_counts"])
            stats["max_age_us"] = stats["max_age_us"] if stats["max_age_us"] is not None else UNKNOWN
            stats["unknown_calendar_grid_states"] = (
                result["missing_calendar_grid_states"]+stats["unknown_state_rows"]
                if not result["duplicate_grid_rows"] and not result["off_grid_rows"] else UNKNOWN)
            obs = np.concatenate(grid_observations[name]) if grid_observations[name] else np.empty(0, dtype=np.int64)
            prior = (previous or {}).get(name)
            stats["boundary_observed_us"] = [int(obs[i]) for i in np.unique(np.r_[np.arange(min(3,len(obs))), np.arange(max(0,len(obs)-3),len(obs))])]
            try:
                if np.any(obs <= 0):
                    raise ValueError("explicit unknown source clocks cannot be bridged as known")
                stats["grid"] = observation_grid(day, axis, obs, prior)
                stats["grid_predecessor"] = list(prior) if prior is not None else None
            except ValueError as exc:
                stats["grid"] = {"status": "NOT_VERIFIED", "reason": str(exc)}
    return result


def observation_grid(day: str, output_us: np.ndarray, observed_us: np.ndarray,
                     previous: tuple[int, int] | None = None, *, cadence_us=100_000,
                     stale_after_us=500_000) -> dict:
    """Exercise causal as-of reads for an entire UTC day, including its edges.

    A missing message is not evidence of no market update. This checks data-layer
    carry only; it neither fabricates a book row nor simulates an order/fill.
    """
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()*1e6)
    ts = np.asarray(output_us, dtype=np.int64)
    obs = np.asarray(observed_us, dtype=np.int64)
    if len(ts) != len(obs) or np.any(np.diff(ts) <= 0):
        raise ValueError("book/observation axes must align and be strictly ordered")
    if np.any(obs > ts) or np.any(np.diff(obs) < 0):
        raise ValueError("future or reversed source observation clock")
    inside = (ts >= start) & (ts < start + DAY_US)
    ts, obs = ts[inside], obs[inside]
    if previous is not None:
        if previous[0] >= start or previous[1] > previous[0]:
            raise ValueError("invalid previous-day observation")
        if len(obs) and obs[0] < previous[1]:
            raise ValueError("source observation regressed across UTC midnight")
        ts, obs = np.r_[previous[0], ts], np.r_[previous[1], obs]
    ticks = np.arange(start, start + DAY_US, cadence_us, dtype=np.int64)
    indices = np.searchsorted(ts, ticks, side="right") - 1
    known = indices >= 0
    selected = obs[indices[known]]
    age = ticks[known] - selected
    repeated = np.diff(selected) == 0
    age_diff = np.diff(age)
    tick_diff = np.diff(ticks[known])
    last = (int(ts[-1]), int(obs[-1])) if len(ts) else previous
    return {
        "grid_rows": len(ticks), "known_state_rows": int(known.sum()),
        "unknown_state_rows": int((~known).sum()),
        "fresh_state_rows": int((age <= stale_after_us).sum()),
        "stale_state_rows": int((age > stale_after_us).sum()),
        "stale_after_us": stale_after_us,
        "max_stale_age_us": int(age.max()) if len(age) else UNKNOWN,
        "future_fill_violations": int((age < 0).sum()),
        "carry_age_violations": int((age_diff[repeated] != tick_diff[repeated]).sum()),
        "cross_day_inherited": previous is not None,
        "first_grid_observed_us": int(obs[indices[0]]) if known[0] else UNKNOWN,
        "first_grid_age_us": int(ticks[0]-obs[indices[0]]) if known[0] else UNKNOWN,
        "last_output_us": last[0] if last else None,
        "last_observed_us": last[1] if last else None,
        "missing_update_semantics": "UNKNOWN_NOT_CONFIRMED_NO_UPDATE",
        "account_order_fifo_continuity": "NOT_TESTED",
    }


def _source_times(path: Path) -> tuple[np.ndarray, dict]:
    """Read real observation clocks, not a fused stream's presentation times."""
    before = path.stat()
    parquet = pq.ParquetFile(path)
    marker = (parquet.metadata.metadata or {}).get(b"narrowgate.book_fusion")
    if marker is not None and marker not in BOOK_REPRESENTATIONS:
        raise ValueError("unrecognized canonical book fusion schema")
    fused = marker is not None
    clock_column = ("observed_timestamp_us" if marker == DAILY_BOOK_MARKER.encode()
                    else "source_observed_timestamp_us" if fused else "timestamp")
    if clock_column not in parquet.schema_arrow.names:
        raise ValueError("canonical fused book lacks its real observation clock")
    pieces = []
    rows = 0
    reversals = 0
    last = None
    columns = [clock_column, "timestamp"] if fused else [clock_column]
    if marker == DAILY_BOOK_MARKER.encode():
        columns.append("top_only")
    for batch in parquet.iter_batches(columns=columns, batch_size=1_000_000):
        rows += len(batch)
        if marker == DAILY_BOOK_MARKER.encode():
            # Top observations never prove that depth/queue state is fresh.
            batch = batch.filter(pa.compute.invert(batch["top_only"]))
        if batch.column(0).null_count:
            raise ValueError("canonical raw exchange clock contains nulls")
        if fused:
            if not pa.types.is_integer(batch.column(0).type) or not pa.types.is_integer(batch.column(1).type) or batch.column(1).null_count:
                raise ValueError("fused observation and presentation clocks require integer microseconds")
            values = batch.column(0).to_numpy().astype(np.int64, copy=False)
            presentation = batch.column(1).to_numpy().astype(np.int64, copy=False)
            if np.any(values <= 0) or np.any(values > presentation):
                raise ValueError("fused source clock is missing or later than presentation")
        else:
            values = _us(batch.column(0))
        delta = np.diff(values if last is None else np.r_[last, values])
        reversals += int((delta < 0).sum())
        if len(values):
            pieces.append(np.unique(values))
            last = values[-1]
    if not pieces:
        raise ValueError("empty canonical book source")
    times = np.unique(np.concatenate(pieces))
    identity = {"sha256": sha256_file(path), "rows": rows,
                "distinct_exchange_times": len(times), "clock_reversals": reversals,
                "first_exchange_us": int(times[0]), "last_exchange_us": int(times[-1]),
                "max_no_new_observation_us": int(np.diff(times).max()) if len(times)>1 else UNKNOWN}
    if fused:
        identity.update(exchange_clock_column=clock_column, presentation_clock_column="timestamp",
                        raw_reconstruction=marker.decode(), native_sequence_authority=False)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("canonical source changed during clock scan")
    return times, identity


def verify_separate_top_view(raw_path: Path, bbo_path: Path, l2_path: Path, clock_path: Path) -> dict:
    """Verify independent BBO values/clocks against retained raw top events.

    This does not weaken any depth freshness guard. Invalid top rows stay in
    the file/clock and are explicitly masked by the shared BBO loader.
    """
    from data.book_top import from_daily_book_rows
    from data.daily_raw import daily_book_receipt

    receipt = daily_book_receipt(raw_path)
    bbo, l2, clock = (pq.read_table(path) for path in (bbo_path, l2_path, clock_path))
    axis = np.asarray(clock["timestamp"])*1000
    if not bbo["timestamp"].equals(clock["timestamp"]) or not l2["timestamp"].equals(clock["timestamp"]):
        raise ValueError("independent top/depth grid axes differ")
    deep = np.asarray(clock["last_observation_timestamp_us"])
    top = np.asarray(clock["bbo_last_observation_timestamp_us"])
    usable = np.asarray(clock["bbo_usable"])
    if (clock["bbo_last_observation_timestamp_us"].null_count or np.any(top <= 0)
            or np.any(top > axis) or np.any(np.diff(top) < 0)
            or not np.array_equal(axis-top, np.asarray(clock["bbo_observation_age_us"]))):
        raise ValueError("independent top source age/causality violation")
    kinds = np.asarray(clock["bbo_observation_kind"].to_pylist())
    if np.any(usable & np.isin(kinds, ("invalid_top", "unknown"))):
        raise ValueError("unknown top was marked usable")
    values = np.column_stack([np.asarray(bbo[name]) for name in (
        "best_bid", "best_bid_qty", "best_ask", "best_ask_qty")])
    deep_values = np.column_stack([np.asarray(l2[name]) for name in (
        "bid_px_1", "bid_qty_1", "ask_px_1", "ask_qty_1")])
    same = (top == deep) & np.all(values == deep_values, axis=1)
    lookup = {}
    seed = (receipt.get("top_initial_state") or {}).get("last_bbo")
    if seed:
        lookup[seed["observed_timestamp_us"]] = [float(seed[name]) for name in (
            "bid_price", "bid_amount", "ask_price", "ask_amount")]
    events = from_daily_book_rows(pq.read_table(raw_path, filters=[("top_only", "=", True)]))
    for event in events.to_pylist():
        lookup[event["timestamp"]] = [float(event[name]) if event[name] is not None else float("nan") for name in (
            "bid_price", "bid_amount", "ask_price", "ask_amount")]
    selected = (~same) & usable
    for stamp in np.unique(top[selected]):
        expected = lookup.get(int(stamp))
        if expected is None or not np.all(values[selected & (top == stamp)] == expected):
            raise ValueError("independent BBO values lack their actual raw observation")
    return {"rows": len(axis), "independent_top_rows": int(selected.sum()),
            "unusable_top_rows": int((~usable).sum()), "future_fill_violations": 0,
            "max_bbo_age_us": int((axis-top).max()) if len(axis) else None,
            "depth_clock_unchanged": True, "top_events_retained": len(events)}


def _fusion_source_binding(raw_path: Path, raw_identity: dict, source: dict,
                           source_quality: dict, book_root: Path, day: str,
                           ts: np.ndarray, bbo: dict, l2: dict) -> tuple[pa.Table, dict, dict]:
    """Verify the current same-pass triplet without rewriting its true clocks."""
    # The verified writer finalizes its receipt after streaming the last row.
    # Late Parquet key/value metadata need not be inside serialized ARROW:schema.
    metadata = pq.ParquetFile(raw_path).metadata.metadata or {}
    marker = metadata.get(b"narrowgate.book_fusion")
    if marker not in BOOK_REPRESENTATIONS:
        raise ValueError("fused selection requires a canonical fusion footer")
    representation = marker.decode()
    daily_book = representation == DAILY_BOOK_MARKER
    try:
        receipt_bytes = metadata[b"narrowgate.book_receipt" if daily_book else b"narrowgate.fusion_receipt"]
        receipt = json.loads(receipt_bytes)
    except (KeyError, ValueError) as exc:
        raise ValueError("fused canonical receipt is missing or invalid") from exc
    if (receipt.get("schema"), receipt.get("day"), receipt.get("symbol"), receipt.get("output_rows")) != (
            representation, day, "BTCUSDC", raw_identity["rows"]):
        raise ValueError("fused canonical receipt scope or rows differ")
    qpath = book_root / "quality" / f"BTCUSDC-{day}.json"
    qsha = sha256_file(qpath)
    q = json.loads(qpath.read_text())
    expected_quality = source.get("quality_sha256") or source_quality.get("source_quality_sha256")
    if not expected_quality or qsha != expected_quality:
        raise ValueError("same-pass fusion quality is not bound to selected inputs")
    if source_quality.get("source_quality_sha256") not in (None, "", qsha):
        raise ValueError("same-pass quality differs from daily selection quality")
    if (q.get("day"), q.get("symbol"), q.get("raw_reconstruction"), q.get("raw_source", {}).get("sha256")) != (
            day, "BTCUSDC", representation, raw_identity["sha256"]):
        raise ValueError("same-pass fusion quality raw identity or scope differs")
    if source.get("raw_sha256") != raw_identity["sha256"]:
        raise ValueError("fused selection canonical content identity differs")
    def included_ids(items):
        result = {(item["source_id"], item["sha256"], int(item["rows"])) for item in items}
        if len(result) != len(items) or not result:
            raise ValueError("fusion source inclusion is empty or duplicated")
        return result
    if not daily_book:
        declared = included_ids(receipt.get("included_sources", []))
        if included_ids(q.get("included_sources", [])) != declared or included_ids(source.get("included_sources", [])) != declared:
            raise ValueError("fusion source inclusion differs across receipt and selection")
    normalized = receipt.get("normalized", {}) if daily_book else receipt.get("stats", {}).get("normalized", {})
    for kind, audit in (("bbo", bbo), ("l2", l2)):
        if (q.get(f"{kind}_output", {}).get("sha256") != audit["sha256"]
                or normalized.get(kind, {}).get("sha256") != audit["sha256"]
                or normalized.get(kind, {}).get("rows") != audit["rows"]):
            raise ValueError(f"fused {kind} differs from same-pass canonical receipt")
    clock_path = book_root / "clock" / f"BTCUSDC-clock-{day}.parquet"
    clock_sha = sha256_file(clock_path)
    if (clock_sha != q.get("clock_output", {}).get("sha256")
            or clock_sha != normalized.get("clock", {}).get("sha256")):
        raise ValueError("fused clock differs from same-pass canonical receipt")
    clock = pq.read_table(clock_path)
    def integers(name):
        column = clock[name].combine_chunks()
        if column.null_count or not pa.types.is_integer(column.type):
            raise ValueError("same-pass observation clocks require non-null integers")
        return column.to_numpy().astype(np.int64, copy=False)
    if normalized.get("clock", {}).get("rows") != clock.num_rows or not np.array_equal(ts, integers("timestamp") * 1000):
        raise ValueError("same-pass clock axis or rows differ")
    observed = integers("last_observation_timestamp_us")
    if np.any(observed <= 0) or not np.array_equal(ts-observed, integers("observation_age_us")):
        raise ValueError("same-pass clock age does not preserve real observation time")
    return clock, q, {"kind": "FUSION_SAME_PASS", "producer_quality_path": str(qpath),
        "producer_quality_sha256": qsha, "canonical_footer_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
        "observation_column": "observed_timestamp_us" if daily_book else "source_observed_timestamp_us",
        **({} if daily_book else {"included_sources": receipt["included_sources"]}),
        "raw_representation": representation, "native_sequence_authority": False,
        "freshness_authority": "HASH_BOUND_SELECTED_NORMALIZED_CLOCK_NOT_RAW_UNION_ACTIVITY"}


def _reconstruction_summary(results: list[dict]) -> str:
    kinds = {result.get("raw_reconstruction", "RETAINED_NOT_REBUILT") for result in results}
    return next(iter(kinds)) if len(kinds) == 1 else "MIXED_RETAINED_AND_RECONSTRUCTED"


def _atomic_json(path: Path, payload: dict) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, allow_nan=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def _clock_reader_boundaries(day: str, quality: dict, checked: dict) -> dict:
    """Use the shared replay clock adapter, without loading price matrices."""
    from models.backtest_tick import _load_book_observations

    result = {}
    for kind in ("l2", "bbo"):
        path = Path(quality[f"{kind}_output"]["path"])
        axis = np.asarray(pq.read_table(path, columns=["timestamp"])["timestamp"], dtype=np.int64)
        if len(axis) != checked["rows"] or np.any(np.diff(axis) <= 0):
            raise ValueError(f"{day} {kind} output axis differs from bound clock")
        sample = np.unique(np.r_[axis[:3], axis[-3:]])
        if sample.tolist() != checked["boundary_output_ts_ms"]:
            raise ValueError(f"{day} {kind} boundary axis differs from clock")
        observed, _, usable = _load_book_observations(path, axis, kind, symbol="BTCUSDC", return_usable=True)
        indices = np.searchsorted(axis, sample)
        if observed[indices].tolist() != checked["channels"][kind]["boundary_observed_us"]:
            raise ValueError(f"{day} {kind} shared reader changed observation clock")
        result[kind] = {"status": "BOUND_CLOCK_ADAPTER_VERIFIED", "output_axis_rows": len(axis),
                        "boundary_rows": len(sample), "boundary_usable": usable[indices].tolist(),
                        "output_sha256": quality[f"{kind}_output"]["sha256"]}
    return result


def audit_book_clocks(records: list[dict], book_root: Path, *, manifest_sha: str) -> dict:
    """Full-calendar bound clock scan; no raw reconstruction or metadata publication."""
    days = [row["calendar_date"] for row in records]
    if not days or days != _calendar_days(days[0], days[-1]):
        raise ValueError("clock acceptance requires one unique continuous calendar")
    started, previous, results = time.monotonic(), {}, []
    pinned = []
    for record in records:
        day = record["calendar_date"]
        row = {"calendar_date": day, "status": "BLOCKED", "research_use": record.get("research_use", UNKNOWN)}
        try:
            quality_path = book_root / "quality" / f"BTCUSDC-{day}.json"
            quality_bytes = quality_path.read_bytes()
            quality = json.loads(quality_bytes)
            if (quality.get("day"), quality.get("symbol")) != (day, "BTCUSDC"):
                raise ValueError("quality day/symbol mismatch")
            bound = quality["clock_output"]
            clock_path = book_root / "clock" / Path(bound["path"]).name
            before = sha256_file(clock_path)
            row.update(quality_sha256=hashlib.sha256(quality_bytes).hexdigest(), clock_sha256=before)
            if before != bound["sha256"]:
                raise ValueError("clock content differs from current quality binding")
            checked = clock_content(day, clock_path, previous=previous)
            if checked["rows"] != bound["rows"]:
                raise ValueError("clock rows differ from current quality binding")
            row.update(checked)
            row["reader"] = _clock_reader_boundaries(day, quality, checked)
            if sha256_file(clock_path) != before or quality_path.read_bytes() != quality_bytes:
                raise ValueError("clock/quality changed during acceptance")
            pinned.append((quality_path, row["quality_sha256"], clock_path, before))
            row["channels"]["bbo"]["invalid_quote_rows"] = quality.get("invalid_spread_buckets", UNKNOWN)
            grid = checked["channels"]["l2"].get("grid", {})
            old = quality.get("observation_grid", {})
            row["quality_grid_changed_fields"] = sorted(key for key, value in grid.items() if old.get(key) != value)
            row["quality_grid_refresh_required"] = bool(row["quality_grid_changed_fields"])
            row["producer_grid_cross_day_inherited"] = old.get("cross_day_inherited", UNKNOWN)
            row["status"] = "CHECKED"
            next_previous = {}
            for kind, stats in checked["channels"].items():
                current = stats.get("grid", {})
                if current.get("status") == "NOT_VERIFIED":
                    row["status"] = "FINDINGS"
                elif current.get("last_output_us") is not None:
                    next_previous[kind] = (current["last_output_us"], current["last_observed_us"])
                if any(stats.get(name, 0) for name in ("future_fill_violations", "age_identity_violations", "source_clock_regressions")):
                    row["status"] = "FINDINGS"
            if any(checked[name] for name in ("duplicate_grid_rows", "off_grid_rows", "axis_regressions")):
                row["status"] = "FINDINGS"
            previous = next_previous
        except (OSError, ValueError, KeyError, TypeError) as exc:
            row.update(status="BLOCKED", error=f"{type(exc).__name__}: {exc}")
            previous = {}  # No unverified predecessor may silently bridge a failed day.
        results.append(row)
        if len(results) % 20 == 0 or len(results) == len(days):
            print(json.dumps({"clock_days": len(results), "total_days": len(days), "last_day": day,
                              "status_counts": dict(Counter(r["status"] for r in results))}), flush=True)
    drift = [path.stem for quality_path, quality_sha, path, digest in pinned
             if sha256_file(quality_path) != quality_sha or sha256_file(path) != digest]
    counters = ("source_observed_rows", "carried_forward_rows", "unknown_state_rows", "unknown_calendar_grid_states",
                "over_500ms_rows", "future_fill_violations", "age_identity_violations", "source_clock_regressions",
                "unusable_rows", "invalid_top_rows")
    channels = {}
    for kind in ("l2", "bbo"):
        stats = [r.get("channels", {}).get(kind, {}) for r in results]
        channels[kind] = {name: sum(s[name] for s in stats) if all(isinstance(s.get(name), int) for s in stats) else UNKNOWN
                          for name in counters}
        channels[kind]["max_age_us"] = max((s["max_age_us"] for s in stats if isinstance(s.get("max_age_us"), int)), default=UNKNOWN)
        channels[kind]["verified_grid_predecessor_days"] = sum(s.get("grid", {}).get("cross_day_inherited") is True for s in stats)
    return {"schema": "calendar_bound_book_clock_validation.v1", "checked_at_utc": datetime.now(UTC).isoformat(),
            "calendar_start": days[0], "calendar_end": days[-1], "date_count": len(days), "calendar_dates": days,
            "calendar_grid_states": len(days)*864_000, "status_counts": dict(Counter(r["status"] for r in results)),
            "status": "COMPLETED" if not drift and all(r["status"] == "CHECKED" for r in results) else "COMPLETED_WITH_FINDINGS",
            "input_manifest_sha256": manifest_sha, "source_drift_days": drift, "channels": channels,
            "quality_grid_refresh_days": [r["calendar_date"] for r in results if r.get("quality_grid_refresh_required")],
            "raw_payload_read": False, "price_payload_read": False, "economic_payload_read": False,
            "current_quality_or_index_written": False, "deep_and_top_state_continuity": "NOT_TESTED",
            "account_order_fifo_continuity": "NOT_TESTED", "research_use_changed": False,
            "observed_vs_carried": "stored_observation_kind_not_age_equals_zero",
            "price_validity": "BBO_bound_quality_only; L2_price_payload_NOT_READ",
            "reader_scope": "shared_clock_adapter_all_output_axes_and_boundary_values; price_matrices_NOT_READ",
            "elapsed_s": time.monotonic()-started, "records": results}


def validate_clock_manifest(manifest: dict) -> None:
    """Current publisher's JSON false and legacy NOT_GRANTED both deny use.

    This validation-only projection never rewrites the source manifest/identity
    and does not accept true, missing or any other admission value.
    """
    projected = {**manifest, "records": [{**row, "channels": [
        {**channel, "economic_admission": "NOT_GRANTED"}
        if channel.get("economic_admission") is False else channel
        for channel in row["channels"]]} for row in manifest["records"]]}
    validate_readability_manifest(projected)


def validate_book_prefix(records: list[dict], results: list[dict], book_root: Path) -> None:
    """Resume/readback checks the actual immutable inputs and published triplet."""
    if [r["calendar_date"] for r in results] != [r["calendar_date"] for r in records[:len(results)]]:
        raise ValueError("completed records are not the requested calendar prefix")
    previous = None
    for record, result in zip(records, results, strict=False):
        day = record["calendar_date"]
        raw = next(c for c in record["channels"] if c["source_id"] == "btcusdc-daily-raw-l2")
        expected_raw = raw.get("source_content_validation", {}).get("sha256")
        if expected_raw and result["raw_source"]["sha256"] != expected_raw:
            raise ValueError(f"completed source differs from frozen input: {day}")
        if sha256_file(Path(raw["files"][0]["path"])) != result["raw_source"]["sha256"]:
            raise ValueError(f"completed raw source identity changed: {day}")
        quality_path = Path(result["quality_path"])
        if sha256_file(quality_path) != result["quality_sha256"]:
            raise ValueError(f"completed quality identity changed: {day}")
        quality = json.loads(quality_path.read_text())
        producer_binding = quality.get("source_clock_binding", {})
        if producer_binding.get("kind") == "FUSION_SAME_PASS":
            producer_path = Path(producer_binding["producer_quality_path"])
            if sha256_file(producer_path) != producer_binding["producer_quality_sha256"]:
                raise ValueError(f"completed same-pass producer quality changed: {day}")
        following = quality.get("source_clock_binding", {}).get("following_capture_partition")
        if following and sha256_file(Path(following["path"])) != following["sha256"]:
            raise ValueError(f"completed adjacent source identity changed: {day}")
        for kind in ("bbo", "l2", "clock"):
            path = book_root / kind / f"BTCUSDC-{kind}-{day}.parquet"
            if sha256_file(path) != quality[kind+"_output"]["sha256"]:
                raise ValueError(f"completed {kind} identity changed: {day}")
        clock = pq.read_table(book_root / "clock" / f"BTCUSDC-clock-{day}.parquet")
        actual = observation_grid(day, np.asarray(clock["timestamp"])*1000,
                                  np.asarray(clock["last_observation_timestamp_us"]), previous)
        if actual != result["grid"] or quality["grid"] != actual:
            raise ValueError(f"completed causal grid changed: {day}")
        previous = (actual["last_output_us"], actual["last_observed_us"])


def validate_book_consumers(records: list[dict], results: list[dict], book_root: Path) -> dict:
    """Read every completed day through the actual shared replay data loaders.

    No strategy is constructed. Warmup rows are retained by the loaders but the
    acceptance grid is the exact UTC calendar; its last book/clock is inherited.
    """
    import contextlib
    import gc
    import io

    from models import backtest_tick as replay
    from models.tick_data_types import book_observation_status, book_observation_times_us

    previous_bbo, previous_l2 = replay.BBO_DIR, replay.L2_DIR
    days = [r["calendar_date"] for r in records]
    if [r["calendar_date"] for r in results] != days:
        raise ValueError("consumer readback requires the full completed calendar")
    old_max_levels = os.environ.pop("MM_L2_MAX_LEVELS", None)
    previous = None
    previous_values = None
    totals = Counter()
    try:
        replay.BBO_DIR, replay.L2_DIR = book_root / "bbo", book_root / "l2"
        for record, result in zip(records, results, strict=True):
            day = record["calendar_date"]
            with contextlib.redirect_stdout(io.StringIO()):
                bbo = replay.load_bbo_data([day], quality_allowed_days=days)
                l2 = replay.load_l2_data([day], quality_allowed_days=days)
            if bbo is None or l2 is None:
                raise ValueError(f"shared loader excluded a calendar day: {day}")
            if book_observation_status(bbo) != "BOUND" or book_observation_status(l2) != "BOUND":
                raise ValueError(f"shared loader lost observation binding: {day}")
            observed = book_observation_times_us(l2)
            clock_path = book_root / "clock" / f"BTCUSDC-clock-{day}.parquet"
            independent = "bbo_last_observation_timestamp_us" in pq.read_schema(clock_path).names
            if independent:
                clock = pq.read_table(clock_path)
                indices = np.searchsorted(np.asarray(clock["timestamp"]), bbo.ts_ms)
                if (np.any(indices >= len(clock)) or not np.array_equal(
                        np.asarray(clock["timestamp"])[indices], bbo.ts_ms)
                        or not np.array_equal(np.asarray(clock["bbo_last_observation_timestamp_us"])[indices],
                                              book_observation_times_us(bbo))):
                    raise ValueError(f"shared loader changed independent BBO clock: {day}")
            elif (not np.array_equal(bbo.ts_ms, l2.ts_ms)
                    or not np.array_equal(book_observation_times_us(bbo), observed)
                    or not np.array_equal(bbo.best_bid, l2.bid_px[:, 0])
                    or not np.array_equal(bbo.best_ask, l2.ask_px[:, 0])):
                raise ValueError(f"shared loader BBO/L2 content mismatch: {day}")
            actual = observation_grid(day, l2.ts_ms*1000, observed, previous)
            if actual != result["grid"]:
                raise ValueError(f"shared loader changed continuous clock semantics: {day}")
            start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()*1e6)
            inside = (l2.ts_ms*1000 >= start) & (l2.ts_ms*1000 < start+DAY_US)
            times, obs = l2.ts_ms[inside]*1000, observed[inside]
            # The state object really carries all retained levels, not only a
            # clock value; unchanged as-of indices select the identical arrays.
            values = np.concatenate([getattr(l2, name)[inside] for name in
                                     ("bid_px", "bid_qty", "ask_px", "ask_qty")], axis=1)
            if previous is not None:
                times, obs = np.r_[previous[0], times], np.r_[previous[1], obs]
                values = np.vstack([previous_values, values])
            ticks = np.arange(start, start+DAY_US, 100_000, dtype=np.int64)
            index = np.searchsorted(times, ticks, side="right")-1
            known = index >= 0
            age = ticks[known] - obs[index[known]]
            totals["observed_at_grid_rows"] += int((age == 0).sum())
            totals["carried_grid_rows"] += int((age > 0).sum())
            totals["unknown_grid_rows"] += int((~known).sum())
            totals["age_exceeds_500ms_rows"] += int((age > 500_000).sum())
            totals["age_exceeds_5s_rows"] += int((age > 5_000_000).sum())
            totals["age_exceeds_30s_rows"] += int((age > 30_000_000).sum())
            totals["midnight_inherited_days"] += int(previous is not None)
            if previous is not None and index[0] == 0 and not np.array_equal(values[0], previous_values):
                raise ValueError("midnight lost retained book levels")
            previous = (actual["last_output_us"], actual["last_observed_us"])
            previous_values = values[-1].copy()
            totals["days"] += 1
            totals["loaded_book_rows_including_warmup"] += len(bbo.ts_ms)
            totals["retained_book_rows"] += int(inside.sum())
            del bbo, l2, values
            gc.collect()
            if totals["days"] % 20 == 0:
                print(json.dumps({"consumer_days": totals["days"], "last_day": day}), flush=True)
    finally:
        replay.BBO_DIR, replay.L2_DIR = previous_bbo, previous_l2
        if old_max_levels is not None:
            os.environ["MM_L2_MAX_LEVELS"] = old_max_levels
    return {"status": "SHARED_LOADERS_CONTINUOUS_READ_VERIFIED", **dict(totals),
            "future_fill_violations": 0, "stale_age_reset_violations": 0,
            "grid_rows": len(days)*864000, "economic_replay": False,
            "raw_reconstruction": _reconstruction_summary(results),
            "observation_semantics": "as_of_exchange_clock_not_capture_completeness"}


def audit_book_calendar(records: list[dict], book_root: Path, output_dir: Path,
                        manifest_sha: str, *, resume=False) -> dict:
    """Bind real source clocks and accept all daily BBO/L2 rows + causal reads.

    Reuses retained normalized prices. Transaction-clock producer outputs are
    stamped with the actual last applied message clock (not bucket starts).
    Resampled producer outputs MUST have their original hash-bound sidecar.
    Timestamp membership is an additional source check, not proof of full raw
    book reconstruction or exchange queue identity.
    """
    source_manifest = book_root / "manifest.json"
    selection = json.loads(source_manifest.read_text())
    source_rows = selection["sources"]
    sources = {r["day"]: r for r in source_rows}
    days = [r["calendar_date"] for r in records]
    if len(sources) != len(source_rows):
        raise ValueError("duplicate selected book source date")
    if not days or days != _calendar_days(days[0], days[-1]):
        raise ValueError("book acceptance requires adjacent unique calendar dates")
    if not set(days).issubset(sources):
        raise ValueError("selected book sources do not cover the requested calendar")
    source_qualities = {}
    if selection.get("daily_quality_sha256"):
        source_quality_csv = book_root / "daily_quality.csv"
        if sha256_file(source_quality_csv) != selection["daily_quality_sha256"]:
            raise ValueError("selected producer quality manifest identity differs")
        with source_quality_csv.open(newline="") as stream:
            source_qualities = {r["day"]: r for r in csv.DictReader(stream)}
    run_identity = {"input_manifest_sha256": manifest_sha,
                    "book_manifest_sha256": sha256_file(source_manifest),
                    "reader_source_sha256": sha256_file(Path(__file__)),
                    "days": [r["calendar_date"] for r in records]}
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=resume)
    results_path = output_dir / "calendar.jsonl"
    state_path = output_dir / "state.json"
    results = []
    execution_history = []
    if resume and state_path.exists():
        state = json.loads(state_path.read_text())
        old_identity = state["identity"]
        # A validator upgrade may reuse a prefix only after current code
        # recomputes every causal grid and verifies all actual source/output
        # hashes. Input/selection/calendar changes still require a new run.
        if {k:v for k,v in old_identity.items() if k != "reader_source_sha256"} != {
                k:v for k,v in run_identity.items() if k != "reader_source_sha256"}:
            raise ValueError("resume identity changed")
        if results_path.exists():
            results = [json.loads(line) for line in results_path.read_text().splitlines()]
        if [r["calendar_date"] for r in results] != run_identity["days"][:len(results)]:
            raise ValueError("resume calendar is not an exact successful prefix")
        validate_book_prefix(records, results, book_root)
        execution_history = list(state.get("execution_history", []))
        if old_identity["reader_source_sha256"] != run_identity["reader_source_sha256"]:
            execution_history.append({"reader_source_sha256": old_identity["reader_source_sha256"],
                                      "completed_prefix_days": len(results),
                                      "prior_status": state["status"],
                                      "prior_error": state.get("error"),
                                      "prefix_revalidated_by_current_code": True})
    elif results_path.exists() or state_path.exists():
        raise FileExistsError("existing book acceptance output")
    previous = None
    previous_source = np.array([], dtype=np.int64)
    if results:
        last = results[-1]
        previous = (last["grid"]["last_output_us"], last["grid"]["last_observed_us"])
        last_record = records[len(results)-1]
        raw = next(c for c in last_record["channels"] if c["source_id"] == "btcusdc-daily-raw-l2")
        previous_source, _ = _source_times(Path(raw["files"][0]["path"]))
    for record in records[len(results):]:
        started = time.monotonic()
        day = record["calendar_date"]
        try:
            source = sources[day]
            raw = next(c for c in record["channels"] if c["source_id"] == "btcusdc-daily-raw-l2")
            raw_path = Path(raw["files"][0]["path"])
            source_times, raw_identity = _source_times(raw_path)
            expected_raw = raw.get("source_content_validation", {}).get("sha256")
            if expected_raw and raw_identity["sha256"] != expected_raw:
                raise ValueError("canonical raw differs from frozen content identity")
            raw_identity["prior_content_binding"] = "SHA_VERIFIED" if expected_raw else "UNBOUND"
            bbo_path = book_root / "bbo" / f"BTCUSDC-bbo-{day}.parquet"
            l2_path = book_root / "l2" / f"BTCUSDC-l2-{day}.parquet"
            bbo = scan_file(bbo_path, day)
            l2 = scan_file(l2_path, day)
            clock_path = book_root / "clock" / f"BTCUSDC-clock-{day}.parquet"
            independent = (pq.ParquetFile(raw_path).metadata.metadata.get(b"narrowgate.book_fusion") == DAILY_BOOK_MARKER.encode()
                           and "bbo_last_observation_timestamp_us" in pq.read_schema(clock_path).names)
            identity_keys = ("time", "top_time") if independent else ("time", "top_time", "top_bid", "top_ask")
            if any(bbo["top_book_digests"].get(k) != l2["top_book_digests"].get(k) for k in identity_keys):
                raise ValueError("BBO/L2 timestamp or top-price identity mismatch")
            if bbo["errors"] or l2["errors"]:
                raise ValueError(f"book content findings: {bbo['errors']} / {l2['errors']}")
            ts = _us(pq.read_table(bbo_path, columns=["timestamp"]).column(0).combine_chunks())
            preserved_clock = None
            fusion_quality = None
            reconstruction = "RETAINED_NOT_REBUILT"
            if source["source_clock"] == "fused_exchange":
                preserved_clock, fusion_quality, binding = _fusion_source_binding(
                    raw_path, raw_identity, source, source_qualities.get(day, {}), book_root, day, ts, bbo, l2)
                obs = np.asarray(preserved_clock["last_observation_timestamp_us"].to_numpy(), dtype=np.int64)
                reconstruction = binding["raw_representation"]
                if independent:
                    binding["independent_top"] = verify_separate_top_view(raw_path, bbo_path, l2_path, clock_path)
            elif source["source_clock"] == "tardis_exchange":
                origin = resolve_portable_path(source["root"])
                qpath = origin / "quality" / f"BTCUSDC-{day}.json"
                q = json.loads(qpath.read_text())
                expected_quality = source_qualities.get(day, {}).get("source_quality_sha256")
                if expected_quality and sha256_file(qpath) != expected_quality:
                    raise ValueError("retained producer quality differs from selection manifest")
                for kind, audit in (("bbo", bbo), ("l2", l2)):
                    if q[f"{kind}_output"]["sha256"] != audit["sha256"]:
                        raise ValueError(f"{kind} differs from producer clock binding")
                cp = origin / "clock" / f"BTCUSDC-clock-{day}.parquet"
                if sha256_file(cp) != q["clock_output"]["sha256"]:
                    raise ValueError("original clock sidecar hash mismatch")
                clock = pq.read_table(cp)
                cts = _us(clock["timestamp"].combine_chunks())
                if not np.array_equal(ts, cts):
                    raise ValueError("original clock axis mismatch")
                name = ("last_observation_timestamp_us" if "last_observation_timestamp_us" in clock.column_names
                        else "exchange_cut_timestamp_us")
                obs = np.asarray(clock[name].to_numpy(), dtype=np.int64)
                binding = {"kind": "HASH_BOUND_RESAMPLE_CLOCK", "producer_quality_sha256": sha256_file(qpath),
                           "retained_original_source_sha256": q.get("raw_inputs", {}).get("incremental_book_L2", {}).get("sha256"),
                           "canonical_original_source_sha256": raw.get("source_content_validation", {}).get("source_sha256")}
            elif source["source_clock"] == "transaction":
                origin = resolve_portable_path(source["root"])
                quality = source.get("source_quality") or {}
                for kind, audit in (("bbo", bbo), ("l2", l2)):
                    expected = quality.get(f"{kind}_sha256")
                    if expected is None:
                        original = origin / kind / f"BTCUSDC-{kind}-{day}.parquet"
                        expected = sha256_file(original)
                    if expected != audit["sha256"]:
                        raise ValueError(f"{kind} differs from transaction-clock producer output")
                obs = ts.copy()
                producer_source = (
                    Path(__file__).resolve().parents[1]
                    / "downloaders" / "cryptohft_orderbook.py"
                )
                binding = {"kind": "ACTUAL_LAST_MESSAGE_CLOCK_NOT_GRID_TIME",
                           "producer": "data.downloaders.cryptohft_orderbook._emit_snapshot",
                           "producer_source_sha256": sha256_file(producer_source)}
            else:
                raise ValueError("unrecognized producer clock semantics")
            # Selected outputs may retain previous-day warmup rows. Verify their
            # observation against the adjacent source too; filter only at query.
            available = np.union1d(previous_source, source_times)
            wanted = np.unique(obs)
            positions = np.searchsorted(available, wanted)
            matched = (positions < len(available))
            matched[matched] &= available[positions[matched]] == wanted[matched]
            original = binding.get("retained_original_source_sha256")
            canonical = binding.get("canonical_original_source_sha256")
            alternate_retained = bool(original and canonical and original != canonical
                                      and binding["kind"] == "HASH_BOUND_RESAMPLE_CLOCK")
            # Hourly capture partitions can place a pre-midnight exchange event
            # in the following file. Use that file only as provenance for events
            # strictly before this UTC day's end, never as future book state.
            next_index = days.index(day) + 1
            if not matched.all() and not alternate_retained and preserved_clock is None and next_index < len(records):
                adjacent = records[next_index]
                adjacent_raw = next(c for c in adjacent["channels"]
                                    if c["source_id"] == "btcusdc-daily-raw-l2")
                adjacent_path = Path(adjacent_raw["files"][0]["path"])
                adjacent_times, adjacent_identity = _source_times(adjacent_path)
                expected = adjacent_raw.get("source_content_validation", {}).get("sha256")
                if expected and adjacent_identity["sha256"] != expected:
                    raise ValueError("adjacent canonical source differs from frozen content identity")
                end = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()*1e6) + DAY_US
                boundary_times = adjacent_times[adjacent_times < end]
                available = np.union1d(available, boundary_times)
                positions = np.searchsorted(available, wanted)
                matched = positions < len(available)
                matched[matched] &= available[positions[matched]] == wanted[matched]
                binding["following_capture_partition"] = {
                    "calendar_date": adjacent["calendar_date"], "path": str(adjacent_path),
                    "sha256": adjacent_identity["sha256"],
                    "pre_midnight_distinct_clocks": len(boundary_times),
                    "use": "EXCHANGE_TIME_MEMBERSHIP_ONLY_NO_FUTURE_STATE",
                }
            if not matched.all() and not alternate_retained:
                raise ValueError(f"{int((~matched).sum())} output source clocks absent from canonical adjacent inputs; first={wanted[~matched][:5].tolist()}")
            binding["canonical_source_membership"] = (
                "DIFFERENT_HASH_BOUND_RETAINED_SOURCE" if alternate_retained
                else "FUSED_REAL_OBSERVATION_MEMBERSHIP_VERIFIED" if preserved_clock is not None
                else "CLOCK_MEMBERSHIP_VERIFIED")
            binding["canonical_clock_membership_missing"] = int((~matched).sum())
            grid = observation_grid(day, ts, obs, previous)
            if grid["future_fill_violations"] or grid["carry_age_violations"]:
                raise ValueError("causal as-of clock acceptance failed")
            clock_path = book_root / "clock" / f"BTCUSDC-clock-{day}.parquet"
            # Fusion already produced and hash-bound the source_id, observation
            # kind, age, and provider-local fields. Do not rewrite that triplet
            # or invalidate its selected same-pass quality during acceptance.
            quality_path = ((output_dir if preserved_clock is not None else book_root)
                            / "quality" / f"BTCUSDC-{day}.json")
            clock_path.parent.mkdir(exist_ok=True)
            quality_path.parent.mkdir(exist_ok=True)
            if preserved_clock is None:
                clock = pa.table({"timestamp": ts//1000,
                                  "last_observation_timestamp_us": obs,
                                  "observation_age_us": ts-obs,
                                  "observation_kind": np.where(np.r_[True, np.diff(obs) != 0],
                                                               "source_observed", "carried_forward"),
                                  "source_id": ["canonical.binance.usdm.BTCUSDC.book"] * len(ts)})
                tmp_clock = clock_path.with_suffix(".parquet.tmp")
                if tmp_clock.exists():
                    raise FileExistsError(tmp_clock)
                pq.write_table(clock, tmp_clock, compression="zstd")
                os.replace(tmp_clock, clock_path)
            quality = {**(fusion_quality or {}), "schema": "book_continuity_acceptance.v1", "day": day, "symbol": "BTCUSDC",
                       "observation_schema": "narrowgate.book_observation.v2",
                       "timestamp_source": "exchange", "source_clock_binding": binding,
                       "raw_source": raw_identity, "grid": grid,
                       "future_fill_violations": grid["future_fill_violations"],
                       "max_stale_age_us": grid["max_stale_age_us"],
                       "cross_channel_contract_valid": False,
                       "provider_normalized_replay_candidate": False,
                       "raw_reconstruction": reconstruction, "exact_queue": False,
                       "economic_admission": False,
                       "research_use": record["research_use"]}
            for kind, path, digest in (("bbo", bbo_path, bbo["sha256"]),
                                       ("l2", l2_path, l2["sha256"]),
                                       ("clock", clock_path, sha256_file(clock_path))):
                quality[kind+"_output"] = {"path": str(path), "sha256": digest,
                                           "size_bytes": path.stat().st_size}
            _atomic_json(quality_path, quality)
            result = {"calendar_date": day, "status": "CONTINUOUS_READ_ACCEPTED",
                      "rows": bbo["rows"], "raw_source": raw_identity,
                      "source_clock_binding": binding, "source_clock_membership_missing": int((~matched).sum()),
                      "quality_path": str(quality_path), "quality_sha256": sha256_file(quality_path),
                      "book_hashes": {"bbo": bbo["sha256"], "l2": l2["sha256"]},
                      "raw_reconstruction": reconstruction,
                      "grid": grid, "research_use": record["research_use"],
                      "elapsed_s": round(time.monotonic()-started, 3)}
            with results_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(result, allow_nan=False) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(results_path, 0o600)
            results.append(result)
            previous = (grid["last_output_us"], grid["last_observed_us"])
            previous_source = source_times
            _atomic_json(state_path, {"identity": run_identity, "completed": len(results),
                                      "status": "RUNNING", "last_day": day,
                                      "execution_history": execution_history})
            print(json.dumps({"day": day, "completed": len(results), "elapsed_s": result["elapsed_s"],
                              "max_age_us": grid["max_stale_age_us"]}), flush=True)
        except Exception as exc:
            _atomic_json(state_path, {"identity": run_identity, "completed": len(results),
                                      "status": "FAILED", "failed_day": day,
                                      "execution_history": execution_history,
                                      "error": f"{type(exc).__name__}: {exc}"})
            raise
    summary = {"identity": run_identity, "status": "COMPLETED", "days": len(results),
               "execution_history": execution_history,
               "grid_rows": sum(r["grid"]["grid_rows"] for r in results),
               "future_fill_violations": sum(r["grid"]["future_fill_violations"] for r in results),
               "carry_age_violations": sum(r["grid"]["carry_age_violations"] for r in results),
               "unknown_grid_rows": sum(r["grid"]["unknown_state_rows"] for r in results),
               "stale_grid_rows": sum(r["grid"]["stale_state_rows"] for r in results),
               "max_stale_age_us": max(r["grid"]["max_stale_age_us"] for r in results),
               "raw_reconstruction": _reconstruction_summary(results), "economic_replay": False}
    _atomic_json(state_path, summary)
    return summary


def _write_new(path: Path, payload: dict) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(payload, stream, allow_nan=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def _batches(path: Path, batch_size: int):
    if path.suffix == ".parquet":
        yield from pq.ParquetFile(path).iter_batches(batch_size=batch_size, use_threads=False)
    else:
        with pa.input_stream(str(path), compression="detect") as stream:
            yield from pacsv.open_csv(stream, read_options=pacsv.ReadOptions(block_size=8*1024*1024))


def _us(column: pa.Array) -> np.ndarray:
    values = column.to_numpy(zero_copy_only=False)
    if np.issubdtype(values.dtype, np.datetime64):
        return values.astype("datetime64[us]").astype(np.int64)
    values = values.astype(np.int64)
    median = np.median(np.abs(values))
    if median >= 1e17:
        return values // 1000
    if median < 1e14:
        return values * 1000
    return values


def scan_file(path: Path, day: str, *, batch_size: int = 131072, symbol: str = "BTCUSDC") -> dict:
    """Full decoding, structural/value checks; NOT an exchange book replay."""
    before = path.stat()
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()*1e6)
    counts = Counter()
    nonfinite = Counter()
    prev = {}
    extrema = {}
    digests = {}
    largest_gaps = []
    occupied = np.zeros(864000, dtype=bool)
    result = {"path": str(path), "size_bytes": before.st_size, "rows": 0,
              "errors": [], "future_fill_violations": UNKNOWN,
              "max_stale_age_us": UNKNOWN, "source_reconstruction": "NOT_RUN",
              "capture_completeness": UNKNOWN, "exact_queue": "NOT_PROVEN"}
    volume = 0.0
    volume_measured = False

    def sequence(name, values, *, unique=False):
        if not len(values):
            return
        previous = prev.get(name)
        delta = np.diff(values if previous is None else np.r_[previous, values])
        counts[name+"_reversals"] += int((delta < 0).sum())
        counts[name+"_adjacent_duplicates"] += int((delta == 0).sum())
        if unique:
            counts[name+"_nonincreasing"] += int((delta <= 0).sum())
        extrema.setdefault(name, [int(values[0]), int(values[0])])
        extrema[name][0] = min(extrema[name][0], int(values.min()))
        extrema[name][1] = max(extrema[name][1], int(values.max()))
        if name == "timestamp_us":
            gaps = delta[delta > 0]
            if len(gaps):
                result["max_internal_gap_us"] = max(result.get("max_internal_gap_us", 0), int(gaps.max()))
                for threshold in (1_000_000, 5_000_000):
                    counts[f"gaps_gt_{threshold}us"] += int((gaps > threshold).sum())
                take = np.flatnonzero(delta > 5_000_000)
                joined = values if previous is None else np.r_[previous, values]
                largest_gaps.extend((int(delta[i]), int(joined[i]), int(joined[i+1])) for i in take)
                largest_gaps.sort(reverse=True)
                del largest_gaps[10:]
        prev[name] = int(values[-1])

    for batch in _batches(path, batch_size):
        if not len(batch):
            continue
        cols = {name: batch.column(i) for i, name in enumerate(batch.schema.names)}
        result.setdefault("columns", list(cols))
        result["rows"] += len(batch)
        # Canonical trades also carry symbol and nullable local_timestamp. Their
        # taker side/volume/ID semantics differ from raw orderbook level updates.
        synthetic = "group_id" in cols and "bucket_start_ms" in cols
        trade = "id" in cols or "agg_trade_id" in cols or synthetic
        raw_market = "local_timestamp" in cols and "symbol" in cols
        raw_book = raw_market and not trade
        feature = "__index_level_0__" in cols
        time_name = next((n for n in ("timestamp", "transact_time", "time", "bucket_start_ms", "__index_level_0__") if n in cols), None)
        if time_name is None:
            raise ValueError("timestamp column missing")
        ts = _us(cols[time_name])
        digests.setdefault("time", hashlib.sha256()).update(ts.astype("<i8").tobytes())
        sequence("timestamp_us", ts, unique=not raw_book and not trade)
        inside = (ts >= start) & (ts < start+DAY_US)
        counts["timestamp_outside_day"] += int((~inside).sum())
        occupied[((ts[inside]-start)//100000).astype(int)] = True
        if raw_market:
            available = cols["local_timestamp"].is_valid().to_numpy(zero_copy_only=False)
            counts["local_timestamp_unobserved_rows"] += int((~available).sum())
            counts["local_timestamp_observed_rows"] += int(available.sum())
            if available.any():
                local = _us(cols["local_timestamp"].filter(pa.array(available)))
                sequence("local_timestamp_us", local)
                counts["local_outside_day"] += int(((local < start) | (local >= start+DAY_US)).sum())
                counts["exchange_after_receive"] += int((ts[available] > local).sum())
            counts["wrong_symbol"] += int(np.sum(cols["symbol"].to_numpy(zero_copy_only=False) != symbol))
            if "is_snapshot" in cols:
                counts["snapshot_rows"] += int(np.count_nonzero(cols["is_snapshot"].to_numpy(zero_copy_only=False)))
        for name, column in cols.items():
            counts["null_cells"] += column.null_count
            if pa.types.is_floating(column.type):
                values = column.to_numpy(zero_copy_only=False)
                nonfinite[name] += int((~np.isfinite(values)).sum())
        def number(name, cols=cols):
            return cols[name].to_numpy(zero_copy_only=False).astype(float)
        if synthetic:
            result.update(aggregation_kind="synthetic_100ms", native_aggtrade_identity=False,
                          execution_event_authority=False)
            first = number("first_event_ts_ms")
            last = number("last_event_ts_ms")
            begin = number("bucket_start_ms")
            end = number("bucket_end_ms")
            ready = number("feature_ready_ts_ms")
            counts["invalid_synthetic_interval"] += int((
                (end - begin != 100) | (begin % 100 != 0) | (ready != end)
                | (first < begin) | (last < first) | (last >= end)
                | ~np.isfinite(first + last + begin + end + ready)
            ).sum())
            counts["wrong_symbol"] += int(np.sum(cols["symbol"].to_numpy(zero_copy_only=False) != symbol))
            counts["invalid_trade_count"] += int((number("trade_count") <= 0).sum())
        for name in ("price", "amount", "quantity", "qty", "volume", "trade_count"):
            if name in cols:
                values = number(name)
                counts["invalid_"+name] += int((~np.isfinite(values) | (values <= 0 if name == "price" else values < 0)).sum())
        qty = next((n for n in ("qty", "quantity", "volume") if n in cols), None)
        if qty and not raw_book:
            volume_measured = True
            volume += float(np.nansum(number(qty)))
        for name in ("id", "agg_trade_id"):
            if name in cols:
                sequence(name, cols[name].to_numpy(zero_copy_only=False).astype(np.int64), unique=True)
        if "first_trade_id" in cols and "last_trade_id" in cols:
            counts["invalid_trade_id_range"] += int((number("last_trade_id") < number("first_trade_id")).sum())
        if "side" in cols:
            sides = cols["side"].to_numpy(zero_copy_only=False)
            counts["invalid_side"] += int((~np.isin(sides, ["buy", "sell"] if trade else ["bid", "ask"])).sum())
        pairs = [("best_bid", "best_ask"), ("bid_price", "ask_price"), ("bid_px_1", "ask_px_1")]
        for bid, ask in pairs:
            if bid in cols and ask in cols:
                b, a = number(bid), number(ask)
                counts["invalid_spread"] += int((~np.isfinite(b) | ~np.isfinite(a) | (b <= 0) | (a <= b)).sum())
                for key, values in (("top_bid", b), ("top_ask", a), ("top_time", ts)):
                    digests.setdefault(key, hashlib.sha256()).update(values.astype("<f8" if key != "top_time" else "<i8").tobytes())
        for side in ("bid", "ask"):
            last = None
            for level in range(1, 21):
                name = f"{side}_px_{level}"
                if name not in cols:
                    break
                values = number(name)
                qty_values = number(f"{side}_qty_{level}")
                valid = np.isfinite(values) & (values > 0)
                counts["missing_depth_cells"] += int((~valid).sum())
                counts["invalid_depth_quantity"] += int((~np.isfinite(qty_values) | (qty_values < 0)).sum())
                if last is not None:
                    counts["depth_order_violation"] += int((valid & np.isfinite(last) & (last > 0) & ((values >= last) if side == "bid" else (values <= last))).sum())
                last = values
        if all(n in cols for n in ("open", "high", "low", "close")):
            o, h, low, c = (number(n) for n in ("open", "high", "low", "close"))
            counts["invalid_ohlc"] += int((~np.isfinite(o+h+low+c) | (low <= 0) | (h < np.maximum(o, c)) | (low > np.minimum(o, c))).sum())
        if "last_event_ts_ms" in cols and not feature:
            event = _us(cols["last_event_ts_ms"])
            # Bars are timestamped at interval START, not their availability time.
            counts["bar_event_outside_interval"] += int(((event < ts) | (event >= ts+1_000_000)).sum())
        observation_name = ("last_observation_timestamp_us" if "last_observation_timestamp_us" in cols
                            else "exchange_cut_timestamp_us" if "exchange_resample_age_us" in cols
                            else "last_provider_local_timestamp_us" if "provider_visibility_delay_us" in cols
                            else None)
        if observation_name:
            obs = _us(cols[observation_name])
            sequence("last_observation_us", obs)
            age = ts-obs
            result["future_fill_violations"] = int(result.get("future_fill_violations", 0) if result["future_fill_violations"] != UNKNOWN else 0) + int((age < 0).sum())
            result["max_stale_age_us"] = max(0 if result["max_stale_age_us"] == UNKNOWN else result["max_stale_age_us"], int(age.max()))
            age_name = next((n for n in ("observation_age_us", "exchange_resample_age_us", "provider_visibility_delay_us") if n in cols), None)
            if age_name:
                counts["observation_age_mismatch"] += int((number(age_name) != age).sum())
    result.update(counts=dict(counts), nonfinite_by_column={k:v for k,v in nonfinite.items() if v},
                  time_and_id_ranges=extrema, occupied_100ms_buckets=int(occupied.sum()),
                  occupied_100ms_ratio=float(occupied.mean()), largest_internal_gaps=largest_gaps,
                  top_book_digests={k:v.hexdigest() for k,v in digests.items()},
                  volume_sum=volume if volume_measured else "NOT_APPLICABLE")
    if "local_timestamp_unobserved_rows" in counts:
        result["receive_clock_status"] = (
            UNKNOWN if not counts["local_timestamp_observed_rows"] else
            "PARTIALLY_OBSERVED" if counts["local_timestamp_unobserved_rows"] else "OBSERVED"
        )
    if "timestamp_us" in extrema:
        first, last = extrema["timestamp_us"]
        result.update(start_gap_us=max(0, first-start), end_gap_us=max(0, start+DAY_US-last))
    result["sha256"] = sha256_file(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        result["errors"].append("INPUT_CHANGED_DURING_SCAN")
    if result["rows"] == 0:
        result["errors"].append("EMPTY")
    if result["future_fill_violations"] != UNKNOWN and result["future_fill_violations"]:
        result["errors"].append("future_fill_violations")
    for name, count in counts.items():
        if count and (name.startswith("invalid_") or name in {"wrong_symbol", "invalid_side", "depth_order_violation", "id_nonincreasing", "agg_trade_id_nonincreasing", "observation_age_mismatch", "bar_event_outside_interval"}):
            result["errors"].append(name)
    result["status"] = "CONTENT_FINDINGS" if result["errors"] else "CONTENT_READABLE"
    return result


def scan_funding(path: Path, day: str, *, symbol: str = "BTCUSDC") -> dict:
    rows = pq.read_table(path).to_pylist() if path.suffix == ".parquet" else json.loads(path.read_text())
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp()*1000)
    selected = [r for r in rows if start <= int(r["fundingTime"]) < start+86400000]
    ids = [(r["symbol"], int(r["fundingTime"])) for r in selected]
    valid = bool(selected) and len(set(ids)) == len(ids) and all(
        r["symbol"] == symbol and r.get("fundingRate") is not None
        and np.isfinite(float(r["fundingRate"])) and r.get("markPrice") is not None
        and np.isfinite(float(r["markPrice"])) and float(r["markPrice"]) > 0 for r in selected)
    return {"path": str(path), "sha256": sha256_file(path), "rows": len(selected),
            "settlement_times_ms": [t for _, t in ids], "duplicate_ids": len(ids)-len(set(ids)),
            "status": "CONTENT_READABLE" if valid else "CONTENT_FINDINGS",
            "errors": [] if valid else ["funding_identity_or_values"],
            "expected_schedule_completeness": UNKNOWN}


def audit_trade_relationships(record: dict, output_dir: Path, manifest_sha: str,
                              records_by_day: dict) -> dict:
    """Audit every day's parents and children with adjacent-day ID context.

    This is source reconciliation, not construction of policy-visible events.
    Parents are accounted on their own file day; borrowed children are context,
    not added to that day's individual-trade denominator.
    """
    from decimal import Decimal

    import pandas as pd

    from research.families.f08_side_taker_lifecycle.audit.binance_trade_mapping import (
        build_individual_aggtrade_mapping,
    )

    day = record["calendar_date"]
    result = {"visibility": "local_only_do_not_publish", "calendar_date": day,
              "input_manifest_sha256": manifest_sha, "inputs": [], "context_inputs": [],
              "research_use": record.get("research_use", {}), "errors": [],
              "created_at_utc": datetime.now(UTC).isoformat(),
              "scope": "TRADE_ID_VALUE_RELATION_NO_POLICY_CLOCK_OR_ECONOMIC_ADMISSION"}
    channels = ("btcusdc-raw-trades", "btcusdc-raw-aggtrades")
    unavailable = _native_mapping_unavailable(record)
    if unavailable:
        result.update(status="UNAVAILABLE", trade_mapping=unavailable)
        _write_new(output_dir / f"{day}.json", result)
        return result

    def read(source_record, channel, low=None, high=None):
        source = next(c for c in source_record["channels"] if c["source_id"] == channel)
        if source.get("symbol", "BTCUSDC") != "BTCUSDC" or source.get(
                "market", "usd_m_perpetual") != "usd_m_perpetual":
            raise ValueError("trade relationship market/symbol mismatch")
        if len(source["files"]) != 1:
            raise ValueError("trade relationship requires one canonical file per channel/day")
        path = Path(source["files"][0]["path"])
        before = path.stat()
        identity = {"calendar_date": source_record["calendar_date"], "source_id": channel,
                    "path": str(path), "sha256": sha256_file(path), "size_bytes": before.st_size}
        key = "id" if channel == channels[0] else "first_trade_id"
        end_key = key if channel == channels[0] else "last_trade_id"
        filters = [(end_key, ">=", int(low)), (key, "<=", int(high))] if low is not None else None
        if path.suffix == ".parquet":
            names = pq.ParquetFile(path).schema_arrow.names
            needed = {"id", "price", "qty", "quantity", "time", "is_buyer_maker", "timestamp",
                      "agg_trade_id", "first_trade_id", "last_trade_id", "transact_time",
                      "normal_quantity", "nq", "symbol", "exchange"}
            frame = pd.read_parquet(path, columns=[n for n in names if n in needed], filters=filters)
        else:
            frame = pd.read_csv(path)
            if filters:
                frame = frame.loc[(frame[end_key] >= low) & (frame[key] <= high)]
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError("trade input changed during reconciliation")
        if "symbol" in frame and not frame["symbol"].eq("BTCUSDC").all():
            raise ValueError("trade rows contain another symbol")
        if "exchange" in frame and not frame["exchange"].eq("binance-futures").all():
            raise ValueError("trade rows contain another venue")
        time_key = "time" if channel == channels[0] else "transact_time"
        if "timestamp" in frame and not np.array_equal(
                pd.to_numeric(frame["timestamp"]).to_numpy(),
                pd.to_numeric(frame[time_key]).to_numpy() * 1000):
            raise ValueError("canonical exchange clock differs from millisecond alias")
        start_ms = int(datetime.fromisoformat(source_record["calendar_date"]).replace(tzinfo=UTC).timestamp()*1000)
        if not pd.to_numeric(frame[time_key]).between(start_ms, start_ms+86_400_000-1).all():
            raise ValueError("trade exchange timestamp outside its UTC file day")
        return frame, identity

    try:
        individual, child_identity = read(record, channels[0])
        aggregate, parent_identity = read(record, channels[1])
        result["inputs"] = [child_identity, parent_identity]
        result["target_individual_rows"] = len(individual)
        result["target_aggregate_rows"] = len(aggregate)
        if individual.empty or aggregate.empty:
            raise ValueError("empty trade source is not proof of no trades")
        ids = pd.to_numeric(individual.id).to_numpy(dtype=np.int64)
        first = pd.to_numeric(aggregate.first_trade_id).to_numpy(dtype=np.int64)
        last = pd.to_numeric(aggregate.last_trade_id).to_numpy(dtype=np.int64)
        result["target_id_ranges"] = {"individual": [int(ids.min()), int(ids.max())],
                                      "aggregate_children": [int(first.min()), int(last.max())],
                                      "aggregate": [int(aggregate.agg_trade_id.min()), int(aggregate.agg_trade_id.max())]}
        result["target_source_order"] = {"individual_id_nonincreasing": int((np.diff(ids) <= 0).sum()),
                                         "aggregate_id_nonincreasing": int((np.diff(aggregate.agg_trade_id.to_numpy(dtype=np.int64)) <= 0).sum())}
        child_parts = {day: individual.loc[(ids >= first.min()) & (ids <= last.max())]}
        parent_parts = {day: aggregate}
        result["adjacent_context"] = {}
        for offset in (-1, 1):
            adjacent = (datetime.fromisoformat(day) + timedelta(days=offset)).date().isoformat()
            neighbor = records_by_day.get(adjacent)
            if neighbor is None:
                result["adjacent_context"][adjacent] = "OUTSIDE_REGISTERED_CALENDAR"
                continue
            children, ci = read(neighbor, channels[0], first.min(), last.max())
            parents, pi = read(neighbor, channels[1], ids.min(), ids.max())
            child_parts[adjacent] = children
            parent_parts[adjacent] = parents
            result["context_inputs"].extend([ci, pi])
            result["adjacent_context"][adjacent] = {"borrowed_children": len(children), "covering_parents": len(parents)}
        all_parents = pd.concat([parent_parts[k] for k in sorted(parent_parts)], ignore_index=True)
        all_parents = all_parents.sort_values("first_trade_id", kind="stable")
        starts = all_parents.first_trade_id.to_numpy(dtype=np.int64)
        ends = all_parents.last_trade_id.to_numpy(dtype=np.int64)
        if all_parents.agg_trade_id.duplicated().any() or (starts[1:] <= ends[:-1]).any():
            raise ValueError("adjacent-day aggregate IDs duplicate or ranges overlap")
        positions = np.searchsorted(starts, ids, side="right") - 1
        covered = (positions >= 0) & (ids <= ends[np.maximum(positions, 0)])
        adjacent_ids = set(pd.concat([p for k,p in parent_parts.items() if k != day], ignore_index=True).agg_trade_id) if len(parent_parts) > 1 else set()
        result["target_child_coverage"] = {
            "mapped": int(covered.sum()), "unmapped": int((~covered).sum()),
            "unmapped_id_examples": [int(v) for v in ids[~covered][:8]],
            "unmapped_quantity_btc": str(sum(
                (Decimal(str(v)) for v in individual.loc[
                    ~covered, "qty" if "qty" in individual else "quantity"]), Decimal(0))),
            "mapped_to_adjacent_day_parent": int(all_parents.iloc[positions[covered]].agg_trade_id.isin(adjacent_ids).sum()),
        }
        children = pd.concat([child_parts[k] for k in sorted(child_parts)], ignore_index=True)
        mapped, summary = build_individual_aggtrade_mapping(children, aggregate, raise_on_findings=False)
        del mapped
        result["trade_mapping"] = summary
        result["borrowed_individual_rows"] = sum(len(p) for k,p in child_parts.items() if k != day)
        if summary["status"] != "passed" or not covered.all():
            result["errors"].append("individual_aggregate_relation")
        if any(result["target_source_order"].values()):
            result["errors"].append("source_id_order")
    except (OSError, ValueError, KeyError, IndexError, StopIteration) as exc:
        result["errors"].append(f"{type(exc).__name__}: {exc}")
    result["status"] = "CONTENT_FINDINGS" if result["errors"] else "CONTENT_READABLE"
    _write_new(output_dir / f"{day}.json", result)
    print(json.dumps({"day": day, "trade_relationship_status": result["status"]}), flush=True)
    return result


def _native_mapping_unavailable(record: dict) -> dict | None:
    source = next((c for c in record["channels"]
                   if c["source_id"] == "btcusdc-raw-aggtrades"), None)
    reason = None
    if source is None:
        reason = "NATIVE_AGGREGATE_SOURCE_NOT_REGISTERED"
    elif source.get("lifecycle") in {"retired", "historical"}:
        reason = "NATIVE_AGGREGATE_SOURCE_RETIRED"
    elif source.get("native_aggtrade_identity") is False or source.get("aggregation_kind") == "synthetic_100ms":
        reason = "SYNTHETIC_BUCKETS_ARE_NOT_NATIVE_F_L_BLOCKS"
    else:
        for item in source.get("files", []):
            path = Path(item["path"])
            if path.is_file() and path.suffix == ".parquet":
                try:
                    schema = pq.ParquetFile(path).schema_arrow
                except (OSError, ValueError):
                    # The normal read path records this source error; failure
                    # to inspect a footer is not evidence of a retired source.
                    continue
                if "group_id" in schema.names or (schema.metadata or {}).get(b"native_aggtrade_identity") == b"false":
                    reason = "SYNTHETIC_BUCKETS_ARE_NOT_NATIVE_F_L_BLOCKS"
    return ({"status": "UNAVAILABLE", "reason": reason,
             "native_aggtrade_identity": False, "synthetic_substitution": False}
            if reason else None)


def _synthetic_conservation(individual: Path, aggregate: Path, day: str) -> dict:
    """Recompute deterministic flow buckets, never exchange-native parents."""
    from decimal import Decimal

    from data.trade_aggregation import AGGREGATE_SCHEMA, build_aggregates

    trades = pq.read_table(individual)
    expected = build_aggregates(trades, day).to_pandas().set_index("group_id")
    observed = pq.read_table(aggregate)
    if not set(AGGREGATE_SCHEMA.names).issubset(observed.column_names):
        raise ValueError("derived aggregate schema incomplete")
    actual = observed.select(AGGREGATE_SCHEMA.names).to_pandas().set_index("group_id")
    duplicates = int(actual.index.duplicated().sum())
    counts = {"duplicate_groups": duplicates,
              "missing_groups": int((~expected.index.isin(actual.index)).sum()),
              "extra_groups": int((~actual.index.isin(expected.index)).sum())}
    if not duplicates:
        common = expected.index.intersection(actual.index)
        for field in expected.columns:
            a, b = expected.loc[common, field], actual.loc[common, field]
            if field in {"price", "quantity"}:
                a, b = a.map(Decimal), b.map(Decimal)
            counts[field + "_mismatch"] = int(a.ne(b).sum())
    return {"status": "CONTENT_FINDINGS" if any(counts.values()) else "CONTENT_READABLE",
            "native_aggtrade_identity": False, "comparison": "INDIVIDUAL_TO_SYNTHETIC_100MS",
            "individual_rows": trades.num_rows, "synthetic_rows": len(actual),
            "expected_synthetic_rows": len(expected),
            "individual_quantity_btc": str(sum((Decimal(v) for v in trades["qty"].to_pylist()), Decimal(0))),
            "synthetic_quantity_btc": str(sum((Decimal(v) for v in actual.quantity), Decimal(0))),
            "synthetic_child_count": int(actual.trade_count.sum()), "counts": counts}


def audit_relationships(record: dict, output_dir: Path, manifest_sha: str) -> dict:
    """Reuse the maintained ID mapper; compare supplied bars to actual trades.

    No mapped rows, policy-visible clocks, labels or economic outputs are saved.
    """
    import pandas as pd

    from research.families.f08_side_taker_lifecycle.audit.binance_trade_mapping import (
        build_individual_aggtrade_mapping,
    )

    day = record["calendar_date"]
    sources = {c["source_id"]: c["files"] for c in record["channels"]}
    result = {"visibility": "local_only_do_not_publish", "calendar_date": day,
              "input_manifest_sha256": manifest_sha, "errors": [], "inputs": []}
    try:
        synthetic = DERIVED_AGGREGATE_ID in sources
        unavailable = _native_mapping_unavailable(record)
        ids = ["btcusdc-raw-trades", "btcusdc-selected-bars401"]
        if synthetic:
            ids.append(DERIVED_AGGREGATE_ID)
        elif not unavailable:
            ids.append("btcusdc-raw-aggtrades")
        if any(len(sources[key]) != 1 for key in ids):
            raise ValueError("relationship audit requires one file per selected channel/day")
        paths = [Path(sources[key][0]["path"]) for key in ids]
        result["inputs"] = [{"path": str(p), "sha256": sha256_file(p)} for p in paths]
        individual = pd.read_parquet(paths[0]) if paths[0].suffix == ".parquet" else pd.read_csv(paths[0])
        if synthetic or unavailable:
            result["trade_mapping"] = unavailable or {
                "status": "UNAVAILABLE", "reason": "SYNTHETIC_CONTRACT_DOES_NOT_AUDIT_NATIVE_PARENTS",
                "native_aggtrade_identity": False, "synthetic_substitution": False}
            if synthetic:
                result["synthetic_conservation"] = _synthetic_conservation(paths[0], paths[2], day)
                if result["synthetic_conservation"]["status"] != "CONTENT_READABLE":
                    result["errors"].append("synthetic_conservation")
            aggregate = individual.rename(columns={"qty": "quantity", "time": "transact_time"})
            result["bar_source_semantics"] = "individual_trades"
        else:
            aggregate = pd.read_parquet(paths[2]) if paths[2].suffix == ".parquet" else pd.read_csv(paths[2])
            try:
                mapped, summary = build_individual_aggtrade_mapping(individual, aggregate)
                result["trade_mapping"] = {k: summary[k] for k in (
                    "status", "individual_rows", "mapped_individual_rows", "aggregate_rows",
                    "mapped_aggregate_rows", "exact_trade_id_range_aggregate_rows",
                    "nonexact_trade_id_range_aggregate_rows", "quantity_identity_counts")}
                del mapped
            except ValueError as exc:
                result["trade_mapping"] = {"status": "FINDING", "reason": str(exc)}
                result["errors"].append("individual_aggregate_mapping")
            result["bar_source_semantics"] = "native_aggregate_trades"
        # Canonical prices/quantities preserve source decimal strings. Convert
        # only this numeric audit view; do not concatenate strings in sums or
        # take lexicographic OHLC extrema.
        aggregate = aggregate.copy()
        for name in ("price", "quantity"):
            aggregate[name] = pd.to_numeric(aggregate[name], errors="raise")
        group = aggregate.assign(second=aggregate.transact_time//1000).groupby("second", sort=True)
        expected = group.agg(open=("price", "first"), high=("price", "max"),
                             low=("price", "min"), close=("price", "last"),
                             volume=("quantity", "sum"), trade_count=("price", "size"))
        bars = pd.read_parquet(paths[1])
        if "timestamp" in bars.columns:
            bar_time = pa.array(bars["timestamp"])
        elif bars.index.name == "timestamp" or isinstance(bars.index, pd.DatetimeIndex):
            bar_time = pa.array(bars.index)
        else:
            raise ValueError("bar timestamp column/index missing")
        bars.index = _us(bar_time)//1_000_000
        counts = {"missing_trade_seconds": int((~expected.index.isin(bars.index)).sum()),
                  "bar_seconds_without_recorded_trades": int((~bars.index.isin(expected.index)).sum()),
                  "duplicate_bar_seconds": int(bars.index.duplicated().sum())}
        if not counts["duplicate_bar_seconds"]:
            common = expected.index.intersection(bars.index)
            for field in ("open", "high", "low", "close", "volume", "trade_count"):
                counts[field+"_mismatch"] = int((~np.isclose(expected.loc[common, field], bars.loc[common, field], rtol=1e-9, atol=1e-8)).sum())
        result["bar_trade_identity"] = counts
        if any(v for k, v in counts.items() if k != "bar_seconds_without_recorded_trades"):
            result["errors"].append("bar_trade_identity")
    except (OSError, ValueError, KeyError, IndexError) as exc:
        result["errors"].append(f"{type(exc).__name__}: {exc}")
    result["status"] = "CONTENT_FINDINGS" if result["errors"] else "CONTENT_READABLE"
    _write_new(output_dir / f"{day}.json", result)
    print(json.dumps({"day": day, "relationship_status": result["status"]}), flush=True)
    return result


def audit_day(record: dict, output_dir: Path, manifest_sha: str) -> dict:
    day = record["calendar_date"]
    output = output_dir / f"{day}.json"
    if output.exists():
        raise FileExistsError(output)
    started = time.monotonic()
    channels = []
    for channel in record["channels"]:
        files = []
        for item in channel["files"]:
            try:
                path = Path(item["path"])
                symbol = channel.get("symbol", "BTCUSDC")
                value = scan_funding(path, day, symbol=symbol) if channel["channel"] == "funding" else scan_file(path, day, symbol=symbol)
            except Exception as exc:
                value = {"path": item["path"], "status": "BLOCKED", "error": f"{type(exc).__name__}: {exc}"}
            files.append(value)
        channels.append({"source_id": channel["source_id"], "files": files,
                         "status": "MISSING" if not files else "BLOCKED" if any(f["status"] == "BLOCKED" for f in files) else "CONTENT_FINDINGS" if any(f["status"] == "CONTENT_FINDINGS" for f in files) else "CONTENT_READABLE",
                         "historical_quality": channel.get("quality", {}),
                         "economic_admission": "NOT_GRANTED"})
        quality = channel.get("quality", {})
        if channel["source_id"].endswith("selected-book") and quality.get("path", "").endswith(".json"):
            qpath = Path(quality["path"])
            clock_check = {"status": "UNBOUND_OR_MISSING", "future_fill_violations": UNKNOWN}
            if qpath.is_file() and sha256_file(qpath) == quality.get("sha256"):
                q = json.loads(qpath.read_text())
                claim = q.get("clock_output", {})
                # Relocation is accepted only after content identity comparison.
                candidates = [Path(claim.get("path", "")), qpath.parent.parent / "clock" / Path(claim.get("path", "missing")).name]
                cp = next((p for p in candidates if p.is_file()), None)
                outputs_bound = len(files) == 2 and all(f.get("sha256") == q.get(k, {}).get("sha256") for f, k in zip(files, ("bbo_output", "l2_output"), strict=True))
                if cp and outputs_bound:
                    clock_check = scan_file(cp, day)
                    clock_check["hash_bound"] = clock_check["sha256"] == claim.get("sha256")
                    clock_check["time_identity"] = clock_check["top_book_digests"]["time"] == files[0]["top_book_digests"]["time"]
            channels[-1]["source_clock_content"] = clock_check
    book = next((c for c in channels if c["source_id"].endswith("selected-book")), {})
    bp = [f.get("top_book_digests") for f in book.get("files", [])]
    result = {"visibility": "local_only_do_not_publish", "calendar_date": day,
              "input_manifest_sha256": manifest_sha, "created_at_utc": datetime.now(UTC).isoformat(),
              "channels": channels, "research_use": record["research_use"],
              "bbo_l2_content_identity": bp[0] == bp[1] if len(bp) == 2 and all(bp) else UNKNOWN,
              "account_continuity": "NOT_TESTED", "elapsed_s": time.monotonic()-started}
    _write_new(output, result)
    print(json.dumps({"day": day, "elapsed_s": round(result["elapsed_s"], 2),
                      "statuses": dict(Counter(c["status"] for c in channels))}), flush=True)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--start-day", required=True)
    parser.add_argument("--end-day", required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--current-summary", type=Path,
                        help="Existing private summary to update atomically in book-clock-only mode")
    parser.add_argument("--workers", type=int, default=2)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--relationships-only", action="store_true")
    mode.add_argument("--trade-mapping-only", action="store_true",
                      help="Reconcile trades/aggTrades by ID with adjacent registered days; no bars or strategy")
    mode.add_argument("--book-continuity-only", action="store_true",
                      help="Bind source observation clocks and audit the complete causal calendar; no economics")
    mode.add_argument("--book-clock-only", action="store_true",
                      help="Scan bound clocks and shared clock reader across all days; no raw/price reconstruction")
    parser.add_argument("--book-root", type=Path)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args(argv)
    manifest = json.loads(args.manifest.read_text())
    (validate_clock_manifest if args.book_clock_only else validate_readability_manifest)(manifest)
    days = _calendar_days(args.start_day, args.end_day)
    rows = [r for r in manifest["records"] if r["calendar_date"] in days]
    if [r["calendar_date"] for r in rows] != days or args.workers < 1:
        raise ValueError("requested full calendar not present or invalid workers")
    if args.book_clock_only:
        if not args.book_root or not args.current_summary or args.output_dir or args.resume or args.workers != 1:
            raise ValueError("book-clock-only requires book-root/current-summary/workers=1; no output-dir/resume")
        if not args.current_summary.is_file():
            raise ValueError("current-summary must already exist")
        digest = sha256_file(args.manifest)
        result = audit_book_clocks(rows, args.book_root, manifest_sha=digest)
        if sha256_file(args.manifest) != digest:
            raise ValueError("readability manifest changed during clock acceptance")
        summary = json.loads(args.current_summary.read_text())
        summary["calendar_clock_validation"] = result
        _atomic_json(args.current_summary, summary)
        print(json.dumps({key: value for key, value in result.items() if key not in {"records", "calendar_dates"}}), flush=True)
        return 0
    if args.current_summary or not args.output_dir:
        raise ValueError("other modes require output-dir and do not accept current-summary")
    if args.book_continuity_only:
        if not args.book_root or args.workers != 1:
            raise ValueError("book continuity requires --book-root and --workers 1")
        audit_book_calendar(rows, args.book_root, args.output_dir,
                            sha256_file(args.manifest), resume=args.resume)
        return 0
    if args.resume or args.book_root:
        raise ValueError("resume/book-root apply only to book-continuity-only")
    args.output_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    digest = sha256_file(args.manifest)
    records_by_day = {r["calendar_date"]: r for r in manifest["records"]}
    audit = (lambda r, o, d: audit_trade_relationships(r, o, d, records_by_day)) if args.trade_mapping_only else audit_relationships if args.relationships_only else audit_day
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(lambda r: audit(r, args.output_dir, digest), rows))
    _write_new(args.output_dir / "summary.json", {
        "visibility": "local_only_do_not_publish", "calendar_start": days[0],
        "calendar_end": days[-1], "days": len(results), "input_manifest_sha256": digest,
        "counts": dict(Counter(r["status"] for r in results)) if args.relationships_only or args.trade_mapping_only else dict(Counter(c["status"] for r in results for c in r["channels"])),
        "scope": "FULL_DECODE_VALUE_CHECKS_NOT_ECONOMIC_OR_EXACT_QUEUE_ADMISSION"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
