"""Outcome-blind, non-admitting calendar inventory of registered daily sources.

Reuse Studio's explicit local inventory, not its UI date range or quality-day
filter. Only metadata and Parquet footers are read; raw streams are not replayed.
Unknown observation coverage is intentionally different from file presence.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from data.quality.calendar_gap_manifest import _calendar_days, sha256_file
from narrowgate.studio_quality import _observe_inventory

UNKNOWN = "UNKNOWN"
# A projection, not a copy of arbitrary audit/report fields.
AUDIT_FIELDS = (
    "raw_ok", "encoding_verified", "rows", "row_count", "file_size_bytes",
    "first_trade_id", "last_trade_id", "repeated_adjacent_id_count",
    "non_monotonic_id_count", "duplicate_file_for_day", "first_time_ms",
    "last_time_ms", "normalized_min_coverage", "source_max_gap_us",
    "source_invalid_spread_buckets", "formal_eligible", "exact_queue_policy_eligible",
    "clock_source", "count", "valid_observed_settlements", "ohlc_time_valid",
)
QUALITY_FIELDS = (
    "schema_version", "source_id", "clock_source", "complete_day", "gap_policy",
    "output_start_us", "output_end_us", "emitted_rows", "possible_rows",
    "source_observed_rows", "carried_forward_rows", "causal_violations",
    "bucket_density", "freshness_union_coverage", "invalid_spread_buckets",
    "continuation_inherited", "continuation_requested",
    "last_observed_exchange_timestamp_us", "last_observed_provider_timestamp_us",
    "cross_channel_contract_valid", "provider_normalized_replay_candidate",
)


def _csv_rows(path: Path) -> dict:
    with path.open(newline="", encoding="utf-8-sig") as stream:
        result = {}
        for row in csv.DictReader(stream):
            day = row.get("day") or row.get("calendar_date")
            if not day or day in result:
                raise ValueError("missing/duplicate date in metadata CSV")
            result[day] = row
        return result


def _defined(value):
    return UNKNOWN if value is None or value == "" else value


def _quality(row: dict) -> dict:
    """Read only a hash-bound selected quality sidecar, never economic payloads."""
    if not row.get("source_quality_path"):
        return {"status": "NOT_REGISTERED"}
    path = Path(row["source_quality_path"])
    if not path.is_file():
        return {"status": "MISSING"}
    digest = sha256_file(path)
    if digest != row.get("source_quality_sha256"):
        return {"status": "IDENTITY_MISMATCH", "sha256": digest}
    if path.suffix == ".csv":
        selected = _csv_rows(path).get(row.get("day"), {})
        return {"status": "RECORDED_CSV_HASH_VERIFIED", "sha256": digest,
                "path": str(path), "fields": {
                    k: _defined(selected.get(k)) for k in AUDIT_FIELDS}}
    try:
        payload = json.loads(path.read_text())
    except (ValueError, OSError) as exc:
        return {"status": "UNREADABLE_METADATA", "sha256": digest,
                "error_type": type(exc).__name__}
    result = {k: _defined(payload.get(k)) for k in QUALITY_FIELDS}
    result.update(status="RECORDED_HASH_VERIFIED", sha256=digest, path=str(path))
    result["logical_message_max_gap_us"] = _defined(
        payload.get("logical_message_gap", {}).get("maximum_us")
    )
    result["output_bindings"] = {
        name: payload.get(f"{name}_output", {}) for name in ("bbo", "l2", "clock")
    }
    return result


def _channel(spec: dict, day: str, audit: dict) -> dict:
    inv = [i for i in spec.get("inventories", ()) if i.get("node") == "local"]
    if len(inv) > 1:
        raise ValueError("multiple local inventories must be resolved explicitly")
    observation = _observe_inventory(inv[0], day, spec["symbol"], spec["version"]) if inv else {}
    files = []
    for path, size, mtime, _inode, _device in observation.get("snapshot") or ():
        item = {"path": path, "size_bytes": size, "mtime_ns": mtime}
        if path.endswith(".parquet"):
            try:
                footer = pq.ParquetFile(path)
                item.update(reader="PARQUET_FOOTER_READABLE", rows=footer.metadata.num_rows,
                            columns=footer.schema_arrow.names)
            except (OSError, ValueError) as exc:
                item.update(reader="BLOCKED", error_type=type(exc).__name__)
        else:
            item["reader"] = "NOT_READ_RAW_STREAM"
        files.append(item)
    quality = _quality(audit)
    present = observation.get("status") == "present_unverified"
    readable = ("METADATA_READABLE" if present and files and
                all(f["reader"] == "PARQUET_FOOTER_READABLE" for f in files)
                else "PRESENT_CONTENT_NOT_READ" if present else "BLOCKED_OR_UNREGISTERED")
    if any(f["reader"] == "BLOCKED" for f in files):
        readable = "BLOCKED"
    # Recorded quality is not current-file verification. Match each output to
    # current stat; a full content rehash is deliberately a separate operation.
    bindings = quality.pop("output_bindings", {})
    binding_matches = []
    for output in bindings.values():
        if output.get("path"):
            path = Path(output["path"])
            binding_matches.append(path.is_file() and path.stat().st_size == output.get("size_bytes"))
    quality["recorded_output_sizes_match"] = all(binding_matches) if binding_matches else UNKNOWN
    return {
        "source_id": spec["id"], "provider": spec["source"],
        "exchange": spec["exchange"], "market": spec["market"], "symbol": spec["symbol"],
        "channel": spec["data_type"], "stage": spec.get("stage", UNKNOWN),
        "lifecycle": spec.get("lifecycle", "current"),
        "aggregation_kind": spec.get("aggregation_kind", UNKNOWN),
        "native_aggtrade_identity": spec.get("native_aggtrade_identity", UNKNOWN),
        "version": spec["version"], "files": files, "reader": readable,
        "reader_reason": observation.get("reason", "NO_LOCAL_INVENTORY"),
        "raw_observed_coverage": UNKNOWN, "secondary_source_coverage": UNKNOWN,
        "causal_carried_coverage": UNKNOWN, "explicit_missing_coverage": UNKNOWN,
        "unknown_missing_coverage": UNKNOWN, "max_stale_age_us": UNKNOWN,
        "max_no_new_observation_us": _defined(audit.get("source_max_gap_us")),
        "future_fill_violations": UNKNOWN,
        "deduplication": {k: _defined(audit.get(k)) for k in
                          ("duplicate_file_for_day", "repeated_adjacent_id_count",
                           "non_monotonic_id_count")},
        "historical_audit": {k: _defined(audit.get(k)) for k in AUDIT_FIELDS},
        "quality": quality,
        "cross_day_data_state": UNKNOWN,
        "economic_admission": "NOT_GRANTED",
    }


def build_readability_manifest(owner_manifest: Path, *, start_day: str,
                               as_of: datetime, dataset_ids: list[str],
                               usage_sources: list[dict] | None = None) -> dict:
    if as_of.tzinfo is None:
        raise ValueError("as_of must be timezone-aware")
    today = as_of.astimezone(UTC).date()
    end = (today - timedelta(days=1)).isoformat()
    owner = json.loads(owner_manifest.read_text())
    window_policy = owner.get("calendar_window_policy", "last_complete_utc_day")
    if window_policy == "owner_fixed":
        if start_day != owner.get("start_day"):
            raise ValueError("start day differs from the owner-fixed calendar")
        end = owner["end_day"]
        if date.fromisoformat(end) >= today:
            raise ValueError("owner-fixed end must be a complete UTC day")
    elif window_policy != "last_complete_utc_day":
        raise ValueError("unknown calendar window policy")
    days = _calendar_days(start_day, end)
    by_id = {s["id"]: s for s in owner["datasets"]}
    if len(by_id) != len(owner["datasets"]) or len(set(dataset_ids)) != len(dataset_ids):
        raise ValueError("duplicate dataset/source selection")
    specs = [by_id[key] for key in dataset_ids]
    audits, identities = {}, []
    for spec in specs:
        path = spec.get("audit", {}).get("path")
        audits[spec["id"]] = _csv_rows(Path(path)) if path else {}
        if path:
            identities.append({"path": path, "sha256": sha256_file(Path(path))})
    usages = []
    for source in usage_sources or ():
        path = Path(source["path"])
        payload = json.loads(path.read_text())
        # Caller explicitly declares metadata-only day fields; never discover
        # dates by scanning arbitrary reports or selecting economic outcomes.
        for field in source["day_fields"]:
            values = payload
            for part in field.split("."):
                values = values[part]
            if not isinstance(values, list):
                raise ValueError("use-rights date field is not a list")
            for day in values:
                date.fromisoformat(day)
            usages.append({"days": values, "role": source["role"],
                           "identity": source["identity"], "sha256": sha256_file(path),
                           "day_field": field})
    records = []
    for day in days:
        rights = [{k: v for k, v in u.items() if k != "days"} for u in usages if day in u["days"]]
        records.append({
            "calendar_date": day, "calendar_status": "COMPLETE_UTC_DAY",
            "channels": [_channel(s, day, audits[s["id"]].get(day, {})) for s in specs],
            "research_use": {"known_previous_use": rights, "training": UNKNOWN,
                             "development": "PREVIOUSLY_USED" if any(
                                 u["role"] == "Development" for u in rights) else UNKNOWN,
                             "validation": "RESERVED" if any(
                                 u["role"] == "Validation" for u in rights) else UNKNOWN,
                             "holdout": "SEALED" if any(
                                 u["role"] == "sealed_holdout" for u in rights) else UNKNOWN,
                             "locked": True if any(u["role"] in {"Validation", "sealed_holdout"}
                                                        for u in rights) else UNKNOWN,
                             "new_use_authorized": False},
            "account_continuity": "NOT_TESTED_DATA_INVENTORY_ONLY",
        })
    counts = {key: dict(Counter(c["reader"] for r in records for c in r["channels"]
                               if c["source_id"] == key)) for key in dataset_ids}
    result = {
        "schema": "calendar_readability.v1", "visibility": "local_only_do_not_publish",
        "created_at_utc": as_of.astimezone(UTC).isoformat(),
        "calendar_start": start_day, "calendar_end": end, "calendar_day_count": len(days),
        "calendar_window_policy": window_policy,
        "partial_day": {"calendar_date": today.isoformat(), "status": "PARTIAL_NOT_IN_DENOMINATOR"},
        "source_manifest": {"path": str(owner_manifest), "sha256": sha256_file(owner_manifest)},
        "audit_inputs": identities, "reader_counts": counts, "records": records,
        "scope": "STAT_AND_PARQUET_FOOTERS_PLUS_RECORDED_METADATA_NOT_FULL_RAW_REVALIDATION",
        "future_fill": {"current_full_calendar_check": "NOT_RUN", "violations": UNKNOWN},
        "rights_scope": "EXPLICIT_PREVIOUS_USE_ONLY_NOT_EXHAUSTIVE_GLOBAL_CLEARANCE",
    }
    validate_readability_manifest(result)
    return result


def validate_readability_manifest(manifest: dict) -> None:
    expected = _calendar_days(manifest["calendar_start"], manifest["calendar_end"])
    actual = [r["calendar_date"] for r in manifest["records"]]
    if actual != expected or len(actual) != manifest["calendar_day_count"]:
        raise ValueError("calendar missing, duplicated, or unordered")
    if manifest["partial_day"]["calendar_date"] in actual:
        raise ValueError("partial date included in complete denominator")
    for row in manifest["records"]:
        if any(c["economic_admission"] != "NOT_GRANTED" for c in row["channels"]):
            raise ValueError("readability cannot grant economic admission")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-manifest", type=Path, required=True)
    parser.add_argument("--dataset", action="append", required=True)
    parser.add_argument("--start-day", default="2025-08-01")
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    parser.add_argument("--usage-config", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    result = build_readability_manifest(
        args.owner_manifest, start_day=args.start_day,
        as_of=args.as_of or datetime.now(UTC), dataset_ids=args.dataset,
        usage_sources=json.loads(args.usage_config.read_text()) if args.usage_config else None,
    )
    # Immutable run artifact: refusal to overwrite protects prior observations.
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(result, stream, ensure_ascii=False, allow_nan=False, indent=2)
        stream.write("\n")
    print(json.dumps({"calendar_day_count": result["calendar_day_count"],
                      "calendar_end": result["calendar_end"],
                      "reader_counts": result["reader_counts"],
                      "sha256": sha256_file(args.output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
