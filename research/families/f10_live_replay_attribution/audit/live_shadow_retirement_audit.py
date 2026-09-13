#!/usr/bin/env python3
"""Audit retired live shadow streams without reading strategy outcomes."""

from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from research.families.f09_campaign_action_uplift.audit.cross_venue_fair_center_shift import (
    CANDIDATE_ACTION,
    project_action_pair,
)
from strategy.cross_venue_fair_price import CrossVenueFairPriceState


DEPTH_RE = re.compile(
    r"DEPTH_SHADOW .*?mp_en=(?P<mp>\w+) .*?imb_en=(?P<imb_en>\w+) "
    r".*?dtox_en=(?P<dtox_en>\w+) .*?micro_shift_bps=(?P<micro>[-+0-9.]+) "
    r".*?kappa_ratio=(?P<kappa>[-+0-9.]+) .*?imb=(?P<imb>[-+0-9.]+) "
    r".*?dtox_bid=(?P<dtox_bid>\w+) dtox_ask=(?P<dtox_ask>\w+)"
)
CAMPAIGN_END_RE = re.compile(
    r"CAMPAIGN_END id=(?P<id>\d+) side=(?P<side>\w+) duration=(?P<duration>[0-9.]+)s "
    r"max_abs_qty=(?P<max_qty>[0-9.]+) pnl=(?P<pnl>[-+0-9.]+) "
    r"mae=(?P<mae>[-+0-9.]+) fills=(?P<fills>\d+) inc=(?P<inc>\d+) red=(?P<red>\d+)"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _input_identity(path: Path) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "bytes": stat.st_size,
        "sha256": _sha256(path),
    }


def _float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    return result if math.isfinite(result) else math.nan


def _int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _quantiles(values: Iterable[float]) -> dict[str, float | None]:
    ordered = sorted(value for value in values if math.isfinite(value))
    if not ordered:
        return {key: None for key in ("p10", "p25", "p50", "p75", "p90", "p95", "p99")}

    def at(probability: float) -> float:
        position = probability * (len(ordered) - 1)
        lower = int(math.floor(position))
        upper = int(math.ceil(position))
        if lower == upper:
            return ordered[lower]
        weight = position - lower
        return ordered[lower] * (1.0 - weight) + ordered[upper] * weight

    return {
        "p10": at(0.10),
        "p25": at(0.25),
        "p50": at(0.50),
        "p75": at(0.75),
        "p90": at(0.90),
        "p95": at(0.95),
        "p99": at(0.99),
    }


def audit_fair_price(path: Path) -> dict[str, Any]:
    counters: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    venue_sets: Counter[str] = Counter()
    source_kinds: Counter[str] = Counter()
    raw_lead_abs: list[float] = []
    effective_ticks_abs: list[float] = []
    requested_ticks_abs: list[float] = []
    start_ts = math.inf
    end_ts = -math.inf

    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            counters["rows"] += 1
            timestamp = _float(row.get("timestamp"))
            if math.isfinite(timestamp):
                start_ts = min(start_ts, timestamp)
                end_ts = max(end_ts, timestamp)
            valid = _int(row.get("valid")) == 1
            transport = _int(row.get("transport_supported")) == 1
            authorized = _int(row.get("action_authorized")) == 1
            counters["valid_rows"] += valid
            counters["transport_supported_rows"] += transport
            counters["action_authorized_rows"] += authorized
            counters["gtx_clamped_rows"] += _int(row.get("gtx_clamped")) == 1
            counters["pair_spread_preserved_rows"] += _int(row.get("pair_spread_preserved")) == 1
            reasons[str(row.get("reason") or "<missing>")] += 1
            venue_sets[str(row.get("venue_ids") or "<missing>")] += 1
            source_kinds[str(row.get("source_kinds") or "<missing>")] += 1
            raw_lead_abs.append(abs(_float(row.get("raw_lead_bps"))))
            effective = _float(row.get("effective_shift_ticks"))
            requested = _float(row.get("requested_shift_ticks"))
            effective_ticks_abs.append(abs(effective))
            requested_ticks_abs.append(abs(requested))
            counters["effective_price_change_rows"] += math.isfinite(effective) and abs(effective) > 0.0
            counters["requested_price_change_rows"] += math.isfinite(requested) and abs(requested) > 0.0
            counters["positive_shift_rows"] += math.isfinite(effective) and effective > 0.0
            counters["negative_shift_rows"] += math.isfinite(effective) and effective < 0.0

    rows = counters["rows"]
    return {
        **dict(counters),
        "valid_fraction": counters["valid_rows"] / rows if rows else None,
        "transport_supported_fraction": counters["transport_supported_rows"] / rows if rows else None,
        "action_authorized_fraction": counters["action_authorized_rows"] / rows if rows else None,
        "start_timestamp": None if start_ts == math.inf else start_ts,
        "end_timestamp": None if end_ts == -math.inf else end_ts,
        "reason_counts": dict(reasons.most_common()),
        "venue_set_counts": dict(venue_sets.most_common()),
        "source_kind_counts": dict(source_kinds.most_common()),
        "absolute_raw_lead_bps_quantiles": _quantiles(raw_lead_abs),
        "absolute_requested_shift_ticks_quantiles": _quantiles(requested_ticks_abs),
        "absolute_effective_shift_ticks_quantiles": _quantiles(effective_ticks_abs),
        "economic_outcomes_present": False,
    }


def audit_fair_price_replay_parity(
    fair_path: Path,
    snapshot_path: Path,
    *,
    tick_size: float = 0.1,
    match_tolerance_s: float = 0.5,
) -> dict[str, Any]:
    snapshots: list[dict[str, Any]] = []
    with snapshot_path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            timestamp = _float(row.get("timestamp"))
            if not math.isfinite(timestamp):
                continue
            snapshots.append(
                {
                    "timestamp": timestamp,
                    "guard_bid": _float(row.get("guard_bid")),
                    "guard_ask": _float(row.get("guard_ask")),
                    "final_bid": _float(row.get("final_bid")),
                    "final_ask": _float(row.get("final_ask")),
                    "snapshot_valid": _int(row.get("snapshot_valid")) == 1,
                }
            )
    snapshots.sort(key=lambda row: row["timestamp"])
    snapshot_times = [row["timestamp"] for row in snapshots]
    counters: Counter[str] = Counter()
    match_lag_ms: list[float] = []

    def nearest(timestamp: float) -> dict[str, Any] | None:
        index = bisect.bisect_left(snapshot_times, timestamp)
        candidates = []
        if index < len(snapshots):
            candidates.append(snapshots[index])
        if index > 0:
            candidates.append(snapshots[index - 1])
        if not candidates:
            return None
        result = min(candidates, key=lambda row: abs(row["timestamp"] - timestamp))
        if abs(result["timestamp"] - timestamp) > match_tolerance_s:
            return None
        return result

    tick_abs_tol = tick_size * 1e-6
    with fair_path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            counters["fair_rows"] += 1
            timestamp = _float(row.get("timestamp"))
            snapshot = nearest(timestamp) if math.isfinite(timestamp) else None
            if snapshot is None:
                counters["unmatched_rows"] += 1
                continue
            counters["matched_rows"] += 1
            match_lag_ms.append(abs(snapshot["timestamp"] - timestamp) * 1_000.0)
            if not snapshot["snapshot_valid"]:
                counters["matched_invalid_snapshot_rows"] += 1
                continue
            baseline_bid = _float(row.get("baseline_bid"))
            baseline_ask = _float(row.get("baseline_ask"))
            if not (
                math.isclose(baseline_bid, snapshot["final_bid"], rel_tol=0.0, abs_tol=tick_abs_tol)
                and math.isclose(baseline_ask, snapshot["final_ask"], rel_tol=0.0, abs_tol=tick_abs_tol)
            ):
                counters["baseline_snapshot_identity_mismatches"] += 1
                continue
            counters["identity_matched_rows"] += 1
            source_kinds = tuple(
                part for part in str(row.get("source_kinds") or "").split("|") if part
            )
            venue_ids = tuple(
                part for part in str(row.get("venue_ids") or "").split("|") if part
            )
            state = CrossVenueFairPriceState(
                schema_version=str(row.get("schema_version") or ""),
                decision_ts_ns=_int(row.get("decision_ts_ns")),
                valid=_int(row.get("valid")) == 1,
                reason=str(row.get("reason") or ""),
                local_mid=_float(row.get("local_mid")),
                fair_price=_float(row.get("external_fair")),
                raw_lead_bps=_float(row.get("raw_lead_bps")),
                gain=_float(row.get("gain")),
                center_shift_price=_float(row.get("center_shift_price")),
                center_shift_bps=_float(row.get("center_shift_bps")),
                confidence=_float(row.get("confidence")),
                dispersion_bps=_float(row.get("dispersion_bps")),
                valid_venues=_int(row.get("valid_venues")),
                venue_ids=venue_ids,
                minimum_basis_samples=_int(row.get("minimum_basis_samples")),
                lead_variance_bps2=_float(row.get("lead_variance_bps2")),
                noise_variance_bps2=_float(row.get("noise_variance_bps2")),
                max_source_age_ms=_float(row.get("max_source_age_ms")),
                max_feed_latency_ms=_float(row.get("max_feed_latency_ms")),
                max_feature_latency_ms=_float(row.get("max_feature_latency_ms")),
                source_kinds=source_kinds,
                transport_supported=_int(row.get("transport_supported")) == 1,
                venues={},
            )
            replay = project_action_pair(
                CANDIDATE_ACTION,
                state,
                baseline_bid=baseline_bid,
                baseline_ask=baseline_ask,
                best_bid=float(snapshot["guard_bid"]),
                best_ask=float(snapshot["guard_ask"]),
                tick_size=tick_size,
            )
            comparisons = {
                "valid": replay.valid == (_int(row.get("valid")) == 1),
                "reason": replay.reason == str(row.get("reason") or ""),
                "requested_shift_ticks": replay.requested_shift_ticks
                == _int(row.get("requested_shift_ticks")),
                "effective_shift_ticks": replay.effective_shift_ticks
                == _int(row.get("effective_shift_ticks")),
                "candidate_bid": math.isclose(
                    replay.candidate_bid,
                    _float(row.get("candidate_bid")),
                    rel_tol=0.0,
                    abs_tol=tick_abs_tol,
                ),
                "candidate_ask": math.isclose(
                    replay.candidate_ask,
                    _float(row.get("candidate_ask")),
                    rel_tol=0.0,
                    abs_tol=tick_abs_tol,
                ),
                "gtx_clamped": replay.gtx_clamped
                == (_int(row.get("gtx_clamped")) == 1),
                "pair_spread_preserved": replay.pair_spread_preserved
                == (_int(row.get("pair_spread_preserved")) == 1),
            }
            failed = [name for name, passed in comparisons.items() if not passed]
            if failed:
                counters["replay_mismatch_rows"] += 1
                for name in failed:
                    counters[f"replay_mismatch:{name}"] += 1
            else:
                counters["replay_exact_match_rows"] += 1
            if _int(row.get("gtx_clamped")) == 1:
                counters["live_gtx_clamped_rows"] += 1
                counters["gtx_clamped_exact_match_rows"] += not failed

    identity_rows = counters["identity_matched_rows"]
    return {
        **dict(counters),
        "match_tolerance_s": match_tolerance_s,
        "match_lag_ms_quantiles": _quantiles(match_lag_ms),
        "replay_exact_match_fraction": (
            counters["replay_exact_match_rows"] / identity_rows if identity_rows else None
        ),
        "cap_parity_scope": (
            "pair shift preserves the already capped baseline spread; the fair-center "
            "projection has no independent spread-cap action"
        ),
    }


def audit_inventory(path: Path) -> dict[str, Any]:
    counters: Counter[str] = Counter()
    campaign_rows: Counter[int] = Counter()
    previous_event_key: tuple[Any, ...] | None = None
    threshold_columns: list[str] = []
    start_ts = math.inf
    end_ts = -math.inf

    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        threshold_columns = [
            name for name in (reader.fieldnames or []) if "_block_if_" in name
        ]
        for row in reader:
            counters["rows"] += 1
            timestamp = _float(row.get("timestamp"))
            if math.isfinite(timestamp):
                start_ts = min(start_ts, timestamp)
                end_ts = max(end_ts, timestamp)
            active = _int(row.get("active")) == 1
            campaign_id = _int(row.get("campaign_id"))
            counters["active_rows"] += active
            counters["flat_rows"] += not active
            if active:
                campaign_rows[campaign_id] += 1
            for column in threshold_columns:
                counters[f"trigger:{column}"] += _int(row.get(column)) == 1

            event_key = (
                campaign_id,
                int(active),
                str(row.get("side") or ""),
                round(_float(row.get("q")), 12),
                _int(row.get("fills")),
                _int(row.get("buy_fills")),
                _int(row.get("sell_fills")),
                _int(row.get("exposure_increasing_fills")),
                _int(row.get("reducing_fills")),
            )
            if event_key != previous_event_key:
                counters["event_state_change_rows"] += 1
                previous_event_key = event_key
            else:
                counters["quote_time_repeat_rows"] += 1

    rows = counters["rows"]
    per_campaign = list(campaign_rows.values())
    return {
        "rows": rows,
        "active_rows": counters["active_rows"],
        "flat_rows": counters["flat_rows"],
        "active_fraction": counters["active_rows"] / rows if rows else None,
        "unique_active_campaigns": len(campaign_rows),
        "event_state_change_rows": counters["event_state_change_rows"],
        "quote_time_repeat_rows": counters["quote_time_repeat_rows"],
        "event_only_retention_fraction": (
            counters["event_state_change_rows"] / rows if rows else None
        ),
        "rows_per_active_campaign_quantiles": _quantiles([float(value) for value in per_campaign]),
        "threshold_trigger_counts": {
            column: counters[f"trigger:{column}"] for column in threshold_columns
        },
        "start_timestamp": None if start_ts == math.inf else start_ts,
        "end_timestamp": None if end_ts == -math.inf else end_ts,
    }


def audit_maker_logs(paths: list[Path]) -> dict[str, Any]:
    counters: Counter[str] = Counter()
    micro_abs: list[float] = []
    kappa_ratios: list[float] = []
    imbalances_abs: list[float] = []
    campaign_pnl: list[float] = []
    campaign_mae: list[float] = []
    campaign_duration: list[float] = []
    campaign_sides: Counter[str] = Counter()

    for path in paths:
        with path.open(errors="replace") as handle:
            for line in handle:
                if "DEPTH_SHADOW" in line:
                    counters["depth_shadow_lines"] += 1
                    match = DEPTH_RE.search(line)
                    if not match:
                        counters["depth_shadow_parse_failures"] += 1
                        continue
                    groups = match.groupdict()
                    counters["microprice_kappa_enabled_lines"] += groups["mp"] == "True"
                    counters["imbalance_asym_enabled_lines"] += groups["imb_en"] == "True"
                    counters["depth_tox_enabled_lines"] += groups["dtox_en"] == "True"
                    counters["depth_tox_bid_trigger_lines"] += groups["dtox_bid"] == "True"
                    counters["depth_tox_ask_trigger_lines"] += groups["dtox_ask"] == "True"
                    micro_abs.append(abs(float(groups["micro"])))
                    kappa_ratios.append(float(groups["kappa"]))
                    imbalances_abs.append(abs(float(groups["imb"])))
                if "CAMPAIGN_END" in line:
                    match = CAMPAIGN_END_RE.search(line)
                    if not match:
                        counters["campaign_end_parse_failures"] += 1
                        continue
                    counters["campaign_end_lines"] += 1
                    groups = match.groupdict()
                    campaign_sides[groups["side"]] += 1
                    campaign_pnl.append(float(groups["pnl"]))
                    campaign_mae.append(float(groups["mae"]))
                    campaign_duration.append(float(groups["duration"]))

    return {
        **dict(counters),
        "absolute_microprice_shift_bps_quantiles": _quantiles(micro_abs),
        "kappa_ratio_quantiles": _quantiles(kappa_ratios),
        "absolute_imbalance_quantiles": _quantiles(imbalances_abs),
        "campaign_end_side_counts": dict(campaign_sides),
        "campaign_terminal_pnl_quantiles": _quantiles(campaign_pnl),
        "campaign_mae_quantiles": _quantiles(campaign_mae),
        "campaign_duration_s_quantiles": _quantiles(campaign_duration),
        "campaign_terminal_pnl_sum": sum(campaign_pnl),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    fair_path = args.input_dir / "cross_venue_fair_price_shadow.csv"
    inventory_path = args.input_dir / "inventory_campaign_shadow.csv"
    snapshot_path = args.input_dir / "quote_snapshot_integrity.csv"
    maker_paths = sorted(args.input_dir.glob("maker.log*"))
    required = [fair_path, inventory_path, snapshot_path, *maker_paths]
    missing = [str(path) for path in required if not path.is_file()]
    if missing or not maker_paths:
        raise SystemExit(f"missing required inputs: {missing or ['maker.log*']}")

    report = {
        "schema_version": "narrowgate.live_shadow_retirement_audit.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "economic_outcomes_read": False,
        "orders_mutated": False,
        "inputs": [_input_identity(path) for path in required],
        "cross_venue_fair_price_shadow": audit_fair_price(fair_path),
        "cross_venue_fair_price_live_replay_parity": audit_fair_price_replay_parity(
            fair_path,
            snapshot_path,
        ),
        "inventory_campaign_quote_time_shadow": audit_inventory(inventory_path),
        "maker_log_depth_and_campaign_events": audit_maker_logs(maker_paths),
        "decision": {
            "cross_venue_fair_price_shadow": "retire_after_historical_admission",
            "inventory_threshold_what_if": "replace_with_event_only_campaign_accounting",
            "closed_depth_candidates_shadow": "retire_keep_active_imbalance_asym",
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
