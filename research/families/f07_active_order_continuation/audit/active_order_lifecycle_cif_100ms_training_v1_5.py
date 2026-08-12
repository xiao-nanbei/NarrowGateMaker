#!/usr/bin/env python3
"""Train an outcome-blind, time-varying 100 ms active-order lifecycle CIF.

The admitted journal does not contain a realized fill-quality label, and this
pipeline is forbidden from opening markouts.  It therefore estimates the three
observable competing terminal causes ``full_fill``, ``cancel_ack`` and
``other_terminal``.  For the existing four-channel probability kernel,
``full_fill`` occupies the first channel and the unclassified adverse-fill
channel is fixed to zero.  That mapping is mechanics-only and cannot produce a
q90 adverse score or action permission.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import uuid
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from execution.order_lifecycle import FILL_RISK_PHASES
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_40day_replay_emitter as emitter,
)
from research.families.f07_active_order_continuation.audit import (
    order_lifecycle_v2_downstream_execution_amendment_v1_5 as provenance,
)

IDENTITY = provenance.TRAINING_IDENTITY
SCHEMA_VERSION = provenance.TRAINING_SCHEMA_VERSION
REPORT_SCHEMA_VERSION = provenance.TRAINING_REPORT_SCHEMA_VERSION
GRID_INTERVAL_MS = 100
CAUSES = ("full_fill", "cancel_ack", "other_terminal")
KERNEL_CHANNELS = (
    "favorable_fill",
    "adverse_fill",
    "cancel_ack",
    "other_terminal",
)
AGE_BIN_EDGES_S = (0.0, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, math.inf)
HOUR_BIN_WIDTH = 6
PRIOR_EXPOSURE_S = 30.0
_NS_PER_S = 1_000_000_000


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact(path: Path) -> dict[str, object]:
    resolved = path.expanduser().resolve()
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": _file_sha256(resolved),
    }


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.partial-{os.getpid()}-{uuid.uuid4().hex}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved)
        descriptor = os.open(resolved.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _age_bin(age_s: float) -> int:
    if not math.isfinite(age_s) or age_s < 0.0:
        raise ValueError("risk age must be finite and non-negative")
    for index, upper in enumerate(AGE_BIN_EDGES_S[1:]):
        if age_s < upper:
            return index
    return len(AGE_BIN_EDGES_S) - 2


def _remaining_class(initial: float, remaining: float) -> str:
    if not (math.isfinite(initial) and math.isfinite(remaining)) or initial <= 0.0:
        raise ValueError("lifecycle quantities must be finite and positive")
    if remaining <= 0.0 or remaining > initial + 1e-12:
        raise ValueError("remaining quantity leaves lifecycle support")
    return "full" if remaining >= initial - 1e-12 else "partial"


def _hour_bin(ts_ns: int) -> int:
    hour = datetime.fromtimestamp(int(ts_ns) / _NS_PER_S, tz=timezone.utc).hour
    return hour // HOUR_BIN_WIDTH


def _cell_key(
    *,
    side: str,
    phase: str,
    age_s: float,
    initial_quantity: float,
    remaining_quantity: float,
    calendar_ts_ns: int,
) -> tuple[str, str, int, str, int]:
    normalized_side = str(side).upper()
    normalized_phase = str(phase).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ValueError("lifecycle side must be BUY or SELL")
    if normalized_phase not in FILL_RISK_PHASES:
        raise ValueError("cell phase is outside fill risk")
    return (
        normalized_side,
        normalized_phase,
        _age_bin(age_s),
        _remaining_class(initial_quantity, remaining_quantity),
        _hour_bin(calendar_ts_ns),
    )


def lifecycle_is_exact_native(rows: Sequence[Mapping[str, object]]) -> bool:
    risk_rows = [
        row
        for row in rows
        if str(row["phase_before"]) in FILL_RISK_PHASES
        or str(row["phase_after"]) in FILL_RISK_PHASES
    ]
    return bool(risk_rows) and all(
        str(row["simulator_queue_source"]) == "native_exchange_book"
        and bool(row["exact_queue_path_valid"])
        for row in risk_rows
    )


def _terminal_cause(row: Mapping[str, object]) -> str | None:
    event = str(row["lifecycle_event"])
    if event == "full_fill":
        return "full_fill"
    if str(row["terminal_observation"]) != "EXCHANGE_TERMINAL":
        return None
    reason = str(row["exchange_terminal_reason"] or row["event_reason"])
    if reason in {"cancel_ack", "cancel_ack_reconciled"}:
        return "cancel_ack"
    if reason == "filled_before_cancel_ack":
        return "full_fill"
    return "other_terminal"


def _next_age_boundary_ns(activation_ts_ns: int, age_s: float) -> int | None:
    index = _age_bin(age_s)
    upper = AGE_BIN_EDGES_S[index + 1]
    if not math.isfinite(upper):
        return None
    return activation_ts_ns + int(round(upper * _NS_PER_S))


def _next_hour_boundary_ns(ts_ns: int) -> int:
    seconds = int(ts_ns) // _NS_PER_S
    current_hour = seconds // 3600
    next_group_hour = ((current_hour // HOUR_BIN_WIDTH) + 1) * HOUR_BIN_WIDTH
    return next_group_hour * 3600 * _NS_PER_S


def accumulate_exact_native_lifecycle(
    rows: Sequence[Mapping[str, object]],
    *,
    exposures: defaultdict[tuple[str, str, int, str, int], float],
    events: defaultdict[tuple[str, str, int, str, int], Counter[str]],
) -> dict[str, int]:
    ordered = sorted(rows, key=lambda item: int(item["lifecycle_sequence"]))
    if not lifecycle_is_exact_native(ordered):
        return {"eligible": 0, "censored": 1, "partial_spell_boundaries": 0}
    activation_indices = [
        index for index, row in enumerate(ordered) if str(row["lifecycle_event"]) == "activate"
    ]
    if len(activation_indices) != 1:
        return {"eligible": 0, "censored": 1, "partial_spell_boundaries": 0}
    activation_index = activation_indices[0]
    activation = ordered[activation_index]
    activation_ts = int(activation["event_visibility_ts_ns"])
    current_ts = activation_ts
    current_phase = str(activation["phase_after"])
    initial_quantity = float(activation["initial_quantity"])
    remaining = float(activation["remaining_quantity_after"])
    partial_boundaries = 0
    terminal_seen = False

    for row in ordered[activation_index + 1 :]:
        event_ts = int(row["event_visibility_ts_ns"])
        if event_ts < current_ts:
            raise ValueError("visibility clock regressed within lifecycle")
        last_cell: tuple[str, str, int, str, int] | None = None
        cursor = current_ts
        while (
            cursor < event_ts
            and current_phase in FILL_RISK_PHASES
            and remaining > 0.0
        ):
            age_s = max(0.0, (cursor - activation_ts) / _NS_PER_S)
            age_boundary = _next_age_boundary_ns(activation_ts, age_s)
            segment_end = min(event_ts, _next_hour_boundary_ns(cursor))
            if age_boundary is not None and age_boundary > cursor:
                segment_end = min(segment_end, age_boundary)
            if segment_end <= cursor:
                raise ValueError("risk exposure segmentation did not advance")
            midpoint = cursor + (segment_end - cursor) // 2
            cell = _cell_key(
                side=str(row["side"]),
                phase=current_phase,
                age_s=max(0.0, (midpoint - activation_ts) / _NS_PER_S),
                initial_quantity=initial_quantity,
                remaining_quantity=remaining,
                calendar_ts_ns=midpoint,
            )
            exposures[cell] += (segment_end - cursor) / _NS_PER_S
            last_cell = cell
            cursor = segment_end

        cause = _terminal_cause(row)
        if cause is not None:
            if last_cell is None:
                probe_ts = max(activation_ts, event_ts - 1)
                last_cell = _cell_key(
                    side=str(row["side"]),
                    phase=str(row["phase_before"]),
                    age_s=max(0.0, (probe_ts - activation_ts) / _NS_PER_S),
                    initial_quantity=initial_quantity,
                    remaining_quantity=float(row["remaining_quantity_before"]),
                    calendar_ts_ns=probe_ts,
                )
            events[last_cell][cause] += 1
            terminal_seen = True
            break

        event = str(row["lifecycle_event"])
        if event == "partial_fill":
            partial_boundaries += 1
        if str(row["terminal_observation"]) == "LOCAL_SHUTDOWN_CENSOR":
            terminal_seen = True
            break
        current_ts = event_ts
        current_phase = str(row["phase_after"])
        remaining = float(row["remaining_quantity_after"])

    if not terminal_seen:
        raise ValueError("eligible lifecycle lacks a terminal observation or explicit censor")
    return {"eligible": 1, "censored": 0, "partial_spell_boundaries": partial_boundaries}


def _parent_key(cell: tuple[str, str, int, str, int]) -> tuple[str, str, int]:
    return cell[0], cell[1], cell[2]


def _serialize_cell_key(cell: tuple[str, str, int, str, int]) -> dict[str, object]:
    return {
        "side": cell[0],
        "phase": cell[1],
        "risk_age_bin": cell[2],
        "remaining_class": cell[3],
        "utc_hour_bin": cell[4],
    }


def fit_rate_table(
    exposures: Mapping[tuple[str, str, int, str, int], float],
    events: Mapping[tuple[str, str, int, str, int], Mapping[str, int]],
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    parent_exposure: defaultdict[tuple[str, str, int], float] = defaultdict(float)
    parent_events: defaultdict[tuple[str, str, int], Counter[str]] = defaultdict(Counter)
    for cell, exposure in exposures.items():
        parent = _parent_key(cell)
        parent_exposure[parent] += float(exposure)
        parent_events[parent].update(events.get(cell, {}))
    parent_rows: list[dict[str, object]] = []
    parent_rates: dict[tuple[str, str, int], dict[str, float]] = {}
    for parent in sorted(parent_exposure):
        exposure = parent_exposure[parent]
        if not math.isfinite(exposure) or exposure <= 0.0:
            raise ValueError("parent exposure must be finite and positive")
        rates = {cause: float(parent_events[parent][cause]) / exposure for cause in CAUSES}
        parent_rates[parent] = rates
        parent_rows.append(
            {
                "side": parent[0],
                "phase": parent[1],
                "risk_age_bin": parent[2],
                "exposure_s": exposure,
                "event_counts": {cause: int(parent_events[parent][cause]) for cause in CAUSES},
                "rates_per_s": rates,
            }
        )
    cells: list[dict[str, object]] = []
    for cell in sorted(exposures):
        exposure = float(exposures[cell])
        parent = _parent_key(cell)
        prior_rates = parent_rates[parent]
        counts = {cause: int(events.get(cell, {}).get(cause, 0)) for cause in CAUSES}
        rates = {
            cause: (counts[cause] + PRIOR_EXPOSURE_S * prior_rates[cause])
            / (exposure + PRIOR_EXPOSURE_S)
            for cause in CAUSES
        }
        cells.append(
            {
                **_serialize_cell_key(cell),
                "exposure_s": exposure,
                "event_counts": counts,
                "rates_per_s": rates,
                "fallback_parent": {
                    "side": parent[0],
                    "phase": parent[1],
                    "risk_age_bin": parent[2],
                },
            }
        )
    return cells, parent_rows


def kernel_rates_from_lifecycle_rates(rates: Mapping[str, object]) -> list[float]:
    if set(rates) != set(CAUSES):
        raise ValueError("lifecycle rate causes differ")
    normalized = [float(rates[cause]) for cause in CAUSES]
    if any(not math.isfinite(value) or value < 0.0 for value in normalized):
        raise ValueError("lifecycle rates must be finite and non-negative")
    return [normalized[0], 0.0, normalized[1], normalized[2]]


def train_panel(
    *,
    plan_path: Path,
    amendment_path: Path,
    lockstep_report_path: Path,
    artifact_path: Path,
    report_path: Path,
) -> tuple[dict[str, object], dict[str, object]]:
    plan_file = plan_path.expanduser().resolve()
    amendment_file = amendment_path.expanduser().resolve()
    lockstep_file = lockstep_report_path.expanduser().resolve()
    amendment, plan = provenance.validate_downstream_execution_amendment(
        amendment_file,
        plan_path=plan_file,
    )
    by_day = emitter.validate_execution_plan(plan)
    cache_root = Path(str(plan["cache_root"])).expanduser().resolve()
    panel_path = cache_root / "panel_manifest.json"
    panel = provenance.validate_panel_manifest_strict(panel_path, plan=plan)
    provenance.validate_lockstep_report_for_training(
        lockstep_file,
        plan_path=plan_file,
        panel_path=panel_path,
        amendment_path=amendment_file,
        amendment=amendment,
        plan=plan,
    )
    days = list(map(str, plan["ordered_utc_days"]))
    if len(days) != 40 or list(map(str, panel.get("ordered_utc_days", []))) != days:
        raise RuntimeError("CIF training requires the frozen 40-day panel")

    exposures: defaultdict[tuple[str, str, int, str, int], float] = defaultdict(float)
    events: defaultdict[tuple[str, str, int, str, int], Counter[str]] = defaultdict(Counter)
    counters: Counter[str] = Counter()
    day_support: list[dict[str, object]] = []
    for day in days:
        manifest = emitter._validate_day_manifest(
            cache_root / "days" / day / "day_manifest.json",
            plan=plan,
            day_row=by_day[day],
        )
        session_root = cache_root / "days" / day / str(manifest["journal_v2"]["session_root"])
        rows, _, _ = emitter._read_journal_parts(session_root)
        by_lifecycle: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_lifecycle[str(row["lifecycle_id"])].append(row)
        before_exposure = math.fsum(exposures.values())
        before_events = sum(sum(counter.values()) for counter in events.values())
        day_counts: Counter[str] = Counter()
        for lifecycle_rows in by_lifecycle.values():
            result = accumulate_exact_native_lifecycle(
                lifecycle_rows,
                exposures=exposures,
                events=events,
            )
            day_counts.update(result)
            counters.update(result)
        manifest_counters = manifest["journal_v2"]["counters"]
        expected_eligible = int(manifest_counters["exact_native_lifecycle_count"])
        expected_censored = int(
            manifest_counters["native_queue_censored_lifecycle_count"]
        )
        if int(day_counts["eligible"]) != expected_eligible:
            raise RuntimeError(
                f"{day}: CIF exact-native denominator differs from admitted journal"
            )
        if int(day_counts["censored"]) != expected_censored:
            raise RuntimeError(
                f"{day}: CIF censor denominator differs from admitted journal"
            )
        day_support.append(
            {
                "day": day,
                "eligible_lifecycle_count": int(day_counts["eligible"]),
                "censored_lifecycle_count": int(day_counts["censored"]),
                "admitted_eligible_lifecycle_count": expected_eligible,
                "admitted_censored_lifecycle_count": expected_censored,
                "denominator_parity": True,
                "risk_exposure_s": math.fsum(exposures.values()) - before_exposure,
                "terminal_cause_count": (
                    sum(sum(counter.values()) for counter in events.values()) - before_events
                ),
            }
        )
    panel_totals = panel.get("mechanics_totals", {})
    expected_panel_eligible = int(panel_totals.get("exact_native_lifecycle_count", -1))
    expected_panel_censored = int(
        panel_totals.get("native_queue_censored_lifecycle_count", -1)
    )
    if int(counters["eligible"]) != expected_panel_eligible:
        raise RuntimeError("CIF exact-native denominator differs from panel manifest")
    if int(counters["censored"]) != expected_panel_censored:
        raise RuntimeError("CIF censor denominator differs from panel manifest")
    if counters["eligible"] <= 0 or not exposures:
        raise RuntimeError("CIF training found no admitted exact-native risk spells")
    cells, parents = fit_rate_table(exposures, events)
    cause_counts = Counter()
    for counter in events.values():
        cause_counts.update(counter)
    model: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "trained_mechanics_only",
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "downstream_execution_amendment": provenance.amendment_reference(
            amendment_file,
            amendment,
        ),
        "plan_sha256": plan["canonical_plan_sha256"],
        "input_artifacts": {
            "execution_plan": _artifact(plan_file),
            "panel_manifest": _artifact(panel_path),
            "python_cpp_lockstep": _artifact(lockstep_file),
        },
        "grid": {
            "interval_ms": GRID_INTERVAL_MS,
            "risk_clock": "event_visibility_ts_ns",
            "calendar_clock": "utc_from_event_visibility_ts_ns",
            "risk_phases": sorted(FILL_RISK_PHASES),
        },
        "cause_contract": {
            "trained_causes": list(CAUSES),
            "realized_fill_quality_read": False,
            "kernel_channels": list(KERNEL_CHANNELS),
            "kernel_rate_mapping": {
                "favorable_fill": "full_fill_aggregate",
                "adverse_fill": "fixed_zero_unclassified_channel",
                "cancel_ack": "cancel_ack",
                "other_terminal": "other_terminal",
            },
            "q90_adverse_score_available": False,
        },
        "conditioning": {
            "side": ["BUY", "SELL"],
            "phase": sorted(FILL_RISK_PHASES),
            "risk_age_bin_edges_s": [
                value if math.isfinite(value) else "inf" for value in AGE_BIN_EDGES_S
            ],
            "remaining_classes": ["full", "partial"],
            "utc_hour_bin_width": HOUR_BIN_WIDTH,
            "cell_prior_exposure_s": PRIOR_EXPOSURE_S,
            "unseen_cell_fallback": "side_phase_risk_age_parent",
        },
        "training_counts": {
            "day_count": len(days),
            "eligible_lifecycle_count": int(counters["eligible"]),
            "censored_lifecycle_count": int(counters["censored"]),
            "partial_spell_boundary_count": int(counters["partial_spell_boundaries"]),
            "risk_exposure_s": math.fsum(exposures.values()),
            "cause_counts": {cause: int(cause_counts[cause]) for cause in CAUSES},
            "admitted_exact_native_lifecycle_count": expected_panel_eligible,
            "admitted_native_queue_censored_lifecycle_count": expected_panel_censored,
            "cell_count": len(cells),
            "parent_cell_count": len(parents),
        },
        "parent_rates": parents,
        "cells": cells,
        "scope": dict(provenance.TRAINING_SCOPE),
        "permissions": dict(provenance.TRAINING_PERMISSIONS),
    }
    model["canonical_artifact_sha256"] = _canonical_sha256(model)
    _atomic_write_json(artifact_path, model)
    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "identity": f"{IDENTITY}_training",
        "status": "passed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "downstream_execution_amendment": provenance.amendment_reference(
            amendment_file,
            amendment,
        ),
        "model_artifact": _artifact(artifact_path),
        "input_artifacts": dict(model["input_artifacts"]),
        "plan_sha256": plan["canonical_plan_sha256"],
        "training_counts": dict(model["training_counts"]),
        "day_support": day_support,
        "gates": {
            "forty_admitted_days": len(days) == 40,
            "python_cpp_event_lockstep_passed": True,
            "exact_native_risk_spells_present": counters["eligible"] > 0,
            "exact_native_denominator_parity": (
                int(counters["eligible"]) == expected_panel_eligible
            ),
            "native_queue_censor_denominator_parity": (
                int(counters["censored"]) == expected_panel_censored
            ),
            "terminal_causes_do_not_exceed_eligible_spells": (
                sum(cause_counts.values()) <= int(counters["eligible"])
            ),
            "positive_risk_exposure": math.fsum(exposures.values()) > 0.0,
            "finite_nonnegative_rates": all(
                all(math.isfinite(float(value)) and float(value) >= 0.0 for value in row["rates_per_s"].values())
                for row in cells
            ),
            "economic_outcomes_not_read": True,
            "downstream_implementation_hashes_bound": True,
            "lockstep_report_canonical_hash_bound": True,
        },
        "scope": dict(model["scope"]),
        "permissions": dict(model["permissions"]),
    }
    report["canonical_report_sha256"] = _canonical_sha256(report)
    if not all(report["gates"].values()):
        raise RuntimeError("CIF training failed closed")
    _atomic_write_json(report_path, report)
    return model, report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--execution-amendment", type=Path, required=True)
    parser.add_argument("--lockstep-report", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    _, report = train_panel(
        plan_path=args.plan,
        amendment_path=args.execution_amendment,
        lockstep_report_path=args.lockstep_report,
        artifact_path=args.artifact,
        report_path=args.report,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
