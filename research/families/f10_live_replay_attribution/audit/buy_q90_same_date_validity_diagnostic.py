#!/usr/bin/env python3
"""Explain same-date q90 replay validity loss without reading economics."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Iterable, Mapping

from research.families.f10_live_replay_attribution.audit import (
    buy_q90_portfolio_path_attribution as historical_q90,
)
from research.families.f10_live_replay_attribution.audit import (
    buy_q90_same_date_mechanics as same_date,
)


SCHEMA_VERSION = "buy_q90_same_date_validity_diagnostic.v1"
IDENTITY = "buy_q90_live_action_rate_transport_parity_v1_validity_diagnostic"
DEFAULT_SPEC_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "buy_q90_live_action_rate_transport_parity_v1_validity_diagnostic_spec_20260731.json"
)
EXTRA_MECHANICS_KEYS = (
    "dynamic_fill_hazard_retain_invalid_count",
    "exchange_book_queue_lookup_count",
    "exchange_book_queue_exact_count",
    "exchange_book_queue_known_zero_count",
    "exchange_book_queue_missing_count",
    "exchange_book_queue_invalidated_order_count",
    "exchange_book_queue_ambiguous_event_count",
    "exchange_book_cancel_trade_ambiguous_order_count",
)


def _load_spec(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected q90 validity diagnostic schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected q90 validity diagnostic identity")
    if payload.get("status") != "frozen_before_validity_diagnostic_output_read":
        raise ValueError("q90 validity diagnostic status drifted")
    frozen = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen) != 64:
        raise ValueError("q90 validity diagnostic spec hash is missing")
    canonical = dict(payload)
    canonical.pop("canonical_spec_sha256", None)
    if same_date.canonical_sha256(canonical) != frozen:
        raise ValueError("q90 validity diagnostic spec hash mismatch")
    permissions = payload.get("permissions") or {}
    if not permissions or any(bool(value) for value in permissions.values()):
        raise ValueError("q90 validity diagnostic cannot grant permissions")
    if not bool(payload.get("economic_outputs_prohibited", False)):
        raise ValueError("q90 validity diagnostic cannot access economics")
    return payload


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    return (
        float(numerator) / float(denominator)
        if float(denominator) > 0.0
        else math.nan
    )


def run_diagnostic(spec_path: Path) -> dict[str, Any]:
    spec = _load_spec(spec_path)
    mechanics_spec_path = Path(spec["same_date_mechanics_spec"]["path"])
    same_date._require_identity(
        mechanics_spec_path,
        str(spec["same_date_mechanics_spec"]["sha256"]),
        "same-date mechanics spec",
    )
    same_date._require_identity(
        Path(spec["same_date_mechanics_implementation"]["path"]),
        str(spec["same_date_mechanics_implementation"]["sha256"]),
        "same-date mechanics implementation",
    )

    original_mechanics_only = same_date._mechanics_only
    original_source_contract = historical_q90._runtime_source_contract

    def expanded_mechanics(result: Mapping[str, Any]) -> dict[str, Any]:
        output = original_mechanics_only(result)
        for key in EXTRA_MECHANICS_KEYS:
            output[key] = int(result.get(key, 0) or 0)
        return output

    def source_without_window_cache(
        source_spec: Mapping[str, Any],
    ) -> dict[str, Any]:
        source = original_source_contract(source_spec)
        source["source_identity"]["window_cache_dir"] = ""
        return source

    same_date._mechanics_only = expanded_mechanics
    historical_q90._runtime_source_contract = source_without_window_cache
    try:
        mechanics_output = same_date.run_same_date(mechanics_spec_path)
    finally:
        same_date._mechanics_only = original_mechanics_only
        historical_q90._runtime_source_contract = original_source_contract

    mechanics = mechanics_output["mechanics"]
    lookups = int(mechanics["exchange_book_queue_lookup_count"])
    exact = int(mechanics["exchange_book_queue_exact_count"])
    known_zero = int(mechanics["exchange_book_queue_known_zero_count"])
    missing = int(mechanics["exchange_book_queue_missing_count"])
    supported = exact + known_zero
    invalidated = int(mechanics["exchange_book_queue_invalidated_order_count"])
    ambiguous = int(mechanics["exchange_book_queue_ambiguous_event_count"])
    valid_evaluations = int(mechanics["dynamic_fill_hazard_valid_eval_count"])
    evaluations = int(mechanics["dynamic_fill_hazard_eval_count"])

    if _safe_ratio(missing, lookups) >= 0.5:
        classification = "activation_exact_level_support_gap_dominant"
    elif _safe_ratio(invalidated, supported) >= 0.5 and ambiguous > 0:
        classification = "exchange_time_path_invalidation_dominant"
    else:
        classification = "mixed_or_unresolved_validity_loss"

    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "day": mechanics_output["day"],
        "quality_grade": mechanics_output["quality_grade"],
        "classification": classification,
        "same_date_rates": mechanics_output["rates"],
        "validity": {
            "evaluation_count": evaluations,
            "valid_evaluation_count": valid_evaluations,
            "valid_evaluation_rate": _safe_ratio(
                valid_evaluations,
                evaluations,
            ),
            "queue_seed_lookup_count": lookups,
            "queue_seed_exact_count": exact,
            "queue_seed_known_zero_count": known_zero,
            "queue_seed_missing_count": missing,
            "queue_seed_support_rate": _safe_ratio(supported, lookups),
            "path_invalidated_order_count": invalidated,
            "path_invalidation_per_supported_seed": _safe_ratio(
                invalidated,
                supported,
            ),
            "ambiguous_event_count": ambiguous,
            "cancel_trade_ambiguous_order_count": int(
                mechanics[
                    "exchange_book_cancel_trade_ambiguous_order_count"
                ]
            ),
            "hold_invalid_evaluation_count": int(
                mechanics["dynamic_fill_hazard_retain_invalid_count"]
            ),
        },
        "economic_outputs_read": False,
        "permissions": dict(spec["permissions"]),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC_PATH)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    output = _json_safe(run_diagnostic(args.spec))
    path = Path(args.output).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            output,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(output["validity"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
