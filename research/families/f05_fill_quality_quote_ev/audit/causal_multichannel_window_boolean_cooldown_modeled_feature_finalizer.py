#!/usr/bin/env python3
"""Finalize the 40-day owner-path feature panel for nested OOF.

The day builder publishes four diagnostic projections.  The OOF consumer uses
exactly one cumulative M2 row per opportunity, including explicitly
UNOBSERVED M2 columns on the seven frozen reduced-support days.  This finalizer
verifies every day receipt, derives an outcome-blind feature schema, and binds
the selected Parquet bytes in one atomic panel manifest.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as feature_engine,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_panel as panel,
)

IDENTITY = panel.IDENTITY
SCHEMA_VERSION = f"{IDENTITY}.panel_manifest.v1"
DEFAULT_ROOT = panel.DEFAULT_OUTPUT_ROOT
MANIFEST_NAME = "panel_manifest.json"
SUCCESS_NAME = "_PANEL_SUCCESS"
SELECTED_TABLE_GLOB = "*/M2.parquet"
EXPECTED_OPPORTUNITIES = 8_600
EXPECTED_HISTORICAL_R0_PREDICATE_COUNT = 360
HISTORICAL_R0_PREDICATE_PREFIX = "predicate::ema_pair_"
SOURCE_SPLIT_SCHEMA_VERSION = panel.SOURCE_SPLIT_SCHEMA_VERSION
EXPECTED_R0_SOURCE_IDENTITY = panel.R0_SOURCE_IDENTITY
EXPECTED_M1_SOURCE_IDENTITY = panel.M1_SOURCE_IDENTITY
EXPECTED_M2_SOURCE_IDENTITY = panel.M2_SOURCE_IDENTITY
EXPECTED_SOURCE_CENSUS_IDENTITY = (
    "multiscale_ema_boolean_cooldown_duration_policy_v1"
)

M0_CONTINUOUS_CANDIDATES = (
    "inventory_before_fill_btc",
    "inventory_after_fill_btc",
    "fill_qty_btc",
    "order_qty_btc",
    "cumulative_filled_qty_before_btc",
    "cumulative_filled_qty_after_btc",
    "remaining_order_qty_after_btc",
    "partial_fill_ordinal",
    "order_age_s",
    "queue_ahead_before_fill_btc",
    "target_price_tick",
    "target_price_displayed_qty_btc",
    "consecutive_units_after",
    "baseline_duration_ms",
    "campaign_age_s",
    "campaign_add_count",
    "campaign_mae_to_date_usdc",
    "campaign_inventory_time_to_date_btc_s",
    "last_same_side_fill_age_s",
    "last_opposite_side_fill_age_s",
    "cooldown_remaining_ms",
    "cooldown_lineage_revision_before",
)


class FeatureFinalizerError(RuntimeError):
    """Raised when the day panel cannot support one immutable OOF artifact."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FeatureFinalizerError(f"cannot read JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise FeatureFinalizerError(f"JSON root must be an object: {path}")
    return payload


def _is_sha256(value: Any) -> bool:
    text = str(value)
    return len(text) == 64 and all(
        character in "0123456789abcdef" for character in text
    )


def _binding_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(binding)
    payload.pop("binding_sha256", None)
    return payload


def _require_binding_digest(binding: Mapping[str, Any], *, label: str) -> None:
    observed = str(binding.get("binding_sha256", ""))
    if (
        not _is_sha256(observed)
        or _canonical_sha256(_binding_payload(binding)) != observed
    ):
        raise FeatureFinalizerError(f"{label} canonical binding drifted")


def _require_bound_file(
    path_value: Any,
    sha256_value: Any,
    *,
    label: str,
) -> Path:
    path = Path(str(path_value)).expanduser().resolve()
    expected = str(sha256_value)
    if not path.is_file() or not _is_sha256(expected) or _sha256(path) != expected:
        raise FeatureFinalizerError(f"{label} file binding drifted")
    return path


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}"
    try:
        with temporary.open("x", encoding="ascii") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(
        path,
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n",
    )


def _channel(name: str) -> str:
    if name.startswith(HISTORICAL_R0_PREDICATE_PREFIX):
        return "historical_bbo_mid_ema_pair"
    if name.startswith(panel.M0_PREDICATE_PREFIX):
        return "m0_context"
    if name.startswith("tri::"):
        body = name.removeprefix("tri::")
        return body.split("__h", 1)[0]
    if name.startswith("value::"):
        return name.removeprefix("value::").split("::", 1)[0]
    raise FeatureFinalizerError(f"cannot derive channel for {name!r}")


def _semantic(name: str) -> str:
    return name.rsplit("::", 1)[-1]


def _predicate_clock(name: str) -> str:
    if name.startswith(HISTORICAL_R0_PREDICATE_PREFIX):
        return "source_census_fill_visible_state"
    if name.startswith(panel.M0_PREDICATE_PREFIX):
        return "fill_visible_assignment_state"
    return "fill_visible_completed_100ms"


def _is_safe_historical_r0_continuous(name: str) -> bool:
    if name in {"ema_causal_volatility_bps", "ema_pair_favorable_fraction"}:
        return True
    if name.startswith(("ema_rel_mid_bps_h", "ema_slope_bps_per_s_h")):
        return True
    if not name.startswith("ema_pair_h"):
        return False
    return name.endswith(
        (
            "_cross_age_s",
            "_arrangement_persistence_s",
            "_favorable_distance_bps",
            "_abs_distance_bps",
            "_volatility_normalized",
            "_favorable_distance_velocity_bps_per_s",
        )
    )


def _validate_source_split(
    manifest: Mapping[str, Any],
    *,
    day: str,
    expected_m2: bool,
) -> dict[str, Any]:
    split = manifest.get("source_split_semantics")
    if not isinstance(split, Mapping):
        raise FeatureFinalizerError(f"feature day lacks source split semantics: {day}")
    m1_sha = split.get("normalized_m1_source_binding_sha256")
    m2_sha = split.get("raw_m2_source_binding_sha256")
    if (
        split.get("schema_version") != SOURCE_SPLIT_SCHEMA_VERSION
        or split.get("r0_source_identity") != EXPECTED_R0_SOURCE_IDENTITY
        or split.get("m1_source_identity") != EXPECTED_M1_SOURCE_IDENTITY
        or split.get("m1_supported") is not True
        or split.get("raw_m2_used_for_m1") is not False
        or not _is_sha256(m1_sha)
        or split.get("m2_source_identity") != EXPECTED_M2_SOURCE_IDENTITY
        or bool(split.get("m2_supported")) != expected_m2
        or bool(split.get("raw_m2_source_opened")) != expected_m2
        or (expected_m2 and not _is_sha256(m2_sha))
        or (not expected_m2 and m2_sha is not None)
    ):
        raise FeatureFinalizerError(f"feature source split drifted: {day}")
    return dict(split)


def _validate_provenance_bindings(
    manifest: Mapping[str, Any],
    *,
    day: str,
) -> dict[str, Any]:
    census = manifest.get("census_binding")
    m0 = manifest.get("m0_binding")
    if not isinstance(census, Mapping) or not isinstance(m0, Mapping):
        raise FeatureFinalizerError(f"feature provenance binding is absent: {day}")
    _require_binding_digest(census, label=f"{day} source census")
    _require_binding_digest(m0, label=f"{day} M0 provider")

    census_data = _require_bound_file(
        census.get("data_path"), census.get("data_sha256"), label=f"{day} census data"
    )
    census_manifest_path = _require_bound_file(
        census.get("manifest_path"),
        census.get("manifest_sha256"),
        label=f"{day} census manifest",
    )
    census_manifest = _load_json(census_manifest_path)
    census_execution = str(census_manifest.get("execution_identity_sha256", ""))
    if (
        census.get("identity") != EXPECTED_SOURCE_CENSUS_IDENTITY
        or census.get("utc_day") != day
        or census.get("economic_outcomes_read") is not False
        or census.get("arm_economic_labels_read") is not False
        or census.get("exact_queue_policy_eligible") is not False
        or census_manifest.get("identity") != EXPECTED_SOURCE_CENSUS_IDENTITY
        or census_manifest.get("utc_day") != day
        or census_manifest.get("economic_outcomes_read") is not False
        or census_manifest.get("validation_read") is not False
        or census_manifest.get("sealed_holdout_read") is not False
        or census_manifest.get("data_sha256") != _sha256(census_data)
        or not _is_sha256(census_execution)
    ):
        raise FeatureFinalizerError(f"source census authority drifted: {day}")

    m0_manifest_binding = m0.get("manifest")
    if not isinstance(m0_manifest_binding, Mapping):
        raise FeatureFinalizerError(f"M0 provider manifest binding is absent: {day}")
    m0_data = _require_bound_file(
        m0.get("path"), m0.get("sha256"), label=f"{day} M0 data"
    )
    m0_manifest_path = _require_bound_file(
        m0_manifest_binding.get("path"),
        m0_manifest_binding.get("sha256"),
        label=f"{day} M0 manifest",
    )
    m0_manifest = _load_json(m0_manifest_path)
    m0_execution = str(m0.get("execution_identity_sha256", ""))
    source_data_sha = str(census.get("data_sha256", ""))
    source_manifest_sha = str(census.get("manifest_sha256", ""))
    if (
        m0.get("mode") != "full_explicit_M0_enrichment"
        or m0.get("provider_identity") != panel.EXPECTED_M0_PROVIDER_IDENTITY
        or m0.get("economic_outcomes_read") is not False
        or m0.get("arm_economic_labels_read") is not False
        or m0_manifest_binding.get("identity") != panel.EXPECTED_M0_PROVIDER_IDENTITY
        or m0_manifest_binding.get("status") != panel.EXPECTED_M0_PROVIDER_STATUS
        or m0_manifest_binding.get("utc_day") != day
        or str(m0_manifest_binding.get("execution_identity_sha256", ""))
        != m0_execution
        or str(m0_manifest_binding.get("source_census_data_sha256", ""))
        != source_data_sha
        or str(m0_manifest_binding.get("source_census_manifest_sha256", ""))
        != source_manifest_sha
        or m0_manifest.get("identity") != panel.EXPECTED_M0_PROVIDER_IDENTITY
        or m0_manifest.get("status") != panel.EXPECTED_M0_PROVIDER_STATUS
        or m0_manifest.get("utc_day") != day
        or str(m0_manifest.get("execution_identity_sha256", "")) != m0_execution
        or str(m0_manifest.get("source_census_data_sha256", ""))
        != source_data_sha
        or str(m0_manifest.get("source_census_manifest_sha256", ""))
        != source_manifest_sha
        or str(m0_manifest.get("data_sha256", "")) != _sha256(m0_data)
        or int(m0_manifest.get("row_count", -1))
        != int(manifest.get("opportunity_count", -2))
        or m0_manifest.get("economic_outcomes_read") is not False
        or m0_manifest.get(
            "arm_economic_labels_read",
            m0_manifest.get("arm_outcomes_read"),
        )
        is not False
        or m0_manifest.get("duration_treatment_applied") is not False
        or m0_manifest.get("exact_queue_policy_eligible") is not False
        or not _is_sha256(m0_execution)
    ):
        raise FeatureFinalizerError(f"M0 provider authority drifted: {day}")
    if m0_execution == census_execution:
        raise FeatureFinalizerError(
            f"label and current M0 execution identities unexpectedly match: {day}"
        )
    return {
        "m0_execution_identity_sha256": m0_execution,
        "source_census_execution_identity_sha256": census_execution,
        "source_census_data_sha256": source_data_sha,
        "source_census_manifest_sha256": source_manifest_sha,
        "m0_binding_sha256": str(m0["binding_sha256"]),
    }


def _read_day(root: Path, day: str) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    day_root = root / day
    manifest_path = day_root / "manifest.json"
    success_path = day_root / panel.DAY_SUCCESS
    data_path = day_root / "M2.parquet"
    if not manifest_path.is_file() or not success_path.is_file() or not data_path.is_file():
        raise FeatureFinalizerError(f"incomplete feature day: {day}")
    manifest = _load_json(manifest_path)
    if (
        manifest.get("identity") != IDENTITY
        or manifest.get("utc_day") != day
        or manifest.get("full_m0_support") is not True
        or manifest.get("owner_modeled_queue") is not True
        or manifest.get("exact_queue_policy_eligible") is not False
        or manifest.get("economic_outcomes_read") is not False
        or manifest.get("arm_economic_labels_read") is not False
        or manifest.get("validation_read") is not False
        or manifest.get("sealed_holdout_read") is not False
    ):
        raise FeatureFinalizerError(f"feature day identity/permission drifted: {day}")
    expected_m2 = day in panel.M2_COMMON_SUPPORT_DAYS
    if bool(manifest.get("m2_day_supported")) != expected_m2:
        raise FeatureFinalizerError(f"M2 support split drifted: {day}")
    source_split = _validate_source_split(
        manifest,
        day=day,
        expected_m2=expected_m2,
    )
    provenance = _validate_provenance_bindings(manifest, day=day)
    blocks = manifest.get("blocks")
    if not isinstance(blocks, Mapping) or set(blocks) != set(panel.FEATURE_BLOCKS):
        raise FeatureFinalizerError(f"feature block inventory drifted: {day}")
    opportunity_count = int(manifest.get("opportunity_count", -1))
    for block in panel.FEATURE_BLOCKS:
        binding = blocks[block]
        if (
            not isinstance(binding, Mapping)
            or int(binding.get("row_count", -1)) != opportunity_count
            or _sha256(day_root / str(binding.get("path", "")))
            != str(binding.get("sha256", ""))
        ):
            raise FeatureFinalizerError(f"{day} {block} binding drifted")
    canonical = str(manifest.get("canonical_manifest_sha256", ""))
    without_digest = dict(manifest)
    without_digest.pop("canonical_manifest_sha256", None)
    if _canonical_sha256(without_digest) != canonical:
        raise FeatureFinalizerError(f"feature day canonical hash drifted: {day}")
    if success_path.read_text(encoding="ascii").strip() != canonical:
        raise FeatureFinalizerError(f"feature day success marker drifted: {day}")
    provenance["source_split_semantics"] = source_split
    return manifest, data_path, provenance


def _finite_columns(
    day_tables: Mapping[str, Path],
    candidates: Sequence[str],
    required_days: Sequence[str],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    remaining = set(candidates)
    rejected: set[str] = set()
    for day in required_days:
        if not remaining:
            break
        frame = pd.read_parquet(day_tables[day], columns=sorted(remaining))
        for name in tuple(remaining):
            numeric = pd.to_numeric(frame[name], errors="coerce").to_numpy(dtype=float)
            if not np.isfinite(numeric).all():
                remaining.remove(name)
                rejected.add(name)
    return tuple(sorted(remaining)), tuple(sorted(rejected))


def _validate_predicates(
    day_tables: Mapping[str, Path],
    columns: Sequence[str],
    required_days: Sequence[str],
) -> None:
    for day in required_days:
        frame = pd.read_parquet(day_tables[day], columns=list(columns))
        for name in columns:
            numeric = pd.to_numeric(frame[name], errors="coerce")
            if numeric.isna().any() or not numeric.isin((-1, 0, 1)).all():
                raise FeatureFinalizerError(
                    f"Boolean predicate {name!r} drifted on {day}"
                )


def _feature_schema(
    day_tables: Mapping[str, Path],
) -> tuple[dict[str, Any], dict[str, Any]]:
    reference = day_tables[
        next(
            day
            for day in panel.PREFIX40_DAYS
            if day in panel.M2_COMMON_SUPPORT_DAYS
        )
    ]
    names = tuple(pq.ParquetFile(reference).schema_arrow.names)
    name_set = set(names)
    historical_r0_predicates = tuple(
        sorted(
            name
            for name in names
            if name.startswith(HISTORICAL_R0_PREDICATE_PREFIX)
        )
    )
    historical_r0_continuous_candidates = tuple(
        sorted(name for name in names if _is_safe_historical_r0_continuous(name))
    )
    m0_predicates = tuple(
        sorted(name for name in names if name.startswith(panel.M0_PREDICATE_PREFIX))
    )
    tri_columns = tuple(sorted(name for name in names if name.startswith("tri::")))
    value_columns = tuple(sorted(name for name in names if name.startswith("value::")))
    if (
        len(historical_r0_predicates) != EXPECTED_HISTORICAL_R0_PREDICATE_COUNT
        or not historical_r0_continuous_candidates
        or not m0_predicates
        or not tri_columns
        or not value_columns
    ):
        raise FeatureFinalizerError("feature schema lacks M0 or market-state columns")

    for day, path in day_tables.items():
        day_names = tuple(pq.ParquetFile(path).schema_arrow.names)
        day_r0_predicates = tuple(
            sorted(
                name
                for name in day_names
                if name.startswith(HISTORICAL_R0_PREDICATE_PREFIX)
            )
        )
        day_r0_continuous = tuple(
            sorted(name for name in day_names if _is_safe_historical_r0_continuous(name))
        )
        if day_r0_predicates != historical_r0_predicates:
            raise FeatureFinalizerError(
                f"immutable historical R0 predicate universe drifted: {day}"
            )
        if day_r0_continuous != historical_r0_continuous_candidates:
            raise FeatureFinalizerError(
                f"safe historical R0 continuous universe drifted: {day}"
            )

    channels = {
        block: {item.name for item in feature_engine.CHANNELS_BY_BLOCK[block]}
        for block in ("R0", "M1", "M2")
    }
    m1_tri = tuple(name for name in tri_columns if _channel(name) in channels["M1"])
    m2_tri = tuple(name for name in tri_columns if _channel(name) in channels["M2"])
    m1_values = tuple(name for name in value_columns if _channel(name) in channels["M1"])
    m2_values = tuple(name for name in value_columns if _channel(name) in channels["M2"])
    m0_values = tuple(name for name in M0_CONTINUOUS_CANDIDATES if name in name_set)

    historical_r0_continuous, r0_rejected = _finite_columns(
        day_tables, historical_r0_continuous_candidates, panel.PREFIX40_DAYS
    )
    m0_continuous, m0_rejected = _finite_columns(
        day_tables, m0_values, panel.PREFIX40_DAYS
    )
    m1_market_continuous, m1_rejected = _finite_columns(
        day_tables, m1_values, panel.PREFIX40_DAYS
    )
    m2_market_continuous, m2_rejected = _finite_columns(
        day_tables, m2_values, panel.M2_COMMON_SUPPORT_DAYS
    )
    _validate_predicates(day_tables, historical_r0_predicates, panel.PREFIX40_DAYS)
    _validate_predicates(day_tables, m0_predicates, panel.PREFIX40_DAYS)
    _validate_predicates(day_tables, m1_tri, panel.PREFIX40_DAYS)
    _validate_predicates(day_tables, m2_tri, panel.M2_COMMON_SUPPORT_DAYS)

    blocks = {
        "R0": {
            "boolean_predicates": list(historical_r0_predicates),
            "continuous_features": list(historical_r0_continuous),
        },
        "M0": {
            "boolean_predicates": list(m0_predicates),
            "continuous_features": list(m0_continuous),
        },
        "M1": {
            "boolean_predicates": list(
                tuple(
                    dict.fromkeys(
                        (*historical_r0_predicates, *m0_predicates, *m1_tri)
                    )
                )
            ),
            "continuous_features": list(
                tuple(
                    dict.fromkeys(
                        (
                            *historical_r0_continuous,
                            *m0_continuous,
                            *m1_market_continuous,
                        )
                    )
                )
            ),
        },
        "M2": {
            "boolean_predicates": list(
                tuple(
                    dict.fromkeys(
                        (*historical_r0_predicates, *m0_predicates, *m2_tri)
                    )
                )
            ),
            "continuous_features": list(
                tuple(
                    dict.fromkeys(
                        (
                            *historical_r0_continuous,
                            *m0_continuous,
                            *m2_market_continuous,
                        )
                    )
                )
            ),
        },
    }
    if any(
        not payload["boolean_predicates"] or not payload["continuous_features"]
        for payload in blocks.values()
    ):
        raise FeatureFinalizerError("every feature block needs Boolean and continuous inputs")
    all_predicates = sorted(
        {name for block in blocks.values() for name in block["boolean_predicates"]}
    )
    groups = {
        "channel": {name: _channel(name) for name in all_predicates},
        "semantic": {name: _semantic(name) for name in all_predicates},
        "clock": {name: _predicate_clock(name) for name in all_predicates},
    }
    audit = {
        "continuous_missingness_selection_is_outcome_blind": True,
        "historical_R0_predicate_count": len(historical_r0_predicates),
        "historical_R0_predicate_universe_sha256": _canonical_sha256(
            list(historical_r0_predicates)
        ),
        "historical_R0_safe_continuous_candidates": list(
            historical_r0_continuous_candidates
        ),
        "R0_rejected_nonfinite": list(r0_rejected),
        "M0_rejected_nonfinite": list(m0_rejected),
        "M1_rejected_nonfinite": list(m1_rejected),
        "M2_rejected_nonfinite_on_prefix33": list(m2_rejected),
        "block_counts": {
            block: {
                "boolean_predicates": len(payload["boolean_predicates"]),
                "continuous_features": len(payload["continuous_features"]),
            }
            for block, payload in blocks.items()
        },
    }
    return {"feature_blocks": blocks, "predicate_groups": groups}, audit


def build_manifest(root: Path) -> dict[str, Any]:
    resolved = Path(root).expanduser().resolve()
    day_manifests: dict[str, dict[str, Any]] = {}
    day_tables: dict[str, Path] = {}
    m0_execution_identities: set[str] = set()
    source_census_execution_identities: set[str] = set()
    files: list[dict[str, Any]] = []
    seen_opportunities: set[str] = set()
    total_rows = 0
    for day in panel.PREFIX40_DAYS:
        day_manifest, data_path, provenance = _read_day(resolved, day)
        identifiers = pd.read_parquet(
            data_path,
            columns=["utc_day", "opportunity_id", "economic_outcomes_read"],
        )
        if (
            len(identifiers) != int(day_manifest["opportunity_count"])
            or not identifiers["utc_day"].astype(str).eq(day).all()
            or identifiers["opportunity_id"].astype(str).duplicated().any()
            or identifiers["economic_outcomes_read"].astype(bool).any()
        ):
            raise FeatureFinalizerError(f"feature denominator drifted: {day}")
        day_ids = set(identifiers["opportunity_id"].astype(str))
        if seen_opportunities & day_ids:
            raise FeatureFinalizerError("opportunity identity repeats across UTC days")
        seen_opportunities.update(day_ids)
        total_rows += len(identifiers)
        day_tables[day] = data_path
        m0_execution_identities.add(provenance["m0_execution_identity_sha256"])
        source_census_execution_identities.add(
            provenance["source_census_execution_identity_sha256"]
        )
        relative = data_path.relative_to(resolved).as_posix()
        files.append(
            {
                "relative_path": relative,
                "sha256": _sha256(data_path),
                "size_bytes": data_path.stat().st_size,
                "role": "cumulative_M2_row_projection_for_OOF",
                "utc_day": day,
            }
        )
        day_manifests[day] = {
            "manifest_relative_path": (data_path.parent / "manifest.json")
            .relative_to(resolved)
            .as_posix(),
            "manifest_sha256": _sha256(data_path.parent / "manifest.json"),
            "success_sha256": _sha256(data_path.parent / panel.DAY_SUCCESS),
            "opportunity_count": len(identifiers),
            "m2_day_supported": day in panel.M2_COMMON_SUPPORT_DAYS,
            "m0_binding_sha256": provenance["m0_binding_sha256"],
            "source_census_data_sha256": provenance[
                "source_census_data_sha256"
            ],
            "source_census_manifest_sha256": provenance[
                "source_census_manifest_sha256"
            ],
            "source_split_semantics": provenance["source_split_semantics"],
        }
    if (
        total_rows != EXPECTED_OPPORTUNITIES
        or len(seen_opportunities) != EXPECTED_OPPORTUNITIES
    ):
        raise FeatureFinalizerError("40-day feature opportunity count drifted")
    if len(m0_execution_identities) != 1:
        raise FeatureFinalizerError("40-day M0 execution identity is not shared")
    if len(source_census_execution_identities) != 1:
        raise FeatureFinalizerError("40-day source census execution identity is not shared")
    m0_execution_identity = next(iter(m0_execution_identities))
    source_census_execution_identity = next(
        iter(source_census_execution_identities)
    )
    if m0_execution_identity == source_census_execution_identity:
        raise FeatureFinalizerError(
            "label and feature execution identities must remain explicitly distinct"
        )
    feature_schema, feature_audit = _feature_schema(day_tables)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "status": "outcome_blind_40day_feature_panel_admitted",
        "label_join_key": "opportunity_id",
        "selected_table_glob": SELECTED_TABLE_GLOB,
        "opportunity_count": total_rows,
        "day_count": len(panel.PREFIX40_DAYS),
        "files": files,
        "day_bindings": day_manifests,
        "frozen_support_split": {
            "prefix40_days": list(panel.PREFIX40_DAYS),
            "m2_common_support_days": list(panel.M2_COMMON_SUPPORT_DAYS),
            "m2_excluded_days": sorted(panel.M2_EXCLUDED_DAYS),
        },
        "feature_schema": feature_schema,
        "feature_block_audit": feature_audit,
        "m0_execution_identity_sha256": m0_execution_identity,
        "source_census_execution_identity_sha256": (
            source_census_execution_identity
        ),
        "label_feature_execution_identity_same": False,
        "execution_identity_comparison": {
            "source_census_execution_identity_sha256": (
                source_census_execution_identity
            ),
            "current_m0_execution_identity_sha256": m0_execution_identity,
            "same": False,
        },
        "source_split_semantics": {
            "schema_version": SOURCE_SPLIT_SCHEMA_VERSION,
            "R0": EXPECTED_R0_SOURCE_IDENTITY,
            "M1": EXPECTED_M1_SOURCE_IDENTITY,
            "M1_support": "prefix40_all_days",
            "M2": EXPECTED_M2_SOURCE_IDENTITY,
            "M2_support": "frozen_prefix33_only",
            "raw_M2_used_for_M1": False,
        },
        "owner_modeled_queue": True,
        "queue_path_semantics": panel.QUEUE_PATH_SEMANTICS,
        "exact_queue_policy_eligible": False,
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_policy_authorized": False,
    }
    manifest["canonical_manifest_sha256"] = _canonical_sha256(manifest)
    return manifest


def finalize(root: Path) -> dict[str, Any]:
    resolved = Path(root).expanduser().resolve()
    manifest_path = resolved / MANIFEST_NAME
    success_path = resolved / SUCCESS_NAME
    if manifest_path.exists() or success_path.exists():
        raise FeatureFinalizerError("panel admission already exists; use validate")
    manifest = build_manifest(resolved)
    _atomic_json(manifest_path, manifest)
    _atomic_text(success_path, f"{_sha256(manifest_path)}\n")
    return manifest


def validate(root: Path) -> dict[str, Any]:
    resolved = Path(root).expanduser().resolve()
    manifest_path = resolved / MANIFEST_NAME
    success_path = resolved / SUCCESS_NAME
    if not manifest_path.is_file() or not success_path.is_file():
        raise FeatureFinalizerError("panel admission is incomplete")
    admitted = _load_json(manifest_path)
    if success_path.read_text(encoding="ascii").strip() != _sha256(manifest_path):
        raise FeatureFinalizerError("panel success marker drifted")
    rebuilt = build_manifest(resolved)
    if admitted != rebuilt:
        raise FeatureFinalizerError("panel manifest no longer matches day artifacts")
    return admitted


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("finalize", "validate"))
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args(argv)
    payload = finalize(args.root) if args.command == "finalize" else validate(args.root)
    print(
        json.dumps(
            {
                "identity": payload["identity"],
                "status": payload["status"],
                "day_count": payload["day_count"],
                "opportunity_count": payload["opportunity_count"],
                "canonical_manifest_sha256": payload["canonical_manifest_sha256"],
                "economic_outcomes_read": False,
                "action_authorized": False,
                "live_policy_authorized": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
