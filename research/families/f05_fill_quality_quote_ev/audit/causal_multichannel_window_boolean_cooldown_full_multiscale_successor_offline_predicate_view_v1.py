"""Hash-bound offline predicate view for the F05 full-multiscale successor.

The admitted mechanics panel stores causal raw continuous values and the
primitive three-valued EMA states.  This module applies the frozen 2025
outcome-blind predicate artifacts to those values and materializes the exact
same predicate view for panel fitting and repeated-policy snapshot execution.
It is offline-only and does not expose a live hook.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_predicates as predicates,
)

IDENTITY = (
    "causal_multichannel_window_boolean_cooldown_full_multiscale_successor_"
    "offline_predicate_view_v1"
)

_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_BUNDLE_SCHEMA = (
    "causal_multichannel_window_boolean_cooldown_duration_v2."
    "multiday_label_panel_nested_oof.v1.predicate_bundle.v1"
)
_BUNDLE_IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
_GROUPS = ("book", "trade")
_SIDES = ("BUY", "SELL")
_MID_OBSERVED = "channel::mid_usdc_per_btc::observed"
_SHORT_CROSS_AGE = "value::mid_usdc_per_btc__h4s__h16s::cross_age_s"
_LONG_CROSS_AGE = "value::mid_usdc_per_btc__h16s__h256s::cross_age_s"
_CROSS_THRESHOLD_S = 16.0


class OfflinePredicateViewError(ValueError):
    """Raised when a predicate bundle or source row cannot be trusted."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("ascii")).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OfflinePredicateViewError(f"cannot load {label}: {path}") from exc
    if not isinstance(raw, dict):
        raise OfflinePredicateViewError(f"{label} root must be an object")
    return raw


def _require_sha(value: Any, *, label: str) -> str:
    digest = str(value).strip().lower()
    if _SHA_RE.fullmatch(digest) is None:
        raise OfflinePredicateViewError(f"{label} is not a lowercase SHA256")
    return digest


@dataclass(frozen=True, slots=True)
class FrozenPredicateBundle:
    path: Path
    file_sha256: str
    canonical_sha256: str
    artifacts: Mapping[str, predicates.PredicateArtifact]
    artifact_file_sha256: Mapping[str, str]

    def receipt(self) -> dict[str, Any]:
        return {
            "identity": IDENTITY,
            "bundle_file_sha256": self.file_sha256,
            "bundle_canonical_sha256": self.canonical_sha256,
            "artifact_file_sha256": dict(sorted(self.artifact_file_sha256.items())),
            "artifact_canonical_sha256": {
                key: artifact.canonical_sha256
                for key, artifact in sorted(self.artifacts.items())
            },
            "reference_days_are_2025": all(
                all(day.startswith("2025-") for day in artifact.reference_days)
                for artifact in self.artifacts.values()
            ),
        }


def load_frozen_predicate_bundle(
    path: str | Path,
    *,
    expected_file_sha256: str,
) -> FrozenPredicateBundle:
    bundle_path = Path(path).expanduser().resolve()
    observed_file_sha = _file_sha256(bundle_path)
    if observed_file_sha != _require_sha(
        expected_file_sha256, label="predicate bundle file SHA256"
    ):
        raise OfflinePredicateViewError("predicate bundle file SHA256 drifted")
    raw = _load_json(bundle_path, label="predicate bundle")
    expected_canonical = _require_sha(
        raw.get("canonical_sha256"), label="predicate bundle canonical SHA256"
    )
    canonical_body = dict(raw)
    canonical_body.pop("canonical_sha256", None)
    if _canonical_sha256(canonical_body) != expected_canonical:
        raise OfflinePredicateViewError("predicate bundle canonical SHA256 drifted")
    if (
        raw.get("identity") != _BUNDLE_IDENTITY
        or raw.get("schema_version") != _BUNDLE_SCHEMA
        or raw.get("m0_artifacts") != []
        or raw.get("cross_clock_clause_authorized") is not False
    ):
        raise OfflinePredicateViewError("predicate bundle identity drifted")
    strict_target = raw.get("strict_2026_target_snapshot")
    if (
        not isinstance(strict_target, Mapping)
        or strict_target.get("book_trade_predicates_may_be_combined_by_study")
        is not True
    ):
        raise OfflinePredicateViewError("predicate bundle target join contract is missing")

    loaded: dict[str, predicates.PredicateArtifact] = {}
    file_hashes: dict[str, str] = {}
    for group in _GROUPS:
        entries = raw.get(group)
        if not isinstance(entries, Mapping) or set(entries) != set(_SIDES):
            raise OfflinePredicateViewError(f"predicate bundle {group} census drifted")
        for side in _SIDES:
            entry = entries[side]
            if not isinstance(entry, Mapping):
                raise OfflinePredicateViewError(f"predicate artifact entry is malformed: {group}.{side}")
            relative = str(entry.get("path", ""))
            if not relative or Path(relative).is_absolute() or ".." in Path(relative).parts:
                raise OfflinePredicateViewError("predicate artifact path is not bundle-relative")
            artifact_path = (bundle_path.parent / relative).resolve()
            try:
                artifact_path.relative_to(bundle_path.parent.resolve())
            except ValueError as exc:
                raise OfflinePredicateViewError("predicate artifact escaped the bundle root") from exc
            expected_artifact_sha = _require_sha(
                entry.get("sha256"), label=f"{group}.{side} artifact SHA256"
            )
            if _file_sha256(artifact_path) != expected_artifact_sha:
                raise OfflinePredicateViewError(f"predicate artifact SHA256 drifted: {group}.{side}")
            try:
                artifact = predicates.PredicateArtifact.from_dict(
                    _load_json(artifact_path, label=f"{group}.{side} predicate artifact")
                )
            except predicates.PredicateContractError as exc:
                raise OfflinePredicateViewError(
                    f"predicate artifact contract drifted: {group}.{side}"
                ) from exc
            if (
                artifact.side != side
                or artifact.source_role != "outcome_blind_2025_single_channel"
                or not artifact.clock_separated_2025
                or any(not day.startswith("2025-") for day in artifact.reference_days)
                or {definition.clock_group for definition in artifact.definitions} != {group}
            ):
                raise OfflinePredicateViewError(f"predicate artifact source drifted: {group}.{side}")
            key = f"{group}.{side}"
            loaded[key] = artifact
            file_hashes[key] = expected_artifact_sha
    return FrozenPredicateBundle(
        path=bundle_path,
        file_sha256=observed_file_sha,
        canonical_sha256=expected_canonical,
        artifacts=MappingProxyType(loaded),
        artifact_file_sha256=MappingProxyType(file_hashes),
    )


def _artifact_input_frame(
    source: pd.DataFrame,
    artifact: predicates.PredicateArtifact,
) -> pd.DataFrame:
    columns = tuple(name for name, _ in artifact.input_schema)
    missing = sorted(set(columns) - set(source.columns))
    if missing:
        raise OfflinePredicateViewError(
            f"canonical feature rows lack predicate inputs: {missing[:12]}"
        )
    return source.loc[:, list(columns)].copy()


def _transform_artifact(
    source: pd.DataFrame,
    artifact: predicates.PredicateArtifact,
) -> pd.DataFrame:
    try:
        transformed = artifact.transform(
            _artifact_input_frame(source, artifact),
            expected_artifact_sha256=artifact.canonical_sha256,
        ).columns
    except predicates.PredicateContractError as exc:
        raise OfflinePredicateViewError("predicate transform failed closed") from exc
    transformed.index = source.index
    return transformed.astype(np.int8)


def _tri_numeric_le(
    values: pd.Series,
    observed: pd.Series,
    *,
    threshold: float,
) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    observed_mask = observed.eq(1) & numeric.notna() & np.isfinite(numeric)
    valid_nonnegative = observed_mask & numeric.ge(0.0)
    output = np.full(len(values), -1, dtype=np.int8)
    output[valid_nonnegative.to_numpy()] = numeric.loc[valid_nonnegative].le(
        threshold
    ).to_numpy(dtype=np.int8)
    return pd.Series(output, index=values.index, dtype=np.int8)


def _direct_panel_predicates(source: pd.DataFrame) -> pd.DataFrame:
    required = {
        _MID_OBSERVED,
        _SHORT_CROSS_AGE,
        _LONG_CROSS_AGE,
        "campaign_age_s",
        "baseline_duration_ms",
    }
    missing = sorted(required - set(source.columns))
    if missing:
        raise OfflinePredicateViewError(f"fixed predicate inputs are missing: {missing}")
    observed = pd.to_numeric(source[_MID_OBSERVED], errors="coerce")
    campaign_age = pd.to_numeric(source["campaign_age_s"], errors="coerce")
    baseline_ms = pd.to_numeric(source["baseline_duration_ms"], errors="coerce")
    campaign_valid = (
        campaign_age.notna()
        & baseline_ms.notna()
        & np.isfinite(campaign_age)
        & np.isfinite(baseline_ms)
        & campaign_age.ge(0.0)
        & baseline_ms.gt(0.0)
    )
    campaign = np.full(len(source), -1, dtype=np.int8)
    campaign[campaign_valid.to_numpy()] = (
        campaign_age.loc[campaign_valid].mul(1_000.0)
        > baseline_ms.loc[campaign_valid]
    ).to_numpy(dtype=np.int8)
    return pd.DataFrame(
        {
            successor.CURRENT_SHORT_CROSS: _tri_numeric_le(
                source[_SHORT_CROSS_AGE], observed, threshold=_CROSS_THRESHOLD_S
            ),
            successor.CURRENT_LONG_CROSS: _tri_numeric_le(
                source[_LONG_CROSS_AGE], observed, threshold=_CROSS_THRESHOLD_S
            ),
            successor.CURRENT_CAMPAIGN_AGE: pd.Series(
                campaign, index=source.index, dtype=np.int8
            ),
        },
        index=source.index,
    )


def materialize_panel_predicates(
    *,
    metadata: pd.DataFrame,
    primitive_boolean: pd.DataFrame,
    continuous: pd.DataFrame,
    bundle: FrozenPredicateBundle,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not (
        metadata.index.equals(primitive_boolean.index)
        and metadata.index.equals(continuous.index)
    ):
        raise OfflinePredicateViewError("predicate panel inputs are not row aligned")
    source = pd.concat((metadata, primitive_boolean, continuous), axis=1)
    if source.columns.duplicated().any():
        duplicated = source.columns[source.columns.duplicated()].tolist()
        raise OfflinePredicateViewError(f"predicate source columns collide: {duplicated[:12]}")
    if "side" not in source:
        raise OfflinePredicateViewError("predicate source lacks side")
    side_values = source["side"].astype(str).str.upper()
    if set(side_values) != set(_SIDES):
        raise OfflinePredicateViewError("predicate source must preserve BUY and SELL")

    parts: list[pd.DataFrame] = []
    side_counts: dict[str, int] = {}
    for side in _SIDES:
        side_source = source.loc[side_values == side]
        side_counts[side] = len(side_source)
        transformed = [
            _transform_artifact(side_source, bundle.artifacts[f"{group}.{side}"])
            for group in _GROUPS
        ]
        combined = pd.concat((*transformed, _direct_panel_predicates(side_source)), axis=1)
        if combined.columns.duplicated().any():
            duplicated = combined.columns[combined.columns.duplicated()].tolist()
            raise OfflinePredicateViewError(
                f"expanded predicate columns collide: {duplicated[:12]}"
            )
        parts.append(combined)
    expanded = pd.concat(parts, axis=0).loc[metadata.index]
    if not np.isin(expanded.to_numpy(copy=False), (-1, 0, 1)).all():
        raise OfflinePredicateViewError("expanded predicates escaped three-valued semantics")

    primitive_names = set(primitive_boolean.columns)
    comparable = sorted(primitive_names & set(expanded.columns))
    if comparable and not primitive_boolean.loc[:, comparable].astype(np.int8).equals(
        expanded.loc[:, comparable].astype(np.int8)
    ):
        raise OfflinePredicateViewError("frozen artifact changed primitive tri-state values")
    receipt = {
        "identity": IDENTITY,
        "bundle": bundle.receipt(),
        "rows": int(len(expanded)),
        "side_rows": side_counts,
        "primitive_predicate_count": int(len(primitive_boolean.columns)),
        "expanded_predicate_count": int(len(expanded.columns)),
        "primitive_overlap_count": int(len(comparable)),
        "fixed_predicates": [
            successor.CURRENT_CAMPAIGN_AGE,
            successor.CURRENT_LONG_CROSS,
            successor.CURRENT_SHORT_CROSS,
        ],
        "economic_outcomes_read": False,
    }
    receipt["canonical_sha256"] = _canonical_sha256(receipt)
    return expanded.astype(np.int8), receipt


def _utc_day_from_feature_row(feature_row: Mapping[str, Any]) -> str:
    raw = feature_row.get("decision_ts_ns")
    if isinstance(raw, bool) or not isinstance(raw, (int, np.integer)) or int(raw) <= 0:
        raise OfflinePredicateViewError("snapshot decision_ts_ns is invalid")
    return pd.Timestamp(int(raw), unit="ns", tz="UTC").strftime("%Y-%m-%d")


def _definition_value(
    definition: predicates.PredicateDefinition,
    feature_row: Mapping[str, Any],
) -> np.int8:
    field = definition.source_field
    raw = feature_row.get(field)
    is_missing = raw is None
    if not is_missing:
        try:
            missing_value = pd.isna(raw)
        except (TypeError, ValueError):
            missing_value = False
        is_missing = bool(missing_value) if isinstance(missing_value, (bool, np.bool_)) else False
    if isinstance(raw, str) and raw.strip().lower() in {
        "",
        "nan",
        "none",
        "null",
        "nat",
    }:
        is_missing = True
    if definition.kind == "preserved_tri":
        if is_missing:
            return np.int8(-1)
        try:
            numeric = float(raw)
        except (TypeError, ValueError) as exc:
            raise OfflinePredicateViewError(f"invalid tri-state source: {field}") from exc
        if not math.isfinite(numeric) or numeric not in {-1.0, 0.0, 1.0}:
            raise OfflinePredicateViewError(f"tri-state source is out of range: {field}")
        return np.int8(int(numeric))
    if definition.kind == "categorical_equals":
        if is_missing:
            return np.int8(-1)
        return np.int8(1 if str(raw).strip().lower() == str(definition.category).lower() else 0)
    if is_missing:
        return np.int8(-1)
    try:
        numeric = float(raw)
    except (TypeError, ValueError) as exc:
        raise OfflinePredicateViewError(f"invalid numeric predicate source: {field}") from exc
    if not math.isfinite(numeric):
        raise OfflinePredicateViewError(f"nonfinite numeric predicate source: {field}")
    assert definition.threshold is not None
    return np.int8(1 if numeric >= float(definition.threshold) else 0)


def _direct_snapshot_predicate(
    name: str,
    *,
    feature_row: Mapping[str, Any],
    baseline_duration_ms: int,
) -> np.int8:
    if name == successor.CURRENT_CAMPAIGN_AGE:
        raw = feature_row.get("campaign_age_s")
        if raw is None or isinstance(raw, bool):
            return np.int8(-1)
        try:
            age = float(raw)
        except (TypeError, ValueError):
            return np.int8(-1)
        if not math.isfinite(age) or age < 0.0 or baseline_duration_ms <= 0:
            return np.int8(-1)
        return np.int8(1 if age * 1_000.0 > baseline_duration_ms else 0)
    if name in {successor.CURRENT_SHORT_CROSS, successor.CURRENT_LONG_CROSS}:
        observed = feature_row.get(_MID_OBSERVED)
        if isinstance(observed, bool):
            return np.int8(-1)
        try:
            observed_state = int(observed)
        except (TypeError, ValueError):
            return np.int8(-1)
        if observed_state != 1:
            return np.int8(-1)
        source = _SHORT_CROSS_AGE if name == successor.CURRENT_SHORT_CROSS else _LONG_CROSS_AGE
        raw = feature_row.get(source)
        if raw is None or isinstance(raw, bool):
            return np.int8(-1)
        try:
            age = float(raw)
        except (TypeError, ValueError):
            return np.int8(-1)
        if not math.isfinite(age) or age < 0.0:
            return np.int8(-1)
        return np.int8(1 if age <= _CROSS_THRESHOLD_S else 0)
    raise OfflinePredicateViewError(f"unsupported direct predicate: {name}")


def materialize_snapshot_predicates(
    *,
    predicate_names: Sequence[str],
    feature_row: Mapping[str, Any],
    side: str,
    baseline_duration_ms: int,
    bundle: FrozenPredicateBundle,
) -> dict[str, int]:
    normalized_side = str(side).upper()
    if normalized_side not in _SIDES:
        raise OfflinePredicateViewError("snapshot predicate side is invalid")
    enriched = dict(feature_row)
    if "utc_day" not in enriched:
        enriched["utc_day"] = _utc_day_from_feature_row(enriched)
    definitions: dict[str, predicates.PredicateDefinition] = {}
    for group in _GROUPS:
        artifact = bundle.artifacts[f"{group}.{normalized_side}"]
        for definition in artifact.definitions:
            prior = definitions.get(definition.name)
            if prior is not None and prior != definition:
                raise OfflinePredicateViewError("predicate definition collision")
            definitions[definition.name] = definition
    output: dict[str, int] = {}
    for name in tuple(dict.fromkeys(str(value) for value in predicate_names)):
        definition = definitions.get(name)
        if definition is not None:
            output[name] = int(_definition_value(definition, enriched))
        else:
            output[name] = int(
                _direct_snapshot_predicate(
                    name,
                    feature_row=enriched,
                    baseline_duration_ms=baseline_duration_ms,
                )
            )
    return output


__all__ = [
    "FrozenPredicateBundle",
    "IDENTITY",
    "OfflinePredicateViewError",
    "load_frozen_predicate_bundle",
    "materialize_panel_predicates",
    "materialize_snapshot_predicates",
]
