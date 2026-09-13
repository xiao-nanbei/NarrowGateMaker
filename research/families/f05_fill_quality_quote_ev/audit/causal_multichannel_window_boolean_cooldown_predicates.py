"""Outcome-blind Boolean predicate construction for cooldown-duration v2.

The artifact fitted here owns only deterministic feature thresholding.  It
never accepts economic columns, never fits on a transform frame, and never
selects a cooldown action.  Existing three-valued predicates are preserved;
continuous action and market state is converted to atomic quantile predicates
whose reference population and clock identity are hash-bound.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Any, Literal

import numpy as np
import pandas as pd

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    CHANNELS_BY_BLOCK,
    COOLDOWN_DEADLINE_OWNER_CATEGORIES,
    COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE,
    COOLDOWN_DEADLINE_OWNER_NONE,
    M0_REQUIRED_FIELDS,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
ARTIFACT_SCHEMA = f"{IDENTITY}.predicate_artifact.v1"
SOURCE_ROLES = (
    "outcome_blind_2025_single_channel",
    "inner_chronological_development",
)
FEATURE_BLOCKS = ("R0", "M0", "M1", "M2")
TRI_VALUES = frozenset({-1, 0, 1})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

_LEAKAGE_TOKENS = (
    "terminal",
    "pnl",
    "reward",
    "label",
    "outcome",
    "markout",
)
_NON_FEATURE_COLUMNS = frozenset(
    {
        "snapshot_id",
        "opportunity_id",
        "assignment_id",
        "fill_event_id",
        "client_order_id",
        "lineage_id",
        "utc_day",
        "day",
        "date",
        "source_panel_role",
        "feature_block",
        "visibility_profile",
        "source_bundle_sha256",
    }
)
_M0_CATEGORICAL_FIELDS = frozenset(
    {
        "side",
        "role_at_fill",
        "queue_state_before_fill",
        "target_price_displayed_qty_status",
        "target_price_displayed_qty_known",
        "fill_is_partial",
        "cooldown_blocker_active",
        "cooldown_deadline_owner",
    }
)
_M0_CONTINUOUS_FIELDS = (
    frozenset(M0_REQUIRED_FIELDS)
    - _M0_CATEGORICAL_FIELDS
    - {
        "assignment_ts_ns",
        "fill_visible_ts_ns",
        "target_price_displayed_qty_is_queue_ahead",
    }
)
_CATEGORICAL_DOMAINS = {
    "side": frozenset({"buy", "sell"}),
    "role_at_fill": frozenset({"opener", "add"}),
    "queue_state_before_fill": frozenset({"exact", "known_zero", "unknown"}),
    "target_price_displayed_qty_status": frozenset(
        {"exact", "known_zero", "unknown"}
    ),
    "target_price_displayed_qty_known": frozenset({"true", "false"}),
    "fill_is_partial": frozenset({"true", "false"}),
    "cooldown_blocker_active": frozenset({"true", "false"}),
    "cooldown_deadline_owner": frozenset(
        COOLDOWN_DEADLINE_OWNER_CATEGORIES
    ),
}


class PredicateContractError(ValueError):
    """Raised when fitting or transforming would violate the frozen contract."""


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


def _validate_no_leakage(columns: Sequence[Any]) -> None:
    bad = sorted(
        str(column)
        for column in columns
        if any(token in str(column).lower() for token in _LEAKAGE_TOKENS)
    )
    if bad:
        raise PredicateContractError(
            f"economic/label columns are prohibited in predicate inputs: {bad}"
        )


def _dtype_family(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series.dtype):
        return "bool"
    if pd.api.types.is_numeric_dtype(series.dtype):
        return "numeric"
    return "text"


def _normalize_days(values: Sequence[Any]) -> tuple[str, ...]:
    if not values:
        raise PredicateContractError("reference_days must be explicit and non-empty")
    days: list[str] = []
    for value in values:
        try:
            parsed = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise PredicateContractError(f"invalid reference day: {value!r}") from exc
        if parsed.tzinfo is not None:
            parsed = parsed.tz_convert("UTC").tz_localize(None)
        if parsed != parsed.normalize():
            raise PredicateContractError(f"reference day includes a time component: {value!r}")
        days.append(parsed.strftime("%Y-%m-%d"))
    normalized = tuple(sorted(set(days)))
    if len(normalized) != len(days):
        raise PredicateContractError("reference_days contain duplicates")
    return normalized


def _normalize_clock_identity(value: str | Mapping[str, str]) -> dict[str, str]:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise PredicateContractError("source_clock_identity is empty")
        return {"shared": text}
    if not isinstance(value, Mapping) or not value:
        raise PredicateContractError("source_clock_identity must be string or mapping")
    normalized = {str(key).strip(): str(item).strip() for key, item in value.items()}
    if any(not key or not item for key, item in normalized.items()):
        raise PredicateContractError("source clock identity has empty keys or values")
    return dict(sorted(normalized.items()))


def _channel_blocks() -> dict[str, str]:
    result: dict[str, str] = {}
    for block in ("R0", "M1", "M2"):
        for channel in CHANNELS_BY_BLOCK[block]:
            result.setdefault(channel.name, channel.block)
    return result


_CHANNEL_BLOCK = _channel_blocks()


def _feature_channel(field: str) -> str | None:
    if field.startswith("channel::"):
        pieces = field.split("::")
        return pieces[1] if len(pieces) > 2 else None
    if field.startswith("value::"):
        body = field.removeprefix("value::")
        for channel in sorted(_CHANNEL_BLOCK, key=len, reverse=True):
            if (
                body == channel
                or body.startswith(f"{channel}::")
                or body.startswith(f"{channel}__")
            ):
                return channel
    if field.startswith("tri::"):
        body = field.removeprefix("tri::")
        for channel in sorted(_CHANNEL_BLOCK, key=len, reverse=True):
            if (
                body == channel
                or body.startswith(f"{channel}::")
                or body.startswith(f"{channel}__")
            ):
                return channel
    return None


def _field_block(field: str) -> str | None:
    if field in M0_REQUIRED_FIELDS:
        return "M0"
    channel = _feature_channel(field)
    return None if channel is None else _CHANNEL_BLOCK[channel]


def _clock_group(field: str) -> Literal["book", "trade", "context"]:
    if field in M0_REQUIRED_FIELDS:
        return "context"
    channel = _feature_channel(field)
    name = (channel or field).lower()
    trade_tokens = (
        "aggressive_",
        "signed_flow",
        "trade_count",
        "run_length",
        "last_aggressive",
        "arrival_tempo",
    )
    book_tokens = (
        "mid",
        "spread",
        "bid",
        "ask",
        "bbo",
        "microprice",
        "depth",
        "queue",
        "depletion",
        "refill",
    )
    has_trade = any(token in name for token in trade_tokens)
    has_book = any(token in name for token in book_tokens)
    if has_trade and has_book:
        raise PredicateContractError(f"field combines book and trade channels: {field!r}")
    if has_trade:
        return "trade"
    if has_book:
        return "book"
    return "context"


def _predicate_name(field: str, quantile: float) -> str:
    basis_points = int(round(float(quantile) * 10_000.0))
    return f"tri::quantile::{field}::ge::q{basis_points:04d}"


def _categorical_definitions() -> tuple[PredicateDefinition, ...]:
    specs = (
        ("tri::m0::side::buy", "side", "BUY"),
        ("tri::m0::side::sell", "side", "SELL"),
        ("tri::m0::role::opener", "role_at_fill", "opener"),
        ("tri::m0::role::add", "role_at_fill", "add"),
        ("tri::m0::queue::exact", "queue_state_before_fill", "exact"),
        (
            "tri::m0::queue::known_zero",
            "queue_state_before_fill",
            "known_zero",
        ),
        ("tri::m0::queue::unknown", "queue_state_before_fill", "unknown"),
        (
            "tri::m0::target_displayed::exact",
            "target_price_displayed_qty_status",
            "exact",
        ),
        (
            "tri::m0::target_displayed::known_zero",
            "target_price_displayed_qty_status",
            "known_zero",
        ),
        (
            "tri::m0::target_displayed::unknown",
            "target_price_displayed_qty_status",
            "unknown",
        ),
        (
            "tri::m0::target_displayed::known",
            "target_price_displayed_qty_known",
            "true",
        ),
        (
            "tri::m0::target_displayed::not_known",
            "target_price_displayed_qty_known",
            "false",
        ),
        ("tri::m0::fill::partial", "fill_is_partial", "true"),
        ("tri::m0::fill::full", "fill_is_partial", "false"),
        (
            "tri::m0::cooldown_blocker::active",
            "cooldown_blocker_active",
            "true",
        ),
        (
            "tri::m0::cooldown_blocker::inactive",
            "cooldown_blocker_active",
            "false",
        ),
        (
            "tri::m0::cooldown_owner::none",
            "cooldown_deadline_owner",
            COOLDOWN_DEADLINE_OWNER_NONE,
        ),
        (
            "tri::m0::cooldown_owner::existing_same_side_lineage",
            "cooldown_deadline_owner",
            COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE,
        ),
    )
    return tuple(
        PredicateDefinition(
            name=name,
            kind="categorical_equals",
            source_field=field,
            block="M0",
            clock_group="context",
            category=category,
        )
        for name, field, category in specs
    )


@dataclass(frozen=True, slots=True)
class PredicateDefinition:
    """One atomic three-valued predicate."""

    name: str
    kind: str
    source_field: str
    block: str
    clock_group: str
    threshold: float | None = None
    quantile: float | None = None
    category: str | None = None

    def __post_init__(self) -> None:
        if not self.name.startswith("tri::"):
            raise PredicateContractError("predicate names must start with tri::")
        if self.kind not in {"preserved_tri", "categorical_equals", "quantile_ge"}:
            raise PredicateContractError(f"unknown predicate kind: {self.kind}")
        if self.block not in FEATURE_BLOCKS:
            raise PredicateContractError(f"invalid predicate block: {self.block}")
        if self.clock_group not in {"book", "trade", "context"}:
            raise PredicateContractError("invalid predicate clock group")
        if self.kind == "quantile_ge":
            if self.threshold is None or self.quantile is None:
                raise PredicateContractError("quantile predicate lacks threshold metadata")
            if not math.isfinite(self.threshold) or not 0.0 < self.quantile < 1.0:
                raise PredicateContractError("invalid quantile predicate metadata")


@dataclass(frozen=True, slots=True)
class PredicateTransformResult:
    columns: pd.DataFrame
    block_mapping: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True, slots=True)
class PredicateArtifact:
    """Hash-bound outcome-blind predicate definitions and reference metadata."""

    schema_version: str
    identity: str
    side: str
    source_role: str
    reference_identity_sha256: str
    reference_days: tuple[str, ...]
    source_clock_identity: Mapping[str, str]
    clock_separated_2025: bool
    quantiles: tuple[float, ...]
    input_schema: tuple[tuple[str, str], ...]
    definitions: tuple[PredicateDefinition, ...]

    def __post_init__(self) -> None:
        if self.schema_version != ARTIFACT_SCHEMA or self.identity != IDENTITY:
            raise PredicateContractError("predicate artifact identity drifted")
        if self.side not in {"BUY", "SELL"}:
            raise PredicateContractError("predicate artifact side must be BUY or SELL")
        if self.source_role not in SOURCE_ROLES:
            raise PredicateContractError("invalid predicate reference source role")
        if not _SHA256_RE.fullmatch(self.reference_identity_sha256):
            raise PredicateContractError("reference identity must be a lowercase SHA256")
        if self.clock_separated_2025 != (self.source_role == "outcome_blind_2025_single_channel"):
            raise PredicateContractError("2025 source role requires clock separation")
        if _normalize_days(self.reference_days) != self.reference_days:
            raise PredicateContractError("reference days are not canonical")
        normalized_clocks = _normalize_clock_identity(self.source_clock_identity)
        object.__setattr__(self, "source_clock_identity", MappingProxyType(normalized_clocks))
        if tuple(sorted(set(self.quantiles))) != self.quantiles:
            raise PredicateContractError("quantiles must be unique and increasing")
        if any(not 0.0 < value < 1.0 for value in self.quantiles):
            raise PredicateContractError("quantiles must lie inside (0, 1)")
        names = tuple(definition.name for definition in self.definitions)
        if names != tuple(sorted(names)) or len(names) != len(set(names)):
            raise PredicateContractError("predicate definitions must be unique and sorted")
        _validate_no_leakage(tuple(name for name, _ in self.input_schema))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "identity": self.identity,
            "side": self.side,
            "source_role": self.source_role,
            "reference_identity_sha256": self.reference_identity_sha256,
            "reference_days": list(self.reference_days),
            "source_clock_identity": dict(self.source_clock_identity),
            "clock_separated_2025": self.clock_separated_2025,
            "cross_channel_threshold_fitting": False,
            "clause_clock_policy": (
                "single_book_or_trade_clock_group"
                if self.clock_separated_2025
                else "cross_channel_allowed"
            ),
            "quantiles": list(self.quantiles),
            "input_schema": [list(item) for item in self.input_schema],
            "definitions": [asdict(definition) for definition in self.definitions],
        }

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.payload())

    def to_dict(self) -> dict[str, Any]:
        payload = self.payload()
        payload["canonical_sha256"] = self.canonical_sha256
        return payload

    def to_json(self) -> str:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> PredicateArtifact:
        payload = dict(raw)
        expected = str(payload.pop("canonical_sha256", ""))
        payload.pop("cross_channel_threshold_fitting", None)
        payload.pop("clause_clock_policy", None)
        try:
            artifact = cls(
                schema_version=str(payload["schema_version"]),
                identity=str(payload["identity"]),
                side=str(payload["side"]),
                source_role=str(payload["source_role"]),
                reference_identity_sha256=str(
                    payload["reference_identity_sha256"]
                ),
                reference_days=tuple(str(value) for value in payload["reference_days"]),
                source_clock_identity={
                    str(key): str(value)
                    for key, value in dict(payload["source_clock_identity"]).items()
                },
                clock_separated_2025=bool(payload["clock_separated_2025"]),
                quantiles=tuple(float(value) for value in payload["quantiles"]),
                input_schema=tuple(
                    (str(item[0]), str(item[1])) for item in payload["input_schema"]
                ),
                definitions=tuple(
                    PredicateDefinition(**dict(item)) for item in payload["definitions"]
                ),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PredicateContractError("invalid predicate artifact payload") from exc
        if not expected or expected != artifact.canonical_sha256:
            raise PredicateContractError("predicate artifact SHA256 drifted")
        return artifact

    @classmethod
    def from_json(cls, payload: str) -> PredicateArtifact:
        try:
            raw = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise PredicateContractError("invalid predicate artifact JSON") from exc
        if not isinstance(raw, Mapping):
            raise PredicateContractError("predicate artifact JSON must be an object")
        return cls.from_dict(raw)

    def validate_clause(self, predicate_names: Sequence[str]) -> None:
        """Fail if a 2025 clause combines provider-local book and trade clocks."""

        known = {definition.name: definition for definition in self.definitions}
        missing = sorted(set(predicate_names) - set(known))
        if missing:
            raise PredicateContractError(f"clause has unknown predicates: {missing}")
        if not self.clock_separated_2025:
            return
        groups = {
            known[name].clock_group
            for name in predicate_names
            if known[name].clock_group in {"book", "trade"}
        }
        if groups == {"book", "trade"}:
            raise PredicateContractError(
                "2025 clock-separated clauses cannot combine book and trade predicates"
            )

    def transform(
        self,
        frame: pd.DataFrame,
        *,
        expected_artifact_sha256: str | None = None,
    ) -> PredicateTransformResult:
        if expected_artifact_sha256 is not None and (
            expected_artifact_sha256 != self.canonical_sha256
        ):
            raise PredicateContractError("expected predicate artifact SHA256 drifted")
        _validate_frame_schema(frame, self.input_schema)
        normalized_sides = {
            str(value).strip().upper()
            for value in frame["side"].dropna().unique()
        }
        if normalized_sides != {self.side}:
            raise PredicateContractError(
                f"predicate target side drifted: expected={self.side} "
                f"observed={sorted(normalized_sides)}"
            )
        output: dict[str, np.ndarray] = {}
        for definition in self.definitions:
            series = frame[definition.source_field]
            if definition.kind == "preserved_tri":
                output[definition.name] = _preserve_tri(series, definition.name)
            elif definition.kind == "categorical_equals":
                output[definition.name] = _categorical_equals(
                    series,
                    definition.category or "",
                    source_field=definition.source_field,
                )
            else:
                output[definition.name] = _quantile_ge(series, float(definition.threshold))
        columns = pd.DataFrame(output, index=frame.index, dtype=np.int8)
        mapping = _block_mapping(self.definitions)
        return PredicateTransformResult(columns=columns, block_mapping=mapping)


def _validate_frame_schema(frame: pd.DataFrame, expected: tuple[tuple[str, str], ...]) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise PredicateContractError("predicate input must be a pandas DataFrame")
    _validate_no_leakage(tuple(frame.columns))
    actual = tuple(sorted((str(name), _dtype_family(frame[name])) for name in frame))
    if actual != expected:
        raise PredicateContractError(
            f"predicate input schema drifted: expected={expected}, actual={actual}"
        )


def _missing_mask(series: pd.Series) -> np.ndarray:
    if _dtype_family(series) == "text":
        lowered = series.astype("string").str.strip().str.lower()
        return (series.isna() | lowered.isin({"", "nan", "none", "null", "nat"})).to_numpy()
    return series.isna().to_numpy()


def _preserve_tri(series: pd.Series, name: str) -> np.ndarray:
    missing = _missing_mask(series)
    numeric = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    bad = ~missing & (~np.isfinite(numeric) | ~np.isin(numeric, tuple(TRI_VALUES)))
    if bad.any():
        raise PredicateContractError(f"existing tri-state predicate drifted: {name}")
    return np.where(missing, -1, numeric).astype(np.int8)


def _normalize_category_value(value: Any) -> str | None:
    if pd.isna(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return "true" if bool(value) else "false"
    text = str(value).strip().lower()
    return None if text in {"", "nan", "null", "nat"} else text


def _validate_categorical_domain(series: pd.Series, source_field: str) -> pd.Series:
    normalized = series.map(_normalize_category_value)
    observed = frozenset(str(value) for value in normalized.dropna().unique())
    allowed = _CATEGORICAL_DOMAINS[source_field]
    if not observed <= allowed:
        raise PredicateContractError(
            f"categorical field domain drifted for {source_field}: {sorted(observed - allowed)}"
        )
    return normalized


def _categorical_equals(series: pd.Series, category: str, *, source_field: str) -> np.ndarray:
    expected = category.lower()
    normalized = _validate_categorical_domain(series, source_field)
    known = normalized.notna().to_numpy()
    return np.where(known, (normalized == expected).to_numpy(dtype=np.int8), -1).astype(np.int8)


def _numeric_values(series: pd.Series, field: str) -> tuple[np.ndarray, np.ndarray]:
    missing = _missing_mask(series)
    numeric = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    bad = ~missing & ~np.isfinite(numeric)
    if bad.any():
        raise PredicateContractError(f"continuous field is nonnumeric/nonfinite: {field}")
    return numeric, missing


def _quantile_ge(series: pd.Series, threshold: float) -> np.ndarray:
    numeric, missing = _numeric_values(series, str(series.name))
    return np.where(missing, -1, numeric >= threshold).astype(np.int8)


def _block_mapping(
    definitions: Sequence[PredicateDefinition],
) -> dict[str, tuple[str, ...]]:
    by_block = {block: [] for block in FEATURE_BLOCKS}
    for definition in definitions:
        by_block[definition.block].append(definition.name)
    r0 = set(by_block["R0"])
    m0 = set(by_block["M0"])
    m1 = m0 | r0 | set(by_block["M1"])
    m2 = m1 | set(by_block["M2"])
    return {
        "R0": tuple(sorted(r0)),
        "M0": tuple(sorted(m0)),
        "M1": tuple(sorted(m1)),
        "M2": tuple(sorted(m2)),
    }


def fit_predicate_artifact(
    reference_frame: pd.DataFrame,
    *,
    side: str,
    source_role: str,
    reference_identity_sha256: str,
    reference_days: Sequence[Any],
    source_clock_identity: str | Mapping[str, str],
    quantiles: Sequence[float] = (0.25, 0.5, 0.75),
) -> PredicateArtifact:
    """Fit atomic thresholds from an explicit outcome-blind reference frame."""

    if not isinstance(reference_frame, pd.DataFrame) or reference_frame.empty:
        raise PredicateContractError("reference_frame must be a non-empty DataFrame")
    if reference_frame.columns.has_duplicates:
        raise PredicateContractError("reference_frame has duplicate columns")
    if source_role not in SOURCE_ROLES:
        raise PredicateContractError(f"unsupported reference source role: {source_role}")
    normalized_side = str(side).strip().upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise PredicateContractError("predicate reference side must be BUY or SELL")
    if not _SHA256_RE.fullmatch(str(reference_identity_sha256)):
        raise PredicateContractError("reference identity must be a lowercase SHA256")
    _validate_no_leakage(tuple(reference_frame.columns))
    days = _normalize_days(reference_days)
    day_column = "utc_day" if "utc_day" in reference_frame else None
    if day_column is None and "day" in reference_frame:
        day_column = "day"
    if day_column is None:
        raise PredicateContractError("reference frame must contain utc_day or day")
    observed_days = _normalize_days(
        tuple(reference_frame[day_column].dropna().astype(str).unique())
    )
    if observed_days != days:
        raise PredicateContractError(
            "reference_days do not match the explicit reference frame"
        )
    if "side" not in reference_frame:
        raise PredicateContractError("reference frame must contain side")
    observed_sides = {
        str(value).strip().upper()
        for value in reference_frame["side"].dropna().unique()
    }
    if observed_sides != {normalized_side}:
        raise PredicateContractError(
            "predicate thresholds must be fitted on one explicit side"
        )
    clocks = _normalize_clock_identity(source_clock_identity)
    clock_separated = source_role == "outcome_blind_2025_single_channel"
    reference_clock_groups = {
        _clock_group(str(field))
        for field in reference_frame.columns
        if str(field).startswith(("tri::", "value::")) and _field_block(str(field)) is not None
    } - {"context"}
    if clock_separated:
        if "shared" in clocks and len(reference_clock_groups) > 1:
            raise PredicateContractError(
                "2025 mixed book/trade reference requires separate clock identities"
            )
        if "shared" not in clocks and not reference_clock_groups <= set(clocks):
            raise PredicateContractError(
                "2025 reference lacks a source clock identity for each channel"
            )
    normalized_quantiles = tuple(sorted(set(float(value) for value in quantiles)))
    if not normalized_quantiles or any(
        not math.isfinite(value) or not 0.0 < value < 1.0 for value in normalized_quantiles
    ):
        raise PredicateContractError("quantiles must be finite and inside (0, 1)")

    schema = tuple(
        sorted((str(name), _dtype_family(reference_frame[name])) for name in reference_frame)
    )
    definitions: list[PredicateDefinition] = []
    for field in sorted(str(name) for name in reference_frame.columns):
        if field.startswith("tri::"):
            block = _field_block(field)
            if block is None:
                raise PredicateContractError(
                    f"existing tri-state predicate has unknown feature block: {field}"
                )
            _preserve_tri(reference_frame[field], field)
            definitions.append(
                PredicateDefinition(
                    name=field,
                    kind="preserved_tri",
                    source_field=field,
                    block=block,
                    clock_group=_clock_group(field),
                )
            )

    present_m0 = set(reference_frame).intersection(M0_REQUIRED_FIELDS)
    if present_m0 and not _M0_CATEGORICAL_FIELDS <= present_m0:
        missing = sorted(_M0_CATEGORICAL_FIELDS - present_m0)
        raise PredicateContractError(f"M0 categorical context is incomplete: {missing}")
    if _M0_CATEGORICAL_FIELDS <= present_m0:
        for field in sorted(_M0_CATEGORICAL_FIELDS):
            _validate_categorical_domain(reference_frame[field], field)
        definitions.extend(_categorical_definitions())

    continuous_fields = [
        field
        for field in sorted(str(name) for name in reference_frame.columns)
        if field in _M0_CONTINUOUS_FIELDS or field.startswith("value::")
    ]
    for field in continuous_fields:
        block = _field_block(field)
        if block is None:
            raise PredicateContractError(f"continuous field has unknown block: {field}")
        clock_group = _clock_group(field)
        numeric, missing = _numeric_values(reference_frame[field], field)
        observed = numeric[~missing]
        if not len(observed):
            continue
        thresholds = np.quantile(
            observed,
            np.asarray(normalized_quantiles, dtype=float),
            method="linear",
        )
        for quantile, threshold in zip(normalized_quantiles, thresholds.tolist(), strict=True):
            definitions.append(
                PredicateDefinition(
                    name=_predicate_name(field, quantile),
                    kind="quantile_ge",
                    source_field=field,
                    block=block,
                    clock_group=clock_group,
                    threshold=float(threshold),
                    quantile=float(quantile),
                )
            )

    definitions.sort(key=lambda item: item.name)
    return PredicateArtifact(
        schema_version=ARTIFACT_SCHEMA,
        identity=IDENTITY,
        side=normalized_side,
        source_role=source_role,
        reference_identity_sha256=str(reference_identity_sha256),
        reference_days=days,
        source_clock_identity=clocks,
        clock_separated_2025=clock_separated,
        quantiles=normalized_quantiles,
        input_schema=schema,
        definitions=tuple(definitions),
    )


def transform_predicates(
    frame: pd.DataFrame,
    artifact: PredicateArtifact,
    *,
    expected_artifact_sha256: str | None = None,
) -> PredicateTransformResult:
    """Apply a frozen artifact without fitting anything on the target frame."""

    return artifact.transform(frame, expected_artifact_sha256=expected_artifact_sha256)


__all__ = [
    "ARTIFACT_SCHEMA",
    "IDENTITY",
    "PredicateArtifact",
    "PredicateContractError",
    "PredicateDefinition",
    "PredicateTransformResult",
    "fit_predicate_artifact",
    "transform_predicates",
]
