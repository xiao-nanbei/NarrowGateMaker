"""Side-specific discrete-time hazards for F02 aggressive first passage.

The module owns only the reach estimand.  It deliberately excludes queue,
fill, order-lifecycle, inventory, and economic outcomes.  A model estimates
the single-event hazard in each 100 ms interval through the 30 second
administrative censor.  The resulting first-passage CDF is obtained by
survival integration, never by fitting a separate horizon classifier.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
from scipy.optimize import minimize

IDENTITY = "p3_aggressive_reach_time_conditioned_hazard_v1"
ARTIFACT_SCHEMA_VERSION = "narrowgate_p3_reach_time_conditioned_hazard.model.v1"
RISK_ROW_SCHEMA_VERSION = "narrowgate_p3_reach_time_conditioned_hazard.risk_rows.v1"

SIDES = ("BUY", "SELL")
TIME_STEP_MS = 100
ADMINISTRATIVE_CENSOR_MS = 30_000
RIGHT_CENSORED_TIME_MS = -1

RAW_DISTANCE_FEATURE = "raw_distance_usdc_per_btc"
TIME_UPPER_FEATURE = "time_upper_ms"
FAST_NORMALIZED_DISTANCE_FEATURE = "distance_over_fast_sigma_sqrt_time"
SLOW_NORMALIZED_DISTANCE_FEATURE = "distance_over_slow_sigma_sqrt_time"
FAST_SIGMA_FEATURE = "fast_sigma_usdc_per_sqrt_s"
SLOW_SIGMA_FEATURE = "slow_sigma_usdc_per_sqrt_s"
STRUCTURAL_FEATURES = (
    RAW_DISTANCE_FEATURE,
    TIME_UPPER_FEATURE,
    FAST_NORMALIZED_DISTANCE_FEATURE,
    SLOW_NORMALIZED_DISTANCE_FEATURE,
)

_FEATURE_NAME_RE = re.compile(r"[a-z][a-z0-9_]*")
_FORBIDDEN_FEATURE_TOKENS = frozenset(
    {
        "source",
        "year",
        "queue",
        "fill",
        "filled",
        "lifecycle",
        "pnl",
        "reward",
        "markout",
    }
)
_PROBABILITY_TOLERANCE = 128.0 * np.finfo(np.float64).eps
_RATE_PROBABILITY_FLOOR = 1e-12
_ARTIFACT_FILES = frozenset({"metadata.json", "model.txt"})

__all__ = [
    "ADMINISTRATIVE_CENSOR_MS",
    "FAST_NORMALIZED_DISTANCE_FEATURE",
    "FAST_SIGMA_FEATURE",
    "HazardGridSpec",
    "HazardRiskRows",
    "PositiveRateCalibration",
    "FirstPassageDistribution",
    "SideHazardModel",
    "SLOW_NORMALIZED_DISTANCE_FEATURE",
    "SLOW_SIGMA_FEATURE",
    "DistanceQuerySample",
    "ArtifactHashes",
    "sample_distance_queries",
    "build_hazard_risk_rows",
    "fit_positive_rate_calibration",
    "hazards_to_first_passage",
    "lightgbm_monotone_constraints",
    "fit_side_hazard_model",
    "save_side_hazard_artifact",
    "load_side_hazard_artifact",
]


def canonical_sha256(payload: Any) -> str:
    """Return a stable SHA256 for a JSON-compatible identity payload."""

    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _side(value: object) -> str:
    side = str(value).strip().upper()
    if side not in SIDES:
        raise ValueError(f"side must be one of {SIDES}, got {value!r}")
    return side


def _readonly_array(values: object, *, dtype: np.dtype[Any]) -> np.ndarray:
    array = np.asarray(values, dtype=dtype).copy()
    array.setflags(write=False)
    return array


@dataclass(frozen=True)
class HazardGridSpec:
    """Formal 100 ms grid with a 30 second administrative right censor."""

    time_step_ms: int = TIME_STEP_MS
    max_horizon_ms: int = ADMINISTRATIVE_CENSOR_MS
    right_censored_time_ms: int = RIGHT_CENSORED_TIME_MS

    def __post_init__(self) -> None:
        if self.time_step_ms != TIME_STEP_MS:
            raise ValueError("formal reach hazard requires a 100 ms grid")
        if self.max_horizon_ms != ADMINISTRATIVE_CENSOR_MS:
            raise ValueError("formal reach hazard requires a 30 second censor")
        if self.right_censored_time_ms != RIGHT_CENSORED_TIME_MS:
            raise ValueError("formal reach hazard requires the frozen censor sentinel")

    @property
    def n_time_bins(self) -> int:
        return self.max_horizon_ms // self.time_step_ms

    def time_upper_ms(self) -> np.ndarray:
        return np.arange(
            self.time_step_ms,
            self.max_horizon_ms + self.time_step_ms,
            self.time_step_ms,
            dtype=np.int32,
        )


DEFAULT_GRID_SPEC = HazardGridSpec()


def _feature_tokens(name: str) -> frozenset[str]:
    return frozenset(token for token in name.lower().split("_") if token)


def _validate_feature_names(feature_names: Sequence[str]) -> tuple[str, ...]:
    names = tuple(str(name) for name in feature_names)
    if len(names) < len(STRUCTURAL_FEATURES):
        raise ValueError("hazard feature schema is missing structural features")
    if names[: len(STRUCTURAL_FEATURES)] != STRUCTURAL_FEATURES:
        raise ValueError(f"hazard features must begin with {STRUCTURAL_FEATURES}, got {names}")
    if len(names) != len(set(names)):
        raise ValueError("hazard feature names must be unique")
    for name in names:
        if _FEATURE_NAME_RE.fullmatch(name) is None:
            raise ValueError(f"invalid hazard feature name: {name!r}")
        forbidden = _feature_tokens(name).intersection(_FORBIDDEN_FEATURE_TOKENS)
        if forbidden:
            raise ValueError(f"forbidden non-tradable hazard feature {name!r}: {sorted(forbidden)}")
    return names


def lightgbm_monotone_constraints(feature_names: Sequence[str]) -> tuple[int, ...]:
    """Return the frozen LightGBM constraint vector for a strict schema."""

    names = _validate_feature_names(feature_names)
    decreasing = {
        RAW_DISTANCE_FEATURE,
        FAST_NORMALIZED_DISTANCE_FEATURE,
        SLOW_NORMALIZED_DISTANCE_FEATURE,
    }
    return tuple(-1 if name in decreasing else 0 for name in names)


def _origin_token(value: object) -> str:
    if isinstance(value, (np.integer, int)) and not isinstance(value, bool):
        return f"int:{int(value)}"
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized or normalized.lower() == "nan":
            raise ValueError("origin IDs must be non-empty and non-NaN")
        return f"str:{normalized}"
    raise TypeError("origin IDs must be integers or strings")


def _coprime_step(raw: int, population: int) -> int:
    if population == 1:
        return 1
    step = raw % population
    if step == 0:
        step = 1
    while math.gcd(step, population) != 1:
        step = step % population + 1
    return step


@dataclass(frozen=True)
class DistanceQuerySample:
    """Outcome-blind systematic sample of origin/distance queries."""

    side: str
    origin_count: int
    distance_population_ticks: tuple[int, ...]
    origin_index: np.ndarray
    distance_index: np.ndarray
    inclusion_probability: np.ndarray
    inverse_probability_weight: np.ndarray
    sampling_identity_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "side", _side(self.side))
        if self.origin_count <= 0:
            raise ValueError("origin_count must be positive")
        population_raw = np.asarray(self.distance_population_ticks)
        if population_raw.ndim != 1 or not np.issubdtype(population_raw.dtype, np.integer):
            raise TypeError("distance population must use a one-dimensional integer dtype")
        population = tuple(int(value) for value in population_raw)
        if not population or any(value <= 0 for value in population):
            raise ValueError("distance population must contain positive ticks")
        if list(population) != sorted(population) or len(population) != len(set(population)):
            raise ValueError("distance population must be strictly increasing")
        object.__setattr__(self, "distance_population_ticks", population)

        origin = _readonly_array(self.origin_index, dtype=np.dtype(np.int64))
        distance = _readonly_array(self.distance_index, dtype=np.dtype(np.int64))
        probability = _readonly_array(self.inclusion_probability, dtype=np.dtype(np.float64))
        inverse = _readonly_array(self.inverse_probability_weight, dtype=np.dtype(np.float64))
        lengths = {len(origin), len(distance), len(probability), len(inverse)}
        if len(lengths) != 1 or not len(origin):
            raise ValueError("query sample arrays must have one common non-zero length")
        if np.any(origin < 0) or np.any(origin >= self.origin_count):
            raise ValueError("query sample origin index is out of bounds")
        if np.any(distance < 0) or np.any(distance >= len(population)):
            raise ValueError("query sample distance index is out of bounds")
        if np.any(~np.isfinite(probability)) or np.any((probability <= 0.0) | (probability > 1.0)):
            raise ValueError("query inclusion probabilities must lie in (0, 1]")
        if not np.allclose(inverse, 1.0 / probability, rtol=0.0, atol=1e-15):
            raise ValueError("query inverse-probability weights are inconsistent")
        if len(str(self.sampling_identity_sha256)) != 64:
            raise ValueError("query sample identity must be a SHA256")
        object.__setattr__(self, "origin_index", origin)
        object.__setattr__(self, "distance_index", distance)
        object.__setattr__(self, "inclusion_probability", probability)
        object.__setattr__(self, "inverse_probability_weight", inverse)

    @property
    def query_count(self) -> int:
        return len(self.origin_index)

    @property
    def distance_ticks(self) -> np.ndarray:
        population = np.asarray(self.distance_population_ticks, dtype=np.int64)
        values = population[self.distance_index]
        values.setflags(write=False)
        return values


def sample_distance_queries(
    *,
    origin_ids: Sequence[object] | np.ndarray,
    distance_ticks: Sequence[int] | np.ndarray,
    samples_per_origin: int,
    side: str,
    seed: int,
) -> DistanceQuerySample:
    """Sample distance queries without observing any reach outcome.

    A hash-derived affine permutation is used for each origin.  The first-order
    inclusion probability is exactly ``samples_per_origin / n_distances``;
    the returned Horvitz--Thompson weight is its reciprocal.
    """

    normalized_side = _side(side)
    raw_origins = np.asarray(origin_ids, dtype=object)
    if raw_origins.ndim != 1:
        raise ValueError("origin IDs must be one-dimensional")
    tokens = tuple(_origin_token(value) for value in raw_origins)
    if not tokens or len(tokens) != len(set(tokens)):
        raise ValueError("origin IDs must be non-empty and unique")
    population_raw = np.asarray(distance_ticks)
    if population_raw.ndim != 1 or not np.issubdtype(population_raw.dtype, np.integer):
        raise TypeError("distance_ticks must use a one-dimensional integer dtype")
    population = tuple(int(value) for value in population_raw)
    if not population or any(value <= 0 for value in population):
        raise ValueError("distance_ticks must contain positive values")
    if list(population) != sorted(population) or len(population) != len(set(population)):
        raise ValueError("distance_ticks must be strictly increasing")
    if isinstance(samples_per_origin, bool) or not isinstance(
        samples_per_origin, (int, np.integer)
    ):
        raise TypeError("samples_per_origin must be an integer")
    count = int(samples_per_origin)
    if count <= 0 or count > len(population):
        raise ValueError("samples_per_origin must lie in [1, n_distances]")
    if isinstance(seed, bool) or not isinstance(seed, (int, np.integer)):
        raise TypeError("sampling seed must be an integer")

    origin_parts: list[int] = []
    distance_parts: list[int] = []
    for origin_index, token in enumerate(tokens):
        digest = hashlib.sha256(f"{int(seed)}|{normalized_side}|{token}".encode()).digest()
        offset = int.from_bytes(digest[:8], "little") % len(population)
        step = _coprime_step(int.from_bytes(digest[8:16], "little"), len(population))
        selected = sorted((offset + rank * step) % len(population) for rank in range(count))
        origin_parts.extend([origin_index] * count)
        distance_parts.extend(selected)

    probability_value = count / float(len(population))
    identity = canonical_sha256(
        {
            "method": "hash_affine_systematic_without_replacement_v1",
            "seed": int(seed),
            "side": normalized_side,
            "origin_ids_sha256": canonical_sha256(tokens),
            "distance_population_ticks": population,
            "samples_per_origin": count,
            "inclusion_probability": probability_value,
        }
    )
    shape = len(origin_parts)
    return DistanceQuerySample(
        side=normalized_side,
        origin_count=len(tokens),
        distance_population_ticks=population,
        origin_index=np.asarray(origin_parts, dtype=np.int64),
        distance_index=np.asarray(distance_parts, dtype=np.int64),
        inclusion_probability=np.full(shape, probability_value, dtype=np.float64),
        inverse_probability_weight=np.full(shape, 1.0 / probability_value, dtype=np.float64),
        sampling_identity_sha256=identity,
    )


@dataclass(frozen=True)
class HazardRiskRows:
    """Expanded at-risk intervals for one side and one query sample."""

    side: str
    feature_names: tuple[str, ...]
    matrix: np.ndarray
    labels: np.ndarray
    sample_weight: np.ndarray
    origin_index: np.ndarray
    distance_index: np.ndarray
    query_index: np.ndarray
    time_upper_ms: np.ndarray
    first_reach_upper_ms: np.ndarray
    right_censored: np.ndarray
    query_inclusion_probability: np.ndarray
    query_inverse_probability_weight: np.ndarray
    sampling_identity_sha256: str
    schema_version: str = RISK_ROW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "side", _side(self.side))
        names = _validate_feature_names(self.feature_names)
        object.__setattr__(self, "feature_names", names)
        matrix = _readonly_array(self.matrix, dtype=np.dtype(np.float32))
        labels = _readonly_array(self.labels, dtype=np.dtype(np.uint8))
        weights = _readonly_array(self.sample_weight, dtype=np.dtype(np.float64))
        origin = _readonly_array(self.origin_index, dtype=np.dtype(np.int64))
        distance = _readonly_array(self.distance_index, dtype=np.dtype(np.int64))
        query = _readonly_array(self.query_index, dtype=np.dtype(np.int64))
        upper = _readonly_array(self.time_upper_ms, dtype=np.dtype(np.int32))
        reach = _readonly_array(self.first_reach_upper_ms, dtype=np.dtype(np.int32))
        censored = _readonly_array(self.right_censored, dtype=np.dtype(np.bool_))
        probability = _readonly_array(self.query_inclusion_probability, dtype=np.dtype(np.float64))
        inverse = _readonly_array(self.query_inverse_probability_weight, dtype=np.dtype(np.float64))
        if matrix.ndim != 2 or matrix.shape[1] != len(names):
            raise ValueError("risk-row feature matrix does not match its schema")
        row_count = matrix.shape[0]
        vectors = (
            labels,
            weights,
            origin,
            distance,
            query,
            upper,
            reach,
            censored,
            probability,
            inverse,
        )
        if not row_count or any(vector.ndim != 1 or len(vector) != row_count for vector in vectors):
            raise ValueError("risk-row vectors must match the non-empty matrix")
        if not np.all(np.isfinite(matrix)):
            raise ValueError("risk-row features must be finite")
        if np.any((labels != 0) & (labels != 1)):
            raise ValueError("hazard labels must be binary")
        if np.any(~np.isfinite(weights)) or np.any(weights <= 0.0):
            raise ValueError("hazard sample weights must be finite and positive")
        if np.any(upper <= 0) or np.any(upper > ADMINISTRATIVE_CENSOR_MS):
            raise ValueError("risk-row time upper endpoints leave formal support")
        if np.any(upper % TIME_STEP_MS):
            raise ValueError("risk-row time upper endpoints must align to 100 ms")
        if np.any(censored != (reach == RIGHT_CENSORED_TIME_MS)):
            raise ValueError("right-censor flags disagree with first-reach endpoints")
        if np.any(probability <= 0.0) or np.any(probability > 1.0):
            raise ValueError("risk-row inclusion probabilities leave (0, 1]")
        if not np.allclose(inverse, 1.0 / probability, rtol=0.0, atol=1e-15):
            raise ValueError("risk-row inverse-probability weights are inconsistent")
        if len(str(self.sampling_identity_sha256)) != 64:
            raise ValueError("risk-row sampling identity must be a SHA256")
        for name, value in (
            ("matrix", matrix),
            ("labels", labels),
            ("sample_weight", weights),
            ("origin_index", origin),
            ("distance_index", distance),
            ("query_index", query),
            ("time_upper_ms", upper),
            ("first_reach_upper_ms", reach),
            ("right_censored", censored),
            ("query_inclusion_probability", probability),
            ("query_inverse_probability_weight", inverse),
        ):
            object.__setattr__(self, name, value)

    @property
    def row_count(self) -> int:
        return len(self.labels)

    @property
    def query_count(self) -> int:
        return int(np.max(self.query_index)) + 1


def _context_matrix(
    context_features: Mapping[str, np.ndarray],
    context_feature_names: Sequence[str],
    *,
    origin_count: int,
) -> tuple[tuple[str, ...], np.ndarray]:
    names = tuple(str(name) for name in context_feature_names)
    full_names = _validate_feature_names((*STRUCTURAL_FEATURES, *names))
    if tuple(context_features) != names:
        raise ValueError("context mapping keys and order must exactly match context_feature_names")
    if not names:
        return full_names, np.empty((origin_count, 0), dtype=np.float64)
    columns: list[np.ndarray] = []
    for name in names:
        values = np.asarray(context_features[name])
        if values.ndim != 1 or len(values) != origin_count:
            raise ValueError(f"context feature {name!r} must have one value per origin")
        if not np.issubdtype(values.dtype, np.number):
            raise TypeError(f"context feature {name!r} must be numeric")
        numeric = values.astype(np.float64, copy=False)
        if not np.all(np.isfinite(numeric)):
            raise ValueError(f"context feature {name!r} must be finite")
        columns.append(numeric)
    return full_names, np.column_stack(columns)


def _sigma_columns(
    context: np.ndarray,
    context_feature_names: Sequence[str],
) -> tuple[np.ndarray, np.ndarray]:
    names = tuple(context_feature_names)
    missing = [
        name
        for name in (FAST_SIGMA_FEATURE, SLOW_SIGMA_FEATURE)
        if name not in names
    ]
    if missing:
        raise ValueError(
            "hazard context is missing volatility-normalization features: "
            f"{missing}"
        )
    fast = context[:, names.index(FAST_SIGMA_FEATURE)]
    slow = context[:, names.index(SLOW_SIGMA_FEATURE)]
    if np.any(fast <= 0.0) or np.any(slow <= 0.0):
        raise ValueError("hazard volatility-normalization sigmas must be positive")
    return fast, slow


def build_hazard_risk_rows(
    *,
    first_reach_upper_ms: np.ndarray,
    queries: DistanceQuerySample,
    context_features: Mapping[str, np.ndarray],
    context_feature_names: Sequence[str],
    tick_size: float,
    origin_weight: np.ndarray | None = None,
    grid: HazardGridSpec = DEFAULT_GRID_SPEC,
) -> HazardRiskRows:
    """Expand sampled first-reach queries into discrete-time risk rows."""

    endpoints_raw = np.asarray(first_reach_upper_ms)
    if endpoints_raw.ndim != 2 or endpoints_raw.shape != (
        queries.origin_count,
        len(queries.distance_population_ticks),
    ):
        raise ValueError("first-reach endpoint matrix does not match query population")
    if not np.issubdtype(endpoints_raw.dtype, np.integer):
        raise TypeError("first-reach upper endpoints must use an integer dtype")
    endpoints = endpoints_raw.astype(np.int64, copy=False)
    selected_endpoints = endpoints[queries.origin_index, queries.distance_index]
    valid_event = (
        (selected_endpoints >= grid.time_step_ms)
        & (selected_endpoints <= grid.max_horizon_ms)
        & (selected_endpoints % grid.time_step_ms == 0)
    )
    censored = selected_endpoints == grid.right_censored_time_ms
    if np.any(~valid_event & ~censored):
        bad = np.unique(selected_endpoints[~valid_event & ~censored]).tolist()
        raise ValueError(f"invalid first-reach upper endpoints: {bad[:8]}")
    if not math.isfinite(float(tick_size)) or float(tick_size) <= 0.0:
        raise ValueError("tick_size must be finite and positive")

    feature_names, context = _context_matrix(
        context_features,
        context_feature_names,
        origin_count=queries.origin_count,
    )
    fast_sigma, slow_sigma = _sigma_columns(context, context_feature_names)
    if origin_weight is None:
        base_weight = np.ones(queries.origin_count, dtype=np.float64)
    else:
        base_weight = np.asarray(origin_weight, dtype=np.float64)
        if base_weight.ndim != 1 or len(base_weight) != queries.origin_count:
            raise ValueError("origin_weight must have one value per origin")
        if np.any(~np.isfinite(base_weight)) or np.any(base_weight <= 0.0):
            raise ValueError("origin_weight must be finite and positive")

    end_bins = np.where(
        censored,
        grid.n_time_bins,
        selected_endpoints // grid.time_step_ms,
    ).astype(np.int64)
    total_rows = int(np.sum(end_bins))
    matrix = np.empty((total_rows, len(feature_names)), dtype=np.float32)
    labels = np.zeros(total_rows, dtype=np.uint8)
    weights = np.empty(total_rows, dtype=np.float64)
    row_origin = np.empty(total_rows, dtype=np.int64)
    row_distance = np.empty(total_rows, dtype=np.int64)
    row_query = np.empty(total_rows, dtype=np.int64)
    row_upper = np.empty(total_rows, dtype=np.int32)
    row_reach = np.empty(total_rows, dtype=np.int32)
    row_censored = np.empty(total_rows, dtype=bool)
    row_probability = np.empty(total_rows, dtype=np.float64)
    row_inverse = np.empty(total_rows, dtype=np.float64)

    cursor = 0
    for query_index, bins in enumerate(end_bins):
        stop = cursor + int(bins)
        origin = int(queries.origin_index[query_index])
        distance_index = int(queries.distance_index[query_index])
        times = np.arange(1, int(bins) + 1, dtype=np.int32) * grid.time_step_ms
        matrix[cursor:stop, 0] = queries.distance_population_ticks[distance_index] * float(
            tick_size
        )
        matrix[cursor:stop, 1] = times
        sqrt_time_s = np.sqrt(times.astype(np.float64) / 1_000.0)
        matrix[cursor:stop, 2] = matrix[cursor:stop, 0] / (
            fast_sigma[origin] * sqrt_time_s
        )
        matrix[cursor:stop, 3] = matrix[cursor:stop, 0] / (
            slow_sigma[origin] * sqrt_time_s
        )
        if context.shape[1]:
            matrix[cursor:stop, len(STRUCTURAL_FEATURES) :] = context[origin]
        if not censored[query_index]:
            labels[stop - 1] = 1
        inverse = float(queries.inverse_probability_weight[query_index])
        probability = float(queries.inclusion_probability[query_index])
        weights[cursor:stop] = base_weight[origin] * inverse
        row_origin[cursor:stop] = origin
        row_distance[cursor:stop] = distance_index
        row_query[cursor:stop] = query_index
        row_upper[cursor:stop] = times
        row_reach[cursor:stop] = int(selected_endpoints[query_index])
        row_censored[cursor:stop] = bool(censored[query_index])
        row_probability[cursor:stop] = probability
        row_inverse[cursor:stop] = inverse
        cursor = stop

    return HazardRiskRows(
        side=queries.side,
        feature_names=feature_names,
        matrix=matrix,
        labels=labels,
        sample_weight=weights,
        origin_index=row_origin,
        distance_index=row_distance,
        query_index=row_query,
        time_upper_ms=row_upper,
        first_reach_upper_ms=row_reach,
        right_censored=row_censored,
        query_inclusion_probability=row_probability,
        query_inverse_probability_weight=row_inverse,
        sampling_identity_sha256=queries.sampling_identity_sha256,
    )


@dataclass(frozen=True)
class PositiveRateCalibration:
    """Positive power map on continuous hazard rates.

    ``lambda_cal = exp(log_scale) * lambda_raw ** exp(log_power)`` is strictly
    increasing in the raw rate.  It therefore cannot reverse a LightGBM
    distance ordering.
    """

    log_scale: float = 0.0
    log_power: float = 0.0
    interval_ms: int = TIME_STEP_MS

    def __post_init__(self) -> None:
        if not math.isfinite(float(self.log_scale)) or not math.isfinite(float(self.log_power)):
            raise ValueError("rate calibration parameters must be finite")
        if int(self.interval_ms) != TIME_STEP_MS:
            raise ValueError("rate calibration interval must be 100 ms")

    @property
    def scale(self) -> float:
        return math.exp(float(self.log_scale))

    @property
    def power(self) -> float:
        return math.exp(float(self.log_power))

    def apply(self, raw_hazards: np.ndarray) -> np.ndarray:
        hazards = np.asarray(raw_hazards, dtype=np.float64)
        if np.any(~np.isfinite(hazards)) or np.any((hazards < 0.0) | (hazards > 1.0)):
            raise ValueError("raw hazards must be finite probabilities")
        clipped = np.clip(
            hazards,
            _RATE_PROBABILITY_FLOOR,
            1.0 - _RATE_PROBABILITY_FLOOR,
        )
        interval_s = self.interval_ms / 1_000.0
        raw_rate = -np.log1p(-clipped) / interval_s
        log_calibrated_rate = float(self.log_scale) + self.power * np.log(raw_rate)
        minimum_rate = -math.log1p(-_RATE_PROBABILITY_FLOOR) / interval_s
        maximum_rate = -math.log(_RATE_PROBABILITY_FLOOR) / interval_s
        calibrated_rate = np.exp(
            np.clip(
                log_calibrated_rate,
                math.log(minimum_rate),
                math.log(maximum_rate),
            )
        )
        calibrated = -np.expm1(-interval_s * calibrated_rate)
        if np.any(~np.isfinite(calibrated)) or np.any((calibrated <= 0.0) | (calibrated >= 1.0)):
            raise ArithmeticError("positive rate calibration left probability support")
        return calibrated

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": "positive_hazard_rate_power_v1",
            "log_scale": float(self.log_scale),
            "log_power": float(self.log_power),
            "interval_ms": int(self.interval_ms),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> PositiveRateCalibration:
        expected = {"method", "log_scale", "log_power", "interval_ms"}
        if set(payload) != expected:
            raise ValueError("rate calibration artifact schema mismatch")
        if payload.get("method") != "positive_hazard_rate_power_v1":
            raise ValueError("unsupported hazard-rate calibration method")
        return cls(
            log_scale=float(payload["log_scale"]),
            log_power=float(payload["log_power"]),
            interval_ms=int(payload["interval_ms"]),
        )


def fit_positive_rate_calibration(
    raw_hazards: np.ndarray,
    labels: np.ndarray,
    sample_weight: np.ndarray | None = None,
    *,
    l2_penalty: float = 1e-6,
) -> PositiveRateCalibration:
    """Fit the positive rate map on a held-out chronological calibration set."""

    raw = np.asarray(raw_hazards, dtype=np.float64)
    observed = np.asarray(labels)
    if raw.ndim != 1 or observed.ndim != 1 or raw.shape != observed.shape or not len(raw):
        raise ValueError("calibration hazards and labels must be equal non-empty vectors")
    if np.any(~np.isfinite(raw)) or np.any((raw < 0.0) | (raw > 1.0)):
        raise ValueError("calibration hazards must be finite probabilities")
    if np.any((observed != 0) & (observed != 1)):
        raise ValueError("calibration labels must be binary")
    if len(np.unique(observed)) != 2:
        raise ValueError("positive rate calibration requires both hazard outcomes")
    if sample_weight is None:
        weights = np.ones(len(raw), dtype=np.float64)
    else:
        weights = np.asarray(sample_weight, dtype=np.float64)
        if weights.ndim != 1 or weights.shape != raw.shape:
            raise ValueError("calibration weights must match hazards")
        if np.any(~np.isfinite(weights)) or np.any(weights <= 0.0):
            raise ValueError("calibration weights must be finite and positive")
    if not math.isfinite(float(l2_penalty)) or float(l2_penalty) < 0.0:
        raise ValueError("l2_penalty must be finite and non-negative")
    labels_float = observed.astype(np.float64, copy=False)
    weight_sum = float(np.sum(weights))

    def objective(theta: np.ndarray) -> float:
        calibration = PositiveRateCalibration(
            log_scale=float(theta[0]),
            log_power=float(theta[1]),
        )
        prediction = calibration.apply(raw)
        logloss = (
            -np.sum(
                weights
                * (labels_float * np.log(prediction) + (1.0 - labels_float) * np.log1p(-prediction))
            )
            / weight_sum
        )
        regularization = float(l2_penalty) * float(np.dot(theta, theta))
        return float(logloss + regularization)

    result = minimize(
        objective,
        x0=np.zeros(2, dtype=np.float64),
        method="L-BFGS-B",
        bounds=((-20.0, 20.0), (-5.0, 5.0)),
        options={"maxiter": 300, "ftol": 1e-12},
    )
    if not result.success or not np.all(np.isfinite(result.x)):
        raise RuntimeError(f"positive hazard-rate calibration failed: {result.message}")
    return PositiveRateCalibration(
        log_scale=float(result.x[0]),
        log_power=float(result.x[1]),
    )


@dataclass(frozen=True)
class FirstPassageDistribution:
    """Discrete first-passage mass, survival, and cumulative incidence."""

    hazards: np.ndarray
    event_mass: np.ndarray
    survival: np.ndarray
    cdf: np.ndarray
    right_censor_mass: np.ndarray

    def assert_invariants(self, *, atol: float = 5e-13) -> None:
        arrays = (self.hazards, self.event_mass, self.survival, self.cdf)
        if any(array.shape != self.hazards.shape for array in arrays):
            raise ArithmeticError("first-passage arrays have inconsistent shapes")
        if self.right_censor_mass.shape != self.hazards.shape[:-1]:
            raise ArithmeticError("right-censor mass has the wrong shape")
        if any(np.any(~np.isfinite(array)) for array in (*arrays, self.right_censor_mass)):
            raise ArithmeticError("first-passage distribution contains non-finite values")
        if np.any((self.hazards < 0.0) | (self.hazards > 1.0)):
            raise ArithmeticError("hazard leaves probability support")
        if np.any(self.event_mass < 0.0) or np.any(self.survival < 0.0):
            raise ArithmeticError("first-passage mass must be non-negative")
        if np.any(np.diff(self.cdf, axis=-1) < -atol):
            raise ArithmeticError("first-passage CDF decreases over time")
        if not np.allclose(self.cdf, 1.0 - self.survival, rtol=0.0, atol=atol):
            raise ArithmeticError("CDF and survival are not complements")
        cumulative_mass = np.cumsum(self.event_mass, axis=-1, dtype=np.float64)
        if not np.allclose(self.cdf, cumulative_mass, rtol=0.0, atol=atol):
            raise ArithmeticError("CDF differs from accumulated first-passage mass")
        terminal_mass = np.sum(self.event_mass, axis=-1, dtype=np.float64)
        terminal_mass += self.right_censor_mass
        if not np.allclose(terminal_mass, 1.0, rtol=0.0, atol=atol):
            raise ArithmeticError("event and right-censor masses do not sum to one")

    @property
    def max_terminal_mass_error(self) -> float:
        total = np.sum(self.event_mass, axis=-1, dtype=np.float64)
        total += self.right_censor_mass
        return float(np.max(np.abs(total - 1.0)))


def hazards_to_first_passage(
    hazards: np.ndarray,
    *,
    grid: HazardGridSpec = DEFAULT_GRID_SPEC,
) -> FirstPassageDistribution:
    """Integrate arbitrary time-varying hazards into a monotone CDF."""

    values = np.asarray(hazards, dtype=np.float64)
    if values.ndim < 1 or values.shape[-1] != grid.n_time_bins:
        raise ValueError("hazards must end in the full 300-bin formal time grid")
    if np.any(~np.isfinite(values)) or np.any((values < 0.0) | (values > 1.0)):
        raise ValueError("hazards must be finite probabilities")
    event_mass = np.empty_like(values)
    survival = np.empty_like(values)
    cdf = np.empty_like(values)
    running_survival = np.ones(values.shape[:-1], dtype=np.float64)
    for index in range(grid.n_time_bins):
        mass = running_survival * values[..., index]
        running_survival = running_survival - mass
        event_mass[..., index] = mass
        survival[..., index] = running_survival
        cdf[..., index] = 1.0 - running_survival
    distribution = FirstPassageDistribution(
        hazards=values,
        event_mass=event_mass,
        survival=survival,
        cdf=cdf,
        right_censor_mass=running_survival.copy(),
    )
    distribution.assert_invariants(atol=max(5e-13, _PROBABILITY_TOLERANCE))
    return distribution


class SideHazardModel:
    """One side-specific LightGBM hazard and positive rate calibrator."""

    def __init__(
        self,
        *,
        side: str,
        booster: lgb.Booster,
        feature_names: Sequence[str],
        calibration: PositiveRateCalibration,
        grid: HazardGridSpec = DEFAULT_GRID_SPEC,
        artifact_identity_sha256: str | None = None,
    ) -> None:
        self.side = _side(side)
        self.feature_names = _validate_feature_names(feature_names)
        if tuple(booster.feature_name()) != self.feature_names:
            raise ValueError("LightGBM feature schema differs from model contract")
        self.booster = booster
        self.calibration = calibration
        self.grid = grid
        if artifact_identity_sha256 is not None and len(artifact_identity_sha256) != 64:
            raise ValueError("artifact identity must be a SHA256")
        self.artifact_identity_sha256 = artifact_identity_sha256

    @property
    def context_feature_names(self) -> tuple[str, ...]:
        return self.feature_names[len(STRUCTURAL_FEATURES) :]

    @property
    def monotone_constraints(self) -> tuple[int, ...]:
        return lightgbm_monotone_constraints(self.feature_names)

    def predict_hazards(self, matrix: np.ndarray) -> np.ndarray:
        values = np.asarray(matrix, dtype=np.float32)
        if values.ndim != 2 or values.shape[1] != len(self.feature_names):
            raise ValueError("prediction matrix differs from strict feature schema")
        if not np.all(np.isfinite(values)):
            raise ValueError("prediction matrix must be finite")
        raw = np.asarray(self.booster.predict(values), dtype=np.float64)
        if raw.shape != (len(values),):
            raise RuntimeError("LightGBM returned an unexpected hazard shape")
        return self.calibration.apply(raw)

    def predict_curve(
        self,
        *,
        context: Mapping[str, float],
        distances_usdc_per_btc: np.ndarray,
    ) -> FirstPassageDistribution:
        """Query a full time curve for each ordered distance in one context."""

        if tuple(context) != self.context_feature_names:
            raise ValueError("prediction context differs from strict feature schema")
        context_values = np.asarray(
            [context[name] for name in self.context_feature_names], dtype=np.float64
        )
        if np.any(~np.isfinite(context_values)):
            raise ValueError("prediction context must be finite")
        distances = np.asarray(distances_usdc_per_btc, dtype=np.float64)
        if distances.ndim != 1 or not len(distances):
            raise ValueError("distances must be a non-empty vector")
        if np.any(~np.isfinite(distances)) or np.any(distances <= 0.0):
            raise ValueError("distances must be finite and positive")
        if len(distances) > 1 and np.any(np.diff(distances) <= 0.0):
            raise ValueError("distances must be strictly increasing")

        times = self.grid.time_upper_ms().astype(np.float64)
        distance_column = np.repeat(distances, self.grid.n_time_bins)
        time_column = np.tile(times, len(distances))
        matrix = np.empty((len(distance_column), len(self.feature_names)), dtype=np.float32)
        matrix[:, 0] = distance_column
        matrix[:, 1] = time_column
        context_by_name = dict(zip(self.context_feature_names, context_values, strict=True))
        fast_sigma = float(context_by_name.get(FAST_SIGMA_FEATURE, 0.0))
        slow_sigma = float(context_by_name.get(SLOW_SIGMA_FEATURE, 0.0))
        if fast_sigma <= 0.0 or slow_sigma <= 0.0:
            raise ValueError("prediction context requires positive fast and slow sigma")
        sqrt_time_s = np.sqrt(time_column / 1_000.0)
        matrix[:, 2] = distance_column / (fast_sigma * sqrt_time_s)
        matrix[:, 3] = distance_column / (slow_sigma * sqrt_time_s)
        if len(context_values):
            matrix[:, len(STRUCTURAL_FEATURES) :] = np.tile(
                context_values, (len(distance_column), 1)
            )
        hazards = self.predict_hazards(matrix).reshape(len(distances), self.grid.n_time_bins)
        if len(distances) > 1 and np.any(np.diff(hazards, axis=0) > 1e-12):
            raise RuntimeError("calibrated hazard violates distance monotonicity")
        distribution = hazards_to_first_passage(hazards, grid=self.grid)
        if len(distances) > 1 and np.any(np.diff(distribution.cdf, axis=0) > 1e-11):
            raise RuntimeError("first-passage CDF violates distance monotonicity")
        return distribution


def _require_two_classes(rows: HazardRiskRows, *, label: str) -> None:
    if len(np.unique(rows.labels)) != 2:
        raise ValueError(f"{label} risk rows require both hazard outcomes")


def fit_side_hazard_model(
    train_rows: HazardRiskRows,
    calibration_rows: HazardRiskRows,
    *,
    lightgbm_parameters: Mapping[str, Any] | None = None,
    num_boost_round: int = 100,
) -> SideHazardModel:
    """Fit one side model and calibrate on separate, caller-frozen rows."""

    if train_rows.side != calibration_rows.side:
        raise ValueError("training and calibration rows must belong to one side")
    if train_rows.feature_names != calibration_rows.feature_names:
        raise ValueError("training and calibration feature schemas differ")
    _require_two_classes(train_rows, label="training")
    _require_two_classes(calibration_rows, label="calibration")
    if int(num_boost_round) <= 0:
        raise ValueError("num_boost_round must be positive")
    constraints = lightgbm_monotone_constraints(train_rows.feature_names)
    supplied = dict(lightgbm_parameters or {})
    if "objective" in supplied and supplied["objective"] != "binary":
        raise ValueError("reach hazard LightGBM objective must be binary")
    if (
        "monotone_constraints" in supplied
        and tuple(supplied["monotone_constraints"]) != constraints
    ):
        raise ValueError("caller monotone constraints differ from the frozen schema")
    parameters: dict[str, Any] = {
        "objective": "binary",
        "verbosity": -1,
        "deterministic": True,
        "force_col_wise": True,
        "seed": 0,
        **supplied,
        "monotone_constraints": list(constraints),
    }
    dataset = lgb.Dataset(
        train_rows.matrix,
        label=train_rows.labels,
        weight=train_rows.sample_weight,
        feature_name=list(train_rows.feature_names),
        free_raw_data=True,
    )
    booster = lgb.train(parameters, dataset, num_boost_round=int(num_boost_round))
    raw_calibration = np.asarray(booster.predict(calibration_rows.matrix), dtype=np.float64)
    calibration = fit_positive_rate_calibration(
        raw_calibration,
        calibration_rows.labels,
        calibration_rows.sample_weight,
    )
    return SideHazardModel(
        side=train_rows.side,
        booster=booster,
        feature_names=train_rows.feature_names,
        calibration=calibration,
    )


@dataclass(frozen=True)
class ArtifactHashes:
    """Hashes required to bind one side-specific model artifact."""

    artifact_identity_sha256: str
    metadata_sha256: str
    booster_sha256: str


def _artifact_core(model: SideHazardModel, booster_sha256: str) -> dict[str, Any]:
    feature_schema_sha256 = canonical_sha256({"ordered_feature_names": list(model.feature_names)})
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "side": model.side,
        "model_kind": "lightgbm_discrete_first_passage_hazard",
        "time_grid": {
            "time_step_ms": model.grid.time_step_ms,
            "administrative_censor_ms": model.grid.max_horizon_ms,
            "right_censored_time_ms": model.grid.right_censored_time_ms,
        },
        "distance_unit": "USDC_per_BTC",
        "feature_names": list(model.feature_names),
        "feature_schema_sha256": feature_schema_sha256,
        "monotone_constraints": list(model.monotone_constraints),
        "calibration": model.calibration.to_dict(),
        "booster_file": "model.txt",
        "booster_sha256": booster_sha256,
        "economic_outcomes_read": False,
        "queue_fill_lifecycle_inputs_read": False,
    }


def save_side_hazard_artifact(
    model: SideHazardModel,
    directory: Path,
) -> ArtifactHashes:
    """Atomically publish one strict side-specific LightGBM artifact."""

    target = Path(directory).expanduser().resolve()
    if target.exists():
        raise FileExistsError(f"hazard artifact target already exists: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=target.parent))
    try:
        booster_path = stage / "model.txt"
        model.booster.save_model(str(booster_path))
        booster_sha256 = _sha256_file(booster_path)
        core = _artifact_core(model, booster_sha256)
        canonical_metadata_sha256 = canonical_sha256(core)
        artifact_identity_sha256 = canonical_sha256(
            {
                "canonical_metadata_sha256": canonical_metadata_sha256,
                "booster_sha256": booster_sha256,
                "side": model.side,
            }
        )
        metadata = {
            **core,
            "canonical_metadata_sha256": canonical_metadata_sha256,
            "artifact_identity_sha256": artifact_identity_sha256,
        }
        metadata_path = stage / "metadata.json"
        metadata_path.write_text(
            json.dumps(metadata, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        metadata_sha256 = _sha256_file(metadata_path)
        os.replace(stage, target)
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    return ArtifactHashes(
        artifact_identity_sha256=artifact_identity_sha256,
        metadata_sha256=metadata_sha256,
        booster_sha256=booster_sha256,
    )


def load_side_hazard_artifact(
    directory: Path,
    *,
    expected_side: str,
    expected_feature_names: Sequence[str],
    expected_artifact_identity_sha256: str | None = None,
) -> SideHazardModel:
    """Load a hash-bound artifact under an exact side and feature schema."""

    target = Path(directory).expanduser().resolve()
    if not target.is_dir():
        raise FileNotFoundError(f"hazard artifact directory missing: {target}")
    observed_files = frozenset(path.name for path in target.iterdir())
    if observed_files != _ARTIFACT_FILES:
        raise ValueError(f"hazard artifact file set mismatch: observed={sorted(observed_files)}")
    metadata_path = target / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    required_keys = {
        "schema_version",
        "identity",
        "side",
        "model_kind",
        "time_grid",
        "distance_unit",
        "feature_names",
        "feature_schema_sha256",
        "monotone_constraints",
        "calibration",
        "booster_file",
        "booster_sha256",
        "economic_outcomes_read",
        "queue_fill_lifecycle_inputs_read",
        "canonical_metadata_sha256",
        "artifact_identity_sha256",
    }
    if set(metadata) != required_keys:
        raise ValueError("hazard artifact metadata schema mismatch")
    canonical = str(metadata["canonical_metadata_sha256"])
    artifact_identity = str(metadata["artifact_identity_sha256"])
    core = dict(metadata)
    core.pop("canonical_metadata_sha256")
    core.pop("artifact_identity_sha256")
    if canonical_sha256(core) != canonical:
        raise ValueError("hazard artifact canonical metadata hash mismatch")
    expected_identity = canonical_sha256(
        {
            "canonical_metadata_sha256": canonical,
            "booster_sha256": str(metadata["booster_sha256"]),
            "side": str(metadata["side"]),
        }
    )
    if artifact_identity != expected_identity:
        raise ValueError("hazard artifact identity hash mismatch")
    if expected_artifact_identity_sha256 is not None and artifact_identity != str(
        expected_artifact_identity_sha256
    ):
        raise ValueError("hazard artifact does not match the expected identity")
    if metadata["schema_version"] != ARTIFACT_SCHEMA_VERSION or metadata["identity"] != IDENTITY:
        raise ValueError("unsupported hazard artifact identity")
    if metadata["side"] != _side(expected_side):
        raise ValueError("hazard artifact side mismatch")
    names = _validate_feature_names(expected_feature_names)
    if tuple(metadata["feature_names"]) != names:
        raise ValueError("hazard artifact feature schema mismatch")
    if metadata["feature_schema_sha256"] != canonical_sha256(
        {"ordered_feature_names": list(names)}
    ):
        raise ValueError("hazard artifact feature schema hash mismatch")
    if tuple(metadata["monotone_constraints"]) != lightgbm_monotone_constraints(names):
        raise ValueError("hazard artifact monotone constraint mismatch")
    if metadata["distance_unit"] != "USDC_per_BTC":
        raise ValueError("hazard artifact distance unit mismatch")
    if metadata["time_grid"] != {
        "time_step_ms": TIME_STEP_MS,
        "administrative_censor_ms": ADMINISTRATIVE_CENSOR_MS,
        "right_censored_time_ms": RIGHT_CENSORED_TIME_MS,
    }:
        raise ValueError("hazard artifact time grid mismatch")
    if metadata["economic_outcomes_read"] is not False:
        raise ValueError("hazard artifact cannot read economic outcomes")
    if metadata["queue_fill_lifecycle_inputs_read"] is not False:
        raise ValueError("hazard artifact cannot read execution lifecycle inputs")
    if metadata["booster_file"] != "model.txt":
        raise ValueError("hazard artifact booster filename mismatch")
    booster_path = target / "model.txt"
    if _sha256_file(booster_path) != metadata["booster_sha256"]:
        raise ValueError("hazard artifact booster hash mismatch")
    calibration = PositiveRateCalibration.from_dict(metadata["calibration"])
    booster = lgb.Booster(model_file=str(booster_path))
    return SideHazardModel(
        side=str(metadata["side"]),
        booster=booster,
        feature_names=names,
        calibration=calibration,
        artifact_identity_sha256=artifact_identity,
    )
