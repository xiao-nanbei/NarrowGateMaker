#!/usr/bin/env python3
"""State-dependent queue intensities and empirical microprice artifacts.

The queue model is an auditable discrete-time approximation to a
queue-reactive Hawkes process:

``lambda_k(t) = mu_k(book_state_t) + alpha_k * excitation_k(t)``.

The state-dependent baseline ``mu`` is estimated from active-order exposure.
An exponential half-life and excitation amplitude are selected by Poisson
log-likelihood on chronological interval rows.  The model is intentionally
small; it is not a claim that one exponential kernel fully represents the
exchange.

The empirical microprice artifact estimates future-mid displacement and
up/down hitting probabilities conditional on spread and top-book imbalance.
It replaces the identity assumption that weighted mid is automatically fair
value.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

QUEUE_SCHEMA_VERSION = "queue_reactive_hawkes.v2"
MICROPRICE_SCHEMA_VERSION = "empirical_microprice.v2"
MICROPRICE_LEGACY_SCHEMA_VERSION = "empirical_microprice.v1"
EVALUATOR_SCHEMA_VERSION = "queue_value_state_evaluator.v1"
MODEL_BUNDLE_SCHEMA_VERSION = "queue_value_model_bundle.v3"
CALIBRATION_SCHEMA_VERSION = "queue_value_model_calibration.v2"
EVENT_COLUMNS = {
    "adverse_market_order": "adverse_market_order_count",
    "cancel": "cancel_count",
    "refill": "refill_count",
}
NATIVE_EXCHANGE_EVENT_COLUMNS = {
    "adverse_market_order": "adverse_market_order_count",
    "cancel": "exchange_book_cancel_count",
    "refill": "exchange_book_refill_count",
}
DEFAULT_HALF_LIFE_GRID_MS = (25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0)
DEFAULT_EXCITATION_MULTIPLIERS = (0.0, 0.25, 0.5, 1.0, 2.0, 4.0)
DEFAULT_STATE_NUMERIC_FEATURES = (
    "spread_ticks",
    "book_imbalance",
    "queue_fraction_left",
)
FORBIDDEN_POLICY_FEATURE_PREFIXES = (
    "exchange_book_",
    "simulator_",
    "future_",
    "terminal_",
    "label_",
    "reward",
)
FORBIDDEN_POLICY_FEATURE_NAMES = {
    "campaign_cost",
    "fill_value",
    "queue_cost",
    "first_event",
    "event_time_ms",
    "censor_ts_ns",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _numeric(frame: pd.DataFrame, name: str) -> np.ndarray:
    if name not in frame:
        raise ValueError(f"required numeric column is missing: {name}")
    values = pd.to_numeric(frame[name], errors="coerce").to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError(f"{name} contains missing or non-finite values")
    return values


def _validate_policy_feature_names(names: Sequence[str]) -> None:
    invalid = sorted(
        {
            str(name)
            for name in names
            if str(name) in FORBIDDEN_POLICY_FEATURE_NAMES
            or str(name).startswith(FORBIDDEN_POLICY_FEATURE_PREFIXES)
        }
    )
    if invalid:
        raise ValueError(
            "queue-value policy features contain simulator-only, target, or "
            f"post-decision fields: {invalid}"
        )


def _quantile_edges(values: np.ndarray, bins: int) -> tuple[float, ...]:
    finite = np.asarray(values, dtype=float)
    finite = finite[np.isfinite(finite)]
    if finite.size == 0 or bins <= 1:
        return ()
    quantiles = np.linspace(0.0, 1.0, bins + 1)[1:-1]
    edges = np.unique(np.quantile(finite, quantiles))
    return tuple(float(value) for value in edges)


def _bin_index(value: float, edges: Sequence[float]) -> int:
    if not math.isfinite(float(value)):
        return -1
    return int(np.searchsorted(np.asarray(edges, dtype=float), value, side="right"))


def _state_key(
    values: Mapping[str, Any],
    *,
    categorical_features: Sequence[str],
    numeric_edges: Mapping[str, Sequence[float]],
) -> str:
    parts = [f"{name}={str(values.get(name, 'missing')).upper()}" for name in categorical_features]
    parts.extend(
        f"{name}=b{_bin_index(float(values.get(name, math.nan)), edges)}"
        for name, edges in numeric_edges.items()
    )
    return "|".join(parts)


def _state_keys_from_frame(
    frame: pd.DataFrame,
    *,
    categorical_features: Sequence[str],
    numeric_edges: Mapping[str, Sequence[float]],
) -> np.ndarray:
    records = frame[list(dict.fromkeys([*categorical_features, *numeric_edges]))].to_dict("records")
    return np.asarray(
        [
            _state_key(
                row,
                categorical_features=categorical_features,
                numeric_edges=numeric_edges,
            )
            for row in records
        ],
        dtype=object,
    )


def _poisson_log_likelihood(
    counts: np.ndarray,
    exposure_s: np.ndarray,
    timestamps_ns: np.ndarray,
    group_keys: np.ndarray,
    baseline_rate: np.ndarray,
    *,
    half_life_ms: float,
    excitation_rate_per_event: float,
) -> float:
    beta_per_ns = math.log(2.0) / max(half_life_ms * 1_000_000.0, 1.0)
    log_likelihood = 0.0
    excitation = 0.0
    previous_ts = 0
    previous_group: Any = None
    lengths = {
        len(counts),
        len(exposure_s),
        len(timestamps_ns),
        len(group_keys),
        len(baseline_rate),
    }
    if len(lengths) != 1:
        raise ValueError("queue likelihood arrays must have identical lengths")
    for count, exposure, timestamp, group, baseline in zip(  # noqa: B905
        counts,
        exposure_s,
        timestamps_ns,
        group_keys,
        baseline_rate,
    ):
        if group != previous_group:
            excitation = 0.0
            previous_ts = int(timestamp)
            previous_group = group
        else:
            elapsed = max(0, int(timestamp) - previous_ts)
            excitation *= math.exp(-beta_per_ns * elapsed)
            previous_ts = int(timestamp)
        intensity = max(1e-12, float(baseline) + excitation_rate_per_event * excitation)
        expected = max(1e-12, intensity * float(exposure))
        observed = max(0.0, float(count))
        log_likelihood += observed * math.log(expected) - expected - math.lgamma(observed + 1.0)
        excitation += observed
    return float(log_likelihood)


@dataclass(frozen=True)
class QueueEventModel:
    event_type: str
    global_rate_per_s: float
    half_life_ms: float
    excitation_rate_per_event: float
    log_likelihood: float
    state_rates_per_s: dict[str, float]


@dataclass(frozen=True)
class QueueReactivePrediction:
    state_key: str
    intensities_per_s: dict[str, float]
    baseline_intensities_per_s: dict[str, float]
    excitation: dict[str, float]


@dataclass(frozen=True)
class QueueReactiveHawkesArtifact:
    schema_version: str
    artifact_id: str
    input_scope: str
    categorical_features: tuple[str, ...]
    numeric_edges: dict[str, tuple[float, ...]]
    exposure_column: str
    timestamp_column: str
    group_columns: tuple[str, ...]
    event_columns: dict[str, str]
    event_models: dict[str, QueueEventModel]
    training_rows: int
    training_days: tuple[str, ...]
    observation_resolution_ms: float = 0.0

    def state_key(self, features: Mapping[str, Any]) -> str:
        return _state_key(
            features,
            categorical_features=self.categorical_features,
            numeric_edges=self.numeric_edges,
        )

    def predict(
        self,
        features: Mapping[str, Any],
        *,
        excitation: Mapping[str, float] | None = None,
    ) -> QueueReactivePrediction:
        state_key = self.state_key(features)
        excitation = dict(excitation or {})
        baseline: dict[str, float] = {}
        intensities: dict[str, float] = {}
        normalized_excitation: dict[str, float] = {}
        for event_type, model in self.event_models.items():
            base = float(
                model.state_rates_per_s.get(
                    state_key,
                    model.global_rate_per_s,
                )
            )
            z = max(0.0, float(excitation.get(event_type, 0.0) or 0.0))
            baseline[event_type] = base
            normalized_excitation[event_type] = z
            intensities[event_type] = max(
                0.0,
                base + float(model.excitation_rate_per_event) * z,
            )
        return QueueReactivePrediction(
            state_key=state_key,
            intensities_per_s=intensities,
            baseline_intensities_per_s=baseline,
            excitation=normalized_excitation,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "artifact_id": self.artifact_id,
            "input_scope": self.input_scope,
            "categorical_features": list(self.categorical_features),
            "numeric_edges": {key: list(values) for key, values in self.numeric_edges.items()},
            "exposure_column": self.exposure_column,
            "timestamp_column": self.timestamp_column,
            "group_columns": list(self.group_columns),
            "event_columns": dict(self.event_columns),
            "event_models": {key: asdict(model) for key, model in self.event_models.items()},
            "training_rows": self.training_rows,
            "training_days": list(self.training_days),
            "observation_resolution_ms": self.observation_resolution_ms,
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_payload(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> QueueReactiveHawkesArtifact:
        if payload.get("schema_version") != QUEUE_SCHEMA_VERSION:
            raise ValueError("unsupported queue-reactive artifact schema")
        return cls(
            schema_version=str(payload["schema_version"]),
            artifact_id=str(payload["artifact_id"]),
            input_scope=str(payload["input_scope"]),
            categorical_features=tuple(payload["categorical_features"]),
            numeric_edges={
                key: tuple(float(value) for value in values)
                for key, values in payload["numeric_edges"].items()
            },
            exposure_column=str(payload["exposure_column"]),
            timestamp_column=str(payload["timestamp_column"]),
            group_columns=tuple(payload["group_columns"]),
            event_columns={
                str(event): str(column)
                for event, column in payload["event_columns"].items()
            },
            event_models={
                key: QueueEventModel(
                    event_type=str(value["event_type"]),
                    global_rate_per_s=float(value["global_rate_per_s"]),
                    half_life_ms=float(value["half_life_ms"]),
                    excitation_rate_per_event=float(value["excitation_rate_per_event"]),
                    log_likelihood=float(value["log_likelihood"]),
                    state_rates_per_s={
                        str(state): float(rate)
                        for state, rate in value["state_rates_per_s"].items()
                    },
                )
                for key, value in payload["event_models"].items()
            },
            training_rows=int(payload["training_rows"]),
            training_days=tuple(str(day) for day in payload["training_days"]),
            observation_resolution_ms=float(payload.get("observation_resolution_ms", 0.0) or 0.0),
        )

    @classmethod
    def load(cls, path: Path) -> QueueReactiveHawkesArtifact:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_payload(payload)


def fit_queue_reactive_hawkes(
    frame: pd.DataFrame,
    *,
    input_scope: str = "local_only",
    categorical_features: Sequence[str] = ("side",),
    numeric_features: Sequence[str] = DEFAULT_STATE_NUMERIC_FEATURES,
    numeric_bins: int = 4,
    exposure_column: str = "interval_ms",
    timestamp_column: str = "interval_end_ts_ns",
    group_columns: Sequence[str] = ("day", "side"),
    event_columns: Mapping[str, str] = EVENT_COLUMNS,
    half_life_grid_ms: Sequence[float] = DEFAULT_HALF_LIFE_GRID_MS,
    excitation_multipliers: Sequence[float] = DEFAULT_EXCITATION_MULTIPLIERS,
    prior_exposure_s: float = 2.0,
) -> QueueReactiveHawkesArtifact:
    if frame.empty:
        raise ValueError("queue-reactive training frame is empty")
    _validate_policy_feature_names(
        tuple(categorical_features) + tuple(numeric_features)
    )
    normalized_event_columns = {
        str(event): str(column)
        for event, column in event_columns.items()
    }
    if set(normalized_event_columns) != set(EVENT_COLUMNS):
        raise ValueError(
            "queue-reactive event columns must define adverse_market_order, "
            "cancel, and refill"
        )
    required = {
        "day",
        *categorical_features,
        *numeric_features,
        exposure_column,
        timestamp_column,
        *group_columns,
        *normalized_event_columns.values(),
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"queue-reactive training columns missing: {missing}")
    working = frame.copy()
    working = working.sort_values(
        [*group_columns, timestamp_column],
        kind="stable",
    ).reset_index(drop=True)
    exposure_s = _numeric(working, exposure_column) / 1_000.0
    if (exposure_s <= 0.0).any():
        raise ValueError("queue-reactive exposure must be positive")
    timestamps_ns = _numeric(working, timestamp_column).astype(np.int64)
    observation_resolution_ms = 0.0
    if "book_state_resolution_ms" in working:
        resolution = _numeric(working, "book_state_resolution_ms")
        finite_resolution = resolution[np.isfinite(resolution) & (resolution > 0.0)]
        if finite_resolution.size:
            observation_resolution_ms = float(np.median(finite_resolution))
    effective_half_life_grid_ms = tuple(
        sorted(
            {
                max(
                    float(value),
                    float(observation_resolution_ms),
                )
                for value in half_life_grid_ms
                if float(value) > 0.0
            }
        )
    )
    if not effective_half_life_grid_ms:
        raise ValueError("half-life grid contains no positive values")
    numeric_edges = {
        name: _quantile_edges(_numeric(working, name), numeric_bins) for name in numeric_features
    }
    state_keys = _state_keys_from_frame(
        working,
        categorical_features=categorical_features,
        numeric_edges=numeric_edges,
    )
    group_keys = working[list(group_columns)].astype(str).agg("|".join, axis=1).to_numpy()
    total_exposure = float(exposure_s.sum())
    event_models: dict[str, QueueEventModel] = {}
    for event_type, count_column in normalized_event_columns.items():
        counts = _numeric(working, count_column)
        if (counts < 0.0).any():
            raise ValueError(f"{count_column} must be non-negative")
        global_rate = float(counts.sum() / max(total_exposure, 1e-12))
        state_rates: dict[str, float] = {}
        for state in sorted(set(state_keys)):
            mask = state_keys == state
            state_count = float(counts[mask].sum())
            state_exposure = float(exposure_s[mask].sum())
            state_rates[str(state)] = (state_count + global_rate * prior_exposure_s) / max(
                state_exposure + prior_exposure_s, 1e-12
            )
        baseline = np.asarray(
            [state_rates[str(state)] for state in state_keys],
            dtype=float,
        )
        best: tuple[float, float, float] | None = None
        for half_life_ms in effective_half_life_grid_ms:
            if half_life_ms <= 0.0:
                continue
            for multiplier in excitation_multipliers:
                alpha = max(0.0, global_rate * float(multiplier))
                likelihood = _poisson_log_likelihood(
                    counts,
                    exposure_s,
                    timestamps_ns,
                    group_keys,
                    baseline,
                    half_life_ms=float(half_life_ms),
                    excitation_rate_per_event=alpha,
                )
                candidate = (likelihood, float(half_life_ms), alpha)
                if best is None or candidate[0] > best[0]:
                    best = candidate
        if best is None:
            raise ValueError("half-life/excitation grids contain no valid values")
        event_models[event_type] = QueueEventModel(
            event_type=event_type,
            global_rate_per_s=global_rate,
            half_life_ms=best[1],
            excitation_rate_per_event=best[2],
            log_likelihood=best[0],
            state_rates_per_s=state_rates,
        )

    identity = {
        "scope": input_scope,
        "features": [*categorical_features, *numeric_features],
        "rows": len(working),
        "days": sorted(working["day"].astype(str).unique()),
        "events": {
            name: {
                "column": column,
                "count": float(_numeric(working, column).sum()),
            }
            for name, column in normalized_event_columns.items()
        },
        "observation_resolution_ms": observation_resolution_ms,
    }
    artifact_id = (
        "queue-reactive-"
        + hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    )
    return QueueReactiveHawkesArtifact(
        schema_version=QUEUE_SCHEMA_VERSION,
        artifact_id=artifact_id,
        input_scope=str(input_scope),
        categorical_features=tuple(categorical_features),
        numeric_edges=numeric_edges,
        exposure_column=exposure_column,
        timestamp_column=timestamp_column,
        group_columns=tuple(group_columns),
        event_columns=normalized_event_columns,
        event_models=event_models,
        training_rows=int(len(working)),
        training_days=tuple(sorted(working["day"].astype(str).unique())),
        observation_resolution_ms=float(observation_resolution_ms),
    )


class QueueReactiveRuntime:
    """Decay event excitation on the same event clock used by replay."""

    def __init__(self, artifact: QueueReactiveHawkesArtifact) -> None:
        self.artifact = artifact
        self._last_ts_ns: int | None = None
        self._excitation = {event: 0.0 for event in artifact.event_models}

    def _decay(self, timestamp_ns: int) -> None:
        if self._last_ts_ns is None:
            self._last_ts_ns = int(timestamp_ns)
            return
        elapsed_ns = max(0, int(timestamp_ns) - self._last_ts_ns)
        for event, model in self.artifact.event_models.items():
            beta = math.log(2.0) / max(model.half_life_ms * 1_000_000.0, 1.0)
            self._excitation[event] *= math.exp(-beta * elapsed_ns)
        self._last_ts_ns = int(timestamp_ns)

    def observe(
        self,
        timestamp_ns: int,
        *,
        adverse_market_order: float = 0.0,
        cancel: float = 0.0,
        refill: float = 0.0,
    ) -> None:
        self._decay(timestamp_ns)
        updates = {
            "adverse_market_order": adverse_market_order,
            "cancel": cancel,
            "refill": refill,
        }
        for event, value in updates.items():
            if event in self._excitation:
                self._excitation[event] += max(0.0, float(value))

    def predict(
        self,
        timestamp_ns: int,
        features: Mapping[str, Any],
    ) -> QueueReactivePrediction:
        self._decay(timestamp_ns)
        return self.artifact.predict(features, excitation=self._excitation)

    def excitation_at(self, timestamp_ns: int) -> dict[str, float]:
        self._decay(timestamp_ns)
        return dict(self._excitation)


@dataclass(frozen=True)
class EmpiricalMicropriceCell:
    rows: int
    expected_mid_delta_ticks: float
    p_up: float
    p_down: float
    p_flat: float


@dataclass(frozen=True)
class EmpiricalMicropricePrediction:
    state_key: str
    rows: int
    expected_mid_delta_ticks: float
    microprice: float
    p_up: float
    p_down: float
    p_flat: float


@dataclass(frozen=True)
class EmpiricalMicropriceArtifact:
    schema_version: str
    artifact_id: str
    input_scope: str
    tick_size: float
    horizon_ms: int
    imbalance_edges: tuple[float, ...]
    spread_edges: tuple[float, ...]
    min_cell_rows: int
    max_abs_ticks: float
    global_cell: EmpiricalMicropriceCell
    cells: dict[str, EmpiricalMicropriceCell]
    training_rows: int
    training_days: tuple[str, ...]
    target_type: str = "endpoint_delta"

    def state_key(
        self,
        *,
        best_bid: float,
        best_ask: float,
        bid_qty: float,
        ask_qty: float,
    ) -> str:
        spread_ticks = max(0.0, (best_ask - best_bid) / self.tick_size)
        denominator = max(0.0, bid_qty) + max(0.0, ask_qty)
        imbalance = (
            (max(0.0, bid_qty) - max(0.0, ask_qty)) / denominator if denominator > 0.0 else 0.0
        )
        return (
            f"spread=b{_bin_index(spread_ticks, self.spread_edges)}|"
            f"imbalance=b{_bin_index(imbalance, self.imbalance_edges)}"
        )

    def predict(
        self,
        *,
        best_bid: float,
        best_ask: float,
        bid_qty: float,
        ask_qty: float,
    ) -> EmpiricalMicropricePrediction:
        if best_bid <= 0.0 or best_ask <= best_bid:
            raise ValueError("empirical microprice requires a valid BBO")
        key = self.state_key(
            best_bid=best_bid,
            best_ask=best_ask,
            bid_qty=bid_qty,
            ask_qty=ask_qty,
        )
        cell = self.cells.get(key, self.global_cell)
        delta = float(
            np.clip(
                cell.expected_mid_delta_ticks,
                -self.max_abs_ticks,
                self.max_abs_ticks,
            )
        )
        mid = 0.5 * (best_bid + best_ask)
        return EmpiricalMicropricePrediction(
            state_key=key,
            rows=cell.rows,
            expected_mid_delta_ticks=delta,
            microprice=mid + delta * self.tick_size,
            p_up=cell.p_up,
            p_down=cell.p_down,
            p_flat=cell.p_flat,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "artifact_id": self.artifact_id,
            "input_scope": self.input_scope,
            "tick_size": self.tick_size,
            "horizon_ms": self.horizon_ms,
            "imbalance_edges": list(self.imbalance_edges),
            "spread_edges": list(self.spread_edges),
            "min_cell_rows": self.min_cell_rows,
            "max_abs_ticks": self.max_abs_ticks,
            "global_cell": asdict(self.global_cell),
            "cells": {key: asdict(cell) for key, cell in self.cells.items()},
            "training_rows": self.training_rows,
            "training_days": list(self.training_days),
            "target_type": self.target_type,
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_payload(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> EmpiricalMicropriceArtifact:
        if payload.get("schema_version") not in {
            MICROPRICE_SCHEMA_VERSION,
            MICROPRICE_LEGACY_SCHEMA_VERSION,
        }:
            raise ValueError("unsupported empirical microprice artifact schema")

        def cell(value: Mapping[str, Any]) -> EmpiricalMicropriceCell:
            return EmpiricalMicropriceCell(
                rows=int(value["rows"]),
                expected_mid_delta_ticks=float(value["expected_mid_delta_ticks"]),
                p_up=float(value["p_up"]),
                p_down=float(value["p_down"]),
                p_flat=float(value["p_flat"]),
            )

        return cls(
            schema_version=str(payload["schema_version"]),
            artifact_id=str(payload["artifact_id"]),
            input_scope=str(payload["input_scope"]),
            tick_size=float(payload["tick_size"]),
            horizon_ms=int(payload["horizon_ms"]),
            imbalance_edges=tuple(float(v) for v in payload["imbalance_edges"]),
            spread_edges=tuple(float(v) for v in payload["spread_edges"]),
            min_cell_rows=int(payload["min_cell_rows"]),
            max_abs_ticks=float(payload["max_abs_ticks"]),
            global_cell=cell(payload["global_cell"]),
            cells={key: cell(value) for key, value in payload["cells"].items()},
            training_rows=int(payload["training_rows"]),
            training_days=tuple(str(day) for day in payload["training_days"]),
            target_type=str(payload.get("target_type", "endpoint_delta")),
        )

    @classmethod
    def load(cls, path: Path) -> EmpiricalMicropriceArtifact:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_payload(payload)


def _microprice_cell(
    delta_ticks: np.ndarray,
    *,
    prior: EmpiricalMicropriceCell | None,
    prior_rows: float,
) -> EmpiricalMicropriceCell:
    rows = int(delta_ticks.size)
    if prior is None:
        expected = float(delta_ticks.mean()) if rows else 0.0
        up = float((delta_ticks > 0.0).mean()) if rows else 1.0 / 3.0
        down = float((delta_ticks < 0.0).mean()) if rows else 1.0 / 3.0
        flat = max(0.0, 1.0 - up - down)
        return EmpiricalMicropriceCell(rows, expected, up, down, flat)
    denominator = rows + prior_rows
    expected = (float(delta_ticks.sum()) + prior.expected_mid_delta_ticks * prior_rows) / max(
        denominator, 1e-12
    )
    up = (float((delta_ticks > 0.0).sum()) + prior.p_up * prior_rows) / max(denominator, 1e-12)
    down = (float((delta_ticks < 0.0).sum()) + prior.p_down * prior_rows) / max(denominator, 1e-12)
    flat = max(0.0, 1.0 - up - down)
    return EmpiricalMicropriceCell(rows, expected, up, down, flat)


def fit_empirical_microprice(
    frame: pd.DataFrame,
    *,
    input_scope: str = "local_only",
    tick_size: float,
    horizon_ms: int,
    numeric_bins: int = 6,
    min_cell_rows: int = 50,
    prior_rows: float = 20.0,
    max_abs_ticks: float = 2.0,
    target_type: str = "endpoint_delta",
) -> EmpiricalMicropriceArtifact:
    if frame.empty:
        raise ValueError("empirical microprice training frame is empty")
    if tick_size <= 0.0 or horizon_ms <= 0:
        raise ValueError("tick_size and horizon_ms must be positive")
    if target_type not in {"endpoint_delta", "first_mid_hit"}:
        raise ValueError(f"unsupported empirical microprice target: {target_type}")
    required = {
        "day",
        "best_bid",
        "best_ask",
        "bid_qty",
        "ask_qty",
    }
    required.add(
        "future_mid"
        if target_type == "endpoint_delta"
        else "future_mid_first_hit_direction"
    )
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"microprice training columns missing: {missing}")
    working = frame.copy()
    horizon_column = (
        "future_mid_horizon_ms"
        if target_type == "endpoint_delta"
        else "future_mid_first_hit_horizon_ms"
    )
    censor_column = (
        "future_mid_censored"
        if target_type == "endpoint_delta"
        else "future_mid_first_hit_censored"
    )
    if horizon_column in working:
        observed_horizon = _numeric(working, horizon_column)
        if not np.allclose(
            observed_horizon,
            float(horizon_ms),
            atol=0.0,
            rtol=0.0,
        ):
            raise ValueError("future_mid_horizon_ms does not match the requested horizon")
    if censor_column in working:
        censored = _numeric(working, censor_column)
        if not np.isin(censored, (0.0, 1.0)).all():
            raise ValueError(f"{censor_column} must be binary")
        working = working.loc[censored == 0.0].copy()
        if working.empty:
            raise ValueError("empirical microprice has no uncensored future-mid rows")
    best_bid = _numeric(working, "best_bid")
    best_ask = _numeric(working, "best_ask")
    bid_qty = np.maximum(0.0, _numeric(working, "bid_qty"))
    ask_qty = np.maximum(0.0, _numeric(working, "ask_qty"))
    if (best_bid <= 0.0).any() or (best_ask <= best_bid).any():
        raise ValueError("microprice training frame contains an invalid BBO")
    mid = 0.5 * (best_bid + best_ask)
    if target_type == "first_mid_hit":
        delta_ticks = _numeric(
            working,
            "future_mid_first_hit_direction",
        )
        if not np.isin(delta_ticks, (-1.0, 0.0, 1.0)).all():
            raise ValueError(
                "future_mid_first_hit_direction must be -1, 0, or 1"
            )
        max_abs_ticks = 1.0
    else:
        future_mid = _numeric(working, "future_mid")
        delta_ticks = (future_mid - mid) / tick_size
    spread_ticks = (best_ask - best_bid) / tick_size
    denominator = bid_qty + ask_qty
    imbalance = np.divide(
        bid_qty - ask_qty,
        denominator,
        out=np.zeros_like(denominator),
        where=denominator > 0.0,
    )
    spread_edges = _quantile_edges(spread_ticks, numeric_bins)
    imbalance_edges = _quantile_edges(imbalance, numeric_bins)
    if len(spread_ticks) != len(imbalance):
        raise ValueError("spread and imbalance arrays must have identical lengths")
    keys = np.asarray(
        [
            (
                f"spread=b{_bin_index(spread, spread_edges)}|"
                f"imbalance=b{_bin_index(imb, imbalance_edges)}"
            )
            for spread, imb in zip(spread_ticks, imbalance)  # noqa: B905
        ],
        dtype=object,
    )
    global_cell = _microprice_cell(delta_ticks, prior=None, prior_rows=0.0)
    cells: dict[str, EmpiricalMicropriceCell] = {}
    for key in sorted(set(keys)):
        values = delta_ticks[keys == key]
        if values.size < min_cell_rows:
            continue
        cells[str(key)] = _microprice_cell(
            values,
            prior=global_cell,
            prior_rows=prior_rows,
        )
    identity = {
        "scope": input_scope,
        "tick_size": tick_size,
        "horizon_ms": horizon_ms,
        "rows": len(working),
        "days": sorted(working["day"].astype(str).unique()),
        "edges": [spread_edges, imbalance_edges],
        "target_type": target_type,
    }
    artifact_id = (
        "empirical-microprice-"
        + hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    )
    return EmpiricalMicropriceArtifact(
        schema_version=MICROPRICE_SCHEMA_VERSION,
        artifact_id=artifact_id,
        input_scope=str(input_scope),
        tick_size=float(tick_size),
        horizon_ms=int(horizon_ms),
        imbalance_edges=imbalance_edges,
        spread_edges=spread_edges,
        min_cell_rows=int(min_cell_rows),
        max_abs_ticks=float(max_abs_ticks),
        global_cell=global_cell,
        cells=cells,
        training_rows=int(len(working)),
        training_days=tuple(sorted(working["day"].astype(str).unique())),
        target_type=str(target_type),
    )


@dataclass(frozen=True)
class QueueValueState:
    active: bool
    reason: str
    maker_expected_ticks: float
    adverse_probability: float
    favorable_probability: float
    adverse_flow_intensity: float
    cancel_intensity: float
    refill_intensity: float
    adverse_to_refill_ratio: float
    queue_state_key: str
    microprice_state_key: str


@dataclass(frozen=True)
class QueueValueStateConfig:
    entry_expected_ticks: float = -0.10
    exit_expected_ticks: float = 0.0
    entry_adverse_probability: float = 0.52
    exit_adverse_probability: float = 0.50
    entry_flow_ratio: float = 1.0
    exit_flow_ratio: float = 0.9


class QueueValueStateEvaluator:
    """Combine local queue intensity and empirical microprice with hysteresis."""

    def __init__(
        self,
        queue_artifact: QueueReactiveHawkesArtifact,
        microprice_artifact: EmpiricalMicropriceArtifact,
        *,
        config: QueueValueStateConfig | None = None,
    ) -> None:
        if queue_artifact.input_scope != microprice_artifact.input_scope:
            raise ValueError("queue and microprice artifacts use different input scopes")
        self.queue_artifact = queue_artifact
        self.microprice_artifact = microprice_artifact
        self.config = config or QueueValueStateConfig()

    @property
    def input_scope(self) -> str:
        return self.queue_artifact.input_scope

    def evaluate(
        self,
        *,
        side: str,
        features: Mapping[str, Any],
        excitation: Mapping[str, float] | None = None,
        was_active: bool = False,
    ) -> QueueValueState:
        side = str(side).upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported side: {side}")
        queue = self.queue_artifact.predict(features, excitation=excitation)
        micro = self.microprice_artifact.predict(
            best_bid=float(features["best_bid"]),
            best_ask=float(features["best_ask"]),
            bid_qty=float(features.get("bid_qty", 0.0) or 0.0),
            ask_qty=float(features.get("ask_qty", 0.0) or 0.0),
        )
        maker_expected = (
            micro.expected_mid_delta_ticks if side == "BUY" else -micro.expected_mid_delta_ticks
        )
        adverse_probability = micro.p_down if side == "BUY" else micro.p_up
        favorable_probability = micro.p_up if side == "BUY" else micro.p_down
        intensities = queue.intensities_per_s
        adverse_flow = float(intensities.get("adverse_market_order", 0.0))
        cancel = float(intensities.get("cancel", 0.0))
        refill = float(intensities.get("refill", 0.0))
        ratio = (adverse_flow + cancel) / max(refill, 1e-9)
        cfg = self.config
        if was_active:
            exited = (
                maker_expected >= cfg.exit_expected_ticks
                and adverse_probability <= cfg.exit_adverse_probability
                and ratio <= cfg.exit_flow_ratio
            )
            active = not exited
            reason = "state_exit" if exited else "hysteresis_hold"
        else:
            active = (
                maker_expected <= cfg.entry_expected_ticks
                and adverse_probability >= cfg.entry_adverse_probability
                and ratio >= cfg.entry_flow_ratio
            )
            reason = "entry_gate" if active else "entry_not_met"
        return QueueValueState(
            active=bool(active),
            reason=reason,
            maker_expected_ticks=float(maker_expected),
            adverse_probability=float(adverse_probability),
            favorable_probability=float(favorable_probability),
            adverse_flow_intensity=adverse_flow,
            cancel_intensity=cancel,
            refill_intensity=refill,
            adverse_to_refill_ratio=float(ratio),
            queue_state_key=queue.state_key,
            microprice_state_key=micro.state_key,
        )


@dataclass(frozen=True)
class QueueValueSideModel:
    queue_artifact: QueueReactiveHawkesArtifact
    microprice_artifact: EmpiricalMicropriceArtifact
    state_config: QueueValueStateConfig
    calibration: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "queue_artifact": self.queue_artifact.to_payload(),
            "microprice_artifact": self.microprice_artifact.to_payload(),
            "state_config": asdict(self.state_config),
            "calibration": self.calibration,
        }

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
    ) -> QueueValueSideModel:
        return cls(
            queue_artifact=QueueReactiveHawkesArtifact.from_payload(payload["queue_artifact"]),
            microprice_artifact=EmpiricalMicropriceArtifact.from_payload(
                payload["microprice_artifact"]
            ),
            state_config=QueueValueStateConfig(**payload["state_config"]),
            calibration=dict(payload.get("calibration") or {}),
        )


@dataclass(frozen=True)
class QueueValueModelBundle:
    schema_version: str
    bundle_id: str
    input_scope: str
    fit_days: tuple[str, ...]
    calibration_days: tuple[str, ...]
    internal_embargo_days: tuple[str, ...]
    sides: dict[str, QueueValueSideModel]
    calibration_passed: bool
    historical_visibility: str
    source_manifest_path: str = ""
    source_manifest_sha256: str = ""
    evidence_split_path: str = ""
    evidence_split_sha256: str = ""

    def side_model(self, side: str) -> QueueValueSideModel:
        normalized = str(side).upper()
        try:
            return self.sides[normalized]
        except KeyError as exc:
            raise ValueError(f"queue-value bundle has no side model for {normalized}") from exc

    def evaluator(self, side: str) -> QueueValueStateEvaluator:
        model = self.side_model(side)
        return QueueValueStateEvaluator(
            model.queue_artifact,
            model.microprice_artifact,
            config=model.state_config,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "bundle_id": self.bundle_id,
            "input_scope": self.input_scope,
            "fit_days": list(self.fit_days),
            "calibration_days": list(self.calibration_days),
            "internal_embargo_days": list(self.internal_embargo_days),
            "sides": {side: model.to_payload() for side, model in sorted(self.sides.items())},
            "calibration_passed": self.calibration_passed,
            "historical_visibility": self.historical_visibility,
            "source_manifest_path": self.source_manifest_path,
            "source_manifest_sha256": self.source_manifest_sha256,
            "evidence_split_path": self.evidence_split_path,
            "evidence_split_sha256": self.evidence_split_sha256,
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_payload(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: Path) -> QueueValueModelBundle:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if payload.get("schema_version") != MODEL_BUNDLE_SCHEMA_VERSION:
            raise ValueError("unsupported queue-value model bundle schema")
        sides = {
            str(side).upper(): QueueValueSideModel.from_payload(model)
            for side, model in payload["sides"].items()
        }
        if set(sides) != {"BUY", "SELL"}:
            raise ValueError("queue-value bundle must contain BUY and SELL")
        scopes = {side_model.queue_artifact.input_scope for side_model in sides.values()} | {
            side_model.microprice_artifact.input_scope for side_model in sides.values()
        }
        if scopes != {str(payload["input_scope"])}:
            raise ValueError("queue-value bundle input scopes do not match")
        for side_model in sides.values():
            _validate_policy_feature_names(
                (
                    *side_model.queue_artifact.categorical_features,
                    *side_model.queue_artifact.numeric_edges,
                )
            )
        return cls(
            schema_version=str(payload["schema_version"]),
            bundle_id=str(payload["bundle_id"]),
            input_scope=str(payload["input_scope"]),
            fit_days=tuple(str(day) for day in payload["fit_days"]),
            calibration_days=tuple(str(day) for day in payload["calibration_days"]),
            internal_embargo_days=tuple(str(day) for day in payload["internal_embargo_days"]),
            sides=sides,
            calibration_passed=bool(payload["calibration_passed"]),
            historical_visibility=str(payload["historical_visibility"]),
            source_manifest_path=str(payload["source_manifest_path"]),
            source_manifest_sha256=str(payload["source_manifest_sha256"]),
            evidence_split_path=str(payload["evidence_split_path"]),
            evidence_split_sha256=str(payload["evidence_split_sha256"]),
        )


def _queue_value_calibration_predictions(
    frame: pd.DataFrame,
    *,
    side: str,
    queue_artifact: QueueReactiveHawkesArtifact,
    microprice_artifact: EmpiricalMicropriceArtifact,
) -> pd.DataFrame:
    """Score held-out rows before observing each interval's realized events."""

    working = frame.copy()
    working = working[working["side"].astype(str).str.upper() == str(side).upper()].copy()
    if working.empty:
        raise ValueError(f"{side} calibration frame is empty")
    working = working.sort_values(
        ["day", queue_artifact.timestamp_column],
        kind="stable",
    ).reset_index(drop=True)
    rows: list[dict[str, Any]] = []
    for day, day_frame in working.groupby("day", sort=True):
        runtime = QueueReactiveRuntime(queue_artifact)
        for record in day_frame.to_dict("records"):
            timestamp_ns = int(record[queue_artifact.timestamp_column])
            queue = runtime.predict(timestamp_ns, record)
            micro = microprice_artifact.predict(
                best_bid=float(record["best_bid"]),
                best_ask=float(record["best_ask"]),
                bid_qty=float(record["bid_qty"]),
                ask_qty=float(record["ask_qty"]),
            )
            exposure_s = max(
                1e-9,
                float(record[queue_artifact.exposure_column]) / 1_000.0,
            )
            if microprice_artifact.target_type == "first_mid_hit":
                future_delta_ticks = float(
                    record["future_mid_first_hit_direction"]
                )
            else:
                future_delta_ticks = (
                    float(record["future_mid"])
                    - 0.5 * (
                        float(record["best_bid"])
                        + float(record["best_ask"])
                    )
                ) / microprice_artifact.tick_size
            maker_expected_ticks = (
                micro.expected_mid_delta_ticks
                if str(side).upper() == "BUY"
                else -micro.expected_mid_delta_ticks
            )
            adverse_probability = micro.p_down if str(side).upper() == "BUY" else micro.p_up
            favorable_probability = micro.p_up if str(side).upper() == "BUY" else micro.p_down
            adverse_intensity = float(queue.intensities_per_s["adverse_market_order"])
            cancel_intensity = float(queue.intensities_per_s["cancel"])
            refill_intensity = float(queue.intensities_per_s["refill"])
            row: dict[str, Any] = {
                "day": str(day),
                "side": str(side).upper(),
                "exposure_s": exposure_s,
                "queue_state_key": queue.state_key,
                "microprice_state_key": micro.state_key,
                "maker_expected_ticks": float(maker_expected_ticks),
                "empirical_adverse_probability": float(adverse_probability),
                "empirical_favorable_probability": float(favorable_probability),
                "adverse_to_refill_ratio": float(
                    (adverse_intensity + cancel_intensity) / max(refill_intensity, 1e-9)
                ),
                "future_mid_delta_ticks": float(future_delta_ticks),
                "predicted_mid_delta_ticks": float(micro.expected_mid_delta_ticks),
                "p_up": float(micro.p_up),
                "p_down": float(micro.p_down),
                "p_flat": float(micro.p_flat),
                "constant_p_up": float(
                    microprice_artifact.global_cell.p_up
                ),
                "constant_p_down": float(
                    microprice_artifact.global_cell.p_down
                ),
                "constant_p_flat": float(
                    microprice_artifact.global_cell.p_flat
                ),
                "constant_mid_delta_ticks": float(
                    microprice_artifact.global_cell.expected_mid_delta_ticks
                ),
                "microprice_target_type": (
                    microprice_artifact.target_type
                ),
            }
            for event_type, count_column in (
                queue_artifact.event_columns.items()
            ):
                observed = max(0.0, float(record[count_column]))
                expected = float(queue.intensities_per_s[event_type]) * exposure_s
                row[f"observed_{event_type}"] = observed
                row[f"expected_{event_type}"] = expected
                row[f"probability_{event_type}"] = float(1.0 - math.exp(-max(0.0, expected)))
            rows.append(row)
            runtime.observe(
                timestamp_ns,
                adverse_market_order=float(
                    record[
                        queue_artifact.event_columns[
                            "adverse_market_order"
                        ]
                    ]
                ),
                cancel=float(
                    record[queue_artifact.event_columns["cancel"]]
                ),
                refill=float(
                    record[queue_artifact.event_columns["refill"]]
                ),
            )
    return pd.DataFrame(rows)


def _brier_score(observed: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.mean(np.square(observed - predicted)))


def _calibration_summary(
    predictions: pd.DataFrame,
    *,
    side: str,
) -> dict[str, Any]:
    hazard: dict[str, Any] = {}
    hazard_pass = True
    for event_type in EVENT_COLUMNS:
        observed = pd.to_numeric(predictions[f"observed_{event_type}"], errors="coerce").to_numpy(
            dtype=float
        )
        expected = pd.to_numeric(predictions[f"expected_{event_type}"], errors="coerce").to_numpy(
            dtype=float
        )
        probability = pd.to_numeric(
            predictions[f"probability_{event_type}"], errors="coerce"
        ).to_numpy(dtype=float)
        occurred = (observed > 0.0).astype(float)
        base_probability = np.full_like(
            occurred,
            float(occurred.mean()),
        )
        observed_total = float(observed.sum())
        expected_total = float(expected.sum())
        ratio = observed_total / expected_total if expected_total > 0.0 else math.inf
        model_brier = _brier_score(occurred, probability)
        baseline_brier = _brier_score(occurred, base_probability)
        event_pass = bool(
            observed_total >= 20.0
            and math.isfinite(ratio)
            and 0.25 <= ratio <= 4.0
            and model_brier <= baseline_brier + 0.02
        )
        hazard_pass = hazard_pass and event_pass
        hazard[event_type] = {
            "observed_total": observed_total,
            "expected_total": expected_total,
            "observed_to_expected": float(ratio),
            "occurrence_brier": model_brier,
            "constant_rate_brier": baseline_brier,
            "passed": event_pass,
        }

    delta = pd.to_numeric(predictions["future_mid_delta_ticks"], errors="coerce").to_numpy(
        dtype=float
    )
    predicted_delta = pd.to_numeric(
        predictions["predicted_mid_delta_ticks"], errors="coerce"
    ).to_numpy(dtype=float)
    observed_class = np.column_stack(
        ((delta > 0.0).astype(float), (delta < 0.0).astype(float), (delta == 0.0).astype(float))
    )
    predicted_class = predictions[["p_up", "p_down", "p_flat"]].to_numpy(dtype=float)
    observed_class_rate = observed_class.mean(axis=0)
    constant_columns = [
        "constant_p_up",
        "constant_p_down",
        "constant_p_flat",
    ]
    constant_rows = predictions[constant_columns].to_numpy(dtype=float)
    if not np.allclose(
        constant_rows,
        constant_rows[[0]],
        atol=1e-12,
        rtol=0.0,
    ):
        raise ValueError("calibration constant probabilities are not frozen")
    baseline_class = constant_rows
    multiclass_brier = float(np.mean(np.square(observed_class - predicted_class).sum(axis=1)))
    baseline_brier = float(np.mean(np.square(observed_class - baseline_class).sum(axis=1)))
    delta_mae = float(np.mean(np.abs(delta - predicted_delta)))
    baseline_delta_values = pd.to_numeric(
        predictions["constant_mid_delta_ticks"],
        errors="coerce",
    ).to_numpy(dtype=float)
    if not np.allclose(
        baseline_delta_values,
        baseline_delta_values[[0]],
        atol=1e-12,
        rtol=0.0,
    ):
        raise ValueError("calibration constant mid delta is not frozen")
    baseline_delta = float(baseline_delta_values[0])
    baseline_delta_mae = float(np.mean(np.abs(delta - baseline_delta)))
    target_types = set(
        predictions["microprice_target_type"].astype(str)
    )
    if len(target_types) != 1:
        raise ValueError("calibration mixes empirical microprice targets")
    target_type = next(iter(target_types))
    hit_mask = delta != 0.0
    hit_direction_rows = int(hit_mask.sum())
    hit_direction_brier = math.nan
    constant_hit_direction_brier = math.nan
    if hit_direction_rows:
        observed_up = (delta[hit_mask] > 0.0).astype(float)
        predicted_hit = predicted_class[hit_mask, :2].sum(axis=1)
        predicted_up = np.divide(
            predicted_class[hit_mask, 0],
            predicted_hit,
            out=np.full(hit_direction_rows, 0.5, dtype=float),
            where=predicted_hit > 1e-12,
        )
        constant_hit_probability = float(
            baseline_class[0, 0]
            / max(
                baseline_class[0, 0] + baseline_class[0, 1],
                1e-12,
            )
        )
        constant_up = np.full_like(observed_up, constant_hit_probability)
        hit_direction_brier = _brier_score(
            observed_up,
            predicted_up,
        )
        constant_hit_direction_brier = _brier_score(
            observed_up,
            constant_up,
        )
    if target_type == "first_mid_hit":
        microprice_pass = bool(
            len(predictions) >= 500
            and hit_direction_rows >= 200
            and multiclass_brier < baseline_brier
            and hit_direction_brier < constant_hit_direction_brier
        )
    else:
        microprice_pass = bool(
            len(predictions) >= 500
            and multiclass_brier <= baseline_brier + 0.02
            and delta_mae <= baseline_delta_mae + 0.10
        )
    return {
        "schema_version": CALIBRATION_SCHEMA_VERSION,
        "side": str(side).upper(),
        "days": sorted(predictions["day"].astype(str).unique()),
        "rows": int(len(predictions)),
        "hazard": hazard,
        "hazard_passed": bool(hazard_pass),
        "microprice": {
            "target_type": target_type,
            "multiclass_brier": multiclass_brier,
            "constant_class_brier": baseline_brier,
            "observed_class_rate": {
                "up": float(observed_class_rate[0]),
                "down": float(observed_class_rate[1]),
                "flat": float(observed_class_rate[2]),
            },
            "frozen_constant_class_rate": {
                "up": float(baseline_class[0, 0]),
                "down": float(baseline_class[0, 1]),
                "flat": float(baseline_class[0, 2]),
            },
            "delta_mae_ticks": delta_mae,
            "constant_delta_mae_ticks": baseline_delta_mae,
            "mean_future_delta_ticks": baseline_delta,
            "hit_direction_rows": hit_direction_rows,
            "hit_direction_brier": hit_direction_brier,
            "constant_hit_direction_brier": (
                constant_hit_direction_brier
            ),
            "passed": microprice_pass,
        },
        "calibration_passed": bool(hazard_pass and microprice_pass),
    }


def _freeze_state_config(
    predictions: pd.DataFrame,
    *,
    minimum_entry_rows: int = 100,
    minimum_entry_rate: float = 0.05,
    maximum_entry_rate: float = 0.25,
) -> tuple[QueueValueStateConfig, dict[str, Any]]:
    expected = pd.to_numeric(predictions["maker_expected_ticks"], errors="coerce").to_numpy(
        dtype=float
    )
    adverse = pd.to_numeric(predictions["empirical_adverse_probability"], errors="coerce").to_numpy(
        dtype=float
    )
    ratio = pd.to_numeric(predictions["adverse_to_refill_ratio"], errors="coerce").to_numpy(
        dtype=float
    )
    finite = np.isfinite(expected) & np.isfinite(adverse) & np.isfinite(ratio)
    expected = expected[finite]
    adverse = adverse[finite]
    ratio = ratio[finite]
    if expected.size < minimum_entry_rows:
        raise ValueError("not enough calibration rows to freeze K1 support")

    candidates: list[dict[str, Any]] = []
    for tail_quantile in (0.80, 0.75, 0.70, 0.65, 0.60, 0.55, 0.50):
        entry_expected = float(np.quantile(expected, 1.0 - tail_quantile))
        entry_adverse = float(np.quantile(adverse, tail_quantile))
        entry_ratio = float(np.quantile(ratio, tail_quantile))
        active = (expected <= entry_expected) & (adverse >= entry_adverse) & (ratio >= entry_ratio)
        rows = int(active.sum())
        rate = float(active.mean())
        candidates.append(
            {
                "tail_quantile": tail_quantile,
                "entry_expected_ticks": entry_expected,
                "entry_adverse_probability": entry_adverse,
                "entry_flow_ratio": entry_ratio,
                "entry_rows": rows,
                "entry_rate": rate,
            }
        )
    eligible = [
        row
        for row in candidates
        if row["entry_rows"] >= minimum_entry_rows
        and minimum_entry_rate <= row["entry_rate"] <= maximum_entry_rate
    ]
    selected = (
        eligible[0]
        if eligible
        else min(
            candidates,
            key=lambda row: abs(row["entry_rate"] - minimum_entry_rate),
        )
    )
    entry_expected = float(selected["entry_expected_ticks"])
    entry_adverse = float(selected["entry_adverse_probability"])
    entry_ratio = float(selected["entry_flow_ratio"])
    exit_expected = max(
        entry_expected + 1e-9,
        float(np.quantile(expected, 0.50)),
    )
    exit_adverse = min(
        entry_adverse - 1e-9,
        float(np.quantile(adverse, 0.50)),
    )
    exit_ratio = min(
        entry_ratio - 1e-9,
        float(np.quantile(ratio, 0.50)),
    )
    config = QueueValueStateConfig(
        entry_expected_ticks=entry_expected,
        exit_expected_ticks=exit_expected,
        entry_adverse_probability=entry_adverse,
        exit_adverse_probability=exit_adverse,
        entry_flow_ratio=entry_ratio,
        exit_flow_ratio=exit_ratio,
    )
    support = {
        "selection_rule": (
            "strongest preregistered joint tail with 5%-25% calibration "
            "activation and at least 100 rows; no action outcome was read"
        ),
        "minimum_entry_rows": int(minimum_entry_rows),
        "minimum_entry_rate": float(minimum_entry_rate),
        "maximum_entry_rate": float(maximum_entry_rate),
        "candidates": candidates,
        "selected": selected,
        "support_passed": bool(selected in eligible),
    }
    return config, support


def fit_side_specific_queue_value_bundle(
    frame: pd.DataFrame,
    *,
    fit_days: Sequence[str],
    calibration_days: Sequence[str],
    internal_embargo_days: Sequence[str],
    input_scope: str,
    tick_size: float,
    horizon_ms: int,
    historical_visibility: str,
    require_native_support: bool = False,
    minimum_native_support: float = 0.98,
    source_manifest_path: str = "",
    source_manifest_sha256: str = "",
    evidence_split_path: str = "",
    evidence_split_sha256: str = "",
) -> tuple[QueueValueModelBundle, pd.DataFrame, dict[str, Any]]:
    fit_days = tuple(str(day) for day in fit_days)
    calibration_days = tuple(str(day) for day in calibration_days)
    internal_embargo_days = tuple(str(day) for day in internal_embargo_days)
    if not fit_days or not calibration_days or not internal_embargo_days:
        raise ValueError("fit, internal embargo, and calibration days are required")
    native_support: dict[str, Any] = {
        "required": bool(require_native_support),
        "minimum_ratio": float(minimum_native_support),
    }
    queue_event_columns = EVENT_COLUMNS
    if require_native_support:
        required_native_columns = {
            "simulator_queue_source",
            "exchange_book_queue_status",
            "exchange_book_queue_path_valid",
            "exchange_book_queue_ambiguous",
            "exchange_book_cancel_count",
            "exchange_book_refill_count",
        }
        missing_native = sorted(required_native_columns - set(frame.columns))
        if missing_native:
            raise ValueError(
                "native-supported queue fitting columns missing: "
                f"{missing_native}"
            )
        seed_supported = (
            frame["simulator_queue_source"].astype(str).eq(
                "native_exchange_book"
            )
            & frame["exchange_book_queue_status"].astype(str).isin(
                {"exact", "known_zero"}
            )
        )
        path_supported = (
            pd.to_numeric(
                frame["exchange_book_queue_path_valid"],
                errors="coerce",
            )
            .fillna(0)
            .astype(bool)
            & ~pd.to_numeric(
                frame["exchange_book_queue_ambiguous"],
                errors="coerce",
            )
            .fillna(0)
            .astype(bool)
        )
        outcome_supported = seed_supported & path_supported
        seed_support_ratio = (
            float(seed_supported.mean()) if len(frame) else 0.0
        )
        path_support_ratio = (
            float(outcome_supported.mean()) if len(frame) else 0.0
        )
        native_support.update(
            {
                "input_rows": int(len(frame)),
                "seed_supported_rows": int(seed_supported.sum()),
                "path_supported_rows": int(outcome_supported.sum()),
                "seed_support_ratio": seed_support_ratio,
                "path_support_ratio": path_support_ratio,
                "support_ratio": seed_support_ratio,
                "excluded_rows": int((~seed_supported).sum()),
                "post_decision_path_loss_rows": int(
                    (seed_supported & ~path_supported).sum()
                ),
                "post_decision_path_filter_applied": False,
                "passed": seed_support_ratio
                >= float(minimum_native_support),
            }
        )
        if not native_support["passed"]:
            raise ValueError(
                "native queue seed-support ratio below gate: "
                f"{seed_support_ratio:.6f} < "
                f"{float(minimum_native_support):.6f}"
            )
        # Seed support is known before treatment and defines the identifiable
        # simulator population. Later snapshot resets and same-millisecond
        # ambiguity are outcomes/censoring, not legal complete-case filters.
        frame = frame.loc[seed_supported].copy()
        queue_event_columns = NATIVE_EXCHANGE_EVENT_COLUMNS
    if not (
        max(fit_days) < min(internal_embargo_days)
        and max(internal_embargo_days) < min(calibration_days)
    ):
        raise ValueError("queue-value fit/embargo/calibration order is not chronological")
    all_days = set(frame["day"].astype(str))
    requested = set(fit_days) | set(calibration_days) | set(internal_embargo_days)
    missing = sorted(requested - all_days)
    if missing:
        raise ValueError(f"queue-value panel is missing frozen days: {missing}")

    side_models: dict[str, QueueValueSideModel] = {}
    prediction_frames: list[pd.DataFrame] = []
    calibration_report: dict[str, Any] = {
        "schema_version": CALIBRATION_SCHEMA_VERSION,
        "fit_days": list(fit_days),
        "internal_embargo_days": list(internal_embargo_days),
        "calibration_days": list(calibration_days),
        "input_scope": str(input_scope),
        "queue_event_columns": dict(queue_event_columns),
        "native_support": native_support,
        "sides": {},
    }
    for side in ("BUY", "SELL"):
        fit_frame = frame[
            frame["day"].astype(str).isin(fit_days)
            & (frame["side"].astype(str).str.upper() == side)
        ].copy()
        calibration_frame = frame[
            frame["day"].astype(str).isin(calibration_days)
            & (frame["side"].astype(str).str.upper() == side)
        ].copy()
        if fit_frame.empty or calibration_frame.empty:
            raise ValueError(f"{side} has no fit or calibration rows")
        queue = fit_queue_reactive_hawkes(
            fit_frame,
            input_scope=input_scope,
            categorical_features=(),
            group_columns=("day",),
            event_columns=queue_event_columns,
        )
        microprice = fit_empirical_microprice(
            fit_frame,
            input_scope=input_scope,
            tick_size=tick_size,
            horizon_ms=horizon_ms,
            target_type="first_mid_hit",
        )
        predictions = _queue_value_calibration_predictions(
            calibration_frame,
            side=side,
            queue_artifact=queue,
            microprice_artifact=microprice,
        )
        calibration = _calibration_summary(predictions, side=side)
        state_config, support = _freeze_state_config(predictions)
        calibration["state_config_support"] = support
        calibration["calibration_passed"] = bool(
            calibration["calibration_passed"] and support["support_passed"]
        )
        predictions["state_entry"] = (
            (predictions["maker_expected_ticks"] <= state_config.entry_expected_ticks)
            & (
                predictions["empirical_adverse_probability"]
                >= state_config.entry_adverse_probability
            )
            & (predictions["adverse_to_refill_ratio"] >= state_config.entry_flow_ratio)
        ).astype(int)
        side_models[side] = QueueValueSideModel(
            queue_artifact=queue,
            microprice_artifact=microprice,
            state_config=state_config,
            calibration=calibration,
        )
        prediction_frames.append(predictions)
        calibration_report["sides"][side] = calibration
    calibration_passed = all(
        bool(model.calibration["calibration_passed"]) for model in side_models.values()
    )
    calibration_report["calibration_passed"] = calibration_passed
    identity = {
        "input_scope": input_scope,
        "fit_days": fit_days,
        "calibration_days": calibration_days,
        "internal_embargo_days": internal_embargo_days,
        "side_artifacts": {
            side: {
                "queue": model.queue_artifact.artifact_id,
                "microprice": model.microprice_artifact.artifact_id,
                "state_config": asdict(model.state_config),
            }
            for side, model in sorted(side_models.items())
        },
        "historical_visibility": historical_visibility,
        "queue_event_columns": dict(queue_event_columns),
        "native_support": native_support,
        "source_manifest_sha256": str(source_manifest_sha256),
        "evidence_split_sha256": str(evidence_split_sha256),
    }
    bundle_id = (
        "queue-value-bundle-"
        + hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    )
    bundle = QueueValueModelBundle(
        schema_version=MODEL_BUNDLE_SCHEMA_VERSION,
        bundle_id=bundle_id,
        input_scope=str(input_scope),
        fit_days=fit_days,
        calibration_days=calibration_days,
        internal_embargo_days=internal_embargo_days,
        sides=side_models,
        calibration_passed=bool(calibration_passed),
        historical_visibility=str(historical_visibility),
        source_manifest_path=str(source_manifest_path),
        source_manifest_sha256=str(source_manifest_sha256),
        evidence_split_path=str(evidence_split_path),
        evidence_split_sha256=str(evidence_split_sha256),
    )
    return (
        bundle,
        pd.concat(prediction_frames, ignore_index=True),
        calibration_report,
    )


def _read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _load_formal_split_identity(
    *,
    source_manifest_path: Path,
    evidence_split_path: Path,
    fit_days: Sequence[str],
    internal_embargo_days: Sequence[str],
    calibration_days: Sequence[str],
) -> dict[str, str]:
    source_path = source_manifest_path.expanduser().resolve()
    evidence_path = evidence_split_path.expanduser().resolve()
    source = json.loads(source_path.read_text(encoding="utf-8"))
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    source_hash = _sha256(source_path)
    evidence_hash = _sha256(evidence_path)
    if str(evidence.get("source_manifest_path", "")) != str(source_path):
        raise ValueError(
            "evidence split does not reference the requested source manifest"
        )
    if str(evidence.get("source_manifest_sha256", "")) != source_hash:
        raise ValueError(
            "evidence split source-manifest hash does not match"
        )
    source_split = source.get("split")
    evidence_panels = evidence.get("panels")
    if not isinstance(source_split, dict) or not isinstance(
        evidence_panels, dict
    ):
        raise ValueError("formal source/evidence manifests have no split data")

    expected_roles = {
        "state_fit": tuple(str(day) for day in fit_days),
        "state_internal_embargo": tuple(
            str(day) for day in internal_embargo_days
        ),
        "state_calibration": tuple(str(day) for day in calibration_days),
    }
    for role, expected in expected_roles.items():
        actual = tuple(str(day) for day in source_split.get(role) or ())
        if actual != expected:
            raise ValueError(
                f"queue-value {role} differs from the frozen source manifest"
            )
    evidence_roles = {
        "development": "train",
        "validation": "validation",
        "sealed_holdout": "test",
    }
    for panel, source_role in evidence_roles.items():
        panel_payload = evidence_panels.get(panel)
        if not isinstance(panel_payload, dict):
            raise ValueError(f"evidence split is missing {panel}")
        panel_days = tuple(
            str(day) for day in panel_payload.get("days") or ()
        )
        source_days = tuple(
            str(day) for day in source_split.get(source_role) or ()
        )
        if panel_days != source_days:
            raise ValueError(
                f"evidence {panel} differs from source role {source_role}"
            )
    transition = tuple(
        str(day)
        for day in source_split.get("state_transition_embargo") or ()
    )
    development = tuple(
        str(day)
        for day in evidence_panels["development"].get("days") or ()
    )
    if (
        not transition
        or not development
        or max(calibration_days) >= min(transition)
        or max(transition) >= min(development)
    ):
        raise ValueError(
            "state calibration, transition embargo, and development are not "
            "strictly chronological"
        )
    return {
        "source_manifest_path": str(source_path),
        "source_manifest_sha256": source_hash,
        "evidence_split_path": str(evidence_path),
        "evidence_split_sha256": evidence_hash,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel", type=Path, required=True)
    parser.add_argument("--queue-output", type=Path, default=None)
    parser.add_argument("--microprice-output", type=Path, default=None)
    parser.add_argument("--bundle-output", type=Path, default=None)
    parser.add_argument("--source-manifest", type=Path, default=None)
    parser.add_argument(
        "--evidence-split-manifest",
        type=Path,
        default=None,
    )
    parser.add_argument("--calibration-report-output", type=Path, default=None)
    parser.add_argument(
        "--calibration-predictions-output",
        type=Path,
        default=None,
    )
    parser.add_argument("--fit-days", nargs="+", default=[])
    parser.add_argument("--calibration-days", nargs="+", default=[])
    parser.add_argument("--internal-embargo-days", nargs="+", default=[])
    parser.add_argument(
        "--historical-visibility",
        default="exchange_time_asof_le_ideal_latency_diagnostic",
    )
    parser.add_argument(
        "--input-scope", choices=("local_only", "local_plus_external"), default="local_only"
    )
    parser.add_argument("--tick-size", type=float, required=True)
    parser.add_argument("--microprice-horizon-ms", type=int, required=True)
    parser.add_argument(
        "--microprice-target",
        choices=("endpoint_delta", "first_mid_hit"),
        default="endpoint_delta",
    )
    parser.add_argument(
        "--require-native-support",
        action="store_true",
        help=(
            "Fit the policy-visible state model on exact/known-zero native "
            "seed rows and use native exact-level cancel/refill events as "
            "targets. Post-decision path loss is reported, not filtered."
        ),
    )
    parser.add_argument(
        "--minimum-native-support",
        type=float,
        default=0.98,
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if not 0.0 < float(args.minimum_native_support) <= 1.0:
        raise SystemExit("--minimum-native-support must be in (0, 1]")
    panel_path = args.panel.expanduser().resolve()
    panel = _read_frame(panel_path)
    if args.bundle_output is not None:
        if args.queue_output is not None or args.microprice_output is not None:
            raise SystemExit("--bundle-output cannot be combined with the pooled outputs")
        if not args.fit_days or not args.calibration_days or not args.internal_embargo_days:
            raise SystemExit(
                "side-specific bundle fitting requires --fit-days, "
                "--internal-embargo-days, and --calibration-days"
            )
        if (
            args.source_manifest is None
            or args.evidence_split_manifest is None
        ):
            raise SystemExit(
                "formal side-specific bundle fitting requires "
                "--source-manifest and --evidence-split-manifest"
            )
        try:
            formal_identity = _load_formal_split_identity(
                source_manifest_path=args.source_manifest,
                evidence_split_path=args.evidence_split_manifest,
                fit_days=args.fit_days,
                internal_embargo_days=args.internal_embargo_days,
                calibration_days=args.calibration_days,
            )
        except (OSError, ValueError) as exc:
            raise SystemExit(str(exc)) from exc
        bundle, predictions, report = fit_side_specific_queue_value_bundle(
            panel,
            fit_days=args.fit_days,
            calibration_days=args.calibration_days,
            internal_embargo_days=args.internal_embargo_days,
            input_scope=args.input_scope,
            tick_size=args.tick_size,
            horizon_ms=args.microprice_horizon_ms,
            historical_visibility=args.historical_visibility,
            require_native_support=bool(args.require_native_support),
            minimum_native_support=float(args.minimum_native_support),
            **formal_identity,
        )
        bundle_path = args.bundle_output.expanduser().resolve()
        report_path = (
            args.calibration_report_output.expanduser().resolve()
            if args.calibration_report_output is not None
            else bundle_path.with_suffix(".calibration.json")
        )
        predictions_path = (
            args.calibration_predictions_output.expanduser().resolve()
            if args.calibration_predictions_output is not None
            else bundle_path.with_suffix(".calibration.parquet")
        )
        bundle.save(bundle_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        predictions_path.parent.mkdir(parents=True, exist_ok=True)
        predictions.to_parquet(predictions_path, index=False)
        print(
            json.dumps(
                {
                    "bundle": str(bundle_path),
                    "bundle_sha256": _sha256(bundle_path),
                    "bundle_id": bundle.bundle_id,
                    "calibration_passed": bundle.calibration_passed,
                    "calibration_report": str(report_path),
                    "calibration_predictions": str(predictions_path),
                    "input_scope": args.input_scope,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    if args.queue_output is None or args.microprice_output is None:
        raise SystemExit("pooled fitting requires --queue-output and --microprice-output")
    queue = fit_queue_reactive_hawkes(panel, input_scope=args.input_scope)
    microprice = fit_empirical_microprice(
        panel,
        input_scope=args.input_scope,
        tick_size=args.tick_size,
        horizon_ms=args.microprice_horizon_ms,
        target_type=args.microprice_target,
    )
    queue_path = args.queue_output.expanduser().resolve()
    microprice_path = args.microprice_output.expanduser().resolve()
    queue.save(queue_path)
    microprice.save(microprice_path)
    print(
        json.dumps(
            {
                "queue_artifact": str(queue_path),
                "queue_sha256": _sha256(queue_path),
                "queue_id": queue.artifact_id,
                "microprice_artifact": str(microprice_path),
                "microprice_sha256": _sha256(microprice_path),
                "microprice_id": microprice.artifact_id,
                "input_scope": args.input_scope,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
