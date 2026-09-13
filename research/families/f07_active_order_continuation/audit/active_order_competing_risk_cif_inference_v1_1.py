"""Numerically robust batch inference for the frozen 100 ms CIF formula.

The historical v1 lifecycle-state implementation remains immutable. This
successor changes only floating-point complement construction so every finite,
non-negative cause-rate vector inside the formula's support is admissible.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import numpy as np

from research.families.f07_active_order_continuation.audit.active_order_competing_risk_cif import (
    CAUSES,
    GRID_INTERVAL_MS,
    GRID_INTERVAL_S,
    CIFMechanicsError,
    GridEdgeSequenceError,
    MissedGridEdgeError,
)

IDENTITY = "active_order_competing_risk_cif_inference_v1_1"
SCHEMA_VERSION = "active_order_competing_risk_cif_inference.v1_1"
_NUMERIC_TOLERANCE = 64.0 * math.ulp(1.0)


def _exact_complement(probabilities: Sequence[float]) -> float:
    total = math.fsum(probabilities)
    if total < 0.0 or total > 1.0 + _NUMERIC_TOLERANCE:
        raise ArithmeticError(f"probability total leaves unit interval: {total}")
    complement = max(0.0, 1.0 - total)
    for _ in range(3):
        mass = math.fsum((complement, *probabilities))
        if mass == 1.0:
            return complement
        complement += 1.0 - mass
    raise ArithmeticError("unable to represent exact probability complement")


def _joint_hazards(rates: np.ndarray) -> tuple[np.ndarray, float]:
    if rates.shape != (len(CAUSES),):
        raise CIFMechanicsError("each rates_per_s row must contain four causes")
    if not np.isfinite(rates).all() or np.any(rates < 0.0):
        raise CIFMechanicsError("rates_per_s must be finite and non-negative")
    total_rate = math.fsum(float(value) for value in rates)
    if not math.isfinite(total_rate):
        raise CIFMechanicsError("sum of rates_per_s must be finite")
    if total_rate == 0.0:
        return np.zeros(len(CAUSES), dtype=np.float64), 1.0

    event_probability = -math.expm1(-GRID_INTERVAL_S * total_rate)
    hazards = rates / total_rate * event_probability
    positive_indices = np.flatnonzero(rates > 0.0)
    residual_index = int(positive_indices[-1])
    hazards[residual_index] = event_probability - math.fsum(
        float(value) for index, value in enumerate(hazards) if index != residual_index
    )
    if hazards[residual_index] < 0.0 and abs(hazards[residual_index]) <= _NUMERIC_TOLERANCE:
        hazards[residual_index] = 0.0
    if not np.isfinite(hazards).all() or np.any(hazards < 0.0):
        raise ArithmeticError("joint hazard normalization produced invalid probabilities")
    return hazards, _exact_complement(tuple(float(value) for value in hazards))


def update_active_order_competing_risk_cif(
    *,
    edges: Sequence[int] | np.ndarray,
    rates_per_s: Sequence[Sequence[float]] | np.ndarray,
    initial_last_edge: int,
    initial_survival: float,
    initial_cif: Sequence[float] | np.ndarray,
) -> dict[str, Any]:
    """Apply chronological cause-rate rows and return a resumable CIF state."""

    edge_array = np.asarray(edges, dtype=np.int64)
    rate_array = np.asarray(rates_per_s, dtype=np.float64)
    cif = np.asarray(initial_cif, dtype=np.float64).copy()
    if edge_array.ndim != 1:
        raise CIFMechanicsError("edges must be a 1D array")
    if rate_array.shape != (edge_array.size, len(CAUSES)):
        raise CIFMechanicsError("rates_per_s must have shape (n_edges, 4)")
    if cif.shape != (len(CAUSES),):
        raise CIFMechanicsError("initial_cif must have shape (4,)")
    if isinstance(initial_last_edge, bool) or initial_last_edge < 0:
        raise CIFMechanicsError("initial_last_edge must be non-negative")
    survival = float(initial_survival)
    if not math.isfinite(survival) or not 0.0 <= survival <= 1.0:
        raise CIFMechanicsError("initial_survival must be finite and within [0, 1]")
    if not np.isfinite(cif).all() or np.any(cif < 0.0) or np.any(cif > 1.0):
        raise CIFMechanicsError("initial_cif must be finite and within [0, 1]")
    if math.fsum((survival, *(float(value) for value in cif))) != 1.0:
        raise CIFMechanicsError("initial survival and CIF must conserve probability mass")

    rows = edge_array.size
    hazards_out = np.empty((rows, len(CAUSES)), dtype=np.float64)
    no_event_out = np.empty(rows, dtype=np.float64)
    survival_before = np.empty(rows, dtype=np.float64)
    survival_after = np.empty(rows, dtype=np.float64)
    cif_before = np.empty((rows, len(CAUSES)), dtype=np.float64)
    cif_after = np.empty((rows, len(CAUSES)), dtype=np.float64)

    last_edge = int(initial_last_edge)
    for row, edge_value in enumerate(edge_array):
        edge = int(edge_value)
        expected = last_edge + 1
        if edge > expected:
            raise MissedGridEdgeError(f"missed 100ms grid edge: expected {expected}, got {edge}")
        if edge < expected:
            raise GridEdgeSequenceError(
                f"duplicate or non-monotone grid edge: expected {expected}, got {edge}"
            )

        hazards, no_event = _joint_hazards(rate_array[row].copy())
        survival_before[row] = survival
        cif_before[row] = cif
        hazards_out[row] = hazards
        no_event_out[row] = no_event

        next_cif = cif + survival * hazards
        next_survival = _exact_complement(tuple(float(value) for value in next_cif))
        product_survival = survival * no_event
        if not math.isclose(
            next_survival,
            product_survival,
            rel_tol=1e-14,
            abs_tol=_NUMERIC_TOLERANCE,
        ):
            raise ArithmeticError("survival update disagrees with joint no-event probability")
        if next_survival > survival or np.any(next_cif < cif):
            raise ArithmeticError("CIF update violates monotonicity")

        survival = next_survival
        cif = next_cif
        last_edge = edge
        survival_after[row] = survival
        cif_after[row] = cif

    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "grid_interval_ms": GRID_INTERVAL_MS,
        "causes": CAUSES,
        "edges": edge_array,
        "hazards": hazards_out,
        "no_event_probability": no_event_out,
        "survival_before": survival_before,
        "survival_after": survival_after,
        "cif_before": cif_before,
        "cif_after": cif_after,
        "final_last_edge": last_edge,
        "final_survival": survival,
        "final_cif": cif.copy(),
    }
