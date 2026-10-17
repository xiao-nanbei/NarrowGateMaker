"""Full-rank source-aware linear encoder for the F05 EMA surface."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

import numpy as np


class F05EmaEncoderError(RuntimeError):
    """Raised when an EMA encoder artifact or training batch is invalid."""


@dataclass(frozen=True, slots=True)
class FullRankEmaEncoder:
    feature_names: tuple[str, ...]
    mean: np.ndarray
    scale: np.ndarray
    components: np.ndarray
    eigenvalues: np.ndarray
    training_rows: int

    def validate(self) -> None:
        width = len(self.feature_names)
        if width == 0 or len(set(self.feature_names)) != width:
            raise F05EmaEncoderError("EMA encoder feature schema is invalid")
        if self.mean.shape != (width,) or self.scale.shape != (width,):
            raise F05EmaEncoderError("EMA encoder location/scale shape drifted")
        if self.components.shape != (width, width):
            raise F05EmaEncoderError("EMA encoder must retain every component")
        if self.eigenvalues.shape != (width,):
            raise F05EmaEncoderError("EMA encoder eigenvalue shape drifted")
        if self.training_rows <= width:
            raise F05EmaEncoderError("EMA encoder training support is too small")
        if not all(
            np.isfinite(value).all()
            for value in (self.mean, self.scale, self.components, self.eigenvalues)
        ):
            raise F05EmaEncoderError("EMA encoder contains nonfinite values")
        if np.any(self.scale <= 0.0) or np.any(self.eigenvalues < -1e-10):
            raise F05EmaEncoderError("EMA encoder scale/eigenvalues are invalid")
        gram = self.components @ self.components.T
        if not np.allclose(gram, np.eye(width), rtol=0.0, atol=1e-8):
            raise F05EmaEncoderError("EMA encoder components are not orthonormal")

    def transform(self, values: np.ndarray) -> np.ndarray:
        self.validate()
        matrix = np.asarray(values, dtype=np.float64)
        if matrix.ndim != 2 or matrix.shape[1] != len(self.feature_names):
            raise F05EmaEncoderError("EMA encoder input schema drifted")
        if not np.isfinite(matrix).all():
            raise F05EmaEncoderError("EMA encoder input contains nonfinite values")
        return ((matrix - self.mean) / self.scale) @ self.components.T


def fit_full_rank_encoder(
    batches: Iterable[np.ndarray],
    *,
    feature_names: Sequence[str],
) -> FullRankEmaEncoder:
    """Fit a correlation eigensurface without selecting components."""

    names = tuple(str(name) for name in feature_names)
    width = len(names)
    count = 0
    total = np.zeros(width, dtype=np.float64)
    cross = np.zeros((width, width), dtype=np.float64)
    for raw in batches:
        matrix = np.asarray(raw, dtype=np.float64)
        if matrix.ndim != 2 or matrix.shape[1] != width or len(matrix) == 0:
            raise F05EmaEncoderError("EMA training batch schema drifted")
        if not np.isfinite(matrix).all():
            raise F05EmaEncoderError("EMA training batch contains nonfinite values")
        count += len(matrix)
        total += matrix.sum(axis=0, dtype=np.float64)
        cross += matrix.T @ matrix
    if count <= width:
        raise F05EmaEncoderError("EMA encoder lacks training support")
    mean = total / float(count)
    covariance = cross / float(count) - np.outer(mean, mean)
    covariance = 0.5 * (covariance + covariance.T)
    variance = np.maximum(np.diag(covariance), 0.0)
    scale = np.sqrt(variance)
    scale = np.where(scale > 1e-12, scale, 1.0)
    correlation = covariance / np.outer(scale, scale)
    correlation = 0.5 * (correlation + correlation.T)
    eigenvalues, eigenvectors = np.linalg.eigh(correlation)
    order = np.argsort(eigenvalues, kind="stable")[::-1]
    eigenvalues = np.maximum(eigenvalues[order], 0.0)
    components = eigenvectors[:, order].T
    # Eigenvector signs are arbitrary.  Canonicalize them for reproducible
    # artifacts by making each row's largest absolute loading positive.
    for row in components:
        pivot = int(np.argmax(np.abs(row)))
        if row[pivot] < 0.0:
            row *= -1.0
    encoder = FullRankEmaEncoder(
        feature_names=names,
        mean=mean,
        scale=scale,
        components=components,
        eigenvalues=eigenvalues,
        training_rows=int(count),
    )
    encoder.validate()
    return encoder


__all__ = [
    "F05EmaEncoderError",
    "FullRankEmaEncoder",
    "fit_full_rank_encoder",
]
