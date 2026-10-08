#!/usr/bin/env python3
"""Research-only exact-distance runtime queries for conditional P3 v4.1.

The adapter deliberately stops at prediction evidence.  It binds each day to
one chronological OOF fold, evaluates BUY and SELL independently at executable
integer-tick prices, and returns a baseline fallback for every unsupported
query.  It does not expose quote, action, or live authority.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from numbers import Integral
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np

from research.families.f02_empirical_p3_touch.audit.p3_touch_volatility_conditioned import (
    ConditionalTouchModel,
)
from research.governance.paths import resolve_research_path

IDENTITY = "p3_touch_exact_distance_surface.v1"
V4_IDENTITY = "p3_touch_volatility_conditioned_v4"
V4_1_IDENTITY = "p3_touch_volatility_conditioned_v4_1"
SIDES = ("BUY", "SELL")
DISTANCE_MIN = Decimal("0.5")
DISTANCE_MAX = Decimal("120")
REQUIRED_CONTEXT_FIELDS = (
    "start_ts_ms",
    "feature_ready_ts_ms",
    "best_bid",
    "best_ask",
    "mid",
    "spread",
    "fast_sigma",
    "slow_sigma",
)
PERMISSIONS = {
    "prediction_research_only": True,
    "action_authority": False,
    "live_authority": False,
}
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class ArtifactIntegrityError(ValueError):
    """Raised before runtime admission when a frozen artifact is inconsistent."""


@dataclass(frozen=True)
class _VerifiedArtifact:
    path: Path
    sha256: str


@dataclass(frozen=True)
class _FoldRuntime:
    fold_id: str
    model: ConditionalTouchModel
    model_artifact: _VerifiedArtifact
    calibration_artifact: _VerifiedArtifact


@dataclass(frozen=True)
class _DayRuntime:
    day: str
    source: str
    fold_id: str
    context_artifact: _VerifiedArtifact
    context: Mapping[str, np.ndarray]
    row_by_start_ts_ms: Mapping[int, int]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: Mapping[str, Any], *, omit: str) -> str:
    normalized = dict(payload)
    normalized.pop(omit, None)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _verify_artifact(
    identity: Mapping[str, Any],
    *,
    label: str,
) -> _VerifiedArtifact:
    if not isinstance(identity, Mapping):
        raise ArtifactIntegrityError(f"{label} identity must be a mapping")
    try:
        path = resolve_research_path(str(identity["path"]))
        expected = str(identity["sha256"]).lower()
    except KeyError as exc:
        raise ArtifactIntegrityError(f"{label} identity is incomplete") from exc
    if _SHA256_PATTERN.fullmatch(expected) is None:
        raise ArtifactIntegrityError(f"{label} has an invalid SHA256")
    if not path.is_file():
        raise ArtifactIntegrityError(f"{label} is missing: {path}")
    observed = _sha256_file(path)
    if observed != expected:
        raise ArtifactIntegrityError(
            f"{label} hash mismatch: observed={observed} expected={expected}"
        )
    return _VerifiedArtifact(path=path, sha256=observed)


def _load_json(artifact: _VerifiedArtifact, *, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(artifact.path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ArtifactIntegrityError(f"{label} is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ArtifactIntegrityError(f"{label} JSON root must be an object")
    return payload


def _require_no_authority(payload: Mapping[str, Any], *, label: str) -> None:
    permissions = payload.get("permissions")
    if not isinstance(permissions, Mapping):
        raise ArtifactIntegrityError(f"{label} lacks permissions")
    for field in ("action_authority", "live_authority"):
        if bool(permissions.get(field, False)):
            raise ArtifactIntegrityError(f"{label} unexpectedly grants {field}")


def _as_positive_decimal(value: Any, *, label: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ArtifactIntegrityError(f"{label} is not decimal") from exc
    if not result.is_finite() or result <= 0:
        raise ArtifactIntegrityError(f"{label} must be finite and positive")
    return result


def _price_to_tick(price: Any, tick_size: Decimal) -> int | None:
    """Return a price's exact tick identity, allowing only float ULP noise."""

    try:
        numeric = float(price)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric) or numeric <= 0.0:
        return None
    ratio = numeric / float(tick_size)
    nearest = round(ratio)
    if abs(ratio - nearest) > 1e-9:
        return None
    return int(nearest)


def _require_tick_integer(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, Integral):
        return None
    return int(value)


def _load_context(
    artifact: _VerifiedArtifact,
    *,
    day: str,
) -> tuple[dict[str, np.ndarray], dict[int, int]]:
    try:
        with np.load(artifact.path, allow_pickle=False) as loaded:
            missing = sorted(set(REQUIRED_CONTEXT_FIELDS).difference(loaded.files))
            if missing:
                raise ArtifactIntegrityError(
                    f"context for {day} lacks fields: {missing}"
                )
            context = {
                field: np.asarray(loaded[field]).copy()
                for field in REQUIRED_CONTEXT_FIELDS
            }
    except ArtifactIntegrityError:
        raise
    except (OSError, ValueError) as exc:
        raise ArtifactIntegrityError(f"context for {day} is not a valid NPZ") from exc

    starts = np.asarray(context["start_ts_ms"], dtype=np.int64)
    if starts.ndim != 1 or starts.size == 0:
        raise ArtifactIntegrityError(f"context for {day} has no one-dimensional rows")
    for field, values in context.items():
        if np.asarray(values).ndim != 1 or len(values) != len(starts):
            raise ArtifactIntegrityError(
                f"context for {day} has inconsistent field shape: {field}"
            )
    if len(np.unique(starts)) != len(starts):
        raise ArtifactIntegrityError(f"context for {day} has duplicate start_ts_ms")
    return context, {int(timestamp): index for index, timestamp in enumerate(starts)}


def _utc_day(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(
        timestamp_ms / 1_000.0,
        tz=timezone.utc,
    ).date().isoformat()


class P3TouchExactDistanceSurface:
    """Hash-bound, chronological-OOF exact-distance P3 query surface."""

    def __init__(
        self,
        *,
        v4_1_spec: Mapping[str, Any],
        day_bindings: Mapping[str, Mapping[str, Any]],
        tick_size: float | str | Decimal,
    ) -> None:
        self._tick_size = _as_positive_decimal(tick_size, label="tick_size")
        self._v4_1_artifact = _verify_artifact(
            v4_1_spec,
            label="conditional P3 v4.1 spec",
        )
        v4_1 = _load_json(self._v4_1_artifact, label="conditional P3 v4.1 spec")
        if v4_1.get("identity") != V4_1_IDENTITY:
            raise ArtifactIntegrityError("unexpected conditional P3 v4.1 identity")
        if v4_1.get("predecessor_identity") != V4_IDENTITY:
            raise ArtifactIntegrityError("v4.1 predecessor identity is not v4")
        canonical = v4_1.get("canonical_spec_identity_sha256")
        if canonical != _canonical_sha256(
            v4_1,
            omit="canonical_spec_identity_sha256",
        ):
            raise ArtifactIntegrityError("conditional P3 v4.1 canonical hash mismatch")
        _require_no_authority(v4_1, label="conditional P3 v4.1 spec")

        identities = v4_1.get("identities")
        if not isinstance(identities, Mapping):
            raise ArtifactIntegrityError("conditional P3 v4.1 identities are missing")
        self._v4_spec_artifact = _verify_artifact(
            identities.get("original_v4_spec", {}),
            label="conditional P3 predecessor v4 spec",
        )
        self._v4_report_artifact = _verify_artifact(
            identities.get("original_v4_report", {}),
            label="conditional P3 predecessor v4 report",
        )
        v4_spec = _load_json(
            self._v4_spec_artifact,
            label="conditional P3 predecessor v4 spec",
        )
        v4_report = _load_json(
            self._v4_report_artifact,
            label="conditional P3 predecessor v4 report",
        )
        self._validate_predecessor(v4_spec=v4_spec, v4_report=v4_report)

        folds, day_to_fold = self._load_folds(
            v4_spec=v4_spec,
            v4_report=v4_report,
        )
        self._folds = folds
        self._days = self._load_days(
            day_bindings=day_bindings,
            day_to_fold=day_to_fold,
        )

    def _validate_predecessor(
        self,
        *,
        v4_spec: Mapping[str, Any],
        v4_report: Mapping[str, Any],
    ) -> None:
        if v4_spec.get("identity") != V4_IDENTITY:
            raise ArtifactIntegrityError("unexpected predecessor v4 spec identity")
        if v4_report.get("identity") != V4_IDENTITY:
            raise ArtifactIntegrityError("unexpected predecessor v4 report identity")
        canonical = v4_spec.get("canonical_spec_identity_sha256")
        if canonical != _canonical_sha256(
            v4_spec,
            omit="canonical_spec_identity_sha256",
        ):
            raise ArtifactIntegrityError("conditional P3 v4 canonical hash mismatch")
        _require_no_authority(v4_spec, label="conditional P3 v4 spec")
        _require_no_authority(v4_report, label="conditional P3 v4 report")

        report_spec = v4_report.get("spec")
        if not isinstance(report_spec, Mapping):
            raise ArtifactIntegrityError("conditional P3 v4 report lacks spec identity")
        report_spec_path = resolve_research_path(str(report_spec.get("path", "")))
        if (
            report_spec_path != self._v4_spec_artifact.path
            or str(report_spec.get("sha256", "")) != self._v4_spec_artifact.sha256
        ):
            raise ArtifactIntegrityError("conditional P3 v4 report/spec identity mismatch")

        estimand = v4_spec.get("estimand")
        if not isinstance(estimand, Mapping):
            raise ArtifactIntegrityError("conditional P3 v4 estimand is missing")
        if estimand.get("event_type") != "touch":
            raise ArtifactIntegrityError("conditional P3 predecessor is not touch")
        if float(estimand.get("horizon_s", 0.0)) != 10.0:
            raise ArtifactIntegrityError("conditional P3 predecessor is not 10 seconds")
        if estimand.get("distance_unit") != "USDC_per_BTC":
            raise ArtifactIntegrityError("conditional P3 predecessor distance unit drift")

    def _load_folds(
        self,
        *,
        v4_spec: Mapping[str, Any],
        v4_report: Mapping[str, Any],
    ) -> tuple[dict[str, _FoldRuntime], dict[str, str]]:
        chronological = v4_spec.get("chronological_oof")
        fold_specs = chronological.get("folds") if isinstance(chronological, Mapping) else None
        if not isinstance(fold_specs, list) or not fold_specs:
            raise ArtifactIntegrityError("conditional P3 v4 OOF folds are missing")

        day_to_fold: dict[str, str] = {}
        ordered_fold_ids: list[str] = []
        for fold in fold_specs:
            if not isinstance(fold, Mapping):
                raise ArtifactIntegrityError("conditional P3 v4 fold is malformed")
            fold_id = str(fold.get("fold_id", ""))
            test_days = fold.get("test_days")
            if not fold_id or fold_id in ordered_fold_ids:
                raise ArtifactIntegrityError("conditional P3 v4 fold IDs are not unique")
            if not isinstance(test_days, list) or not test_days:
                raise ArtifactIntegrityError(f"{fold_id} has no OOF test days")
            ordered_fold_ids.append(fold_id)
            for raw_day in test_days:
                day = str(raw_day)
                if day in day_to_fold:
                    raise ArtifactIntegrityError(
                        f"day {day} belongs to multiple conditional P3 OOF folds"
                    )
                day_to_fold[day] = fold_id

        report_artifacts = v4_report.get("fold_artifacts")
        if not isinstance(report_artifacts, Mapping):
            raise ArtifactIntegrityError("conditional P3 v4 fold artifacts are missing")
        if set(report_artifacts) != set(ordered_fold_ids):
            raise ArtifactIntegrityError("conditional P3 v4 fold artifact set mismatch")

        feature_contract = v4_spec.get("model", {}).get("feature_contract")
        if not isinstance(feature_contract, Mapping):
            raise ArtifactIntegrityError("conditional P3 v4 feature contract is missing")
        if float(feature_contract.get("horizon_s", 0.0)) != 10.0:
            raise ArtifactIntegrityError("conditional P3 model horizon drift")

        folds: dict[str, _FoldRuntime] = {}
        for fold_id in ordered_fold_ids:
            artifact_set = report_artifacts[fold_id]
            if not isinstance(artifact_set, Mapping):
                raise ArtifactIntegrityError(f"{fold_id} artifacts are malformed")
            model_artifact = _verify_artifact(
                artifact_set.get("model", {}),
                label=f"{fold_id} model.txt",
            )
            calibration_artifact = _verify_artifact(
                artifact_set.get("calibration", {}),
                label=f"{fold_id} positive_platt",
            )
            calibration = _load_json(
                calibration_artifact,
                label=f"{fold_id} positive_platt",
            )
            try:
                intercept = float(calibration["intercept"])
                slope = float(calibration["slope"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ArtifactIntegrityError(
                    f"{fold_id} positive_platt parameters are invalid"
                ) from exc
            if not math.isfinite(intercept) or not math.isfinite(slope) or slope <= 0.0:
                raise ArtifactIntegrityError(
                    f"{fold_id} positive_platt must have a positive finite slope"
                )
            try:
                booster = lgb.Booster(model_file=str(model_artifact.path))
            except Exception as exc:
                raise ArtifactIntegrityError(
                    f"{fold_id} model.txt cannot be loaded"
                ) from exc
            folds[fold_id] = _FoldRuntime(
                fold_id=fold_id,
                model=ConditionalTouchModel(
                    booster=booster,
                    calibration=calibration,
                    feature_contract=feature_contract,
                ),
                model_artifact=model_artifact,
                calibration_artifact=calibration_artifact,
            )
        return folds, day_to_fold

    def _load_days(
        self,
        *,
        day_bindings: Mapping[str, Mapping[str, Any]],
        day_to_fold: Mapping[str, str],
    ) -> dict[str, _DayRuntime]:
        if not isinstance(day_bindings, Mapping) or not day_bindings:
            raise ArtifactIntegrityError("at least one day context binding is required")
        days: dict[str, _DayRuntime] = {}
        context_paths: set[Path] = set()
        for raw_day, binding in day_bindings.items():
            day = str(raw_day)
            if day in days or not isinstance(binding, Mapping):
                raise ArtifactIntegrityError(f"invalid duplicate day binding: {day}")
            expected_fold = day_to_fold.get(day)
            fold_id = str(binding.get("fold_id", ""))
            if expected_fold is None:
                raise ArtifactIntegrityError(f"day {day} has no predecessor OOF fold")
            if fold_id != expected_fold or fold_id not in self._folds:
                raise ArtifactIntegrityError(f"day {day} OOF fold binding mismatch")
            context_artifact = _verify_artifact(
                binding.get("context", {}),
                label=f"{day} context NPZ",
            )
            if context_artifact.path in context_paths:
                raise ArtifactIntegrityError("context NPZ cannot bind multiple days")
            context_paths.add(context_artifact.path)
            context, row_by_start = _load_context(context_artifact, day=day)
            days[day] = _DayRuntime(
                day=day,
                source=str(binding.get("source", "native")),
                fold_id=fold_id,
                context_artifact=context_artifact,
                context=context,
                row_by_start_ts_ms=row_by_start,
            )
        return days

    @property
    def bound_days(self) -> tuple[str, ...]:
        return tuple(sorted(self._days))

    @property
    def permissions(self) -> dict[str, bool]:
        return dict(PERMISSIONS)

    def _identity_payload(self, day_runtime: _DayRuntime | None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "v4_1_spec_sha256": self._v4_1_artifact.sha256,
            "predecessor_v4_spec_sha256": self._v4_spec_artifact.sha256,
            "predecessor_v4_report_sha256": self._v4_report_artifact.sha256,
        }
        if day_runtime is not None:
            fold = self._folds[day_runtime.fold_id]
            payload.update(
                {
                    "context_npz_sha256": day_runtime.context_artifact.sha256,
                    "fold_model_sha256": fold.model_artifact.sha256,
                    "fold_positive_platt_sha256": fold.calibration_artifact.sha256,
                }
            )
        return payload

    def _fallback(
        self,
        *,
        day: str,
        decision_ts_ms: Any,
        reason: str,
        day_runtime: _DayRuntime | None = None,
    ) -> dict[str, Any]:
        return {
            "identity": IDENTITY,
            "supported": False,
            "fallback_required": True,
            "fallback_reason": reason,
            "day": str(day),
            "decision_ts_ms": decision_ts_ms,
            "source": None if day_runtime is None else day_runtime.source,
            "fold_id": None if day_runtime is None else day_runtime.fold_id,
            "predictions": {},
            "artifact_hashes": self._identity_payload(day_runtime),
            "permissions": dict(PERMISSIONS),
        }

    def query(
        self,
        *,
        day: str,
        decision_ts_ms: int,
        best_bid_ticks: int,
        best_ask_ticks: int,
        candidate_price_ticks: Mapping[str, Sequence[int]],
    ) -> dict[str, Any]:
        """Query exact side-specific reach probabilities at one 10s boundary.

        BBO and executable candidate prices are represented only by integer
        tick identities.  Any unsupported input returns an empty prediction
        payload and requires the caller to retain its baseline behavior.
        """

        day = str(day)
        day_runtime = self._days.get(day)
        if day_runtime is None:
            return self._fallback(
                day=day,
                decision_ts_ms=decision_ts_ms,
                reason="day_not_bound_to_oof_context",
            )
        decision = _require_tick_integer(decision_ts_ms)
        if decision is None:
            return self._fallback(
                day=day,
                decision_ts_ms=decision_ts_ms,
                reason="decision_timestamp_not_integer_ms",
                day_runtime=day_runtime,
            )
        if decision % 10_000 != 0:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="decision_not_canonical_10s_boundary",
                day_runtime=day_runtime,
            )
        try:
            decision_day = _utc_day(decision)
        except (OSError, OverflowError, ValueError):
            decision_day = ""
        if decision_day != day:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="decision_timestamp_day_mismatch",
                day_runtime=day_runtime,
            )
        row_index = day_runtime.row_by_start_ts_ms.get(decision)
        if row_index is None:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="context_start_ts_exact_match_missing",
                day_runtime=day_runtime,
            )

        context = day_runtime.context
        feature_ready = int(np.asarray(context["feature_ready_ts_ms"])[row_index])
        if feature_ready > decision:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="feature_ready_after_decision",
                day_runtime=day_runtime,
            )
        caller_bid = _require_tick_integer(best_bid_ticks)
        caller_ask = _require_tick_integer(best_ask_ticks)
        if caller_bid is None or caller_ask is None or caller_bid >= caller_ask:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="caller_bbo_not_valid_integer_ticks",
                day_runtime=day_runtime,
            )
        context_bid = _price_to_tick(
            np.asarray(context["best_bid"])[row_index],
            self._tick_size,
        )
        context_ask = _price_to_tick(
            np.asarray(context["best_ask"])[row_index],
            self._tick_size,
        )
        if context_bid is None or context_ask is None or context_bid >= context_ask:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="context_bbo_not_valid_integer_ticks",
                day_runtime=day_runtime,
            )
        if caller_bid != context_bid or caller_ask != context_ask:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="caller_context_bbo_tick_mismatch",
                day_runtime=day_runtime,
            )
        if not isinstance(candidate_price_ticks, Mapping) or not candidate_price_ticks:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="candidate_set_missing",
                day_runtime=day_runtime,
            )
        if not set(candidate_price_ticks).issubset(SIDES):
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="candidate_side_unsupported",
                day_runtime=day_runtime,
            )

        prepared: dict[str, tuple[list[int], np.ndarray]] = {}
        for side in SIDES:
            if side not in candidate_price_ticks:
                continue
            raw_prices = candidate_price_ticks[side]
            if isinstance(raw_prices, (str, bytes)):
                raw_prices = ()
            prices = [_require_tick_integer(value) for value in raw_prices]
            if not prices or any(price is None for price in prices):
                return self._fallback(
                    day=day,
                    decision_ts_ms=decision,
                    reason="candidate_price_not_integer_tick",
                    day_runtime=day_runtime,
                )
            integer_prices = [int(price) for price in prices if price is not None]
            if any(price <= 0 for price in integer_prices):
                return self._fallback(
                    day=day,
                    decision_ts_ms=decision,
                    reason="candidate_price_tick_not_positive",
                    day_runtime=day_runtime,
                )
            if len(set(integer_prices)) != len(integer_prices):
                return self._fallback(
                    day=day,
                    decision_ts_ms=decision,
                    reason="candidate_price_tick_duplicate",
                    day_runtime=day_runtime,
                )
            if side == "BUY":
                if any(price >= caller_ask for price in integer_prices):
                    return self._fallback(
                        day=day,
                        decision_ts_ms=decision,
                        reason="candidate_gtx_invalid",
                        day_runtime=day_runtime,
                    )
                distance_ticks = [caller_bid - price for price in integer_prices]
            else:
                if any(price <= caller_bid for price in integer_prices):
                    return self._fallback(
                        day=day,
                        decision_ts_ms=decision,
                        reason="candidate_gtx_invalid",
                        day_runtime=day_runtime,
                    )
                distance_ticks = [price - caller_ask for price in integer_prices]
            distances_decimal = [
                Decimal(distance) * self._tick_size for distance in distance_ticks
            ]
            if any(
                distance < DISTANCE_MIN or distance > DISTANCE_MAX
                for distance in distances_decimal
            ):
                return self._fallback(
                    day=day,
                    decision_ts_ms=decision,
                    reason="distance_outside_strict_support",
                    day_runtime=day_runtime,
                )
            prepared[side] = (
                integer_prices,
                np.asarray([float(distance) for distance in distances_decimal]),
            )

        fold = self._folds[day_runtime.fold_id]
        predictions: dict[str, list[dict[str, Any]]] = {}
        try:
            for side in SIDES:
                if side not in prepared:
                    continue
                prices, distances = prepared[side]
                probabilities = np.asarray(
                    fold.model.predict(
                        context,
                        side=side,
                        distances=distances,
                        row_indices=np.full(
                            distances.shape,
                            row_index,
                            dtype=np.int64,
                        ),
                    ),
                    dtype=np.float64,
                ).reshape(-1)
                if (
                    probabilities.shape != distances.shape
                    or not np.all(np.isfinite(probabilities))
                    or np.any(probabilities < 0.0)
                    or np.any(probabilities > 1.0)
                ):
                    raise ValueError("invalid conditional probability output")
                predictions[side] = [
                    {
                        "side": side,
                        "price_ticks": int(price),
                        "price": float(Decimal(price) * self._tick_size),
                        "distance_usdc_per_btc": float(distance),
                        "probability": float(probability),
                    }
                    for price, distance, probability in zip(
                        prices,
                        distances,
                        probabilities,
                        strict=True,
                    )
                ]
        except Exception:
            return self._fallback(
                day=day,
                decision_ts_ms=decision,
                reason="conditional_touch_prediction_invalid",
                day_runtime=day_runtime,
            )

        return {
            "identity": IDENTITY,
            "supported": True,
            "fallback_required": False,
            "fallback_reason": None,
            "day": day,
            "decision_ts_ms": decision,
            "feature_ready_ts_ms": feature_ready,
            "source": day_runtime.source,
            "fold_id": day_runtime.fold_id,
            "best_bid_ticks": caller_bid,
            "best_ask_ticks": caller_ask,
            "tick_size": float(self._tick_size),
            "predictions": predictions,
            "artifact_hashes": self._identity_payload(day_runtime),
            "permissions": dict(PERMISSIONS),
        }


__all__ = [
    "ArtifactIntegrityError",
    "DISTANCE_MAX",
    "DISTANCE_MIN",
    "IDENTITY",
    "P3TouchExactDistanceSurface",
]
