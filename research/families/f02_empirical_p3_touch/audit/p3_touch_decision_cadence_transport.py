#!/usr/bin/env python3
"""Historical decision-cadence transport audit for conditional P3 v4.1.

The frozen v4.1 surface was trained and evaluated at canonical, non-overlapping
10-second boundaries.  This module evaluates the same chronological OOF models
at baseline-eligible placement decisions without fitting or calibrating on the
new rows.  Labels are strict-future aggressive reach over ten seconds.

The audit is prediction-only.  It does not read fills, queue outcomes, markout,
PnL, reward, or action assignments, and it cannot authorize quote mapping.
"""

from __future__ import annotations

import hashlib
import math
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd

from research.families.f02_empirical_p3_touch.audit.p3_touch_calibration import (
    _buyer_maker,
    _timestamp_ms,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_decision_cadence_context import (
    DECISION_CONTEXT_FIELDS,
    DecisionCadenceContextBatch,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_exact_distance_surface import (
    DISTANCE_MAX,
    DISTANCE_MIN,
    V4_1_IDENTITY,
    V4_IDENTITY,
    ArtifactIntegrityError,
    _canonical_sha256,
    _load_json,
    _require_no_authority,
    _verify_artifact,
)
from research.families.f02_empirical_p3_touch.audit.p3_touch_volatility_conditioned import (
    ConditionalTouchModel,
)
from research.families.f02_empirical_p3_touch.fill_probability import (
    FillProbabilityModel,
)
from research.governance.paths import resolve_research_path

IDENTITY = "p3_touch_decision_cadence_transport_v1"
SCHEMA_VERSION = "narrowgate_p3_touch_decision_cadence_transport.v1"
SIDES = ("BUY", "SELL")
ACTION_OFFSETS = {
    "closer_4tick": -4,
    "closer_2tick": -2,
    "closer_1tick": -1,
    "current": 0,
    "farther_1tick": 1,
    "farther_2tick": 2,
    "farther_4tick": 4,
}
ACTION_NAMES = tuple(ACTION_OFFSETS)
CURRENT_ACTION = "current"
PERMISSIONS = {
    "development_only": True,
    "historical_transport_diagnostic": True,
    "independent_confirmation": False,
    "model_refit_performed": False,
    "calibration_refit_performed": False,
    "economic_outcomes_read": False,
    "quote_mapping_authority": False,
    "action_authority": False,
    "live_authority": False,
    "aws_receive_time_transport_supported": False,
    "validation_read": False,
    "sealed_holdout_read": False,
}


@dataclass(frozen=True)
class TransportGates:
    """Frozen prediction-only gates for decision-cadence transport."""

    minimum_supported_days: int = 30
    required_oof_fold_count: int = 4
    minimum_context_coverage: float = 0.95
    minimum_candidate_distance_coverage: float = 0.95
    maximum_iace_worsening: float = 0.002
    bootstrap_draws: int = 20_000
    bootstrap_seed: int = 20260803
    require_both_side_score_improvement: bool = True
    require_zero_monotonicity_violations: bool = True


@dataclass(frozen=True)
class _FoldModel:
    fold_id: str
    model: ConditionalTouchModel
    model_sha256: str
    calibration_sha256: str


class DecisionCadenceOOFModels:
    """Hash-bound v4.1 OOF models plus the frozen static-v2 comparator."""

    def __init__(
        self,
        *,
        v4_1_spec: Mapping[str, Any],
        v2_artifact: Mapping[str, Any],
    ) -> None:
        self._v4_1_artifact = _verify_artifact(
            v4_1_spec,
            label="conditional P3 v4.1 spec",
        )
        v4_1 = _load_json(self._v4_1_artifact, label="conditional P3 v4.1 spec")
        if v4_1.get("identity") != V4_1_IDENTITY:
            raise ArtifactIntegrityError("unexpected conditional P3 v4.1 identity")
        if v4_1.get("predecessor_identity") != V4_IDENTITY:
            raise ArtifactIntegrityError("v4.1 predecessor identity is not v4")
        if v4_1.get("canonical_spec_identity_sha256") != _canonical_sha256(
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
        v4_spec = _load_json(self._v4_spec_artifact, label="conditional P3 v4 spec")
        v4_report = _load_json(self._v4_report_artifact, label="conditional P3 v4 report")
        self._validate_v4(v4_spec=v4_spec, v4_report=v4_report)
        self._folds, self._day_to_fold = self._load_folds(
            v4_spec=v4_spec,
            v4_report=v4_report,
        )

        self._v2_artifact = _verify_artifact(v2_artifact, label="current P3 v2 artifact")
        self._v2 = FillProbabilityModel.load(self._v2_artifact.path)
        semantics = self._v2.semantic_identity(require_artifact_hash=True)
        if semantics != {
            "event_type": "touch",
            "horizon_s": 10.0,
            "distance_unit": "USDC_per_BTC",
            "artifact_sha256": self._v2_artifact.sha256,
        }:
            raise ArtifactIntegrityError("current P3 v2 semantic identity drift")

    def _validate_v4(
        self,
        *,
        v4_spec: Mapping[str, Any],
        v4_report: Mapping[str, Any],
    ) -> None:
        if v4_spec.get("identity") != V4_IDENTITY or v4_report.get("identity") != V4_IDENTITY:
            raise ArtifactIntegrityError("conditional P3 predecessor identity drift")
        if v4_spec.get("canonical_spec_identity_sha256") != _canonical_sha256(
            v4_spec,
            omit="canonical_spec_identity_sha256",
        ):
            raise ArtifactIntegrityError("conditional P3 v4 canonical hash mismatch")
        _require_no_authority(v4_spec, label="conditional P3 v4 spec")
        _require_no_authority(v4_report, label="conditional P3 v4 report")
        report_spec = v4_report.get("spec")
        if not isinstance(report_spec, Mapping):
            raise ArtifactIntegrityError("conditional P3 v4 report lacks spec identity")
        if (
            resolve_research_path(str(report_spec.get("path", "")))
            != self._v4_spec_artifact.path
            or str(report_spec.get("sha256", "")) != self._v4_spec_artifact.sha256
        ):
            raise ArtifactIntegrityError("conditional P3 v4 report/spec mismatch")
        estimand = v4_spec.get("estimand")
        if not isinstance(estimand, Mapping) or estimand.get("event_type") != "touch":
            raise ArtifactIntegrityError("conditional P3 predecessor is not touch")
        if float(estimand.get("horizon_s", 0.0)) != 10.0:
            raise ArtifactIntegrityError("conditional P3 predecessor horizon drift")
        if estimand.get("distance_unit") != "USDC_per_BTC":
            raise ArtifactIntegrityError("conditional P3 predecessor distance drift")

    def _load_folds(
        self,
        *,
        v4_spec: Mapping[str, Any],
        v4_report: Mapping[str, Any],
    ) -> tuple[dict[str, _FoldModel], dict[str, str]]:
        fold_specs = v4_spec.get("chronological_oof", {}).get("folds")
        report_artifacts = v4_report.get("fold_artifacts")
        feature_contract = v4_spec.get("model", {}).get("feature_contract")
        if not isinstance(fold_specs, list) or not fold_specs:
            raise ArtifactIntegrityError("conditional P3 OOF folds are missing")
        if not isinstance(report_artifacts, Mapping):
            raise ArtifactIntegrityError("conditional P3 fold artifacts are missing")
        if not isinstance(feature_contract, Mapping):
            raise ArtifactIntegrityError("conditional P3 feature contract is missing")

        day_to_fold: dict[str, str] = {}
        folds: dict[str, _FoldModel] = {}
        expected_fold_ids: list[str] = []
        for fold_spec in fold_specs:
            fold_id = str(fold_spec.get("fold_id", ""))
            test_days = fold_spec.get("test_days")
            if not fold_id or fold_id in expected_fold_ids:
                raise ArtifactIntegrityError("conditional P3 OOF fold IDs are invalid")
            if not isinstance(test_days, list) or not test_days:
                raise ArtifactIntegrityError(f"{fold_id} has no OOF days")
            expected_fold_ids.append(fold_id)
            for raw_day in test_days:
                day = str(raw_day)
                if day in day_to_fold:
                    raise ArtifactIntegrityError(f"OOF day belongs to multiple folds: {day}")
                day_to_fold[day] = fold_id

        if set(report_artifacts) != set(expected_fold_ids):
            raise ArtifactIntegrityError("conditional P3 fold artifact set mismatch")
        for fold_id in expected_fold_ids:
            artifact_set = report_artifacts[fold_id]
            model_artifact = _verify_artifact(
                artifact_set.get("model", {}),
                label=f"{fold_id} model",
            )
            calibration_artifact = _verify_artifact(
                artifact_set.get("calibration", {}),
                label=f"{fold_id} calibration",
            )
            calibration = _load_json(calibration_artifact, label=f"{fold_id} calibration")
            intercept = float(calibration.get("intercept", math.nan))
            slope = float(calibration.get("slope", math.nan))
            if not math.isfinite(intercept) or not math.isfinite(slope) or slope <= 0.0:
                raise ArtifactIntegrityError(f"{fold_id} calibration is invalid")
            booster = lgb.Booster(model_file=str(model_artifact.path))
            folds[fold_id] = _FoldModel(
                fold_id=fold_id,
                model=ConditionalTouchModel(
                    booster=booster,
                    calibration=calibration,
                    feature_contract=feature_contract,
                ),
                model_sha256=model_artifact.sha256,
                calibration_sha256=calibration_artifact.sha256,
            )
        return folds, day_to_fold

    @property
    def identity(self) -> dict[str, Any]:
        return {
            "v4_1_spec_sha256": self._v4_1_artifact.sha256,
            "v4_spec_sha256": self._v4_spec_artifact.sha256,
            "v4_report_sha256": self._v4_report_artifact.sha256,
            "v2_artifact_sha256": self._v2_artifact.sha256,
            "folds": {
                fold_id: {
                    "model_sha256": fold.model_sha256,
                    "calibration_sha256": fold.calibration_sha256,
                }
                for fold_id, fold in self._folds.items()
            },
        }

    def fold_id(self, day: str) -> str:
        try:
            return self._day_to_fold[str(day)]
        except KeyError as exc:
            raise ArtifactIntegrityError(f"day is outside frozen OOF support: {day}") from exc

    def predict_v4(
        self,
        *,
        day: str,
        context: Mapping[str, np.ndarray],
        side: str,
        distances: np.ndarray,
    ) -> np.ndarray:
        if side not in SIDES:
            raise ValueError("side must be BUY or SELL")
        distances = np.asarray(distances, dtype=np.float64).reshape(-1)
        if np.any(distances < float(DISTANCE_MIN)) or np.any(
            distances > float(DISTANCE_MAX)
        ):
            raise ValueError("decision-cadence distance is outside v4 support")
        starts = np.asarray(context["start_ts_ms"], dtype=np.int64)
        if len(starts) != len(distances):
            raise ValueError("decision context and distance shapes differ")
        fold = self._folds[self.fold_id(day)]
        result = np.asarray(
            fold.model.predict(
                context,
                side=side,
                distances=distances,
                row_indices=np.arange(len(distances), dtype=np.int64),
            ),
            dtype=np.float64,
        )
        if (
            result.shape != distances.shape
            or not np.isfinite(result).all()
            or np.any(result < 0.0)
            or np.any(result > 1.0)
        ):
            raise ValueError("conditional P3 produced invalid probabilities")
        return result

    def predict_v2(self, distances: np.ndarray) -> np.ndarray:
        result = np.asarray(self._v2.prob(distances), dtype=np.float64)
        if not np.isfinite(result).all() or np.any(result < 0.0) or np.any(result > 1.0):
            raise ValueError("static P3 v2 produced invalid probabilities")
        return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_official_aggressive_trades(
    paths: Sequence[Path],
    *,
    expected_sha256: Mapping[str, str],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Load hash-bound official trades for strict-future reach labels."""

    frames: list[pd.DataFrame] = []
    for raw_path in paths:
        path = raw_path.expanduser().resolve()
        expected = str(expected_sha256.get(str(path), ""))
        if len(expected) != 64 or _sha256(path) != expected:
            raise ArtifactIntegrityError(f"official aggTrades identity mismatch: {path}")
        frame = pd.read_csv(
            path,
            usecols=["price", "transact_time", "is_buyer_maker"],
        ).dropna(subset=["price", "transact_time"])
        frames.append(frame)
    if not frames:
        raise ValueError("at least one official aggTrades file is required")
    trades = pd.concat(frames, ignore_index=True)
    timestamps = _timestamp_ms(trades["transact_time"])
    prices = pd.to_numeric(trades["price"], errors="coerce").to_numpy(dtype=np.float64)
    buyer_maker = _buyer_maker(trades["is_buyer_maker"])
    valid = np.isfinite(prices) & (prices > 0.0)
    order = np.argsort(timestamps[valid], kind="stable")
    return timestamps[valid][order], prices[valid][order], buyer_maker[valid][order]


def _strict_future_extreme(
    query_ts_ms: np.ndarray,
    event_ts_ms: np.ndarray,
    values: np.ndarray,
    *,
    horizon_ms: int,
    minimum: bool,
) -> np.ndarray:
    """Return min/max values for each strict interval ``(t, t + H)``."""

    query = np.asarray(query_ts_ms, dtype=np.int64).reshape(-1)
    event_ts = np.asarray(event_ts_ms, dtype=np.int64).reshape(-1)
    event_values = np.asarray(values, dtype=np.float64).reshape(-1)
    if event_ts.shape != event_values.shape or np.any(np.diff(event_ts) < 0):
        raise ValueError("future-reach event tape must be aligned and sorted")
    if int(horizon_ms) <= 0:
        raise ValueError("future-reach horizon must be positive")

    order = np.argsort(query, kind="stable")
    sorted_query = query[order]
    result = np.full(len(query), np.nan, dtype=np.float64)
    active: deque[int] = deque()
    right = 0
    for output_index, start in zip(order, sorted_query, strict=True):
        end = int(start) + int(horizon_ms)
        while right < len(event_ts) and int(event_ts[right]) < end:
            value = event_values[right]
            if minimum:
                while active and event_values[active[-1]] >= value:
                    active.pop()
            else:
                while active and event_values[active[-1]] <= value:
                    active.pop()
            active.append(right)
            right += 1
        while active and int(event_ts[active[0]]) <= int(start):
            active.popleft()
        if active:
            result[output_index] = event_values[active[0]]
    return result


def strict_future_aggressive_reach(
    decisions: pd.DataFrame,
    *,
    trade_ts_ms: np.ndarray,
    trade_prices: np.ndarray,
    buyer_maker: np.ndarray,
    horizon_ms: int = 10_000,
) -> np.ndarray:
    """Compute side-correct aggressive reach after each decision."""

    if decisions.empty:
        return np.asarray([], dtype=np.float64)
    timestamps = decisions["decision_ts_ms"].to_numpy(dtype=np.int64)
    sides = decisions["side"].astype(str).to_numpy()
    bids = decisions["best_bid"].to_numpy(dtype=np.float64)
    asks = decisions["best_ask"].to_numpy(dtype=np.float64)
    reach = np.full(len(decisions), -np.inf, dtype=np.float64)
    for side in SIDES:
        rows = sides == side
        if not np.any(rows):
            continue
        if side == "BUY":
            events = np.asarray(buyer_maker, dtype=bool)
            extreme = _strict_future_extreme(
                timestamps[rows],
                np.asarray(trade_ts_ms)[events],
                np.asarray(trade_prices)[events],
                horizon_ms=horizon_ms,
                minimum=True,
            )
            available = np.isfinite(extreme)
            values = np.full(len(extreme), -np.inf, dtype=np.float64)
            values[available] = bids[rows][available] - extreme[available]
        else:
            events = ~np.asarray(buyer_maker, dtype=bool)
            extreme = _strict_future_extreme(
                timestamps[rows],
                np.asarray(trade_ts_ms)[events],
                np.asarray(trade_prices)[events],
                horizon_ms=horizon_ms,
                minimum=False,
            )
            available = np.isfinite(extreme)
            values = np.full(len(extreme), -np.inf, dtype=np.float64)
            values[available] = extreme[available] - asks[rows][available]
        reach[rows] = values
    return reach


def score_decision_day(
    batch: DecisionCadenceContextBatch,
    *,
    models: DecisionCadenceOOFModels,
    trade_ts_ms: np.ndarray,
    trade_prices: np.ndarray,
    buyer_maker: np.ndarray,
    tick_size: float = 0.1,
) -> pd.DataFrame:
    """Score the frozen seven-distance grid on one historical decision day."""

    supported = batch.supported
    if supported.empty:
        return pd.DataFrame()
    days = supported["day"].astype(str).unique()
    if len(days) != 1:
        raise ValueError("one score_decision_day call must contain exactly one UTC day")
    day = str(days[0])
    if not math.isfinite(float(tick_size)) or float(tick_size) <= 0.0:
        raise ValueError("tick_size must be positive")

    reach = strict_future_aggressive_reach(
        supported,
        trade_ts_ms=trade_ts_ms,
        trade_prices=trade_prices,
        buyer_maker=buyer_maker,
    )
    scored_parts: list[pd.DataFrame] = []
    for side in SIDES:
        rows = supported.loc[supported["side"].eq(side)].reset_index(drop=True)
        if rows.empty:
            continue
        row_reach = reach[supported["side"].eq(side).to_numpy()]
        bid_ticks = np.rint(rows["best_bid"].to_numpy(dtype=np.float64) / tick_size).astype(
            np.int64
        )
        ask_ticks = np.rint(rows["best_ask"].to_numpy(dtype=np.float64) / tick_size).astype(
            np.int64
        )
        baseline_ticks = rows["baseline_price_tick"].to_numpy(dtype=np.int64)
        current_distance_ticks = (
            bid_ticks - baseline_ticks if side == "BUY" else baseline_ticks - ask_ticks
        )
        context = {
            field: rows[field].to_numpy(copy=True) for field in DECISION_CONTEXT_FIELDS
        }
        for action, offset in ACTION_OFFSETS.items():
            distance_ticks = current_distance_ticks + int(offset)
            distance = distance_ticks.astype(np.float64) * float(tick_size)
            valid = (
                (distance >= float(DISTANCE_MIN))
                & (distance <= float(DISTANCE_MAX))
                & (distance_ticks > 0)
            )
            if not np.any(valid):
                continue
            selected_context = {
                field: np.asarray(values)[valid] for field, values in context.items()
            }
            p_v4 = models.predict_v4(
                day=day,
                context=selected_context,
                side=side,
                distances=distance[valid],
            )
            p_v2 = models.predict_v2(distance[valid])
            part = rows.loc[
                valid,
                [
                    "decision_id",
                    "day",
                    "side",
                    "inventory_role",
                    "campaign_id",
                    "decision_ts_ms",
                ],
            ].copy()
            part["fold_id"] = models.fold_id(day)
            part["action"] = action
            part["distance_usdc_per_btc"] = distance[valid]
            part["aggressive_reach_usdc_per_btc"] = row_reach[valid]
            part["touch_label"] = (row_reach[valid] >= distance[valid]).astype(np.int8)
            part["p_v4"] = p_v4
            part["p_v2"] = p_v2
            scored_parts.append(part)
    if not scored_parts:
        return pd.DataFrame()
    return pd.concat(scored_parts, ignore_index=True)


def _brier(prediction: pd.Series, target: pd.Series) -> float:
    return float(np.mean((prediction.to_numpy(dtype=float) - target.to_numpy(dtype=float)) ** 2))


def summarize_scored_day(
    scored: pd.DataFrame,
    *,
    denominator: pd.DataFrame,
    context_batch: DecisionCadenceContextBatch,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return daily and campaign summaries without retaining row-level labels."""

    rows: list[dict[str, Any]] = []
    campaigns: list[dict[str, Any]] = []
    day = str(denominator["day"].astype(str).iloc[0])
    for side in SIDES:
        total = int(denominator["side"].astype(str).eq(side).sum())
        context_rows = int(context_batch.frame["side"].astype(str).eq(side).mul(
            context_batch.frame["supported"].astype(bool)
        ).sum())
        side_scored = scored.loc[scored["side"].eq(side)]
        current = side_scored.loc[side_scored["action"].eq(CURRENT_ACTION)]
        candidate_ids = side_scored.groupby("decision_id", observed=True)["action"].nunique()
        candidate_rows = int(candidate_ids.eq(len(ACTION_NAMES)).sum())
        for metric, frame in (("current", current), ("candidate_grid", side_scored)):
            if frame.empty:
                continue
            brier_v4 = _brier(frame["p_v4"], frame["touch_label"])
            brier_v2 = _brier(frame["p_v2"], frame["touch_label"])
            rows.append(
                {
                    "day": day,
                    "fold_id": str(frame["fold_id"].iloc[0]),
                    "side": side,
                    "metric": metric,
                    "denominator_rows": total,
                    "context_supported_rows": context_rows,
                    "candidate_supported_rows": candidate_rows,
                    "context_coverage": context_rows / total if total else 0.0,
                    "candidate_distance_coverage": candidate_rows / total if total else 0.0,
                    "score_rows": int(len(frame)),
                    "touch_rate": float(frame["touch_label"].mean()),
                    "brier_v4": brier_v4,
                    "brier_v2": brier_v2,
                    "brier_delta_v4_minus_v2": brier_v4 - brier_v2,
                }
            )
        for (campaign_id, role), frame in current.groupby(
            ["campaign_id", "inventory_role"], observed=True
        ):
            campaigns.append(
                {
                    "day": day,
                    "side": side,
                    "campaign_id": campaign_id,
                    "inventory_role": str(role),
                    "rows": int(len(frame)),
                    "brier_v4": _brier(frame["p_v4"], frame["touch_label"]),
                    "brier_v2": _brier(frame["p_v2"], frame["touch_label"]),
                }
            )
    return pd.DataFrame(rows), pd.DataFrame(campaigns)


def calibration_summary(scored: pd.DataFrame) -> pd.DataFrame:
    """Fixed-bin calibration summary for current and seven-distance support."""

    rows: list[dict[str, Any]] = []
    for side in SIDES:
        side_rows = scored.loc[scored["side"].eq(side)]
        for metric, frame in (
            ("current", side_rows.loc[side_rows["action"].eq(CURRENT_ACTION)]),
            ("candidate_grid", side_rows),
        ):
            for model in ("v4", "v2"):
                probability = frame[f"p_{model}"].to_numpy(dtype=np.float64)
                target = frame["touch_label"].to_numpy(dtype=np.float64)
                bins = np.minimum((probability * 10.0).astype(np.int8), 9)
                total = len(frame)
                iace = 0.0
                for bin_id in range(10):
                    mask = bins == bin_id
                    if not np.any(mask):
                        continue
                    mean_prediction = float(np.mean(probability[mask]))
                    mean_observed = float(np.mean(target[mask]))
                    weight = float(np.sum(mask)) / float(total)
                    iace += weight * abs(mean_prediction - mean_observed)
                    rows.append(
                        {
                            "side": side,
                            "metric": metric,
                            "model": model,
                            "bin_id": bin_id,
                            "rows": int(np.sum(mask)),
                            "mean_prediction": mean_prediction,
                            "mean_observed": mean_observed,
                            "absolute_error": abs(mean_prediction - mean_observed),
                            "iace": iace,
                        }
                    )
                for row in rows:
                    if row["side"] == side and row["metric"] == metric and row["model"] == model:
                        row["iace"] = iace
    return pd.DataFrame(rows)


def aggregate_calibration_summaries(parts: Sequence[pd.DataFrame]) -> pd.DataFrame:
    """Combine fixed-bin day summaries using row-count weights."""

    if not parts:
        raise ValueError("calibration aggregation requires at least one day")
    frame = pd.concat(parts, ignore_index=True)
    required = {
        "side",
        "metric",
        "model",
        "bin_id",
        "rows",
        "mean_prediction",
        "mean_observed",
    }
    if not required.issubset(frame.columns):
        raise ValueError("calibration summary schema is incomplete")
    frame["prediction_sum"] = frame["rows"] * frame["mean_prediction"]
    frame["observed_sum"] = frame["rows"] * frame["mean_observed"]
    grouped = (
        frame.groupby(["side", "metric", "model", "bin_id"], observed=True)
        .agg(
            rows=("rows", "sum"),
            prediction_sum=("prediction_sum", "sum"),
            observed_sum=("observed_sum", "sum"),
        )
        .reset_index()
    )
    grouped["mean_prediction"] = grouped["prediction_sum"] / grouped["rows"]
    grouped["mean_observed"] = grouped["observed_sum"] / grouped["rows"]
    grouped["absolute_error"] = (
        grouped["mean_prediction"] - grouped["mean_observed"]
    ).abs()
    grouped["iace"] = 0.0
    for key, indices in grouped.groupby(["side", "metric", "model"], observed=True).groups.items():
        del key
        selected = grouped.loc[indices]
        total = float(selected["rows"].sum())
        iace = float(np.sum(selected["rows"] * selected["absolute_error"]) / total)
        grouped.loc[indices, "iace"] = iace
    return grouped.drop(columns=["prediction_sum", "observed_sum"])


def _bootstrap_mean_ci(
    values: np.ndarray,
    *,
    draws: int,
    seed: int,
) -> tuple[float, float]:
    values = np.asarray(values, dtype=np.float64)
    if values.size < 2:
        return math.nan, math.nan
    generator = np.random.default_rng(int(seed))
    samples = generator.integers(0, values.size, size=(int(draws), values.size))
    means = np.mean(values[samples], axis=1)
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def evaluate_transport(
    *,
    daily_metrics: pd.DataFrame,
    calibration: pd.DataFrame,
    monotonicity_violations: int,
    gates: TransportGates | None = None,
) -> dict[str, Any]:
    """Apply frozen transport gates to daily-clustered prediction evidence."""

    gates = TransportGates() if gates is None else gates
    if daily_metrics.empty:
        raise ValueError("decision-cadence transport has no daily metrics")
    score_rows: list[dict[str, Any]] = []
    score_supported = True
    for (side, metric), frame in daily_metrics.groupby(["side", "metric"], observed=True):
        values = frame.sort_values("day")["brier_delta_v4_minus_v2"].to_numpy(dtype=float)
        lower, upper = _bootstrap_mean_ci(
            values,
            draws=gates.bootstrap_draws,
            seed=gates.bootstrap_seed + len(score_rows),
        )
        mean = float(np.mean(values))
        passed = bool(mean <= 0.0 and upper < 0.0)
        score_supported &= passed
        score_rows.append(
            {
                "side": str(side),
                "metric": str(metric),
                "days": int(len(values)),
                "mean_brier_delta_v4_minus_v2": mean,
                "day_cluster_ci95_lower": lower,
                "day_cluster_ci95_upper": upper,
                "passed": passed,
            }
        )

    coverage = daily_metrics.groupby("side", observed=True).agg(
        minimum_context_coverage=("context_coverage", "min"),
        minimum_candidate_distance_coverage=("candidate_distance_coverage", "min"),
    )
    context_coverage_passed = bool(
        coverage["minimum_context_coverage"].ge(gates.minimum_context_coverage).all()
    )
    candidate_coverage_passed = bool(
        coverage["minimum_candidate_distance_coverage"]
        .ge(gates.minimum_candidate_distance_coverage)
        .all()
    )

    iace = (
        calibration.groupby(["side", "metric", "model"], observed=True)["iace"]
        .max()
        .unstack("model")
    )
    iace["worsening_v4_minus_v2"] = iace["v4"] - iace["v2"]
    calibration_passed = bool(
        iace["worsening_v4_minus_v2"].le(gates.maximum_iace_worsening).all()
    )
    days = sorted(daily_metrics["day"].astype(str).unique())
    folds = sorted(daily_metrics["fold_id"].astype(str).unique())
    gate_results = {
        "minimum_supported_days": len(days) >= gates.minimum_supported_days,
        "required_oof_fold_count": len(folds) == gates.required_oof_fold_count,
        "minimum_context_coverage": context_coverage_passed,
        "minimum_candidate_distance_coverage": candidate_coverage_passed,
        "proper_score": (not gates.require_both_side_score_improvement) or score_supported,
        "calibration": calibration_passed,
        "monotonicity": (not gates.require_zero_monotonicity_violations)
        or int(monotonicity_violations) == 0,
    }
    supported = bool(all(gate_results.values()))
    return {
        "schema_version": SCHEMA_VERSION,
        "identity": IDENTITY,
        "question": "does frozen canonical-v4.1 transport to actual baseline decision cadence",
        "estimand": {
            "event_type": "strict_future_aggressive_reach",
            "horizon_s": 10.0,
            "lower_endpoint": "exclusive",
            "upper_endpoint": "exclusive",
            "distance_unit": "USDC_per_BTC",
            "candidate_grid": list(ACTION_NAMES),
            "p3_role": "prediction_input_only",
            "fill_or_queue_interpretation": False,
        },
        "gates": asdict(gates),
        "gate_results": gate_results,
        "supported": supported,
        "decision": (
            "decision_cadence_transport_supported"
            if supported
            else "stop_before_direct_value_decision_cadence_transport_failed"
        ),
        "support": {
            "days": days,
            "day_count": len(days),
            "folds": folds,
            "fold_count": len(folds),
            "coverage_by_side": coverage.reset_index().to_dict("records"),
        },
        "proper_score": score_rows,
        "calibration": iace.reset_index().to_dict("records"),
        "monotonicity_violations": int(monotonicity_violations),
        "permissions": dict(PERMISSIONS),
    }


__all__ = [
    "ACTION_NAMES",
    "ACTION_OFFSETS",
    "DecisionCadenceOOFModels",
    "IDENTITY",
    "PERMISSIONS",
    "SCHEMA_VERSION",
    "TransportGates",
    "aggregate_calibration_summaries",
    "calibration_summary",
    "evaluate_transport",
    "load_official_aggressive_trades",
    "score_decision_day",
    "strict_future_aggressive_reach",
    "summarize_scored_day",
]
