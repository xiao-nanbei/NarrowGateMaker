"""Shadow-only inference boundary for the direct placement-fill surface.

This module deliberately does not choose a quote action.  It exposes
P(fill by horizon | do(new placement action), decision-visible state) for the
three paired placement actions.  KEEP and REPLACE start from an already-active
order and therefore belong to a separate continuation model.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ACTION_DELTAS = {
    "closer_1tick": -1,
    "current": 0,
    "farther_1tick": 1,
}
HORIZONS_MS = (1_000, 5_000, 10_000)
ROLES = ("opener", "add", "reducing")
STAGE_ORDER = {
    "development": 0,
    "validation": 1,
    "sealed_holdout": 2,
    "late_evidence": 3,
}

MODEL_FEATURES = (
    "distance_ticks",
    "log_horizon_ms",
    "distance_vol_units",
    "bbo_spread_ticks",
    "inventory",
    "inventory_ratio",
    "campaign_active",
    "campaign_age_log1p",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "toxicity",
    "markout_ema",
    "depth_age_log1p",
    "sigma_sq_raw_log1p",
    "sigma_sq_blended_log1p",
    "kappa_used",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total_log1p",
    "final_pair_spread",
    "final_quote_skew",
    "allow_exposure_increase",
    "exposure_increasing",
    "side_adverse_pause",
    "defense_guard",
    "defense_pause",
    "local_extreme_pause",
    "role_opener",
    "role_add",
    "role_reducing",
)

RAW_CONTEXT_FIELDS = (
    "best_bid",
    "best_ask",
    "current_quote_price",
    "inventory",
    "inventory_ratio",
    "campaign_active",
    "campaign_age_s",
    "campaign_max_abs_qty_so_far",
    "campaign_pnl_so_far",
    "campaign_adverse_excursion_so_far",
    "campaign_exposure_increasing_fills_so_far",
    "campaign_reducing_fills_so_far",
    "toxicity",
    "markout_ema",
    "depth_age_s",
    "sigma_sq_raw",
    "sigma_sq_blended",
    "quote_horizon_s",
    "kappa_used",
    "microprice_shift_bps",
    "l2_quote_flip_rate",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "l2_near_depth_total",
    "final_pair_spread",
    "final_quote_skew",
    "allow_exposure_increase",
    "exposure_increasing",
    "side_adverse_pause",
    "defense_guard",
    "defense_pause",
    "local_extreme_pause",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _logistic(value: float) -> float:
    if value >= 0.0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _logit(probability: float) -> float:
    probability = min(max(float(probability), 1e-7), 1.0 - 1e-7)
    return math.log(probability / (1.0 - probability))


def _number(context: Mapping[str, Any], name: str) -> float:
    if name not in context:
        raise ValueError(f"placement fill context is missing {name!r}")
    value = float(context[name])
    if not math.isfinite(value):
        raise ValueError(f"placement fill context has non-finite {name!r}")
    return value


def _qualification_stage(report: Mapping[str, Any]) -> str | None:
    keys = (
        ("late_evidence", "late_evidence_prediction_gate_passed"),
        ("sealed_holdout", "sealed_holdout_prediction_gate_passed"),
        ("validation", "validation_prediction_gate_passed"),
        ("development", "development_prediction_gate_passed"),
    )
    for stage, key in keys:
        if bool(report.get(key, False)):
            return stage
    return None


@dataclass(frozen=True)
class PlacementFillProbability:
    side: str
    role: str
    action: str
    horizon_ms: int
    requested_price: float
    distance_ticks: float
    probability: float


@dataclass(frozen=True)
class PlacementFillSurface:
    family_id: str
    artifact_sha256: str
    qualification_report_sha256: str
    qualification_stage: str
    probabilities: tuple[PlacementFillProbability, ...]
    prediction_only: bool = True
    action_or_live_authorized: bool = False
    active_order_keep_replace_included: bool = False

    def probability(self, action: str, horizon_ms: int) -> float:
        matches = [
            row.probability
            for row in self.probabilities
            if row.action == action and row.horizon_ms == int(horizon_ms)
        ]
        if len(matches) != 1:
            raise KeyError((action, horizon_ms))
        return float(matches[0])


class PlacementFillProbabilityScorer:
    """Load a frozen CIF artifact and emit a prediction-only action surface."""

    def __init__(
        self,
        *,
        artifact: Mapping[str, Any],
        family_id: str,
        artifact_sha256: str,
        qualification_report_sha256: str,
        qualification_stage: str,
        tick_size: float,
    ) -> None:
        if tick_size <= 0.0:
            raise ValueError("tick_size must be positive")
        if tuple(artifact.get("features", ())) != MODEL_FEATURES:
            raise RuntimeError("placement-fill artifact feature contract changed")
        if artifact.get("active_order_keep_replace") != "separate_not_built":
            raise RuntimeError("placement artifact mixed KEEP/REPLACE outcomes")
        if not bool(artifact.get("placement_actions_pooled_inside_calibration_cells")):
            raise RuntimeError("placement artifact uses an unsupported gate identity")
        self._artifact = artifact
        self.family_id = family_id
        self.artifact_sha256 = artifact_sha256
        self.qualification_report_sha256 = qualification_report_sha256
        self.qualification_stage = qualification_stage
        self.tick_size = float(tick_size)

    @classmethod
    def load(
        cls,
        artifact_path: str | Path,
        qualification_report_path: str | Path,
        *,
        tick_size: float,
        minimum_stage: str = "sealed_holdout",
    ) -> PlacementFillProbabilityScorer:
        if minimum_stage not in STAGE_ORDER:
            raise ValueError(f"unknown minimum qualification stage={minimum_stage!r}")
        artifact_path = Path(artifact_path).expanduser().resolve()
        report_path = Path(qualification_report_path).expanduser().resolve()
        report = json.loads(report_path.read_text(encoding="utf-8"))
        stage = _qualification_stage(report)
        if stage is None or STAGE_ORDER[stage] < STAGE_ORDER[minimum_stage]:
            raise RuntimeError(
                f"placement-fill artifact reached {stage!r}, below required "
                f"stage {minimum_stage!r}"
            )
        if report.get("prediction_qualification") != "prediction_transfer_shadow_gate":
            raise RuntimeError("qualification report is not a shadow transfer gate")
        if bool(report.get("action_or_live_authorization", False)):
            raise RuntimeError("prediction qualification unexpectedly authorizes action")
        expected = report.get("outputs", {}).get("artifact", {}).get("sha256")
        actual = _sha256(artifact_path)
        if expected != actual:
            raise RuntimeError("placement-fill artifact hash does not match report")

        # Importing the research runtime is intentionally lazy. Merely importing
        # the live strategy package must not load sklearn/joblib.
        import joblib

        artifact = joblib.load(artifact_path)
        if artifact.get("prediction_qualification") not in (
            None,
            "prediction_transfer_shadow_gate",
        ):
            raise RuntimeError("artifact has an incompatible prediction qualification")
        if bool(artifact.get("action_or_live_authorization", False)):
            raise RuntimeError("prediction artifact must not carry action authority")
        return cls(
            artifact=artifact,
            family_id=str(report["family_id"]),
            artifact_sha256=actual,
            qualification_report_sha256=_sha256(report_path),
            qualification_stage=stage,
            tick_size=tick_size,
        )

    def _feature_rows(
        self,
        *,
        side: str,
        role: str,
        context: Mapping[str, Any],
    ) -> tuple[list[dict[str, float]], list[tuple[str, int, float, float]]]:
        side = str(side).upper()
        role = str(role).lower()
        if side not in ("BUY", "SELL"):
            raise ValueError(f"unsupported side={side!r}")
        if role not in ROLES:
            raise ValueError(f"unsupported inventory role={role!r}")
        for name in RAW_CONTEXT_FIELDS:
            _number(context, name)

        best_bid = _number(context, "best_bid")
        best_ask = _number(context, "best_ask")
        if not 0.0 < best_bid < best_ask:
            raise ValueError("invalid BBO for placement-fill scoring")
        current_tick = int(round(_number(context, "current_quote_price") / self.tick_size))
        variance = max(0.0, _number(context, "sigma_sq_blended"))
        quote_horizon_s = max(1e-6, _number(context, "quote_horizon_s"))
        expected_move = math.sqrt(variance * quote_horizon_s)

        common = {
            "bbo_spread_ticks": max(1.0, (best_ask - best_bid) / self.tick_size),
            "inventory": _number(context, "inventory"),
            "inventory_ratio": _number(context, "inventory_ratio"),
            "campaign_active": _number(context, "campaign_active"),
            "campaign_age_log1p": math.log1p(max(0.0, _number(context, "campaign_age_s"))),
            "campaign_max_abs_qty_so_far": _number(context, "campaign_max_abs_qty_so_far"),
            "campaign_pnl_so_far": _number(context, "campaign_pnl_so_far"),
            "campaign_adverse_excursion_so_far": _number(context, "campaign_adverse_excursion_so_far"),
            "campaign_exposure_increasing_fills_so_far": _number(context, "campaign_exposure_increasing_fills_so_far"),
            "campaign_reducing_fills_so_far": _number(context, "campaign_reducing_fills_so_far"),
            "toxicity": _number(context, "toxicity"),
            "markout_ema": _number(context, "markout_ema"),
            "depth_age_log1p": math.log1p(max(0.0, _number(context, "depth_age_s") * 1000.0)),
            "sigma_sq_raw_log1p": math.log1p(max(0.0, _number(context, "sigma_sq_raw"))),
            "sigma_sq_blended_log1p": math.log1p(variance),
            "kappa_used": _number(context, "kappa_used"),
            "microprice_shift_bps": _number(context, "microprice_shift_bps"),
            "l2_quote_flip_rate": _number(context, "l2_quote_flip_rate"),
            "l2_book_refresh_ratio": _number(context, "l2_book_refresh_ratio"),
            "l2_book_cancel_ratio": _number(context, "l2_book_cancel_ratio"),
            "l2_near_depth_total_log1p": math.log1p(max(0.0, _number(context, "l2_near_depth_total"))),
            "final_pair_spread": _number(context, "final_pair_spread"),
            "final_quote_skew": _number(context, "final_quote_skew"),
            "allow_exposure_increase": _number(context, "allow_exposure_increase"),
            "exposure_increasing": _number(context, "exposure_increasing"),
            "side_adverse_pause": _number(context, "side_adverse_pause"),
            "defense_guard": _number(context, "defense_guard"),
            "defense_pause": _number(context, "defense_pause"),
            "local_extreme_pause": _number(context, "local_extreme_pause"),
            "role_opener": float(role == "opener"),
            "role_add": float(role == "add"),
            "role_reducing": float(role == "reducing"),
        }

        rows: list[dict[str, float]] = []
        identity: list[tuple[str, int, float, float]] = []
        side_direction = 1 if side == "BUY" else -1
        for action, distance_delta in ACTION_DELTAS.items():
            candidate_tick = current_tick - side_direction * int(distance_delta)
            price = float(candidate_tick) * self.tick_size
            distance_ticks = (
                (best_bid - price) / self.tick_size
                if side == "BUY"
                else (price - best_ask) / self.tick_size
            )
            distance_ticks = max(0.0, round(distance_ticks, 6))
            for horizon_ms in HORIZONS_MS:
                row = dict(common)
                row.update(
                    distance_ticks=distance_ticks,
                    log_horizon_ms=math.log(float(horizon_ms)),
                    distance_vol_units=(
                        distance_ticks
                        * self.tick_size
                        / max(expected_move, self.tick_size)
                    ),
                )
                rows.append(row)
                identity.append((action, horizon_ms, price, distance_ticks))
        return rows, identity

    def score(
        self,
        *,
        side: str,
        role: str,
        context: Mapping[str, Any],
    ) -> PlacementFillSurface:
        side = str(side).upper()
        role = str(role).lower()
        rows, identity = self._feature_rows(side=side, role=role, context=context)

        import pandas as pd

        frame = pd.DataFrame(rows, columns=MODEL_FEATURES)
        bundle = self._artifact["models"][side]
        raw = bundle["model"].predict_proba(frame)[:, 1]
        affine = bundle["calibrator"]
        probabilities: list[PlacementFillProbability] = []
        for value, (action, horizon_ms, price, distance_ticks) in zip(  # noqa: B905
            raw, identity
        ):
            offset = float(bundle["cell_offsets"][(role, int(horizon_ms))])
            probability = _logistic(
                float(affine["intercept"])
                + float(affine["slope"]) * _logit(float(value))
                + offset
            )
            probabilities.append(
                PlacementFillProbability(
                    side=side,
                    role=role,
                    action=action,
                    horizon_ms=int(horizon_ms),
                    requested_price=float(price),
                    distance_ticks=float(distance_ticks),
                    probability=min(max(probability, 1e-7), 1.0 - 1e-7),
                )
            )
        return PlacementFillSurface(
            family_id=self.family_id,
            artifact_sha256=self.artifact_sha256,
            qualification_report_sha256=self.qualification_report_sha256,
            qualification_stage=self.qualification_stage,
            probabilities=tuple(probabilities),
        )
