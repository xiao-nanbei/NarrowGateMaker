from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np

from strategy.placement_fill_probability import (
    MODEL_FEATURES,
    PlacementFillProbabilityScorer,
)


class _DistanceModel:
    def predict_proba(self, frame):
        distance = frame["distance_ticks"].to_numpy(dtype=float)
        horizon = frame["log_horizon_ms"].to_numpy(dtype=float)
        probability = np.clip(0.25 - 0.02 * distance + 0.02 * (horizon - 6.9), 0.01, 0.9)
        return np.column_stack([1.0 - probability, probability])


def _write_artifact(tmp_path: Path, *, validation_passed: bool) -> tuple[Path, Path]:
    offsets = {
        (role, horizon): 0.0
        for role in ("opener", "add", "reducing")
        for horizon in (1_000, 5_000, 10_000)
    }
    artifact = {
        "features": MODEL_FEATURES,
        "models": {
            side: {
                "model": _DistanceModel(),
                "calibrator": {"intercept": 0.0, "slope": 1.0},
                "cell_offsets": offsets,
            }
            for side in ("BUY", "SELL")
        },
        "active_order_keep_replace": "separate_not_built",
        "placement_actions_pooled_inside_calibration_cells": True,
        "action_or_live_authorization": False,
    }
    artifact_path = tmp_path / "artifact.joblib"
    joblib.dump(artifact, artifact_path)
    import hashlib

    digest = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    report = {
        "family_id": "placement_fill_cif_test",
        "prediction_qualification": "prediction_transfer_shadow_gate",
        "development_prediction_gate_passed": True,
        "validation_prediction_gate_passed": validation_passed,
        "outputs": {"artifact": {"sha256": digest}},
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    return artifact_path, report_path


def _context() -> dict[str, float]:
    return {
        "best_bid": 99.9,
        "best_ask": 100.1,
        "current_quote_price": 99.8,
        "inventory": 0.001,
        "inventory_ratio": 0.04,
        "campaign_active": 1.0,
        "campaign_age_s": 12.0,
        "campaign_max_abs_qty_so_far": 0.002,
        "campaign_pnl_so_far": -0.1,
        "campaign_adverse_excursion_so_far": -0.2,
        "campaign_exposure_increasing_fills_so_far": 1.0,
        "campaign_reducing_fills_so_far": 0.0,
        "toxicity": 0.2,
        "markout_ema": -0.1,
        "depth_age_s": 0.05,
        "sigma_sq_raw": 2.0,
        "sigma_sq_blended": 2.0,
        "quote_horizon_s": 1.0,
        "kappa_used": 0.06,
        "microprice_shift_bps": 0.1,
        "l2_quote_flip_rate": 0.2,
        "l2_book_refresh_ratio": 0.3,
        "l2_book_cancel_ratio": 0.2,
        "l2_near_depth_total": 20.0,
        "final_pair_spread": 28.0,
        "final_quote_skew": 0.0,
        "allow_exposure_increase": 1.0,
        "exposure_increasing": 1.0,
        "side_adverse_pause": 0.0,
        "defense_guard": 0.0,
        "defense_pause": 0.0,
        "local_extreme_pause": 0.0,
    }


def test_scorer_is_shadow_only_and_emits_nine_probabilities(tmp_path: Path) -> None:
    artifact, report = _write_artifact(tmp_path, validation_passed=True)
    scorer = PlacementFillProbabilityScorer.load(
        artifact, report, tick_size=0.1, minimum_stage="validation"
    )
    surface = scorer.score(side="BUY", role="add", context=_context())

    assert len(surface.probabilities) == 9
    assert surface.prediction_only
    assert not surface.action_or_live_authorized
    assert not surface.active_order_keep_replace_included
    for horizon in (1_000, 5_000, 10_000):
        assert surface.probability("closer_1tick", horizon) >= surface.probability("current", horizon)
        assert surface.probability("current", horizon) >= surface.probability("farther_1tick", horizon)


def test_default_loader_requires_sealed_holdout(tmp_path: Path) -> None:
    artifact, report = _write_artifact(tmp_path, validation_passed=True)
    try:
        PlacementFillProbabilityScorer.load(artifact, report, tick_size=0.1)
    except RuntimeError as exc:
        assert "below required stage" in str(exc)
    else:
        raise AssertionError("development/validation evidence unlocked a live-facing scorer")


def test_context_missing_feature_fails_closed(tmp_path: Path) -> None:
    artifact, report = _write_artifact(tmp_path, validation_passed=True)
    scorer = PlacementFillProbabilityScorer.load(
        artifact, report, tick_size=0.1, minimum_stage="validation"
    )
    context = _context()
    del context["l2_book_refresh_ratio"]
    try:
        scorer.score(side="BUY", role="add", context=context)
    except ValueError as exc:
        assert "l2_book_refresh_ratio" in str(exc)
    else:
        raise AssertionError("missing causal feature was silently filled")
