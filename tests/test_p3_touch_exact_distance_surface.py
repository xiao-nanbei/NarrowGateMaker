from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from research.families.f02_empirical_p3_touch.audit import (
    p3_touch_exact_distance_surface as exact_surface,
)

DAY = "2026-06-19"
OTHER_DAY = "2026-06-20"
DECISION_TS_MS = int(
    datetime(2026, 6, 19, 0, 0, 10, tzinfo=timezone.utc).timestamp() * 1_000
)
TICK_SIZE = "0.1"
BEST_BID_TICKS = 2_000
BEST_ASK_TICKS = 2_001


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_sha(payload: dict[str, Any], field: str) -> str:
    normalized = dict(payload)
    normalized.pop(field, None)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_canonical_json(path: Path, payload: dict[str, Any]) -> None:
    field = "canonical_spec_identity_sha256"
    payload[field] = _canonical_sha(payload, field)
    _write_json(path, payload)


class _FakeBooster:
    loaded_paths: list[str] = []

    def __init__(self, *, model_file: str) -> None:
        self.loaded_paths.append(model_file)


class _FakeConditionalTouchModel:
    calls: list[dict[str, Any]] = []

    def __init__(self, booster, calibration, feature_contract) -> None:
        self.booster = booster
        self.calibration = dict(calibration)
        self.feature_contract = dict(feature_contract)

    def predict(self, context, *, side, distances, row_indices):
        self.calls.append(
            {
                "side": side,
                "distances": np.asarray(distances, dtype=np.float64).copy(),
                "row_indices": np.asarray(row_indices, dtype=np.int64).copy(),
                "start_ts_ms": np.asarray(context["start_ts_ms"]).copy(),
            }
        )
        offset = 0.10 if side == "BUY" else 0.50
        return offset + np.asarray(distances, dtype=np.float64) / 1_000.0


@dataclass
class _Artifacts:
    v4_1_ref: dict[str, str]
    day_bindings: dict[str, dict[str, Any]]
    paths: dict[str, Path]
    hashes: dict[str, str]

    def load(self) -> exact_surface.P3TouchExactDistanceSurface:
        return exact_surface.P3TouchExactDistanceSurface(
            v4_1_spec=self.v4_1_ref,
            day_bindings=self.day_bindings,
            tick_size=TICK_SIZE,
        )


def _build_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    feature_ready_ts_ms: int = DECISION_TS_MS - 1,
    context_best_bid: float = 200.0,
    context_best_ask: float = 200.1,
    duplicate_oof_day: bool = False,
) -> _Artifacts:
    _FakeBooster.loaded_paths = []
    _FakeConditionalTouchModel.calls = []
    monkeypatch.setattr(exact_surface.lgb, "Booster", _FakeBooster)
    monkeypatch.setattr(
        exact_surface,
        "ConditionalTouchModel",
        _FakeConditionalTouchModel,
    )

    model_paths: dict[str, Path] = {}
    calibration_paths: dict[str, Path] = {}
    fold_artifacts: dict[str, dict[str, Any]] = {}
    for index, fold_id in enumerate(("fold_a", "fold_b"), start=1):
        fold_dir = tmp_path / fold_id
        fold_dir.mkdir()
        model_path = fold_dir / "model.txt"
        model_path.write_text(f"fake fold model {index}\n", encoding="utf-8")
        calibration_path = fold_dir / "positive_platt.json"
        _write_json(
            calibration_path,
            {"intercept": float(index), "slope": 1.0 + 0.1 * index},
        )
        model_paths[fold_id] = model_path
        calibration_paths[fold_id] = calibration_path
        fold_artifacts[fold_id] = {
            "model": {"path": str(model_path), "sha256": _sha256(model_path)},
            "calibration": {
                "path": str(calibration_path),
                "sha256": _sha256(calibration_path),
            },
        }

    fold_b_days = [OTHER_DAY]
    if duplicate_oof_day:
        fold_b_days.append(DAY)
    v4_spec_path = tmp_path / "v4_spec.json"
    v4_spec = {
        "identity": exact_surface.V4_IDENTITY,
        "estimand": {
            "event_type": "touch",
            "horizon_s": 10.0,
            "distance_unit": "USDC_per_BTC",
        },
        "chronological_oof": {
            "folds": [
                {"fold_id": "fold_a", "test_days": [DAY]},
                {"fold_id": "fold_b", "test_days": fold_b_days},
            ]
        },
        "model": {
            "feature_contract": {
                "horizon_s": 10.0,
                "calm_upper": 0.75,
                "shock_lower": 1.5,
            }
        },
        "permissions": {"action_authority": False, "live_authority": False},
    }
    _write_canonical_json(v4_spec_path, v4_spec)

    v4_report_path = tmp_path / "v4_report.json"
    v4_report = {
        "identity": exact_surface.V4_IDENTITY,
        "spec": {"path": str(v4_spec_path), "sha256": _sha256(v4_spec_path)},
        "fold_artifacts": fold_artifacts,
        "permissions": {"action_authority": False, "live_authority": False},
    }
    _write_json(v4_report_path, v4_report)

    v4_1_spec_path = tmp_path / "v4_1_spec.json"
    v4_1_spec = {
        "identity": exact_surface.V4_1_IDENTITY,
        "predecessor_identity": exact_surface.V4_IDENTITY,
        "identities": {
            "original_v4_spec": {
                "path": str(v4_spec_path),
                "sha256": _sha256(v4_spec_path),
            },
            "original_v4_report": {
                "path": str(v4_report_path),
                "sha256": _sha256(v4_report_path),
            },
        },
        "permissions": {"action_authority": False, "live_authority": False},
    }
    _write_canonical_json(v4_1_spec_path, v4_1_spec)

    context_path = tmp_path / "context.npz"
    np.savez_compressed(
        context_path,
        start_ts_ms=np.asarray([DECISION_TS_MS], dtype=np.int64),
        feature_ready_ts_ms=np.asarray([feature_ready_ts_ms], dtype=np.int64),
        best_bid=np.asarray([context_best_bid], dtype=np.float64),
        best_ask=np.asarray([context_best_ask], dtype=np.float64),
        mid=np.asarray([(context_best_bid + context_best_ask) / 2.0]),
        spread=np.asarray([context_best_ask - context_best_bid]),
        fast_sigma=np.asarray([2.0]),
        slow_sigma=np.asarray([3.0]),
    )
    hashes = {
        "v4_1": _sha256(v4_1_spec_path),
        "v4": _sha256(v4_spec_path),
        "report": _sha256(v4_report_path),
        "context": _sha256(context_path),
        "model": _sha256(model_paths["fold_a"]),
        "calibration": _sha256(calibration_paths["fold_a"]),
    }
    return _Artifacts(
        v4_1_ref={"path": str(v4_1_spec_path), "sha256": hashes["v4_1"]},
        day_bindings={
            DAY: {
                "source": "native",
                "fold_id": "fold_a",
                "context": {
                    "path": str(context_path),
                    "sha256": hashes["context"],
                },
            }
        },
        paths={
            "v4_1": v4_1_spec_path,
            "v4": v4_spec_path,
            "report": v4_report_path,
            "context": context_path,
            "model": model_paths["fold_a"],
            "unused_model": model_paths["fold_b"],
            "calibration": calibration_paths["fold_a"],
        },
        hashes=hashes,
    )


def _valid_query(surface: exact_surface.P3TouchExactDistanceSurface, **overrides):
    values = {
        "day": DAY,
        "decision_ts_ms": DECISION_TS_MS,
        "best_bid_ticks": BEST_BID_TICKS,
        "best_ask_ticks": BEST_ASK_TICKS,
        "candidate_price_ticks": {
            "BUY": [BEST_BID_TICKS - 7],
            "SELL": [BEST_ASK_TICKS + 13],
        },
    }
    values.update(overrides)
    return surface.query(**values)


def test_exact_continuous_distance_queries_each_side_without_averaging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    artifacts = _build_artifacts(tmp_path, monkeypatch)
    surface = artifacts.load()
    result = _valid_query(surface)

    assert result["supported"] is True
    assert result["fallback_reason"] is None
    assert [call["side"] for call in _FakeConditionalTouchModel.calls] == [
        "BUY",
        "SELL",
    ]
    np.testing.assert_array_equal(
        _FakeConditionalTouchModel.calls[0]["row_indices"],
        [0],
    )
    np.testing.assert_allclose(
        _FakeConditionalTouchModel.calls[0]["distances"],
        [0.7],
        rtol=0.0,
        atol=1e-12,
    )
    np.testing.assert_allclose(
        _FakeConditionalTouchModel.calls[1]["distances"],
        [1.3],
        rtol=0.0,
        atol=1e-12,
    )
    assert result["predictions"]["BUY"][0]["distance_usdc_per_btc"] == pytest.approx(
        0.7
    )
    assert result["predictions"]["SELL"][0]["distance_usdc_per_btc"] == pytest.approx(
        1.3
    )
    assert result["predictions"]["BUY"][0]["probability"] == pytest.approx(0.1007)
    assert result["predictions"]["SELL"][0]["probability"] == pytest.approx(0.5013)
    assert result["source"] == "native"
    assert result["fold_id"] == "fold_a"
    assert result["artifact_hashes"] == {
        "v4_1_spec_sha256": artifacts.hashes["v4_1"],
        "predecessor_v4_spec_sha256": artifacts.hashes["v4"],
        "predecessor_v4_report_sha256": artifacts.hashes["report"],
        "context_npz_sha256": artifacts.hashes["context"],
        "fold_model_sha256": artifacts.hashes["model"],
        "fold_positive_platt_sha256": artifacts.hashes["calibration"],
    }
    assert result["permissions"] == {
        "prediction_research_only": True,
        "action_authority": False,
        "live_authority": False,
    }
    assert len(_FakeBooster.loaded_paths) == 2


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        (
            {"decision_ts_ms": DECISION_TS_MS + 100},
            "decision_not_canonical_10s_boundary",
        ),
        (
            {"decision_ts_ms": DECISION_TS_MS + 10_000},
            "context_start_ts_exact_match_missing",
        ),
        (
            {"best_bid_ticks": BEST_BID_TICKS - 1},
            "caller_context_bbo_tick_mismatch",
        ),
        (
            {"best_bid_ticks": 2_001, "best_ask_ticks": 2_001},
            "caller_bbo_not_valid_integer_ticks",
        ),
    ],
)
def test_clock_and_caller_bbo_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, Any],
    reason: str,
):
    surface = _build_artifacts(tmp_path, monkeypatch).load()
    result = _valid_query(surface, **overrides)

    assert result["supported"] is False
    assert result["fallback_required"] is True
    assert result["fallback_reason"] == reason
    assert result["predictions"] == {}
    assert _FakeConditionalTouchModel.calls == []


def test_feature_ready_after_decision_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    surface = _build_artifacts(
        tmp_path,
        monkeypatch,
        feature_ready_ts_ms=DECISION_TS_MS + 1,
    ).load()
    result = _valid_query(surface)

    assert result["fallback_reason"] == "feature_ready_after_decision"
    assert result["predictions"] == {}


def test_context_bbo_must_be_tick_aligned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    surface = _build_artifacts(
        tmp_path,
        monkeypatch,
        context_best_bid=200.05,
    ).load()
    result = _valid_query(surface)

    assert result["fallback_reason"] == "context_bbo_not_valid_integer_ticks"
    assert result["predictions"] == {}


@pytest.mark.parametrize(
    ("candidates", "reason"),
    [
        ({"BUY": [1_993.0]}, "candidate_price_not_integer_tick"),
        ({"BUY": [BEST_ASK_TICKS]}, "candidate_gtx_invalid"),
        ({"SELL": [BEST_BID_TICKS]}, "candidate_gtx_invalid"),
        ({"BUY": [BEST_BID_TICKS - 4]}, "distance_outside_strict_support"),
        ({"SELL": [BEST_ASK_TICKS + 1_201]}, "distance_outside_strict_support"),
    ],
)
def test_tick_gtx_and_support_fail_closed_without_clamping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    candidates: dict[str, list[Any]],
    reason: str,
):
    surface = _build_artifacts(tmp_path, monkeypatch).load()
    result = _valid_query(surface, candidate_price_ticks=candidates)

    assert result["supported"] is False
    assert result["fallback_reason"] == reason
    assert result["predictions"] == {}
    assert _FakeConditionalTouchModel.calls == []


@pytest.mark.parametrize("artifact_name", ["v4_1", "context", "unused_model", "calibration"])
def test_all_frozen_artifacts_are_hash_verified_before_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_name: str,
):
    artifacts = _build_artifacts(tmp_path, monkeypatch)
    with artifacts.paths[artifact_name].open("ab") as handle:
        handle.write(b"tamper")

    with pytest.raises(exact_surface.ArtifactIntegrityError, match="hash mismatch"):
        artifacts.load()


def test_each_day_must_have_one_unique_oof_fold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    artifacts = _build_artifacts(
        tmp_path,
        monkeypatch,
        duplicate_oof_day=True,
    )

    with pytest.raises(
        exact_surface.ArtifactIntegrityError,
        match="belongs to multiple conditional P3 OOF folds",
    ):
        artifacts.load()
