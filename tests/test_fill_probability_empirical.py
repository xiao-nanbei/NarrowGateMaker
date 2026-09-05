import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from research.families.f02_empirical_p3_touch.fill_probability import FillProbabilityModel

PUBLIC_P3 = (
    Path(__file__).resolve().parents[1]
    / "examples/public_dry_run_model_bundle/fill_prob_params.json"
)


def test_empirical_fill_probability_round_trip(tmp_path, monkeypatch):
    model = FillProbabilityModel(
        model_type="empirical_survival",
        delta_grid=[0.1, 1.0, 2.0, 4.0],
        probability_grid=[0.8, 0.5, 0.25, 0.05],
        schema_version="narrowgate_p3_touch_calibration.v2",
        metadata={
            "event_type": "touch",
            "horizon_s": 10.0,
            "distance_unit": "USDC_per_BTC",
            "fit_days": ["2026-01-01"],
        },
    )
    path = tmp_path / "fill_prob_params.json"
    model.save(path)
    original_read = Path.read_bytes
    raw = path.read_bytes()
    reads = 0

    def read_and_replace(candidate):
        nonlocal reads
        reads += 1
        observed = original_read(candidate)
        candidate.write_bytes(b"replaced after this read")
        return observed

    monkeypatch.setattr(Path, "read_bytes", read_and_replace)
    loaded = FillProbabilityModel.load(path)
    assert reads == 1
    assert loaded.artifact_sha256 == hashlib.sha256(raw).hexdigest()
    assert loaded.artifact_path == path.resolve()
    detached = FillProbabilityModel.from_bytes(raw, require_live_compatible=True)
    assert detached.artifact_path is None
    assert detached.semantic_identity() == loaded.semantic_identity()
    assert reads == 1
    monkeypatch.setattr(Path, "read_bytes", original_read)
    path.write_bytes(raw)
    assert loaded.schema_version == "narrowgate_p3_touch_calibration.v2"
    assert loaded.model_type == "empirical_survival"
    assert loaded.metadata["horizon_s"] == 10.0
    assert loaded.semantic_identity() == {
        "event_type": "touch",
        "horizon_s": 10.0,
        "distance_origin": "same_side_best_bid_or_ask_at_window_start",
        "distance_unit": "USDC_per_BTC",
        "side": "pooled_buy_sell",
        "queue_included": False,
        "artifact_sha256": loaded.artifact_sha256,
    }
    assert np.allclose(loaded.prob([0.1, 1.0, 4.0]), [0.8, 0.5, 0.05])
    assert loaded.optimal_delta() > 0.0
    assert loaded.effective_kappa() > 0.0
    payload = json.loads(path.read_text())
    assert payload["delta_star"] > 0.0
    assert payload["kappa_eff"] > 0.0
    assert payload["metadata"]["event_type"] == "touch"
    assert payload["metadata"]["distance_origin"] == (
        "same_side_best_bid_or_ask_at_window_start"
    )
    assert payload["metadata"]["side"] == "pooled_buy_sell"
    assert payload["metadata"]["queue_included"] is False


def test_frozen_v2_touch_identity_is_inferred_without_rewriting(tmp_path):
    path = tmp_path / "fill_prob_params.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "narrowgate_p3_touch_calibration.v2",
                "model_type": "empirical_survival",
                "delta_grid": [0.1, 1.0, 2.0],
                "probability_grid": [0.8, 0.4, 0.1],
                "metadata": {
                    "horizon_s": 10.0,
                    "distance_unit": "USDC_per_BTC",
                    "touch_source": "side-correct aggTrades against causal BBO",
                    "queue_included": False,
                },
            }
        ),
        encoding="utf-8",
    )
    before = path.read_bytes()
    model = FillProbabilityModel.load(path)

    assert model.semantic_identity()["event_type"] == "touch"
    assert model.semantic_identity()["distance_origin"] == (
        "same_side_best_bid_or_ask_at_window_start"
    )
    assert model.semantic_identity()["side"] == "pooled_buy_sell"
    assert model.semantic_identity()["queue_included"] is False
    assert path.read_bytes() == before


def test_legacy_su_artifact_still_loads(tmp_path):
    path = tmp_path / "fill_prob_params.json"
    path.write_text(json.dumps({"xi": 0.0, "lam": 1.0, "gamma": 0.0, "delta0": 1.0}))
    model = FillProbabilityModel.load(path)
    assert model.model_type == "su_johnson"
    assert model.schema_version == "legacy_su_johnson.v1"
    payload = json.loads(path.read_bytes()) | {"kappa_eff": 1.0, "delta_star": 1.0}
    with pytest.raises(ValueError, match="only empirical P3"):
        FillProbabilityModel.from_bytes(json.dumps(payload).encode(), require_live_compatible=True)


def test_public_p3_fixture_remains_offline_only():
    assert FillProbabilityModel.load(PUBLIC_P3).metadata["authority"] == "public_dry_run_only"
    with pytest.raises(ValueError, match="public_dry_run_only"):
        FillProbabilityModel.load(PUBLIC_P3, require_live_compatible=True)


@pytest.mark.parametrize("field", ("kappa_eff", "delta_star"))
@pytest.mark.parametrize("value", (None, 0.0, -1.0, float("nan"), float("inf"), False, "invalid"))
def test_live_p3_rejects_invalid_stored_scalars(field, value):
    payload = json.loads(PUBLIC_P3.read_bytes())
    payload["metadata"].pop("authority")
    payload[field] = value
    raw = json.dumps(payload).encode()
    assert FillProbabilityModel.from_bytes(raw).model_type == "empirical_survival"
    with pytest.raises(ValueError, match=f"{field} must be positive and finite"):
        FillProbabilityModel.from_bytes(raw, require_live_compatible=True)


def test_live_p3_retains_empirical_semantic_checks():
    payload = json.loads(PUBLIC_P3.read_bytes())
    payload["metadata"].pop("authority")
    payload["metadata"]["horizon_s"] = 9.0
    with pytest.raises(ValueError, match="horizon_s must equal 10"):
        FillProbabilityModel.from_bytes(json.dumps(payload).encode(), require_live_compatible=True)


@pytest.mark.parametrize("field", ("delta_grid", "probability_grid"))
def test_live_p3_rejects_nonfinite_empirical_grid(field):
    payload = json.loads(PUBLIC_P3.read_bytes())
    payload["metadata"].pop("authority")
    payload[field][1] = float("nan")
    with pytest.raises(ValueError, match="empirical grids must be finite"):
        FillProbabilityModel.from_bytes(json.dumps(payload).encode(), require_live_compatible=True)
