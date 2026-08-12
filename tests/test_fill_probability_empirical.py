import json

import numpy as np

from research.families.f02_empirical_p3_touch.fill_probability import FillProbabilityModel


def test_empirical_fill_probability_round_trip(tmp_path):
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
    loaded = FillProbabilityModel.load(path)
    assert loaded.schema_version == "narrowgate_p3_touch_calibration.v2"
    assert loaded.model_type == "empirical_survival"
    assert loaded.metadata["horizon_s"] == 10.0
    assert loaded.semantic_identity() == {
        "event_type": "touch",
        "horizon_s": 10.0,
        "distance_unit": "USDC_per_BTC",
        "artifact_sha256": loaded.artifact_sha256,
    }
    assert np.allclose(loaded.prob([0.1, 1.0, 4.0]), [0.8, 0.5, 0.05])
    assert loaded.optimal_delta() > 0.0
    assert loaded.effective_kappa() > 0.0
    payload = json.loads(path.read_text())
    assert payload["delta_star"] > 0.0
    assert payload["kappa_eff"] > 0.0
    assert payload["metadata"]["event_type"] == "touch"


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
    assert path.read_bytes() == before


def test_legacy_su_artifact_still_loads(tmp_path):
    path = tmp_path / "fill_prob_params.json"
    path.write_text(json.dumps({"xi": 0.0, "lam": 1.0, "gamma": 0.0, "delta0": 1.0}))
    model = FillProbabilityModel.load(path)
    assert model.model_type == "su_johnson"
    assert model.schema_version == "legacy_su_johnson.v1"
