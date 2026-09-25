"""Current model protocol and retired-interface rejection with controlled models."""
import json
from types import SimpleNamespace

import numpy as np
import pytest

from data.tardis_input import CONTRACT
from features.quote_ev import feature_array
from research.families.f05_fill_quality_quote_ev.quote_ev import MODEL_SCHEMA, QuoteEVModel, quote_side_model_names


@pytest.mark.parametrize("value", [None, "bad", np.nan, np.inf, -np.inf])
def test_missing_values_are_not_neutral(value):
    assert np.isnan(feature_array({"x": value}, ["x"], missing_policy="native_nan")).all()
    with pytest.raises(ValueError, match="feature"):
        feature_array({"x": value}, ["x"], missing_policy="reject")
    with pytest.raises(ValueError, match="unsupported"):
        feature_array({"x": value}, ["x"], missing_policy="legacy_zero")


@pytest.mark.parametrize("mutation", [None, "missing_meta", "wrong_source", "wrong_columns", "missing_policy"])
def test_model_loading_and_prediction(tmp_path, monkeypatch, mutation):
    import lightgbm
    identity = dict(input_contract_id=CONTRACT, observation_contract_id="observations",
                    feature_contract_id="features", source_manifest_sha256="source",
                    training_contract_id="training", label_contract_id="opportunity-labels")
    names = quote_side_model_names("bid")
    all_names = [names["fill_prob"], names["extreme_adverse"], *names["markout_buckets"].values()]
    for name in all_names:
        (tmp_path/(name+".txt")).write_text("synthetic, not trained")
        meta = {**identity, "schema": MODEL_SCHEMA, "feature_cols": ["ordinary_quantity"], "missing_policy": "native_nan",
                "bucket_values": [-1., 1.], "classes": [0, 1]}
        if name == all_names[0]:
            if mutation == "missing_meta":
                continue
            if mutation == "wrong_source":
                meta["source_manifest_sha256"] = "other"
            if mutation == "wrong_columns":
                meta["feature_cols"] = ["other"]
            if mutation == "missing_policy":
                meta.pop("missing_policy")
        (tmp_path/(name+"_meta.json")).write_text(json.dumps(meta))
    seen = []

    class Booster:
        def __init__(self, model_file):
            self.bucket = any(name in model_file for name in names["markout_buckets"].values())

        def feature_name(self):
            return ["ordinary_quantity"]

        def predict(self, row):
            seen.append(row)
            return [[.5, .5]] if self.bucket else [.5]

    monkeypatch.setattr(lightgbm, "Booster", Booster)
    if mutation:
        with pytest.raises(ValueError):
            QuoteEVModel.load(tmp_path, input_identity=identity)
    else:
        model = QuoteEVModel.load(tmp_path, input_identity=identity)
        model.predict({})
        assert seen and all(np.isnan(row).all() for row in seen)
        from data.observation import FeatureFrame
        from decimal import Decimal
        frame = FeatureFrame(CONTRACT, "observations", "features", 20, 19,
                             (("ordinary_quantity", Decimal(10)),), (),
                             (("ordinary_quantity", False),))
        seen.clear()
        model.predict_frame(frame, decision_ns=20)
        assert seen and all(np.isnan(row).all() for row in seen)
        with pytest.raises(ValueError, match="future"):
            model.predict_frame(frame, decision_ns=19)
    with pytest.raises(ValueError, match="identity required"):
        QuoteEVModel.load(tmp_path)


def test_no_implicit_feature_schema():
    with pytest.raises(ValueError, match="explicit feature"):
        QuoteEVModel(fill_prob_model=object())


@pytest.mark.parametrize('bad_head', ['fill', 'bucket', 'adverse'])
def test_nonfinite_model_output_remains_unknown(bad_head):
    def head(value):
        return SimpleNamespace(predict=lambda row: np.array([value]))

    model = QuoteEVModel(fill_prob_model=head(np.nan if bad_head == 'fill' else .5),
        bucket_models={h: head(np.nan if bad_head == 'bucket' else .5) for h in (1, 5, 30)},
        extreme_adverse_model=head(np.nan if bad_head == 'adverse' else .5),
        fill_prob_features=['x'], bucket_features={h: ['x'] for h in (1, 5, 30)},
        extreme_adverse_features=['x'], bucket_values={h: [-1., 1.] for h in (1, 5, 30)},
        bucket_classes={h: [0] for h in (1, 5, 30)}, missing_policy='native_nan')
    with pytest.raises(ValueError, match='nonfinite quote EV'):
        model.predict({'x': 1.})


def test_historical_entrypoints_refuse_implicit_legacy_inputs(tmp_path):
    from research.families.f05_fill_quality_quote_ev import quote_ev_shadow_eval
    from research.families.f06_placement_fill_cif.audit.placement_fill_panel import main
    assert not hasattr(quote_ev_shadow_eval, 'run')
    assert not hasattr(QuoteEVModel, 'load_legacy')
    with pytest.raises(ValueError, match="legacy-input"):
        main(["--config", str(tmp_path/"absent"), "--feature-context-dir", str(tmp_path),
              "--latency-telemetry", str(tmp_path/"absent")])
