"""Uniform valid-label loss weights must not restore sparse-day amplification."""
from copy import deepcopy
from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from research.families.f03_causal_13_head import ml_model as m
from research.families.f03_causal_13_head import label_equal_training as training
from research.families.f03_causal_13_head import label_equal_final as final


def contract():
    days = tuple(f"2025-08-{n:02}" for n in range(1, 5))
    policy = {
        "schema_version": "narrowgate.f03.valid_label_equal.v1",
        "decision_clock": "feature_ready_index", "half_life_days": "inf",
        "reference_date": "2025-08-04", "date_normalization": "none",
        "class_adjustment": "none",
        "previous_time_weight": {"formula": "exp(-lambda * days_ago / 30.44)",
                                 "lambda": 0.0, "reference_date": "2025-08-04"},
        "outcome_end_columns": {h: "label_outcome_end_" + h for h in m.MODEL_SPECS},
    }
    return m.TrainOnlySelectionContract(
        "test", "", "", "tardis_public_input", days[:2], days[2:3], days[3:],
        days, "", "", "", "", policy, ("mid",),
    )


def frame():
    timestamps = [pd.Timestamp("2025-08-01", tz="UTC")]
    timestamps += list(pd.date_range("2025-08-02", periods=100, freq="10s", tz="UTC"))
    timestamps += list(pd.date_range("2025-08-04", periods=20, freq="10s", tz="UTC"))
    index = pd.DatetimeIndex(timestamps)
    result = pd.DataFrame({"mid": np.arange(len(index), dtype=float), "sample_weight": 1.0}, index=index)
    for head, spec in m.MODEL_SPECS.items():
        result[spec[0]] = np.arange(len(index)) % 2
        result["label_outcome_end_" + head] = index + pd.Timedelta(seconds=1)
    return result


@pytest.mark.parametrize("head", m.MODEL_SPECS)
def test_sparse_day_gets_one_row_not_one_day_of_mass(head):
    c = contract()
    part = frame().iloc[:101]
    x, y, weights, _, report = m.prepare_time_weighted_xy(part, head, c, phase="fit")
    np.testing.assert_array_equal(weights, np.ones(101))
    assert len(x) == len(y) == 101
    assert [d["weight_sum"] for d in report["daily"]] == [1.0, 100.0]
    assert report["weighted_effective_rows_concentration_only"] == 101
    assert report["row_weight_ratio_max_min"] == 1
    assert report["effective_sample_size_is_independence_claim"] is False
    old = deepcopy(c.sample_weight_policy)
    old.update(schema_version="narrowgate.f03.time_half_life_daily.v2", date_normalization="effective_head_rows")
    _, _, old_weights, _, old_report = m.prepare_time_weighted_xy(
        part, head, replace(c, sample_weight_policy=old), phase="fit")
    assert old_weights.iloc[0] / old_weights.iloc[1] == pytest.approx(100)
    assert old_report["daily"][0]["weight_sum"] == pytest.approx(old_report["daily"][1]["weight_sum"])


def test_label_masks_endpoint_purge_and_unweighted_selection_preserved():
    c = contract()
    head = next(iter(m.MODEL_SPECS))
    part = frame().iloc[:101].copy()
    label, endpoint = m.MODEL_SPECS[head][0], "label_outcome_end_" + head
    part.loc[part.index[1], label] = np.nan
    part.loc[part.index[2], endpoint] = pd.NaT
    part.loc[part.index[3], endpoint] = pd.Timestamp("2025-08-03", tz="UTC")
    _, y, w, _, report = m.prepare_time_weighted_xy(part, head, c, phase="fit")
    assert len(y) == len(w) == 98
    assert report["missing_outcome_end_rows"] == report["outcome_outside_phase_days_rows"] == 1
    assert (w == 1).all()
    assert m.prepare_time_weighted_xy(frame().iloc[101:], head, c, phase="selection")[2] is None
    part.sample_weight = 2.0
    with pytest.raises(ValueError, match="declared time-only"):
        m.prepare_time_weighted_xy(part, head, c, phase="fit")


@pytest.mark.parametrize("key,value", [("date_normalization", "effective_head_rows"),
                                       ("half_life_days", 120), ("class_adjustment", "balanced")])
def test_policy_rejects_hidden_reweighting(key, value):
    c = contract()
    policy = dict(c.sample_weight_policy, **{key: value})
    with pytest.raises(ValueError, match="forbids"):
        m._validate_sample_weight_policy(policy, c.refit_days)


@pytest.mark.parametrize("override", [{"is_unbalance": True}, {"unbalance": True},
                                      {"scale_pos_weight": 2.0}, {"class_weight": "balanced"}])
def test_fitter_rejects_class_reweighting(override):
    f, c = frame(), contract()
    identity = {"feature_timestamp_semantics": "feature_ready_index", "feature_cutoff_semantics": "feature_ready_index",
                "public_input_contract": {"decision_time_semantics": "feature_ready_index"}}
    with pytest.raises(ValueError, match="classification reweighting"):
        m.train_one("touch_conditioned_up_probability_10000ms", f.iloc[:101], f.iloc[101:],
                    params_override=override, refit_df=f, selection_contract=c, feature_identity=identity)


def test_real_classifier_fit_and_refit_have_unit_weights_and_no_balance():
    f, c = frame(), contract()
    identity = {"feature_timestamp_semantics": "feature_ready_index", "feature_cutoff_semantics": "feature_ready_index",
                "public_input_contract": {"decision_time_semantics": "feature_ready_index"}}
    model, meta = m.train_one("touch_conditioned_up_probability_10000ms", f.iloc[:101], f.iloc[101:],
        params_override={"n_estimators": 3, "n_jobs": 1, "min_child_samples": 2, "verbosity": -1},
        refit_df=f, selection_contract=c, feature_identity=identity)
    assert meta["selection_fit_params"]["is_unbalance"] is False
    assert meta["refit_fit_params"]["is_unbalance"] is False
    assert meta["class_imbalance_adjustment"]["is_unbalance"] is False
    assert meta["per_head_time_weighting"]["refit"]["weight_max"] == 1
    assert np.isfinite(model.predict_proba(f[["mid"]])).all()


def test_candidate_policy_and_parameters_do_not_mutate_parent():
    original = contract().sample_weight_policy
    old = dict(original, schema_version="narrowgate.f03.time_half_life_daily.v2",
               date_normalization="effective_head_rows")
    new = training.equal_policy(old)
    assert old["date_normalization"] == "effective_head_rows"
    assert new == original
    params = {"is_unbalance": True, "n_jobs": 6, "seed": 42, "num_leaves": 127}
    assert training.head_params(params, True) == dict(params, is_unbalance=False, n_jobs=4)
    assert params["is_unbalance"] is True and params["n_jobs"] == 6
    with pytest.raises(ValueError, match="alias"):
        training.head_params(dict(params, scale_pos_weight=2), True)


def test_new_final_identity_cannot_alias_prior_model_or_extend_calendar(monkeypatch):
    spec = dict(experiment_id=final.EXPERIMENT, baseline_id=final.final.BASELINE,
        parent_id='final-000-2026-05-28', arm_id=final.CANDIDATE,
        start_ns=final.final.START_NS, end_ns=final.final.START_NS + 2 * final.final.DAY_NS,
        input_manifest_id='1' * 64, model_identity='2' * 64, release_identity='3' * 64,
        seed_pair={'rng_seed': 42, 'latency_seed': 59})
    assert final.identity(spec) == spec
    for change in ({'arm_id': 'F03_BTCUSDC_TRAIN300_13HEAD_HINF_20261014'},
                   {'baseline_id': 'B0_RESPONSE_20261007'}, {'end_ns': final.final.END_NS + 1}):
        with pytest.raises(ValueError):
            final.identity(dict(spec, **change))
    calls = []
    monkeypatch.setattr(final.final, 'execute_cell', lambda prepared, **kwargs: calls.append(kwargs))
    final.execute_cell('unchanged-market', spec=spec)
    assert calls == [dict(spec=spec, experiment=final.EXPERIMENT, arms=final.ARMS)]
