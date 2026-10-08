import pytest

from research.families.f05_fill_quality_quote_ev.quote_ev import QuoteEVPrediction


def test_production_prediction_keeps_conditional_and_joint_units_separate():
    import numpy as np
    from types import SimpleNamespace
    from research.families.f05_fill_quality_quote_ev.quote_ev import QuoteEVModel

    def head(value):
        return SimpleNamespace(predict=lambda row: np.asarray([value]))

    model = QuoteEVModel(
        fill_prob_model=head(0.2),
        bucket_models={h: head([1.0]) for h in (1, 5, 30)},
        extreme_adverse_model=head(0.8),
        fill_prob_features=["x"],
        bucket_features={h: ["x"] for h in (1, 5, 30)},
        bucket_values={h: [5.0] for h in (1, 5, 30)},
        bucket_classes={h: [0] for h in (1, 5, 30)},
        extreme_adverse_features=["x"],
        missing_policy="reject",
    )
    result = model.predict({"x": 1.0})
    assert result.lifecycle_fill_probability == 0.2
    assert result.maker_markout_bps_given_fill_30000ms == 5.0
    assert result.expected_maker_markout_bps_per_opportunity_30s == 1.0
    assert result.extreme_adverse_probability_given_fill_30000ms == 0.8
    assert result.fill_and_extreme_adverse_probability_30000ms == 0.2 * 0.8
    for retired in ("toxic_30s", "fill_prob", "toxic_given_fill_30s",
                    "extreme_adverse_given_fill", "fill_markout_30s"):
        with pytest.raises(AttributeError):
            getattr(result, retired)


def test_only_explicit_opportunity_markout_is_readable() -> None:
    prediction = QuoteEVPrediction(
        expected_maker_markout_bps_per_opportunity_30s=0.125
    )

    assert prediction.expected_maker_markout_bps_per_opportunity_30s == pytest.approx(0.125)
    with pytest.raises(AttributeError):
        _ = prediction.ev_30s


def test_old_prediction_constructor_is_rejected() -> None:
    with pytest.raises(TypeError):
        QuoteEVPrediction(ev_30s=0.125)


def test_shadow_report_requires_current_column_and_preserves_unknown() -> None:
    import pandas as pd
    from research.families.f05_fill_quality_quote_ev.quote_ev_shadow_eval import (
        PREDICTED_VALUE_COLUMN,
        _predicted_value,
    )

    values = _predicted_value(pd.DataFrame({PREDICTED_VALUE_COLUMN: [0.125, None]}))
    assert values.iloc[0] == 0.125
    assert pd.isna(values.iloc[1])
    with pytest.raises(KeyError):
        _predicted_value(pd.DataFrame({"pred_ev_30s": [0.125]}))


def test_p3_old_import_path_is_absent() -> None:
    import importlib.util

    assert importlib.util.find_spec(
        "research.families.f02_empirical_p3_touch.fill_probability"
    ) is None


def test_weighted_mid_proxy_has_no_retired_alias_or_dual_output():
    import strategy.quote_core as core

    assert not hasattr(core, "microprice_from_book")
    assert core.weighted_mid_proxy_from_book([(99.0, 3.0)], [(101.0, 1.0)]) == 100.5
    assert core.reservation_price(100.0, 0.01, 0.1, 4.0, 5.0) == 99.98
    with pytest.raises(TypeError):
        core.reservation_price(mid=100.0, q=0.01, gamma=0.1, sigma_sq=4.0)
