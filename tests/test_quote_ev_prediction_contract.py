import pytest

from research.families.f05_fill_quality_quote_ev.quote_ev import QuoteEVPrediction


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
