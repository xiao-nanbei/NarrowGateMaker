import pytest

from research.families.f05_fill_quality_quote_ev.quote_ev import QuoteEVPrediction


def test_ev_30s_is_a_read_only_compatibility_alias() -> None:
    prediction = QuoteEVPrediction(
        expected_maker_markout_bps_per_opportunity_30s=0.125
    )

    assert prediction.ev_30s == pytest.approx(0.125)
    with pytest.raises(AttributeError):
        prediction.ev_30s = 0.5
