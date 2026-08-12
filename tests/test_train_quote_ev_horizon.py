import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.train_quote_ev import _label_horizon_valid


def test_label_horizon_uses_configured_grid_gap() -> None:
    frame = pd.DataFrame(
        {
            "feature_dt": pd.date_range(
                "2026-07-01T00:00:00Z",
                periods=5,
                freq="10s",
            )
        }
    )

    permissive = _label_horizon_valid(frame, max_gap_s=15.0)
    strict = _label_horizon_valid(frame, max_gap_s=5.0)

    assert permissive.tolist() == [True, True, False, False, False]
    assert not strict.any()


def test_label_horizon_refuses_unvalidated_default() -> None:
    with pytest.raises(ValueError, match="refusing to treat every horizon as valid"):
        _label_horizon_valid(pd.DataFrame({"order_id": ["1"]}), max_gap_s=15.0)
