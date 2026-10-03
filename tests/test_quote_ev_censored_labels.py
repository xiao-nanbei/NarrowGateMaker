import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.train_quote_ev import build_labels, _train_lgbm


def _orders():
    return pd.DataFrame({"day": ["2026-07-01"] * 2, "order_id": [1, 2], "side": ["BUY"] * 2, "inventory": [0.0, 0.0]})


def test_missing_horizon_never_falls_back_to_ev_or_zero():
    fills = pd.DataFrame({"day": ["2026-07-01"], "order_id": [1], "side": ["BUY"], "ev_30s": [100.0], "markout_30s": [3.0]})
    labels = build_labels(_orders(), fills, max_inventory=1)
    filled = labels.iloc[0]
    assert np.isnan(filled["label_bid_fill_markout_1s"])
    assert np.isnan(filled["label_bid_markout_bucket_1s"])
    assert np.isnan(filled["label_bid_toxic_1s"])
    assert filled["label_bid_fill_markout_30s"] == 3
    assert labels.iloc[1]["label_bid_filled"] == 0
    assert np.isnan(labels.iloc[1]["label_bid_fill_markout_30s"])
    assert "label_bid_fill_markout_30s_actual_end_ms" not in labels


def test_partial_fill_unknown_cannot_be_averaged_away_and_endpoint_is_preserved(tmp_path):
    fills = pd.DataFrame({"day": ["2026-07-01"] * 2, "order_id": [1, 1], "side": ["BUY"] * 2,
                          "fill_qty": [1.0, 2.0], "markout_1s": [3.0, np.nan], "markout_5s": [2.0, 4.0],
                          "markout_30s": [np.nan, np.nan], "toxic_30s": [None, "False"],
                          "markout_5s_actual_end_ms": [10000, 20000]})
    labels = build_labels(_orders(), fills, max_inventory=1)
    row = labels.iloc[0]
    assert np.isnan(row["label_bid_fill_markout_1s"])
    assert row["label_bid_fill_markout_1s_known_fill_count"] == 1
    assert row["label_bid_fill_markout_1s_fill_count"] == 2
    assert row["label_bid_fill_markout_5s"] == pytest.approx(10 / 3)
    assert row["label_bid_fill_markout_5s_actual_end_ms"] == 20000
    assert np.isnan(row["label_bid_toxic_30s"])
    assert np.isnan(row["label_bid_extreme_adverse_any"])
    path = tmp_path / "labels.parquet"
    labels.to_parquet(path)
    assert pd.isna(pd.read_parquet(path).iloc[0]["label_bid_markout_bucket_1s"])


def test_actual_training_entry_rejects_unknown_only_split(tmp_path):
    unknown = pd.DataFrame({"feature": [1.0], "label": [np.nan]})
    with pytest.raises(ValueError, match="unknown_label_rows_excluded.*1"):
        _train_lgbm("test", unknown, unknown, ["feature"], "label", False, tmp_path)


def test_actual_training_entry_filters_per_head_before_model_fit(monkeypatch, tmp_path):
    import lightgbm

    class StopAfterVerifiedFit(Exception):
        pass

    def inspect_fit(self, *, X, y, eval_set, callbacks):
        assert X["feature"].tolist() == [2.0, 3.0]
        assert y.tolist() == [0.0, 4.0]
        assert eval_set[0][1].tolist() == [4.0]
        raise StopAfterVerifiedFit

    monkeypatch.setattr(lightgbm.LGBMRegressor, "fit", inspect_fit)
    train = pd.DataFrame({"feature": [1., 2., 3.], "label": [np.nan, 0., 4.]})
    valid = train.iloc[[0, 2]]
    with pytest.raises(StopAfterVerifiedFit):
        _train_lgbm("test", train, valid, ["feature"], "label", False, tmp_path)
