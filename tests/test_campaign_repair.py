import json
import math

import pytest

from strategy.campaign_repair import (
    CampaignRepairModel,
    CampaignRepairProbabilityHistory,
    CampaignRepairSideModel,
    build_campaign_repair_features,
)


def _features(inventory: float, *, microprice_shift_bps: float, markout_ema: float):
    return build_campaign_repair_features(
        inventory=inventory,
        order_size=0.001,
        max_inventory=0.026,
        campaign_age_s=120.0,
        campaign_max_abs_qty_so_far=abs(inventory),
        campaign_pnl_so_far=-0.25,
        campaign_adverse_excursion_so_far=-0.50,
        campaign_exposure_increasing_fills_so_far=3,
        campaign_reducing_fills_so_far=1,
        l2_book_refresh_ratio=0.30,
        l2_book_cancel_ratio=0.10,
        l2_quote_flip_rate=0.05,
        near_depth_total=2.0,
        microprice_shift_bps=microprice_shift_bps,
        toxicity=0.40,
        markout_ema=markout_ema,
        side_quote_fill_probability=0.20,
        side_quote_markout_30s=-0.30,
    )


def test_campaign_repair_features_use_inventory_relative_directions():
    long_features = _features(0.003, microprice_shift_bps=-1.5, markout_ema=-2.0)
    short_features = _features(-0.003, microprice_shift_bps=1.5, markout_ema=2.0)

    assert long_features["microprice_shift_inventory_adverse_bps"] == pytest.approx(1.5)
    assert short_features["microprice_shift_inventory_adverse_bps"] == pytest.approx(1.5)
    assert long_features["inventory_side_markout_risk"] == pytest.approx(2.0)
    assert short_features["inventory_side_markout_risk"] == pytest.approx(2.0)
    assert long_features["campaign_add_minus_reduce_fills_so_far"] == 2.0


def test_campaign_repair_model_round_trip_preserves_side_specific_scores(tmp_path):
    long = CampaignRepairSideModel(
        side="LONG",
        base_rate=0.5,
        base_logit=0.0,
        numeric_cuts={"abs_inventory": (0.002,)},
        contributions={"abs_inventory": {"b00": 1.0, "b01": -1.0}},
        contribution_scale=1.0,
    )
    short = CampaignRepairSideModel(
        side="SHORT",
        base_rate=0.5,
        base_logit=0.0,
        numeric_cuts={"abs_inventory": (0.002,)},
        contributions={"abs_inventory": {"b00": -1.0, "b01": 1.0}},
        contribution_scale=1.0,
    )
    model = CampaignRepairModel(
        long_model=long,
        short_model=short,
        model_id="unit-test",
        training_end_day="2026-01-01",
    )
    path = tmp_path / "repair.json"
    path.write_text(json.dumps(model.to_dict()), encoding="utf-8")
    restored = CampaignRepairModel.load(path)

    features = {"abs_inventory": 0.003}
    assert restored.score(0.003, features) < 0.5
    assert restored.score(-0.003, features) > 0.5
    assert math.isnan(restored.score(0.0, features))


def test_campaign_repair_history_is_causal_and_resets_between_campaigns():
    history = CampaignRepairProbabilityHistory(max_history_ms=10_000)
    start = 1_700_000_000_000_000_000

    current, change = history.update(
        campaign_id=1,
        ts_ns=start,
        probability=0.8,
        lookback_ms=1_000,
    )
    assert current == pytest.approx(0.8)
    assert math.isnan(change)

    _, change = history.update(
        campaign_id=1,
        ts_ns=start + 1_000_000_000,
        probability=0.5,
        lookback_ms=1_000,
    )
    assert change == pytest.approx(-0.3)

    _, reset_change = history.update(
        campaign_id=2,
        ts_ns=start + 2_000_000_000,
        probability=0.4,
        lookback_ms=1_000,
    )
    assert math.isnan(reset_change)
