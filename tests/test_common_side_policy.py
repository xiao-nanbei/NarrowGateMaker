import math

import pytest

from strategy.policy_guards import (
    CommonSidePolicyInput,
    POLICY_REASON_ADVERSE,
    POLICY_REASON_BURST,
    POLICY_REASON_DEFENSE,
    POLICY_REASON_MARKOUT,
    POLICY_REASON_STALE_WARN,
    POLICY_REASON_THIN_DEPTH,
    evaluate_common_side_policy,
)


def test_common_side_policy_composes_live_guards():
    result = evaluate_common_side_policy(
        CommonSidePolicyInput(
            exposure_increasing=True,
            inventory_ratio=0.5,
            depth_age_s=3.0,
            max_book_age_s=5.0,
            markout_ema=-5.0,
            markout_spread_scale=0.2,
            markout_reference=10.0,
            microprice_shift_bps=0.6,
            l2_quote_flip_rate=0.40,
            l2_book_cancel_ratio=0.05,
            l2_near_depth_total=0.4,
            thin_depth_threshold=0.8,
            side_adverse=True,
            side_adverse_pause=True,
            defense_guard=True,
            defense_spread_mult=1.50,
            defense_pause=False,
        )
    )

    assert result.allow_post
    assert not result.allow_exposure_increase
    assert result.spread_mult == pytest.approx(1.50)
    assert result.size_mult == pytest.approx(0.45)
    assert result.reason_mask & POLICY_REASON_STALE_WARN
    assert result.reason_mask & POLICY_REASON_MARKOUT
    assert result.reason_mask & POLICY_REASON_ADVERSE
    assert result.reason_mask & POLICY_REASON_DEFENSE
    assert result.reason_mask & POLICY_REASON_BURST
    assert result.reason_mask & POLICY_REASON_THIN_DEPTH


def test_common_side_policy_defense_pause_is_hard_but_adverse_pause_is_exposure_only():
    adverse = evaluate_common_side_policy(
        CommonSidePolicyInput(
            exposure_increasing=True,
            side_adverse=True,
            side_adverse_pause=True,
        )
    )
    defense = evaluate_common_side_policy(
        CommonSidePolicyInput(
            exposure_increasing=False,
            defense_guard=True,
            defense_pause=True,
        )
    )

    assert adverse.allow_post
    assert not adverse.allow_exposure_increase
    assert not defense.allow_post


def test_common_side_policy_nonfinite_book_age_is_hard_stale():
    result = evaluate_common_side_policy(
        CommonSidePolicyInput(
            exposure_increasing=True,
            depth_age_s=math.inf,
            max_book_age_s=5.0,
        )
    )
    assert not result.allow_post
