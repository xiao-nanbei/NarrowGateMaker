import pytest

from strategy.global_flow import GLOBAL_FLOW_SCHEMA_VERSION, GlobalFlowState
from strategy.multi_market_policy import (
    POST_FILL_STOP_ADD_MODE,
    MultiMarketPolicy,
    MultiMarketPolicyConfig,
    MultiMarketPolicyContext,
)


def _state(direction: float) -> GlobalFlowState:
    return GlobalFlowState(
        schema_version=GLOBAL_FLOW_SCHEMA_VERSION,
        as_of_receive_ts_ns=1_700_000_000_000_000_000,
        windows={
            250: {
                "valid": 1,
                "spot": {
                    "valid": 1,
                    "venue_agreement": 1.0,
                    "mid_move_bps": direction * 1.0,
                },
                "perp": {
                    "valid": 1,
                    "venue_agreement": 1.0,
                    "mid_move_bps": direction * 1.2,
                },
                "global_flow_pressure": direction * 0.5,
                "local_bridge_move_bps": direction * 0.8,
            }
        },
    )


def _policy() -> MultiMarketPolicy:
    return MultiMarketPolicy(
        MultiMarketPolicyConfig(
            enabled=True,
            mode=POST_FILL_STOP_ADD_MODE,
            horizon_ms=250,
            min_abs_inventory=0.004,
            min_campaign_fills=1,
            max_repair_probability=0.6,
            min_repair_probability_drop=0.05,
        )
    )


@pytest.mark.parametrize(
    ("inventory", "global_direction", "blocked_side", "reducing_side"),
    [
        (0.006, -1.0, "BUY", "SELL"),
        (-0.006, 1.0, "SELL", "BUY"),
    ],
)
def test_post_fill_stop_add_is_side_specific_and_never_blocks_reducing(
    inventory, global_direction, blocked_side, reducing_side
):
    decision = _policy().evaluate(
        MultiMarketPolicyContext(
            decision_ts_ns=1_700_000_000_000_000_000,
            inventory=inventory,
            campaign_active=True,
            campaign_age_s=60.0,
            campaign_fills=2,
            repair_probability=0.4,
            repair_probability_change=-0.1,
            repair_probability_age_ms=10.0,
            global_flow_state=_state(global_direction),
        )
    )

    assert decision.for_side(blocked_side).active
    assert not decision.for_side(blocked_side).allow_exposure_increase
    assert not decision.for_side(reducing_side).active
    assert decision.for_side(reducing_side).allow_exposure_increase


def test_post_fill_policy_does_not_block_without_repair_deterioration():
    decision = _policy().evaluate(
        MultiMarketPolicyContext(
            decision_ts_ns=1_700_000_000_000_000_000,
            inventory=0.006,
            campaign_active=True,
            campaign_age_s=60.0,
            campaign_fills=2,
            repair_probability=0.7,
            repair_probability_change=0.1,
            repair_probability_age_ms=10.0,
            global_flow_state=_state(-1.0),
        )
    )

    assert decision.buy.allow_exposure_increase
    assert decision.sell.allow_exposure_increase
    assert decision.buy.reason == "noop"


def test_disabled_policy_is_exact_noop_even_with_adverse_state():
    decision = MultiMarketPolicy().evaluate(
        MultiMarketPolicyContext(
            decision_ts_ns=1,
            inventory=0.006,
            campaign_active=True,
            campaign_age_s=60.0,
            campaign_fills=2,
            repair_probability=0.2,
            repair_probability_change=-0.5,
            repair_probability_age_ms=0.0,
            global_flow_state=_state(-1.0),
        )
    )

    assert decision.buy.allow_exposure_increase
    assert decision.sell.allow_exposure_increase
    assert not decision.buy.active
    assert not decision.sell.active
