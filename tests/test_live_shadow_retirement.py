from __future__ import annotations

import copy
from types import SimpleNamespace

from live.config import _parse
from strategy.maker_engine import MakerEngine
from strategy.quote_core import quote_core_config_from_live_config


def test_inventory_threshold_shadow_requires_explicit_enable() -> None:
    disabled = SimpleNamespace(
        logging=SimpleNamespace(
            inventory_campaign_shadow_enabled=False,
            inventory_campaign_shadow_log="logs/inventory_campaign_shadow.csv",
            trade_log="logs/trades.csv",
        )
    )
    enabled = copy.deepcopy(disabled)
    enabled.logging.inventory_campaign_shadow_enabled = True

    assert MakerEngine._resolve_inventory_campaign_shadow_log_path(disabled) == ""
    assert (
        MakerEngine._resolve_inventory_campaign_shadow_log_path(enabled)
        == "logs/inventory_campaign_shadow.csv"
    )


def test_shadow_retirement_does_not_change_quote_core_contract() -> None:
    retired = _parse({})
    retired.strategy.cross_venue_fair_price_shadow_enabled = False
    retired.depth_execution.shadow_enabled = False
    retired.depth_execution.imbalance_asym.enabled = True
    retired.logging.inventory_campaign_shadow_enabled = False
    retired.strategy.dynamic_fill_hazard_shadow_enabled = True
    retired.strategy.dynamic_fill_hazard_action_enabled = False
    retired.ml.enabled = True
    retired.ml.ret_skew = 0.0
    historical = copy.deepcopy(retired)
    historical.strategy.cross_venue_fair_price_shadow_enabled = True
    historical.depth_execution.shadow_enabled = True
    historical.logging.inventory_campaign_shadow_enabled = True

    assert quote_core_config_from_live_config(retired) == quote_core_config_from_live_config(
        historical
    )
    assert retired.strategy.cross_venue_fair_price_shadow_enabled is False
    assert retired.depth_execution.shadow_enabled is False
    assert retired.depth_execution.imbalance_asym.enabled is True
    assert retired.logging.inventory_campaign_shadow_enabled is False
    assert retired.strategy.dynamic_fill_hazard_shadow_enabled is True
    assert retired.strategy.dynamic_fill_hazard_action_enabled is False
    assert retired.ml.enabled is True
