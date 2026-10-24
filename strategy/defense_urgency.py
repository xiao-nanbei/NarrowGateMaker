"""Default-off contribution arbitration before the existing quote consumers.

No orders are created here. Native and Python quote cores consume the same
resolved configuration; all downstream guard composition remains unchanged.
"""
from dataclasses import dataclass, replace
import math

MODES = ('B0', 'DEFENSE_PAUSE_RELEASE_ON_LOSS',
         'DEFENSE_ALL_RELEASE_ON_LOSS', 'URGENCY_OFF_WHILE_DEFENSE_PAUSES')


@dataclass(frozen=True)
class ContributionRemoval:
    defense_all: bool = False
    defense_pause: bool = False
    urgency: bool = False


def conditional_removal(mode, *, potential_active, potential_pause,
                        pure_reducing, visible_state_consistent, visible_pnl):
    if mode not in MODES:
        raise ValueError('undeclared conditional mode')
    none = ContributionRemoval()
    if (mode == 'B0' or pure_reducing is not True
            or visible_state_consistent is not True):
        return none
    if (not isinstance(visible_pnl, (int, float))
            or isinstance(visible_pnl, bool) or not math.isfinite(visible_pnl)):
        return none
    if mode == 'URGENCY_OFF_WHILE_DEFENSE_PAUSES':
        return ContributionRemoval(urgency=potential_active is True and potential_pause is True)
    if potential_active is not True or visible_pnl >= 0:
        return none
    return (ContributionRemoval(defense_pause=True)
            if mode == 'DEFENSE_PAUSE_RELEASE_ON_LOSS'
            else ContributionRemoval(defense_all=True))


def resolve_quote_config(state, cfg, pred, depth):
    """Resolve using the legal original quantity, never a modified quote.

    The predicate is evaluated once before urgency, using the very same
    prediction normalization, weighted-mid and defense function as quote_core.
    Only the reducing side can have active defense in the original consumer,
    so removing its contribution via config does not affect the adding side.
    Unknown/incoherent visible state and crossing quantities retain B0.
    """
    from strategy import quote_core as qc
    from strategy.risk_selection import inventory_role_for_target

    mode = cfg.defense_urgency_mode
    if mode not in MODES:
        raise ValueError('undeclared conditional mode')
    if mode == 'B0':
        return cfg
    if state.defense_urgency_visible_state_consistent is not True:
        return cfg
    values = (state.inventory, state.mid, state.unrealized_pnl, cfg.order_size,
              state.mo_ema_bid, state.mo_ema_ask)
    if not all(math.isfinite(v) for v in values) or state.mid <= 0 or cfg.order_size <= 0:
        return cfg
    side = 'SELL' if state.inventory > 0 else 'BUY'
    role = inventory_role_for_target(side, state.inventory, cfg.order_size)
    if role != 'reducing':
        return cfg
    probability = qc._get(pred, 'touch_conditioned_up_probability_10000ms', None)
    predicted_return = qc._get(pred, 'touch_conditioned_price_change_fraction_10000ms', None)
    if not all(isinstance(v, (int, float)) and math.isfinite(v)
               for v in (probability, predicted_return)):
        return cfg
    depth = depth or qc.DepthSnapshot()
    fair = (qc._weighted_mid_proxy(depth, cfg.weighted_mid_proxy_levels, state.mid)
            if depth.has_book and cfg.use_depth_weighted_mid_proxy else state.mid)
    potential = qc._side_defense_state(
        side, inventory=state.inventory, max_inventory=cfg.max_inventory,
        dir_signal=probability - .5 if cfg.ml_enabled else 0.,
        pred_ret=predicted_return,
        markout_ema=state.mo_ema_ask if side == 'SELL' else state.mo_ema_bid,
        weighted_mid_proxy_shift_bps=(fair-state.mid)/state.mid*10000.,
        unrealized_pnl=state.unrealized_pnl, cfg=cfg)
    removal = conditional_removal(
        mode, potential_active=potential['active'], potential_pause=potential['pause'],
        pure_reducing=True, visible_state_consistent=True,
        visible_pnl=state.unrealized_pnl)
    if removal.defense_all:
        return replace(cfg, defense_guard_enabled=False)
    if removal.defense_pause:
        return replace(cfg, defense_pause=False)
    if removal.urgency:
        return replace(cfg, exit_urgency_strength=0.)
    return cfg
