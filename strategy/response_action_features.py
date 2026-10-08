"""Decision-visible action features; history ablation keeps algebraic basics."""
import math

BASE = ('spread', 'own_depth', 'opposite_depth', 'own_touch_qty', 'side_sign',
        'book_age_s', 'touch_delta_qty', 'neighbor_delta_qty', 'utc_time_sin',
        'utc_time_cos', 'past_price_variation_10s') + tuple(
            f'{name}_{w}s' for w in (1, 5, 10)
            for name in ('trade_pressure', 'trade_activity', 'past_mid_move')) + (
        'pressure_x_depth_change', 'pressure_acceleration', 'order_price_qty',
        'order_price_delta_qty', 'order_distance_from_touch')
HISTORY = ('recovery_age_s', 'observed_recovery_fraction',
           'order_recovery_age_s', 'order_observed_recovery_fraction')
CONTEXT = ('pred_dir', 'pred_ret', 'tox_bid', 'tox_ask', 'book_imb',
           'raw_half_spread', 'raw_mid_shift', 'order_age_s', 'confirmed_fill_qty',
           'local_inventory', 'signed_target_delta_ticks', 'role_opener', 'role_add', 'role_reducing')
FEATURES = BASE+CONTEXT+HISTORY+tuple(name+'_missing' for name in HISTORY)
ABLATION = BASE+CONTEXT


def extract(row, *, materialize=True):
    """Absent recovery anchors are encoded separately, never called zero recovery."""
    micro, context = row['features'], row.get('action_context', {})
    values = {} if materialize else None
    for source, names in ((micro, BASE), (context, CONTEXT)):
        for name in names:
            value = source.get(name)
            if value is None or not math.isfinite(value):
                return None
            if materialize:
                values[name] = value
    for name in HISTORY:
        value = micro.get(name)
        if value is not None and not math.isfinite(value):
            raise ValueError('nonfinite response history')
        if materialize:
            values[name] = 0. if value is None else value
            values[name+'_missing'] = float(value is None)
    return values if materialize else True
