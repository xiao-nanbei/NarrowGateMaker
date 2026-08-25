#include "quote_core.hpp"

#include <cmath>

namespace narrowgate_cpp {
namespace {

int bounded_levels(int requested, std::size_t bid_count, std::size_t ask_count) {
    const int n = std::max(1, requested);
    return std::max(1, std::min(n, static_cast<int>(std::min(bid_count, ask_count))));
}

bool unique_valid_level(const DepthSideView& levels, std::size_t index) {
    if (!levels.valid(index)) {
        return false;
    }
    const double price = levels.price(index);
    for (std::size_t j = 0; j < index; ++j) {
        if (levels.valid(j) && levels.price(j) == price) {
            return false;
        }
    }
    return true;
}

std::size_t unique_valid_size(const DepthSideView& levels) {
    std::size_t count = 0;
    for (std::size_t i = 0; i < levels.size(); ++i) {
        count += unique_valid_level(levels, i) ? 1U : 0U;
    }
    return count;
}

double depth_qty_sum(const DepthSideView& levels, int n) {
    double out = 0.0;
    int included = 0;
    for (std::size_t i = 0; i < levels.size() && included < n; ++i) {
        if (!unique_valid_level(levels, i)) {
            continue;
        }
        out += levels.quantity(i);
        ++included;
    }
    return out;
}

template <int DefaultLevels>
double near_depth_total(const DepthView& depth, int requested_levels) {
    // near_depth_total 的口径要和 Python trace/live 侧保持一致；它会影响 thin_depth、
    // defense/adverse bucket 和 quote-EV 证据分桶，不只是一个展示字段。
    if (!depth.has_book()) {
        return 0.0;
    }
    const int levels = requested_levels > 0
        ? requested_levels
        : DefaultLevels;
    const int n = bounded_levels(levels, unique_valid_size(depth.bids), unique_valid_size(depth.asks));
    return depth_qty_sum(depth.bids, n) + depth_qty_sum(depth.asks, n);
}

double depth_imbalance(const DepthView& depth, int requested_levels) {
    if (!depth.has_book()) {
        return 0.0;
    }
    const int n = bounded_levels(requested_levels, unique_valid_size(depth.bids), unique_valid_size(depth.asks));
    const double bid_qty = depth_qty_sum(depth.bids, n);
    const double ask_qty = depth_qty_sum(depth.asks, n);
    return safe_div(bid_qty - ask_qty, bid_qty + ask_qty);
}

double microprice(const DepthView& depth, int requested_levels, double fallback_mid) {
    if (!depth.has_book()) {
        return fallback_mid;
    }
    const int n = bounded_levels(requested_levels, unique_valid_size(depth.bids), unique_valid_size(depth.asks));
    const double bid_qty = depth_qty_sum(depth.bids, n);
    const double ask_qty = depth_qty_sum(depth.asks, n);
    const double total = bid_qty + ask_qty;
    const double best_bid = depth.best_bid();
    const double best_ask = depth.best_ask();
    if (total <= 1e-12 || best_ask <= best_bid) {
        return 0.5 * (best_bid + best_ask);
    }
    const double mid = 0.5 * (best_bid + best_ask);
    const double half = 0.5 * (best_ask - best_bid);
    const double imb = safe_div(bid_qty - ask_qty, total);
    return mid + imb * half;
}

double estimate_depth_kappa(
    const DepthView& depth,
    double kappa_base,
    double depth_baseline,
    int requested_levels,
    double min_ratio
) {
    if (!depth.has_book() || depth_baseline <= 0.0) {
        return kappa_base;
    }
    const int n = bounded_levels(requested_levels, unique_valid_size(depth.bids), unique_valid_size(depth.asks));
    const double bid_depth = depth_qty_sum(depth.bids, n);
    const double ask_depth = depth_qty_sum(depth.asks, n);
    const double avg_depth = 0.5 * (bid_depth + ask_depth);
    if (avg_depth <= 0.0) {
        return kappa_base;
    }
    const double ratio_floor = clamp(min_ratio, 0.05, 3.0);
    const double ratio = clamp(avg_depth / depth_baseline, ratio_floor, 3.0);
    return std::max(kappa_base * ratio, 1e-12);
}

double depth_tox_mult(double mid, const DepthView& depth, const QuoteCoreConfig& cfg) {
    if (!cfg.depth_tox_enabled || !depth.has_book()) {
        return 1.0;
    }
    const double imb = depth_imbalance(depth, cfg.depth_tox_levels);
    double micro_shift_bps = 0.0;
    if (mid > 0.0) {
        const double fair = microprice(depth, 3, mid);
        micro_shift_bps = (fair - mid) / mid * 10000.0;
    }
    if (std::abs(imb) >= std::abs(cfg.depth_tox_imbalance_threshold) ||
        std::abs(micro_shift_bps) >= std::abs(cfg.depth_tox_microprice_shift_bps)) {
        return std::max(1.0, cfg.depth_tox_spread_mult);
    }
    return 1.0;
}

template <Side S>
bool exposure_increasing(double inventory, double quantity, double lot_size) {
    const double lot = std::abs(lot_size);
    if (!std::isfinite(inventory) || !std::isfinite(quantity) ||
        !std::isfinite(lot) || quantity <= 0.0 || lot <= 0.0) {
        return true;
    }
    const double tolerance = std::max(lot * 1e-9, 1e-12);
    if constexpr (is_buy_v<S>) {
        if (inventory >= -tolerance) {
            return true;
        }
        return inventory + quantity > tolerance;
    } else {
        if (inventory <= tolerance) {
            return true;
        }
        return inventory - quantity < -tolerance;
    }
}

struct SideAdverseState {
    bool active = false;
    bool pause = false;
    bool exposure_increasing = false;
    bool toxicity = false;
    bool markout = false;
    bool direction = false;
    bool ret = false;
    bool microprice = false;
    bool thin_depth = false;
    double spread_mult = 1.0;
};

template <Side S>
SideAdverseState side_adverse_state(
    double inventory,
    double quantity,
    double lot_size,
    double dir_signal,
    double pred_ret,
    double toxicity,
    double markout_ema,
    bool markout_pause_latch,
    double microprice_shift_bps,
    double near_depth,
    const QuoteCoreConfig& cfg
) {
    SideAdverseState out;
    out.exposure_increasing = exposure_increasing<S>(inventory, quantity, lot_size);
    // adverse 只作用于增加该侧库存风险的报价；减库存方向不应因为 toxicity/markout 被挡掉。
    if (!cfg.adverse_guard_enabled) {
        return out;
    }

    const double sign = adverse_signal_sign<S>();
    out.toxicity = toxicity >= cfg.adverse_toxicity_threshold;
    out.markout = markout_ema < -std::abs(cfg.adverse_markout_threshold);
    double pause_threshold = std::abs(cfg.adverse_markout_pause_threshold);
    if (pause_threshold <= 0.0) {
        pause_threshold = std::abs(cfg.adverse_markout_threshold);
    }
    const bool markout_pause_raw = markout_ema < -pause_threshold;
    const bool markout_pause = cfg.adverse_markout_pause_hybrid
        ? markout_pause_latch
        : markout_pause_raw;

    if (cfg.adverse_dir_threshold > 0.0) {
        out.direction = sign * dir_signal >= std::abs(cfg.adverse_dir_threshold);
    }
    if (cfg.adverse_ret_bps_threshold > 0.0) {
        out.ret = sign * pred_ret * 10000.0 >= std::abs(cfg.adverse_ret_bps_threshold);
    }
    if (cfg.adverse_microprice_shift_bps > 0.0) {
        out.microprice = sign * microprice_shift_bps >= std::abs(cfg.adverse_microprice_shift_bps);
    }
    out.thin_depth = cfg.adverse_thin_depth_threshold > 0.0 &&
        near_depth > 0.0 && near_depth < cfg.adverse_thin_depth_threshold;
    out.active = out.exposure_increasing &&
        (out.toxicity || out.markout || out.direction || out.ret || out.microprice);
    out.spread_mult = std::max(1.0, cfg.adverse_spread_mult);
    if (out.active && out.thin_depth) {
        out.spread_mult *= std::max(1.0, cfg.adverse_thin_depth_mult);
    }
    out.pause = out.active && cfg.adverse_pause &&
        (out.toxicity || markout_pause || out.direction || out.ret || out.microprice);
    return out;
}

struct SideDefenseState {
    bool active = false;
    bool pause = false;
    bool reducing = false;
    bool emergency = false;
    bool markout = false;
    bool direction = false;
    bool ret = false;
    bool microprice = false;
    double spread_mult = 1.0;
};

template <Side S>
SideDefenseState side_defense_state(
    double inventory,
    double max_inventory,
    double dir_signal,
    double pred_ret,
    double markout_ema,
    double microprice_shift_bps,
    double unrealized_pnl,
    const QuoteCoreConfig& cfg
) {
    SideDefenseState out;
    if constexpr (is_buy_v<S>) {
        out.reducing = inventory < 0.0;
    } else {
        out.reducing = inventory > 0.0;
    }

    const double inv_ratio = std::abs(inventory) / std::max(max_inventory, 1e-12);
    const bool emergency_inventory = cfg.defense_emergency_inventory_ratio > 0.0 &&
        inv_ratio >= cfg.defense_emergency_inventory_ratio;
    const bool emergency_loss = cfg.defense_emergency_loss > 0.0 &&
        unrealized_pnl <= -std::abs(cfg.defense_emergency_loss);
    out.emergency = emergency_inventory || emergency_loss;

    if (!cfg.defense_guard_enabled) {
        return out;
    }

    const double sign = defense_signal_sign<S>();
    out.markout = markout_ema < -std::abs(cfg.defense_markout_threshold);
    if (cfg.defense_dir_threshold > 0.0) {
        out.direction = sign * dir_signal >= std::abs(cfg.defense_dir_threshold);
    }
    if (cfg.defense_ret_bps_threshold > 0.0) {
        out.ret = sign * pred_ret * 10000.0 >= std::abs(cfg.defense_ret_bps_threshold);
    }
    if (cfg.defense_microprice_shift_bps > 0.0) {
        out.microprice = sign * microprice_shift_bps >= std::abs(cfg.defense_microprice_shift_bps);
    }

    const bool needs_extreme = cfg.defense_dir_threshold > 0.0 ||
        cfg.defense_ret_bps_threshold > 0.0 ||
        cfg.defense_microprice_shift_bps > 0.0;
    const bool extreme = needs_extreme ? (out.direction || out.ret || out.microprice) : true;
    out.active = out.reducing && !out.emergency && out.markout && extreme;
    out.pause = out.active && cfg.defense_pause;
    out.spread_mult = std::max(1.0, cfg.defense_spread_mult);
    return out;
}

template <Side S>
void fill_side_context(
    SideQuoteContext& ctx,
    const SideAdverseState& adverse,
    const SideDefenseState& defense,
    double raw_price,
    double pre_guard_price,
    double final_price,
    double mid,
    double best_bid,
    double best_ask,
    double pair_spread,
    bool mid_guard,
    bool post_only
) {
    ctx.raw_price = raw_price;
    ctx.pre_guard_price = pre_guard_price;
    ctx.final_price = final_price;
    ctx.raw_distance_to_mid = side_distance_to_mid<S>(mid, raw_price, 0.0);
    ctx.final_distance_to_mid = side_distance_to_mid<S>(mid, final_price, 0.0);
    ctx.final_pair_spread = pair_spread;
    ctx.spread_mult = std::max(adverse.spread_mult, defense.active ? defense.spread_mult : 1.0);
    ctx.side_adverse = adverse.active;
    ctx.side_adverse_pause = adverse.pause;
    ctx.adverse_toxicity = adverse.toxicity;
    ctx.adverse_markout = adverse.markout;
    ctx.adverse_direction = adverse.direction;
    ctx.adverse_ret = adverse.ret;
    ctx.adverse_microprice = adverse.microprice;
    ctx.adverse_thin_depth = adverse.thin_depth;
    ctx.defense_guard = defense.active;
    ctx.defense_pause = defense.pause;
    ctx.defense_reducing = defense.reducing;
    ctx.defense_emergency = defense.emergency;
    ctx.defense_markout = defense.markout;
    ctx.defense_direction = defense.direction;
    ctx.defense_ret = defense.ret;
    ctx.defense_microprice = defense.microprice;
    ctx.defense_spread_mult = defense.spread_mult;
    ctx.mid_guard = mid_guard;
    ctx.post_only = post_only;
    if constexpr (is_buy_v<S>) {
        ctx.raw_quote_delta_to_bbo = best_bid > 0.0 ? best_bid - raw_price : 0.0;
        ctx.pre_guard_delta_to_bbo = best_bid > 0.0 ? best_bid - pre_guard_price : 0.0;
        ctx.final_quote_delta_to_bbo = best_bid > 0.0 ? best_bid - final_price : 0.0;
    } else {
        ctx.raw_quote_delta_to_bbo = best_ask > 0.0 ? raw_price - best_ask : 0.0;
        ctx.pre_guard_delta_to_bbo = best_ask > 0.0 ? pre_guard_price - best_ask : 0.0;
        ctx.final_quote_delta_to_bbo = best_ask > 0.0 ? final_price - best_ask : 0.0;
    }
}

template <Side S>
double apply_routing_side_price(
    double mid,
    double price,
    double spread_mult,
    double tick
) {
    if (mid <= 0.0 || std::abs(spread_mult - 1.0) <= 1e-12) {
        return price;
    }
    spread_mult = std::max(0.05, spread_mult);
    if constexpr (is_buy_v<S>) {
        const double dist = std::max(mid - price, tick);
        return std::min(floor_tick(mid - dist * spread_mult, tick), mid - tick);
    } else {
        const double dist = std::max(price - mid, tick);
        return std::max(ceil_tick(mid + dist * spread_mult, tick), mid + tick);
    }
}

double routing_policy_qty(
    double desired,
    double base,
    double price,
    double lot,
    double min_qty,
    double min_notional
) {
    double qty = floor_lot(std::max(0.0, desired), lot);
    if (desired <= 0.0 || price <= 0.0 || lot <= 0.0) {
        return qty;
    }

    const double minq = std::max(
        std::ceil(std::max(min_qty, lot) / lot - 1e-12) * lot,
        lot
    );
    const double min_notional_qty = min_notional > 0.0
        ? std::ceil(min_notional / price / lot - 1e-12) * lot
        : lot;
    const double min_filter_qty = std::max(minq, min_notional_qty);
    const bool base_is_valid =
        base + 1e-12 >= min_filter_qty &&
        (min_notional <= 0.0 || base * price + 1e-8 >= min_notional);

    // Match Python routing exactly: size shaping may restore a valid base
    // order to the exchange minimum, but it must never turn an invalid base
    // order into a larger risk exposure.
    if (base_is_valid && qty < min_filter_qty) {
        qty = min_filter_qty;
    }
    return floor_lot(qty, lot);
}

}  // namespace

std::tuple<double, double, bool, double> apply_final_spread_cap(
    double mid,
    double bid_price,
    double ask_price,
    double max_spread,
    double tick_size
) {
    const double tick = std::max(std::abs(tick_size), 1e-12);
    if (mid <= 0.0 || max_spread <= 0.0 || ask_price <= bid_price) {
        return {bid_price, ask_price, false, 0.0};
    }

    const double spread = ask_price - bid_price;
    if (spread <= max_spread + 1e-12) {
        return {bid_price, ask_price, false, 0.0};
    }

    const double excess = spread - max_spread;
    double bid_dist = std::max(mid - bid_price, tick);
    double ask_dist = std::max(ask_price - mid, tick);
    const double dist_sum = bid_dist + ask_dist;
    if (max_spread > 2.0 * tick && dist_sum > 1e-12) {
        const double usable = max_spread - 2.0 * tick;
        bid_dist = tick + usable * bid_dist / dist_sum;
        ask_dist = tick + usable * ask_dist / dist_sum;
        bid_price = ceil_tick(mid - bid_dist, tick);
        if (bid_price >= mid) {
            bid_price = floor_tick(mid, tick);
            if (bid_price >= mid) {
                bid_price -= tick;
            }
        }
        ask_price = floor_tick(mid + ask_dist, tick);
        if (ask_price <= mid) {
            ask_price = ceil_tick(mid, tick);
            if (ask_price <= mid) {
                ask_price += tick;
            }
        }
    } else {
        bid_price = floor_tick(mid - tick, tick);
        ask_price = ceil_tick(mid + tick, tick);
    }

    return {bid_price, ask_price, true, excess};
}

LiveRoutingResult compute_live_routing_decision(
    const LiveRoutingInput& input,
    const LiveRoutingPolicy& bid_policy,
    const LiveRoutingPolicy& ask_policy
) {
    LiveRoutingResult out;
    const double tick = std::max(std::abs(input.tick_size), 1e-12);
    const double lot = std::max(std::abs(input.lot_size), 1e-12);

    out.bid_price = apply_routing_side_price<Side::Buy>(
        input.mid, input.base_bid_price, bid_policy.spread_mult, tick);
    out.ask_price = apply_routing_side_price<Side::Sell>(
        input.mid, input.base_ask_price, ask_policy.spread_mult, tick);
    const auto capped = apply_final_spread_cap(
        input.mid, out.bid_price, out.ask_price, input.max_spread, tick);
    out.post_policy_cap_hit = std::get<2>(capped);
    if (out.post_policy_cap_hit) {
        out.bid_price = std::get<0>(capped);
        out.ask_price = std::get<1>(capped);
    }
    if (input.best_ask > 0.0 && out.bid_price >= input.best_ask) {
        out.bid_price = input.best_ask - tick;
    }
    if (input.best_bid > 0.0 && out.ask_price <= input.best_bid) {
        out.ask_price = input.best_bid + tick;
    }

    out.can_bid_after_inventory = input.inventory < input.max_inventory;
    out.can_ask_after_inventory = input.inventory > -input.max_inventory;
    const bool bid_exposure_increasing = exposure_increasing<Side::Buy>(
        input.inventory, input.order_size, input.lot_size);
    const bool ask_exposure_increasing = exposure_increasing<Side::Sell>(
        input.inventory, input.order_size, input.lot_size);
    out.can_bid = out.can_bid_after_inventory && bid_policy.allow_post &&
        (bid_policy.allow_exposure_increase || !bid_exposure_increasing);
    out.can_ask = out.can_ask_after_inventory && ask_policy.allow_post &&
        (ask_policy.allow_exposure_increase || !ask_exposure_increasing);

    const double threshold = input.requote_threshold_bps / 10000.0;
    if (input.bid_active && input.bid_active_price > 0.0) {
        const double drift = std::abs(out.bid_price - input.bid_active_price) /
            input.bid_active_price;
        out.bid_needs_update = drift > threshold;
        if (bid_policy.order_ttl_ms > 0.0 && input.bid_age_ms >= bid_policy.order_ttl_ms) {
            out.bid_needs_update = true;
        }
    }
    if (input.ask_active && input.ask_active_price > 0.0) {
        const double drift = std::abs(out.ask_price - input.ask_active_price) /
            input.ask_active_price;
        out.ask_needs_update = drift > threshold;
        if (ask_policy.order_ttl_ms > 0.0 && input.ask_age_ms >= ask_policy.order_ttl_ms) {
            out.ask_needs_update = true;
        }
    }

    out.bid_size = input.order_size;
    out.ask_size = input.order_size;
    if (input.eta > 0.0 && input.max_inventory > 1e-10) {
        const double q_norm = input.inventory / input.max_inventory;
        if (input.inventory > 0.0) {
            out.bid_size = std::max(
                lot, floor_lot(input.order_size * std::exp(-input.eta * q_norm), lot));
        } else if (input.inventory < 0.0) {
            out.ask_size = std::max(
                lot, floor_lot(input.order_size * std::exp(input.eta * q_norm), lot));
        }
    }
    if (input.symmetric_size) {
        const double mirrored = std::min(out.bid_size, out.ask_size);
        out.bid_size = mirrored;
        out.ask_size = mirrored;
    }
    out.bid_size = routing_policy_qty(
        out.bid_size * bid_policy.size_mult,
        input.order_size,
        out.bid_price,
        lot,
        input.min_qty,
        input.min_notional
    );
    out.ask_size = routing_policy_qty(
        out.ask_size * ask_policy.size_mult,
        input.order_size,
        out.ask_price,
        lot,
        input.min_qty,
        input.min_notional
    );

    if (input.inventory > 0.0) {
        const double room = floor_lot(
            std::max(0.0, input.max_inventory - input.inventory), lot);
        out.bid_size = room >= lot ? std::min(out.bid_size, room) : 0.0;
    } else if (input.inventory < -lot) {
        const double close_cap = floor_lot(std::abs(input.inventory), lot);
        if (close_cap >= input.min_qty && close_cap * out.bid_price >= input.min_notional) {
            out.bid_size = std::min(out.bid_size, close_cap);
        }
    }
    if (input.inventory < 0.0) {
        const double room = floor_lot(
            std::max(0.0, input.max_inventory - std::abs(input.inventory)), lot);
        out.ask_size = room >= lot ? std::min(out.ask_size, room) : 0.0;
    } else if (input.inventory > lot) {
        const double close_cap = floor_lot(input.inventory, lot);
        if (close_cap >= input.min_qty && close_cap * out.ask_price >= input.min_notional) {
            out.ask_size = std::min(out.ask_size, close_cap);
        }
    }

    return out;
}

QuoteCoreResult compute_quote_core(
    const QuoteState& state,
    const QuoteCoreConfig& cfg,
    const QuotePrediction& pred,
    const DepthView& depth
) {
    QuoteCoreResult out;
    const double tick = std::max(std::abs(cfg.tick_size), 1e-12);
    const double mid = state.mid;
    if (mid <= 0.0 || !std::isfinite(mid)) {
        return out;
    }

    const double q = state.inventory;
    const double gamma = std::max(cfg.gamma, 1e-12);
    out.sigma_sq_raw = std::max(state.sigma_sq, 0.0);
    double sigma_sq = std::max(state.sigma_sq, 1e-6);
    if (cfg.ml_enabled && cfg.vol_blend > 0.0 && pred.vol_10s > 1e-8) {
        sigma_sq = (1.0 - cfg.vol_blend) * sigma_sq + cfg.vol_blend * std::max(pred.vol_10s, 0.0);
    }
    sigma_sq = std::max(sigma_sq, 1e-6);
    out.sigma_sq_blended = sigma_sq;
    const double sigma_sq_horizon = sigma_sq * std::max(cfg.quote_horizon_s, 1e-6);

    const double kappa_base = cfg.p3_kappa_eff > 0.0 ? cfg.p3_kappa_eff : cfg.kappa;
    double kappa_used = std::max(kappa_base, 1e-12);
    const double kappa_before_depth = kappa_used;
    double fair = mid;
    if (depth.has_book() && cfg.use_depth_microprice) {
        fair = microprice(depth, cfg.microprice_levels, mid);
    }
    if (depth.has_book() && cfg.use_depth_kappa) {
        kappa_used = estimate_depth_kappa(
            depth,
            kappa_used,
            cfg.kappa_depth_baseline,
            cfg.kappa_levels,
            cfg.depth_kappa_ratio
        );
    }
    out.kappa_before_depth = kappa_before_depth;
    out.kappa_used = kappa_used;

    double regime_spread_scale = 1.0;
    double g_base = gamma;
    if (cfg.regime_enabled) {
        if (cfg.liq_baseline > 0.0 && state.trade_intensity > 0.0) {
            const double liq_ratio = state.trade_intensity / cfg.liq_baseline;
            const double liq_scale = 1.0 / std::max(std::sqrt(liq_ratio), 0.2);
            regime_spread_scale *= clamp(liq_scale, cfg.gamma_liq_scale_min, cfg.gamma_liq_scale_max);
        }
        if (cfg.vol_baseline > 0.0) {
            double vol_sq_ratio = sigma_sq / (cfg.vol_baseline * cfg.vol_baseline);
            vol_sq_ratio = std::max(vol_sq_ratio, 0.09);
            const double vol_scale = std::pow(vol_sq_ratio, cfg.vol_power * 0.5);
            regime_spread_scale *= clamp(vol_scale, cfg.gamma_scale_min, cfg.gamma_scale_max);
        }
    }

    if (cfg.max_inventory > 0.0 && std::abs(q) > 0.0) {
        const double inv_ratio = std::abs(q) / cfg.max_inventory;
        g_base *= 1.0 + inv_ratio * inv_ratio;
    }

    const double dir_signal = cfg.ml_enabled ? pred.dir_10s - 0.5 : 0.0;
    const bool active_dir = std::abs(dir_signal) > cfg.dir_threshold;
    double g_eff = g_base;
    if (active_dir && cfg.gamma_dir_bonus > 0.0) {
        double align = 0.0;
        if (q > 0.0) {
            align = dir_signal;
        } else if (q < 0.0) {
            align = -dir_signal;
        }
        g_eff = g_base * (1.0 - cfg.gamma_dir_bonus * align * 2.0);
        g_eff = clamp(g_eff, g_base * 0.2, g_base * 3.0);
    }

    double reservation = fair - q * g_eff * sigma_sq_horizon;
    const double kappa_spread = std::max(kappa_used * cfg.kappa_ratio, 1e-12);
    double delta = gamma * sigma_sq_horizon + (2.0 / gamma) * std::log(1.0 + gamma / kappa_spread);
    const double delta_raw = delta;
    delta *= regime_spread_scale;
    const double delta_after_regime = delta;
    if (state.ber_active && cfg.ber_spread_mult > 1.0) {
        delta *= cfg.ber_spread_mult;
    }
    if (cfg.markout_spread_scale > 0.0 && state.mo_ema_all != 0.0) {
        const double mo_ratio = state.mo_ema_all / std::max(state.mo_ref, 1e-6);
        const double mo_adj = 1.0 - cfg.markout_spread_scale * std::tanh(mo_ratio);
        delta *= clamp(mo_adj, 0.5, 2.0);
    }
    const double depth_tox = depth_tox_mult(mid, depth, cfg);
    delta *= depth_tox;
    if (cfg.regime_enabled && cfg.p3_delta_star > 0.0) {
        delta = std::max(delta, 2.0 * cfg.p3_delta_star);
    }
    const double min_spread = 2.0 * std::abs(cfg.maker_fee) * mid + tick;
    delta = std::max(delta, min_spread);

    const double near_depth = near_depth_total<10>(depth, cfg.trace_book_imb_levels);
    const double trace_book_imb = depth_imbalance(depth, cfg.trace_book_imb_levels);
    out.delta_raw = delta_raw;
    out.delta_after_regime = delta_after_regime;
    out.delta_pre_cap = delta;
    out.near_depth_total = near_depth;
    out.book_imb = trace_book_imb;
    out.depth_tox_mult = depth_tox;
    double cap_bps = cfg.max_spread_bps;
    if (cfg.dynamic_cap_enabled && cfg.dynamic_cap_base_bps > 0.0) {
        double cap_mult = 1.0;
        if (cfg.dynamic_cap_var_baseline > 1e-12) {
            const double cap_ratio = std::max(1.0, sigma_sq / cfg.dynamic_cap_var_baseline);
            cap_mult = std::pow(cap_ratio, cfg.dynamic_cap_alpha);
        }
        if (cfg.dynamic_cap_liq_beta > 0.0 && cfg.dynamic_cap_liq_baseline > 1e-12 &&
            near_depth > 1e-12) {
            cap_mult *= std::pow(cfg.dynamic_cap_liq_baseline / near_depth, cfg.dynamic_cap_liq_beta);
        }
        cap_mult = clamp(cap_mult, cfg.dynamic_cap_min_mult, cfg.dynamic_cap_max_mult);
        cap_bps = cfg.dynamic_cap_base_bps * cap_mult;
    }

    out.cap_bps = cap_bps;
    const int cap_mode = clamp(cfg.spread_cap_mode, 0, 2);
    bool cap_exposure_block = false;
    if (cap_bps > 0.0) {
        out.max_spread = mid * cap_bps / 10000.0;
        if (delta > out.max_spread) {
            out.flags.cap_hit = true;
            out.flags.delta_cap = true;
            if (cap_mode == 0) {
                delta = out.max_spread;
            } else if (cap_mode == 1) {
                cap_exposure_block = true;
            }
        }
    }

    const double delta_after_cap = delta;
    double half_d = 0.5 * delta;
    out.delta_after_cap = delta_after_cap;
    out.half_d = half_d;

    if (cfg.inventory_skew_strength > 0.0 && cfg.max_inventory > 1e-10) {
        reservation -= cfg.inventory_skew_strength * (q / cfg.max_inventory) * delta;
    }

    if (active_dir && cfg.skew_strength > 0.0) {
        double shift = cfg.skew_strength * dir_signal * delta;
        if (cfg.max_inventory > 1e-10 && std::abs(q) > 1e-10) {
            const double inv_ratio = clamp(std::abs(q) / cfg.max_inventory, 0.0, 1.0);
            const bool adds_exposure = (q > 0.0 && shift > 0.0) || (q < 0.0 && shift < 0.0);
            if (adds_exposure) {
                shift *= 1.0 - inv_ratio;
            }
        }
        reservation += shift;
    }

    if (cfg.ml_enabled && cfg.ret_skew > 0.0) {
        double shift = pred.ret_10s * cfg.ret_skew * mid;
        const double max_shift = cfg.ret_shift_max_pct * half_d;
        shift = clamp(shift, -max_shift, max_shift);
        if (cfg.max_inventory > 1e-10 && std::abs(q) > 1e-10) {
            const double inv_ratio = clamp(std::abs(q) / cfg.max_inventory, 0.0, 1.0);
            const bool adds_exposure = (q > 0.0 && shift > 0.0) || (q < 0.0 && shift < 0.0);
            if (adds_exposure) {
                shift *= 1.0 - inv_ratio;
            }
        }
        reservation += shift;
    }
    out.reservation_price = reservation;
    out.raw_reservation_shift = reservation - fair;

    double asym = 0.0;
    if (active_dir && cfg.asym_strength > 0.0) {
        asym = cfg.asym_strength * dir_signal * 2.0;
    }
    if (cfg.exit_urgency_strength > 0.0 && std::abs(q) > 1e-8 && state.position_open) {
        double hold_ratio = 0.0;
        if (cfg.position_timeout_s > 0.0) {
            hold_ratio = clamp(state.hold_time_s / cfg.position_timeout_s, 0.0, 1.0);
        }
        const double time_urg = hold_ratio * hold_ratio;
        double pnl_urg = 0.0;
        if (sigma_sq > 1e-10 && state.unrealized_pnl < 0.0) {
            const double dollar_vol = std::sqrt(
                sigma_sq * std::max(cfg.pnl_volatility_horizon_s, 1e-6)
            ) * std::abs(q);
            if (dollar_vol > 1e-8) {
                pnl_urg = std::min(-state.unrealized_pnl / dollar_vol, 3.0);
            }
        }
        double signal_urg = 0.0;
        if (q > 0.0 && dir_signal < 0.0) {
            signal_urg = std::min(std::abs(dir_signal) * 2.0, 1.0);
        } else if (q < 0.0 && dir_signal > 0.0) {
            signal_urg = std::min(std::abs(dir_signal) * 2.0, 1.0);
        }
        const double urgency =
            cfg.urgency_time_weight * time_urg +
            cfg.urgency_pnl_weight * pnl_urg +
            cfg.urgency_signal_weight * signal_urg;
        const double inv_sign = q > 0.0 ? 1.0 : -1.0;
        asym -= inv_sign * std::min(urgency, 1.0) * cfg.exit_urgency_strength;
    }
    const double micro_shift_bps = mid > 0.0 ? (fair - mid) / mid * 10000.0 : 0.0;
    if (depth.has_book() && cfg.book_imb_strength > 0.0) {
        asym += depth_imbalance(depth, cfg.book_imb_levels) * cfg.book_imb_strength;
    }
    if (cfg.markout_spread_scale > 0.0 && (state.mo_ema_bid != 0.0 || state.mo_ema_ask != 0.0)) {
        const double mo_diff = state.mo_ema_bid - state.mo_ema_ask;
        asym += cfg.markout_side_asymmetry_sign * cfg.markout_spread_scale *
            std::tanh(mo_diff / std::max(state.mo_ref, 1e-6)) * 0.5;
    }
    if (cfg.max_inventory > 1e-10 && std::abs(q) > 1e-10) {
        const double inv_ratio = clamp(std::abs(q) / cfg.max_inventory, 0.0, 1.0);
        if (cfg.inventory_signal_fade_strength > 0.0) {
            const bool adds_exposure = (q > 0.0 && asym > 0.0) || (q < 0.0 && asym < 0.0);
            if (adds_exposure) {
                asym *= std::max(0.0, 1.0 - cfg.inventory_signal_fade_strength * inv_ratio);
            }
        }
        if (cfg.inventory_asym_strength > 0.0) {
            const double inv_sign = q > 0.0 ? 1.0 : -1.0;
            asym -= inv_sign * cfg.inventory_asym_strength * inv_ratio;
        }
    }
    asym = clamp(asym, -0.9, 0.9);
    out.asym = asym;
    out.microprice_shift_bps = micro_shift_bps;

    const double raw_half = 0.5 * out.delta_pre_cap;
    const double raw_hd_bid = raw_half * (1.0 - asym);
    const double raw_hd_ask = raw_half * (1.0 + asym);
    const double hd_bid = half_d * (1.0 - asym);
    const double hd_ask = half_d * (1.0 + asym);

    const double raw_bid = reservation - raw_hd_bid;
    const double raw_ask = reservation + raw_hd_ask;
    const double raw_pair_spread = std::max(raw_ask - raw_bid, tick);
    out.raw_asym_shift = raw_half * asym;
    out.raw_quote_skew = raw_pair_spread > 1e-12
        ? ((raw_ask - mid) - (mid - raw_bid)) / raw_pair_spread
        : 0.0;
    double bid_price = floor_tick(reservation - hd_bid, tick);
    double ask_price = ceil_tick(reservation + hd_ask, tick);
    const double pre_guard_bid = bid_price;
    const double pre_guard_ask = ask_price;

    bool mid_guard_bid = false;
    bool mid_guard_ask = false;
    if (bid_price >= mid) {
        mid_guard_bid = true;
        bid_price = floor_tick(mid, tick);
        if (bid_price >= mid) {
            bid_price -= tick;
        }
    }
    if (ask_price <= mid) {
        mid_guard_ask = true;
        ask_price = ceil_tick(mid, tick);
        if (ask_price <= mid) {
            ask_price += tick;
        }
    }
    out.mid_guard_bid = mid_guard_bid;
    out.mid_guard_ask = mid_guard_ask;
    out.flags.mid_guard = mid_guard_bid || mid_guard_ask;

    bool post_only_bid = false;
    bool post_only_ask = false;
    if (state.best_ask > 0.0 && bid_price >= state.best_ask) {
        bid_price = state.best_ask - tick;
        post_only_bid = true;
    }
    if (state.best_bid > 0.0 && ask_price <= state.best_bid) {
        ask_price = state.best_bid + tick;
        post_only_ask = true;
    }
    out.post_only_bid = post_only_bid;
    out.post_only_ask = post_only_ask;
    out.flags.post_only = post_only_bid || post_only_ask;

    bool final_compressed = false;
    double cap_excess = 0.0;
    if (out.max_spread > 0.0) {
        const double pre_final_spread = ask_price - bid_price;
        auto capped = apply_final_spread_cap(mid, bid_price, ask_price, out.max_spread, tick);
        const bool hit = std::get<2>(capped);
        if (hit) {
            cap_excess = std::get<3>(capped);
            out.flags.cap_hit = true;
            if (cap_mode == 0) {
                bid_price = std::get<0>(capped);
                ask_price = std::get<1>(capped);
                final_compressed = true;
                out.final_cap_rounding = pre_final_spread <= out.max_spread + 2.0 * tick + 1e-12;
                out.final_cap_mid_guard = out.flags.mid_guard;
                out.final_cap_post_only = out.flags.post_only;
                out.final_cap_delta = out.flags.delta_cap;
            } else if (cap_mode == 1) {
                cap_exposure_block = true;
            }
        }
    }
    out.final_cap_excess = cap_excess;

    const auto bid_adverse = side_adverse_state<Side::Buy>(
        q, cfg.order_size, cfg.lot_size, dir_signal, pred.ret_10s, pred.tox_bid, state.mo_ema_bid,
        state.bid_adverse_markout_pause_latch, micro_shift_bps, near_depth, cfg
    );
    const auto ask_adverse = side_adverse_state<Side::Sell>(
        q, cfg.order_size, cfg.lot_size, dir_signal, pred.ret_10s, pred.tox_ask, state.mo_ema_ask,
        state.ask_adverse_markout_pause_latch, micro_shift_bps, near_depth, cfg
    );
    const auto bid_defense = side_defense_state<Side::Buy>(
        q, cfg.max_inventory, dir_signal, pred.ret_10s, state.mo_ema_bid,
        micro_shift_bps, state.unrealized_pnl, cfg
    );
    const auto ask_defense = side_defense_state<Side::Sell>(
        q, cfg.max_inventory, dir_signal, pred.ret_10s, state.mo_ema_ask,
        micro_shift_bps, state.unrealized_pnl, cfg
    );

    if (bid_adverse.active && !bid_adverse.pause) {
        const double bid_dist = std::max(mid - bid_price, tick);
        bid_price = floor_tick(mid - bid_dist * bid_adverse.spread_mult, tick);
        if (bid_price >= mid) {
            bid_price = floor_tick(mid, tick);
            if (bid_price >= mid) {
                bid_price -= tick;
            }
        }
    }
    if (ask_adverse.active && !ask_adverse.pause) {
        const double ask_dist = std::max(ask_price - mid, tick);
        ask_price = ceil_tick(mid + ask_dist * ask_adverse.spread_mult, tick);
        if (ask_price <= mid) {
            ask_price = ceil_tick(mid, tick);
            if (ask_price <= mid) {
                ask_price += tick;
            }
        }
    }

    const double pair_spread = std::max(ask_price - bid_price, tick);
    const double final_quote_skew = pair_spread > 1e-12
        ? ((ask_price - mid) - (mid - bid_price)) / pair_spread
        : 0.0;

    fill_side_context<Side::Buy>(
        out.buy, bid_adverse, bid_defense, raw_bid, pre_guard_bid, bid_price,
        mid, state.best_bid, state.best_ask, pair_spread, mid_guard_bid, post_only_bid
    );
    fill_side_context<Side::Sell>(
        out.sell, ask_adverse, ask_defense, raw_ask, pre_guard_ask, ask_price,
        mid, state.best_bid, state.best_ask, pair_spread, mid_guard_ask, post_only_ask
    );
    out.buy.final_quote_skew = final_quote_skew;
    out.sell.final_quote_skew = final_quote_skew;
    out.buy.near_depth_total = near_depth;
    out.sell.near_depth_total = near_depth;
    out.buy.cap_exposure_block = cap_exposure_block && bid_adverse.exposure_increasing;
    out.sell.cap_exposure_block = cap_exposure_block && ask_adverse.exposure_increasing;

    out.bid_price = bid_price;
    out.ask_price = ask_price;
    out.spread = pair_spread;
    out.raw_half_spread = raw_half;
    out.capped_half_spread = 0.5 * delta_after_cap;
    out.raw_mid_shift = 0.5 * (raw_bid + raw_ask) - fair;
    out.fair = fair;
    out.flags.final_compressed = final_compressed;
    out.flags.bid_adverse = bid_adverse.active;
    out.flags.ask_adverse = ask_adverse.active;
    out.flags.defense_guard = bid_defense.active || ask_defense.active;
    out.flags.cap_exposure_block = cap_exposure_block;

    return out;
}

}  // namespace narrowgate_cpp
