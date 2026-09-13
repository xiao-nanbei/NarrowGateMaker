"""Outcome-blind causal features for the multichannel cooldown v2 study.

The module owns only window, clock, EMA, missing-state, and action-context
semantics.  It never reads cooldown rewards or chooses a duration.  Economic
labels and the ordered Boolean learner live in later, hash-bound stages.
"""

from __future__ import annotations

import copy
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from enum import IntEnum
from itertools import combinations
from numbers import Integral, Real
from typing import Any, Literal

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"
SCHEMA_VERSION = f"{IDENTITY}.feature_state.v2"

# This is the admitted BBO/L2 data grid, not an economic action horizon.
BASE_WINDOW_WIDTH_NS = 100_000_000
MAX_EXPLICIT_WINDOW_COUNT = 864_000
TOP_K_DEPTH_LEVELS = 20
PRICE_TICK_SIZE_USDC_PER_BTC = 0.1
EMA_HALF_LIVES_S = (
    0.5,
    1.0,
    2.0,
    4.0,
    8.0,
    16.0,
    32.0,
    64.0,
    128.0,
    256.0,
)

CONTROL_SECONDS_PER_FILL_UNIT = 85.0
BUY_DURATION_POLICY_IDS = (
    "CONTROL_85N",
    "FIXED_79S",
    "FIXED_173S",
    "FIXED_223S",
    "FIXED_356S",
    "FIXED_640S",
    "FIXED_709S",
    "FIXED_2048S",
)
SELL_DURATION_POLICY_IDS = (
    "CONTROL_85N",
    "FIXED_79S",
    "FIXED_166S",
    "FIXED_211S",
    "FIXED_349S",
    "FIXED_660S",
    "FIXED_686S",
    "FIXED_1748S",
)


def fit_cumulative_depth_shape(
    prices: Sequence[Any],
    quantities: Sequence[Any],
    *,
    side: Literal["bid", "ask"],
) -> tuple[float, float] | None:
    """Fit cumulative depth without an SVD for every 100ms book window.

    The three-parameter model is identical to ``lstsq([1, x, .5*x^2], C)``.
    Centering and scaling ``x`` keeps the explicit 3x3 normal-equation solve
    stable while avoiding millions of tiny LAPACK calls in strict-native
    feature replay.
    """

    if side not in {"bid", "ask"}:
        raise FeatureContractError("depth-shape side must be bid or ask")
    if len(prices) != TOP_K_DEPTH_LEVELS or len(quantities) != TOP_K_DEPTH_LEVELS:
        return None
    try:
        first_price = float(prices[0])
    except (TypeError, ValueError, IndexError):
        return None
    if not math.isfinite(first_price) or first_price <= 0.0:
        return None

    rows: list[tuple[float, float]] = []
    cumulative = 0.0
    previous_distance = -math.inf
    for raw_price, raw_quantity in zip(prices, quantities, strict=True):
        try:
            price = float(raw_price)
            quantity = float(raw_quantity)
        except (TypeError, ValueError):
            return None
        if (
            not math.isfinite(price)
            or not math.isfinite(quantity)
            or price <= 0.0
            or quantity < 0.0
        ):
            return None
        distance = first_price - price if side == "bid" else price - first_price
        if distance <= previous_distance:
            return None
        previous_distance = distance
        cumulative += quantity
        rows.append((distance, cumulative))

    count = float(TOP_K_DEPTH_LEVELS)
    mean_distance = sum(distance for distance, _ in rows) / count
    scale = max(abs(distance - mean_distance) for distance, _ in rows)
    if not math.isfinite(scale) or scale <= 0.0:
        return None

    sum_u = 0.0
    sum_u2 = 0.0
    sum_u3 = 0.0
    sum_u4 = 0.0
    sum_y = 0.0
    sum_uy = 0.0
    sum_u2y = 0.0
    for distance, cumulative_depth in rows:
        u = (distance - mean_distance) / scale
        u2 = u * u
        sum_u += u
        sum_u2 += u2
        sum_u3 += u2 * u
        sum_u4 += u2 * u2
        sum_y += cumulative_depth
        sum_uy += u * cumulative_depth
        sum_u2y += u2 * cumulative_depth

    # Symmetric normal matrix for columns [1, u, u^2].
    a = count
    b = sum_u
    c = sum_u2
    d = sum_u2
    e = sum_u3
    f = sum_u4
    determinant = (
        a * (d * f - e * e)
        - b * (b * f - c * e)
        + c * (b * e - c * d)
    )
    if not math.isfinite(determinant) or abs(determinant) <= 1e-12:
        return None

    inverse_11 = a * f - c * c
    inverse_12 = b * c - a * e
    inverse_22 = a * d - b * b
    linear_u = (
        (c * e - b * f) * sum_y
        + inverse_11 * sum_uy
        + inverse_12 * sum_u2y
    ) / determinant
    quadratic_u = (
        (b * e - c * d) * sum_y
        + inverse_12 * sum_uy
        + inverse_22 * sum_u2y
    ) / determinant

    scale2 = scale * scale
    slope = linear_u / scale - 2.0 * quadratic_u * mean_distance / scale2
    convexity = 2.0 * quadratic_u / scale2
    if not math.isfinite(slope) or not math.isfinite(convexity):
        return None
    return float(slope), float(convexity)


class FeatureContractError(ValueError):
    """Raised when a causal feature or action-context invariant is violated."""


class TriState(IntEnum):
    """Boolean state with an explicit unobserved value."""

    UNOBSERVED = -1
    FALSE = 0
    TRUE = 1


def tri_not(value: TriState | int) -> TriState:
    state = TriState(value)
    if state is TriState.UNOBSERVED:
        return state
    return TriState.TRUE if state is TriState.FALSE else TriState.FALSE


def tri_and(values: Sequence[TriState | int]) -> TriState:
    states = tuple(TriState(value) for value in values)
    if not states:
        raise FeatureContractError("AND requires at least one literal")
    if TriState.FALSE in states:
        return TriState.FALSE
    if TriState.UNOBSERVED in states:
        return TriState.UNOBSERVED
    return TriState.TRUE


def tri_or(values: Sequence[TriState | int]) -> TriState:
    states = tuple(TriState(value) for value in values)
    if not states:
        raise FeatureContractError("OR requires at least one clause")
    if TriState.TRUE in states:
        return TriState.TRUE
    if TriState.UNOBSERVED in states:
        return TriState.UNOBSERVED
    return TriState.FALSE


@dataclass(frozen=True, slots=True)
class CausalWindowContract:
    """Frozen source-window semantics for v2."""

    base_window_width_ns: int = BASE_WINDOW_WIDTH_NS
    maximum_explicit_window_count: int = MAX_EXPLICIT_WINDOW_COUNT
    boundary: str = "left_closed_right_open"
    partial_current_window: str = "exclude"
    ema_semantics: str = "standard_recursive_with_hash_bound_checkpoint"
    decision_clock: str = "strategy_visible_exposure_fill_callback"
    visibility_clock: str = "feature_ready_ts_ns"
    gap_policy: str = "unobserved_no_forward_fill"
    restart_policy: str = "restore_bound_checkpoint_else_control_during_warmup"

    def __post_init__(self) -> None:
        if self.base_window_width_ns <= 0:
            raise FeatureContractError("base window width must be positive")
        if self.maximum_explicit_window_count <= 0:
            raise FeatureContractError("maximum window count must be positive")
        if self.boundary != "left_closed_right_open":
            raise FeatureContractError("v2 window boundary drifted")
        if self.partial_current_window != "exclude":
            raise FeatureContractError("partial current windows must be excluded")


@dataclass(frozen=True, slots=True)
class ChannelSpec:
    name: str
    block: str
    unit: str
    side_transform: str = "identity"

    def __post_init__(self) -> None:
        if self.block not in {"R0", "M1", "M2"}:
            raise FeatureContractError(f"unsupported channel block: {self.block}")
        if self.side_transform not in {"identity", "maker_favorable"}:
            raise FeatureContractError(
                f"unsupported side transform: {self.side_transform}"
            )


R0_CHANNELS = (
    ChannelSpec("mid_usdc_per_btc", "R0", "USDC_per_BTC", "maker_favorable"),
)

M1_CHANNELS = (
    *R0_CHANNELS,
    ChannelSpec("spread_bps", "M1", "bps"),
    ChannelSpec("best_bid_qty_btc", "M1", "BTC"),
    ChannelSpec("best_ask_qty_btc", "M1", "BTC"),
    ChannelSpec("bbo_imbalance", "M1", "unitless", "maker_favorable"),
    ChannelSpec(
        "microprice_deviation_bps", "M1", "bps", "maker_favorable"
    ),
)

M2_CHANNELS = (
    *M1_CHANNELS,
    ChannelSpec("aggressive_buy_qty_btc_per_s", "M2", "BTC_per_s"),
    ChannelSpec("aggressive_sell_qty_btc_per_s", "M2", "BTC_per_s"),
    ChannelSpec("signed_flow_imbalance", "M2", "unitless", "maker_favorable"),
    ChannelSpec("trade_count_per_s", "M2", "events_per_s"),
    ChannelSpec("buy_run_length", "M2", "events"),
    ChannelSpec("sell_run_length", "M2", "events"),
    ChannelSpec("last_aggressive_buy_age_s", "M2", "s"),
    ChannelSpec("last_aggressive_sell_age_s", "M2", "s"),
    ChannelSpec("topk_bid_depth_btc", "M2", "BTC"),
    ChannelSpec("topk_ask_depth_btc", "M2", "BTC"),
    ChannelSpec("depth_imbalance", "M2", "unitless", "maker_favorable"),
    ChannelSpec("bid_depth_slope_btc_per_tick", "M2", "BTC_per_tick"),
    ChannelSpec("ask_depth_slope_btc_per_tick", "M2", "BTC_per_tick"),
    ChannelSpec(
        "bid_depth_convexity_btc_per_tick2", "M2", "BTC_per_tick2"
    ),
    ChannelSpec(
        "ask_depth_convexity_btc_per_tick2", "M2", "BTC_per_tick2"
    ),
    ChannelSpec(
        "topk_bid_displayed_depth_increase_btc_per_s", "M2", "BTC_per_s"
    ),
    ChannelSpec(
        "topk_bid_displayed_depth_decrease_btc_per_s", "M2", "BTC_per_s"
    ),
    ChannelSpec(
        "topk_ask_displayed_depth_increase_btc_per_s", "M2", "BTC_per_s"
    ),
    ChannelSpec(
        "topk_ask_displayed_depth_decrease_btc_per_s", "M2", "BTC_per_s"
    ),
    ChannelSpec(
        "bid_exact_level_displayed_depletion_btc_per_s",
        "M2",
        "BTC_per_s",
    ),
    ChannelSpec(
        "bid_exact_level_displayed_refill_btc_per_s",
        "M2",
        "BTC_per_s",
    ),
    ChannelSpec(
        "ask_exact_level_displayed_depletion_btc_per_s",
        "M2",
        "BTC_per_s",
    ),
    ChannelSpec(
        "ask_exact_level_displayed_refill_btc_per_s",
        "M2",
        "BTC_per_s",
    ),
)

# Registered ideas that deliberately remain outside the first active M2
# schema.  The repository has no causal, unit-matching formula for these
# fields yet; emitting made-up zeros would turn missing support into evidence.
M2_DEFERRED_CHANNELS = {
    "arrival_tempo_hz": "no definition distinct from trade_count_per_s",
    "trade_price_impact_bps": "no causal impact estimand",
    "trade_absorption_ratio": "no causal absorption estimand",
    "within_window_mid_change_bps": (
        "normalized BBO retains one final accepted state per 100ms bucket, "
        "not both a within-window opening and closing mid"
    ),
    "flow_aligned_within_window_mid_change_bps": (
        "within-window mid change is unavailable and provider-local book "
        "time cannot be joined to official exchange-time trades"
    ),
    "causal_cancel_attributed_depletion_btc_per_s": (
        "raw native level decreases are observed, but public messages do not "
        "identify cancel ownership separately from trades or replacement"
    ),
    "liquidity_owner_refill_btc_per_s": (
        "raw native level increases are observed, but public messages do not "
        "identify liquidity-owner refill"
    ),
}

CHANNELS_BY_BLOCK = {
    "R0": R0_CHANNELS,
    "M1": M1_CHANNELS,
    "M2": M2_CHANNELS,
}

M0_REQUIRED_FIELDS = (
    "assignment_ts_ns",
    "fill_visible_ts_ns",
    "side",
    "role_at_fill",
    "inventory_before_fill_btc",
    "inventory_after_fill_btc",
    "fill_qty_btc",
    "order_qty_btc",
    "cumulative_filled_qty_before_btc",
    "cumulative_filled_qty_after_btc",
    "remaining_order_qty_after_btc",
    "partial_fill_ordinal",
    "fill_is_partial",
    "order_age_s",
    "queue_ahead_before_fill_btc",
    "queue_state_before_fill",
    "target_price_tick",
    "target_price_displayed_qty_btc",
    "target_price_displayed_qty_status",
    "target_price_displayed_qty_known",
    "target_price_displayed_qty_is_queue_ahead",
    "consecutive_units_after",
    "baseline_duration_ms",
    "campaign_age_s",
    "campaign_add_count",
    "campaign_mae_to_date_usdc",
    "campaign_inventory_time_to_date_btc_s",
    "last_same_side_fill_age_s",
    "last_opposite_side_fill_age_s",
    "cooldown_remaining_ms",
    "cooldown_blocker_active",
    "cooldown_lineage_revision_before",
    "cooldown_deadline_owner",
)

# These ages are genuinely undefined before the corresponding event has ever
# occurred.  Encoding that state as zero would falsely mean "just happened".
M0_NULLABLE_FIELDS = (
    "last_same_side_fill_age_s",
    "last_opposite_side_fill_age_s",
    "queue_ahead_before_fill_btc",
    "target_price_displayed_qty_btc",
)

COOLDOWN_DEADLINE_OWNER_NONE = "none"
COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE = (
    "existing_same_side_lineage"
)
COOLDOWN_DEADLINE_OWNER_CATEGORIES = (
    COOLDOWN_DEADLINE_OWNER_NONE,
    COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE,
)
_RAW_COOLDOWN_DEADLINE_OWNER_RE = re.compile(
    r"^(buy|sell)-lineage-([1-9][0-9]*)$"
)


def ema_pairs(
    half_lives_s: Sequence[float] = EMA_HALF_LIVES_S,
) -> tuple[tuple[float, float], ...]:
    values = tuple(float(value) for value in half_lives_s)
    if values != tuple(sorted(set(values))) or not values:
        raise FeatureContractError("EMA half-lives must be unique and increasing")
    if any(not math.isfinite(value) or value <= 0.0 for value in values):
        raise FeatureContractError("EMA half-lives must be positive and finite")
    return tuple(combinations(values, 2))


def _label(value: float) -> str:
    return f"h{float(value):g}s".replace(".", "p")


def pair_key(channel: str, fast: float, slow: float) -> str:
    if fast >= slow:
        raise FeatureContractError("EMA pair requires fast < slow")
    return f"{channel}__{_label(fast)}__{_label(slow)}"


def _finite_float(row: Mapping[str, Any], name: str) -> float:
    try:
        raw = row[name]
        if isinstance(raw, bool) or not isinstance(raw, Real):
            raise TypeError(name)
        value = float(raw)
    except (KeyError, TypeError, ValueError) as exc:
        raise FeatureContractError(f"missing or invalid M0 field: {name}") from exc
    if not math.isfinite(value):
        raise FeatureContractError(f"nonfinite M0 field: {name}")
    return value


def _nonnegative_int(row: Mapping[str, Any], name: str) -> int:
    value = row[name]
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise FeatureContractError(f"M0 field must be an integer: {name}")
    parsed = int(value)
    if parsed < 0:
        raise FeatureContractError(f"M0 field must be non-negative: {name}")
    return parsed


def _normalize_cooldown_deadline_owner(
    *,
    raw_owner: Any,
    side: str,
    blocker_active: bool,
    cooldown_remaining_ms: float,
    lineage_revision_before: int,
) -> str:
    owner = str(raw_owner).strip().lower()
    remaining_active = cooldown_remaining_ms > 0.0
    if blocker_active != remaining_active:
        raise FeatureContractError(
            "cooldown blocker disagrees with cooldown remaining time"
        )
    if not blocker_active:
        if owner != COOLDOWN_DEADLINE_OWNER_NONE:
            raise FeatureContractError(
                "inactive cooldown must have no deadline owner"
            )
        return COOLDOWN_DEADLINE_OWNER_NONE
    if lineage_revision_before < 1:
        raise FeatureContractError(
            "active cooldown requires an existing lineage revision"
        )
    if owner == COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE:
        return owner
    match = _RAW_COOLDOWN_DEADLINE_OWNER_RE.fullmatch(owner)
    if match is None:
        raise FeatureContractError(
            "active cooldown deadline owner is not a supported lineage identity"
        )
    owner_side, owner_revision = match.groups()
    if owner_side != side.lower() or int(owner_revision) != lineage_revision_before:
        raise FeatureContractError(
            "cooldown deadline owner disagrees with side or lineage revision"
        )
    return COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE


def validate_m0_context(row: Mapping[str, Any]) -> dict[str, Any]:
    """Validate decision-visible action magnitude and campaign context."""

    extra = sorted(set(row) - set(M0_REQUIRED_FIELDS))
    if extra:
        raise FeatureContractError(f"M0 context has unknown fields: {extra}")
    missing = [name for name in M0_REQUIRED_FIELDS if name not in row]
    if missing:
        raise FeatureContractError(f"M0 context is incomplete: {missing}")
    output = {name: row[name] for name in M0_REQUIRED_FIELDS}
    side = str(row["side"]).upper()
    role = str(row["role_at_fill"]).lower()
    if side not in {"BUY", "SELL"} or role not in {"opener", "add"}:
        raise FeatureContractError("M0 supports only BUY/SELL opener/add fills")
    assignment_ts = int(row["assignment_ts_ns"])
    fill_visible_ts = int(row["fill_visible_ts_ns"])
    if assignment_ts <= 0 or fill_visible_ts <= 0 or assignment_ts < fill_visible_ts:
        raise FeatureContractError("assignment/fill visibility clocks are invalid")
    before = _finite_float(row, "inventory_before_fill_btc")
    after = _finite_float(row, "inventory_after_fill_btc")
    fill_qty = _finite_float(row, "fill_qty_btc")
    order_qty = _finite_float(row, "order_qty_btc")
    cumulative_before = _finite_float(row, "cumulative_filled_qty_before_btc")
    cumulative_after = _finite_float(row, "cumulative_filled_qty_after_btc")
    remaining_after = _finite_float(row, "remaining_order_qty_after_btc")
    units = _finite_float(row, "consecutive_units_after")
    baseline_ms = _finite_float(row, "baseline_duration_ms")
    if fill_qty <= 0.0 or units <= 0.0:
        raise FeatureContractError("exposure fill quantity/units must be positive")
    if order_qty <= 0.0 or min(cumulative_before, cumulative_after, remaining_after) < 0.0:
        raise FeatureContractError("order/partial-fill quantities are invalid")
    if not math.isclose(
        cumulative_before + fill_qty,
        cumulative_after,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise FeatureContractError("partial-fill cumulative quantity drifted")
    if not math.isclose(
        cumulative_after + remaining_after,
        order_qty,
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise FeatureContractError("partial-fill remaining quantity drifted")
    partial_ordinal = _nonnegative_int(row, "partial_fill_ordinal")
    if partial_ordinal < 1:
        raise FeatureContractError("partial_fill_ordinal must be >= 1")
    output["partial_fill_ordinal"] = partial_ordinal
    if type(row["fill_is_partial"]) is not bool:
        raise FeatureContractError("fill_is_partial must be bool")
    if bool(row["fill_is_partial"]) != (remaining_after > 1e-12):
        raise FeatureContractError("fill_is_partial disagrees with remaining quantity")
    output["fill_is_partial"] = bool(row["fill_is_partial"])
    if not math.isclose(abs(after - before), fill_qty, abs_tol=1e-12):
        raise FeatureContractError("fill quantity does not match inventory transition")
    if side == "BUY" and not (before >= -1e-12 and after > before):
        raise FeatureContractError("BUY opportunity is not exposure increasing")
    if side == "SELL" and not (before <= 1e-12 and after < before):
        raise FeatureContractError("SELL opportunity is not exposure increasing")
    expected_role = "opener" if abs(before) <= 1e-12 else "add"
    if role != expected_role:
        raise FeatureContractError("opener/add role disagrees with pre-fill inventory")
    # The deployed control keeps a one-unit floor for partial first fills.
    expected_baseline_ms = (
        CONTROL_SECONDS_PER_FILL_UNIT * max(1.0, units) * 1_000.0
    )
    if not math.isclose(baseline_ms, expected_baseline_ms, abs_tol=1e-6):
        raise FeatureContractError("baseline duration is not CONTROL_85N")
    for name in (
        "campaign_age_s",
        "campaign_inventory_time_to_date_btc_s",
        "order_age_s",
        "cooldown_remaining_ms",
    ):
        if _finite_float(row, name) < 0.0:
            raise FeatureContractError(f"M0 field must be non-negative: {name}")
    for name in M0_NULLABLE_FIELDS:
        if row[name] is None:
            output[name] = None
            continue
        if _finite_float(row, name) < 0.0:
            raise FeatureContractError(f"M0 field must be non-negative: {name}")
    queue_state = str(row["queue_state_before_fill"]).strip().lower()
    if queue_state not in {"exact", "known_zero", "unknown"}:
        raise FeatureContractError("queue_state_before_fill is invalid")
    queue_ahead = output["queue_ahead_before_fill_btc"]
    if queue_state == "unknown" and queue_ahead is not None:
        raise FeatureContractError("unknown queue state must not invent queue ahead")
    if queue_state in {"exact", "known_zero"} and queue_ahead is None:
        raise FeatureContractError("known queue state requires queue ahead")
    if queue_state == "known_zero" and not math.isclose(
        float(queue_ahead), 0.0, rel_tol=0.0, abs_tol=1e-12
    ):
        raise FeatureContractError("known_zero queue state requires zero queue ahead")
    output["queue_state_before_fill"] = queue_state
    target_tick = _nonnegative_int(row, "target_price_tick")
    if target_tick < 1:
        raise FeatureContractError("target_price_tick must be >= 1")
    output["target_price_tick"] = target_tick
    target_status = str(row["target_price_displayed_qty_status"]).strip().lower()
    if target_status not in {"exact", "known_zero", "unknown"}:
        raise FeatureContractError(
            "target_price_displayed_qty_status is invalid"
        )
    if type(row["target_price_displayed_qty_known"]) is not bool:
        raise FeatureContractError(
            "target_price_displayed_qty_known must be bool"
        )
    target_known = bool(row["target_price_displayed_qty_known"])
    if target_known != (target_status in {"exact", "known_zero"}):
        raise FeatureContractError(
            "target displayed-quantity known flag disagrees with status"
        )
    target_qty = row["target_price_displayed_qty_btc"]
    if target_status == "unknown":
        if target_qty is not None:
            raise FeatureContractError(
                "unknown target displayed quantity must be null"
            )
        output["target_price_displayed_qty_btc"] = None
    else:
        parsed_target_qty = _finite_float(
            row, "target_price_displayed_qty_btc"
        )
        if parsed_target_qty < 0.0:
            raise FeatureContractError(
                "target displayed quantity must be non-negative"
            )
        if target_status == "known_zero" and not math.isclose(
            parsed_target_qty, 0.0, rel_tol=0.0, abs_tol=1e-12
        ):
            raise FeatureContractError(
                "known_zero target displayed quantity must be zero"
            )
        output["target_price_displayed_qty_btc"] = parsed_target_qty
    if type(row["target_price_displayed_qty_is_queue_ahead"]) is not bool:
        raise FeatureContractError(
            "target_price_displayed_qty_is_queue_ahead must be bool"
        )
    if bool(row["target_price_displayed_qty_is_queue_ahead"]):
        raise FeatureContractError(
            "displayed target quantity cannot claim queue-ahead identity"
        )
    output["target_price_displayed_qty_status"] = target_status
    output["target_price_displayed_qty_known"] = target_known
    output["target_price_displayed_qty_is_queue_ahead"] = False
    output["campaign_add_count"] = _nonnegative_int(row, "campaign_add_count")
    lineage_revision_before = _nonnegative_int(
        row, "cooldown_lineage_revision_before"
    )
    output["cooldown_lineage_revision_before"] = lineage_revision_before
    if type(row["cooldown_blocker_active"]) is not bool:
        raise FeatureContractError("cooldown_blocker_active must be bool")
    blocker_active = bool(row["cooldown_blocker_active"])
    output["cooldown_blocker_active"] = blocker_active
    _finite_float(row, "campaign_mae_to_date_usdc")
    output["cooldown_deadline_owner"] = _normalize_cooldown_deadline_owner(
        raw_owner=row["cooldown_deadline_owner"],
        side=side,
        blocker_active=blocker_active,
        cooldown_remaining_ms=_finite_float(row, "cooldown_remaining_ms"),
        lineage_revision_before=lineage_revision_before,
    )
    output["side"] = side
    output["role_at_fill"] = role
    return output


@dataclass(frozen=True, slots=True)
class CausalWindowObservation:
    """One completed market window made visible at a causal ready clock."""

    left_ts_ns: int
    right_ts_ns: int
    feature_ready_ts_ns: int
    market_generation: int
    depth_generation: int
    values: Mapping[str, float | None]
    source_gap: bool = False
    source_stale: bool = False
    warmup_admitted: bool = False


@dataclass(slots=True)
class _PairState:
    effective_sign: int = 0
    arrangement_start_ts_ns: int | None = None
    last_cross_ts_ns: int | None = None
    last_cross_direction: int = 0


class _ChannelEmaState:
    def __init__(self, spec: ChannelSpec, half_lives_s: Sequence[float]) -> None:
        self.spec = spec
        self.half_lives_s = tuple(float(value) for value in half_lives_s)
        self.pairs = ema_pairs(self.half_lives_s)
        self._index = {value: index for index, value in enumerate(self.half_lives_s)}
        self.ema: list[float] = []
        self.velocity: list[float] = []
        self.acceleration: list[float] = []
        self.last_value: float | None = None
        self.last_ts_ns: int | None = None
        self.current_window_observed = False
        self.pair_state = {pair: _PairState() for pair in self.pairs}

    def update(self, *, ts_ns: int, value: float | None, observed: bool) -> None:
        self.current_window_observed = bool(observed)
        if not observed:
            return
        if value is None or not math.isfinite(float(value)):
            raise FeatureContractError(f"observed channel is nonfinite: {self.spec.name}")
        timestamp = int(ts_ns)
        x = float(value)
        if self.last_ts_ns is None:
            self.ema = [x] * len(self.half_lives_s)
            self.velocity = [0.0] * len(self.half_lives_s)
            self.acceleration = [0.0] * len(self.half_lives_s)
            self.last_value = x
            self.last_ts_ns = timestamp
            return
        if timestamp <= self.last_ts_ns:
            raise FeatureContractError("channel EMA clock must increase")
        delta_s = float(timestamp - self.last_ts_ns) / 1_000_000_000.0
        prior = tuple(self.ema)
        prior_velocity = tuple(self.velocity)
        for index, half_life in enumerate(self.half_lives_s):
            decay = math.exp(-math.log(2.0) * delta_s / half_life)
            current = decay * prior[index] + (1.0 - decay) * x
            self.ema[index] = current
            self.velocity[index] = (current - prior[index]) / delta_s
            self.acceleration[index] = (
                self.velocity[index] - prior_velocity[index]
            ) / delta_s
        for fast, slow in self.pairs:
            distance = self.ema[self._index[fast]] - self.ema[self._index[slow]]
            raw_sign = 1 if distance > 0.0 else -1 if distance < 0.0 else 0
            state = self.pair_state[(fast, slow)]
            if raw_sign:
                if state.effective_sign == 0:
                    state.effective_sign = raw_sign
                    state.arrangement_start_ts_ns = timestamp
                elif raw_sign != state.effective_sign:
                    state.effective_sign = raw_sign
                    state.arrangement_start_ts_ns = timestamp
                    state.last_cross_ts_ns = timestamp
                    state.last_cross_direction = raw_sign
        self.last_value = x
        self.last_ts_ns = timestamp

    def snapshot(self, *, side: str, decision_ts_ns: int) -> dict[str, Any]:
        output: dict[str, Any] = {}
        observed = bool(
            self.current_window_observed
            and self.last_ts_ns is not None
            and self.last_ts_ns <= int(decision_ts_ns)
        )
        output[f"channel::{self.spec.name}::observed"] = int(observed)
        if not observed:
            for fast, slow in self.pairs:
                prefix = pair_key(self.spec.name, fast, slow)
                output[f"tri::{prefix}::positive_ordering"] = int(
                    TriState.UNOBSERVED
                )
                output[f"tri::{prefix}::last_cross_positive"] = int(
                    TriState.UNOBSERVED
                )
            return output
        side_sign = 1.0 if str(side).upper() == "BUY" else -1.0
        transform = side_sign if self.spec.side_transform == "maker_favorable" else 1.0
        for half_life, value, velocity, acceleration in zip(
            self.half_lives_s,
            self.ema,
            self.velocity,
            self.acceleration,
            strict=True,
        ):
            label = _label(half_life)
            output[f"value::{self.spec.name}::ema::{label}"] = transform * value
            output[f"value::{self.spec.name}::slope::{label}"] = transform * velocity
            output[f"value::{self.spec.name}::curvature::{label}"] = (
                transform * acceleration
            )
        for fast, slow in self.pairs:
            prefix = pair_key(self.spec.name, fast, slow)
            fast_i = self._index[fast]
            slow_i = self._index[slow]
            raw_distance = self.ema[fast_i] - self.ema[slow_i]
            signed_distance = transform * raw_distance
            signed_velocity = transform * (
                self.velocity[fast_i] - self.velocity[slow_i]
            )
            raw_acceleration = self.acceleration[fast_i] - self.acceleration[slow_i]
            signed_acceleration = transform * raw_acceleration
            state = self.pair_state[(fast, slow)]
            ordering_sign = int(math.copysign(1, transform)) * state.effective_sign
            if ordering_sign == 0:
                ordering = TriState.UNOBSERVED
            else:
                ordering = TriState.TRUE if ordering_sign > 0 else TriState.FALSE
            output[f"tri::{prefix}::positive_ordering"] = int(ordering)
            if state.last_cross_ts_ns is None:
                last_cross = TriState.UNOBSERVED
                cross_age_s: float | None = None
            else:
                last_cross = (
                    TriState.TRUE
                    if transform * state.last_cross_direction > 0
                    else TriState.FALSE
                )
                cross_age_s = (
                    int(decision_ts_ns) - state.last_cross_ts_ns
                ) / 1_000_000_000.0
            output[f"tri::{prefix}::last_cross_positive"] = int(last_cross)
            output[f"value::{prefix}::cross_age_s"] = cross_age_s
            output[f"value::{prefix}::arrangement_persistence_s"] = (
                None
                if state.arrangement_start_ts_ns is None
                else (
                    int(decision_ts_ns) - state.arrangement_start_ts_ns
                )
                / 1_000_000_000.0
            )
            output[f"value::{prefix}::signed_distance"] = signed_distance
            output[f"value::{prefix}::abs_distance"] = abs(raw_distance)
            output[f"value::{prefix}::signed_distance_velocity"] = signed_velocity
            output[f"value::{prefix}::signed_distance_acceleration"] = (
                signed_acceleration
            )
            expansion_product = raw_distance * (
                self.velocity[fast_i] - self.velocity[slow_i]
            )
            output[f"tri::{prefix}::expanding"] = int(
                TriState.TRUE
                if expansion_product > 0
                else TriState.FALSE
            )
            output[f"tri::{prefix}::converging"] = int(
                TriState.TRUE if expansion_product < 0 else TriState.FALSE
            )
        return output


class CausalMultichannelEmaState:
    """Recursive multichannel EMA state updated only by completed windows."""

    def __init__(
        self,
        *,
        block: str,
        contract: CausalWindowContract | None = None,
        half_lives_s: Sequence[float] = EMA_HALF_LIVES_S,
        warmup_admitted: bool = False,
        warmup_identity: str = "",
    ) -> None:
        if block not in CHANNELS_BY_BLOCK:
            raise FeatureContractError(f"unsupported feature block: {block}")
        self.block = block
        self.contract = contract or CausalWindowContract()
        self.half_lives_s = tuple(float(value) for value in half_lives_s)
        self.channels = {
            spec.name: _ChannelEmaState(spec, self.half_lives_s)
            for spec in CHANNELS_BY_BLOCK[block]
        }
        self.warmup_admitted = bool(warmup_admitted)
        self.warmup_identity = str(warmup_identity)
        if self.warmup_admitted and not self.warmup_identity:
            raise FeatureContractError("admitted warmup requires a bound identity")
        self.last_right_ts_ns: int | None = None
        self.last_feature_ready_ts_ns: int | None = None
        self.last_market_generation: int | None = None
        self.last_depth_generation: int | None = None
        self.window_count = 0
        self.gap_window_count = 0

    def update(self, observation: CausalWindowObservation) -> None:
        width = self.contract.base_window_width_ns
        expected_channels = set(self.channels)
        actual_channels = {str(name) for name in observation.values}
        if actual_channels != expected_channels:
            raise FeatureContractError(
                "window channel schema drifted: "
                f"missing={sorted(expected_channels - actual_channels)} "
                f"extra={sorted(actual_channels - expected_channels)}"
            )
        if observation.right_ts_ns - observation.left_ts_ns != width:
            raise FeatureContractError("window width drifted")
        if observation.left_ts_ns % width or observation.right_ts_ns % width:
            raise FeatureContractError("window is not aligned to the frozen grid")
        if observation.feature_ready_ts_ns < observation.right_ts_ns:
            raise FeatureContractError("window became ready before its right edge")
        if self.last_right_ts_ns is not None:
            if observation.right_ts_ns <= self.last_right_ts_ns:
                raise FeatureContractError("window right edge did not increase")
            if observation.left_ts_ns != self.last_right_ts_ns:
                raise FeatureContractError(
                    "missing windows must be emitted explicitly as source gaps"
                )
            if observation.feature_ready_ts_ns < int(self.last_feature_ready_ts_ns):
                raise FeatureContractError("feature-ready clock regressed")
            if observation.market_generation <= int(self.last_market_generation):
                raise FeatureContractError("market generation did not increase")
            if observation.depth_generation < int(self.last_depth_generation):
                raise FeatureContractError("depth generation regressed")
        invalid_window = bool(observation.source_gap or observation.source_stale)
        if invalid_window:
            self.gap_window_count += 1
        for name, channel in self.channels.items():
            value = observation.values.get(name)
            observed = not invalid_window and value is not None
            channel.update(
                ts_ns=observation.right_ts_ns,
                value=value,
                observed=observed,
            )
        self.last_right_ts_ns = int(observation.right_ts_ns)
        self.last_feature_ready_ts_ns = int(observation.feature_ready_ts_ns)
        self.last_market_generation = int(observation.market_generation)
        self.last_depth_generation = int(observation.depth_generation)
        self.window_count += 1

    def feature_row(
        self,
        *,
        side: str,
        decision_ts_ns: int,
        m0_context: Mapping[str, Any],
    ) -> dict[str, Any]:
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise FeatureContractError("feature row requires BUY or SELL")
        if self.last_feature_ready_ts_ns is None:
            raise FeatureContractError("no completed causal window is available")
        if self.last_feature_ready_ts_ns > int(decision_ts_ns):
            raise FeatureContractError("feature-ready state crossed the decision cutoff")
        context = validate_m0_context(m0_context)
        if context["side"] != normalized_side:
            raise FeatureContractError("M0 side disagrees with feature side")
        output: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "identity": IDENTITY,
            "feature_block": self.block,
            "base_window_width_ns": self.contract.base_window_width_ns,
            "maximum_explicit_window_count": (
                self.contract.maximum_explicit_window_count
            ),
            "last_window_right_ts_ns": self.last_right_ts_ns,
            "feature_ready_ts_ns": self.last_feature_ready_ts_ns,
            "decision_ts_ns": int(decision_ts_ns),
            "market_generation": self.last_market_generation,
            "depth_generation": self.last_depth_generation,
            "window_count": self.window_count,
            "gap_window_count": self.gap_window_count,
            "warmup_admitted": self.warmup_admitted,
            "warmup_identity": self.warmup_identity,
            "support_valid": self.warmup_admitted,
            **context,
        }
        channel_support = True
        for channel in self.channels.values():
            row = channel.snapshot(side=normalized_side, decision_ts_ns=decision_ts_ns)
            channel_support &= bool(row[f"channel::{channel.spec.name}::observed"])
            output.update(row)
        output["channel_support_valid"] = channel_support
        output["support_valid"] = bool(output["support_valid"] and channel_support)
        return output

    def channel_feature_row(
        self,
        *,
        channel_name: str,
        side: str,
        decision_ts_ns: int,
    ) -> dict[str, Any]:
        """Return one channel's causal state without manufacturing M0 fields.

        Production policies that select only market-state literals use this
        narrow projection.  It shares the exact recursive EMA and crossover
        implementation with the full research feature row while keeping
        unavailable order/depth fields explicitly outside the live interface.
        """

        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise FeatureContractError("channel feature row requires BUY or SELL")
        if self.last_feature_ready_ts_ns is None:
            raise FeatureContractError("no completed causal window is available")
        if self.last_feature_ready_ts_ns > int(decision_ts_ns):
            raise FeatureContractError("feature-ready state crossed the decision cutoff")
        channel = self.channels.get(str(channel_name))
        if channel is None:
            raise FeatureContractError(f"unknown channel: {channel_name}")
        return channel.snapshot(
            side=normalized_side,
            decision_ts_ns=int(decision_ts_ns),
        )

    def mark_current_window_unobserved(self) -> None:
        """Invalidate one decision view without erasing recursive EMA history."""

        for channel in self.channels.values():
            channel.current_window_observed = False

    def checkpoint(self) -> dict[str, Any]:
        """Return a serializable state that must be hash-bound by its caller."""

        return {
            "schema_version": f"{SCHEMA_VERSION}.checkpoint",
            "identity": IDENTITY,
            "block": self.block,
            "contract": asdict(self.contract),
            "half_lives_s": list(self.half_lives_s),
            "warmup_admitted": self.warmup_admitted,
            "warmup_identity": self.warmup_identity,
            "last_right_ts_ns": self.last_right_ts_ns,
            "last_feature_ready_ts_ns": self.last_feature_ready_ts_ns,
            "last_market_generation": self.last_market_generation,
            "last_depth_generation": self.last_depth_generation,
            "window_count": self.window_count,
            "gap_window_count": self.gap_window_count,
            "channels": copy.deepcopy(
                {
                    name: {
                        "ema": state.ema,
                        "velocity": state.velocity,
                        "acceleration": state.acceleration,
                        "last_value": state.last_value,
                        "last_ts_ns": state.last_ts_ns,
                        "current_window_observed": state.current_window_observed,
                        "pair_state": {
                            f"{fast:g}|{slow:g}": asdict(pair_state)
                            for (fast, slow), pair_state in state.pair_state.items()
                        },
                    }
                    for name, state in self.channels.items()
                }
            ),
        }

    @classmethod
    def restore(cls, payload: Mapping[str, Any]) -> CausalMultichannelEmaState:
        if payload.get("identity") != IDENTITY or payload.get("schema_version") != (
            f"{SCHEMA_VERSION}.checkpoint"
        ):
            raise FeatureContractError("checkpoint identity drifted")
        instance = cls(
            block=str(payload["block"]),
            contract=CausalWindowContract(**dict(payload["contract"])),
            half_lives_s=tuple(payload["half_lives_s"]),
            warmup_admitted=bool(payload["warmup_admitted"]),
            warmup_identity=str(payload["warmup_identity"]),
        )
        instance.last_right_ts_ns = payload.get("last_right_ts_ns")
        instance.last_feature_ready_ts_ns = payload.get("last_feature_ready_ts_ns")
        instance.last_market_generation = payload.get("last_market_generation")
        instance.last_depth_generation = payload.get("last_depth_generation")
        instance.window_count = int(payload.get("window_count", 0))
        instance.gap_window_count = int(payload.get("gap_window_count", 0))
        raw_channels = payload.get("channels")
        if set(raw_channels or ()) != set(instance.channels):
            raise FeatureContractError("checkpoint channel universe drifted")
        for name, state in instance.channels.items():
            raw = raw_channels[name]
            state.ema = [float(value) for value in raw["ema"]]
            state.velocity = [float(value) for value in raw["velocity"]]
            state.acceleration = [float(value) for value in raw["acceleration"]]
            state.last_value = raw.get("last_value")
            state.last_ts_ns = raw.get("last_ts_ns")
            state.current_window_observed = bool(raw["current_window_observed"])
            expected_keys = {f"{fast:g}|{slow:g}" for fast, slow in state.pairs}
            if set(raw["pair_state"]) != expected_keys:
                raise FeatureContractError("checkpoint EMA pair universe drifted")
            for fast, slow in state.pairs:
                row = raw["pair_state"][f"{fast:g}|{slow:g}"]
                state.pair_state[(fast, slow)] = _PairState(**row)
        return instance


def feature_schema() -> dict[str, Any]:
    return {
        "identity": IDENTITY,
        "schema_version": SCHEMA_VERSION,
        "window_contract": asdict(CausalWindowContract()),
        "m0_required_fields": list(M0_REQUIRED_FIELDS),
        "m0_nullable_fields": list(M0_NULLABLE_FIELDS),
        "m0_categorical_domains": {
            "cooldown_deadline_owner": list(
                COOLDOWN_DEADLINE_OWNER_CATEGORIES
            )
        },
        "top_k_depth_levels": TOP_K_DEPTH_LEVELS,
        "price_tick_size_usdc_per_btc": PRICE_TICK_SIZE_USDC_PER_BTC,
        "depth_shape_formula": (
            "for side distance x_i in ticks and cumulative displayed depth "
            "C_i in BTC, OLS fit C_i=beta0+beta1*x_i+0.5*gamma*x_i^2; "
            "slope=beta1 BTC/tick and convexity=gamma BTC/tick^2"
        ),
        "displayed_depth_change_formula": (
            "for consecutive observed normalized buckets, increase=max(D_t-"
            "D_prev,0)/dt and decrease=max(D_prev-D_t,0)/dt in BTC/s"
        ),
        "displayed_depth_change_is_exact_depletion_refill": False,
        "blocks": {
            block: [asdict(spec) for spec in channels]
            for block, channels in CHANNELS_BY_BLOCK.items()
        },
        "ema_half_lives_s": list(EMA_HALF_LIVES_S),
        "ema_pair_count_per_channel": len(ema_pairs()),
        "cross_semantics": "three_valued_true_false_unobserved",
        "not_unobserved_semantics": "unobserved",
        "cross_channel_ema_pairs_forbidden": True,
        "deferred_m2_channels": dict(M2_DEFERRED_CHANNELS),
        "economic_outcomes_read": False,
    }


__all__ = [
    "BASE_WINDOW_WIDTH_NS",
    "BUY_DURATION_POLICY_IDS",
    "CausalMultichannelEmaState",
    "CausalWindowContract",
    "CausalWindowObservation",
    "CHANNELS_BY_BLOCK",
    "COOLDOWN_DEADLINE_OWNER_CATEGORIES",
    "COOLDOWN_DEADLINE_OWNER_EXISTING_SAME_SIDE_LINEAGE",
    "COOLDOWN_DEADLINE_OWNER_NONE",
    "CONTROL_SECONDS_PER_FILL_UNIT",
    "EMA_HALF_LIVES_S",
    "FeatureContractError",
    "IDENTITY",
    "M0_REQUIRED_FIELDS",
    "M0_NULLABLE_FIELDS",
    "MAX_EXPLICIT_WINDOW_COUNT",
    "M2_DEFERRED_CHANNELS",
    "PRICE_TICK_SIZE_USDC_PER_BTC",
    "SCHEMA_VERSION",
    "SELL_DURATION_POLICY_IDS",
    "TOP_K_DEPTH_LEVELS",
    "TriState",
    "ema_pairs",
    "feature_schema",
    "pair_key",
    "tri_and",
    "tri_not",
    "tri_or",
    "validate_m0_context",
]
