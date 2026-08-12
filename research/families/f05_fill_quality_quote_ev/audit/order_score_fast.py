"""Streaming order-level score audit for large retained-day panels.

The generic audit runner keeps the full order-level table in memory because it
also writes a reusable training table.  That is convenient for small panels but
too heavy for retained-all evidence.  This module reads the cached
``*.order_level.csv`` twice and writes only compact score sanity/daily outputs.
"""

from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from research.families.f10_live_replay_attribution.audit.metrics import (
    MICRO_MACRO_WINDOWS_S,
    ORDER_LEVEL_SCORE_COLS,
    PATH_RATIO_EPS_BPS,
    _fmt_path_feature,
    _micro_macro_regime,
    _order_scores,
)


SCORES = ORDER_LEVEL_SCORE_COLS

REPLAY_OVERLAY_FIELDS = (
    "queue_init",
    "queue_left",
    "queue_local_rank",
    "queue_regime_mult",
    "queue_mo_mult",
    "queue_deplete_mult",
    "fill_eligible",
    "lifetime_ms",
    "best_bid",
    "best_ask",
    "mid",
    "near_depth_total",
    "final_pair_spread",
    "bid_quote_fill_prob",
    "bid_quote_fill_markout_30s",
    "ask_quote_fill_prob",
    "ask_quote_fill_markout_30s",
    "xmarket_retreat_ttl_ms",
    "xmarket_retreat",
    "xmarket_retreat_pause",
    "xmarket_retreat_ttl_effective",
    "xmarket_ref_adverse",
    "xmarket_spot_adverse",
    "xmarket_ref_adverse_ret_max",
    "xmarket_spot_adverse_ret_max",
    "final_distance_to_mid",
    "l2_near_depth_total",
    "l2_book_refresh_ratio",
    "l2_book_cancel_ratio",
    "mo_ema_bid",
    "mo_ema_ask",
    "tox_bid",
    "tox_ask",
)

PATH_OVERLAY_FIELDS = tuple(
    [f"range_{w}s_bps" for w in MICRO_MACRO_WINDOWS_S]
    + [f"rv_{w}s_bps" for w in MICRO_MACRO_WINDOWS_S]
    + [f"ret_{w}s_bps" for w in MICRO_MACRO_WINDOWS_S]
    + [f"path_count_{w}s" for w in MICRO_MACRO_WINDOWS_S]
    + [
        "quote_distance_micro_5s",
        "quote_distance_micro_10s",
        "quote_distance_micro",
        "micro_macro_range_ratio",
        "micro_macro_vol_ratio",
        "inventory_horizon_range_ratio",
        "trend_efficiency_60s",
        "trend_efficiency_300s",
        "side_trend_adverse_60s_bps",
        "side_trend_adverse_300s_bps",
        "micro_macro_regime",
    ]
)


def _float(row: dict[str, str], key: str, default: float = 0.0) -> float:
    try:
        value = row.get(key, "")
        return float(value) if value not in ("", None) else default
    except (TypeError, ValueError):
        return default


def _int(row: dict[str, str], key: str, default: int = 0) -> int:
    try:
        value = row.get(key, "")
        return int(float(value)) if value not in ("", None) else default
    except (TypeError, ValueError):
        return default


def _side(row: dict[str, str]) -> str:
    text = str(row.get("side", "")).upper()
    if text in {"BUY", "BID", "LONG"}:
        return "BUY"
    if text in {"SELL", "ASK", "SHORT"}:
        return "SELL"
    return text


def _norm_ts(value: float) -> float:
    if not math.isfinite(value) or value <= 0.0:
        return 0.0
    return value / 1000.0 if value > 10_000_000_000 else value


def _fixed_bucket(v: float) -> str:
    if v < 0.33:
        return "low"
    if v < 0.66:
        return "mid"
    return "high"


def _decile_bucket(v: float) -> str:
    if not math.isfinite(v):
        return "missing"
    idx = min(9, max(0, int(v * 10.0)))
    return f"score_{idx * 10:02d}_{(idx + 1) * 10:02d}"


def _quantile_thresholds(values: list[float]) -> tuple[float, float, float]:
    if not values:
        return (float("inf"), float("inf"), float("inf"))
    values.sort()
    n = len(values)

    def pick(q: float) -> float:
        idx = min(n - 1, max(0, int(q * (n - 1))))
        return values[idx]

    return (pick(0.70), pick(0.85), pick(0.95))


def _q_bucket(v: float, thresholds: tuple[float, float, float]) -> str:
    q70, q85, q95 = thresholds
    if v < q70:
        return "q000_070"
    if v < q85:
        return "q070_085"
    if v < q95:
        return "q085_095"
    return "q095_100"


def _boolish(row: dict[str, str], key: str) -> Optional[bool]:
    text = str(row.get(key, "")).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _positive_eligibility_flags(row: dict[str, str], scores: dict[str, float | str]) -> dict[str, bool | str]:
    """Quote-time white-list style eligibility for absorptive maker fills.

    中文说明：这不是 policy，也不是“赚钱桶”。它只把用户提出的交集
    固化成 quote-time 可见条件，后续看 denominator、campaign terminal
    outcome 和 daily OOS。所有字段都必须来自挂单前状态或 replay order
    overlay，不能用 fill 后 markout/campaign label 反推。
    """
    side = row.get("side", "")
    fill_prob = float(scores.get("fill_probability_score", _float(row, "fill_probability_score")))
    fill_quality = float(scores.get("fill_quality_score", _float(row, "fill_quality_score")))
    toxic = float(scores.get("toxic_risk_score", _float(row, "toxic_risk_score")))
    campaign_outcome = float(scores.get("campaign_outcome_risk_score", _float(row, "campaign_outcome_risk_score")))
    resil = float(scores.get("resiliency_score", _float(row, "resiliency_score")))
    micro_reversion = float(scores.get("micro_reversion_score", _float(row, "micro_reversion_score")))
    trend_inventory_risk = float(scores.get("trend_inventory_risk_score", _float(row, "trend_inventory_risk_score")))

    near_depth = _float(row, "near_depth_total", float("nan"))
    refresh = _float(row, "l2_book_refresh_ratio", float("nan"))
    cancel = _float(row, "l2_book_cancel_ratio", float("nan"))
    queue_rank = _float(row, "queue_local_rank", _float(row, "sell_resil_rank", float("nan")))
    queue_init = _float(row, "queue_init", float("nan"))
    queue_deplete = _float(row, "queue_deplete_mult", float("nan"))
    queue_mo = _float(row, "queue_mo_mult", float("nan"))
    reason = str(row.get("reason_text", "")).lower()

    ref_adv_bool = _boolish(row, "xmarket_ref_adverse")
    spot_adv_bool = _boolish(row, "xmarket_spot_adverse")
    retreat_bool = _boolish(row, "xmarket_retreat_pause")
    ttl_effective = _boolish(row, "xmarket_retreat_ttl_effective")
    sell_ref_adv = _float(row, "sell_resil_ref_adv", float("nan"))
    sell_spot_adv = _float(row, "sell_resil_spot_adv", float("nan"))
    ref_ret = abs(_float(row, "xmarket_ref_adverse_ret_max", 0.0))
    spot_ret = abs(_float(row, "xmarket_spot_adverse_ret_max", 0.0))
    xmarket_known = any(v is not None for v in (ref_adv_bool, spot_adv_bool, retreat_bool, ttl_effective)) or math.isfinite(sell_ref_adv) or math.isfinite(sell_spot_adv)
    xmarket_adverse = (
        bool(ref_adv_bool)
        or bool(spot_adv_bool)
        or bool(retreat_bool)
        or bool(ttl_effective)
        or (math.isfinite(sell_ref_adv) and abs(sell_ref_adv) > 0.0)
        or (math.isfinite(sell_spot_adv) and abs(sell_spot_adv) > 0.0)
        or ref_ret > 0.0
        or spot_ret > 0.0
    )
    xmarket_non_adverse = xmarket_known and not xmarket_adverse

    depth_ok = math.isfinite(near_depth) and 0.75 <= near_depth <= 6.0
    depth_strict = math.isfinite(near_depth) and 1.0 <= near_depth <= 4.0
    refill_edge = (refresh - cancel) if math.isfinite(refresh) and math.isfinite(cancel) else float("nan")
    refill_ok = (math.isfinite(refill_edge) and refill_edge >= -0.05) or (math.isfinite(queue_deplete) and queue_deplete >= 0.90)
    refill_strict = (math.isfinite(refill_edge) and refill_edge >= 0.0) or (math.isfinite(queue_deplete) and queue_deplete >= 1.00)
    queue_ok = (
        (math.isfinite(queue_rank) and queue_rank <= 0.75)
        or (
            math.isfinite(queue_init)
            and math.isfinite(near_depth)
            and near_depth > 0.0
            and queue_init <= max(0.50, near_depth * 0.50)
        )
    )
    flow_ok = (math.isfinite(queue_mo) and 0.50 <= queue_mo <= 1.40) or resil >= 0.50
    local_absorb = (
        depth_ok
        and refill_ok
        and queue_ok
        and flow_ok
        and micro_reversion >= 0.40
        and trend_inventory_risk < 0.66
        and "thin" not in reason
    )
    local_absorb_strict = (
        depth_strict
        and refill_strict
        and queue_ok
        and flow_ok
        and resil >= 0.50
        and micro_reversion >= 0.55
        and trend_inventory_risk < 0.50
        and "thin" not in reason
    )

    strict = (
        side in {"BUY", "SELL"}
        and fill_quality >= 0.66
        and campaign_outcome <= 0.40
        and toxic < 0.33
        and fill_prob >= 0.50
        and xmarket_non_adverse
        and local_absorb_strict
    )
    broad = (
        side in {"BUY", "SELL"}
        and fill_quality >= 0.55
        and campaign_outcome <= 0.50
        and toxic < 0.50
        and fill_prob >= 0.40
        and (not xmarket_adverse)
        and local_absorb
    )
    noneligible_high_risk = (
        side in {"BUY", "SELL"}
        and not broad
        and (campaign_outcome >= 0.66 or toxic >= 0.66)
    )
    return {
        "fill_quality_high": fill_quality >= 0.66,
        "campaign_outcome_low": campaign_outcome <= 0.40,
        "toxic_low": toxic < 0.33,
        "fill_probability_not_low": fill_prob >= 0.50,
        "xmarket_known": xmarket_known,
        "xmarket_non_adverse": xmarket_non_adverse,
        "local_absorb": local_absorb,
        "local_absorb_strict": local_absorb_strict,
        "micro_reversion_ok": micro_reversion >= 0.55,
        "trend_inventory_risk_low": trend_inventory_risk < 0.50,
        "eligible_strict": strict,
        "eligible_broad": broad,
        "noneligible_high_risk": noneligible_high_risk,
        "eligibility_reason": (
            "strict_positive_eligibility" if strict else
            "broad_positive_eligibility" if broad else
            "noneligible_high_risk" if noneligible_high_risk else
            "other"
        ),
    }


class ReplayCampaignLabelBuilder:
    """Build terminal campaign labels from replay fills.

    中文说明：retained-all order-level CSV 很大；为了避免只为了 terminal
    campaign outcome 重写 2GB+ 表，这里单独扫描较小的 fills trace，按与
    ``metrics.replay_order_level_rows`` 相同的 per-day campaign_id 规则生成
    terminal label。fast audit 读 order-level 时再按 ``(day, campaign_id)``
    连接。
    """

    def __init__(self, day: str):
        self.day = day
        self.campaign_id = 0
        self.active = False
        self.start_ts = 0.0
        self.start_equity = 0.0
        self.q = 0.0
        self.cash = 0.0
        self.max_abs_q = 0.0
        self.min_delta = 0.0
        self.max_delta = 0.0
        self.early_min = {300: 0.0, 600: 0.0, 1200: 0.0}
        self.last_ts = 0.0
        self.last_px = 0.0
        self.labels: dict[tuple[str, str], dict[str, str]] = {}

    def _equity(self, px: float) -> float:
        return self.cash + self.q * px

    def _resync_position(self, q: float, px: float) -> None:
        if not math.isfinite(q) or abs(q - self.q) <= 1e-9:
            return
        equity = self._equity(px) if px > 0.0 else self.cash
        self.q = q
        if px > 0.0:
            self.cash = equity - self.q * px

    def _start(self, ts: float, px: float) -> None:
        self.campaign_id += 1
        self.active = True
        self.start_ts = ts
        self.start_equity = self._equity(px)
        self.max_abs_q = abs(self.q)
        self.min_delta = 0.0
        self.max_delta = 0.0
        self.early_min = {300: 0.0, 600: 0.0, 1200: 0.0}

    def _mark(self, ts: float, px: float) -> None:
        if not self.active or px <= 0.0:
            return
        delta = self._equity(px) - self.start_equity
        self.max_abs_q = max(self.max_abs_q, abs(self.q))
        self.min_delta = min(self.min_delta, delta)
        self.max_delta = max(self.max_delta, delta)
        elapsed = ts - self.start_ts
        for window_s in self.early_min:
            if elapsed <= window_s:
                self.early_min[window_s] = min(self.early_min[window_s], delta)

    def _label_name(self, *, closed: bool, final_delta: float) -> str:
        if not closed:
            return "open_risk"
        if final_delta >= 0.0 and self.min_delta < -0.25:
            return "repaired_after_drawdown"
        if final_delta >= 0.0:
            return "positive_flat"
        if self.min_delta <= -1.0 or self.max_abs_q >= 0.010:
            return "loss_tail"
        return "negative_flat"

    def _close(self, ts: float, px: float, *, closed: bool) -> None:
        if not self.active:
            return
        self._mark(ts, px)
        final_delta = self._equity(px) - self.start_equity if px > 0.0 else self.max_delta
        label = self._label_name(closed=closed, final_delta=final_delta)
        cid = str(self.campaign_id)
        self.labels[(self.day, cid)] = {
            "terminal_campaign_label": label,
            "terminal_campaign_repaired": "1" if label in {"repaired_after_drawdown", "positive_flat"} else "0",
            "terminal_campaign_tail_loss": "1" if label == "loss_tail" else "0",
            "terminal_campaign_bad": "1" if label in {"negative_flat", "loss_tail", "open_risk"} else "0",
            "terminal_campaign_outcome_risk_target": {
                "positive_flat": "0.000000",
                "repaired_after_drawdown": "0.250000",
                "negative_flat": "0.700000",
                "loss_tail": "1.000000",
                "open_risk": "1.000000",
            }.get(label, ""),
            "terminal_final_total_pnl_delta": f"{final_delta:.10f}",
            "terminal_realized_pnl_delta": f"{final_delta:.10f}",
            "terminal_min_total_pnl_delta": f"{self.min_delta:.10f}",
            "terminal_max_total_pnl_delta": f"{self.max_delta:.10f}",
            "terminal_early_5m_min_pnl_delta": f"{self.early_min[300]:.10f}",
            "terminal_early_10m_min_pnl_delta": f"{self.early_min[600]:.10f}",
            "terminal_early_20m_min_pnl_delta": f"{self.early_min[1200]:.10f}",
            "terminal_early_drawdown_20m": f"{abs(min(0.0, self.early_min[1200])):.10f}",
            "terminal_campaign_duration_s": f"{max(0.0, ts - self.start_ts):.6f}",
            "terminal_campaign_max_abs_inventory": f"{self.max_abs_q:.10f}",
        }
        self.active = False
        if closed:
            self.q = 0.0
            self.cash = 0.0

    def apply(self, row: dict[str, str]) -> None:
        side = _side(row)
        qty = _float(row, "fill_qty")
        px = _float(row, "fill_trade_px", _float(row, "price"))
        ts = _norm_ts(_float(row, "fill_ts"))
        if side not in {"BUY", "SELL"} or qty <= 0.0 or px <= 0.0 or ts <= 0.0:
            return
        self.last_ts = ts
        self.last_px = px
        prev_q = _float(row, "inventory", self.q)
        self._resync_position(prev_q, px)
        signed_qty = qty if side == "BUY" else -qty
        self.cash += -qty * px if side == "BUY" else qty * px
        self.q = prev_q + signed_qty
        if not self.active and abs(self.q) >= 1e-12:
            self._start(ts, px)
        if self.active:
            self._mark(ts, px)
            if abs(self.q) < 1e-12:
                self._close(ts, px, closed=True)

    def finish(self) -> None:
        if self.active:
            self._close(self.last_ts or self.start_ts, self.last_px or 1.0, closed=False)


def _load_replay_campaign_labels_filtered(path: Path, days: Optional[set[str]]) -> dict[tuple[str, str], dict[str, str]]:
    rows_by_day: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in _iter_rows(path):
        day = row.get("day", "")
        if not day:
            ts = _norm_ts(_float(row, "fill_ts"))
            if ts > 0.0:
                day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if days is not None and day not in days:
            continue
        if day:
            rows_by_day[day].append(row)
    labels: dict[tuple[str, str], dict[str, str]] = {}
    for day, rows in rows_by_day.items():
        builder = ReplayCampaignLabelBuilder(day)
        rows.sort(key=lambda r: _norm_ts(_float(r, "fill_ts")))
        for row in rows:
            builder.apply(row)
        builder.finish()
        labels.update(builder.labels)
    return labels


@dataclass
class Acc:
    orders: int = 0
    filled: int = 0
    fill_rate_sum: float = 0.0
    markout_30_sum: float = 0.0
    tail_m50: int = 0
    campaign_age_sum: float = 0.0
    campaign_max_abs_sum: float = 0.0
    terminal_labeled: int = 0
    terminal_pnl_sum: float = 0.0
    terminal_repair_sum: float = 0.0
    terminal_tail_sum: float = 0.0
    terminal_bad_sum: float = 0.0
    terminal_target_sum: float = 0.0
    terminal_early_drawdown_sum: float = 0.0

    def add(self, row: dict[str, str], terminal_labels: Optional[dict[tuple[str, str], dict[str, str]]] = None) -> None:
        self.orders += 1
        filled = _int(row, "filled")
        self.filled += filled
        markout = _float(row, "markout_30s_bps")
        self.markout_30_sum += markout
        if filled and markout <= -50.0:
            self.tail_m50 += 1
        self.campaign_age_sum += _float(row, "campaign_age_s")
        self.campaign_max_abs_sum += _float(row, "campaign_max_abs_qty")
        label = terminal_labels.get((row.get("day", ""), row.get("campaign_id", "")), {}) if terminal_labels else {}
        terminal_label = row.get("terminal_campaign_label") or label.get("terminal_campaign_label")
        if terminal_label:
            self.terminal_labeled += 1
            self.terminal_pnl_sum += _float(row, "terminal_final_total_pnl_delta", _float(label, "terminal_final_total_pnl_delta"))
            self.terminal_repair_sum += _float(row, "terminal_campaign_repaired", _float(label, "terminal_campaign_repaired"))
            self.terminal_tail_sum += _float(row, "terminal_campaign_tail_loss", _float(label, "terminal_campaign_tail_loss"))
            self.terminal_bad_sum += _float(row, "terminal_campaign_bad", _float(label, "terminal_campaign_bad"))
            self.terminal_target_sum += _float(row, "terminal_campaign_outcome_risk_target", _float(label, "terminal_campaign_outcome_risk_target"))
            self.terminal_early_drawdown_sum += _float(row, "terminal_early_drawdown_20m", _float(label, "terminal_early_drawdown_20m"))

    def as_row(self) -> dict[str, str]:
        orders = max(self.orders, 1)
        filled = max(self.filled, 1)
        return {
            "orders": str(self.orders),
            "filled_orders": str(self.filled),
            "fill_rate": f"{self.filled / orders:.10f}",
            "avg_markout_30s_bps": f"{self.markout_30_sum / orders:.10f}",
            "tail_rate_m50_30s": f"{self.tail_m50 / filled:.10f}" if self.filled else "0.0000000000",
            "avg_campaign_age_s": f"{self.campaign_age_sum / orders:.10f}",
            "avg_campaign_max_abs_qty": f"{self.campaign_max_abs_sum / orders:.10f}",
            "terminal_labeled_orders": str(self.terminal_labeled),
            "avg_terminal_campaign_pnl": f"{self.terminal_pnl_sum / max(self.terminal_labeled, 1):.10f}" if self.terminal_labeled else "0.0000000000",
            "terminal_repair_rate": f"{self.terminal_repair_sum / max(self.terminal_labeled, 1):.10f}" if self.terminal_labeled else "0.0000000000",
            "terminal_tail_loss_rate": f"{self.terminal_tail_sum / max(self.terminal_labeled, 1):.10f}" if self.terminal_labeled else "0.0000000000",
            "terminal_bad_rate": f"{self.terminal_bad_sum / max(self.terminal_labeled, 1):.10f}" if self.terminal_labeled else "0.0000000000",
            "avg_terminal_outcome_risk_target": f"{self.terminal_target_sum / max(self.terminal_labeled, 1):.10f}" if self.terminal_labeled else "0.0000000000",
            "avg_terminal_early_20m_drawdown": f"{self.terminal_early_drawdown_sum / max(self.terminal_labeled, 1):.10f}" if self.terminal_labeled else "0.0000000000",
        }


@dataclass
class CalibrationAcc:
    orders: int = 0
    filled: int = 0
    score_sum: float = 0.0
    target_sum: float = 0.0
    target_n: int = 0
    markout_30_sum: float = 0.0
    tail_m50: int = 0
    terminal_labeled: int = 0
    terminal_pnl_sum: float = 0.0
    terminal_tail_sum: float = 0.0
    terminal_bad_sum: float = 0.0
    terminal_early_drawdown_sum: float = 0.0
    queue_init_sum: float = 0.0
    queue_init_n: int = 0
    queue_rank_sum: float = 0.0
    queue_rank_n: int = 0
    near_depth_sum: float = 0.0
    near_depth_n: int = 0
    exact_l2_spread_sum: float = 0.0
    exact_l2_spread_n: int = 0
    ttl_budget_sum: float = 0.0
    ttl_budget_n: int = 0
    observed_lifetime_sum: float = 0.0
    observed_lifetime_n: int = 0
    fill_eligible_sum: float = 0.0
    fill_eligible_n: int = 0

    def add(
        self,
        row: dict[str, str],
        *,
        score: str,
        score_value: float,
        terminal_labels: Optional[dict[tuple[str, str], dict[str, str]]] = None,
    ) -> None:
        self.orders += 1
        self.score_sum += score_value if math.isfinite(score_value) else 0.0
        filled = _int(row, "filled")
        self.filled += filled
        markout = _float(row, "markout_30s_bps")
        self.markout_30_sum += markout
        if filled and markout <= -50.0:
            self.tail_m50 += 1
        label = terminal_labels.get((row.get("day", ""), row.get("campaign_id", "")), {}) if terminal_labels else {}
        terminal_label = row.get("terminal_campaign_label") or label.get("terminal_campaign_label")
        if terminal_label:
            self.terminal_labeled += 1
            self.terminal_pnl_sum += _float(row, "terminal_final_total_pnl_delta", _float(label, "terminal_final_total_pnl_delta"))
            self.terminal_tail_sum += _float(row, "terminal_campaign_tail_loss", _float(label, "terminal_campaign_tail_loss"))
            self.terminal_bad_sum += _float(row, "terminal_campaign_bad", _float(label, "terminal_campaign_bad"))
            self.terminal_early_drawdown_sum += _float(row, "terminal_early_drawdown_20m", _float(label, "terminal_early_drawdown_20m"))
        if score in {"micro_fill_reach_score", "fill_probability_score", "reducing_burst_risk_score"}:
            self.target_sum += filled
            self.target_n += 1
        elif score in {"campaign_outcome_risk_score", "lifecycle_risk_score"} and terminal_label:
            self.target_sum += _float(row, "terminal_campaign_outcome_risk_target", _float(label, "terminal_campaign_outcome_risk_target"))
            self.target_n += 1

        for key, attr_sum, attr_n in (
            ("queue_init", "queue_init_sum", "queue_init_n"),
            ("queue_local_rank", "queue_rank_sum", "queue_rank_n"),
            ("near_depth_total", "near_depth_sum", "near_depth_n"),
            ("exact_l2_spread_bps", "exact_l2_spread_sum", "exact_l2_spread_n"),
            ("ttl_budget_ms", "ttl_budget_sum", "ttl_budget_n"),
        ):
            v = _float(row, key, float("nan"))
            if math.isfinite(v):
                setattr(self, attr_sum, getattr(self, attr_sum) + v)
                setattr(self, attr_n, getattr(self, attr_n) + 1)
        life = _float(row, "observed_lifetime_ms", float("nan"))
        if filled and math.isfinite(life):
            self.observed_lifetime_sum += life
            self.observed_lifetime_n += 1
        fill_eligible = row.get("fill_eligible", "")
        if fill_eligible not in ("", None):
            self.fill_eligible_sum += 1.0 if str(fill_eligible).strip().lower() in {"1", "true", "yes", "y"} else 0.0
            self.fill_eligible_n += 1

    def as_row(self) -> dict[str, str]:
        orders = max(self.orders, 1)
        filled = max(self.filled, 1)
        target_n = max(self.target_n, 1)
        avg_score = self.score_sum / orders
        avg_target = self.target_sum / target_n if self.target_n else 0.0

        def avg(total: float, n: int) -> str:
            return f"{total / max(n, 1):.10f}" if n else "0.0000000000"

        return {
            "orders": str(self.orders),
            "filled_orders": str(self.filled),
            "fill_rate": f"{self.filled / orders:.10f}",
            "avg_score": f"{avg_score:.10f}",
            "target_labeled_orders": str(self.target_n),
            "avg_target": f"{avg_target:.10f}" if self.target_n else "0.0000000000",
            "calibration_error_score_minus_target": f"{avg_score - avg_target:.10f}" if self.target_n else "0.0000000000",
            "avg_markout_30s_bps": f"{self.markout_30_sum / orders:.10f}",
            "tail_rate_m50_30s": f"{self.tail_m50 / filled:.10f}" if self.filled else "0.0000000000",
            "terminal_labeled_orders": str(self.terminal_labeled),
            "avg_terminal_campaign_pnl": avg(self.terminal_pnl_sum, self.terminal_labeled),
            "terminal_tail_loss_rate": avg(self.terminal_tail_sum, self.terminal_labeled),
            "terminal_bad_rate": avg(self.terminal_bad_sum, self.terminal_labeled),
            "avg_terminal_early_20m_drawdown": avg(self.terminal_early_drawdown_sum, self.terminal_labeled),
            "avg_queue_init": avg(self.queue_init_sum, self.queue_init_n),
            "avg_queue_local_rank": avg(self.queue_rank_sum, self.queue_rank_n),
            "avg_near_depth_total": avg(self.near_depth_sum, self.near_depth_n),
            "avg_exact_l2_spread_bps": avg(self.exact_l2_spread_sum, self.exact_l2_spread_n),
            "avg_ttl_budget_ms": avg(self.ttl_budget_sum, self.ttl_budget_n),
            "avg_observed_lifetime_ms_filled": avg(self.observed_lifetime_sum, self.observed_lifetime_n),
            "fill_eligible_rate": avg(self.fill_eligible_sum, self.fill_eligible_n),
        }


def _burst_count_bucket(count: int) -> str:
    if count <= 0:
        return "burst_0"
    if count == 1:
        return "burst_1"
    if count == 2:
        return "burst_2"
    return "burst_3p"


def _order_ts(row: dict[str, str]) -> float:
    return _norm_ts(_float(row, "timestamp", _float(row, "submit_ts", _float(row, "quote_ts"))))


def _fill_ts(row: dict[str, str]) -> float:
    return _norm_ts(_float(row, "fill_ts", _float(row, "timestamp")))


def _order_inventory_reducing(row: dict[str, str]) -> bool:
    side = _side(row)
    qty = _float(row, "quantity", _float(row, "filled_qty"))
    q_before = _float(row, "q_before", float("nan"))
    if side not in {"BUY", "SELL"} or qty <= 0.0 or not math.isfinite(q_before):
        return False
    signed_qty = qty if side == "BUY" else -qty
    return abs(q_before + signed_qty) < abs(q_before) - 1e-12


def _refill_bucket(row: dict[str, str]) -> str:
    refresh = _float(row, "l2_book_refresh_ratio", float("nan"))
    cancel = _float(row, "l2_book_cancel_ratio", float("nan"))
    if not (math.isfinite(refresh) and math.isfinite(cancel)):
        return "refill_missing"
    edge = refresh - cancel
    if edge >= 0.02:
        return "refill_positive"
    if edge <= -0.02:
        return "refill_negative"
    return "refill_flat"


def _flow_bucket(row: dict[str, str]) -> str:
    flow_decel = _float(row, "sell_resil_flow_decel", float("nan"))
    if math.isfinite(flow_decel):
        if flow_decel < 0.20:
            return "flow_decel_low"
        if flow_decel < 0.50:
            return "flow_decel_mid"
        return "flow_decel_high"
    queue_mo = _float(row, "queue_mo_mult", float("nan"))
    if not math.isfinite(queue_mo):
        return "flow_missing"
    if queue_mo < 0.80:
        return "queue_mo_weak"
    if queue_mo <= 1.20:
        return "queue_mo_neutral"
    return "queue_mo_aggressive"


def _lifecycle_shadow_candidate(row: dict[str, str], scores: dict[str, float | str]) -> bool:
    """Narrow reducing-side lifecycle shadow condition.

    中文说明：这不是固定 reducing cooldown。只有 quote-time 同时满足
    burst_1+、趋势库存风险高、refill 弱/负、campaign 修复前景差时才命中。
    """
    if not _order_inventory_reducing(row):
        return False
    if _float(row, "reducing_burst_count_8s", 0.0) < 1.0:
        return False
    trend = float(scores.get("trend_inventory_risk_score", _float(row, "trend_inventory_risk_score")))
    campaign = float(scores.get("campaign_outcome_risk_score", _float(row, "campaign_outcome_risk_score")))
    lifecycle = float(scores.get("lifecycle_risk_score", _float(row, "lifecycle_risk_score")))
    refill_edge = _float(row, "sell_resil_refill_edge", float("nan"))
    if not math.isfinite(refill_edge):
        refresh = _float(row, "l2_book_refresh_ratio", float("nan"))
        cancel = _float(row, "l2_book_cancel_ratio", float("nan"))
        refill_edge = refresh - cancel if math.isfinite(refresh) and math.isfinite(cancel) else float("nan")
    return (
        math.isfinite(refill_edge)
        and trend >= 0.66
        and campaign >= 0.66
        and lifecycle >= 0.66
        and refill_edge <= 0.02
    )


class ReducingFillBurstTracker:
    """Quote-time state for recent inventory-reducing fills.

    中文说明：固定 reducing-side cooldown 会误杀自然修复。这个 tracker
    不做反事实挡单，只在每笔 order submit 前记录同方向减仓 fill 是否
    刚刚连续发生。后续用 trend/refill/flow 条件判断这种 burst 是否有害。
    """

    def __init__(self) -> None:
        self._history: dict[str, deque[tuple[float, float]]] = {"BUY": deque(), "SELL": deque()}

    def snapshot(self, *, ts: float, side: str) -> dict[str, str]:
        history = self._history.get(side, deque())
        while history and ts - history[0][0] > 60.0:
            history.popleft()
        out: dict[str, str] = {}
        for window_s in (4.0, 8.0, 12.0):
            items = [(t, q) for t, q in history if 0.0 <= ts - t <= window_s]
            suffix = int(window_s)
            out[f"reducing_burst_count_{suffix}s"] = str(len(items))
            out[f"reducing_burst_qty_{suffix}s"] = f"{sum(q for _, q in items):.10f}"
        out["reducing_burst_last_age_s"] = f"{max(0.0, ts - history[-1][0]):.6f}" if history else ""
        count8 = int(out["reducing_burst_count_8s"])
        qty8 = _float(out, "reducing_burst_qty_8s")
        # 中文说明：policy/evidence 讨论的是 burst_1+，所以一笔 prior
        # reducing fill 就应进入高风险分数；qty 只作为强度修正。
        score = min(1.0, 0.85 * (1.0 if count8 >= 1 else 0.0) + 0.15 * min(qty8 / 0.003, 1.0))
        out["reducing_burst_score"] = f"{score:.6f}"
        out["reducing_burst_bucket_8s"] = _burst_count_bucket(count8)
        return out

    def observe_fill_if_reducing(self, row: dict[str, str], *, ts: float) -> None:
        if not _order_inventory_reducing(row):
            return
        side = _side(row)
        if side not in {"BUY", "SELL"}:
            return
        qty = _float(row, "filled_qty", _float(row, "quantity"))
        if qty > 0.0:
            self._history[side].append((ts, qty))


def _iter_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open(newline="") as f:
        yield from csv.DictReader(f)


def _precompute_day_path_features(day_rows: list[dict[str, str]]) -> tuple[list[tuple[float, float]], list[float]]:
    """Attach quote-time micro/macro path features to replay order rows.

    中文说明：retained-all fast audit 会扫几百万笔 order。如果每行都重新
    扫 5s/10s/20s/60s/300s mid window，会把 evidence 迭代拖成 O(N×窗口)
    的慢路径。这里按 day 一次性滚动维护 min/max 和 return-squared prefix，
    后续 overlay 只复制字段。
    """
    items: list[tuple[float, float, dict[str, str]]] = []
    seen: set[tuple[float, float]] = set()
    for row in day_rows:
        ts = _norm_ts(_float(row, "submit_ts", _float(row, "quote_ts", float("nan"))))
        mid = _float(row, "mid", float("nan"))
        if ts > 0.0 and mid > 0.0:
            items.append((ts, mid, row))
            seen.add((ts, mid))
    items.sort(key=lambda x: x[0])
    n = len(items)
    mid_series = sorted(seen, key=lambda x: x[0])
    mid_ts = [x[0] for x in mid_series]
    if n == 0:
        return mid_series, mid_ts

    ts_values = [x[0] for x in items]
    mid_values = [x[1] for x in items]
    ret_sq = [0.0] * n
    for i in range(1, n):
        prev = mid_values[i - 1]
        ret = (mid_values[i] - prev) / prev * 10_000.0 if prev > 0.0 else 0.0
        ret_sq[i] = ret * ret
    prefix_ret_sq = [0.0] * (n + 1)
    for i, value in enumerate(ret_sq):
        prefix_ret_sq[i + 1] = prefix_ret_sq[i] + value

    stats_by_window: dict[int, dict[str, list[float]]] = {}
    for window_s in MICRO_MACRO_WINDOWS_S:
        left = 0
        minq: deque[int] = deque()
        maxq: deque[int] = deque()
        ranges = [0.0] * n
        rvs = [0.0] * n
        rets = [0.0] * n
        counts = [0.0] * n
        for i, (ts, mid) in enumerate(zip(ts_values, mid_values)):
            while minq and mid_values[minq[-1]] >= mid:
                minq.pop()
            minq.append(i)
            while maxq and mid_values[maxq[-1]] <= mid:
                maxq.pop()
            maxq.append(i)
            cutoff = ts - float(window_s)
            while left <= i and ts_values[left] < cutoff:
                left += 1
            while minq and minq[0] < left:
                minq.popleft()
            while maxq and maxq[0] < left:
                maxq.popleft()
            count = i - left + 1
            counts[i] = float(count)
            if count <= 0 or mid <= 0.0:
                continue
            low = mid_values[minq[0]] if minq else mid
            high = mid_values[maxq[0]] if maxq else mid
            first = mid_values[left]
            ranges[i] = (high - low) / mid * 10_000.0
            rets[i] = (mid - first) / first * 10_000.0 if first > 0.0 else 0.0
            if i > left:
                sum_sq = prefix_ret_sq[i + 1] - prefix_ret_sq[left + 1]
                rvs[i] = math.sqrt(max(0.0, sum_sq / max(1, i - left)))
        stats_by_window[window_s] = {
            "range": ranges,
            "rv": rvs,
            "ret": rets,
            "count": counts,
        }

    def ratio(num: float, den: float) -> float:
        if not (math.isfinite(num) and math.isfinite(den)):
            return math.nan
        return num / max(abs(den), PATH_RATIO_EPS_BPS)

    for i, (_, mid, row) in enumerate(items):
        for window_s in MICRO_MACRO_WINDOWS_S:
            stats = stats_by_window[window_s]
            row[f"range_{window_s}s_bps"] = _fmt_path_feature(stats["range"][i])
            row[f"rv_{window_s}s_bps"] = _fmt_path_feature(stats["rv"][i])
            row[f"ret_{window_s}s_bps"] = _fmt_path_feature(stats["ret"][i])
            row[f"path_count_{window_s}s"] = _fmt_path_feature(stats["count"][i])

        quote_distance_bps = float("nan")
        distance = _float(row, "final_distance_to_mid", float("nan"))
        if mid > 0.0 and math.isfinite(distance):
            quote_distance_bps = abs(distance) / mid * 10_000.0
        range_5 = stats_by_window[5]["range"][i]
        range_10 = stats_by_window[10]["range"][i]
        range_20 = stats_by_window[20]["range"][i]
        range_300 = stats_by_window[300]["range"][i]
        rv_10 = stats_by_window[10]["rv"][i]
        rv_300 = stats_by_window[300]["rv"][i]
        ret_60 = stats_by_window[60]["ret"][i]
        ret_300 = stats_by_window[300]["ret"][i]
        trend_eff_60 = ratio(abs(ret_60), stats_by_window[60]["range"][i])
        trend_eff_300 = ratio(abs(ret_300), range_300)
        micro_macro_range = ratio(range_10, range_300)
        micro_macro_vol = ratio(rv_10, rv_300)
        side = _side(row)
        row["quote_distance_micro_5s"] = _fmt_path_feature(ratio(quote_distance_bps, range_5))
        row["quote_distance_micro_10s"] = _fmt_path_feature(ratio(quote_distance_bps, range_10))
        row["quote_distance_micro"] = row["quote_distance_micro_10s"]
        row["micro_macro_range_ratio"] = _fmt_path_feature(micro_macro_range)
        row["micro_macro_vol_ratio"] = _fmt_path_feature(micro_macro_vol)
        row["inventory_horizon_range_ratio"] = _fmt_path_feature(ratio(range_20, range_300))
        row["trend_efficiency_60s"] = _fmt_path_feature(trend_eff_60)
        row["trend_efficiency_300s"] = _fmt_path_feature(trend_eff_300)
        if side == "BUY":
            row["side_trend_adverse_60s_bps"] = _fmt_path_feature(max(0.0, -ret_60))
            row["side_trend_adverse_300s_bps"] = _fmt_path_feature(max(0.0, -ret_300))
        elif side == "SELL":
            row["side_trend_adverse_60s_bps"] = _fmt_path_feature(max(0.0, ret_60))
            row["side_trend_adverse_300s_bps"] = _fmt_path_feature(max(0.0, ret_300))
        else:
            row["side_trend_adverse_60s_bps"] = ""
            row["side_trend_adverse_300s_bps"] = ""
        row["micro_macro_regime"] = _micro_macro_regime(micro_macro_range, trend_eff_300)
    return mid_series, mid_ts


class ReplayOrderOverlay:
    """Per-day replay order field overlay for cached order-level rows."""

    def __init__(self, path: Optional[Path]):
        self.path = path
        self.file = path.open(newline="") if path else None
        self.reader = csv.DictReader(self.file) if self.file else None
        self.next_row: Optional[dict[str, str]] = next(self.reader, None) if self.reader else None
        self.current_day = ""
        self.current_rows: dict[str, dict[str, str]] = {}
        self.current_mid_series: list[tuple[float, float]] = []
        self.current_mid_ts: list[float] = []

    def close(self) -> None:
        if self.file:
            self.file.close()

    def _load_next_day(self) -> None:
        self.current_rows = {}
        self.current_mid_series = []
        self.current_mid_ts = []
        if self.next_row is None:
            self.current_day = ""
            return
        day = self.next_row.get("day", "")
        self.current_day = day
        day_rows: list[dict[str, str]] = []
        while self.next_row is not None and self.next_row.get("day", "") == day:
            day_rows.append(self.next_row)
            oid = self.next_row.get("order_id", "")
            if oid:
                self.current_rows[oid] = self.next_row
            self.next_row = next(self.reader, None) if self.reader else None
        self.current_mid_series, self.current_mid_ts = _precompute_day_path_features(day_rows)

    def get(self, day: str, order_id: str) -> dict[str, str]:
        if not self.reader or not day or not order_id:
            return {}
        while self.next_row is not None and (not self.current_day or self.current_day < day):
            self._load_next_day()
            if self.current_day >= day:
                break
        if self.current_day != day:
            return {}
        return self.current_rows.get(order_id, {})


def _with_replay_overlay(row: dict[str, str], overlay: Optional[ReplayOrderOverlay]) -> dict[str, str]:
    if overlay is None:
        return row
    replay = overlay.get(row.get("day", ""), row.get("client_order_id", ""))
    if not replay:
        return row
    out = dict(row)
    for key in REPLAY_OVERLAY_FIELDS:
        value = replay.get(key, "")
        if value != "":
            if key == "xmarket_retreat_ttl_ms":
                out["ttl_budget_ms"] = value
            elif key == "lifetime_ms":
                out["observed_lifetime_ms"] = value
            else:
                out[key] = value
    for key in PATH_OVERLAY_FIELDS:
        value = replay.get(key, "")
        if value != "":
            out[key] = value
    side = out.get("side", "")
    if side == "BUY":
        out["side_quote_fill_prob"] = replay.get("bid_quote_fill_prob", out.get("side_quote_fill_prob", ""))
        out["side_quote_fill_markout_30s"] = replay.get("bid_quote_fill_markout_30s", out.get("side_quote_fill_markout_30s", ""))
        out["toxicity"] = replay.get("tox_bid", out.get("toxicity", ""))
        out["markout_ema"] = replay.get("mo_ema_bid", out.get("markout_ema", ""))
    elif side == "SELL":
        out["side_quote_fill_prob"] = replay.get("ask_quote_fill_prob", out.get("side_quote_fill_prob", ""))
        out["side_quote_fill_markout_30s"] = replay.get("ask_quote_fill_markout_30s", out.get("side_quote_fill_markout_30s", ""))
        out["toxicity"] = replay.get("tox_ask", out.get("toxicity", ""))
        out["markout_ema"] = replay.get("mo_ema_ask", out.get("markout_ema", ""))
    mid = _float(out, "mid")
    distance = _float(out, "final_distance_to_mid", float("nan"))
    if mid > 0.0 and math.isfinite(distance):
        out["quote_distance"] = f"{distance:.10f}"
        out["quote_distance_bps"] = f"{distance / mid * 10_000.0:.10f}"
    if out.get("l2_near_depth_total"):
        out["near_depth_total"] = out["l2_near_depth_total"]
    best_bid = _float(out, "best_bid", float("nan"))
    best_ask = _float(out, "best_ask", float("nan"))
    if mid > 0.0 and math.isfinite(best_bid) and math.isfinite(best_ask) and best_ask >= best_bid:
        out["exact_l2_spread_bps"] = f"{(best_ask - best_bid) / mid * 10_000.0:.10f}"
    return out


def _collect_toxic_thresholds(
    path: Path,
    *,
    replay_orders_path: Optional[Path] = None,
    days: Optional[set[str]] = None,
) -> tuple[dict[str, tuple[float, float, float]], dict[tuple[str, str], tuple[float, float, float]]]:
    side_values: dict[str, list[float]] = defaultdict(list)
    day_side_values: dict[tuple[str, str], list[float]] = defaultdict(list)
    overlay = ReplayOrderOverlay(replay_orders_path)
    try:
        for raw_row in _iter_rows(path):
            row = _with_replay_overlay(raw_row, overlay)
            side = row.get("side", "")
            day = row.get("day", "")
            if days is not None and day not in days:
                continue
            if side not in ("BUY", "SELL") or not day:
                continue
            v = float(_order_scores(row).get("toxic_risk_score", _float(row, "toxic_risk_score")))
            side_values[side].append(v)
            day_side_values[(day, side)].append(v)
    finally:
        overlay.close()
    return (
        {side: _quantile_thresholds(vals) for side, vals in side_values.items()},
        {key: _quantile_thresholds(vals) for key, vals in day_side_values.items()},
    )


def _build_tables(
    path: Path,
    side_thresholds: dict[str, tuple[float, float, float]],
    day_side_thresholds: dict[tuple[str, str], tuple[float, float, float]],
    terminal_labels: Optional[dict[tuple[str, str], dict[str, str]]] = None,
    replay_orders_path: Optional[Path] = None,
    days: Optional[set[str]] = None,
) -> tuple[
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
]:
    agg: dict[tuple[str, str, str], Acc] = defaultdict(Acc)
    daily: dict[tuple[str, str, str, str], Acc] = defaultdict(Acc)
    calibration: dict[tuple[str, str, str], CalibrationAcc] = defaultdict(CalibrationAcc)
    elig: dict[tuple[str, str], Acc] = defaultdict(Acc)
    elig_daily: dict[tuple[str, str, str], Acc] = defaultdict(Acc)
    elig_components: dict[tuple[str, str], Acc] = defaultdict(Acc)
    reducing_burst: dict[tuple[str, str, str], Acc] = defaultdict(Acc)
    reducing_burst_daily: dict[tuple[str, str, str, str], Acc] = defaultdict(Acc)
    lifecycle_shadow: dict[tuple[str, str], Acc] = defaultdict(Acc)
    lifecycle_shadow_daily: dict[tuple[str, str, str], Acc] = defaultdict(Acc)
    burst_tracker = ReducingFillBurstTracker()
    tracker_day = ""
    overlay = ReplayOrderOverlay(replay_orders_path)
    try:
        for raw_row in _iter_rows(path):
            row = _with_replay_overlay(raw_row, overlay)
            side = row.get("side", "")
            day = row.get("day", "")
            if days is not None and day not in days:
                continue
            if side not in ("BUY", "SELL") or not day:
                continue
            if day != tracker_day:
                burst_tracker = ReducingFillBurstTracker()
                tracker_day = day
            ts = _order_ts(row)
            if ts > 0.0:
                row.update(burst_tracker.snapshot(ts=ts, side=side))
            order_reducing = _order_inventory_reducing(row)
            row["order_inventory_reducing"] = "1" if order_reducing else "0"
            scores = _order_scores(row)
            lifecycle_hit = _lifecycle_shadow_candidate(row, scores)
            flags = _positive_eligibility_flags(row, scores)
            elig[("ALL", "all_orders")].add(row, terminal_labels=terminal_labels)
            elig[(side, "all_orders")].add(row, terminal_labels=terminal_labels)
            elig_daily[(day, "ALL", "all_orders")].add(row, terminal_labels=terminal_labels)
            elig_daily[(day, side, "all_orders")].add(row, terminal_labels=terminal_labels)
            for mask in ("eligible_strict", "eligible_broad", "noneligible_high_risk"):
                if flags.get(mask):
                    elig[("ALL", mask)].add(row, terminal_labels=terminal_labels)
                    elig[(side, mask)].add(row, terminal_labels=terminal_labels)
                    elig_daily[(day, "ALL", mask)].add(row, terminal_labels=terminal_labels)
                    elig_daily[(day, side, mask)].add(row, terminal_labels=terminal_labels)
            for component in (
                "fill_quality_high",
                "campaign_outcome_low",
                "toxic_low",
                "fill_probability_not_low",
                "xmarket_known",
                "xmarket_non_adverse",
                "local_absorb",
                "local_absorb_strict",
                "micro_reversion_ok",
                "trend_inventory_risk_low",
            ):
                if flags.get(component):
                    elig_components[(side, component)].add(row, terminal_labels=terminal_labels)
                    elig_components[("ALL", component)].add(row, terminal_labels=terminal_labels)
            if order_reducing:
                trend_bucket = _fixed_bucket(float(scores.get("trend_inventory_risk_score", _float(row, "trend_inventory_risk_score"))))
                reversion_bucket = _fixed_bucket(float(scores.get("micro_reversion_score", _float(row, "micro_reversion_score"))))
                burst8 = row.get("reducing_burst_bucket_8s", "burst_0") or "burst_0"
                refill_bucket = _refill_bucket(row)
                flow_bucket = _flow_bucket(row)
                regime = row.get("micro_macro_regime", "missing") or "missing"
                group_items = {
                    "all_reducing_orders": "all",
                    "burst8": burst8,
                    "burst8_x_trend_risk": f"{burst8}|trend_{trend_bucket}",
                    "burst8_x_micro_reversion": f"{burst8}|reversion_{reversion_bucket}",
                    "burst8_x_refill": f"{burst8}|{refill_bucket}",
                    "burst8_x_flow": f"{burst8}|{flow_bucket}",
                    "burst8_x_regime": f"{burst8}|{regime}",
                    "burst8_x_trend_refill_flow": f"{burst8}|trend_{trend_bucket}|{refill_bucket}|{flow_bucket}",
                }
                for dimension, bucket in group_items.items():
                    reducing_burst[(side, dimension, bucket)].add(row, terminal_labels=terminal_labels)
                    reducing_burst[("ALL", dimension, bucket)].add(row, terminal_labels=terminal_labels)
                    reducing_burst_daily[(day, side, dimension, bucket)].add(row, terminal_labels=terminal_labels)
                    reducing_burst_daily[(day, "ALL", dimension, bucket)].add(row, terminal_labels=terminal_labels)
                lifecycle_shadow[(side, "all_reducing_orders")].add(row, terminal_labels=terminal_labels)
                lifecycle_shadow[("ALL", "all_reducing_orders")].add(row, terminal_labels=terminal_labels)
                lifecycle_shadow_daily[(day, side, "all_reducing_orders")].add(row, terminal_labels=terminal_labels)
                lifecycle_shadow_daily[(day, "ALL", "all_reducing_orders")].add(row, terminal_labels=terminal_labels)
                if lifecycle_hit:
                    lifecycle_shadow[(side, "reducing_burst_lifecycle_narrow")].add(row, terminal_labels=terminal_labels)
                    lifecycle_shadow[("ALL", "reducing_burst_lifecycle_narrow")].add(row, terminal_labels=terminal_labels)
                    lifecycle_shadow_daily[(day, side, "reducing_burst_lifecycle_narrow")].add(row, terminal_labels=terminal_labels)
                    lifecycle_shadow_daily[(day, "ALL", "reducing_burst_lifecycle_narrow")].add(row, terminal_labels=terminal_labels)
            for score in SCORES:
                v = float(scores.get(score, _float(row, score)))
                if score == "toxic_risk_score":
                    agg_bucket = _q_bucket(v, side_thresholds.get(side, (float("inf"),) * 3))
                    day_bucket = _q_bucket(v, day_side_thresholds.get((day, side), (float("inf"),) * 3))
                else:
                    agg_bucket = day_bucket = _fixed_bucket(v)
                agg[(score, side, agg_bucket)].add(row, terminal_labels=terminal_labels)
                daily[(day, score, side, day_bucket)].add(row, terminal_labels=terminal_labels)
                if score in {
                    "campaign_outcome_risk_score",
                    "micro_fill_reach_score",
                    "fill_probability_score",
                    "reducing_burst_risk_score",
                    "lifecycle_risk_score",
                }:
                    calibration[(score, side, _decile_bucket(v))].add(
                        row,
                        score=score,
                        score_value=v,
                        terminal_labels=terminal_labels,
                    )
            if _int(row, "filled") and order_reducing:
                fill_ts = _fill_ts(row)
                if fill_ts > 0.0:
                    burst_tracker.observe_fill_if_reducing(row, ts=fill_ts)
    finally:
        overlay.close()

    agg_rows: list[dict[str, str]] = []
    for (score, side, bucket), acc in sorted(agg.items()):
        mode = "side_rank_quantile" if score == "toxic_risk_score" else "fixed_0p33_0p66"
        agg_rows.append({"score": score, "side": side, "bucket": bucket, "bucket_mode": mode, **acc.as_row()})

    daily_rows: list[dict[str, str]] = []
    for (day, score, side, bucket), acc in sorted(daily.items()):
        mode = "day_side_rank_quantile" if score == "toxic_risk_score" else "fixed_0p33_0p66"
        daily_rows.append({"day": day, "score": score, "side": side, "bucket": bucket, "bucket_mode": mode, **acc.as_row()})
    calibration_rows: list[dict[str, str]] = []
    for (score, side, bucket), acc in sorted(calibration.items()):
        target = (
            "filled"
            if score in {"micro_fill_reach_score", "fill_probability_score", "reducing_burst_risk_score"}
            else "terminal_campaign_outcome_risk_target"
        )
        calibration_rows.append({
            "score": score,
            "side": side,
            "bucket": bucket,
            "bucket_mode": "fixed_score_decile",
            "target": target,
            **acc.as_row(),
        })
    elig_rows: list[dict[str, str]] = []
    for (side, mask), acc in sorted(elig.items()):
        elig_rows.append({"side": side, "mask": mask, **acc.as_row()})
    elig_daily_rows: list[dict[str, str]] = []
    for (day, side, mask), acc in sorted(elig_daily.items()):
        elig_daily_rows.append({"day": day, "side": side, "mask": mask, **acc.as_row()})
    component_rows: list[dict[str, str]] = []
    for (side, component), acc in sorted(elig_components.items()):
        component_rows.append({"side": side, "component": component, **acc.as_row()})
    elig_summary_rows = _positive_eligibility_summary_rows(elig_daily_rows)
    burst_rows: list[dict[str, str]] = []
    for (side, dimension, bucket), acc in sorted(reducing_burst.items()):
        burst_rows.append({"side": side, "dimension": dimension, "bucket": bucket, **acc.as_row()})
    burst_daily_rows: list[dict[str, str]] = []
    for (day, side, dimension, bucket), acc in sorted(reducing_burst_daily.items()):
        burst_daily_rows.append({"day": day, "side": side, "dimension": dimension, "bucket": bucket, **acc.as_row()})
    burst_summary_rows = _reducing_burst_summary_rows(burst_daily_rows)
    lifecycle_shadow_rows = [
        {"side": side, "shadow_rule": rule, "knob": "shorter_ttl_or_pacing_shadow", **acc.as_row()}
        for (side, rule), acc in sorted(lifecycle_shadow.items())
    ]
    lifecycle_shadow_daily_rows = [
        {"day": day, "side": side, "shadow_rule": rule, "knob": "shorter_ttl_or_pacing_shadow", **acc.as_row()}
        for (day, side, rule), acc in sorted(lifecycle_shadow_daily.items())
    ]
    return (
        agg_rows,
        daily_rows,
        calibration_rows,
        elig_rows,
        elig_daily_rows,
        component_rows + elig_summary_rows,
        burst_rows,
        burst_daily_rows,
        burst_summary_rows,
        lifecycle_shadow_rows,
        lifecycle_shadow_daily_rows,
    )


def _reducing_burst_summary_rows(daily_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Compare burst buckets against same-day same-side reducing-order baseline."""
    lookup = _by(daily_rows, "day", "side", "dimension", "bucket")
    out: dict[tuple[str, str, str], dict[str, float]] = {}
    for row in daily_rows:
        day = row.get("day", "")
        side = row.get("side", "")
        dimension = row.get("dimension", "")
        bucket = row.get("bucket", "")
        if not day or side not in {"BUY", "SELL", "ALL"} or dimension == "all_reducing_orders":
            continue
        base = lookup.get((day, side, "all_reducing_orders", "all"), {})
        if not base:
            continue
        key = (side, dimension, bucket)
        acc = out.setdefault(key, {
            "days": 0,
            "support_days": 0,
            "orders": 0,
            "filled_orders": 0,
            "terminal_labeled_orders": 0,
            "sum_dfill": 0.0,
            "sum_dmarkout": 0.0,
            "sum_dterminal_pnl": 0.0,
            "sum_drepair": 0.0,
            "sum_dtail": 0.0,
            "sum_dbad": 0.0,
            "sum_dearly_dd": 0.0,
            "worse_terminal_days": 0,
            "worse_tail_days": 0,
            "worse_early_dd_days": 0,
        })
        acc["days"] += 1
        acc["orders"] += _float(row, "orders")
        acc["filled_orders"] += _float(row, "filled_orders")
        acc["terminal_labeled_orders"] += _float(row, "terminal_labeled_orders")
        if _float(row, "orders") < 50 or _float(row, "filled_orders") < 3 or _float(row, "terminal_labeled_orders") < 10:
            continue
        acc["support_days"] += 1
        dfill = _float(row, "fill_rate") - _float(base, "fill_rate")
        dmarkout = _float(row, "avg_markout_30s_bps") - _float(base, "avg_markout_30s_bps")
        dpnl = _float(row, "avg_terminal_campaign_pnl") - _float(base, "avg_terminal_campaign_pnl")
        drepair = _float(row, "terminal_repair_rate") - _float(base, "terminal_repair_rate")
        dtail = _float(row, "terminal_tail_loss_rate") - _float(base, "terminal_tail_loss_rate")
        dbad = _float(row, "terminal_bad_rate") - _float(base, "terminal_bad_rate")
        dearly = _float(row, "avg_terminal_early_20m_drawdown") - _float(base, "avg_terminal_early_20m_drawdown")
        acc["sum_dfill"] += dfill
        acc["sum_dmarkout"] += dmarkout
        acc["sum_dterminal_pnl"] += dpnl
        acc["sum_drepair"] += drepair
        acc["sum_dtail"] += dtail
        acc["sum_dbad"] += dbad
        acc["sum_dearly_dd"] += dearly
        acc["worse_terminal_days"] += int(dpnl < 0.0)
        acc["worse_tail_days"] += int(dtail > 0.0)
        acc["worse_early_dd_days"] += int(dearly > 0.0)

    rows: list[dict[str, str]] = []
    for (side, dimension, bucket), acc in sorted(out.items()):
        support = max(acc["support_days"], 1)
        avg_dpnl = acc["sum_dterminal_pnl"] / support
        avg_dtail = acc["sum_dtail"] / support
        avg_dearly = acc["sum_dearly_dd"] / support
        rows.append({
            "side": side,
            "dimension": dimension,
            "bucket": bucket,
            "days": str(int(acc["days"])),
            "support_days": str(int(acc["support_days"])),
            "orders": str(int(acc["orders"])),
            "filled_orders": str(int(acc["filled_orders"])),
            "terminal_labeled_orders": str(int(acc["terminal_labeled_orders"])),
            "avg_delta_fill_rate_vs_all_reducing": f"{acc['sum_dfill'] / support:.10f}",
            "avg_delta_markout_30s_bps_vs_all_reducing": f"{acc['sum_dmarkout'] / support:.10f}",
            "avg_delta_terminal_campaign_pnl_vs_all_reducing": f"{avg_dpnl:.10f}",
            "avg_delta_terminal_repair_rate_vs_all_reducing": f"{acc['sum_drepair'] / support:.10f}",
            "avg_delta_terminal_tail_loss_rate_vs_all_reducing": f"{avg_dtail:.10f}",
            "avg_delta_terminal_bad_rate_vs_all_reducing": f"{acc['sum_dbad'] / support:.10f}",
            "avg_delta_terminal_early_20m_drawdown_vs_all_reducing": f"{avg_dearly:.10f}",
            "worse_terminal_days": str(int(acc["worse_terminal_days"])),
            "worse_tail_days": str(int(acc["worse_tail_days"])),
            "worse_early_dd_days": str(int(acc["worse_early_dd_days"])),
            "verdict": (
                "harmful_burst_candidate"
                if acc["support_days"] >= 10
                and avg_dpnl < 0.0
                and (avg_dtail > 0.0 or avg_dearly > 0.0)
                else "diagnostic_only"
            ),
        })
    return rows


def _positive_eligibility_summary_rows(daily_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Compare eligibility masks against same-day same-side baseline."""
    lookup = _by(daily_rows, "day", "side", "mask")
    masks = sorted({r["mask"] for r in daily_rows if r["mask"] != "all_orders"})
    days_sides = sorted({(r["day"], r["side"]) for r in daily_rows if r["mask"] == "all_orders"})
    out: dict[tuple[str, str], dict[str, float]] = {}
    for day, side in days_sides:
        base = lookup.get((day, side, "all_orders"), {})
        if not base:
            continue
        for mask in masks:
            row = lookup.get((day, side, mask), {})
            key = (side, mask)
            acc = out.setdefault(key, {
                "days": 0,
                "support_days": 0,
                "filled_orders": 0,
                "orders": 0,
                "terminal_labeled_orders": 0,
                "better_terminal_pnl_days": 0,
                "better_repair_days": 0,
                "lower_tail_days": 0,
                "lower_early_drawdown_days": 0,
                "lower_campaign_max_abs_days": 0,
                "sum_dfill_rate": 0.0,
                "sum_dmarkout_30s": 0.0,
                "sum_dterminal_pnl": 0.0,
                "sum_drepair": 0.0,
                "sum_dtail": 0.0,
                "sum_dearly_dd": 0.0,
                "sum_dcampaign_max_abs": 0.0,
            })
            acc["days"] += 1
            orders = _float(row, "orders")
            fills = _float(row, "filled_orders")
            terminal_labeled = _float(row, "terminal_labeled_orders")
            acc["orders"] += orders
            acc["filled_orders"] += fills
            acc["terminal_labeled_orders"] += terminal_labeled
            if orders < 100 or fills < 5 or terminal_labeled < 20:
                continue
            acc["support_days"] += 1
            dfill = _float(row, "fill_rate") - _float(base, "fill_rate")
            dmo = _float(row, "avg_markout_30s_bps") - _float(base, "avg_markout_30s_bps")
            dpnl = _float(row, "avg_terminal_campaign_pnl") - _float(base, "avg_terminal_campaign_pnl")
            drepair = _float(row, "terminal_repair_rate") - _float(base, "terminal_repair_rate")
            dtail = _float(row, "terminal_tail_loss_rate") - _float(base, "terminal_tail_loss_rate")
            dearly = _float(row, "avg_terminal_early_20m_drawdown") - _float(base, "avg_terminal_early_20m_drawdown")
            dmax = _float(row, "avg_campaign_max_abs_qty") - _float(base, "avg_campaign_max_abs_qty")
            acc["sum_dfill_rate"] += dfill
            acc["sum_dmarkout_30s"] += dmo
            acc["sum_dterminal_pnl"] += dpnl
            acc["sum_drepair"] += drepair
            acc["sum_dtail"] += dtail
            acc["sum_dearly_dd"] += dearly
            acc["sum_dcampaign_max_abs"] += dmax
            acc["better_terminal_pnl_days"] += int(dpnl >= 0.0)
            acc["better_repair_days"] += int(drepair >= 0.0)
            acc["lower_tail_days"] += int(dtail <= 0.0)
            acc["lower_early_drawdown_days"] += int(dearly <= 0.0)
            acc["lower_campaign_max_abs_days"] += int(dmax <= 0.0)

    rows: list[dict[str, str]] = []
    for (side, mask), acc in sorted(out.items()):
        support = max(acc["support_days"], 1)
        rows.append({
            "side": side,
            "component": f"summary:{mask}",
            "days": str(int(acc["days"])),
            "support_days": str(int(acc["support_days"])),
            "orders": str(int(acc["orders"])),
            "filled_orders": str(int(acc["filled_orders"])),
            "terminal_labeled_orders": str(int(acc["terminal_labeled_orders"])),
            "avg_delta_fill_rate_vs_all": f"{acc['sum_dfill_rate'] / support:.10f}",
            "avg_delta_markout_30s_bps_vs_all": f"{acc['sum_dmarkout_30s'] / support:.10f}",
            "avg_delta_terminal_campaign_pnl_vs_all": f"{acc['sum_dterminal_pnl'] / support:.10f}",
            "avg_delta_terminal_repair_rate_vs_all": f"{acc['sum_drepair'] / support:.10f}",
            "avg_delta_terminal_tail_loss_rate_vs_all": f"{acc['sum_dtail'] / support:.10f}",
            "avg_delta_terminal_early_20m_drawdown_vs_all": f"{acc['sum_dearly_dd'] / support:.10f}",
            "avg_delta_campaign_max_abs_qty_vs_all": f"{acc['sum_dcampaign_max_abs'] / support:.10f}",
            "better_terminal_pnl_days": str(int(acc["better_terminal_pnl_days"])),
            "better_repair_days": str(int(acc["better_repair_days"])),
            "lower_tail_days": str(int(acc["lower_tail_days"])),
            "lower_early_drawdown_days": str(int(acc["lower_early_drawdown_days"])),
            "lower_campaign_max_abs_days": str(int(acc["lower_campaign_max_abs_days"])),
            "verdict": (
                "review_shadow_only"
                if acc["support_days"] >= 20
                and acc["sum_dterminal_pnl"] >= 0.0
                and acc["sum_dtail"] <= 0.0
                and acc["sum_dearly_dd"] <= 0.0
                else "diagnostic_only"
            ),
        })
    return rows


def _by(rows: list[dict[str, str]], *keys: str) -> dict[tuple[str, ...], dict[str, str]]:
    return {tuple(r[k] for k in keys): r for r in rows}


def _sanity_rows(agg_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    lookup = _by(agg_rows, "score", "side", "bucket")
    out: list[dict[str, str]] = []
    for side in ("BUY", "SELL"):
        for score in SCORES:
            if score == "toxic_risk_score":
                low_bucket, high_bucket = "q000_070", "q095_100"
            elif score in {"micro_fill_reach_score", "fill_probability_score"}:
                # 中文说明：micro reach / queue-exact-L2 fill probability 都是
                # 压缩的 quote-time 排序分数，retained panel 里通常没有
                # <0.33 的 low 桶；用 mid-vs-high 看它是否有排序力。
                low_bucket, high_bucket = "mid", "high"
            else:
                low_bucket, high_bucket = "low", "high"
            low = lookup.get((score, side, low_bucket), {})
            high = lookup.get((score, side, high_bucket), {})
            if not low or not high:
                verdict = "insufficient"
            else:
                d_fill = _float(high, "fill_rate") - _float(low, "fill_rate")
                d_mo = _float(high, "avg_markout_30s_bps") - _float(low, "avg_markout_30s_bps")
                d_tail = _float(high, "tail_rate_m50_30s") - _float(low, "tail_rate_m50_30s")
                d_age = _float(high, "avg_campaign_age_s") - _float(low, "avg_campaign_age_s")
                d_max = _float(high, "avg_campaign_max_abs_qty") - _float(low, "avg_campaign_max_abs_qty")
                d_terminal_pnl = _float(high, "avg_terminal_campaign_pnl") - _float(low, "avg_terminal_campaign_pnl")
                d_repair = _float(high, "terminal_repair_rate") - _float(low, "terminal_repair_rate")
                d_terminal_tail = _float(high, "terminal_tail_loss_rate") - _float(low, "terminal_tail_loss_rate")
                d_terminal_bad = _float(high, "terminal_bad_rate") - _float(low, "terminal_bad_rate")
                d_terminal_target = _float(high, "avg_terminal_outcome_risk_target") - _float(low, "avg_terminal_outcome_risk_target")
                d_early_dd = _float(high, "avg_terminal_early_20m_drawdown") - _float(low, "avg_terminal_early_20m_drawdown")
                if score in {"micro_fill_reach_score", "fill_probability_score", "reducing_burst_risk_score"}:
                    verdict = "pass" if d_fill >= 0 else "fail"
                    expectation = (
                        "high_reducing_burst_fill_rate_above_low"
                        if score == "reducing_burst_risk_score"
                        else "high_fill_rate_above_low"
                    )
                elif score == "fill_quality_score":
                    verdict = "pass" if d_mo >= 0 else "fail"
                    expectation = "high_markout_above_low"
                elif score == "toxic_risk_score":
                    verdict = "pass" if d_mo <= 0 or d_tail >= 0 else "fail"
                    expectation = "p95_markout_or_tail_worse_than_p70"
                elif score == "campaign_risk_score":
                    verdict = "pass" if d_age >= 0 and d_max >= 0 else "fail"
                    expectation = "high_inventory_exposure_above_low"
                elif score == "campaign_outcome_risk_score":
                    verdict = "pass" if (d_terminal_target >= 0 or d_terminal_tail >= 0 or d_terminal_pnl <= 0) else "fail"
                    expectation = "high_terminal_campaign_risk_above_low"
                elif score == "micro_reversion_score":
                    verdict = "pass" if (d_mo >= -0.25 and d_terminal_tail <= 0.02) else "fail"
                    expectation = "high_not_more_toxic_and_terminal_not_worse"
                elif score == "trend_inventory_risk_score":
                    verdict = "pass" if (d_mo <= 0 or d_tail >= 0 or d_terminal_target >= 0 or d_terminal_pnl <= 0) else "fail"
                    expectation = "high_markout_or_terminal_worse_than_low"
                elif score == "lifecycle_risk_score":
                    verdict = "pass" if (d_terminal_pnl <= 0 or d_terminal_tail >= 0 or d_early_dd >= 0) else "fail"
                    expectation = "high_terminal_or_early_campaign_risk_above_low"
                else:
                    verdict = "pass" if d_mo >= -0.25 else "fail"
                    expectation = "high_not_more_toxic_than_low"
                row = {
                    "side": side,
                    "score": score,
                    "expectation": expectation,
                    "bucket_mode": high.get("bucket_mode", ""),
                    "low_bucket": low_bucket,
                    "high_bucket": high_bucket,
                    "low_orders": low.get("orders", "0"),
                    "high_orders": high.get("orders", "0"),
                    "low_filled_orders": low.get("filled_orders", "0"),
                    "high_filled_orders": high.get("filled_orders", "0"),
                    "low_fill_rate": low.get("fill_rate", "0"),
                    "high_fill_rate": high.get("fill_rate", "0"),
                    "low_avg_markout_30s_bps": low.get("avg_markout_30s_bps", "0"),
                    "high_avg_markout_30s_bps": high.get("avg_markout_30s_bps", "0"),
                    "low_tail_rate_m50_30s": low.get("tail_rate_m50_30s", "0"),
                    "high_tail_rate_m50_30s": high.get("tail_rate_m50_30s", "0"),
                    "delta_high_minus_low_fill_rate": f"{d_fill:.10f}",
                    "delta_high_minus_low_markout_30s_bps": f"{d_mo:.10f}",
                    "delta_high_minus_low_tail_rate_m50_30s": f"{d_tail:.10f}",
                    "delta_high_minus_low_campaign_age_s": f"{d_age:.10f}",
                    "delta_high_minus_low_campaign_max_abs_qty": f"{d_max:.10f}",
                    "delta_high_minus_low_terminal_campaign_pnl": f"{d_terminal_pnl:.10f}",
                    "delta_high_minus_low_terminal_repair_rate": f"{d_repair:.10f}",
                    "delta_high_minus_low_terminal_tail_loss_rate": f"{d_terminal_tail:.10f}",
                    "delta_high_minus_low_terminal_bad_rate": f"{d_terminal_bad:.10f}",
                    "delta_high_minus_low_terminal_outcome_risk_target": f"{d_terminal_target:.10f}",
                    "delta_high_minus_low_terminal_early_20m_drawdown": f"{d_early_dd:.10f}",
                    "verdict": verdict,
                }
                out.append(row)
                continue
            out.append({"side": side, "score": score, "expectation": "", "verdict": verdict})
    return out


def _daily_pass_summary(daily_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    lookup = _by(daily_rows, "day", "score", "side", "bucket")
    keys = {(r["day"], r["score"], r["side"]) for r in daily_rows}
    out: dict[tuple[str, str], dict[str, float]] = {}
    for day, score, side in sorted(keys):
        if score == "toxic_risk_score":
            low_bucket, high_bucket = "q000_070", "q095_100"
        elif score in {"micro_fill_reach_score", "fill_probability_score"}:
            low_bucket, high_bucket = "mid", "high"
        else:
            low_bucket, high_bucket = "low", "high"
        low = lookup.get((day, score, side, low_bucket), {})
        high = lookup.get((day, score, side, high_bucket), {})
        key = (score, side)
        acc = out.setdefault(key, {
            "days": 0,
            "pass_like": 0,
            "bad": 0,
            "insufficient": 0,
            "sum_dfill": 0.0,
            "sum_dmo": 0.0,
            "sum_dage": 0.0,
            "sum_dmax": 0.0,
            "sum_dterminal_pnl": 0.0,
            "sum_dterminal_repair": 0.0,
            "sum_dterminal_tail": 0.0,
            "sum_dterminal_bad": 0.0,
            "sum_dterminal_target": 0.0,
            "sum_dterminal_early_dd": 0.0,
        })
        if not low or not high:
            continue
        acc["days"] += 1
        if _float(high, "filled_orders") < 5 or _float(high, "orders") < 100 or _float(low, "orders") <= 0:
            acc["insufficient"] += 1
            continue
        d_fill = _float(high, "fill_rate") - _float(low, "fill_rate")
        d_mo = _float(high, "avg_markout_30s_bps") - _float(low, "avg_markout_30s_bps")
        d_age = _float(high, "avg_campaign_age_s") - _float(low, "avg_campaign_age_s")
        d_max = _float(high, "avg_campaign_max_abs_qty") - _float(low, "avg_campaign_max_abs_qty")
        d_terminal_pnl = _float(high, "avg_terminal_campaign_pnl") - _float(low, "avg_terminal_campaign_pnl")
        d_terminal_repair = _float(high, "terminal_repair_rate") - _float(low, "terminal_repair_rate")
        d_terminal_tail = _float(high, "terminal_tail_loss_rate") - _float(low, "terminal_tail_loss_rate")
        d_terminal_bad = _float(high, "terminal_bad_rate") - _float(low, "terminal_bad_rate")
        d_terminal_target = _float(high, "avg_terminal_outcome_risk_target") - _float(low, "avg_terminal_outcome_risk_target")
        d_terminal_early_dd = _float(high, "avg_terminal_early_20m_drawdown") - _float(low, "avg_terminal_early_20m_drawdown")
        acc["sum_dfill"] += d_fill
        acc["sum_dmo"] += d_mo
        acc["sum_dage"] += d_age
        acc["sum_dmax"] += d_max
        acc["sum_dterminal_pnl"] += d_terminal_pnl
        acc["sum_dterminal_repair"] += d_terminal_repair
        acc["sum_dterminal_tail"] += d_terminal_tail
        acc["sum_dterminal_bad"] += d_terminal_bad
        acc["sum_dterminal_target"] += d_terminal_target
        acc["sum_dterminal_early_dd"] += d_terminal_early_dd
        if score in {"micro_fill_reach_score", "fill_probability_score", "reducing_burst_risk_score"}:
            good = d_fill >= 0
        elif score == "fill_quality_score":
            good = d_mo >= 0
        elif score == "toxic_risk_score":
            good = d_mo <= 0 or _float(high, "tail_rate_m50_30s") >= _float(low, "tail_rate_m50_30s")
        elif score == "campaign_risk_score":
            good = d_age >= 0 and d_max >= 0
        elif score == "campaign_outcome_risk_score":
            good = d_terminal_target >= 0 or d_terminal_tail >= 0 or d_terminal_pnl <= 0
        elif score == "micro_reversion_score":
            good = d_mo >= -0.25 and d_terminal_tail <= 0.02
        elif score == "trend_inventory_risk_score":
            good = d_mo <= 0 or _float(high, "tail_rate_m50_30s") >= _float(low, "tail_rate_m50_30s") or d_terminal_target >= 0 or d_terminal_pnl <= 0
        elif score == "lifecycle_risk_score":
            good = d_terminal_pnl <= 0 or d_terminal_tail >= 0 or d_terminal_early_dd >= 0
        else:
            good = d_mo >= -0.25
        acc["pass_like" if good else "bad"] += 1

    rows: list[dict[str, str]] = []
    for (score, side), acc in sorted(out.items()):
        denom = max(acc["pass_like"] + acc["bad"], 1)
        rows.append({
            "score": score,
            "side": side,
            "days": str(int(acc["days"])),
            "pass_like_days": str(int(acc["pass_like"])),
            "bad_days": str(int(acc["bad"])),
            "insufficient_days": str(int(acc["insufficient"])),
            "avg_delta_fill_rate": f"{acc['sum_dfill'] / denom:.10f}",
            "avg_delta_markout_30s_bps": f"{acc['sum_dmo'] / denom:.10f}",
            "avg_delta_campaign_age_s": f"{acc['sum_dage'] / denom:.10f}",
            "avg_delta_campaign_max_abs_qty": f"{acc['sum_dmax'] / denom:.10f}",
            "avg_delta_terminal_campaign_pnl": f"{acc['sum_dterminal_pnl'] / denom:.10f}",
            "avg_delta_terminal_repair_rate": f"{acc['sum_dterminal_repair'] / denom:.10f}",
            "avg_delta_terminal_tail_loss_rate": f"{acc['sum_dterminal_tail'] / denom:.10f}",
            "avg_delta_terminal_bad_rate": f"{acc['sum_dterminal_bad'] / denom:.10f}",
            "avg_delta_terminal_outcome_risk_target": f"{acc['sum_dterminal_target'] / denom:.10f}",
            "avg_delta_terminal_early_20m_drawdown": f"{acc['sum_dterminal_early_dd'] / denom:.10f}",
        })
    return rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--order-level-csv", type=Path, required=True)
    ap.add_argument(
        "--replay-fills-csv",
        type=Path,
        default=None,
        help="Optional replay fills trace used to attach terminal campaign labels without rewriting the order-level CSV.",
    )
    ap.add_argument(
        "--replay-orders-csv",
        type=Path,
        default=None,
        help="Optional replay orders trace used to overlay quote-time queue/exact-L2/lifetime fields onto cached order-level rows.",
    )
    ap.add_argument(
        "--days",
        nargs="*",
        default=None,
        help="Optional UTC day filter, e.g. --days 2026-06-26 2026-07-01.",
    )
    ap.add_argument("--out-prefix", type=Path, required=True)
    args = ap.parse_args()

    days = set(args.days) if args.days else None
    side_thresholds, day_side_thresholds = _collect_toxic_thresholds(
        args.order_level_csv,
        replay_orders_path=args.replay_orders_csv,
        days=days,
    )
    terminal_labels = _load_replay_campaign_labels_filtered(args.replay_fills_csv, days) if args.replay_fills_csv else None
    (
        agg_rows,
        daily_rows,
        calibration_rows,
        eligibility_rows,
        eligibility_daily_rows,
        eligibility_component_rows,
        reducing_burst_rows,
        reducing_burst_daily_rows,
        reducing_burst_summary_rows,
        lifecycle_shadow_rows,
        lifecycle_shadow_daily_rows,
    ) = _build_tables(
        args.order_level_csv,
        side_thresholds,
        day_side_thresholds,
        terminal_labels=terminal_labels,
        replay_orders_path=args.replay_orders_csv,
        days=days,
    )
    sanity_rows = _sanity_rows(agg_rows)
    daily_summary_rows = _daily_pass_summary(daily_rows)

    _write_csv(args.out_prefix.with_suffix(".score_buckets_fast.csv"), agg_rows)
    _write_csv(args.out_prefix.with_suffix(".score_daily_fast.csv"), daily_rows)
    _write_csv(args.out_prefix.with_suffix(".score_calibration_fast.csv"), calibration_rows)
    _write_csv(args.out_prefix.with_suffix(".score_sanity_fast.csv"), sanity_rows)
    _write_csv(args.out_prefix.with_suffix(".score_daily_pass_summary_fast.csv"), daily_summary_rows)
    _write_csv(args.out_prefix.with_suffix(".positive_eligibility_fast.csv"), eligibility_rows)
    _write_csv(args.out_prefix.with_suffix(".positive_eligibility_daily_fast.csv"), eligibility_daily_rows)
    _write_csv(args.out_prefix.with_suffix(".positive_eligibility_components_fast.csv"), eligibility_component_rows)
    _write_csv(args.out_prefix.with_suffix(".reducing_fill_burst_fast.csv"), reducing_burst_rows)
    _write_csv(args.out_prefix.with_suffix(".reducing_fill_burst_daily_fast.csv"), reducing_burst_daily_rows)
    _write_csv(args.out_prefix.with_suffix(".reducing_fill_burst_summary_fast.csv"), reducing_burst_summary_rows)
    _write_csv(args.out_prefix.with_suffix(".lifecycle_shadow_fast.csv"), lifecycle_shadow_rows)
    _write_csv(args.out_prefix.with_suffix(".lifecycle_shadow_daily_fast.csv"), lifecycle_shadow_daily_rows)
    print(f"wrote {args.out_prefix}.score_*_fast.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
