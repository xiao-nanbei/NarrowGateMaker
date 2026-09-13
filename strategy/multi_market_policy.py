"""Bounded cross-venue policy decisions for maker replay and shadow use."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

from strategy.global_flow import GlobalFlowState

MULTI_MARKET_POLICY_SCHEMA_VERSION = "multi_market_policy.v1"
NOOP_MODE = "noop"
POST_FILL_STOP_ADD_MODE = "post_fill_stop_add"


@dataclass(frozen=True)
class MultiMarketPolicyConfig:
    enabled: bool = False
    mode: str = NOOP_MODE
    horizon_ms: int = 250
    min_abs_inventory: float = 0.0
    min_campaign_age_s: float = 0.0
    min_campaign_fills: int = 1
    require_spot_and_perp: bool = True
    min_venue_agreement: float = 2.0 / 3.0
    min_external_move_bps: float = 0.25
    min_global_flow_pressure: float = 0.10
    min_bridge_move_bps: float = 0.10
    max_repair_probability: float = 0.60
    min_repair_probability_drop: float = 0.05
    repair_lookback_ms: int = 1_000
    repair_max_age_ms: int = 1_000

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> MultiMarketPolicyConfig:
        enabled = bool(params.get("multi_market_policy_enabled", False))
        mode = str(params.get("multi_market_policy_mode", NOOP_MODE) or NOOP_MODE).lower()
        if not enabled:
            mode = NOOP_MODE
        if mode not in {NOOP_MODE, POST_FILL_STOP_ADD_MODE}:
            raise ValueError(f"unsupported multi_market_policy_mode={mode!r}")
        return cls(
            enabled=enabled,
            mode=mode,
            horizon_ms=max(1, int(params.get("multi_market_policy_horizon_ms", 250) or 250)),
            min_abs_inventory=max(
                0.0, float(params.get("multi_market_policy_min_abs_inventory", 0.0) or 0.0)
            ),
            min_campaign_age_s=max(
                0.0, float(params.get("multi_market_policy_min_campaign_age_s", 0.0) or 0.0)
            ),
            min_campaign_fills=max(
                1, int(params.get("multi_market_policy_min_campaign_fills", 1) or 1)
            ),
            require_spot_and_perp=bool(
                params.get("multi_market_policy_require_spot_and_perp", True)
            ),
            min_venue_agreement=min(
                1.0,
                max(
                    0.0,
                    float(params.get("multi_market_policy_min_venue_agreement", 2.0 / 3.0)),
                ),
            ),
            min_external_move_bps=max(
                0.0, float(params.get("multi_market_policy_min_external_move_bps", 0.25))
            ),
            min_global_flow_pressure=max(
                0.0,
                float(params.get("multi_market_policy_min_global_flow_pressure", 0.10)),
            ),
            min_bridge_move_bps=max(
                0.0, float(params.get("multi_market_policy_min_bridge_move_bps", 0.10))
            ),
            max_repair_probability=min(
                1.0,
                max(
                    0.0,
                    float(params.get("multi_market_policy_max_repair_probability", 0.60)),
                ),
            ),
            min_repair_probability_drop=max(
                0.0,
                float(params.get("multi_market_policy_min_repair_probability_drop", 0.05)),
            ),
            repair_lookback_ms=max(
                1, int(params.get("multi_market_policy_repair_lookback_ms", 1_000) or 1_000)
            ),
            repair_max_age_ms=max(
                1, int(params.get("multi_market_policy_repair_max_age_ms", 1_000) or 1_000)
            ),
        )


@dataclass(frozen=True)
class MultiMarketPolicyContext:
    decision_ts_ns: int
    inventory: float
    campaign_active: bool
    campaign_age_s: float
    campaign_fills: int
    repair_probability: float = math.nan
    repair_probability_change: float = math.nan
    repair_probability_age_ms: float = math.inf
    global_flow_state: GlobalFlowState | None = None


@dataclass(frozen=True)
class MultiMarketSideDecision:
    side: str
    allow_exposure_increase: bool = True
    active: bool = False
    reason: str = "noop"


@dataclass(frozen=True)
class MultiMarketPolicyDecision:
    schema_version: str
    mode: str
    buy: MultiMarketSideDecision
    sell: MultiMarketSideDecision
    evidence: dict[str, Any] = field(default_factory=dict)

    def for_side(self, side: str) -> MultiMarketSideDecision:
        normalized = str(side).upper()
        if normalized == "BUY":
            return self.buy
        if normalized == "SELL":
            return self.sell
        raise ValueError(f"unsupported side={side!r}")


class MultiMarketPolicy:
    """Evaluate an isolated cross-venue action without changing quote prices."""

    def __init__(self, config: MultiMarketPolicyConfig | None = None) -> None:
        self.config = config or MultiMarketPolicyConfig()

    def evaluate(self, context: MultiMarketPolicyContext) -> MultiMarketPolicyDecision:
        evidence = self._state_evidence(context)
        if not self.config.enabled or self.config.mode == NOOP_MODE:
            return self._decision(reason="noop", evidence=evidence)
        if self.config.mode == POST_FILL_STOP_ADD_MODE:
            return self._post_fill_stop_add(context, evidence)
        raise ValueError(f"unsupported multi-market policy mode={self.config.mode!r}")

    def _post_fill_stop_add(
        self,
        context: MultiMarketPolicyContext,
        evidence: dict[str, Any],
    ) -> MultiMarketPolicyDecision:
        inventory = float(context.inventory)
        if (
            not context.campaign_active
            or abs(inventory) <= 1e-12
            or abs(inventory) < self.config.min_abs_inventory
            or context.campaign_fills < self.config.min_campaign_fills
            or context.campaign_age_s < self.config.min_campaign_age_s
        ):
            return self._decision(reason="campaign_ineligible", evidence=evidence)
        state = context.global_flow_state
        if state is None:
            return self._decision(reason="global_state_missing", evidence=evidence)
        window = state.window(self.config.horizon_ms)
        if not window or not bool(window.get("valid", 0)):
            return self._decision(reason="global_state_invalid", evidence=evidence)
        if not (
            math.isfinite(context.repair_probability)
            and math.isfinite(context.repair_probability_change)
            and context.repair_probability_age_ms <= self.config.repair_max_age_ms
        ):
            return self._decision(
                reason="repair_probability_missing_or_stale", evidence=evidence
            )
        repair_deteriorating = (
            context.repair_probability <= self.config.max_repair_probability
            and context.repair_probability_change
            <= -self.config.min_repair_probability_drop
        )
        if not repair_deteriorating:
            return self._decision(reason="repair_not_deteriorating", evidence=evidence)

        inventory_sign = 1.0 if inventory > 0.0 else -1.0

        def _adverse(value: Any, minimum: float) -> bool:
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return False
            return math.isfinite(numeric) and inventory_sign * numeric <= -minimum

        spot = window.get("spot", {})
        perp = window.get("perp", {})
        factors = (spot, perp) if self.config.require_spot_and_perp else tuple(
            factor for factor in (spot, perp) if bool(factor.get("valid", 0))
        )
        if not factors or any(not bool(factor.get("valid", 0)) for factor in factors):
            return self._decision(reason="external_consensus_invalid", evidence=evidence)
        external_adverse = all(
            float(factor.get("venue_agreement", 0.0) or 0.0)
            >= self.config.min_venue_agreement
            and _adverse(factor.get("mid_move_bps"), self.config.min_external_move_bps)
            for factor in factors
        )
        global_flow_adverse = _adverse(
            window.get("global_flow_pressure"),
            self.config.min_global_flow_pressure,
        )
        bridge_adverse = _adverse(
            window.get("local_bridge_move_bps"),
            self.config.min_bridge_move_bps,
        )
        evidence.update(
            {
                "external_adverse": int(external_adverse),
                "global_flow_adverse": int(global_flow_adverse),
                "bridge_adverse": int(bridge_adverse),
            }
        )
        if not (external_adverse and global_flow_adverse and bridge_adverse):
            return self._decision(reason="adverse_conjunction_not_met", evidence=evidence)

        add_side = "BUY" if inventory > 0.0 else "SELL"
        return self._decision(
            blocked_side=add_side,
            reason="post_fill_global_adverse_repair_down",
            evidence=evidence,
        )

    def _state_evidence(self, context: MultiMarketPolicyContext) -> dict[str, Any]:
        evidence: dict[str, Any] = {
            "horizon_ms": self.config.horizon_ms,
            "inventory": float(context.inventory),
            "campaign_active": int(context.campaign_active),
            "campaign_age_s": float(context.campaign_age_s),
            "campaign_fills": int(context.campaign_fills),
            "repair_probability": context.repair_probability,
            "repair_probability_change": context.repair_probability_change,
            "repair_probability_age_ms": context.repair_probability_age_ms,
            "global_state_valid": 0,
        }
        state = context.global_flow_state
        if state is None:
            return evidence
        window = state.window(self.config.horizon_ms)
        if not window:
            return evidence
        spot = window.get("spot", {})
        perp = window.get("perp", {})
        evidence.update(
            {
                "global_state_valid": int(bool(window.get("valid", 0))),
                "spot_valid": int(bool(spot.get("valid", 0))),
                "perp_valid": int(bool(perp.get("valid", 0))),
                "spot_fresh_venues": int(spot.get("fresh_venues", 0) or 0),
                "perp_fresh_venues": int(perp.get("fresh_venues", 0) or 0),
                "spot_venue_agreement": float(
                    spot.get("venue_agreement", 0.0) or 0.0
                ),
                "perp_venue_agreement": float(
                    perp.get("venue_agreement", 0.0) or 0.0
                ),
                "spot_move_bps": spot.get("mid_move_bps", math.nan),
                "perp_move_bps": perp.get("mid_move_bps", math.nan),
                "global_flow_pressure": window.get(
                    "global_flow_pressure", math.nan
                ),
                "global_mid_move_bps": window.get(
                    "global_mid_move_bps", math.nan
                ),
                "local_bridge_move_bps": window.get(
                    "local_bridge_move_bps", math.nan
                ),
                "global_minus_bridge_bps": window.get(
                    "global_minus_bridge_bps", math.nan
                ),
            }
        )
        return evidence

    def _decision(
        self,
        *,
        blocked_side: str | None = None,
        reason: str,
        evidence: dict[str, Any] | None = None,
    ) -> MultiMarketPolicyDecision:
        def _side(side: str) -> MultiMarketSideDecision:
            blocked = blocked_side == side
            return MultiMarketSideDecision(
                side=side,
                allow_exposure_increase=not blocked,
                active=blocked,
                reason=reason if blocked else "noop",
            )

        return MultiMarketPolicyDecision(
            schema_version=MULTI_MARKET_POLICY_SCHEMA_VERSION,
            mode=self.config.mode,
            buy=_side("BUY"),
            sell=_side("SELL"),
            evidence=dict(evidence or {}),
        )
