"""Versioned restart-safe state for continuous replay."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "continuous_replay_state.v2"
RESTART_RESET_FIELDS = (
    "active_orders",
    "pending_cancel",
    "queue_state",
    "order_age",
    "q90_hazard_cursor",
    "runtime_campaign",
    "fill_cooldown",
    "loss_cooldown",
    "markout_ema",
    "signal_runtime",
)
_EPS = 1e-10


def validate_replay_initial_state(
    payload: Any, *, backend: str = "python"
) -> dict[str, Any]:
    """Distinguish partial diagnostic state from a canonical live snapshot.

    The legacy dictionary is a deliberately partial replay input, not a live
    checkpoint. A producer's completeness claim does not establish that this
    consumer restores its signal, order-queue and control-loop state.
    """
    if payload is None:
        return {}
    if not isinstance(payload, Mapping):
        raise ValueError("initial_live_state must be a mapping")
    if backend not in {"python", "cpp"}:
        raise ValueError("initial-state backend must be python or cpp")
    schema = str(payload.get("schema_version", ""))
    canonical_fields = {
        "adverse_markout_pause", "fill_cooldown_lineage", "order_lifecycle",
        "quote_policy_clocks", "signal_feature_dag_warmup",
    }
    if schema.startswith("narrowgate_live_initial_runtime_state.") or (
        canonical_fields.intersection(payload)
    ):
        from models.replay.prospective_baseline_epoch import (
            PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS,
        )

        missing = [
            field for field in PROSPECTIVE_INITIAL_STATE_REQUIRED_DOMAINS
            if not isinstance(payload.get(field), Mapping) or not payload[field]
        ]
        completeness = payload.get("completeness")
        producer_unsupported = (
            completeness.get("unsupported_initial_state_fields", ())
            if isinstance(completeness, Mapping) else ()
        )
        detail = "; missing domains: " + ", ".join(missing) if missing else ""
        if producer_unsupported:
            detail += "; producer unsupported state: " + repr(producer_unsupported)
        raise ValueError(
            f"canonical live snapshot cannot be restored by {backend} replay: "
            "unsupported restore domains: signal_feature_dag_warmup, "
            "quote_policy_clocks, order_lifecycle (inherited queue/ACK state), "
            "q90_runtime, defense_and_stale_guards, sync_degrade, "
            "post_fill_response; adverse_markout_pause and fill_cooldown_lineage "
            "are not the experimental replay ABI" + detail
        )
    if schema == SCHEMA_VERSION:
        raise ValueError(
            "continuous replay checkpoint must use the continuous replay runner; "
            "it is not an initial_live_state snapshot"
        )
    if schema not in {"", "narrowgate.live_replay_initial_state.v1"}:
        raise ValueError(f"unsupported initial_live_state schema: {schema}")
    # These Python-only domains used to disappear at the native boundary.
    if backend == "cpp":
        unsupported = [
            key for key in ("active_orders", "markout", "fill_cooldown", "campaign")
            if payload.get(key)
        ]
        if unsupported:
            raise ValueError(
                "C++ replay cannot restore initial_live_state domains: "
                + ", ".join(unsupported) + "; use Python replay"
            )
    return dict(payload)


def canonical_sha256(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class EconomicCampaignState:
    """Campaign attribution that survives process and calendar boundaries."""

    campaign_id: str
    side: str
    start_ts_ms: int
    start_equity_usdc: float
    peak_abs_inventory_btc: float

    def validate(self, position_btc: float) -> None:
        if not self.campaign_id.strip():
            raise ValueError("economic campaign_id must be non-empty")
        if self.side not in {"LONG", "SHORT"}:
            raise ValueError("economic campaign side must be LONG or SHORT")
        if self.start_ts_ms < 0:
            raise ValueError("economic campaign start timestamp must be non-negative")
        if not math.isfinite(self.start_equity_usdc):
            raise ValueError("economic campaign start equity must be finite")
        if not math.isfinite(self.peak_abs_inventory_btc) or self.peak_abs_inventory_btc <= 0:
            raise ValueError("economic campaign peak inventory must be positive")
        if self.side == "LONG" and position_btc <= _EPS:
            raise ValueError("LONG economic campaign requires positive inventory")
        if self.side == "SHORT" and position_btc >= -_EPS:
            raise ValueError("SHORT economic campaign requires negative inventory")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> EconomicCampaignState:
        return cls(
            campaign_id=str(payload["campaign_id"]),
            side=str(payload["side"]),
            start_ts_ms=int(payload["start_ts_ms"]),
            start_equity_usdc=float(payload["start_equity_usdc"]),
            peak_abs_inventory_btc=float(payload["peak_abs_inventory_btc"]),
        )


@dataclass(frozen=True)
class ContinuousReplayState:
    """Economic state plus explicit evidence that transient order state is clean."""

    arm_id: str
    checkpoint_ts_ms: int
    cash_usdc: float
    position_btc: float
    average_entry_price: float
    cumulative_realized_pnl_usdc: float
    cumulative_fees_usdc: float
    equity_anchor_usdc: float
    last_mark_price: float
    cumulative_pnl_usdc: float
    economic_campaign: EconomicCampaignState | None = None
    restart_generation: int = 0
    orders_terminal: bool = True
    active_order_count: int = 0
    pending_cancel_count: int = 0
    queue_cursor_count: int = 0
    q90_cursor_count: int = 0
    feature_warmup_ready: bool = False
    quoting_enabled: bool = False
    runtime_reset_fields: tuple[str, ...] = ()
    cumulative_funding_usdc: float = 0.0
    last_funding_ts_ms: int = -1
    schema_version: str = SCHEMA_VERSION

    @property
    def equity_usdc(self) -> float:
        return self.cash_usdc + self.position_btc * self.last_mark_price

    @property
    def restart_safe(self) -> bool:
        return bool(
            self.orders_terminal
            and self.active_order_count == 0
            and self.pending_cancel_count == 0
            and self.queue_cursor_count == 0
            and self.q90_cursor_count == 0
        )

    def validate(self, *, require_restart_safe: bool = False) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError("unsupported continuous replay state schema")
        if not self.arm_id.strip():
            raise ValueError("continuous replay arm_id must be non-empty")
        if self.checkpoint_ts_ms < 0 or self.restart_generation < 0:
            raise ValueError("checkpoint timestamp and restart generation must be non-negative")
        numeric = (
            self.cash_usdc,
            self.position_btc,
            self.average_entry_price,
            self.cumulative_realized_pnl_usdc,
            self.cumulative_fees_usdc,
            self.equity_anchor_usdc,
            self.last_mark_price,
            self.cumulative_pnl_usdc,
            self.cumulative_funding_usdc,
        )
        if any(not math.isfinite(value) for value in numeric):
            raise ValueError("continuous replay state contains a non-finite value")
        if self.last_mark_price <= 0:
            raise ValueError("continuous replay mark price must be positive")
        if not -1 <= self.last_funding_ts_ms <= self.checkpoint_ts_ms:
            raise ValueError("funding timestamp must be absent or not later than the checkpoint")
        # Signed transaction costs: positive is a fee paid; negative is an
        # exchange rebate received.  Finiteness is enforced by ``numeric``.
        if abs(self.position_btc) <= _EPS:
            if abs(self.average_entry_price) > _EPS:
                raise ValueError("flat state must have zero average entry price")
            if self.economic_campaign is not None:
                raise ValueError("flat state cannot retain an open economic campaign")
        else:
            if self.average_entry_price <= 0:
                raise ValueError("non-flat state requires a positive average entry price")
            if self.economic_campaign is None:
                raise ValueError("non-flat state requires an economic campaign identity")
            self.economic_campaign.validate(self.position_btc)
        expected_pnl = self.equity_usdc - self.equity_anchor_usdc
        if not math.isclose(
            self.cumulative_pnl_usdc,
            expected_pnl,
            rel_tol=0.0,
            abs_tol=1e-8,
        ):
            raise ValueError("cumulative PnL does not equal cash plus marked inventory")
        counts = (
            self.active_order_count,
            self.pending_cancel_count,
            self.queue_cursor_count,
            self.q90_cursor_count,
        )
        if any(count < 0 for count in counts):
            raise ValueError("transient replay counts cannot be negative")
        if self.orders_terminal and any(count != 0 for count in counts):
            raise ValueError("terminal order state cannot retain orders or cursors")
        if self.quoting_enabled and not self.feature_warmup_ready:
            raise ValueError("quoting cannot be enabled before causal warmup is ready")
        if self.quoting_enabled and not self.restart_safe:
            raise ValueError("quoting cannot resume with inherited transient order state")
        if require_restart_safe and not self.restart_safe:
            raise ValueError("continuous replay checkpoint is not restart-safe")

    def for_planned_restart(self, checkpoint_ts_ms: int) -> ContinuousReplayState:
        """Preserve economics while clearing every process-local state family."""

        state = replace(
            self,
            checkpoint_ts_ms=int(checkpoint_ts_ms),
            restart_generation=self.restart_generation + 1,
            orders_terminal=True,
            active_order_count=0,
            pending_cancel_count=0,
            queue_cursor_count=0,
            q90_cursor_count=0,
            feature_warmup_ready=False,
            quoting_enabled=False,
            runtime_reset_fields=RESTART_RESET_FIELDS,
        )
        state.validate(require_restart_safe=True)
        return state

    def with_mark(self, checkpoint_ts_ms: int, mark_price: float) -> ContinuousReplayState:
        equity = self.cash_usdc + self.position_btc * float(mark_price)
        state = replace(
            self,
            checkpoint_ts_ms=int(checkpoint_ts_ms),
            last_mark_price=float(mark_price),
            cumulative_pnl_usdc=equity - self.equity_anchor_usdc,
        )
        state.validate()
        return state

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["runtime_reset_fields"] = list(self.runtime_reset_fields)
        return payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ContinuousReplayState:
        campaign_payload = payload.get("economic_campaign")
        state = cls(
            arm_id=str(payload["arm_id"]),
            checkpoint_ts_ms=int(payload["checkpoint_ts_ms"]),
            cash_usdc=float(payload["cash_usdc"]),
            position_btc=float(payload["position_btc"]),
            average_entry_price=float(payload["average_entry_price"]),
            cumulative_realized_pnl_usdc=float(
                payload["cumulative_realized_pnl_usdc"]
            ),
            cumulative_fees_usdc=float(payload["cumulative_fees_usdc"]),
            equity_anchor_usdc=float(payload["equity_anchor_usdc"]),
            last_mark_price=float(payload["last_mark_price"]),
            cumulative_pnl_usdc=float(payload["cumulative_pnl_usdc"]),
            economic_campaign=(
                EconomicCampaignState.from_dict(campaign_payload)
                if isinstance(campaign_payload, Mapping)
                else None
            ),
            restart_generation=int(payload.get("restart_generation", 0)),
            orders_terminal=bool(payload.get("orders_terminal", True)),
            active_order_count=int(payload.get("active_order_count", 0)),
            pending_cancel_count=int(payload.get("pending_cancel_count", 0)),
            queue_cursor_count=int(payload.get("queue_cursor_count", 0)),
            q90_cursor_count=int(payload.get("q90_cursor_count", 0)),
            feature_warmup_ready=bool(payload.get("feature_warmup_ready", False)),
            quoting_enabled=bool(payload.get("quoting_enabled", False)),
            runtime_reset_fields=tuple(payload.get("runtime_reset_fields", ())),
            cumulative_funding_usdc=float(payload.get("cumulative_funding_usdc", 0.0)),
            last_funding_ts_ms=int(payload.get("last_funding_ts_ms", -1)),
            schema_version=str(payload.get("schema_version", "")),
        )
        state.validate()
        return state


def write_checkpoint(path: Path, state: ContinuousReplayState) -> str:
    state.validate(require_restart_safe=True)
    payload = state.to_dict()
    payload["state_sha256"] = canonical_sha256(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.partial")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return str(payload["state_sha256"])


def read_checkpoint(path: Path) -> ContinuousReplayState:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = str(payload.pop("state_sha256", ""))
    if canonical_sha256(payload) != expected:
        raise ValueError("continuous replay checkpoint hash mismatch")
    state = ContinuousReplayState.from_dict(payload)
    state.validate(require_restart_safe=True)
    return state
