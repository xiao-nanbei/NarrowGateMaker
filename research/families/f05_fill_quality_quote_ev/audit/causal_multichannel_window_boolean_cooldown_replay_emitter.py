"""Replay bridge for atomic cooldown-v2 assignment snapshots.

The bridge advances a streaming, outcome-blind 100ms feature state only as far
as the strategy-visible fill cutoff.  It then binds that state to the exact
partial-fill, order, queue, inventory, and campaign context supplied by the
Python replay engine.  It does not choose a duration or read an economic
outcome.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    BASE_WINDOW_WIDTH_NS,
    IDENTITY,
    M0_REQUIRED_FIELDS,
    CausalMultichannelEmaState,
    CausalWindowObservation,
    FeatureContractError,
    validate_m0_context,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    HISTORICAL_EXCHANGE_EVENT_PROFILE,
    CooldownAssignmentSnapshotV2,
    SnapshotContractError,
    capture_cooldown_assignment_snapshot,
)

REPLAY_EMITTER_SCHEMA_VERSION = f"{IDENTITY}.replay_emitter.v1"


class ReplayEmitterError(RuntimeError):
    """Raised when replay cannot form one causal assignment snapshot."""


def build_cpp_predicate_row(cpp: Any, opportunity: Mapping[str, Any]) -> Any:
    """Build the generic C++ ABI row for one replay-visible fill."""

    required = (
        "exposure_fill_ordinal",
        "fill_visible_ts_ms",
        "side",
        "campaign_id",
        "opportunity_id",
        "policy_input_valid",
        "feature::support_valid",
        "feature::channel_support_valid",
        "owner_fallback_reason",
    )
    missing = tuple(name for name in required if name not in opportunity)
    if missing:
        raise ReplayEmitterError(
            f"C++ target predicate row lacks required fields: {list(missing)}"
        )
    row = cpp.F05CooldownPredicateRow()
    row.exposure_fill_ordinal = int(opportunity["exposure_fill_ordinal"])
    row.fill_ts_ms = int(opportunity["fill_visible_ts_ms"])
    row.side = cpp.Side.Buy if str(opportunity["side"]).upper() == "BUY" else cpp.Side.Sell
    row.campaign_id = int(opportunity["campaign_id"])
    row.snapshot_id = str(opportunity["opportunity_id"])
    row.policy_input_valid = bool(opportunity["policy_input_valid"])
    row.support_valid = bool(opportunity["feature::support_valid"])
    row.channel_support_valid = bool(opportunity["feature::channel_support_valid"])
    fallback_reason = opportunity["owner_fallback_reason"]
    row.snapshot_fallback_reason = (
        "" if fallback_reason is None or fallback_reason != fallback_reason else str(fallback_reason)
    )
    row.predicate_values = []
    return row


def validate_cpp_predicate_row(
    cpp: Any,
    row: Any,
    opportunity: Mapping[str, Any],
    *,
    expected_predicate_count: int,
) -> None:
    """Fail closed when a replay row drifts before entering the C++ ABI."""

    if not isinstance(expected_predicate_count, int) or expected_predicate_count <= 0:
        raise ReplayEmitterError("C++ predicate count is invalid")
    expected_side = cpp.Side.Buy if str(opportunity["side"]).upper() == "BUY" else cpp.Side.Sell
    identity = {
        "exposure_fill_ordinal": (
            int(row.exposure_fill_ordinal),
            int(opportunity["exposure_fill_ordinal"]),
        ),
        "fill_ts_ms": (int(row.fill_ts_ms), int(opportunity["fill_visible_ts_ms"])),
        "campaign_id": (int(row.campaign_id), int(opportunity["campaign_id"])),
        "snapshot_id": (str(row.snapshot_id), str(opportunity["opportunity_id"])),
    }
    drifted = [name for name, (actual, expected) in identity.items() if actual != expected]
    if drifted or row.side != expected_side:
        raise ReplayEmitterError(
            "C++ target predicate-row identity drifted: " + ",".join(drifted or ["side"])
        )
    if int(row.exposure_fill_ordinal) <= 0 or int(row.fill_ts_ms) <= 0:
        raise ReplayEmitterError("C++ target predicate-row identity is incomplete")
    expected_support = {
        "policy_input_valid": bool(opportunity["policy_input_valid"]),
        "support_valid": bool(opportunity["feature::support_valid"]),
        "channel_support_valid": bool(opportunity["feature::channel_support_valid"]),
    }
    observed_support = {
        "policy_input_valid": bool(row.policy_input_valid),
        "support_valid": bool(row.support_valid),
        "channel_support_valid": bool(row.channel_support_valid),
    }
    if observed_support != expected_support:
        raise ReplayEmitterError("C++ target predicate-row support state drifted")
    values = list(row.predicate_values)
    if values and len(values) != expected_predicate_count:
        raise ReplayEmitterError("C++ target predicate-row width drifted")
    allowed = {
        cpp.F05TriState.UNOBSERVED,
        cpp.F05TriState.FALSE,
        cpp.F05TriState.TRUE,
    }
    if any(value not in allowed for value in values):
        raise ReplayEmitterError("C++ target predicate-row contains an invalid state")


@dataclass(frozen=True, slots=True)
class ReplayEmitterAudit:
    feature_block: str
    windows_consumed: int
    snapshots_emitted: int
    fallback_snapshots: int
    last_window_right_ts_ns: int | None
    last_feature_ready_ts_ns: int | None
    warmup_admitted: bool
    economic_outcomes_read: bool = False


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def _known_clock(ts_ns: int) -> dict[str, Any]:
    if int(ts_ns) <= 0:
        raise ReplayEmitterError("known replay clock must be positive")
    return {
        "ts_ns": int(ts_ns),
        "valid": True,
        "unknown": False,
        "reason": "valid",
    }


def _unknown_clock(reason: str) -> dict[str, Any]:
    if not str(reason).strip():
        raise ReplayEmitterError("unknown replay clock requires a reason")
    return {
        "ts_ns": None,
        "valid": False,
        "unknown": True,
        "reason": str(reason),
    }


class CooldownV2ReplayEmitter:
    """Advance one causal feature stream and freeze fill-visible snapshots."""

    def __init__(
        self,
        *,
        feature_block: str,
        observations: Iterator[CausalWindowObservation],
        warmup_cutoff_ts_ns: int,
        warmup_identity: str,
        identity_hashes: Mapping[str, str],
        source_cursor_prefixes: Mapping[str, str],
        snapshot_sink: Callable[[CooldownAssignmentSnapshotV2], None] | None = None,
        retain_snapshots: bool = False,
    ) -> None:
        if int(warmup_cutoff_ts_ns) <= 0:
            raise ReplayEmitterError("warmup cutoff must be positive")
        if not str(warmup_identity).strip():
            raise ReplayEmitterError("warmup identity is empty")
        if set(source_cursor_prefixes) != {"market", "depth", "trade"}:
            raise ReplayEmitterError("source cursor prefixes are incomplete")
        if any(not str(value).strip() for value in source_cursor_prefixes.values()):
            raise ReplayEmitterError("source cursor prefix is empty")
        self.feature_block = str(feature_block)
        self._observations = iter(observations)
        self._next_observation = next(self._observations, None)
        self._warmup_cutoff_ts_ns = int(warmup_cutoff_ts_ns)
        self._warmup_identity = str(warmup_identity)
        self._identity_hashes = dict(identity_hashes)
        self._source_cursor_prefixes = dict(source_cursor_prefixes)
        self._snapshot_sink = snapshot_sink
        self._retain_snapshots = bool(retain_snapshots)
        self._snapshots: list[CooldownAssignmentSnapshotV2] = []
        self._snapshots_emitted = 0
        self._fallback_snapshots = 0
        self._state = CausalMultichannelEmaState(block=self.feature_block)
        self._raw_warmup_admission: bool | None = None

    @property
    def snapshots(self) -> tuple[CooldownAssignmentSnapshotV2, ...]:
        return tuple(self._snapshots)

    def _observe_raw_warmup_admission(
        self, observation: CausalWindowObservation
    ) -> None:
        admitted = observation.warmup_admitted
        if type(admitted) is not bool:
            raise ReplayEmitterError("raw warmup admission must be bool")
        if int(observation.right_ts_ns) <= self._warmup_cutoff_ts_ns:
            if admitted:
                raise ReplayEmitterError(
                    "raw warmup admission appeared before the policy interval"
                )
            return
        if self._raw_warmup_admission is None:
            self._raw_warmup_admission = admitted
            if admitted:
                self._state.warmup_admitted = True
                self._state.warmup_identity = self._warmup_identity
            return
        if admitted != self._raw_warmup_admission:
            raise ReplayEmitterError(
                "raw warmup admission changed after the first target window"
            )

    def _advance_to(self, fill_visible_ts_ns: int) -> str | None:
        cutoff = int(fill_visible_ts_ns)
        while (
            self._next_observation is not None
            and int(self._next_observation.feature_ready_ts_ns) <= cutoff
        ):
            observation = self._next_observation
            self._state.update(observation)
            self._observe_raw_warmup_admission(observation)
            self._next_observation = next(self._observations, None)
        if self._state.last_right_ts_ns is None:
            raise ReplayEmitterError("fill arrived before the first completed window")
        age_ns = cutoff - int(self._state.last_right_ts_ns)
        if age_ns < 0:
            raise ReplayEmitterError("feature state crossed the fill-visible cutoff")
        if age_ns >= BASE_WINDOW_WIDTH_NS:
            self._state.mark_current_window_unobserved()
            return "completed_window_stream_stale_at_fill_visible_cutoff"
        return None

    def _source_bindings(
        self, fallback_reason: str | None = None
    ) -> dict[str, dict[str, Any]]:
        generation = self._state.last_market_generation
        feature_ready = self._state.last_feature_ready_ts_ns
        if generation is None or feature_ready is None:
            raise ReplayEmitterError("feature source identity is unavailable")
        bindings: dict[str, dict[str, Any]] = {}
        for name in ("market", "depth", "trade"):
            cursor = (
                f"{self._source_cursor_prefixes[name]}:"
                f"{int(feature_ready)}:{int(generation)}"
            )
            bindings[name] = {
                "generation": int(generation),
                "cursor": cursor,
                "feature_generation": int(generation),
                "feature_cursor": cursor,
                "valid": fallback_reason is None,
                "unknown": False,
                "reason": (
                    "valid_historical_exchange_time_source"
                    if fallback_reason is None
                    else str(fallback_reason)
                ),
            }
        return bindings

    def capture_exposure_fill(
        self,
        *,
        assignment_id: str,
        fill_event_id: str,
        client_order_id: str,
        lineage_id: str,
        lineage_revision: int,
        partial_fill_ordinal: int,
        partial_fill_qty_btc: float,
        fill_exchange_ts_ns: int,
        fill_visible_ts_ns: int,
        m0_context: Mapping[str, Any],
    ) -> CooldownAssignmentSnapshotV2:
        """Capture one historical exchange-time exploratory policy input."""

        if set(m0_context) != set(M0_REQUIRED_FIELDS):
            raise ReplayEmitterError("M0 replay emitter schema drifted")
        try:
            m0 = validate_m0_context(m0_context)
        except FeatureContractError as exc:
            raise ReplayEmitterError(f"invalid replay M0 context: {exc}") from exc
        source_fallback_reason = self._advance_to(int(fill_visible_ts_ns))
        try:
            feature_row = self._state.feature_row(
                side=str(m0["side"]),
                decision_ts_ns=int(fill_visible_ts_ns),
                m0_context=m0,
            )
        except FeatureContractError as exc:
            raise ReplayEmitterError(f"failed to freeze feature row: {exc}") from exc
        feature_ready = int(self._state.last_feature_ready_ts_ns or 0)
        snapshot_seed = {
            "identity": IDENTITY,
            "assignment_id": str(assignment_id),
            "fill_event_id": str(fill_event_id),
            "lineage_id": str(lineage_id),
            "lineage_revision": int(lineage_revision),
            "partial_fill_ordinal": int(partial_fill_ordinal),
            "feature_ready_ts_ns": feature_ready,
        }
        payload = {
            "snapshot_id": f"cooldown-v2-{_canonical_sha256(snapshot_seed)}",
            "assignment_id": str(assignment_id),
            "fill_event_id": str(fill_event_id),
            "client_order_id": str(client_order_id),
            "lineage_id": str(lineage_id),
            "lineage_revision": int(lineage_revision),
            "partial_fill_ordinal": int(partial_fill_ordinal),
            "partial_fill_qty_btc": float(partial_fill_qty_btc),
            "visibility_profile": HISTORICAL_EXCHANGE_EVENT_PROFILE,
            "clocks": {
                "assignment": _known_clock(int(m0["assignment_ts_ns"])),
                "fill_exchange": _known_clock(int(fill_exchange_ts_ns)),
                "fill_receive": _unknown_clock(
                    "historical_fill_receive_timestamp_unavailable"
                ),
                "fill_visible": _known_clock(int(fill_visible_ts_ns)),
                "feature_ready": _known_clock(feature_ready),
            },
            "sources": self._source_bindings(source_fallback_reason),
            "identity_hashes": dict(self._identity_hashes),
            "m0_context": m0,
            "feature_row": feature_row,
        }
        try:
            snapshot = capture_cooldown_assignment_snapshot(payload)
        except SnapshotContractError as exc:
            raise ReplayEmitterError(f"assignment snapshot failed closed: {exc}") from exc
        self._snapshots_emitted += 1
        self._fallback_snapshots += int(not snapshot.policy_input_valid)
        if self._snapshot_sink is not None:
            self._snapshot_sink(snapshot)
        if self._retain_snapshots:
            self._snapshots.append(snapshot)
        return snapshot

    def audit(self) -> ReplayEmitterAudit:
        return ReplayEmitterAudit(
            feature_block=self.feature_block,
            windows_consumed=int(self._state.window_count),
            snapshots_emitted=int(self._snapshots_emitted),
            fallback_snapshots=int(self._fallback_snapshots),
            last_window_right_ts_ns=self._state.last_right_ts_ns,
            last_feature_ready_ts_ns=self._state.last_feature_ready_ts_ns,
            warmup_admitted=bool(self._state.warmup_admitted),
            economic_outcomes_read=False,
        )


__all__ = [
    "CooldownV2ReplayEmitter",
    "REPLAY_EMITTER_SCHEMA_VERSION",
    "ReplayEmitterAudit",
    "ReplayEmitterError",
    "build_cpp_predicate_row",
    "validate_cpp_predicate_row",
]
