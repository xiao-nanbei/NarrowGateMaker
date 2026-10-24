"""Shared coverage vocabulary for Boolean cooldown research and telemetry."""

from __future__ import annotations

from enum import StrEnum


class BooleanCooldownCoverageReason(StrEnum):
    ELIGIBLE_FEATURE_READY = "eligible_feature_ready"
    INELIGIBLE_EVENT = "ineligible_event"
    WARMUP_INCOMPLETE = "warmup_incomplete"
    FEATURE_STALE = "feature_stale"
    PREDICATE_UNOBSERVED = "predicate_unobserved"
    SAFETY_FALLBACK = "safety_fallback"
    POLICY_CONTROL = "policy_control"
    SOURCE_UNAVAILABLE = "source_unavailable"
    CACHE_UNAVAILABLE = "cache_unavailable"
    BINDING_INVALID = "binding_invalid"
    LIFECYCLE_UNIDENTIFIED = "lifecycle_unidentified"
    GTX_PREACTIVATION_EXACT_ZERO = "gtx_preactivation_exact_zero"
    GTX_ACK_UNKNOWN_CENSORED = "gtx_ack_unknown_censored"


def classify_boolean_cooldown_coverage(
    *,
    eligible_event: bool,
    support_valid: bool,
    selected_action: str,
    fallback_reason: str | None,
    source_available: bool = True,
    cache_available: bool = True,
    lifecycle_identified: bool = True,
    safety_fallback: bool = False,
) -> BooleanCooldownCoverageReason:
    """Map all evaluators to one stable, non-economic coverage reason."""

    if not eligible_event:
        return BooleanCooldownCoverageReason.INELIGIBLE_EVENT
    if not lifecycle_identified:
        return BooleanCooldownCoverageReason.LIFECYCLE_UNIDENTIFIED
    if not source_available:
        return BooleanCooldownCoverageReason.SOURCE_UNAVAILABLE
    if not cache_available:
        return BooleanCooldownCoverageReason.CACHE_UNAVAILABLE
    if safety_fallback:
        return BooleanCooldownCoverageReason.SAFETY_FALLBACK
    reason = str(fallback_reason or "").strip().lower()
    if not support_valid:
        if "warmup" in reason:
            return BooleanCooldownCoverageReason.WARMUP_INCOMPLETE
        if "stale" in reason or "feature_age" in reason:
            return BooleanCooldownCoverageReason.FEATURE_STALE
        if "unobserved" in reason:
            return BooleanCooldownCoverageReason.PREDICATE_UNOBSERVED
        if any(token in reason for token in ("hash", "binding", "drift")):
            return BooleanCooldownCoverageReason.BINDING_INVALID
        return BooleanCooldownCoverageReason.SAFETY_FALLBACK
    if str(selected_action) == "CONTROL_85N":
        return BooleanCooldownCoverageReason.POLICY_CONTROL
    return BooleanCooldownCoverageReason.ELIGIBLE_FEATURE_READY


__all__ = [
    "BooleanCooldownCoverageReason",
    "classify_boolean_cooldown_coverage",
]
