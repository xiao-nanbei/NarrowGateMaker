"""Shared outcome and trace contract for future lineage-randomized F09 actions.

The contract deliberately has no action semantics.  It fixes the assignment
time origin, accounting identities, randomization registration, and native
trace completeness required before a future action may read Development
outcomes.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "lineage_randomized_outcome_contract.v2"
TRACE_SCHEMA_VERSION = "narrowgate_lineage_randomized_trace.v2"
EVENT_JOURNAL_SCHEMA_VERSION = "narrowgate_lineage_event_journal.v2"
CONTRACT_ID = "lineage_randomized_outcome_contract_v2"

PRIMARY_ESTIMAND = "decision_to_campaign_terminal_value_usdc"
ALLOWED_PRE_ASSIGNMENT_COVARIATES = frozenset(
    {
        "pre_assignment_campaign_pnl_usdc",
        "assignment_inventory_btc",
        "campaign_age_at_assignment_ms",
    }
)
REQUIRED_RANDOMIZATION_STRATA = ("UTC_day", "side")

CANONICAL_EQUITY_COLUMNS = (
    "campaign_start_equity_usdc",
    "assignment_equity_usdc",
    "lineage_terminal_equity_usdc",
    "campaign_terminal_equity_usdc",
)

REQUIRED_NATIVE_TRACE_COLUMNS = frozenset(
    {
        "trace_schema_version",
        "mechanism_extension_schema",
        "lineage_uid",
        "lineage_id",
        "campaign_id",
        "day",
        "side",
        "action",
        "behavior_propensity",
        "randomization_stratum",
        "assignment_ts_ms",
        "assignment_before_downstream_path",
        "assignment_fixed_within_lineage",
        "assignment_inventory_btc",
        "campaign_age_at_assignment_ms",
        "final_blocker",
        "final_action_change_ts_ms",
        "final_action_change_status",
        "lineage_terminal_ts_ms",
        "campaign_terminal_ts_ms",
        "lineage_terminal_reason",
        "campaign_terminal_reason",
        "campaign_start_equity_usdc",
        "assignment_equity_usdc",
        "lineage_terminal_equity_usdc",
        "campaign_terminal_equity_usdc",
        "pre_assignment_campaign_pnl_usdc",
        "lineage_reward_usdc",
        "post_lineage_continuation_value_usdc",
        "decision_to_campaign_terminal_value_usdc",
        "accounting_identity_error_usdc",
    }
)

MECHANISM_EXTENSION_COLUMNS = {
    "none": frozenset(),
    "variance_time_v1": frozenset(
        {
            "assignment_consecutive_same_side_fill_units",
            "terminal_consecutive_same_side_fill_units",
            "variance_budget_bps2_at_assignment",
            "variance_budget_bps2_at_terminal",
            "variance_accumulated_qv_bps2_at_terminal",
            "wall_ready_ts_ms",
            "variance_ready_ts_ms",
            "variance_ready_status",
            "clock_direction",
        }
    ),
}

REQUIRED_EVENT_JOURNAL_COLUMNS = frozenset(
    {
        "event_schema_version",
        "lineage_uid",
        "lineage_id",
        "campaign_id",
        "day",
        "side",
        "action",
        "event_seq",
        "event_type",
        "event_ts_ms",
        "episode_index",
    }
)
REQUIRED_TERMINAL_EVENT_COUNTS = (
    "assignment",
    "lineage_terminal",
    "campaign_terminal",
)
REQUIRED_PRODUCER_DENOMINATORS = (
    "assigned",
    "lineage_finalized",
    "campaign_terminalized",
    "emitted",
    "producer_validated",
    "unique_lineage_uid",
    "assignment_events",
    "lineage_terminal_events",
    "campaign_terminal_events",
)

ROOT = Path(__file__).resolve().parents[4]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_contract_sha256(payload: Mapping[str, Any]) -> str:
    """Hash a contract while excluding only its self-identity field."""

    normalized = dict(payload)
    normalized.pop("canonical_contract_sha256", None)
    raw = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def validate_foundation_contract(payload: Mapping[str, Any]) -> None:
    """Fail closed when the shared foundation contract changes semantics."""

    if str(payload.get("schema_version", "")) != SCHEMA_VERSION:
        raise ValueError("unexpected lineage randomized outcome contract schema")
    if str(payload.get("contract_id", "")) != CONTRACT_ID:
        raise ValueError("unexpected lineage randomized outcome contract id")
    frozen_hash = str(payload.get("canonical_contract_sha256", ""))
    if len(frozen_hash) != 64 or canonical_contract_sha256(payload) != frozen_hash:
        raise ValueError("lineage randomized outcome contract hash mismatch")
    implementation = payload.get("implementation_identity") or {}
    identities = {
        "contract_module_sha256": Path(__file__).resolve(),
        "contract_test_sha256": (
            ROOT / "tests" / "test_lineage_randomized_outcome_contract_v2.py"
        ),
    }
    for key, path in identities.items():
        if not path.is_file() or _sha256_file(path) != str(implementation.get(key, "")):
            raise ValueError(f"lineage outcome contract implementation drifted: {key}")

    outcome = payload.get("outcome_contract") or {}
    if outcome.get("primary_estimand") != PRIMARY_ESTIMAND:
        raise ValueError("primary estimand must start at lineage assignment")
    if outcome.get("unit") != "USDC_per_lineage_assignment":
        raise ValueError("primary outcome unit must be USDC per lineage assignment")
    if not bool(outcome.get("primary_estimand_frozen_before_outcome_read", False)):
        raise ValueError("primary estimand must be frozen before outcomes")
    pre_assignment = outcome.get("pre_assignment_campaign_pnl") or {}
    if bool(pre_assignment.get("outcome_eligible", True)):
        raise ValueError("pre-assignment campaign PnL cannot be an action outcome")
    allowed_uses = set(pre_assignment.get("allowed_uses") or ())
    if allowed_uses != {"covariate", "balance_diagnostic"}:
        raise ValueError("pre-assignment PnL uses must be covariate and balance only")

    randomization = payload.get("randomization_contract") or {}
    if tuple(randomization.get("stratification_keys") or ()) != REQUIRED_RANDOMIZATION_STRATA:
        raise ValueError("randomization must be stratified by UTC day and side")
    if randomization.get("design") != "independent_prf_bernoulli_0.5":
        raise ValueError("randomization must use independent PRF Bernoulli draws")
    probabilities = randomization.get("probabilities") or {}
    if len(probabilities) != 2 or any(
        not math.isclose(float(value), 0.5, abs_tol=1e-12)
        for value in probabilities.values()
    ):
        raise ValueError("future lineage actions require exact 0.5/0.5 propensity")

    registration = payload.get("future_action_registration") or {}
    required = set(registration.get("required_fields") or ())
    if not {
        "action_family_id",
        "primary_estimand",
        "covariate_adjustment",
        "randomization_seed",
        "action_semantics",
        "producer_identity",
        "trace_schema_version",
        "mechanism_extension_schema",
    }.issubset(required):
        raise ValueError("future action registration requirements are incomplete")
    sequential = payload.get("sequential_treatment_contract") or {}
    if (
        sequential.get("downstream_assignment_policy")
        != "persist_to_campaign_terminal_no_rerandomization"
        or int(sequential.get("maximum_assignments_per_campaign", 0)) != 1
        or bool(sequential.get("sequential_regime_estimators_supported", True))
    ):
        raise ValueError("campaign-terminal outcome has an overlapping treatment")

    trace = payload.get("native_trace_contract") or {}
    if trace.get("schema_version") != TRACE_SCHEMA_VERSION:
        raise ValueError("native lineage trace schema mismatch")
    if not bool(trace.get("coverage_must_equal_one_or_fail", False)):
        raise ValueError("native lineage trace must require complete coverage")
    if set(trace.get("core_required_columns") or ()) != set(REQUIRED_NATIVE_TRACE_COLUMNS):
        raise ValueError("native lineage trace required-column contract drifted")
    if trace.get("event_journal_schema_version") != EVENT_JOURNAL_SCHEMA_VERSION:
        raise ValueError("native lineage event-journal schema drifted")
    if set(trace.get("event_journal_required_columns") or ()) != set(
        REQUIRED_EVENT_JOURNAL_COLUMNS
    ):
        raise ValueError("native lineage event-journal columns drifted")
    if not bool(trace.get("producer_denominator_audit_required", False)):
        raise ValueError("native lineage producer denominator audit is required")
    frozen_extensions = {
        str(name): frozenset(columns)
        for name, columns in (trace.get("mechanism_extensions") or {}).items()
    }
    if frozen_extensions != MECHANISM_EXTENSION_COLUMNS:
        raise ValueError("native lineage mechanism-extension contract drifted")

    permissions = payload.get("permissions") or {}
    forbidden = [name for name, value in permissions.items() if bool(value)]
    if forbidden:
        raise ValueError(
            "foundation contract cannot grant research authority: "
            + ", ".join(sorted(forbidden))
        )


def validate_action_registration(
    registration: Mapping[str, Any],
    foundation: Mapping[str, Any],
) -> None:
    """Validate a future action's preregistration before any path is generated."""

    validate_foundation_contract(foundation)
    if registration.get("foundation_contract_id") != CONTRACT_ID:
        raise ValueError("action registration does not bind contract v2")
    if not str(registration.get("action_family_id", "")).strip():
        raise ValueError("action registration lacks a stable family id")
    try:
        int(registration["randomization_seed"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("action registration lacks a frozen randomization seed") from exc
    action_semantics = registration.get("action_semantics")
    if not isinstance(action_semantics, Mapping) or not action_semantics:
        raise ValueError("action registration lacks explicit action semantics")
    if registration.get("primary_estimand") != PRIMARY_ESTIMAND:
        raise ValueError("action registration changed the primary estimand")
    if tuple(registration.get("stratification_keys") or ()) != REQUIRED_RANDOMIZATION_STRATA:
        raise ValueError("action registration changed randomization strata")
    if not bool(registration.get("assignment_before_downstream_path", False)):
        raise ValueError("assignment must precede downstream path generation")
    if (
        registration.get("downstream_assignment_policy")
        != "persist_to_campaign_terminal_no_rerandomization"
        or int(registration.get("maximum_assignments_per_campaign", 0)) != 1
    ):
        raise ValueError("future action permits overlapping campaign treatments")
    if registration.get("trace_schema_version") != TRACE_SCHEMA_VERSION:
        raise ValueError("action registration changed native trace schema")
    extension_name = str(registration.get("mechanism_extension_schema", ""))
    if extension_name not in MECHANISM_EXTENSION_COLUMNS:
        raise ValueError("action registration has an unknown mechanism extension")
    producer_identity = registration.get("producer_identity")
    if not isinstance(producer_identity, Mapping) or not producer_identity:
        raise ValueError("action registration lacks producer artifact identity")
    for name, digest in producer_identity.items():
        if not str(name).strip() or len(str(digest)) != 64:
            raise ValueError("action producer identity must contain SHA256 hashes")
        try:
            int(str(digest), 16)
        except ValueError as exc:
            raise ValueError(
                "action producer identity must contain SHA256 hashes"
            ) from exc
        path = Path(str(name)).expanduser()
        if not path.is_absolute():
            path = ROOT / path
        if not path.is_file() or _sha256_file(path) != str(digest):
            raise ValueError(f"action producer artifact drifted: {name}")

    adjustment = registration.get("covariate_adjustment")
    if not isinstance(adjustment, Mapping) or "enabled" not in adjustment:
        raise ValueError("covariate adjustment must be explicitly enabled or disabled")
    enabled = bool(adjustment["enabled"])
    mode = str(adjustment.get("mode", ""))
    covariates = tuple(adjustment.get("covariates") or ())
    if any(name not in ALLOWED_PRE_ASSIGNMENT_COVARIATES for name in covariates):
        raise ValueError("covariate adjustment contains a post-assignment field")
    if enabled:
        expected = {
            "mode": "lin_v1",
            "primary_or_sensitivity": "primary",
            "formula": (
                "Y_post ~ action + day_side_FE + centered_X + "
                "action:centered_X"
            ),
            "missing_policy": "fail",
            "variance": "UTC_day_cluster_robust",
        }
        if not covariates or any(adjustment.get(key) != value for key, value in expected.items()):
            raise ValueError("enabled covariate adjustment is not frozen lin_v1")
    elif covariates or mode != "none":
        raise ValueError("disabled covariate adjustment must freeze mode=none")
    if not bool(adjustment.get("frozen_before_outcome_read", False)):
        raise ValueError("covariate adjustment must be frozen before outcomes")


def derive_post_assignment_outcomes(
    frame: pd.DataFrame,
    *,
    tolerance_usdc: float = 1e-9,
) -> pd.DataFrame:
    """Derive the canonical post-assignment outcomes and enforce accounting."""

    missing = sorted(set(CANONICAL_EQUITY_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError("lineage equity panel is missing: " + ", ".join(missing))
    out = frame.copy()
    for column in CANONICAL_EQUITY_COLUMNS:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    values = out[list(CANONICAL_EQUITY_COLUMNS)].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("lineage equity panel contains non-finite values")

    out["pre_assignment_campaign_pnl_usdc"] = (
        out["assignment_equity_usdc"] - out["campaign_start_equity_usdc"]
    )
    out["lineage_reward_usdc"] = (
        out["lineage_terminal_equity_usdc"] - out["assignment_equity_usdc"]
    )
    out["post_lineage_continuation_value_usdc"] = (
        out["campaign_terminal_equity_usdc"]
        - out["lineage_terminal_equity_usdc"]
    )
    out[PRIMARY_ESTIMAND] = (
        out["campaign_terminal_equity_usdc"] - out["assignment_equity_usdc"]
    )
    out["accounting_identity_error_usdc"] = (
        out[PRIMARY_ESTIMAND]
        - out["lineage_reward_usdc"]
        - out["post_lineage_continuation_value_usdc"]
    )
    max_error = float(out["accounting_identity_error_usdc"].abs().max())
    if max_error > float(tolerance_usdc):
        raise ValueError(
            "post-assignment accounting identity failed: "
            f"max_abs_error={max_error:.12g}"
        )
    return out


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError(f"native lineage trace has invalid {column}")
    return values


def validate_native_lineage_trace(
    frame: pd.DataFrame,
    foundation: Mapping[str, Any],
    *,
    event_journal: pd.DataFrame | None = None,
    producer_audit: Mapping[str, Any] | None = None,
    tolerance_usdc: float = 1e-9,
) -> pd.DataFrame:
    """Validate complete native trace output for a future F09 action identity."""

    validate_foundation_contract(foundation)
    missing = sorted(REQUIRED_NATIVE_TRACE_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError("native lineage trace is missing: " + ", ".join(missing))
    if frame.empty:
        raise ValueError("native lineage trace is empty")
    if not frame["trace_schema_version"].eq(TRACE_SCHEMA_VERSION).all():
        raise ValueError("native lineage trace mixes schema identities")
    if frame["lineage_uid"].isna().any() or frame["lineage_uid"].astype(str).eq("").any():
        raise ValueError("native lineage trace lacks a stable lineage_uid")
    if frame["lineage_uid"].duplicated().any():
        raise ValueError("native lineage trace lineage_uid is not unique")
    if frame.duplicated(["day", "campaign_id"]).any():
        raise ValueError("campaign-terminal outcome has more than one assignment")
    if not frame["side"].isin(("BUY", "SELL")).all():
        raise ValueError("native lineage trace side is invalid")
    expected_stratum = frame["day"].astype(str) + "|" + frame["side"].astype(str)
    if not frame["randomization_stratum"].astype(str).eq(expected_stratum).all():
        raise ValueError("native lineage trace randomization stratum drifted")
    propensity = _numeric(frame, "behavior_propensity")
    if not np.allclose(propensity, 0.5, atol=1e-12):
        raise ValueError("native lineage trace propensity differs from 0.5")
    if not _numeric(frame, "assignment_before_downstream_path").eq(1).all():
        raise ValueError("lineage assignment occurred after downstream path")
    if not _numeric(frame, "assignment_fixed_within_lineage").eq(1).all():
        raise ValueError("lineage assignment changed before terminal")

    _numeric(frame, "assignment_inventory_btc")
    for column in (
        "campaign_age_at_assignment_ms",
        "assignment_ts_ms",
        "lineage_terminal_ts_ms",
        "campaign_terminal_ts_ms",
    ):
        values = _numeric(frame, column)
        if (values < 0.0).any():
            raise ValueError(f"native lineage trace has negative {column}")
    assignment_ts = _numeric(frame, "assignment_ts_ms")
    lineage_terminal_ts = _numeric(frame, "lineage_terminal_ts_ms")
    campaign_terminal_ts = _numeric(frame, "campaign_terminal_ts_ms")
    if (
        (lineage_terminal_ts < assignment_ts).any()
        or (campaign_terminal_ts < lineage_terminal_ts).any()
    ):
        raise ValueError("native lineage terminal timestamps are out of order")

    extension_names = set(frame["mechanism_extension_schema"].astype(str))
    if len(extension_names) != 1:
        raise ValueError("native lineage trace mixes mechanism extensions")
    extension_name = next(iter(extension_names))
    if extension_name not in MECHANISM_EXTENSION_COLUMNS:
        raise ValueError("native lineage trace has an unknown mechanism extension")
    extension_missing = sorted(
        MECHANISM_EXTENSION_COLUMNS[extension_name] - set(frame.columns)
    )
    if extension_missing:
        raise ValueError(
            "native lineage trace mechanism extension is missing: "
            + ", ".join(extension_missing)
        )
    if extension_name == "variance_time_v1":
        for column in MECHANISM_EXTENSION_COLUMNS[extension_name] - {
            "variance_ready_status",
            "clock_direction",
        }:
            values = _numeric(frame, column)
            if (values < 0.0).any():
                raise ValueError(f"native lineage trace has negative {column}")
        ready_status = frame["variance_ready_status"].astype(str)
        if not ready_status.isin(("observed", "censored_before_ready")).all():
            raise ValueError("variance ready status is incomplete")
        ready_ts = pd.to_numeric(frame["variance_ready_ts_ms"], errors="coerce")
        observed = ready_status.eq("observed")
        if ready_ts[observed].isna().any() or (ready_ts[observed] <= 0).any():
            raise ValueError("observed variance ready rows lack a timestamp")
        if ready_ts[~observed].notna().any() and (ready_ts[~observed] != 0).any():
            raise ValueError("censored variance ready rows carry a future timestamp")
        direction = frame["clock_direction"].astype(str)
        if not direction.isin(("earlier", "later", "equal", "censored_unknown")).all():
            raise ValueError("clock direction is incomplete")
    if frame["final_blocker"].isna().any() or frame["final_blocker"].astype(str).eq("").any():
        raise ValueError("native lineage trace final blocker is missing")
    change_status = frame["final_action_change_status"].astype(str)
    if not change_status.isin(
        (
            "observed",
            "eligible_no_change",
            "no_eligible_decision",
            "censored_before_resolution",
        )
    ).all():
        raise ValueError("final action-change status is incomplete")
    change_ts = pd.to_numeric(frame["final_action_change_ts_ms"], errors="coerce")
    changed = change_status.eq("observed")
    if change_ts[changed].isna().any() or (change_ts[changed] <= 0).any():
        raise ValueError("observed action changes lack a timestamp")
    if change_ts[~changed].notna().any() and (change_ts[~changed] != 0).any():
        raise ValueError("no-change lineages carry an action-change timestamp")

    for column in (
        "lineage_terminal_reason",
        "campaign_terminal_reason",
    ):
        if frame[column].isna().any() or frame[column].astype(str).eq("").any():
            raise ValueError(f"native lineage trace has missing {column}")

    derived = derive_post_assignment_outcomes(
        frame,
        tolerance_usdc=tolerance_usdc,
    )
    for column in (
        "pre_assignment_campaign_pnl_usdc",
        "lineage_reward_usdc",
        "post_lineage_continuation_value_usdc",
        PRIMARY_ESTIMAND,
        "accounting_identity_error_usdc",
    ):
        logged = _numeric(frame, column)
        if not np.allclose(
            logged,
            derived[column],
            atol=float(tolerance_usdc),
            rtol=0.0,
        ):
            raise ValueError(f"native lineage trace accounting drifted for {column}")
    _validate_event_journal(frame, event_journal)
    _validate_producer_audit(frame, event_journal, producer_audit)
    return derived


def _validate_event_journal(
    frame: pd.DataFrame,
    event_journal: pd.DataFrame | None,
) -> None:
    if event_journal is None or event_journal.empty:
        raise ValueError("native lineage event journal is missing")
    missing = sorted(REQUIRED_EVENT_JOURNAL_COLUMNS - set(event_journal.columns))
    if missing:
        raise ValueError("native lineage event journal is missing: " + ", ".join(missing))
    if not event_journal["event_schema_version"].eq(
        EVENT_JOURNAL_SCHEMA_VERSION
    ).all():
        raise ValueError("native lineage event journal mixes schema identities")
    trace_uids = set(frame["lineage_uid"].astype(str))
    event_uids = set(event_journal["lineage_uid"].astype(str))
    if trace_uids != event_uids:
        raise ValueError("native lineage event journal coverage differs from trace")
    event_ts = pd.to_numeric(event_journal["event_ts_ms"], errors="coerce")
    event_seq = pd.to_numeric(event_journal["event_seq"], errors="coerce")
    episode_index = pd.to_numeric(event_journal["episode_index"], errors="coerce")
    if (
        event_ts.isna().any()
        or event_seq.isna().any()
        or episode_index.isna().any()
        or (event_ts < 0).any()
        or (event_seq < 0).any()
        or (episode_index < 0).any()
    ):
        raise ValueError("native lineage event journal has invalid clocks or sequence")
    for lineage_uid, rows in event_journal.groupby("lineage_uid", sort=False):
        ordered = rows.sort_values("event_seq", kind="stable")
        observed_seq = ordered["event_seq"].astype(int).to_numpy()
        if not np.array_equal(observed_seq, np.arange(len(ordered), dtype=int)):
            raise ValueError(f"lineage event sequence is not contiguous: {lineage_uid}")
        observed_ts = ordered["event_ts_ms"].astype(np.int64).to_numpy()
        if np.any(np.diff(observed_ts) < 0):
            raise ValueError(f"lineage event time reverses: {lineage_uid}")
        counts = ordered["event_type"].astype(str).value_counts()
        for event_type in REQUIRED_TERMINAL_EVENT_COUNTS:
            if int(counts.get(event_type, 0)) != 1:
                raise ValueError(
                    f"lineage journal requires one {event_type}: {lineage_uid}"
                )
        if ordered.iloc[0]["event_type"] != "assignment":
            raise ValueError(f"lineage journal must start with assignment: {lineage_uid}")
        if ordered.iloc[-1]["event_type"] != "campaign_terminal":
            raise ValueError(
                f"lineage journal must end with campaign terminal: {lineage_uid}"
            )


def _validate_producer_audit(
    frame: pd.DataFrame,
    event_journal: pd.DataFrame | None,
    producer_audit: Mapping[str, Any] | None,
) -> None:
    if not isinstance(producer_audit, Mapping):
        raise ValueError("native lineage producer audit is missing")
    if producer_audit.get("trace_schema_version") != TRACE_SCHEMA_VERSION:
        raise ValueError("native lineage producer audit trace schema drifted")
    if producer_audit.get("event_schema_version") != EVENT_JOURNAL_SCHEMA_VERSION:
        raise ValueError("native lineage producer audit event schema drifted")
    if not bool(producer_audit.get("coverage_complete", False)):
        raise ValueError("native lineage producer reported incomplete coverage")
    if int(producer_audit.get("open_lineage_count", -1)) != 0:
        raise ValueError("native lineage producer returned open lineages")
    if int(producer_audit.get("pending_campaign_record_count", -1)) != 0:
        raise ValueError("native lineage producer returned pending campaign records")
    denominators = producer_audit.get("denominator_counts") or {}
    if set(denominators) != set(REQUIRED_PRODUCER_DENOMINATORS):
        raise ValueError("native lineage producer denominators are incomplete")
    expected_rows = int(len(frame))
    expected_events = {
        "assignment_events": expected_rows,
        "lineage_terminal_events": expected_rows,
        "campaign_terminal_events": expected_rows,
    }
    observed_event_counts = (
        event_journal["event_type"].astype(str).value_counts()
        if event_journal is not None
        else pd.Series(dtype=int)
    )
    for key in REQUIRED_PRODUCER_DENOMINATORS:
        value = int(denominators.get(key, -1))
        expected = expected_events.get(key, expected_rows)
        if value != expected:
            raise ValueError(
                f"native lineage producer denominator drifted for {key}: "
                f"observed={value} expected={expected}"
            )
    for key, event_type in (
        ("assignment_events", "assignment"),
        ("lineage_terminal_events", "lineage_terminal"),
        ("campaign_terminal_events", "campaign_terminal"),
    ):
        if int(observed_event_counts.get(event_type, 0)) != int(denominators[key]):
            raise ValueError(f"native lineage event count drifted for {key}")


def required_native_trace_columns() -> Sequence[str]:
    return tuple(sorted(REQUIRED_NATIVE_TRACE_COLUMNS))
