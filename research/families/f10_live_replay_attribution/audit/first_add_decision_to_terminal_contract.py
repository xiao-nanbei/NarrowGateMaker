"""Contract for decision-visible first-add loss attribution.

This module validates evidence identity and native lifecycle rows only. It does
not fit a policy, estimate action uplift, or authorize an F09 experiment.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "first_add_decision_to_terminal_loss_diagnostic.v1"
TRACE_SCHEMA_VERSION = "first_add_decision_to_terminal_trace.v1"
IDENTITY = "first_add_decision_to_terminal_loss_diagnostic_v1"
PRIMARY_ESTIMAND = "decision_to_campaign_terminal_value_usdc"
ROOT = Path(__file__).resolve().parents[4]

REQUIRED_TRACE_COLUMNS = frozenset(
    {
        "trace_schema_version",
        "day",
        "quality_grade",
        "campaign_id",
        "decision_id",
        "decision_ts_ms",
        "order_id",
        "order_submit_ts_ms",
        "fill_ts_ms",
        "campaign_terminal_ts_ms",
        "side",
        "inventory_role",
        "exact_decision_order_fill_join",
        "decision_visible_feature_ready_ts_max_ms",
        "decision_equity_usdc",
        "campaign_terminal_equity_usdc",
        PRIMARY_ESTIMAND,
    }
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_spec_sha256(payload: Mapping[str, Any]) -> str:
    normalized = dict(payload)
    normalized.pop("canonical_spec_sha256", None)
    raw = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def validate_spec(payload: Mapping[str, Any]) -> None:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected first-add diagnostic schema")
    if payload.get("identity") != IDENTITY:
        raise ValueError("unexpected first-add diagnostic identity")
    frozen_hash = str(payload.get("canonical_spec_sha256", ""))
    if len(frozen_hash) != 64 or canonical_spec_sha256(payload) != frozen_hash:
        raise ValueError("first-add diagnostic spec hash mismatch")
    estimand = payload.get("estimand") or {}
    if estimand.get("primary") != PRIMARY_ESTIMAND:
        raise ValueError("first-add diagnostic changed its primary estimand")
    if estimand.get("unit") != "USDC_per_first_add_decision":
        raise ValueError("first-add diagnostic unit drifted")
    if not bool(estimand.get("observational_not_action_uplift", False)):
        raise ValueError("first-add diagnostic cannot claim action uplift")
    panels = payload.get("panels") or {}
    primary = tuple(panels.get("development_primary_grade_a_days") or ())
    sensitivity = tuple(panels.get("development_sensitivity_grade_b_days") or ())
    if len(primary) != 24 or len(sensitivity) != 16:
        raise ValueError("first-add diagnostic must preserve the 24A/16B split")
    if set(primary) & set(sensitivity):
        raise ValueError("first-add primary and sensitivity days overlap")
    if tuple(primary) != tuple(sorted(primary)) or tuple(sensitivity) != tuple(
        sorted(sensitivity)
    ):
        raise ValueError("first-add diagnostic days must be chronological")
    lifecycle = payload.get("native_lifecycle_contract") or {}
    if set(lifecycle.get("required_columns") or ()) != set(REQUIRED_TRACE_COLUMNS):
        raise ValueError("first-add native lifecycle columns drifted")
    if lifecycle.get("join") != "decision_id_to_order_id_to_fill_to_campaign":
        raise ValueError("first-add diagnostic requires an exact lifecycle join")
    if not bool(lifecycle.get("coverage_must_equal_one_or_fail", False)):
        raise ValueError("first-add diagnostic cannot filter unmatched rows")
    implementation = payload.get("implementation_identity") or {}
    for key, path in (
        ("contract_module_sha256", Path(__file__).resolve()),
        (
            "contract_test_sha256",
            ROOT / "tests" / "test_first_add_decision_to_terminal_contract.py",
        ),
    ):
        if not path.is_file() or sha256_file(path) != str(implementation.get(key, "")):
            raise ValueError(f"first-add diagnostic implementation drifted: {key}")
    permissions = payload.get("permissions") or {}
    if any(bool(value) for value in permissions.values()):
        raise ValueError("first-add diagnostic spec cannot grant authority")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    values = pd.to_numeric(frame[column], errors="coerce")
    if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
        raise ValueError(f"first-add trace has invalid {column}")
    return values


def validate_native_trace(
    frame: pd.DataFrame,
    spec: Mapping[str, Any],
) -> pd.DataFrame:
    validate_spec(spec)
    missing = sorted(REQUIRED_TRACE_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError("first-add native trace is missing: " + ", ".join(missing))
    if frame.empty:
        raise ValueError("first-add native trace is empty")
    if not frame["trace_schema_version"].eq(TRACE_SCHEMA_VERSION).all():
        raise ValueError("first-add native trace schema drifted")
    if frame.duplicated(["day", "campaign_id"]).any():
        raise ValueError("first-add native trace has multiple rows per campaign")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("first-add decision_id is not unique")
    if frame.assign(_order_id=frame["order_id"].astype(str)).duplicated(
        ["day", "_order_id"]
    ).any():
        raise ValueError("first-add day/order_id is not unique")
    if not frame["side"].astype(str).str.upper().isin(("BUY", "SELL")).all():
        raise ValueError("first-add side is invalid")
    if not frame["inventory_role"].astype(str).eq("add").all():
        raise ValueError("first-add diagnostic accepts add decisions only")
    if not _numeric(frame, "exact_decision_order_fill_join").eq(1).all():
        raise ValueError("first-add lifecycle join is not exact")

    decision_ts = _numeric(frame, "decision_ts_ms")
    submit_ts = _numeric(frame, "order_submit_ts_ms")
    fill_ts = _numeric(frame, "fill_ts_ms")
    terminal_ts = _numeric(frame, "campaign_terminal_ts_ms")
    feature_ready_ts = _numeric(frame, "decision_visible_feature_ready_ts_max_ms")
    if (
        (feature_ready_ts > decision_ts).any()
        or (submit_ts < decision_ts).any()
        or (fill_ts < submit_ts).any()
        or (terminal_ts < fill_ts).any()
    ):
        raise ValueError("first-add lifecycle or feature clock is non-causal")

    panels = spec["panels"]
    grade_by_day = {
        **{
            str(day): "A"
            for day in panels["development_primary_grade_a_days"]
        },
        **{
            str(day): "B"
            for day in panels["development_sensitivity_grade_b_days"]
        },
    }
    observed_days = set(frame["day"].astype(str))
    if not observed_days.issubset(grade_by_day):
        raise ValueError("first-add trace read a day outside frozen Development")
    expected_grades = frame["day"].astype(str).map(grade_by_day)
    if not frame["quality_grade"].astype(str).eq(expected_grades).all():
        raise ValueError("first-add trace quality grade drifted")

    decision_equity = _numeric(frame, "decision_equity_usdc")
    terminal_equity = _numeric(frame, "campaign_terminal_equity_usdc")
    logged_value = _numeric(frame, PRIMARY_ESTIMAND)
    expected_value = terminal_equity - decision_equity
    if not np.allclose(logged_value, expected_value, atol=1e-9, rtol=0.0):
        raise ValueError("first-add decision-to-terminal accounting drifted")
    return frame.copy()


def validate_quality_identity(spec: Mapping[str, Any]) -> pd.DataFrame:
    """Validate the external quality ledger before Development production."""

    validate_spec(spec)
    identity = spec.get("quality_identity") or {}
    path = Path(str(identity.get("path", ""))).expanduser()
    if not path.is_file() or sha256_file(path) != str(identity.get("sha256", "")):
        raise ValueError("first-add quality ledger identity drifted")
    quality = pd.read_csv(path, dtype={"day": str, "quality_grade": str})
    if quality["day"].duplicated().any():
        raise ValueError("first-add quality ledger contains duplicate days")
    by_day = quality.set_index("day")
    panels = spec["panels"]
    expected = {
        **{
            str(day): "A"
            for day in panels["development_primary_grade_a_days"]
        },
        **{
            str(day): "B"
            for day in panels["development_sensitivity_grade_b_days"]
        },
    }
    missing = sorted(set(expected) - set(by_day.index))
    if missing:
        raise ValueError("first-add quality ledger is missing: " + ", ".join(missing))
    for day, grade in expected.items():
        row = by_day.loc[day]
        if str(row["quality_grade"]) != grade:
            raise ValueError(f"first-add quality grade drifted for {day}")
        if not bool(row.get("native_sequence_eligible", False)) or not bool(
            row.get("normalized_formal_eligible", False)
        ):
            raise ValueError(f"first-add native market-data identity failed for {day}")
        if grade == "A" and not bool(
            row.get("formal_training_replay_eligible", False)
        ):
            raise ValueError(f"first-add Grade-A day is not formally eligible: {day}")
    return quality.loc[quality["day"].isin(expected)].copy()


def required_trace_columns() -> Sequence[str]:
    return tuple(sorted(REQUIRED_TRACE_COLUMNS))
