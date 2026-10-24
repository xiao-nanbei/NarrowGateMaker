"""Structural checks shared by future paired placement-surface identities."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PAIRED_ACTIONS = ("closer_1tick", "current", "farther_1tick")
CLOCK_CONTRACT_SCHEMA = "paired_prediction_clock_contract.v1"


@dataclass(frozen=True)
class PredictionClockContract:
    """Spec-bound identity for one permissible ex-ante prediction clock."""

    source_id: str
    clock_column: str
    unit: str
    causal_cut: str
    source_identity_sha256: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_prediction_clock_source_identity(
    clock_contract: PredictionClockContract,
    artifact_path: Path,
) -> dict[str, Any]:
    """Verify the selected clock producer against its actual artifact bytes."""

    if not isinstance(clock_contract, PredictionClockContract):
        raise TypeError("clock_contract must be parsed from the frozen family Spec")
    resolved = artifact_path.expanduser().resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"prediction clock producer is missing: {resolved}")
    actual = _sha256_file(resolved)
    if actual != clock_contract.source_identity_sha256:
        raise RuntimeError(
            "prediction clock producer identity mismatch: "
            f"expected={clock_contract.source_identity_sha256} actual={actual} "
            f"path={resolved}"
        )
    return {
        "path": str(resolved),
        "size_bytes": int(resolved.stat().st_size),
        "sha256": actual,
        "source_id": clock_contract.source_id,
    }


def prediction_clock_contract_from_spec(
    spec: Mapping[str, Any],
) -> PredictionClockContract:
    """Parse and validate the selected clock from a frozen family Spec."""

    raw = spec.get("prediction_clock_contract")
    if not isinstance(raw, Mapping):
        raise ValueError("Spec is missing prediction_clock_contract")
    if raw.get("schema_version") != CLOCK_CONTRACT_SCHEMA:
        raise ValueError("unsupported prediction clock contract schema")
    source_id = str(raw.get("selected_source_id", "")).strip()
    sources = raw.get("allowed_sources")
    if not source_id or not isinstance(sources, Mapping) or source_id not in sources:
        raise ValueError("selected prediction clock source is not Spec-allowed")
    source = sources[source_id]
    if not isinstance(source, Mapping):
        raise ValueError("prediction clock source contract must be an object")
    required_true = ("ex_ante", "cohort_common")
    if any(source.get(field) is not True for field in required_true):
        raise ValueError("prediction clock source must be ex-ante and cohort-common")
    if source.get("outcome_dependent") is not False:
        raise ValueError("prediction clock source must forbid outcome dependence")
    clock_column = str(source.get("clock_column", "")).strip()
    unit = str(source.get("unit", "")).strip()
    causal_cut = str(source.get("causal_cut", "")).strip()
    source_hash = str(source.get("source_identity_sha256", "")).strip().lower()
    if not clock_column or not unit or not causal_cut:
        raise ValueError("prediction clock source identity is incomplete")
    if len(source_hash) != 64 or any(char not in "0123456789abcdef" for char in source_hash):
        raise ValueError("prediction clock source identity requires a SHA256")
    return PredictionClockContract(
        source_id=source_id,
        clock_column=clock_column,
        unit=unit,
        causal_cut=causal_cut,
        source_identity_sha256=source_hash,
    )


def common_clock_diagnostics(
    frame: pd.DataFrame,
    *,
    clock_column: str,
    group_columns: Sequence[str] = ("cohort_id",),
    actions: Sequence[str] = PAIRED_ACTIONS,
    tolerance_ms: float = 0.0,
) -> dict[str, Any]:
    """Measure whether paired actions are evaluated on one ex-ante clock."""

    required = {*group_columns, "action", clock_column}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"paired clock frame is missing columns: {missing}")
    selected = frame.loc[frame["action"].astype(str).isin(actions)].copy()
    duplicate = selected.duplicated([*group_columns, "action"], keep=False)
    if duplicate.any():
        raise ValueError("paired clock frame has duplicate group/action rows")
    pivot = selected.pivot(
        index=list(group_columns), columns="action", values=clock_column
    )
    missing_actions = [action for action in actions if action not in pivot.columns]
    if missing_actions:
        raise ValueError(f"paired clock frame is missing actions: {missing_actions}")
    complete = pivot.dropna(subset=list(actions))
    if complete.empty:
        raise ValueError("paired clock frame has no complete action cohorts")
    values = complete.loc[:, list(actions)].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("paired clock values must be finite")
    spread = values.max(axis=1) - values.min(axis=1)
    violating = spread > float(tolerance_ms)
    return {
        "complete_groups": int(len(complete)),
        "violating_groups": int(violating.sum()),
        "all_common": bool(not violating.any()),
        "tolerance_ms": float(tolerance_ms),
        "spread_ms": {
            "minimum": float(np.min(spread)),
            "p10": float(np.quantile(spread, 0.1)),
            "median": float(np.median(spread)),
            "p90": float(np.quantile(spread, 0.9)),
            "maximum": float(np.max(spread)),
        },
    }


def _paired_action_completeness(
    frame: pd.DataFrame,
    *,
    group_columns: Sequence[str],
    actions: Sequence[str],
) -> tuple[int, int]:
    required = {*group_columns, "action"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"paired clock frame is missing columns: {missing}")
    expected = frozenset(str(action) for action in actions)
    selected = frame.loc[frame["action"].astype(str).isin(expected)]
    observed = selected.groupby(list(group_columns), observed=True)["action"].agg(
        lambda values: frozenset(str(value) for value in values)
    )
    if observed.empty:
        raise ValueError("paired clock frame has no action cohorts")
    incomplete = int(sum(actions_seen != expected for actions_seen in observed))
    return int(len(observed)), incomplete


def assert_common_prediction_clock(
    frame: pd.DataFrame,
    *,
    clock_contract: PredictionClockContract,
    group_columns: Sequence[str] = ("cohort_id",),
    actions: Sequence[str] = PAIRED_ACTIONS,
    tolerance_ms: float = 0.0,
) -> dict[str, Any]:
    """Fail fast when an action-realized clock enters a paired invariant."""

    if not isinstance(clock_contract, PredictionClockContract):
        raise TypeError("clock_contract must be parsed from the frozen family Spec")
    total_groups, incomplete_groups = _paired_action_completeness(
        frame,
        group_columns=group_columns,
        actions=actions,
    )
    if incomplete_groups:
        raise RuntimeError(
            "paired action surface requires every cohort to contain every "
            f"Spec action; {incomplete_groups}/{total_groups} groups are incomplete"
        )
    diagnostics = common_clock_diagnostics(
        frame,
        clock_column=clock_contract.clock_column,
        group_columns=group_columns,
        actions=actions,
        tolerance_ms=tolerance_ms,
    )
    diagnostics["clock_source"] = {
        "source_id": clock_contract.source_id,
        "clock_column": clock_contract.clock_column,
        "unit": clock_contract.unit,
        "causal_cut": clock_contract.causal_cut,
        "source_identity_sha256": clock_contract.source_identity_sha256,
    }
    if not diagnostics["all_common"]:
        raise RuntimeError(
            "paired action surface requires a cohort-common ex-ante prediction "
            f"clock; {diagnostics['violating_groups']}/"
            f"{diagnostics['complete_groups']} groups differ"
        )
    return diagnostics
