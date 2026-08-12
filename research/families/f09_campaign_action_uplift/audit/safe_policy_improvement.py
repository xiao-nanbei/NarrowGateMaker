#!/usr/bin/env python3
"""SPIBB-style baseline fallback for queue-value actions.

This module does not manufacture support with a regression model.  Candidate
actions are retained only in state buckets with logged action support,
effective sample size, and a day-clustered uplift lower bound above the frozen
threshold.  Every other row executes the baseline action.

The M0/M1 gate is deliberately asymmetric: an external-market artifact cannot
advance unless the local-only M0 family already passes.  M1 must then show
incremental value and survive each registered leave-one-venue-out result.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SCHEMA_VERSION = "spibb_baseline_fallback.v1"
M0_M1_GATE_SCHEMA_VERSION = "local_m0_external_m1_gate.v1"


@dataclass(frozen=True)
class SpiBBConfig:
    baseline_action: str = "keep"
    candidate_action: str = "cancel_until_state_exit"
    state_columns: tuple[str, ...] = (
        "side",
        "queue_state_key",
        "microprice_state_key",
    )
    day_column: str = "day"
    action_column: str = "action"
    propensity_column: str = "behavior_propensity"
    minimum_candidate_rows: int = 100
    minimum_baseline_rows: int = 100
    minimum_effective_sample_size: float = 100.0
    minimum_uplift_lower_bound: float = 0.0
    bootstrap_trials: int = 500
    random_seed: int = 20260718


@dataclass(frozen=True)
class SpiBBStateDecision:
    state_id: str
    candidate_rows: int
    baseline_rows: int
    effective_sample_size: float
    uplift_mean: float
    uplift_p025: float
    accepted: bool
    reason: str


@dataclass(frozen=True)
class SpiBBPolicyArtifact:
    schema_version: str
    policy_id: str
    evidence_sha256: str
    input_scope: str
    config: SpiBBConfig
    accepted_state_ids: tuple[str, ...]
    state_decisions: tuple[SpiBBStateDecision, ...]
    training_days: tuple[str, ...]

    def action_for_state(self, state_id: str) -> tuple[str, str]:
        if str(state_id) in set(self.accepted_state_ids):
            return self.config.candidate_action, "supported_positive_lcb"
        return self.config.baseline_action, "spibb_baseline_fallback"

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "evidence_sha256": self.evidence_sha256,
            "input_scope": self.input_scope,
            "config": asdict(self.config),
            "accepted_state_ids": list(self.accepted_state_ids),
            "state_decisions": [
                asdict(decision) for decision in self.state_decisions
            ],
            "training_days": list(self.training_days),
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(self.to_payload(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: Path) -> SpiBBPolicyArtifact:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported SPIBB artifact schema")
        raw_config = dict(payload["config"])
        raw_config["state_columns"] = tuple(raw_config["state_columns"])
        return cls(
            schema_version=str(payload["schema_version"]),
            policy_id=str(payload["policy_id"]),
            evidence_sha256=str(payload.get("evidence_sha256", "")),
            input_scope=str(payload["input_scope"]),
            config=SpiBBConfig(**raw_config),
            accepted_state_ids=tuple(payload["accepted_state_ids"]),
            state_decisions=tuple(
                SpiBBStateDecision(**value)
                for value in payload["state_decisions"]
            ),
            training_days=tuple(str(day) for day in payload["training_days"]),
        )


def _state_id(frame: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"SPIBB state columns missing: {missing}")
    return frame[list(columns)].astype(str).agg("|".join, axis=1)


def _uplift_column(frame: pd.DataFrame) -> pd.Series:
    if "ope_dr_uplift" in frame:
        return pd.to_numeric(frame["ope_dr_uplift"], errors="coerce")
    if {"ope_dr_value", "_ope_reward"} <= set(frame.columns):
        return (
            pd.to_numeric(frame["ope_dr_value"], errors="coerce")
            - pd.to_numeric(frame["_ope_reward"], errors="coerce")
        )
    if "dr_pseudo_outcome" in frame:
        return pd.to_numeric(frame["dr_pseudo_outcome"], errors="coerce")
    raise ValueError(
        "SPIBB requires ope_dr_uplift, ope_dr_value/_ope_reward, or "
        "dr_pseudo_outcome"
    )


def _effective_sample_size(weights: np.ndarray) -> float:
    finite = weights[np.isfinite(weights) & (weights > 0.0)]
    denominator = float(np.square(finite).sum())
    return (
        float(finite.sum() ** 2 / denominator)
        if denominator > 0.0
        else 0.0
    )


def _cluster_lower_bound(
    frame: pd.DataFrame,
    *,
    uplift_column: str,
    day_column: str,
    trials: int,
    seed: int,
) -> tuple[float, float]:
    working = frame[[day_column, uplift_column]].copy()
    working[uplift_column] = pd.to_numeric(
        working[uplift_column], errors="coerce"
    )
    working = working.dropna(subset=[uplift_column])
    if working.empty:
        return math.nan, math.nan
    clusters = working.groupby(day_column, sort=True)[uplift_column].agg(
        ["sum", "count"]
    )
    mean = float(clusters["sum"].sum() / clusters["count"].sum())
    if trials <= 0:
        return mean, math.nan
    sums = clusters["sum"].to_numpy(dtype=float)
    counts = clusters["count"].to_numpy(dtype=float)
    rng = np.random.default_rng(seed)
    samples = np.empty(trials, dtype=float)
    for index in range(trials):
        selected = rng.integers(0, len(clusters), size=len(clusters))
        samples[index] = sums[selected].sum() / max(counts[selected].sum(), 1.0)
    return mean, float(np.quantile(samples, 0.025))


def fit_spibb_baseline_fallback(
    frame: pd.DataFrame,
    *,
    input_scope: str = "local_only",
    config: SpiBBConfig | None = None,
) -> tuple[pd.DataFrame, SpiBBPolicyArtifact, dict[str, Any]]:
    cfg = config or SpiBBConfig()
    required = {
        cfg.day_column,
        cfg.action_column,
        cfg.propensity_column,
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"SPIBB input missing columns: {missing}")
    if frame.empty:
        raise ValueError("SPIBB input is empty")
    working = frame.copy()
    working["_spibb_state_id"] = _state_id(working, cfg.state_columns)
    working["_spibb_uplift"] = _uplift_column(working)
    actions = set(working[cfg.action_column].astype(str))
    expected_actions = {cfg.baseline_action, cfg.candidate_action}
    if actions - expected_actions:
        raise ValueError(
            f"SPIBB input contains actions outside the frozen family: "
            f"{sorted(actions - expected_actions)}"
        )

    decisions: list[SpiBBStateDecision] = []
    accepted: list[str] = []
    for state_id, state in working.groupby("_spibb_state_id", sort=True):
        candidate = state[
            state[cfg.action_column].astype(str) == cfg.candidate_action
        ]
        baseline = state[
            state[cfg.action_column].astype(str) == cfg.baseline_action
        ]
        propensity = pd.to_numeric(
            candidate[cfg.propensity_column], errors="coerce"
        ).to_numpy(dtype=float)
        weights = np.divide(
            1.0,
            propensity,
            out=np.zeros_like(propensity),
            where=np.isfinite(propensity) & (propensity > 0.0),
        )
        ess = _effective_sample_size(weights)
        mean, lower = _cluster_lower_bound(
            state,
            uplift_column="_spibb_uplift",
            day_column=cfg.day_column,
            trials=cfg.bootstrap_trials,
            seed=cfg.random_seed
            + int(hashlib.sha256(str(state_id).encode()).hexdigest()[:8], 16),
        )
        reasons: list[str] = []
        if len(candidate) < cfg.minimum_candidate_rows:
            reasons.append("candidate_support")
        if len(baseline) < cfg.minimum_baseline_rows:
            reasons.append("baseline_support")
        if ess < cfg.minimum_effective_sample_size:
            reasons.append("ess")
        if not math.isfinite(lower) or lower <= cfg.minimum_uplift_lower_bound:
            reasons.append("uplift_lcb")
        accepted_state = not reasons
        if accepted_state:
            accepted.append(str(state_id))
        decisions.append(
            SpiBBStateDecision(
                state_id=str(state_id),
                candidate_rows=int(len(candidate)),
                baseline_rows=int(len(baseline)),
                effective_sample_size=float(ess),
                uplift_mean=float(mean),
                uplift_p025=float(lower),
                accepted=bool(accepted_state),
                reason="passed" if accepted_state else "|".join(reasons),
            )
        )

    identity_columns = [
        cfg.day_column,
        cfg.action_column,
        cfg.propensity_column,
        "_spibb_state_id",
        "_spibb_uplift",
    ]
    if "decision_id" in working:
        identity_columns.insert(1, "decision_id")
    canonical_evidence = (
        working[identity_columns]
        .sort_values(identity_columns, kind="stable")
        .to_csv(index=False, float_format="%.17g", lineterminator="\n")
    )
    evidence_sha256 = hashlib.sha256(
        canonical_evidence.encode("utf-8")
    ).hexdigest()
    identity = {
        "input_scope": input_scope,
        "config": asdict(cfg),
        "days": sorted(working[cfg.day_column].astype(str).unique()),
        "rows": len(working),
        "accepted_states": sorted(accepted),
        "evidence_sha256": evidence_sha256,
        "state_decisions": [asdict(decision) for decision in decisions],
    }
    policy_id = "spibb-" + hashlib.sha256(
        json.dumps(identity, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    artifact = SpiBBPolicyArtifact(
        schema_version=SCHEMA_VERSION,
        policy_id=policy_id,
        evidence_sha256=evidence_sha256,
        input_scope=str(input_scope),
        config=cfg,
        accepted_state_ids=tuple(sorted(accepted)),
        state_decisions=tuple(decisions),
        training_days=tuple(
            sorted(working[cfg.day_column].astype(str).unique())
        ),
    )
    accepted_set = set(artifact.accepted_state_ids)
    working["spibb_candidate_action"] = cfg.candidate_action
    working["spibb_executed_action"] = np.where(
        working["_spibb_state_id"].isin(accepted_set),
        cfg.candidate_action,
        cfg.baseline_action,
    )
    working["spibb_fallback"] = (
        working["spibb_executed_action"].astype(str) == cfg.baseline_action
    ).astype(int)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "policy_id": policy_id,
        "evidence_sha256": evidence_sha256,
        "input_scope": input_scope,
        "rows": int(len(working)),
        "days": int(working[cfg.day_column].nunique()),
        "state_count": int(len(decisions)),
        "accepted_state_count": int(len(accepted)),
        "candidate_execution_rate": float(
            (working["spibb_executed_action"] == cfg.candidate_action).mean()
        ),
        "spibb_gate_passed": bool(accepted),
        "baseline_fallback_is_default": True,
        "config": asdict(cfg),
    }
    return working, artifact, summary


def evaluate_local_m0_external_m1_gate(
    m0_summary: Mapping[str, Any],
    m1_summary: Mapping[str, Any],
    *,
    leave_one_venue_out: Mapping[str, Mapping[str, Any]] | None = None,
    minimum_incremental_uplift: float = 0.0,
) -> dict[str, Any]:
    """Require a passing local policy before external information can advance."""

    m0_pass = bool(
        m0_summary.get("spibb_gate_passed")
        or m0_summary.get("numerical_ope_gate_passed")
    )
    m1_pass = bool(
        m1_summary.get("spibb_gate_passed")
        or m1_summary.get("numerical_ope_gate_passed")
    )

    def uplift(summary: Mapping[str, Any]) -> float:
        if "estimators" in summary:
            return float(
                summary["estimators"].get(
                    "candidate_minus_behavior_dr_uplift", math.nan
                )
            )
        return float(summary.get("dr_uplift", math.nan))

    def lower(summary: Mapping[str, Any]) -> float:
        if "day_cluster_bootstrap" in summary:
            return float(
                summary["day_cluster_bootstrap"].get("uplift_p025", math.nan)
            )
        return float(summary.get("uplift_p025", math.nan))

    m0_uplift = uplift(m0_summary)
    m1_uplift = uplift(m1_summary)
    m1_increment = m1_uplift - m0_uplift
    loo_rows: dict[str, Any] = {}
    loo_pass = True
    for venue, summary in (leave_one_venue_out or {}).items():
        venue_uplift = uplift(summary)
        venue_lower = lower(summary)
        passed = (
            math.isfinite(venue_uplift)
            and math.isfinite(venue_lower)
            and venue_lower >= 0.0
        )
        loo_rows[str(venue)] = {
            "uplift": venue_uplift,
            "uplift_p025": venue_lower,
            "passed": passed,
        }
        loo_pass = loo_pass and passed
    reasons: list[str] = []
    if not m0_pass:
        reasons.append("local_m0_not_passed")
    if not m1_pass:
        reasons.append("external_m1_not_passed")
    if not math.isfinite(m1_increment) or m1_increment <= minimum_incremental_uplift:
        reasons.append("no_incremental_external_value")
    if leave_one_venue_out and not loo_pass:
        reasons.append("leave_one_venue_out_failed")
    passed = not reasons
    return {
        "schema_version": M0_M1_GATE_SCHEMA_VERSION,
        "passed": passed,
        "status": "external_m1_eligible" if passed else "external_diagnostic_only",
        "m0_passed": m0_pass,
        "m1_passed": m1_pass,
        "m0_uplift": m0_uplift,
        "m1_uplift": m1_uplift,
        "incremental_m1_minus_m0": m1_increment,
        "minimum_incremental_uplift": float(minimum_incremental_uplift),
        "leave_one_venue_out": loo_rows,
        "reasons": reasons,
    }


def _read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    return pd.read_csv(path)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ope-rows", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--input-scope", choices=("local_only", "local_plus_external"), default="local_only")
    parser.add_argument("--min-action-rows", type=int, default=100)
    parser.add_argument("--min-ess", type=float, default=100.0)
    parser.add_argument("--min-uplift-lcb", type=float, default=0.0)
    parser.add_argument("--bootstrap-trials", type=int, default=500)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    config = SpiBBConfig(
        minimum_candidate_rows=args.min_action_rows,
        minimum_baseline_rows=args.min_action_rows,
        minimum_effective_sample_size=args.min_ess,
        minimum_uplift_lower_bound=args.min_uplift_lcb,
        bootstrap_trials=args.bootstrap_trials,
    )
    rows, artifact, summary = fit_spibb_baseline_fallback(
        _read_frame(args.ope_rows.expanduser().resolve()),
        input_scope=args.input_scope,
        config=config,
    )
    prefix = args.output_prefix.expanduser().resolve()
    prefix.parent.mkdir(parents=True, exist_ok=True)
    rows.to_parquet(prefix.with_suffix(".spibb_rows.parquet"), index=False)
    artifact.save(prefix.with_suffix(".spibb_policy.json"))
    prefix.with_suffix(".spibb_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
