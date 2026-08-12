#!/usr/bin/env python3
"""Evaluate a frozen local-action replay panel without rerunning replay."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import platform
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.families.f09_campaign_action_uplift.audit.local_action_uplift import (  # noqa: E402
    OPE_FEATURES,
    QUEUE_VALUE_OPE_FEATURES,
    QUEUE_VALUE_NET_OPE_FEATURES,
    validate_action_panel,
)
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (  # noqa: E402
    OPEConfig,
    evaluate_fixed_holdout_policy,
    evaluate_offline_policy,
    write_outputs,
)
from research.families.f09_campaign_action_uplift.audit.toxic_fill_selectivity import paired_dr_selectivity  # noqa: E402
from models.replay_policies import (  # noqa: E402
    LOCAL_ACTIONS,
    QUEUE_VALUE_CANCEL_REENTER_ACTIONS,
    QUEUE_VALUE_KEEP_CANCEL_ACTIONS,
    SELL_ADD_SKIP_ACTIONS,
)

SCHEMA_VERSION = "local_action_ope_report.v1"
QUEUE_ACTION_FAMILIES = frozenset(
    {
        "queue_value_keep_cancel",
        "queue_value_cancel_reenter",
        "queue_value_net_keep_cancel",
    }
)


def _resolve_action_family(panel: pd.DataFrame) -> dict[str, Any]:
    logged_actions = set(panel["action"].astype(str))
    families = (
        (
            "queue_value_net_keep_cancel",
            QUEUE_VALUE_KEEP_CANCEL_ACTIONS,
            "keep",
            QUEUE_VALUE_NET_OPE_FEATURES,
            False,
        ),
        (
            "queue_value_cancel_reenter",
            QUEUE_VALUE_CANCEL_REENTER_ACTIONS,
            "keep",
            QUEUE_VALUE_OPE_FEATURES,
            False,
        ),
        (
            "queue_value_keep_cancel",
            QUEUE_VALUE_KEEP_CANCEL_ACTIONS,
            "keep",
            QUEUE_VALUE_OPE_FEATURES,
            False,
        ),
        (
            "sell_add_skip",
            SELL_ADD_SKIP_ACTIONS,
            "baseline",
            OPE_FEATURES,
            True,
        ),
        ("local_quote", LOCAL_ACTIONS, "baseline", OPE_FEATURES, True),
    )
    matches: list[dict[str, Any]] = []
    declared_family = ""
    if "action_family" in panel.columns:
        declared = {
            str(value)
            for value in panel["action_family"].dropna().astype(str)
            if str(value)
        }
        if len(declared) > 1:
            raise ValueError(
                f"panel contains multiple action_family values: {sorted(declared)}"
            )
        declared_family = next(iter(declared), "")
    for name, actions, baseline_action, features, require_zero_queue_cost in families:
        if name == "queue_value_net_keep_cancel" and declared_family != name:
            continue
        if declared_family and name != declared_family:
            continue
        probability_columns = {f"behavior_prob_{action}" for action in actions}
        if logged_actions.issubset(set(actions)) and probability_columns.issubset(
            panel.columns
        ):
            matches.append(
                {
                    "name": name,
                    "actions": actions,
                    "baseline_action": baseline_action,
                    "features": features,
                    "require_zero_queue_cost": require_zero_queue_cost,
                }
            )
    if len(matches) != 1:
        raise ValueError(
            "could not identify one frozen action family from logged actions and "
            f"behavior probability columns; matches={[value['name'] for value in matches]}"
        )
    return matches[0]


def _runtime_versions() -> dict[str, str]:
    import numpy as np
    import sklearn

    return {
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "scikit_learn": sklearn.__version__,
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_metadata(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    resolved = path.expanduser().resolve()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"panel metadata must be a JSON object: {resolved}")
    return payload


def _validate_queue_ope_evidence(
    panel: pd.DataFrame,
    *,
    family_name: str,
    metadata: dict[str, Any] | None,
    panel_label: str,
) -> None:
    """Require the main runner's native-support gates for queue OPE.

    Native path support is observed after treatment. The standalone report
    must never recover an apparently complete sample by silently filtering
    unsupported rows or by ignoring the main runner's OPE block decision.
    """

    if family_name not in QUEUE_ACTION_FAMILIES:
        return
    if metadata is None:
        raise ValueError(
            f"{panel_label} queue action panel requires companion metadata JSON"
        )
    if metadata.get("action_family") != family_name:
        raise ValueError(
            f"{panel_label} metadata action_family does not match panel: "
            f"{metadata.get('action_family')!r} != {family_name!r}"
        )
    expected_runtime_source = str(
        metadata.get("queue_runtime_event_source_expected", "")
    )
    observed_runtime_sources = metadata.get(
        "queue_runtime_event_sources_observed"
    )
    if not expected_runtime_source or observed_runtime_sources != [
        expected_runtime_source
    ]:
        raise ValueError(
            f"{panel_label} metadata has invalid queue runtime event-source "
            "identity"
        )
    if "queue_runtime_event_source" not in panel.columns:
        raise ValueError(
            f"{panel_label} is missing queue_runtime_event_source"
        )
    panel_runtime_sources = sorted(
        {
            str(value)
            for value in panel["queue_runtime_event_source"]
            .fillna("")
            .astype(str)
            if str(value)
        }
    )
    if panel_runtime_sources != [expected_runtime_source]:
        raise ValueError(
            f"{panel_label} queue runtime event source does not match metadata"
        )
    if "ope_block_reason" not in metadata:
        raise ValueError(f"{panel_label} metadata is missing ope_block_reason")
    block_reason = metadata.get("ope_block_reason")
    if block_reason not in (None, ""):
        raise ValueError(
            f"{panel_label} OPE is blocked by main runner: {block_reason}"
        )

    source_integrity = metadata.get("native_source_integrity")
    if not isinstance(source_integrity, dict):
        raise ValueError(
            f"{panel_label} metadata is missing native_source_integrity"
        )
    if source_integrity.get("passed") is not True:
        raise ValueError(f"{panel_label} native source integrity did not pass")
    integrity_failures = {
        str(name): value
        for name, value in source_integrity.items()
        if name != "passed"
        and isinstance(value, (int, float))
        and not isinstance(value, bool)
        and (not math.isfinite(float(value)) or float(value) != 0.0)
    }
    if integrity_failures:
        raise ValueError(
            f"{panel_label} native source integrity has failures: "
            f"{integrity_failures}"
        )

    support = metadata.get("native_action_support")
    if not isinstance(support, dict):
        raise ValueError(f"{panel_label} metadata is missing native_action_support")
    for gate in ("seed_gate", "path_gate"):
        if support.get(gate) is not True:
            raise ValueError(f"{panel_label} native {gate} did not pass")

    metadata_rows = support.get("rows")
    outcome_supported_rows = support.get("outcome_supported_rows")
    if metadata_rows is None or int(metadata_rows) != len(panel):
        raise ValueError(
            f"{panel_label} metadata rows do not match action panel rows"
        )
    if (
        outcome_supported_rows is None
        or int(outcome_supported_rows) != int(metadata_rows)
    ):
        raise ValueError(
            f"{panel_label} contains unsupported/post-treatment censored outcomes"
        )
    for field in ("ambiguous_rows", "invalid_path_rows"):
        value = support.get(field)
        if value is None or int(value) != 0:
            raise ValueError(
                f"{panel_label} contains unsupported/post-treatment censoring: "
                f"{field}={value!r}"
            )

    if "native_exchange_outcome_supported" not in panel.columns:
        raise ValueError(
            f"{panel_label} is missing native_exchange_outcome_supported"
        )
    outcome_supported = pd.to_numeric(
        panel["native_exchange_outcome_supported"], errors="coerce"
    )
    if outcome_supported.isna().any() or not outcome_supported.eq(1).all():
        raise ValueError(
            f"{panel_label} contains unsupported/post-treatment censored outcomes"
        )

    optional_binary_requirements = {
        "native_exchange_seed_supported": 1,
        "exchange_book_queue_path_valid": 1,
        "exchange_book_queue_ambiguous": 0,
    }
    for field, expected in optional_binary_requirements.items():
        if field not in panel.columns:
            continue
        values = pd.to_numeric(panel[field], errors="coerce")
        if values.isna().any() or not values.eq(expected).all():
            raise ValueError(
                f"{panel_label} contains unsupported/post-treatment censoring: "
                f"{field} must be {expected} for every row"
            )
    if "native_exchange_support_reason" in panel.columns:
        reasons = panel["native_exchange_support_reason"].fillna("").astype(str)
        if not reasons.eq("supported").all():
            raise ValueError(
                f"{panel_label} contains unsupported native support reasons"
            )


def _outcome_panels(panel: pd.DataFrame, tail_threshold: float) -> dict[str, pd.DataFrame]:
    outputs: dict[str, pd.DataFrame] = {}
    reward = panel.copy()
    reward["ope_target"] = pd.to_numeric(reward["reward"], errors="coerce")
    outputs["reward"] = reward

    terminal = panel.copy()
    terminal["ope_target"] = pd.to_numeric(
        terminal["terminal_campaign_pnl"], errors="coerce"
    )
    outputs["terminal"] = terminal

    tail = panel.copy()
    terminal_pnl = pd.to_numeric(tail["terminal_campaign_pnl"], errors="coerce")
    tail["ope_target"] = -(terminal_pnl <= float(tail_threshold)).astype(float)
    outputs["tail_avoidance"] = tail

    direct_fill = panel.copy()
    direct_fill["ope_target"] = (
        pd.to_numeric(direct_fill["intervention_fill_count"], errors="coerce") > 0
    ).astype(float)
    outputs["intervention_fill"] = direct_fill

    fill_probability = panel.copy()
    filled = (
        pd.to_numeric(
            fill_probability["intervention_fill_count"], errors="coerce"
        )
        > 0
    )
    fill_probability["ope_target"] = filled.astype(float)
    outputs["fill_probability"] = fill_probability

    toxic_fill = panel.copy()
    markout = pd.to_numeric(
        toxic_fill.get(
            "fill_value_markout_bps",
            pd.Series(math.nan, index=toxic_fill.index),
        ),
        errors="coerce",
    )
    if "fill_value_threshold_bps" in toxic_fill:
        threshold = pd.to_numeric(
            toxic_fill["fill_value_threshold_bps"], errors="coerce"
        )
    else:
        threshold = pd.Series(0.0, index=toxic_fill.index, dtype=float)
    horizon_censored = pd.to_numeric(
        toxic_fill.get(
            "fill_value_horizon_censored",
            pd.Series(0.0, index=toxic_fill.index),
        ),
        errors="coerce",
    ).fillna(1.0).astype(bool)
    toxic_fill["ope_target"] = (
        filled
        & (
            horizon_censored
            | markout.isna()
            | markout.lt(threshold.fillna(0.0))
        )
    ).astype(float)
    toxic_fill["toxic_fill_definition"] = (
        "filled and (maker-signed markout below frozen threshold or "
        "markout horizon censored)"
    )
    outputs["toxic_fill_probability"] = toxic_fill
    return outputs


def _summary_fields(summary: dict[str, Any], prefix: str) -> dict[str, Any]:
    estimators = summary["estimators"]
    interval = summary["day_cluster_bootstrap"]
    overlap = summary["overlap"]
    daily = summary["daily_uplift"]
    return {
        f"{prefix}_status": summary["status"],
        f"{prefix}_gate_passed": bool(summary["numerical_ope_gate_passed"]),
        f"{prefix}_candidate_value": estimators["candidate_clipped_dr_value"],
        f"{prefix}_uplift": estimators["candidate_minus_behavior_dr_uplift"],
        f"{prefix}_uplift_p025": interval["uplift_p025"],
        f"{prefix}_uplift_p50": interval["uplift_p50"],
        f"{prefix}_uplift_p975": interval["uplift_p975"],
        f"{prefix}_ess": overlap["effective_sample_size"],
        f"{prefix}_raw_weight_max": overlap["raw_weight_max"],
        f"{prefix}_unsupported_mass": overlap["mean_unsupported_candidate_mass"],
        f"{prefix}_daily_positive_rate": daily["positive_rate"],
        f"{prefix}_daily_positive_days": daily["positive_days"],
        f"{prefix}_daily_negative_days": daily["negative_days"],
    }


def _paired_action_contrast(
    candidate_rows: pd.DataFrame,
    baseline_rows: pd.DataFrame,
    *,
    candidate_action: str,
    baseline_action: str,
    candidate_summary: dict[str, Any],
    baseline_summary: dict[str, Any],
    bootstrap_trials: int,
    random_seed: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    keys = ["day", "decision_id"]
    for name, frame in (("candidate", candidate_rows), ("baseline", baseline_rows)):
        missing = sorted(set(keys + ["ope_dr_value"]) - set(frame.columns))
        if missing:
            raise ValueError(f"{name} OPE rows missing action-contrast columns: {missing}")
        if frame.duplicated(keys).any():
            raise ValueError(f"{name} OPE rows contain duplicate decision keys")
    baseline_values = baseline_rows[keys + ["ope_dr_value"]].rename(
        columns={"ope_dr_value": "ope_baseline_dr_value"}
    )
    contrast = candidate_rows.merge(
        baseline_values,
        on=keys,
        how="inner",
        validate="one_to_one",
    )
    if len(contrast) != len(candidate_rows) or len(contrast) != len(baseline_rows):
        raise ValueError("candidate and baseline OPE rows do not share identical decisions")
    contrast = contrast.rename(columns={"ope_dr_value": "ope_candidate_dr_value"})
    contrast["ope_dr_uplift"] = (
        pd.to_numeric(contrast["ope_candidate_dr_value"], errors="coerce")
        - pd.to_numeric(contrast["ope_baseline_dr_value"], errors="coerce")
    )
    if contrast["ope_dr_uplift"].isna().any():
        raise ValueError("paired action contrast produced non-finite DR uplift")

    clusters = contrast.groupby("day", sort=True)["ope_dr_uplift"].agg(
        ["sum", "count", "mean"]
    )
    uplift_mean = float(clusters["sum"].sum() / clusters["count"].sum())
    if bootstrap_trials > 0:
        sums = clusters["sum"].to_numpy(dtype=float)
        counts = clusters["count"].to_numpy(dtype=float)
        rng = np.random.default_rng(random_seed)
        samples = np.empty(int(bootstrap_trials), dtype=float)
        for index in range(len(samples)):
            selected = rng.integers(0, len(clusters), size=len(clusters))
            samples[index] = sums[selected].sum() / max(
                counts[selected].sum(), 1.0
            )
        p025, p50, p975 = (
            float(value) for value in np.quantile(samples, [0.025, 0.5, 0.975])
        )
    else:
        p025 = p50 = p975 = math.nan
    daily_positive = clusters["mean"] > 0.0
    daily_negative = clusters["mean"] < 0.0
    summary = {
        "schema_version": "paired_action_dr_contrast.v1",
        "candidate_action": candidate_action,
        "baseline_action": baseline_action,
        "rows": int(len(contrast)),
        "days": int(len(clusters)),
        "dr_uplift": uplift_mean,
        "day_cluster_bootstrap": {
            "trials": int(bootstrap_trials),
            "uplift_p025": p025,
            "uplift_p50": p50,
            "uplift_p975": p975,
        },
        "daily_uplift": {
            "positive_rate": float(daily_positive.mean()),
            "positive_days": int(daily_positive.sum()),
            "negative_days": int(daily_negative.sum()),
            "zero_days": int((~daily_positive & ~daily_negative).sum()),
        },
        "candidate_effective_sample_size": float(
            candidate_summary["overlap"]["effective_sample_size"]
        ),
        "baseline_effective_sample_size": float(
            baseline_summary["overlap"]["effective_sample_size"]
        ),
        "numerical_contrast_gate_passed": bool(
            candidate_summary["numerical_ope_gate_passed"]
            and baseline_summary["numerical_ope_gate_passed"]
        ),
    }
    return contrast, summary


def _write_action_contrast(
    prefix: Path,
    rows: pd.DataFrame,
    summary: dict[str, Any],
) -> dict[str, str]:
    paths = {
        "rows": str(prefix.with_suffix(".action_contrast_rows.parquet")),
        "summary": str(prefix.with_suffix(".action_contrast_summary.json")),
    }
    rows.to_parquet(paths["rows"], index=False)
    Path(paths["summary"]).write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return paths


def _attach_action_contrasts(
    scope_rows: list[dict[str, Any]],
    evaluations: dict[tuple[str, str], tuple[pd.DataFrame, dict[str, Any]]],
    *,
    output_prefix: Path,
    scope: str,
    actions: tuple[str, ...],
    baseline_action: str,
    bootstrap_trials: int,
    random_seed: int,
    min_candidate_tail_events: int,
) -> None:
    row_by_candidate = {str(row["candidate"]): row for row in scope_rows}
    baseline_row = row_by_candidate[baseline_action]
    for candidate in actions:
        row = row_by_candidate[candidate]
        row["baseline_tail_events"] = int(
            baseline_row["logged_candidate_tail_events"]
        )
        row["eligible_for_later"] = False
        if candidate == baseline_action:
            continue
        contrast_summaries: dict[str, dict[str, Any]] = {}
        for outcome in (
            "reward",
            "terminal",
            "tail_avoidance",
            "intervention_fill",
            "fill_probability",
            "toxic_fill_probability",
        ):
            candidate_rows, candidate_summary = evaluations[(candidate, outcome)]
            baseline_rows, baseline_summary = evaluations[(baseline_action, outcome)]
            contrast_rows, contrast_summary = _paired_action_contrast(
                candidate_rows,
                baseline_rows,
                candidate_action=candidate,
                baseline_action=baseline_action,
                candidate_summary=candidate_summary,
                baseline_summary=baseline_summary,
                bootstrap_trials=bootstrap_trials,
                random_seed=random_seed,
            )
            contrast_summaries[outcome] = contrast_summary
            _write_action_contrast(
                output_prefix.parent
                / f"{output_prefix.name}_{scope}_{candidate}_vs_{baseline_action}_{outcome}",
                contrast_rows,
                contrast_summary,
            )
            interval = contrast_summary["day_cluster_bootstrap"]
            daily = contrast_summary["daily_uplift"]
            row[f"{outcome}_action_uplift"] = contrast_summary["dr_uplift"]
            row[f"{outcome}_action_uplift_p025"] = interval["uplift_p025"]
            row[f"{outcome}_action_uplift_p50"] = interval["uplift_p50"]
            row[f"{outcome}_action_uplift_p975"] = interval["uplift_p975"]
            row[f"{outcome}_action_daily_positive_rate"] = daily["positive_rate"]
        selectivity = paired_dr_selectivity(
            candidate_fill_rows=evaluations[(candidate, "fill_probability")][0],
            baseline_fill_rows=evaluations[(baseline_action, "fill_probability")][0],
            candidate_toxic_rows=evaluations[
                (candidate, "toxic_fill_probability")
            ][0],
            baseline_toxic_rows=evaluations[
                (baseline_action, "toxic_fill_probability")
            ][0],
            bootstrap_trials=bootstrap_trials,
            random_seed=random_seed + 37,
        )
        selectivity_path = (
            output_prefix.parent
            / f"{output_prefix.name}_{scope}_{candidate}_vs_{baseline_action}_toxic_selectivity.json"
        )
        selectivity_path.write_text(
            json.dumps(selectivity, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        point = selectivity["point"]
        intervals = selectivity["day_cluster_bootstrap"]["intervals"]
        row["fills_retention"] = point["fills_retention"]
        row["toxic_fills_retention"] = point["toxic_fills_retention"]
        row["toxic_fill_reduction"] = point["toxic_fill_reduction"]
        row["fill_reduction"] = point["fill_reduction"]
        row["toxic_reduction_surplus"] = point[
            "toxic_reduction_surplus"
        ]
        row["toxic_reduction_surplus_p025"] = intervals[
            "toxic_reduction_surplus"
        ]["p025"]
        row["toxic_reduction_leverage"] = point[
            "toxic_reduction_leverage"
        ]
        row["toxic_fill_selectivity_log_ratio"] = point[
            "toxic_selectivity_log_ratio"
        ]
        row["toxic_fill_selectivity_log_ratio_p025"] = intervals[
            "toxic_selectivity_log_ratio"
        ]["p025"]
        row["toxic_fill_selectivity_nonlinear_score"] = point[
            "nonlinear_selectivity_score"
        ]
        row["toxic_selectivity_artifact"] = str(selectivity_path)
        reward = contrast_summaries["reward"]
        terminal = contrast_summaries["terminal"]
        tail = contrast_summaries["tail_avoidance"]
        row["tail_event_support_passed"] = bool(
            int(row["logged_candidate_tail_events"])
            >= int(min_candidate_tail_events)
            and int(row["baseline_tail_events"]) >= int(min_candidate_tail_events)
        )
        row["eligible_for_later"] = bool(
            reward["numerical_contrast_gate_passed"]
            and terminal["numerical_contrast_gate_passed"]
            and tail["numerical_contrast_gate_passed"]
            and row["tail_event_support_passed"]
            and reward["day_cluster_bootstrap"]["uplift_p025"] > 0.0
            and terminal["day_cluster_bootstrap"]["uplift_p025"] >= 0.0
            and tail["day_cluster_bootstrap"]["uplift_p025"] >= 0.0
            and intervals["toxic_reduction_surplus"]["p025"] > 0.0
            and intervals["toxic_selectivity_log_ratio"]["p025"] > 0.0
        )


def evaluate_panel(
    panel: pd.DataFrame,
    *,
    panel_metadata: dict[str, Any] | None = None,
    output_prefix: Path,
    min_train_days: int,
    test_days: int,
    embargo_days: int,
    min_action_rows: int,
    min_effective_sample_size: float,
    max_importance_weight: float,
    bootstrap_trials: int,
    random_seed: int,
    tail_threshold: float,
    min_candidate_tail_events: int,
) -> pd.DataFrame:
    family = _resolve_action_family(panel)
    _validate_queue_ope_evidence(
        panel,
        family_name=family["name"],
        metadata=panel_metadata,
        panel_label="panel",
    )
    actions = family["actions"]
    feature_names = family["features"]
    baseline_action = family["baseline_action"]
    validate_action_panel(
        panel,
        actions=actions,
        require_zero_queue_cost=family["require_zero_queue_cost"],
        require_price_bound=not family["name"].startswith("queue_value_"),
    )
    outcome_panels = _outcome_panels(panel, tail_threshold)
    rollup: list[dict[str, Any]] = []
    for scope, scoped_index in (
        ("pooled", panel.index),
        ("buy", panel.index[panel["side"].astype(str).str.upper() == "BUY"]),
        ("sell", panel.index[panel["side"].astype(str).str.upper() == "SELL"]),
    ):
        scoped_source = panel.loc[scoped_index]
        if scoped_source.empty:
            continue
        scope_rows: list[dict[str, Any]] = []
        evaluations: dict[
            tuple[str, str], tuple[pd.DataFrame, dict[str, Any]]
        ] = {}
        for candidate in actions:
            logged_candidate = scoped_source[
                scoped_source["action"].astype(str) == candidate
            ]
            row: dict[str, Any] = {
                "scope": scope,
                "candidate": candidate,
                "panel_rows": int(len(scoped_source)),
                "logged_candidate_rows": int(len(logged_candidate)),
                "logged_candidate_effective_rate": float(
                    pd.to_numeric(
                        logged_candidate.get("action_effective", pd.Series(dtype=float)),
                        errors="coerce",
                    ).mean()
                )
                if len(logged_candidate)
                else math.nan,
                "logged_candidate_reward_mean": float(
                    pd.to_numeric(logged_candidate["reward"], errors="coerce").mean()
                )
                if len(logged_candidate)
                else math.nan,
                "logged_candidate_terminal_mean": float(
                    pd.to_numeric(
                        logged_candidate["terminal_campaign_pnl"], errors="coerce"
                    ).mean()
                )
                if len(logged_candidate)
                else math.nan,
                "logged_candidate_tail_rate": float(
                    (
                        pd.to_numeric(
                            logged_candidate["terminal_campaign_pnl"], errors="coerce"
                        )
                        <= tail_threshold
                    ).mean()
                )
                if len(logged_candidate)
                else math.nan,
                "logged_candidate_tail_events": int(
                    (
                        pd.to_numeric(
                            logged_candidate["terminal_campaign_pnl"], errors="coerce"
                        )
                        <= tail_threshold
                    ).sum()
                ),
            }
            row["tail_event_support_passed"] = bool(
                row["logged_candidate_tail_events"] >= int(min_candidate_tail_events)
            )
            summaries: dict[str, dict[str, Any]] = {}
            for outcome, outcome_panel in outcome_panels.items():
                candidate_panel = outcome_panel.loc[scoped_index].copy()
                candidate_panel["candidate_action"] = candidate
                rows, folds, action_support, summary = evaluate_offline_policy(
                    candidate_panel,
                    feature_names=feature_names,
                    config=OPEConfig(
                        reward_col="ope_target",
                        split_mode="chronological",
                        min_train_days=int(min_train_days),
                        test_days=int(test_days),
                        embargo_days=int(embargo_days),
                        min_train_rows=max(500, int(min_action_rows) * 8),
                        min_action_rows=int(min_action_rows),
                        min_effective_sample_size=float(min_effective_sample_size),
                        max_importance_weight=float(max_importance_weight),
                        bootstrap_trials=int(bootstrap_trials),
                        random_seed=int(random_seed),
                    ),
                )
                summaries[outcome] = summary
                evaluations[(candidate, outcome)] = (rows, summary)
                write_outputs(
                    output_prefix.parent
                    / f"{output_prefix.name}_{scope}_{candidate}_{outcome}",
                    rows,
                    folds,
                    action_support,
                    summary,
                )
                row.update(_summary_fields(summary, outcome))

            scope_rows.append(row)
        _attach_action_contrasts(
            scope_rows,
            evaluations,
            output_prefix=output_prefix,
            scope=scope,
            actions=actions,
            baseline_action=baseline_action,
            bootstrap_trials=bootstrap_trials,
            random_seed=random_seed,
            min_candidate_tail_events=min_candidate_tail_events,
        )
        rollup.extend(scope_rows)
    return pd.DataFrame(rollup)


def evaluate_fixed_panel(
    training_panel: pd.DataFrame,
    holdout_panel: pd.DataFrame,
    *,
    training_metadata: dict[str, Any] | None = None,
    holdout_metadata: dict[str, Any] | None = None,
    output_prefix: Path,
    min_action_rows: int,
    min_effective_sample_size: float,
    max_importance_weight: float,
    bootstrap_trials: int,
    random_seed: int,
    tail_threshold: float,
    min_candidate_tail_events: int,
) -> pd.DataFrame:
    """Fit nuisance models on development and evaluate a disjoint frozen panel."""

    training_family = _resolve_action_family(training_panel)
    holdout_family = _resolve_action_family(holdout_panel)
    if training_family["name"] != holdout_family["name"]:
        raise ValueError("training and holdout panels use different action families")
    _validate_queue_ope_evidence(
        training_panel,
        family_name=training_family["name"],
        metadata=training_metadata,
        panel_label="training panel",
    )
    _validate_queue_ope_evidence(
        holdout_panel,
        family_name=holdout_family["name"],
        metadata=holdout_metadata,
        panel_label="holdout panel",
    )
    actions = training_family["actions"]
    feature_names = training_family["features"]
    baseline_action = training_family["baseline_action"]
    for frame in (training_panel, holdout_panel):
        validate_action_panel(
            frame,
            actions=actions,
            require_zero_queue_cost=training_family["require_zero_queue_cost"],
            require_price_bound=not training_family["name"].startswith(
                "queue_value_"
            ),
        )
    training_days = set(training_panel["day"].astype(str))
    holdout_days = set(holdout_panel["day"].astype(str))
    overlap = sorted(training_days & holdout_days)
    if overlap:
        raise ValueError(f"fixed evidence panels overlap on days: {overlap}")

    training_outcomes = _outcome_panels(training_panel, tail_threshold)
    holdout_outcomes = _outcome_panels(holdout_panel, tail_threshold)
    rollup: list[dict[str, Any]] = []
    scopes = (
        ("pooled", training_panel.index, holdout_panel.index),
        (
            "buy",
            training_panel.index[
                training_panel["side"].astype(str).str.upper() == "BUY"
            ],
            holdout_panel.index[
                holdout_panel["side"].astype(str).str.upper() == "BUY"
            ],
        ),
        (
            "sell",
            training_panel.index[
                training_panel["side"].astype(str).str.upper() == "SELL"
            ],
            holdout_panel.index[
                holdout_panel["side"].astype(str).str.upper() == "SELL"
            ],
        ),
    )
    for scope, training_index, holdout_index in scopes:
        scoped_holdout = holdout_panel.loc[holdout_index]
        if training_index.empty or scoped_holdout.empty:
            continue
        scope_rows: list[dict[str, Any]] = []
        evaluations: dict[
            tuple[str, str], tuple[pd.DataFrame, dict[str, Any]]
        ] = {}
        for candidate in actions:
            logged_candidate = scoped_holdout[
                scoped_holdout["action"].astype(str) == candidate
            ]
            row: dict[str, Any] = {
                "scope": scope,
                "candidate": candidate,
                "training_rows": int(len(training_index)),
                "panel_rows": int(len(scoped_holdout)),
                "logged_candidate_rows": int(len(logged_candidate)),
                "logged_candidate_effective_rate": float(
                    pd.to_numeric(
                        logged_candidate.get(
                            "action_effective", pd.Series(dtype=float)
                        ),
                        errors="coerce",
                    ).mean()
                )
                if len(logged_candidate)
                else math.nan,
                "logged_candidate_reward_mean": float(
                    pd.to_numeric(logged_candidate["reward"], errors="coerce").mean()
                )
                if len(logged_candidate)
                else math.nan,
                "logged_candidate_terminal_mean": float(
                    pd.to_numeric(
                        logged_candidate["terminal_campaign_pnl"], errors="coerce"
                    ).mean()
                )
                if len(logged_candidate)
                else math.nan,
                "logged_candidate_tail_rate": float(
                    (
                        pd.to_numeric(
                            logged_candidate["terminal_campaign_pnl"],
                            errors="coerce",
                        )
                        <= tail_threshold
                    ).mean()
                )
                if len(logged_candidate)
                else math.nan,
                "logged_candidate_tail_events": int(
                    (
                        pd.to_numeric(
                            logged_candidate["terminal_campaign_pnl"],
                            errors="coerce",
                        )
                        <= tail_threshold
                    ).sum()
                ),
            }
            row["tail_event_support_passed"] = bool(
                row["logged_candidate_tail_events"] >= int(min_candidate_tail_events)
            )
            summaries: dict[str, dict[str, Any]] = {}
            for outcome in training_outcomes:
                training_target = training_outcomes[outcome].loc[training_index].copy()
                holdout_target = holdout_outcomes[outcome].loc[holdout_index].copy()
                training_target["candidate_action"] = candidate
                holdout_target["candidate_action"] = candidate
                rows, folds, action_support, summary = evaluate_fixed_holdout_policy(
                    training_target,
                    holdout_target,
                    feature_names=feature_names,
                    config=OPEConfig(
                        reward_col="ope_target",
                        min_train_rows=max(500, int(min_action_rows) * 8),
                        min_action_rows=int(min_action_rows),
                        min_effective_sample_size=float(
                            min_effective_sample_size
                        ),
                        max_importance_weight=float(max_importance_weight),
                        bootstrap_trials=int(bootstrap_trials),
                        random_seed=int(random_seed),
                    ),
                )
                summaries[outcome] = summary
                evaluations[(candidate, outcome)] = (rows, summary)
                write_outputs(
                    output_prefix.parent
                    / f"{output_prefix.name}_{scope}_{candidate}_{outcome}",
                    rows,
                    folds,
                    action_support,
                    summary,
                )
                row.update(_summary_fields(summary, outcome))

            scope_rows.append(row)
        _attach_action_contrasts(
            scope_rows,
            evaluations,
            output_prefix=output_prefix,
            scope=scope,
            actions=actions,
            baseline_action=baseline_action,
            bootstrap_trials=bootstrap_trials,
            random_seed=random_seed,
            min_candidate_tail_events=min_candidate_tail_events,
        )
        rollup.extend(scope_rows)
    return pd.DataFrame(rollup)


def _markdown(rollup: pd.DataFrame, metadata: dict[str, Any]) -> str:
    lines = [
        "# Local Action OPE Rollup",
        "",
        f"- Panel rows: `{metadata['rows']}`",
        f"- Max importance weight: `{metadata['max_importance_weight']}`",
        f"- Extreme terminal-tail threshold: `{metadata['tail_threshold']}` USDC",
        "- Tail metric: terminal campaign MTM at or below the threshold; this is "
        "distinct from the broader `metrics.py` `loss_tail` label",
        f"- Minimum logged candidate tail events: "
        f"`{metadata['min_candidate_tail_events']}`",
        f"- Eligible for later: `{metadata['eligible_for_later']}`",
        "",
        "| scope | candidate | support | K1-K0 reward uplift [2.5%,97.5%] | K1-K0 terminal uplift | extreme-tail events | tail support | ESS | later |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rollup.to_dict("records"):
        action_uplift = row.get("reward_action_uplift", math.nan)
        action_p025 = row.get("reward_action_uplift_p025", math.nan)
        action_p975 = row.get("reward_action_uplift_p975", math.nan)
        terminal_uplift = row.get("terminal_action_uplift", math.nan)
        lines.append(
            f"| {row['scope']} | `{row['candidate']}` | {row['logged_candidate_rows']} | "
            f"{action_uplift:+.6f} "
            f"[{action_p025:+.6f},{action_p975:+.6f}] | "
            f"{terminal_uplift:+.6f} | "
            f"{row['logged_candidate_tail_events']} | "
            f"{bool(row['tail_event_support_passed'])} | "
            f"{row['reward_ess']:.1f} | {bool(row['eligible_for_later'])} |"
        )
    lines.extend(
        [
            "",
            "> Passing numerical overlap gates does not prove exchangeability or no-interference. "
            "The fixed candidate must also pass paired replay fills/campaign/tail gates before live use.",
            "",
            "> Zero logged candidate tail events is missing tail support, not evidence that the "
            "candidate eliminates tails.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--panel-csv", type=Path, default=None)
    parser.add_argument("--panel-metadata-json", type=Path, default=None)
    parser.add_argument("--training-panel-csv", type=Path, default=None)
    parser.add_argument("--training-panel-metadata-json", type=Path, default=None)
    parser.add_argument("--holdout-panel-csv", type=Path, default=None)
    parser.add_argument("--holdout-panel-metadata-json", type=Path, default=None)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--min-train-days", type=int, default=50)
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--min-action-rows", type=int, default=50)
    parser.add_argument("--min-effective-sample-size", type=float, default=100.0)
    parser.add_argument("--max-importance-weight", type=float, default=40.0)
    parser.add_argument("--bootstrap-trials", type=int, default=1000)
    parser.add_argument("--random-seed", type=int, default=20260713)
    parser.add_argument("--tail-threshold", type=float, default=-5.0)
    parser.add_argument("--min-candidate-tail-events", type=int, default=5)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    fixed_holdout = (
        args.training_panel_csv is not None or args.holdout_panel_csv is not None
    )
    if fixed_holdout:
        if args.panel_csv is not None:
            raise SystemExit(
                "--panel-csv cannot be combined with fixed train/holdout inputs"
            )
        if args.training_panel_csv is None or args.holdout_panel_csv is None:
            raise SystemExit(
                "fixed evaluation requires both --training-panel-csv and "
                "--holdout-panel-csv"
            )
        training_path = args.training_panel_csv.expanduser().resolve()
        panel_path = args.holdout_panel_csv.expanduser().resolve()
        training_panel = pd.read_csv(training_path)
        training_metadata = _load_metadata(args.training_panel_metadata_json)
        panel_metadata = _load_metadata(args.holdout_panel_metadata_json)
    else:
        if args.panel_csv is None:
            raise SystemExit(
                "provide --panel-csv or fixed training/holdout panel inputs"
            )
        training_path = None
        panel_path = args.panel_csv.expanduser().resolve()
        training_metadata = None
        panel_metadata = _load_metadata(args.panel_metadata_json)
    output_prefix = args.output_prefix.expanduser().resolve()
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    panel = pd.read_csv(panel_path)
    family = _resolve_action_family(panel)
    if fixed_holdout:
        rollup = evaluate_fixed_panel(
            training_panel,
            panel,
            training_metadata=training_metadata,
            holdout_metadata=panel_metadata,
            output_prefix=output_prefix,
            min_action_rows=args.min_action_rows,
            min_effective_sample_size=args.min_effective_sample_size,
            max_importance_weight=args.max_importance_weight,
            bootstrap_trials=args.bootstrap_trials,
            random_seed=args.random_seed,
            tail_threshold=args.tail_threshold,
            min_candidate_tail_events=args.min_candidate_tail_events,
        )
    else:
        rollup = evaluate_panel(
            panel,
            panel_metadata=panel_metadata,
            output_prefix=output_prefix,
            min_train_days=args.min_train_days,
            test_days=args.test_days,
            embargo_days=args.embargo_days,
            min_action_rows=args.min_action_rows,
            min_effective_sample_size=args.min_effective_sample_size,
            max_importance_weight=args.max_importance_weight,
            bootstrap_trials=args.bootstrap_trials,
            random_seed=args.random_seed,
            tail_threshold=args.tail_threshold,
            min_candidate_tail_events=args.min_candidate_tail_events,
        )
    rollup_path = output_prefix.with_suffix(".rollup.csv")
    metadata_path = output_prefix.with_suffix(".summary.json")
    markdown_path = output_prefix.with_suffix(".md")
    rollup.to_csv(rollup_path, index=False)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "action_family": family["name"],
        "registered_actions": list(family["actions"]),
        "baseline_action": family["baseline_action"],
        "ope_feature_columns": list(family["features"]),
        "panel_path": str(panel_path),
        "panel_sha256": _sha256(panel_path),
        "evaluation_design": (
            "fixed_development_to_holdout"
            if fixed_holdout
            else "chronological_cross_fit"
        ),
        "training_panel_path": str(training_path or ""),
        "training_panel_sha256": (
            _sha256(training_path) if training_path is not None else ""
        ),
        "rows": int(len(panel)),
        "days": int(panel["day"].nunique()),
        "runtime": _runtime_versions(),
        "max_importance_weight": float(args.max_importance_weight),
        "tail_threshold": float(args.tail_threshold),
        "tail_metric": "extreme_terminal_tail",
        "tail_definition": "terminal_campaign_pnl <= tail_threshold",
        "broader_campaign_label": (
            "metrics.py loss_tail also uses campaign MAE and max-inventory conditions; "
            "it is not the same outcome"
        ),
        "min_candidate_tail_events": int(args.min_candidate_tail_events),
        "eligible_for_later": rollup.loc[
            rollup["eligible_for_later"], ["scope", "candidate"]
        ].to_dict("records"),
        "warning": (
            "This evaluates one randomized intervention per campaign. It does not "
            "identify unsupported opener/reducing actions or a multi-step campaign policy."
        ),
    }
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    markdown_path.write_text(_markdown(rollup, metadata), encoding="utf-8")
    print(
        json.dumps(
            {
                "rollup": str(rollup_path),
                "summary": str(metadata_path),
                "markdown": str(markdown_path),
                "eligible_for_later": metadata["eligible_for_later"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
