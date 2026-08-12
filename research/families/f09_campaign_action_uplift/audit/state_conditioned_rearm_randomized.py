#!/usr/bin/env python3
"""Generate a frozen side-specific post-cooldown rearm action panel.

The behavior policy randomizes exactly once per inventory campaign after the
current add-side cooldown has expired:

* ``baseline_rearm`` resumes the baseline add quote;
* ``continue_block_until_recovery`` keeps skipping add quote cycles while the
  preregistered adverse-flow/weak-refill/weak-recovery state remains active.

Reducing quotes, order size, and inventory limits are unchanged.  This module
has no live wiring and deliberately uses the Python authoritative replay until
the multi-cycle state machine has C++ parity.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from models import backtest_tick as bt
from research.families.f01_fixed_parameter_racing import daily_smoke_sweep as smoke
from models.audit.evidence_split import load_evidence_panel
from models.audit.experiment_manifest import (
    build_manifest,
    git_workspace_identity,
    write_code_checkpoint,
    write_manifest,
)
from research.families.f09_campaign_action_uplift.audit.local_action_uplift import _clean_summary, _live_like_params
from models.backtest_config import (
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from models.replay_contract import (
    configure_fixed_latency_distribution,
    freeze_replay_contract,
    validate_frozen_replay_contract,
    write_replay_contract,
)
from models.replay_policies import (
    STATE_CONDITIONED_REARM_ACTIONS,
    normalize_state_conditioned_rearm_probabilities,
)

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "state_conditioned_rearm_randomized.v1"
FAMILY_IDS = {
    "SELL": "sell_state_conditioned_rearm_after85_v1",
    "BUY": "buy_state_conditioned_rearm_after85_v1",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _set_book_roots(bbo_dir: str | Path, l2_dir: str | Path) -> None:
    bt.BBO_DIR = Path(bbo_dir).expanduser().resolve()
    bt.L2_DIR = Path(l2_dir).expanduser().resolve()
    if not bt.BBO_DIR.is_dir() or not bt.L2_DIR.is_dir():
        raise FileNotFoundError(
            f"strict BBO/L2 roots are unavailable: {bt.BBO_DIR} | {bt.L2_DIR}"
        )


def _run_day(task: tuple[str, str, str, dict[str, Any]]) -> dict[str, Any]:
    day, symbol, side, raw_base = task
    base = dict(raw_base)
    _set_book_roots(
        str(base["_historical_bbo_dir"]),
        str(base["_historical_l2_dir"]),
    )
    model_dir = base.get("resolved_model_dir") or base.get("model_dir")
    bt.configure_symbol(symbol, model_dir_override=model_dir)
    base["state_conditioned_rearm_seed"] = int(
        base.get("state_conditioned_rearm_seed", 20260722)
    ) + int(day.replace("-", ""))
    validate_frozen_replay_contract(base)
    started = time.perf_counter()
    window = smoke._load_window(day, base)
    result = bt._simulate_tick_with_engine(
        "python",
        window["trades"],
        window["var_ts_ms"],
        window["var_ssq"],
        base,
        ml_data=window["ml_data"],
        bbo_data=window["bbo_data"],
        l2_data=window["l2_data"],
        var_ti=window["var_ti"],
        var_retsq=window["var_retsq"],
    )
    rows = [
        {"day": day, **row}
        for row in result.get("_state_conditioned_rearm_trace", ())
        if str(row.get("side", "")).upper() == side
    ]
    daily = {
        "day": day,
        "runtime_s": time.perf_counter() - started,
        "interventions": len(rows),
        "effective_candidate_interventions": sum(
            int(row.get("action_effective", 0) or 0) for row in rows
        ),
        "blocked_quote_cycles": sum(
            int(row.get("blocked_quote_cycles", 0) or 0) for row in rows
        ),
        **_clean_summary(result, "randomized"),
    }
    return {"day": day, "actions": rows, "daily": daily}


def validate_panel(frame: pd.DataFrame, *, side: str) -> None:
    if frame.empty:
        raise ValueError("state-conditioned rearm produced no interventions")
    required = {
        "day",
        "decision_id",
        "campaign_id",
        "side",
        "inventory_role",
        "action",
        "behavior_propensity",
        "reward",
        "fill_value",
        "campaign_cost",
        "queue_cost",
        "reward_identity_error",
        "entry_state_active",
        "entry_state_data_valid",
        "action_effective",
        "baseline_cooldown_total_ms",
        "baseline_rearm_elapsed_ms",
        "blocked_quote_cycles",
        "external_reference_used",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"state-conditioned rearm panel is missing: {missing}")
    if frame["decision_id"].astype(str).duplicated().any():
        raise ValueError("decision_id must be unique")
    if frame.groupby(["day", "campaign_id"], sort=False).size().max() != 1:
        raise ValueError("each campaign may contain only one randomized intervention")
    if set(frame["side"].astype(str).str.upper()) != {side}:
        raise ValueError(f"panel must be {side}-only")
    if set(frame["inventory_role"].astype(str).str.lower()) != {"add"}:
        raise ValueError("rearm may intervene only on exposure-increasing add quotes")
    if set(frame["action"].astype(str)) - set(STATE_CONDITIONED_REARM_ACTIONS):
        raise ValueError("panel contains an unregistered rearm action")
    probability_columns = [
        f"behavior_prob_{action}" for action in STATE_CONDITIONED_REARM_ACTIONS
    ]
    probabilities = frame[probability_columns].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(probabilities.to_numpy(dtype=float)).all():
        raise ValueError("behavior probabilities must be finite")
    if not np.allclose(probabilities.to_numpy(dtype=float), 0.5, atol=1e-12):
        raise ValueError("formal rearm behavior policy must remain exact 50/50")
    if not np.allclose(
        pd.to_numeric(frame["behavior_propensity"], errors="coerce"),
        0.5,
        atol=1e-12,
    ):
        raise ValueError("logged behavior propensity must equal 0.5")
    numeric = frame[
        ["reward", "fill_value", "campaign_cost", "queue_cost", "reward_identity_error"]
    ].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(numeric.to_numpy(dtype=float)).all():
        raise ValueError("reward accounting fields must be finite")
    if numeric["reward_identity_error"].abs().max() > 1e-9:
        raise ValueError("reward identity does not close")
    if (pd.to_numeric(frame["queue_cost"], errors="coerce").abs() > 1e-12).any():
        raise ValueError("rearm must not reset the queue before its state exit")
    if (
        pd.to_numeric(frame["baseline_rearm_elapsed_ms"], errors="coerce")
        + 1e-9
        < pd.to_numeric(frame["baseline_cooldown_total_ms"], errors="coerce")
    ).any():
        raise ValueError("an intervention occurred before the baseline cooldown ended")
    effective = pd.to_numeric(frame["action_effective"], errors="coerce").eq(1)
    if (
        frame.loc[effective, "action"].astype(str)
        != "continue_block_until_recovery"
    ).any():
        raise ValueError("only the candidate action may be effective")
    if pd.to_numeric(frame.loc[effective, "entry_state_active"], errors="coerce").ne(1).any():
        raise ValueError("candidate action was applied outside the frozen entry state")
    if pd.to_numeric(frame.loc[effective, "blocked_quote_cycles"], errors="coerce").lt(1).any():
        raise ValueError("effective candidates must block at least one add quote cycle")
    if pd.to_numeric(frame["external_reference_used"], errors="coerce").ne(0).any():
        raise ValueError("local M0 rearm may not use external reference state")
    if "state_conditioned_rearm_policy_version" in frame:
        versions = set(
            frame["state_conditioned_rearm_policy_version"].astype(str)
        )
        if versions == {"composite_recovery_v2"}:
            recovery_columns = {
                "recovery_shock_decay_score",
                "recovery_refill_score",
                "recovery_microprice_score",
                "recovery_queue_score",
                "recovery_composite_score",
                "recovery_score_threshold",
            }
            missing_recovery = sorted(recovery_columns - set(frame.columns))
            if missing_recovery:
                raise ValueError(
                    f"composite recovery panel is missing: {missing_recovery}"
                )
            numeric_recovery = frame[list(recovery_columns)].apply(
                pd.to_numeric, errors="coerce"
            )
            if not np.isfinite(numeric_recovery.to_numpy(dtype=float)).all():
                raise ValueError("composite recovery scores must be finite")
            active = pd.to_numeric(
                frame["entry_state_active"], errors="coerce"
            ).eq(1)
            if (
                numeric_recovery.loc[active, "recovery_composite_score"]
                >= numeric_recovery.loc[active, "recovery_score_threshold"]
            ).any():
                raise ValueError(
                    "composite candidate entered after the recovery event"
                )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--side", choices=("SELL", "BUY"), default="SELL")
    parser.add_argument("--panel", choices=("development", "validation", "sealed_holdout"), required=True)
    parser.add_argument("--evidence-split", type=Path, required=True)
    parser.add_argument("--access-decision", type=Path, default=None)
    parser.add_argument("--allow-sealed-holdout", action="store_true")
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--queue-calibration-artifact", type=Path, required=True)
    parser.add_argument("--bbo-dir", type=Path, required=True)
    parser.add_argument("--l2-dir", type=Path, required=True)
    parser.add_argument("--live-perf-telemetry", type=Path, required=True)
    parser.add_argument("--live-perf-latency-mode", choices=("avg", "max", "sum"), default="avg")
    parser.add_argument("--latency-profile-id", required=True)
    parser.add_argument(
        "--latency-environment",
        default="aws_tokyo_ec2_2vcpu_4g_amazon_linux",
    )
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--window-cache-dir", type=Path, default=None)
    parser.add_argument("--refresh-partials", action="store_true")
    parser.add_argument("--trace-max", type=int, default=100_000)
    parser.add_argument("--random-seed", type=int, default=20260722)
    parser.add_argument("--min-followup-s", type=float, default=1_800.0)
    parser.add_argument(
        "--frozen-family-spec",
        type=Path,
        default=None,
        help=(
            "Optional recovery_event_rearm_family.v1 spec. Without it the "
            "closed after85_v1 mechanism is reproduced exactly."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    side = str(args.side).upper()
    family_spec_path = (
        args.frozen_family_spec.expanduser().resolve()
        if args.frozen_family_spec is not None
        else None
    )
    family_spec: dict[str, Any] | None = None
    if family_spec_path is not None:
        family_spec = json.loads(family_spec_path.read_text(encoding="utf-8"))
        if family_spec.get("schema_version") != "recovery_event_rearm_family.v1":
            raise ValueError("unsupported frozen recovery-event family spec")
        if str(family_spec.get("side", "")).upper() != side:
            raise ValueError("frozen family side differs from --side")
        if family_spec.get("policy_version") != "composite_recovery_v2":
            raise ValueError("frozen family does not use composite_recovery_v2")
        if family_spec.get("actions") != list(STATE_CONDITIONED_REARM_ACTIONS):
            raise ValueError("frozen family action registry differs from replay")
        if family_spec.get("behavior_probabilities") != (
            normalize_state_conditioned_rearm_probabilities()
        ):
            raise ValueError("frozen family must retain exact 50/50 overlap")
        family_id = str(family_spec["family_id"])
    else:
        family_id = FAMILY_IDS[side]
    evidence_path = args.evidence_split.expanduser().resolve()
    days, evidence_identity = load_evidence_panel(
        evidence_path,
        args.panel,
        allow_sealed_holdout=bool(args.allow_sealed_holdout),
        access_decision_path=(
            args.access_decision.expanduser().resolve()
            if args.access_decision is not None
            else None
        ),
    )
    if evidence_identity["family_id"] != family_id:
        raise ValueError("evidence split belongs to a different action family")
    if evidence_identity["actions"] != list(STATE_CONDITIONED_REARM_ACTIONS):
        raise ValueError("evidence split action set differs from replay")
    if evidence_identity["behavior_probabilities"] != {
        "baseline_rearm": 0.5,
        "continue_block_until_recovery": 0.5,
    }:
        raise ValueError("evidence split must freeze exact 50/50 propensities")
    if evidence_identity["sides"] != [side]:
        raise ValueError("evidence split side differs from replay")
    if family_spec is not None:
        frozen_evidence = family_spec.get("evidence_split") or {}
        if frozen_evidence.get("sha256") != evidence_identity["manifest_sha256"]:
            raise ValueError("evidence split hash differs from frozen family spec")
    if not days:
        raise ValueError(f"{args.panel} contains no days")

    config = args.config.expanduser().resolve()
    queue_artifact = args.queue_calibration_artifact.expanduser().resolve()
    telemetry = args.live_perf_telemetry.expanduser().resolve()
    bbo_dir = args.bbo_dir.expanduser().resolve()
    l2_dir = args.l2_dir.expanduser().resolve()
    output_prefix = args.output_prefix.expanduser().resolve()
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    _set_book_roots(bbo_dir, l2_dir)

    bt.configure_symbol(args.symbol)
    base = load_tick_base_params(
        symbol=args.symbol,
        config_path=config,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        queue_calibration_path=queue_artifact,
        strict_calibration=True,
    )
    _live_like_params(base)
    if not np.isclose(float(base.get("fill_cooldown", 0.0)), 85.0, atol=1e-12):
        raise ValueError("this family is defined against the current 85-second baseline")
    if float(base.get("fill_cooldown_reducing", 0.0) or 0.0) != 0.0:
        raise ValueError("reducing-side cooldown must remain disabled")
    samples = bt._load_live_perf_latency_samples(
        telemetry, mode=args.live_perf_latency_mode
    )
    base["_new_order_latency_samples_ms"] = samples["new_order_latency_samples_ms"]
    base["_cancel_order_latency_samples_ms"] = samples["cancel_order_latency_samples_ms"]
    configure_fixed_latency_distribution(
        base,
        scenario="baseline",
        profile_id=args.latency_profile_id,
        environment=args.latency_environment,
    )
    base.update(
        {
            "_historical_bbo_dir": str(bbo_dir),
            "_historical_l2_dir": str(l2_dir),
            "trace_quotes_max": 0,
            "trace_decisions_max": 0,
            "trace_queue_events_max": 0,
            "trace_fills_max": 0,
            "local_action_ope_enabled": False,
            "trace_local_action_ope_max": 0,
            "sell_add_skip_ope_enabled": False,
            "queue_value_keep_cancel_randomized_enabled": False,
            "queue_value_cancel_reenter_randomized_enabled": False,
            "safe_add_rearm_randomized_enabled": False,
            "state_conditioned_rearm_enabled": True,
            "state_conditioned_rearm_sides": (side,),
            "state_conditioned_rearm_probabilities": normalize_state_conditioned_rearm_probabilities(),
            "state_conditioned_rearm_seed": int(args.random_seed),
            "state_conditioned_rearm_min_elapsed_s": 85.0,
            "state_conditioned_rearm_min_followup_s": float(args.min_followup_s),
            "trace_state_conditioned_rearm_max": int(args.trace_max),
        }
    )
    if family_spec is not None:
        recovery_spec = family_spec.get("recovery_event") or {}
        base.update(
            {
                "state_conditioned_rearm_policy_version": (
                    "composite_recovery_v2"
                ),
                "recovery_event_rearm_score_threshold": float(
                    recovery_spec["score_threshold"]
                ),
                "recovery_event_rearm_max_book_age_ms": float(
                    recovery_spec.get("max_book_age_ms", 2_000.0)
                ),
                "recovery_event_rearm_flow_reference_floor": 0.05,
                "recovery_event_rearm_component_epsilon": float(
                    recovery_spec.get("component_epsilon", 1e-6)
                ),
            }
        )
    if args.window_cache_dir is not None:
        base["_window_cache_dir"] = str(args.window_cache_dir.expanduser().resolve())
    validate_formal_replay_calibration(base, require_latency=True)
    contract = freeze_replay_contract(base, purpose="formal", initial_state_mode="fresh_start", root=ROOT)
    validate_frozen_replay_contract(base)
    contract_path = output_prefix.with_suffix(".replay_contract.json")
    if contract_path.exists():
        existing_contract = json.loads(contract_path.read_text(encoding="utf-8"))
        if existing_contract.get("contract_sha256") != contract["contract_sha256"]:
            raise FileExistsError("output prefix already belongs to another replay contract")
    else:
        write_replay_contract(contract, contract_path)

    code_identity = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        output_prefix.parent / f"{output_prefix.name}.code_checkpoint",
        repo_root=ROOT,
        code_identity=code_identity,
    )
    partial_dir = output_prefix.parent / f"{output_prefix.name}.partial"
    partial_dir.mkdir(parents=True, exist_ok=True)
    run_identity = {
        "schema_version": SCHEMA_VERSION,
        "family_id": family_id,
        "side": side,
        "panel": args.panel,
        "days": days,
        "workspace_sha256": code_identity["workspace_sha256"],
        "config_sha256": _sha256(config),
        "queue_artifact_sha256": _sha256(queue_artifact),
        "telemetry_sha256": _sha256(telemetry),
        "bbo_build_sha256": _sha256(bbo_dir.parent / "build_identity.json"),
        "evidence_split_sha256": evidence_identity["manifest_sha256"],
        "replay_contract_sha256": contract["contract_sha256"],
        "random_seed": int(args.random_seed),
        "family_spec_sha256": (
            _sha256(family_spec_path) if family_spec_path is not None else ""
        ),
    }
    identity_path = partial_dir / "run_identity.json"
    if identity_path.exists() and not args.refresh_partials:
        if json.loads(identity_path.read_text(encoding="utf-8")) != run_identity:
            raise RuntimeError("partial output identity differs; choose a new prefix")
    else:
        identity_path.write_text(
            json.dumps(run_identity, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    results: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in days:
        action_path = partial_dir / f"{day}.actions.csv"
        daily_path = partial_dir / f"{day}.daily.csv"
        if not args.refresh_partials and action_path.exists() and daily_path.exists():
            results.append(
                {
                    "day": day,
                    "actions": pd.read_csv(action_path).to_dict("records"),
                    "daily": pd.read_csv(daily_path).iloc[0].to_dict(),
                }
            )
        else:
            pending.append(day)

    tasks = [(day, args.symbol, side, base) for day in pending]
    workers = max(1, min(int(args.workers), max(len(tasks), 1)))
    if workers == 1:
        iterator = map(_run_day, tasks)
        for item in iterator:
            results.append(item)
            pd.DataFrame(item["actions"]).to_csv(partial_dir / f"{item['day']}.actions.csv", index=False)
            pd.DataFrame([item["daily"]]).to_csv(partial_dir / f"{item['day']}.daily.csv", index=False)
            print(
                f"{item['day']}: interventions={len(item['actions'])} "
                f"effective={item['daily']['effective_candidate_interventions']}",
                flush=True,
            )
    elif tasks:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_run_day, task): task[0] for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                item = future.result()
                results.append(item)
                pd.DataFrame(item["actions"]).to_csv(partial_dir / f"{item['day']}.actions.csv", index=False)
                pd.DataFrame([item["daily"]]).to_csv(partial_dir / f"{item['day']}.daily.csv", index=False)
                print(
                    f"{item['day']}: interventions={len(item['actions'])} "
                    f"effective={item['daily']['effective_candidate_interventions']}",
                    flush=True,
                )

    results.sort(key=lambda item: item["day"])
    panel = pd.DataFrame([row for item in results for row in item["actions"]])
    daily = pd.DataFrame([item["daily"] for item in results])
    validate_panel(panel, side=side)
    panel_path = output_prefix.with_suffix(".action_panel.csv")
    daily_path = output_prefix.with_suffix(".daily.csv")
    days_path = output_prefix.with_suffix(".days.csv")
    metadata_path = output_prefix.with_suffix(".metadata.json")
    panel.to_csv(panel_path, index=False)
    daily.to_csv(daily_path, index=False)
    pd.DataFrame({"day": days}).to_csv(days_path, index=False)
    candidate = panel[panel["action"].astype(str).eq("continue_block_until_recovery")]
    effective = candidate[pd.to_numeric(candidate["action_effective"], errors="coerce").eq(1)]
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "family_id": family_id,
        "action_family": (
            "recovery_event_rearm"
            if family_spec is not None
            else "state_conditioned_rearm"
        ),
        "panel_role": args.panel,
        "engine": "python_authoritative_randomized_replay",
        "days": days,
        "rows": int(len(panel)),
        "campaigns": int(panel[["day", "campaign_id"]].drop_duplicates().shape[0]),
        "side": side,
        "action_counts": {str(key): int(value) for key, value in panel["action"].value_counts().sort_index().items()},
        "behavior_probabilities": normalize_state_conditioned_rearm_probabilities(),
        "candidate_effective_rows": int(len(effective)),
        "candidate_effective_rate": float(len(effective) / max(len(candidate), 1)),
        "candidate_multicycle_rows": int((pd.to_numeric(effective["blocked_quote_cycles"], errors="coerce") > 1).sum()),
        "candidate_median_blocked_cycles": float(pd.to_numeric(effective["blocked_quote_cycles"], errors="coerce").median()) if len(effective) else 0.0,
        "candidate_median_block_duration_s": float(pd.to_numeric(effective["state_block_duration_s"], errors="coerce").median()) if len(effective) else 0.0,
        "one_intervention_per_campaign": True,
        "reducing_side_modified": False,
        "order_size_modified": False,
        "inventory_limit_modified": False,
        "external_reference_used": False,
        "baseline_fill_cooldown_s": 85.0,
        "state_conditioned_rearm_policy_version": str(
            base.get(
                "state_conditioned_rearm_policy_version",
                "adverse_conjunction_v1",
            )
        ),
        "family_spec_path": (
            str(family_spec_path) if family_spec_path is not None else ""
        ),
        "family_spec_sha256": (
            _sha256(family_spec_path) if family_spec_path is not None else ""
        ),
        "reward": "fill_value - campaign_cost - queue_cost",
        "evidence_split": evidence_identity,
        "replay_contract_path": str(contract_path),
        "replay_contract_sha256": contract["contract_sha256"],
        "config_path": str(config),
        "config_sha256": _sha256(config),
        "queue_artifact_path": str(queue_artifact),
        "queue_artifact_sha256": _sha256(queue_artifact),
        "telemetry_path": str(telemetry),
        "telemetry_sha256": _sha256(telemetry),
        "code_checkpoint": checkpoint,
        "workspace_sha256": code_identity["workspace_sha256"],
        "ope_block_reason": "",
        "promotion_status": "not_evaluated",
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    manifest = build_manifest(
        {
            "experiment_id": output_prefix.name,
            "engine": metadata["engine"],
            "config_path": str(config),
            "dataset_manifest_path": str(days_path),
            "feature_schema_version": (
                "post_fill_shock_refill_microprice_queue_recovery.v1"
                if family_spec is not None
                else "post_fill_shock_refill_recovery_path.v1"
            ),
            "model_versions": {
                "outcome_policy": (
                    "recovery_event_rearm_dr.v1"
                    if family_spec is not None
                    else "state_conditioned_rearm_dr.v1"
                )
            },
            "label_versions": {
                "reward": "decision_to_campaign_terminal_mtm.v1",
                "tail": "terminal_campaign_pnl_thresholds.v1",
            },
            "splits": {args.panel: days},
            "baseline_definition": {
                "name": "current_85s_add_fill_cooldown",
                "config_sha256": _sha256(config),
                "replay_contract_sha256": contract["contract_sha256"],
            },
            "action_definition": {
                "family_id": family_id,
                "side": side,
                "actions": list(STATE_CONDITIONED_REARM_ACTIONS),
                "probabilities": normalize_state_conditioned_rearm_probabilities(),
                "eligibility": "first baseline-eligible post-cooldown exposure-increasing add decision per campaign",
                "candidate": "continue skipping add cycles until adverse state exits",
            },
            "artifact_paths": [
                str(evidence_path), str(panel_path), str(daily_path),
                str(metadata_path), str(contract_path), str(queue_artifact),
                str(bbo_dir.parent / "build_identity.json"),
                *(
                    [str(family_spec_path)]
                    if family_spec_path is not None
                    else []
                ),
            ],
            "metrics": metadata,
            "promotion_status": "not_evaluated",
            "notes": (
                "Replay research only; composite recovery threshold was frozen "
                "by support/activity preflight without value outcomes."
                if family_spec is not None
                else "Replay research only; no live wiring and no fixed-cooldown search."
            ),
        },
        repo_root=ROOT,
        code_identity=code_identity,
    )
    manifest_path = output_prefix.with_suffix(".experiment_manifest.json")
    if manifest_path.exists():
        raise FileExistsError(f"refusing to overwrite manifest: {manifest_path}")
    write_manifest(manifest_path, manifest)
    print(json.dumps({"panel": str(panel_path), "daily": str(daily_path), "metadata": str(metadata_path), "manifest": str(manifest_path)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
