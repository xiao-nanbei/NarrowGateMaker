#!/usr/bin/env python3
"""Run one-campaign-one-decision R0/R1/R2 safe-add randomization."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models import backtest_tick as bt  # noqa: E402
from research.families.f01_fixed_parameter_racing import daily_smoke_sweep as smoke  # noqa: E402
from models.audit.experiment_manifest import (  # noqa: E402
    build_manifest,
    git_workspace_identity,
    write_code_checkpoint,
    write_manifest,
)
from research.families.f09_campaign_action_uplift.audit.local_action_uplift import (  # noqa: E402
    _clean_summary,
    _live_like_params,
    _sha256,
)
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (  # noqa: E402
    OPEConfig,
    evaluate_offline_policy,
)
from research.families.f09_campaign_action_uplift.audit.offline_policy_evaluation import (  # noqa: E402
    write_outputs as write_ope_outputs,
)
from research.families.f09_campaign_action_uplift.audit.safe_add_rearm_ope_panel import (  # noqa: E402
    OPE_FEATURES,
    support_only_panel,
    validate_randomized_panel,
)
from models.backtest_config import (  # noqa: E402
    load_tick_base_params,
    validate_formal_replay_calibration,
)
from models.replay_policies import (  # noqa: E402
    SAFE_ADD_REARM_ACTIONS,
    normalize_safe_add_rearm_probabilities,
)

SCHEMA_VERSION = "safe_add_rearm_randomized.v2"


def _run_day(task: tuple[str, str, dict[str, Any], bool, bool]) -> dict[str, Any]:
    day, symbol, raw_base, support_only, skip_control = task
    base = dict(raw_base)
    base["safe_add_rearm_randomized_seed"] = int(
        base.get("safe_add_rearm_randomized_seed", 20260714)
    ) + int(day.replace("-", ""))
    model_dir = base.get("resolved_model_dir") or base.get("model_dir")
    bt.configure_symbol(symbol, model_dir_override=model_dir)
    started = time.perf_counter()
    window = smoke._load_window(day, base)

    control = None
    if not support_only and not skip_control:
        control_params = dict(base)
        control_params["safe_add_rearm_randomized_enabled"] = False
        control_params["trace_safe_add_rearm_intervention_max"] = 0
        control = bt._simulate_tick_with_engine(
            "python",
            window["trades"],
            window["var_ts_ms"],
            window["var_ssq"],
            control_params,
            ml_data=window["ml_data"],
            bbo_data=window["bbo_data"],
            l2_data=window["l2_data"],
            var_ti=window["var_ti"],
            var_retsq=window["var_retsq"],
        )
    randomized = bt._simulate_tick_with_engine(
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
    actions = [
        {"day": day, **row}
        for row in randomized["_safe_add_rearm_intervention_trace"]
    ]
    daily: dict[str, Any] = {
        "day": day,
        "runtime_s": time.perf_counter() - started,
        "interventions": len(actions),
    }
    if control is not None:
        daily.update(_clean_summary(control, "control"))
        daily.update(_clean_summary(randomized, "randomized"))
        daily["pnl_delta"] = float(daily["randomized_pnl"]) - float(
            daily["control_pnl"]
        )
        daily["fills_delta"] = int(daily["randomized_fills_total"]) - int(
            daily["control_fills_total"]
        )
    return {"day": day, "actions": actions, "daily": daily}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", nargs="+", default=[])
    parser.add_argument("--days-file", type=Path, default=None)
    parser.add_argument("--symbol", default="BTCUSDC")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--window-cache-dir", type=Path, default=None)
    parser.add_argument("--refresh-window-cache", action="store_true")
    parser.add_argument("--refresh-partials", action="store_true")
    parser.add_argument("--strict-calibration", action="store_true")
    parser.add_argument("--live-perf-telemetry", type=Path, default=None)
    parser.add_argument(
        "--live-perf-latency-mode", choices=("avg", "max", "sum"), default="avg"
    )
    parser.add_argument("--latency-profile-id", default="")
    parser.add_argument("--trace-max", type=int, default=50_000)
    parser.add_argument("--random-seed", type=int, default=20260714)
    parser.add_argument("--elapsed-s", type=float, default=20.0)
    parser.add_argument(
        "--panel-role",
        choices=("smoke", "development", "embargo", "later"),
        default="smoke",
    )
    parser.add_argument(
        "--action-probabilities-json",
        default="",
        help="Complete R0/R1/R2 JSON mapping; default is 0.80/0.10/0.10.",
    )
    parser.add_argument("--evaluate-ope", action="store_true")
    parser.add_argument(
        "--support-only",
        action="store_true",
        help=(
            "Run only randomized action-bearing replay, skip the control, and "
            "persist no reward/PnL columns."
        ),
    )
    parser.add_argument(
        "--skip-control",
        action="store_true",
        help=(
            "Keep the complete randomized outcome panel but skip the redundant "
            "non-randomized daily control replay. R0 is the action-level control."
        ),
    )
    parser.add_argument("--min-train-days", type=int, default=50)
    parser.add_argument("--test-days", type=int, default=10)
    parser.add_argument("--embargo-days", type=int, default=1)
    parser.add_argument("--min-action-rows", type=int, default=50)
    parser.add_argument("--min-effective-sample-size", type=float, default=100.0)
    parser.add_argument("--bootstrap-trials", type=int, default=500)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.trace_max <= 0:
        raise SystemExit("--trace-max must be positive")
    if args.elapsed_s < 0.0:
        raise SystemExit("--elapsed-s cannot be negative")
    if args.support_only and args.evaluate_ope:
        raise SystemExit("--support-only cannot be combined with --evaluate-ope")
    if args.live_perf_telemetry is not None and not args.latency_profile_id.strip():
        raise SystemExit("--latency-profile-id is required with empirical latency")
    requested_days = list(args.days)
    if args.days_file is not None:
        day_frame = pd.read_csv(args.days_file.expanduser().resolve())
        if "day" not in day_frame:
            raise SystemExit("--days-file must contain a day column")
        requested_days.extend(day_frame["day"].astype(str).tolist())
    if not requested_days:
        raise SystemExit("provide --days or --days-file")
    days = smoke._normalize_days(requested_days)
    raw_probabilities = (
        json.loads(args.action_probabilities_json)
        if args.action_probabilities_json
        else None
    )
    probabilities = normalize_safe_add_rearm_probabilities(raw_probabilities)
    config = args.config.expanduser().resolve()
    if "PUBLIC TEMPLATE" in config.read_text(encoding="utf-8")[:4096]:
        raise SystemExit(
            "refusing randomized strategy replay with the tracked PUBLIC TEMPLATE; "
            "provide the frozen private/EC2 rolling-baseline config"
        )
    output_prefix = args.output_prefix.expanduser().resolve()
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    code_identity = git_workspace_identity(ROOT)
    checkpoint = write_code_checkpoint(
        output_prefix.parent / f"{output_prefix.name}.code_checkpoint",
        repo_root=ROOT,
        code_identity=code_identity,
    )

    bt.configure_symbol(args.symbol)
    base = load_tick_base_params(
        symbol=args.symbol,
        config_path=config,
        configure_symbol=bt.configure_symbol,
        require_historical_bbo=True,
        strict_calibration=bool(args.strict_calibration),
    )
    _live_like_params(base)
    base.update(
        {
            "trace_quotes_max": 0,
            "trace_decisions_max": 0,
            "trace_queue_events_max": 0,
            "trace_fills_max": 0,
            "trace_safe_add_rearm_max": 0,
            "local_action_ope_enabled": False,
            "trace_local_action_ope_max": 0,
            "safe_add_rearm_randomized_enabled": True,
            "trace_safe_add_rearm_intervention_max": int(args.trace_max),
            "safe_add_rearm_randomized_probabilities": probabilities,
            "safe_add_rearm_randomized_seed": int(args.random_seed),
            "safe_add_rearm_randomized_elapsed_s": float(args.elapsed_s),
        }
    )
    if args.live_perf_telemetry is not None:
        telemetry = args.live_perf_telemetry.expanduser().resolve()
        samples = bt._load_live_perf_latency_samples(
            telemetry, mode=args.live_perf_latency_mode
        )
        base["_new_order_latency_samples_ms"] = samples[
            "new_order_latency_samples_ms"
        ]
        base["_cancel_order_latency_samples_ms"] = samples[
            "cancel_order_latency_samples_ms"
        ]
    if args.strict_calibration:
        validate_formal_replay_calibration(base, require_latency=True)
    if args.window_cache_dir:
        base["_window_cache_dir"] = str(
            args.window_cache_dir.expanduser().resolve()
        )
    if args.refresh_window_cache:
        base["_refresh_window_cache"] = True

    partial_dir = output_prefix.parent / f"{output_prefix.name}.partial"
    partial_dir.mkdir(parents=True, exist_ok=True)
    identity_path = partial_dir / "run_identity.json"
    run_identity = {
        "schema_version": SCHEMA_VERSION,
        "workspace_sha256": code_identity["workspace_sha256"],
        "config_sha256": _sha256(config),
        "days": days,
        "probabilities": probabilities,
        "elapsed_s": float(args.elapsed_s),
        "latency_profile_id": args.latency_profile_id,
        "random_seed": int(args.random_seed),
        "panel_role": str(args.panel_role),
        "support_only": bool(args.support_only),
        "skip_control": bool(args.skip_control),
    }
    if identity_path.exists() and not args.refresh_partials:
        existing = json.loads(identity_path.read_text(encoding="utf-8"))
        if existing != run_identity:
            raise RuntimeError("partial output identity differs; use a new prefix")
    identity_path.write_text(
        json.dumps(run_identity, indent=2, sort_keys=True) + "\n", encoding="utf-8"
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

    tasks = [
        (
            day,
            args.symbol,
            base,
            bool(args.support_only),
            bool(args.skip_control),
        )
        for day in pending
    ]
    workers = max(1, min(int(args.workers), max(len(tasks), 1)))
    if workers == 1:
        iterator = map(_run_day, tasks)
        for item in iterator:
            results.append(item)
            pd.DataFrame(item["actions"]).to_csv(
                partial_dir / f"{item['day']}.actions.csv", index=False
            )
            pd.DataFrame([item["daily"]]).to_csv(
                partial_dir / f"{item['day']}.daily.csv", index=False
            )
            suffix = ""
            if not args.support_only:
                suffix = f" pnl_delta={item['daily']['pnl_delta']:+.4f}"
            print(
                f"{item['day']}: interventions={len(item['actions'])}{suffix}",
                flush=True,
            )
    elif tasks:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_run_day, task): task[0] for task in tasks}
            for future in concurrent.futures.as_completed(futures):
                item = future.result()
                results.append(item)
                pd.DataFrame(item["actions"]).to_csv(
                    partial_dir / f"{item['day']}.actions.csv", index=False
                )
                pd.DataFrame([item["daily"]]).to_csv(
                    partial_dir / f"{item['day']}.daily.csv", index=False
                )
                print(
                    f"{item['day']}: interventions={len(item['actions'])}", flush=True
                )

    results.sort(key=lambda item: item["day"])
    panel = pd.DataFrame([row for item in results for row in item["actions"]])
    daily = pd.DataFrame([item["daily"] for item in results])
    validate_randomized_panel(panel)
    if args.support_only:
        panel = support_only_panel(panel)
        panel_path = output_prefix.with_suffix(".support_panel.csv")
    else:
        panel_path = output_prefix.with_suffix(".action_panel.csv")
    daily_path = output_prefix.with_suffix(".daily.csv")
    metadata_path = output_prefix.with_suffix(".metadata.json")
    days_path = output_prefix.with_suffix(".days.csv")
    panel.to_csv(panel_path, index=False)
    daily.to_csv(daily_path, index=False)
    pd.DataFrame({"day": days}).to_csv(days_path, index=False)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "engine": "python_authoritative_randomized_replay",
        "days": days,
        "rows": int(len(panel)),
        "campaigns": int(panel[["day", "campaign_id"]].drop_duplicates().shape[0]),
        "action_counts": {
            str(key): int(value)
            for key, value in panel["action"].value_counts().sort_index().items()
        },
        "behavior_probabilities": probabilities,
        "elapsed_s": float(args.elapsed_s),
        "one_intervention_per_campaign": True,
        "reducing_side_modified": False,
        "order_size_modified": False,
        "inventory_limit_modified": False,
        "support_only": bool(args.support_only),
        "control_replay_run": bool(not args.support_only and not args.skip_control),
        "action_bearing_evidence": True,
        "strategy_evidence": False,
        "config_path": str(config),
        "config_sha256": _sha256(config),
        "latency_profile_id": args.latency_profile_id,
        "code_checkpoint": checkpoint,
        "workspace_sha256": code_identity["workspace_sha256"],
        "panel_role": str(args.panel_role),
        "promotion_status": "not_evaluated",
    }
    if not args.support_only:
        metadata.update(
            {
                "reward": "fill_value - campaign_cost - queue_cost",
                "campaign_cost": (
                    "accounting residual; randomized reward is identified"
                ),
                "censored_rows": int(
                    pd.to_numeric(panel["campaign_censored"]).sum()
                ),
            }
        )
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    ope_paths: dict[str, dict[str, str]] = {}
    if args.evaluate_ope:
        for scope, scoped_panel in (
            ("pooled", panel),
            ("buy", panel[panel["side"].astype(str).str.upper() == "BUY"]),
            ("sell", panel[panel["side"].astype(str).str.upper() == "SELL"]),
        ):
            if scoped_panel.empty:
                continue
            for candidate in SAFE_ADD_REARM_ACTIONS:
                candidate_panel = scoped_panel.copy()
                candidate_panel["candidate_action"] = candidate
                rows, folds, actions, summary = evaluate_offline_policy(
                    candidate_panel,
                    feature_names=OPE_FEATURES,
                    config=OPEConfig(
                        split_mode="chronological",
                        min_train_days=int(args.min_train_days),
                        test_days=int(args.test_days),
                        embargo_days=int(args.embargo_days),
                        min_train_rows=max(500, int(args.min_action_rows) * 8),
                        min_action_rows=int(args.min_action_rows),
                        min_effective_sample_size=float(
                            args.min_effective_sample_size
                        ),
                        bootstrap_trials=int(args.bootstrap_trials),
                        random_seed=int(args.random_seed),
                    ),
                )
                ope_paths[f"{scope}.{candidate}"] = write_ope_outputs(
                    output_prefix.parent
                    / f"{output_prefix.name}_{scope}_{candidate}",
                    rows,
                    folds,
                    actions,
                    summary,
                )

    manifest = build_manifest(
        {
            "experiment_id": output_prefix.name,
            "engine": "python_authoritative_randomized_replay",
            "config_path": str(config),
            "dataset_manifest_path": str(days_path),
            "feature_schema_version": "safe_add_rearm_local_state.v1",
            "model_versions": {"ope": "doubly_robust.v1"},
            "label_versions": (
                {"support": "action_submission_and_fill_count.v1"}
                if args.support_only
                else {
                    "reward": "decision_to_campaign_terminal_mtm.v1",
                    "fill_value": "maker_signed_30s_usdc.v1",
                }
            ),
            "splits": {str(args.panel_role): days, "late_holdout": []},
            "baseline_definition": {
                "name": "current_fill_cooldown_control",
                "config_sha256": _sha256(config),
                "latency_profile_id": args.latency_profile_id,
            },
            "action_definition": {
                "actions": list(SAFE_ADD_REARM_ACTIONS),
                "probabilities": probabilities,
                "elapsed_s": float(args.elapsed_s),
                "eligibility": "first add-side quote blocked only by fill cooldown",
            },
            "artifact_paths": [
                str(panel_path),
                str(daily_path),
                str(metadata_path),
                str(days_path),
            ],
            "metrics": metadata,
            "promotion_status": "not_evaluated",
            "notes": (
                "Support-only replay: outcomes embargoed; no live policy wiring."
                if args.support_only
                else "Replay research only; no live policy wiring."
            ),
        },
        repo_root=ROOT,
        code_identity=code_identity,
    )
    manifest_path = output_prefix.with_suffix(".experiment_manifest.json")
    if manifest_path.exists():
        raise FileExistsError(f"refusing to overwrite manifest: {manifest_path}")
    write_manifest(manifest_path, manifest)
    print(
        json.dumps(
            {
                "panel": str(panel_path),
                "daily": str(daily_path),
                "metadata": str(metadata_path),
                "manifest": str(manifest_path),
                "ope": ope_paths,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
