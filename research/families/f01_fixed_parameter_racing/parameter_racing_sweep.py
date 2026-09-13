#!/usr/bin/env python3
"""Racing-style constrained parameter selection for NarrowGate.

This is a coordinator, not a new replay engine. It generates arm specs from
``research.families.f01_fixed_parameter_racing.parameter_selection``, optionally calls the Python-authoritative
``campaign_outcome_replay_audit.py`` on a small day block, and scores the
result with constraint-first gates.

Workflow:

1. coverage report: active / live-only / shadow / archived;
2. arm generation: one-factor and/or Sobol/random candidates;
3. quick-smoke block: few representative days and a capped arm set, no promotion decision;
4. quick-full-main-effect block: explicit full one-factor pass over the selected groups;
5. retained39 block: 39-good-day OOS for survivors;
6. pnl-survival block: run good days chronologically and stop arms after
   cumulative PnL / drawdown thresholds are breached;
7. full retained + blocked OOS only for final candidates.

中文说明：这个 runner 的默认行为是 dry-run。必须显式加 `--execute`
才会启动 replay，避免误触发长时间 sweep 或干扰当前稳定环境。
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_paths import data_root, window_cache_root  # noqa: E402
from models.backtest_config import load_tick_base_params  # noqa: E402
from models.symbol_paths import DEFAULT_SYMBOL  # noqa: E402
from research.families.f01_fixed_parameter_racing.audit.paired_screening import (  # noqa: E402
    screen_paired_daily_arms,
)
from research.families.f01_fixed_parameter_racing.parameter_selection import (  # noqa: E402
    ArmSpec,
    composite_arms,
    constraint_score_rollup,
    live_active_sobol_arms,
    mechanism_local_sobol_arms,
    sampled_arms,
    single_factor_arms,
    write_arm_specs,
    write_coverage_report,
)

DEFAULT_QUICK_DAYS = (
    "2026-01-15",
    "2026-04-22",
    "2026-06-26",
    "2026-07-01",
)

DEFAULT_QUICK_SMOKE_MAX_ARMS = 30


def _is_demo_config(config_path: Path) -> bool:
    """Return True for the public/demo config that must not drive formal sweeps."""
    try:
        text = config_path.read_text(encoding="utf-8")
    except OSError:
        return False
    return "models/example_model_bundle" in text or "Values here are safe examples" in text

DEFAULT_RETAINED39_DAYS = (
    "2026-01-15",
    "2026-01-22",
    "2026-02-10",
    "2026-02-20",
    "2026-03-17",
    "2026-04-22",
    "2026-05-15",
    "2026-05-31",
    "2026-06-01",
    "2026-06-02",
    "2026-06-03",
    "2026-06-04",
    "2026-06-05",
    "2026-06-06",
    "2026-06-07",
    "2026-06-08",
    "2026-06-09",
    "2026-06-10",
    "2026-06-11",
    "2026-06-12",
    "2026-06-13",
    "2026-06-14",
    "2026-06-15",
    "2026-06-16",
    "2026-06-17",
    "2026-06-18",
    "2026-06-19",
    "2026-06-20",
    "2026-06-21",
    "2026-06-22",
    "2026-06-23",
    "2026-06-24",
    "2026-06-26",
    "2026-06-27",
    "2026-06-28",
    "2026-06-29",
    "2026-06-30",
    "2026-07-01",
    "2026-07-02",
)


def _dedupe_arms(arms: list[ArmSpec]) -> list[ArmSpec]:
    out: list[ArmSpec] = []
    seen: set[str] = set()
    for arm in arms:
        if arm.name in seen:
            continue
        seen.add(arm.name)
        out.append(arm)
    return out


def _read_arm_specs(path: Path) -> list[ArmSpec]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("arms", [])
    if not isinstance(payload, list):
        raise SystemExit(f"--input-arm-spec-json must contain a list or {{'arms': list}}: {path}")
    arms: list[ArmSpec] = []
    for idx, row in enumerate(payload):
        if not isinstance(row, dict):
            raise SystemExit(f"arm spec row {idx} is not an object")
        name = str(row.get("name", "")).strip()
        if not name:
            raise SystemExit(f"arm spec row {idx} missing name")
        overrides = row.get("overrides", {})
        if not isinstance(overrides, dict):
            raise SystemExit(f"arm spec {name} overrides must be an object")
        arms.append(
            ArmSpec(
                name=name,
                group=str(row.get("group", "input")).strip() or "input",
                overrides=overrides,
                note=str(row.get("note", "")).strip(),
            )
        )
    return _dedupe_arms(arms)


def _canonical_stage(stage: str) -> str:
    return stage


def _stage_days(stage: str, explicit: list[str] | None) -> list[str]:
    stage = _canonical_stage(stage)
    if explicit:
        return explicit
    if stage in {"quick-smoke", "quick-full-main-effect"}:
        return list(DEFAULT_QUICK_DAYS)
    if stage == "retained39":
        return list(DEFAULT_RETAINED39_DAYS)
    raise SystemExit(f"Provide --days for stage={stage}")


def _manifest_days(
    *,
    manifest: Path | None,
    start_day: str,
    end_day: str,
    explicit_days: list[str] | None,
    extra_days: list[str] | None,
) -> list[str]:
    """Return sorted retained-good days for chronological PnL survival.

    中文说明：survival racing 必须从 good-day manifest 取日期，避免把坏日
    或缺 exact L2 的日期重新混进参数选择。刚下载但尚未刷新 manifest 的
    日期可用 --survival-extra-days 显式补入。
    """
    days: list[str] = []
    if explicit_days:
        days.extend(str(day)[:10] for day in explicit_days)
    elif manifest is not None:
        frame = pd.read_csv(manifest)
        if frame.empty:
            raise SystemExit(f"Empty day manifest: {manifest}")
        day_col = "day" if "day" in frame.columns else frame.columns[0]
        days.extend(str(day)[:10] for day in frame[day_col].dropna().astype(str))
    else:
        raise SystemExit("pnl-survival requires --manifest or --days")
    if extra_days:
        days.extend(str(day)[:10] for day in extra_days)
    out = sorted({day for day in days if start_day <= day <= end_day})
    if not out:
        raise SystemExit(f"No pnl-survival days in range {start_day}..{end_day}")
    return out


def _cap_arms_round_robin(arms: list[ArmSpec], max_arms: int) -> list[ArmSpec]:
    """Keep baseline, then sample arms across groups instead of taking a prefix.

    中文说明：quick-smoke 不能只拿 parameter registry 的前 30 个，否则会偏向
    spread/guard 而漏掉 cooldown/execution。这里按 group round-robin 抽样，
    让第一轮机制 smoke 至少覆盖每类 active 参数。
    """
    if max_arms <= 0 or len(arms) <= max_arms:
        return arms
    baseline = [arm for arm in arms if arm.name == "baseline"]
    rest = [arm for arm in arms if arm.name != "baseline"]
    by_group: dict[str, list[ArmSpec]] = {}
    for arm in rest:
        by_group.setdefault(arm.group, []).append(arm)
    groups = sorted(by_group)
    selected: list[ArmSpec] = list(baseline)
    while len(selected) < max_arms and any(by_group.values()):
        for group in groups:
            bucket = by_group.get(group, [])
            if not bucket:
                continue
            selected.append(bucket.pop(0))
            if len(selected) >= max_arms:
                break
    return selected


def _run_campaign_audit(
    *,
    symbol: str,
    config_path: Path,
    days: list[str],
    arms_path: Path,
    arms: list[ArmSpec],
    tag: str,
    workers: int,
    window_cache_dir: str,
    live_perf_telemetry: str,
    live_perf_latency_mode: str,
    arm_chunk_size: int,
    refresh_window_cache: bool,
    engine: str,
    no_cpp_baseline_parity_gate: bool,
    cpp_parity_max_fill_diff_rate: float,
    cpp_parity_max_pnl_diff: float,
    strict_calibration: bool = False,
) -> Path:
    cmd = [
        sys.executable,
        str(ROOT / "models" / "campaign_outcome_replay_audit.py"),
        "--symbol",
        symbol,
        "--config",
        str(config_path),
        "--days",
        *days,
        "--arm-spec-json",
        str(arms_path),
        "--arms",
        *(arm.name for arm in arms),
        "--tag",
        tag,
        "--live-like-replay-baseline",
        "--workers",
        str(max(1, workers)),
        "--engine",
        engine,
    ]
    if no_cpp_baseline_parity_gate:
        cmd.append("--no-cpp-baseline-parity-gate")
    if strict_calibration:
        cmd.append("--strict-calibration")
    if engine == "cpp":
        cmd.extend(
            [
                "--cpp-parity-max-fill-diff-rate",
                str(float(cpp_parity_max_fill_diff_rate)),
                "--cpp-parity-max-pnl-diff",
                str(float(cpp_parity_max_pnl_diff)),
            ]
        )
    if window_cache_dir:
        cmd.extend(["--window-cache-dir", window_cache_dir])
    if refresh_window_cache:
        cmd.append("--refresh-window-cache")
    if arm_chunk_size > 0:
        cmd.extend(["--arm-chunk-size", str(int(arm_chunk_size))])
    if live_perf_telemetry:
        cmd.extend(["--live-perf-telemetry", live_perf_telemetry, "--live-perf-latency-mode", live_perf_latency_mode])
    env = os.environ.copy()
    env.setdefault("OMP_NUM_THREADS", "1")
    env.setdefault("OPENBLAS_NUM_THREADS", "1")
    env.setdefault("MKL_NUM_THREADS", "1")
    env.setdefault("NUMEXPR_NUM_THREADS", "1")
    env["MM_LIVE_CONFIG"] = str(config_path)
    print("Executing:", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=str(ROOT), env=env, check=True)
    results_dir = Path(
        env.get("MM_RESULTS_DIR", str(data_root(ROOT) / "backtest_results_btcusdc"))
    ).expanduser()
    return results_dir / f"campaign_outcome_replay_{tag}_{symbol.lower()}.rollup.csv"


def _write_plan(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {path}")


def _num(row: pd.Series, col: str, default: float = 0.0) -> float:
    if col not in row.index:
        return default
    value = pd.to_numeric(pd.Series([row[col]]), errors="coerce").iloc[0]
    if pd.isna(value):
        return default
    return float(value)


def _run_pnl_survival(
    *,
    symbol: str,
    config_path: Path,
    days: list[str],
    arms_path: Path,
    arms: list[ArmSpec],
    tag: str,
    out_dir: Path,
    workers: int,
    window_cache_dir: str,
    live_perf_telemetry: str,
    live_perf_latency_mode: str,
    arm_chunk_size: int,
    refresh_window_cache: bool,
    engine: str,
    no_cpp_baseline_parity_gate: bool,
    cpp_parity_max_fill_diff_rate: float,
    cpp_parity_max_pnl_diff: float,
    strict_calibration: bool,
    survival_metric: str,
    survival_min_cum_pnl: float,
    survival_min_delta_pnl: float,
    survival_max_drawdown: float,
    survival_resume_state_csv: Path | None,
) -> None:
    """Run chronological PnL-first racing and persist state after every day."""
    if "baseline" not in {arm.name for arm in arms}:
        arms = [ArmSpec("baseline", "baseline", {}, "Current live config baseline.")] + arms
    by_name = {arm.name: arm for arm in arms}
    active_names = ["baseline"] + [arm.name for arm in arms if arm.name != "baseline"]
    state: dict[str, dict[str, Any]] = {
        name: {
            "arm": name,
            "group": by_name[name].group,
            "cum_metric": 0.0,
            "cum_delta_vs_baseline": 0.0,
            "peak_metric": 0.0,
            "max_drawdown": 0.0,
            "days_run": 0,
            "alive": True,
            "stopped_day": "",
            "stop_reason": "",
        }
        for name in active_names
    }
    state_rows: list[dict[str, Any]] = []
    eliminated_rows: list[dict[str, Any]] = []
    state_path = out_dir / f"{tag}_survival_state.csv"
    eliminated_path = out_dir / f"{tag}_survival_eliminated.csv"
    survivor_path = out_dir / f"{tag}_survivor_arms.json"
    leaderboard_path = out_dir / f"{tag}_survival_leaderboard.csv"
    resume_last_day = ""
    if survival_resume_state_csv is not None:
        resume_frame = pd.read_csv(survival_resume_state_csv)
        if resume_frame.empty or "day" not in resume_frame.columns or "arm" not in resume_frame.columns:
            raise SystemExit(f"Invalid survival resume state CSV: {survival_resume_state_csv}")
        resume_last_day = str(resume_frame["day"].dropna().astype(str).max())[:10]
        resume_last = resume_frame.loc[resume_frame["day"].astype(str).str[:10] == resume_last_day].copy()
        base_resume = resume_last.loc[resume_last["arm"].astype(str) == "baseline"]
        if base_resume.empty:
            raise SystemExit(f"Resume state missing baseline row for {resume_last_day}: {survival_resume_state_csv}")
        for _, row in resume_last.iterrows():
            name = str(row["arm"])
            if name not in state:
                continue
            st = state[name]
            st["cum_metric"] = _num(row, "cum_metric")
            st["cum_delta_vs_baseline"] = _num(row, "cum_delta_vs_baseline")
            st["peak_metric"] = _num(row, "peak_metric")
            st["max_drawdown"] = _num(row, "max_drawdown")
            st["days_run"] = int(_num(row, "day_index", _num(row, "days_run", 0.0)))
            stop_reasons: list[str] = []
            if name != "baseline":
                if math.isfinite(survival_min_cum_pnl) and float(st["cum_metric"]) <= survival_min_cum_pnl:
                    stop_reasons.append(f"cum_{survival_metric}<={survival_min_cum_pnl:g}")
                if math.isfinite(survival_min_delta_pnl) and float(st["cum_delta_vs_baseline"]) <= survival_min_delta_pnl:
                    stop_reasons.append(f"cum_delta<={survival_min_delta_pnl:g}")
                if math.isfinite(survival_max_drawdown) and float(st["max_drawdown"]) >= survival_max_drawdown:
                    stop_reasons.append(f"max_drawdown>={survival_max_drawdown:g}")
            if stop_reasons:
                st["alive"] = False
                st["stopped_day"] = resume_last_day
                st["stop_reason"] = ";".join(stop_reasons)
                eliminated_rows.append(dict(st))
            state_rows.append(
                {
                    "day_index": int(st["days_run"]),
                    "day": resume_last_day,
                    "arm": name,
                    "group": st["group"],
                    "daily_metric": _num(row, "daily_metric"),
                    "baseline_daily_metric": _num(row, "baseline_daily_metric"),
                    "daily_delta_vs_baseline": _num(row, "daily_delta_vs_baseline"),
                    "cum_metric": st["cum_metric"],
                    "cum_delta_vs_baseline": st["cum_delta_vs_baseline"],
                    "peak_metric": st["peak_metric"],
                    "max_drawdown": st["max_drawdown"],
                    "alive_after_day": bool(st["alive"]),
                    "stopped_day": st["stopped_day"],
                    "stop_reason": st["stop_reason"],
                    "resume_seed": True,
                    "replay_pnl_sum": _num(row, "replay_pnl_sum"),
                    "terminal_pnl_sum": _num(row, "terminal_pnl_sum"),
                    "replay_inv_adj_sum": _num(row, "replay_inv_adj_sum"),
                    "fills_total": _num(row, "fills_total"),
                    "campaigns": _num(row, "campaigns"),
                    "loss_tail": _num(row, "loss_tail"),
                    "decision_pause_rate": _num(row, "decision_pause_rate"),
                    "decision_keep_rate": _num(row, "decision_keep_rate"),
                    "replay_avg_final_spread": _num(row, "replay_avg_final_spread"),
                }
            )
        days = [day for day in days if day > resume_last_day]
        active_names = ["baseline"] + [name for name in active_names if name != "baseline" and state[name]["alive"]]
        print(
            f"[survival] resume from {survival_resume_state_csv}: "
            f"last_day={resume_last_day} active={len(active_names)}/{len(state)} "
            f"remaining_days={len(days)}",
            flush=True,
        )
        pd.DataFrame(state_rows).to_csv(state_path, index=False)
        pd.DataFrame(eliminated_rows).to_csv(eliminated_path, index=False)
        write_arm_specs([by_name[name] for name in active_names], survivor_path)

    for day_idx, day in enumerate(days, start=1):
        live_names = [name for name in active_names if state[name]["alive"]]
        active_arms = [by_name[name] for name in live_names]
        if not active_arms:
            print(f"No active arms remain before {day}; stopping.", flush=True)
            break
        effective_day_idx = day_idx + (int(state["baseline"]["days_run"]) if resume_last_day else 0)
        day_tag = f"{tag}_day{effective_day_idx:03d}_{day.replace('-', '')}"
        # Keep one C++ baseline parity gate at the start of the survival run.
        # Re-running it every day would dominate the speed benefit of racing.
        day_no_cpp_gate = bool(no_cpp_baseline_parity_gate or resume_last_day or (engine == "cpp" and day_idx > 1))
        rollup_path = _run_campaign_audit(
            symbol=symbol,
            config_path=config_path,
            days=[day],
            arms_path=arms_path,
            arms=active_arms,
            tag=day_tag,
            workers=workers,
            window_cache_dir=window_cache_dir,
            live_perf_telemetry=live_perf_telemetry,
            live_perf_latency_mode=live_perf_latency_mode,
            arm_chunk_size=arm_chunk_size,
            refresh_window_cache=refresh_window_cache,
            engine=engine,
            no_cpp_baseline_parity_gate=day_no_cpp_gate,
            cpp_parity_max_fill_diff_rate=cpp_parity_max_fill_diff_rate,
            cpp_parity_max_pnl_diff=cpp_parity_max_pnl_diff,
            strict_calibration=strict_calibration,
        )
        daily = pd.read_csv(rollup_path)
        if survival_metric not in daily.columns:
            raise SystemExit(f"{rollup_path} missing survival metric column: {survival_metric}")
        if "baseline" not in set(daily["arm"].astype(str)):
            raise SystemExit(f"{rollup_path} missing baseline row")
        base_row = daily.loc[daily["arm"].astype(str) == "baseline"].iloc[0]
        baseline_metric = _num(base_row, survival_metric)

        for _, row in daily.iterrows():
            name = str(row["arm"])
            if name not in state:
                continue
            metric = _num(row, survival_metric)
            st = state[name]
            st["cum_metric"] = float(st["cum_metric"]) + metric
            st["cum_delta_vs_baseline"] = float(st["cum_delta_vs_baseline"]) + (metric - baseline_metric)
            st["days_run"] = int(st["days_run"]) + 1
            st["peak_metric"] = max(float(st["peak_metric"]), float(st["cum_metric"]))
            st["max_drawdown"] = max(float(st["max_drawdown"]), float(st["peak_metric"]) - float(st["cum_metric"]))

            stop_reasons: list[str] = []
            if name != "baseline":
                if math.isfinite(survival_min_cum_pnl) and float(st["cum_metric"]) <= survival_min_cum_pnl:
                    stop_reasons.append(f"cum_{survival_metric}<={survival_min_cum_pnl:g}")
                if math.isfinite(survival_min_delta_pnl) and float(st["cum_delta_vs_baseline"]) <= survival_min_delta_pnl:
                    stop_reasons.append(f"cum_delta<={survival_min_delta_pnl:g}")
                if math.isfinite(survival_max_drawdown) and float(st["max_drawdown"]) >= survival_max_drawdown:
                    stop_reasons.append(f"max_drawdown>={survival_max_drawdown:g}")
            if stop_reasons and st["alive"]:
                st["alive"] = False
                st["stopped_day"] = day
                st["stop_reason"] = ";".join(stop_reasons)
                eliminated_rows.append(dict(st))

            state_rows.append(
                {
                    "day_index": effective_day_idx,
                    "day": day,
                    "arm": name,
                    "group": st["group"],
                    "daily_metric": metric,
                    "baseline_daily_metric": baseline_metric,
                    "daily_delta_vs_baseline": metric - baseline_metric,
                    "cum_metric": st["cum_metric"],
                    "cum_delta_vs_baseline": st["cum_delta_vs_baseline"],
                    "peak_metric": st["peak_metric"],
                    "max_drawdown": st["max_drawdown"],
                    "alive_after_day": bool(st["alive"]),
                    "stopped_day": st["stopped_day"],
                    "stop_reason": st["stop_reason"],
                    "replay_pnl_sum": _num(row, "replay_pnl_sum"),
                    "terminal_pnl_sum": _num(row, "terminal_pnl_sum"),
                    "replay_inv_adj_sum": _num(row, "replay_inv_adj_sum"),
                    "fills_total": _num(row, "fills_total"),
                    "campaigns": _num(row, "campaigns"),
                    "loss_tail": _num(row, "loss_tail"),
                    "decision_pause_rate": _num(row, "decision_pause_rate"),
                    "decision_keep_rate": _num(row, "decision_keep_rate"),
                    "replay_avg_final_spread": _num(row, "replay_avg_final_spread"),
                }
            )

        active_names = ["baseline"] + [name for name in active_names if name != "baseline" and state[name]["alive"]]
        pd.DataFrame(state_rows).to_csv(state_path, index=False)
        pd.DataFrame(eliminated_rows).to_csv(eliminated_path, index=False)
        write_arm_specs([by_name[name] for name in active_names], survivor_path)
        stopped_today = sum(1 for row in eliminated_rows if row.get("stopped_day") == day)
        alive_count = sum(1 for st in state.values() if st["alive"])
        print(
            f"[survival] {day}: alive={alive_count}/{len(state)} "
            f"stopped_today={stopped_today} state={state_path}",
            flush=True,
        )
        if all(not st["alive"] for name, st in state.items() if name != "baseline"):
            print(
                "[survival] all non-baseline arms eliminated; stopping without "
                "running baseline-only days.",
                flush=True,
            )
            break

    leaderboard = pd.DataFrame(state.values())
    if not leaderboard.empty:
        leaderboard = leaderboard.sort_values(["alive", "cum_metric", "cum_delta_vs_baseline"], ascending=[False, False, False])
    leaderboard.to_csv(leaderboard_path, index=False)
    print(f"Saved survival state: {state_path}")
    print(f"Saved eliminated arms: {eliminated_path}")
    print(f"Saved survivor arms: {survivor_path}")
    print(f"Saved survival leaderboard: {leaderboard_path}")
    cols = ["arm", "alive", "days_run", "cum_metric", "cum_delta_vs_baseline", "max_drawdown", "stopped_day", "stop_reason"]
    print(leaderboard[cols].head(30).to_string(index=False))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL)
    parser.add_argument("--config", type=Path, default=ROOT / "live" / "config.yaml")
    parser.add_argument("--tag", default="parameter_racing")
    parser.add_argument(
        "--stage",
        choices=("quick-smoke", "quick-full-main-effect", "retained39", "pnl-survival", "custom"),
        default="quick-smoke",
    )
    parser.add_argument("--days", nargs="*", default=None)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Good-day manifest used by --stage pnl-survival when --days is not provided.",
    )
    parser.add_argument("--survival-start-day", default="2026-01-01")
    parser.add_argument("--survival-end-day", default="2026-07-06")
    parser.add_argument(
        "--survival-extra-days",
        nargs="*",
        default=[],
        help="Explicit good/newly validated days to append to the manifest range, e.g. 2026-07-04 2026-07-05 2026-07-06.",
    )
    parser.add_argument(
        "--survival-metric",
        choices=("replay_pnl_sum", "terminal_pnl_sum"),
        default="replay_pnl_sum",
        help="Metric accumulated for pnl-survival stopping and ranking.",
    )
    parser.add_argument(
        "--survival-min-cum-pnl",
        type=float,
        default=-320.0,
        help="Stop non-baseline arms once cumulative survival metric falls below this value.",
    )
    parser.add_argument(
        "--survival-min-delta-pnl",
        type=float,
        default=-140.0,
        help="Stop non-baseline arms once cumulative metric minus baseline falls below this value.",
    )
    parser.add_argument(
        "--survival-max-drawdown",
        type=float,
        default=180.0,
        help="Stop non-baseline arms once max drawdown of the cumulative metric reaches this value.",
    )
    parser.add_argument(
        "--survival-resume-state-csv",
        type=Path,
        default=None,
        help=(
            "Resume pnl-survival from an existing survival_state.csv. The latest day "
            "in that file seeds cumulative PnL/drawdown, then current thresholds are "
            "re-applied before continuing with later days."
        ),
    )
    parser.add_argument("--groups", nargs="*", default=["spread", "guard", "cooldown", "lifecycle", "execution", "ml"])
    parser.add_argument(
        "--input-arm-spec-json",
        type=Path,
        default=None,
        help="Use an existing arm-spec JSON instead of generating one from parameter_selection.py.",
    )
    parser.add_argument("--single-factor", action="store_true", help="Generate one-factor arms for all selected groups.")
    parser.add_argument("--sample-method", choices=("sobol", "random"), default="sobol")
    parser.add_argument("--n-sampled", type=int, default=0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--live-active-sobol",
        action="store_true",
        help=(
            "Generate coupled live-active Sobol arms: p3_kappa_eff, quote/spread, "
            "guard, cooldown, and ML knobs with paired cap/base."
        ),
    )
    parser.add_argument(
        "--mechanism-local-sobol",
        action="store_true",
        help=(
            "Generate a narrower Sobol surface around raw-improving but mechanism-failed "
            "parents from --source-scored-rollup / --source-arm-spec-json."
        ),
    )
    parser.add_argument(
        "--source-scored-rollup",
        type=Path,
        default=None,
        help="Scored rollup from a previous broad smoke, used by --mechanism-local-sobol.",
    )
    parser.add_argument(
        "--source-arm-spec-json",
        type=Path,
        default=None,
        help="Arm spec JSON that matches --source-scored-rollup, used by --mechanism-local-sobol.",
    )
    parser.add_argument(
        "--local-raw-parents",
        type=int,
        default=12,
        help="How many raw-improving failed parents seed --mechanism-local-sobol.",
    )
    parser.add_argument(
        "--local-mechanism-parents",
        type=int,
        default=8,
        help="How many closest-mechanism failed parents seed --mechanism-local-sobol.",
    )
    parser.add_argument(
        "--max-arms",
        type=int,
        default=0,
        help=(
            "Optional arm cap. quick-smoke defaults to 30 when omitted; "
            "quick-full-main-effect and retained39 are uncapped unless this is set."
        ),
    )
    parser.add_argument(
        "--early-reject-after-days",
        type=int,
        default=0,
        help=(
            "Optional racing pass: run the first N days, keep hard-gate survivors, "
            "then run remaining days only for survivors. Writes probe/survivor outputs."
        ),
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--engine",
        choices=("python", "cpp"),
        default="python",
        help="Replay engine passed to campaign_outcome_replay_audit.py.",
    )
    parser.add_argument(
        "--no-cpp-baseline-parity-gate",
        action="store_true",
        help="Disable the baseline parity gate when --engine cpp is used.",
    )
    parser.add_argument(
        "--cpp-parity-max-fill-diff-rate",
        type=float,
        default=0.05,
        help="Forwarded to campaign_outcome_replay_audit.py when --engine cpp.",
    )
    parser.add_argument(
        "--cpp-parity-max-pnl-diff",
        type=float,
        default=5.0,
        help="Forwarded to campaign_outcome_replay_audit.py when --engine cpp.",
    )
    parser.add_argument(
        "--arm-chunk-size",
        type=int,
        default=0,
        help=(
            "Forwarded to campaign_outcome_replay_audit.py. "
            "Use 6-8 for wide smoke runs so partial results arrive by arm chunk "
            "instead of after a whole day finishes."
        ),
    )
    parser.add_argument(
        "--window-cache-dir",
        default=str(window_cache_root(ROOT)),
    )
    parser.add_argument(
        "--refresh-window-cache",
        action="store_true",
        help="Force campaign replay to rebuild tick-window caches before running this sweep.",
    )
    parser.add_argument(
        "--allow-demo-config",
        action="store_true",
        help="Allow --execute with the public demo config. Formal sweeps should use docs/private/ec2_live_config.yaml.",
    )
    parser.add_argument("--live-perf-telemetry", default="")
    parser.add_argument("--live-perf-latency-mode", choices=("avg", "max", "sum"), default="avg")
    parser.add_argument(
        "--strict-calibration",
        action="store_true",
        help="Require complete formal replay calibration before any arm is executed.",
    )
    parser.add_argument("--out-dir", type=Path, default=ROOT / "logs" / "parameter_racing")
    parser.add_argument(
        "--rescore-daily-csv",
        type=Path,
        default=None,
        help=(
            "Rebase an existing campaign daily CSV to --selection-baseline-arm, "
            "write canonical paired_screen_v2 evidence/ranking, and exit without replay."
        ),
    )
    parser.add_argument(
        "--selection-baseline-arm",
        default="baseline",
        help="Arm name treated as the rolling live baseline by --rescore-daily-csv.",
    )
    parser.add_argument(
        "--selection-output",
        type=Path,
        default=None,
        help="Optional output CSV for --rescore-daily-csv.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually run campaign replay. Default is dry-run/spec generation only.")
    args = parser.parse_args(argv)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    stage = _canonical_stage(args.stage)
    tag = f"{args.tag}_{stage}"
    if args.rescore_daily_csv is not None:
        selected = screen_paired_daily_arms(
            pd.read_csv(args.rescore_daily_csv),
            baseline_arm=str(args.selection_baseline_arm),
        )
        output = args.selection_output or args.out_dir / f"{tag}_paired_screen_v2.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        selected.to_csv(output, index=False)
        print(f"Saved {output}")
        display = [
            "arm",
            "screening_rank",
            "scorecard_ranking_score",
            "scorecard_gate_pass",
            "scorecard_candidate_class",
            "scorecard_economic_class",
            "behavior_class",
            "raw_delta_sum",
            "terminal_delta_sum",
            "inv_adj_delta_sum",
            "activity_adjusted_raw_delta",
            "campaign_adjusted_terminal_delta",
            "tail_campaign_delta",
            "fills_ratio",
            "inventory_time_ratio",
            "campaign_mae_ratio",
            "campaign_duration_ratio",
            "raw_win_rate",
            "terminal_win_rate",
            "joint_paired_t",
            "multiple_test_signal",
            "scorecard_gate_notes",
        ]
        print(selected[display].head(30).to_string(index=False))
        return
    if args.execute and not args.allow_demo_config and _is_demo_config(args.config):
        raise SystemExit(
            "Refusing to execute a parameter sweep with the public/demo config. "
            "Fetch the EC2 live config into docs/private/ec2_live_config.yaml and pass "
            "--config docs/private/ec2_live_config.yaml, or use --allow-demo-config for CI/demo only."
        )
    coverage_paths = write_coverage_report(args.config, args.out_dir / f"{tag}_coverage")
    arms: list[ArmSpec] = []
    if args.input_arm_spec_json is not None:
        arms = _read_arm_specs(args.input_arm_spec_json)
    elif args.mechanism_local_sobol:
        if args.n_sampled <= 0:
            raise SystemExit("--mechanism-local-sobol requires --n-sampled")
        if args.source_scored_rollup is None or args.source_arm_spec_json is None:
            raise SystemExit("--mechanism-local-sobol requires --source-scored-rollup and --source-arm-spec-json")
        source_scored = pd.read_csv(args.source_scored_rollup)
        source_arms = _read_arm_specs(args.source_arm_spec_json)
        baseline_hints = load_tick_base_params(
            symbol=args.symbol,
            config_path=args.config,
            require_historical_bbo=True,
            include_queue_calibration=False,
        )
        arms.extend(
            mechanism_local_sobol_arms(
                scored=source_scored,
                source_arms=source_arms,
                n=args.n_sampled,
                method=args.sample_method,
                seed=args.seed,
                n_raw_parents=args.local_raw_parents,
                n_mechanism_parents=args.local_mechanism_parents,
                baseline_hints=baseline_hints,
            )
        )
    elif args.live_active_sobol:
        if args.n_sampled <= 0:
            raise SystemExit("--live-active-sobol requires --n-sampled")
        arms.extend(live_active_sobol_arms(n=args.n_sampled, method=args.sample_method, seed=args.seed))
    else:
        if args.single_factor or args.n_sampled <= 0:
            arms.extend(single_factor_arms(groups=args.groups))
            arms.extend(composite_arms(groups=args.groups))
        if args.n_sampled > 0:
            if not arms:
                arms.append(ArmSpec("baseline", "baseline", {}, "Current live config baseline."))
            arms.extend(sampled_arms(n=args.n_sampled, groups=args.groups, method=args.sample_method, seed=args.seed))
            arms.extend(composite_arms(groups=args.groups))
    arms = _dedupe_arms(arms)
    max_arms = int(args.max_arms or 0)
    if max_arms <= 0 and stage == "quick-smoke":
        max_arms = DEFAULT_QUICK_SMOKE_MAX_ARMS
    if max_arms > 0:
        arms = _cap_arms_round_robin(arms, max_arms)
    arms_path = args.out_dir / f"{tag}_arms.json"
    write_arm_specs(arms, arms_path)
    if stage == "pnl-survival":
        days = _manifest_days(
            manifest=args.manifest,
            start_day=args.survival_start_day,
            end_day=args.survival_end_day,
            explicit_days=args.days,
            extra_days=args.survival_extra_days,
        )
    else:
        days = _stage_days(stage, args.days)
    plan_path = args.out_dir / f"{tag}_plan.json"
    _write_plan(
        plan_path,
        {
            "symbol": args.symbol,
            "config": str(args.config),
            "stage": stage,
            "days": days,
            "groups": args.groups,
            "n_arms": len(arms),
            "max_arms": max_arms,
            "mechanism_local_sobol": bool(args.mechanism_local_sobol),
            "source_scored_rollup": str(args.source_scored_rollup) if args.source_scored_rollup else "",
            "source_arm_spec_json": str(args.source_arm_spec_json) if args.source_arm_spec_json else "",
            "local_raw_parents": int(args.local_raw_parents),
            "local_mechanism_parents": int(args.local_mechanism_parents),
            "early_reject_after_days": int(args.early_reject_after_days or 0),
            "manifest": str(args.manifest) if args.manifest else "",
            "survival_start_day": args.survival_start_day,
            "survival_end_day": args.survival_end_day,
            "survival_extra_days": args.survival_extra_days,
            "survival_metric": args.survival_metric,
            "survival_min_cum_pnl": float(args.survival_min_cum_pnl),
            "survival_min_delta_pnl": float(args.survival_min_delta_pnl),
            "survival_max_drawdown": float(args.survival_max_drawdown),
            "survival_resume_state_csv": str(args.survival_resume_state_csv) if args.survival_resume_state_csv else "",
            "execute": bool(args.execute),
            "engine": args.engine,
            "cpp_baseline_parity_gate": bool(args.engine == "cpp" and not args.no_cpp_baseline_parity_gate),
            "cpp_parity_max_fill_diff_rate": float(args.cpp_parity_max_fill_diff_rate),
            "cpp_parity_max_pnl_diff": float(args.cpp_parity_max_pnl_diff),
            "strict_calibration": bool(args.strict_calibration),
            "coverage": coverage_paths,
            "arms_json": str(arms_path),
            "window_cache_dir": args.window_cache_dir,
            "refresh_window_cache": bool(args.refresh_window_cache),
            "selection_policy": (
                "pnl-survival; chronological good days; stop non-baseline arms on cumulative PnL/delta/drawdown thresholds"
                if stage == "pnl-survival"
                else "constraint-first; baseline-relative hard gates; no live restart from quick stage."
            ),
        },
    )
    print(f"Generated {len(arms)} arm specs")
    if not args.execute:
        print("Dry-run only. Add --execute to run campaign replay.")
        return

    if stage == "pnl-survival":
        _run_pnl_survival(
            symbol=args.symbol,
            config_path=args.config,
            days=days,
            arms_path=arms_path,
            arms=arms,
            tag=tag,
            out_dir=args.out_dir,
            workers=args.workers,
            window_cache_dir=args.window_cache_dir,
            live_perf_telemetry=args.live_perf_telemetry,
            live_perf_latency_mode=args.live_perf_latency_mode,
            arm_chunk_size=args.arm_chunk_size,
            refresh_window_cache=args.refresh_window_cache,
            engine=args.engine,
            no_cpp_baseline_parity_gate=args.no_cpp_baseline_parity_gate,
            cpp_parity_max_fill_diff_rate=args.cpp_parity_max_fill_diff_rate,
            cpp_parity_max_pnl_diff=args.cpp_parity_max_pnl_diff,
            strict_calibration=args.strict_calibration,
            survival_metric=args.survival_metric,
            survival_min_cum_pnl=args.survival_min_cum_pnl,
            survival_min_delta_pnl=args.survival_min_delta_pnl,
            survival_max_drawdown=args.survival_max_drawdown,
            survival_resume_state_csv=args.survival_resume_state_csv,
        )
        return

    early_n = int(args.early_reject_after_days or 0)
    if early_n > 0 and len(days) > early_n:
        probe_days = days[:early_n]
        remaining_days = days[early_n:]
        probe_tag = f"{tag}_probe"
        probe_rollup = _run_campaign_audit(
            symbol=args.symbol,
            config_path=args.config,
            days=probe_days,
            arms_path=arms_path,
            arms=arms,
            tag=probe_tag,
            workers=args.workers,
            window_cache_dir=args.window_cache_dir,
            live_perf_telemetry=args.live_perf_telemetry,
            live_perf_latency_mode=args.live_perf_latency_mode,
            arm_chunk_size=args.arm_chunk_size,
            refresh_window_cache=args.refresh_window_cache,
            engine=args.engine,
            no_cpp_baseline_parity_gate=args.no_cpp_baseline_parity_gate,
            cpp_parity_max_fill_diff_rate=args.cpp_parity_max_fill_diff_rate,
            cpp_parity_max_pnl_diff=args.cpp_parity_max_pnl_diff,
            strict_calibration=args.strict_calibration,
        )
        probe_scored = constraint_score_rollup(pd.read_csv(probe_rollup))
        probe_scored_path = args.out_dir / f"{probe_tag}_scored_rollup.csv"
        probe_scored.to_csv(probe_scored_path, index=False)
        keep_names = set(probe_scored.loc[probe_scored["hard_gate_pass"], "arm"].astype(str))
        keep_names.add("baseline")
        survivor_arms = [arm for arm in arms if arm.name in keep_names]
        survivor_path = args.out_dir / f"{tag}_survivor_arms.json"
        write_arm_specs(survivor_arms, survivor_path)
        print(
            f"Early reject kept {len(survivor_arms)}/{len(arms)} arms after "
            f"{len(probe_days)} day(s). Probe scored: {probe_scored_path}"
        )
        if not remaining_days:
            scored_path = args.out_dir / f"{tag}_scored_rollup.csv"
            probe_scored.to_csv(scored_path, index=False)
            print(f"Saved {scored_path}")
            print(probe_scored[["arm", "hard_gate_pass", "constraint_first_score", "constraint_notes"]].head(20).to_string(index=False))
            return
        rollup_path = _run_campaign_audit(
            symbol=args.symbol,
            config_path=args.config,
            days=remaining_days,
            arms_path=survivor_path,
            arms=survivor_arms,
            tag=f"{tag}_survivors",
            workers=args.workers,
            window_cache_dir=args.window_cache_dir,
            live_perf_telemetry=args.live_perf_telemetry,
            live_perf_latency_mode=args.live_perf_latency_mode,
            arm_chunk_size=args.arm_chunk_size,
            refresh_window_cache=args.refresh_window_cache,
            engine=args.engine,
            no_cpp_baseline_parity_gate=args.no_cpp_baseline_parity_gate,
            cpp_parity_max_fill_diff_rate=args.cpp_parity_max_fill_diff_rate,
            cpp_parity_max_pnl_diff=args.cpp_parity_max_pnl_diff,
            strict_calibration=args.strict_calibration,
        )
    else:
        rollup_path = _run_campaign_audit(
            symbol=args.symbol,
            config_path=args.config,
            days=days,
            arms_path=arms_path,
            arms=arms,
            tag=tag,
            workers=args.workers,
            window_cache_dir=args.window_cache_dir,
            live_perf_telemetry=args.live_perf_telemetry,
            live_perf_latency_mode=args.live_perf_latency_mode,
            arm_chunk_size=args.arm_chunk_size,
            refresh_window_cache=args.refresh_window_cache,
            engine=args.engine,
            no_cpp_baseline_parity_gate=args.no_cpp_baseline_parity_gate,
            cpp_parity_max_fill_diff_rate=args.cpp_parity_max_fill_diff_rate,
            cpp_parity_max_pnl_diff=args.cpp_parity_max_pnl_diff,
            strict_calibration=args.strict_calibration,
        )
    scored = constraint_score_rollup(pd.read_csv(rollup_path))
    scored_path = args.out_dir / f"{tag}_scored_rollup.csv"
    scored.to_csv(scored_path, index=False)
    print(f"Saved {scored_path}")
    print(scored[["arm", "hard_gate_pass", "constraint_first_score", "constraint_notes"]].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
