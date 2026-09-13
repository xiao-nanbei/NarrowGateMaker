#!/usr/bin/env python3
"""Freeze Development-only live-stack mechanics for variance-time rearm.

This identity never reads reward, PnL, markout, Validation, or holdout. It
turns the predecessor's boolean blocker placeholders into hash-bound evidence
cells and keeps offline replay authority separate from AWS receive-time
transport and live authorization.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_feasibility as v1,
)
from strategy.dynamic_fill_hazard_model import (
    DynamicFillHazardActionPolicy,
    DynamicFillHazardBundle,
)
from strategy.replay_controls import load_sync_degrade_events

ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "volatility_time_add_rearm_live_stack_parity.v1"
FAMILY_ID = "volatility_time_add_rearm_live_stack_parity_v1"
SYNC_TRIGGER_TOKEN = "SYNC_ADJUST_DEGRADE:"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def git_workspace_identity(root: Path) -> dict[str, Any]:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "status", "--porcelain=v1"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    tracked_patch = subprocess.run(
        ["git", "diff", "--binary", "--no-ext-diff", "--"],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    return {
        "head": head,
        "dirty": bool(status.strip()),
        "porcelain_status_sha256": hashlib.sha256(
            status.encode("utf-8")
        ).hexdigest(),
        "tracked_binary_diff_sha256": hashlib.sha256(tracked_patch).hexdigest(),
        "untracked_path_list_sha256": hashlib.sha256(untracked).hexdigest(),
    }


def previous_day(day: str) -> str:
    value = datetime.strptime(day, "%Y-%m-%d").date() - timedelta(days=1)
    return value.isoformat()


def build_bbo_source_manifest(
    normalized_root: Path,
    daily_quality_path: Path,
    days: list[str],
) -> list[dict[str, Any]]:
    """Hash every target and D-1 BBO file actually read by the clock loader."""

    quality = pd.read_csv(daily_quality_path, dtype={"day": str}).set_index(
        "day", drop=False
    )
    consumers: dict[tuple[str, str], set[str]] = defaultdict(set)
    for day in days:
        consumers[(previous_day(day), "warmup_d_minus_1")].add(day)
        consumers[(day, "target_day")].add(day)

    rows: list[dict[str, Any]] = []
    for (source_day, role), used_by in sorted(consumers.items()):
        path = normalized_root / "bbo" / f"BTCUSDC-bbo-{source_day}.parquet"
        if not path.is_file():
            raise FileNotFoundError(
                f"required {role} normalized BBO is missing: {path}"
            )
        if source_day not in quality.index:
            raise ValueError(f"daily-quality identity lacks BBO day {source_day}")
        row = quality.loc[source_day]
        actual = sha256_file(path)
        expected = str(row["bbo_sha256"])
        if actual != expected:
            raise ValueError(f"normalized BBO hash mismatch for {source_day}")
        if role == "target_day" and not bool(row["formal_eligible"]):
            raise ValueError(f"target BBO day is not formal-eligible: {source_day}")
        rows.append(
            {
                "source_day": source_day,
                "role": role,
                "used_by_days": sorted(used_by),
                "path": str(path),
                "sha256": actual,
                "formal_eligible": bool(row["formal_eligible"]),
                "coverage": float(row["bbo_coverage"]),
                "p99_gap_s": float(row["bbo_p99_gap_s"]),
            }
        )
    return rows


def annotate_reconstructible_lineage(
    events: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Keep only state after two within-day side transitions.

    At a fresh day boundary both side counters are unknown. The first side
    transition makes only the opposite counter known-zero. The second transition
    makes both counters reconstructible without inventing a previous-day state.
    """

    parts: list[pd.DataFrame] = []
    summaries: list[dict[str, Any]] = []
    for day, daily in events.groupby("day", sort=True):
        frame = daily.sort_values(["fill_ts", "order_id"], kind="stable").copy()
        last_side = ""
        transitions = 0
        anchor_position: int | None = None
        for position, side in enumerate(frame["side"].astype(str).str.upper()):
            if last_side and side != last_side:
                transitions += 1
                if transitions == 2:
                    anchor_position = position
            last_side = side
        flags = [
            anchor_position is not None and position >= anchor_position
            for position in range(len(frame))
        ]
        frame["cooldown_lineage_reconstructible"] = flags
        anchor_ts = (
            int(round(float(frame.iloc[anchor_position]["fill_ts"])))
            if anchor_position is not None
            else 0
        )
        frame["cooldown_lineage_anchor_ts_ms"] = anchor_ts
        frame["cooldown_lineage_transition_count"] = transitions
        parts.append(frame)
        summaries.append(
            {
                "day": str(day),
                "fill_rows": int(len(frame)),
                "side_transitions": int(transitions),
                "anchor_ts_ms": anchor_ts,
                "reconstructible_fill_rows": int(sum(flags)),
                "left_censored_fill_rows": int(len(frame) - sum(flags)),
                "lineage_supported": bool(anchor_position is not None),
            }
        )
    annotated = pd.concat(parts, ignore_index=True) if parts else events.copy()
    return annotated, pd.DataFrame(summaries)


def read_junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    cases: list[str] = []
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for suite in suites:
        for key in totals:
            totals[key] += int(suite.attrib.get(key, 0) or 0)
        for case in suite.findall("testcase"):
            cases.append(str(case.attrib.get("name", "")))
    return {
        **totals,
        "test_names": sorted(set(cases)),
        "passed": totals["tests"] > 0
        and totals["failures"] == 0
        and totals["errors"] == 0,
    }


def validate_sync_tape_source(
    tape_path: Path,
    source_log_path: Path,
    *,
    expected_tape_sha256: str,
    expected_source_sha256: str,
    environment: str,
) -> dict[str, Any]:
    if sha256_file(tape_path) != expected_tape_sha256:
        raise ValueError("sync-degrade tape hash mismatch")
    if sha256_file(source_log_path) != expected_source_sha256:
        raise ValueError("sync-degrade source log hash mismatch")
    loaded = load_sync_degrade_events(
        mode="frozen_tape",
        tape_path=tape_path,
        expected_sha256=expected_tape_sha256,
        expected_environment=environment,
    )
    text = source_log_path.read_text(encoding="utf-8", errors="replace")
    observed_trigger_rows = len(re.findall(re.escape(SYNC_TRIGGER_TOKEN), text))
    if observed_trigger_rows != int(loaded.timestamps_ms.size):
        raise ValueError(
            "sync-degrade tape event count differs from the bound live log"
        )
    timestamps = re.findall(r"(?m)^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", text)
    if not timestamps:
        raise ValueError("sync-degrade source log has no parseable timestamps")
    first_log_ts_ms = int(
        datetime.strptime(timestamps[0], "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1_000
    )
    last_log_ts_ms = int(
        datetime.strptime(timestamps[-1], "%Y-%m-%d %H:%M:%S")
        .replace(tzinfo=timezone.utc)
        .timestamp()
        * 1_000
    )
    if (
        first_log_ts_ms > int(loaded.coverage_start_ts_ms)
        or last_log_ts_ms < int(loaded.coverage_end_ts_ms)
    ):
        raise ValueError("sync-degrade source log does not cover the frozen tape window")
    return {
        "path": str(tape_path),
        "sha256": expected_tape_sha256,
        "source_log_path": str(source_log_path),
        "source_log_sha256": expected_source_sha256,
        "event_count": int(loaded.timestamps_ms.size),
        "coverage_start_ts_ms": int(loaded.coverage_start_ts_ms),
        "coverage_end_ts_ms": int(loaded.coverage_end_ts_ms),
        "source_log_first_ts_ms": first_log_ts_ms,
        "source_log_last_ts_ms": last_log_ts_ms,
        "environment": str(loaded.environment),
        "promotion_eligible": bool(loaded.promotion_eligible),
        "trigger_path_observed": bool(loaded.timestamps_ms.size > 0),
    }


def require_identity(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    if sha256_file(path) != str(expected_sha256):
        raise ValueError(f"{label} hash mismatch: {path}")


def require_config_values(
    payload: dict[str, Any], expected_values: dict[str, Any]
) -> None:
    for dotted_key, expected in expected_values.items():
        value: Any = payload
        for key in dotted_key.split("."):
            if not isinstance(value, dict) or key not in value:
                raise ValueError(f"operational config lacks {dotted_key}")
            value = value[key]
        if value != expected:
            raise ValueError(
                f"operational config {dotted_key} changed: "
                f"expected {expected!r}, found {value!r}"
            )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected live-stack parity spec schema")
    permissions = spec["permissions"]
    if any(
        bool(permissions[key])
        for key in (
            "reward_or_pnl_read",
            "markout_read",
            "validation_read",
            "sealed_holdout_read",
            "action_experiment_authorized",
            "live_deployment_authorized",
        )
    ):
        raise ValueError("live-stack parity identity must remain mechanics-only")

    panels = spec["panels"]
    require_identity(
        Path(panels["source_split_path"]).resolve(),
        panels["source_split_sha256"],
        "frozen source split",
    )

    config_identity = spec["operational_config_identity"]
    config_path = Path(config_identity["path"]).resolve()
    require_identity(
        config_path,
        config_identity["sha256"],
        "operational config",
    )
    config_payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(config_payload, dict):
        raise ValueError("operational config root must be a mapping")
    require_config_values(config_payload, config_identity["expected_values"])

    implementation = spec["implementation_identity"]
    require_identity(
        Path(__file__).resolve(), implementation["evaluator_sha256"], "evaluator"
    )
    for relative, expected in implementation["source_sha256"].items():
        require_identity(ROOT / relative, expected, relative)
    native_module_path = Path(implementation["native_module_path"]).resolve()
    require_identity(
        native_module_path,
        implementation["native_module_sha256"],
        "frozen C++ native module",
    )
    require_identity(
        Path(implementation["predecessor_blocker_contract_path"]).resolve(),
        implementation["predecessor_blocker_contract_sha256"],
        "path-dependent blocker contract",
    )

    predecessor = spec["predecessor_identity"]
    predecessor_report_path = Path(predecessor["report_path"]).resolve()
    predecessor_manifest_path = Path(predecessor["manifest_path"]).resolve()
    require_identity(
        predecessor_report_path,
        predecessor["report_sha256"],
        "v2.1 report",
    )
    require_identity(
        predecessor_manifest_path,
        predecessor["manifest_sha256"],
        "v2.1 manifest",
    )
    predecessor_report = json.loads(
        predecessor_report_path.read_text(encoding="utf-8")
    )
    if not predecessor_report.get("variance_clock_mechanics_passed", False):
        raise ValueError("predecessor variance clock did not pass")
    evaluated_path = Path(
        predecessor_report["artifacts"]["evaluation_episodes"]
    ).resolve()
    require_identity(
        evaluated_path,
        predecessor["evaluation_episodes_sha256"],
        "v2.1 evaluation episodes",
    )
    evaluated = pd.read_parquet(
        evaluated_path,
        columns=["side", "ready_delay_ms", "timing_delta_s", "cpp_variance_clock_match"],
    )
    variance_kernel_supported = bool(evaluated["cpp_variance_clock_match"].all())

    source = spec["source_identity"]
    fill_trace_path = Path(source["fill_trace_path"]).resolve()
    quality_path = Path(source["normalized_l2_quality_path"]).resolve()
    normalized_root = Path(source["normalized_l2_root"]).resolve()
    require_identity(fill_trace_path, source["fill_trace_sha256"], "fill trace")
    require_identity(
        quality_path,
        source["normalized_l2_quality_sha256"],
        "normalized L2 quality",
    )
    days = [str(day) for day in panels["development_days"]]
    bbo_manifest = build_bbo_source_manifest(normalized_root, quality_path, days)
    bbo_manifest_hash = canonical_sha256(bbo_manifest)
    if bbo_manifest_hash != source["bbo_source_manifest_sha256"]:
        raise ValueError("target plus D-1 BBO source manifest changed")

    events = v1.load_fill_events(fill_trace_path, days)
    annotated, lineage_daily = annotate_reconstructible_lineage(events)
    safe_events = annotated[annotated["cooldown_lineage_reconstructible"]].copy()
    mechanics = spec["lineage_contract"]
    safe_episodes = v1.build_fill_unit_episodes(
        safe_events,
        order_size_btc=float(mechanics["order_size_btc"]),
        lot_size_btc=float(mechanics["lot_size_btc"]),
    )
    reconstructible_rate = float(len(safe_events) / max(len(events), 1))
    daily_lineage_supported = bool(
        reconstructible_rate >= float(mechanics["minimum_reconstructible_fill_rate"])
        and lineage_daily["lineage_supported"].all()
    )

    test_identity = spec["test_identity"]
    junit_path = Path(test_identity["junit_xml_path"]).resolve()
    require_identity(junit_path, test_identity["junit_xml_sha256"], "JUnit evidence")
    junit = read_junit(junit_path)
    missing_tests = sorted(
        set(test_identity["required_test_names"]) - set(junit["test_names"])
    )
    if not junit["passed"] or missing_tests:
        raise ValueError(f"parity JUnit evidence is incomplete: {missing_tests}")

    q90 = spec["buy_q90_identity"]
    model_path = Path(q90["model_path"]).resolve()
    policy_path = Path(q90["policy_path"]).resolve()
    bundle = DynamicFillHazardBundle.load(
        model_path,
        expected_file_sha256=q90["model_sha256"],
        shadow_sides=("BUY",),
    )
    policy = DynamicFillHazardActionPolicy.load(
        policy_path,
        expected_file_sha256=q90["policy_sha256"],
        model_bundle=bundle,
    )

    sync = spec["sync_degrade_identity"]
    sync_evidence = validate_sync_tape_source(
        Path(sync["event_tape_path"]).resolve(),
        Path(sync["source_log_path"]).resolve(),
        expected_tape_sha256=sync["event_tape_sha256"],
        expected_source_sha256=sync["source_log_sha256"],
        environment=sync["environment"],
    )

    latency_rows: list[dict[str, Any]] = []
    for entry in spec["latency_identity"]["artifacts"]:
        path = Path(entry["path"]).resolve()
        require_identity(path, entry["sha256"], "latency artifact")
        latency_rows.append({"path": str(path), "sha256": entry["sha256"]})

    parity_cells = [
        {
            "control": "variance_time_clock_integrator",
            "python_supported": True,
            "cpp_supported": variance_kernel_supported,
            "input_bound": True,
            "parity_passed": variance_kernel_supported,
            "failure_reason": "",
        },
        {
            "control": "same_side_reducing_fill_deadline",
            "python_supported": True,
            "cpp_supported": True,
            "input_bound": True,
            "parity_passed": True,
            "failure_reason": "",
        },
        {
            "control": "consecutive_loss_cooldown",
            "python_supported": True,
            "cpp_supported": True,
            "input_bound": True,
            "parity_passed": True,
            "failure_reason": "",
        },
        {
            "control": "sync_degrade_frozen_tape",
            "python_supported": True,
            "cpp_supported": True,
            "input_bound": True,
            "parity_passed": True,
            "failure_reason": (
                "bound full-day tape contains no trigger; trigger behavior is "
                "covered by deterministic synthetic parity only"
                if not sync_evidence["trigger_path_observed"]
                else ""
            ),
        },
        {
            "control": "buy_q90_cancel_ack_recovery_reentry",
            "python_supported": True,
            "cpp_supported": False,
            "input_bound": True,
            "parity_passed": False,
            "failure_reason": (
                "C++ replay has no strategy-independent native snapshot/delta "
                "scheduler or exact-level path scorer and fails closed"
            ),
        },
        {
            "control": "variance_time_full_strategy_candidate_path",
            "python_supported": False,
            "cpp_supported": False,
            "input_bound": True,
            "parity_passed": False,
            "failure_reason": (
                "v2.1 integrates isolated baseline episodes; candidate rearm does "
                "not yet regenerate downstream orders, fills, inventory, and blockers"
            ),
        },
    ]
    full_stack_parity = all(bool(row["parity_passed"]) for row in parity_cells)
    binding_blockers = [
        row["control"] for row in parity_cells if not bool(row["parity_passed"])
    ]
    aws_transport_supported = False
    offline_action_replay_supported = bool(
        full_stack_parity and daily_lineage_supported
    )

    output.mkdir(parents=True, exist_ok=False)
    bbo_manifest_path = output / "bbo_target_and_dminus1_manifest.json"
    lineage_path = output / "daily_cooldown_lineage.csv"
    parity_path = output / "blocker_parity_cells.csv"
    report_path = output / "report.json"
    markdown_path = output / "report.md"
    bbo_manifest_path.write_text(
        json.dumps(bbo_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lineage_daily.to_csv(lineage_path, index=False)
    pd.DataFrame(parity_cells).to_csv(parity_path, index=False)

    mechanical_rates = (
        evaluated.assign(
            material_timing_change=evaluated["timing_delta_s"].abs()
            > float(spec["mechanical_timing_contract"]["material_delta_s"])
        )
        .groupby(["ready_delay_ms", "side"], as_index=False)[
            "material_timing_change"
        ]
        .mean()
        .rename(columns={"material_timing_change": "mechanical_effective_rate"})
        .to_dict("records")
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": "hold_action_candidate_path_and_cpp_q90_parity_incomplete",
        "variance_clock_mechanics_supported": True,
        "full_live_stack_parity_supported": full_stack_parity,
        "offline_action_replay_supported": offline_action_replay_supported,
        "aws_receive_time_transport_supported": aws_transport_supported,
        "live_authorization_supported": False,
        "unmasked_action_effective_rate": None,
        "unmasked_action_effective_rate_reason": (
            "undefined until candidate timing is integrated into the full strategy path"
        ),
        "mechanical_effective_rates": mechanical_rates,
        "binding_blockers": binding_blockers,
        "parity_cells": parity_cells,
        "lineage": {
            "source_fill_rows": int(len(events)),
            "reconstructible_fill_rows": int(len(safe_events)),
            "reconstructible_fill_rate": reconstructible_rate,
            "reconstructible_episodes": int(len(safe_episodes)),
            "daily_fresh_start_lineage_supported": daily_lineage_supported,
            "continuous_live_lineage_supported": False,
            "continuous_live_failure_reason": (
                "source replay and evaluator reset at UTC day boundaries; no restored "
                "cross-day consecutive units/cooldown deadline are available"
            ),
        },
        "buy_q90": {
            "model_path": str(model_path),
            "model_sha256": bundle.file_sha256,
            "model_family_id": bundle.family_id,
            "policy_path": str(policy_path),
            "policy_sha256": policy.file_sha256,
            "policy_id": policy.policy_id,
        },
        "sync_degrade": sync_evidence,
        "latency_artifacts": latency_rows,
        "operational_config": {
            "path": str(config_path),
            "sha256": sha256_file(config_path),
            "expected_values": config_identity["expected_values"],
        },
        "source_split": {
            "path": str(Path(panels["source_split_path"]).resolve()),
            "sha256": panels["source_split_sha256"],
        },
        "bbo_source_manifest": {
            "path": str(bbo_manifest_path),
            "canonical_sha256": bbo_manifest_hash,
            "rows": int(len(bbo_manifest)),
            "target_rows": int(
                sum(row["role"] == "target_day" for row in bbo_manifest)
            ),
            "warmup_rows": int(
                sum(row["role"] == "warmup_d_minus_1" for row in bbo_manifest)
            ),
        },
        "test_evidence": {
            "path": str(junit_path),
            "sha256": sha256_file(junit_path),
            **junit,
        },
        "native_module": {
            "path": str(native_module_path),
            "sha256": sha256_file(native_module_path),
        },
        "panels": {
            "development_days": days,
            "validation_days_read": [],
            "sealed_holdout_days_read": [],
        },
        "permissions": {
            "reward_or_pnl_read": False,
            "markout_read": False,
            "validation_read": False,
            "sealed_holdout_read": False,
            "action_experiment_created": False,
            "action_experiment_authorized": False,
            "live_deployment_authorized": False,
        },
        "spec": {"path": str(spec_path), "sha256": sha256_file(spec_path)},
        "predecessor": {
            "report_path": str(predecessor_report_path),
            "report_sha256": sha256_file(predecessor_report_path),
        },
        "artifacts": {
            "bbo_manifest": str(bbo_manifest_path),
            "lineage_daily": str(lineage_path),
            "parity_cells": str(parity_path),
        },
        "workspace": git_workspace_identity(ROOT),
    }
    report["report_payload_sha256"] = canonical_sha256(report)
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    lines = [
        "# Volatility-Time Add Rearm Live-Stack Parity v1",
        "",
        "Development-only mechanics/parity identity. No reward, PnL, markout, Validation, or holdout was read.",
        "",
        f"- decision: `{report['decision']}`",
        f"- variance-clock mechanics supported: `{report['variance_clock_mechanics_supported']}`",
        f"- full live-stack parity supported: `{report['full_live_stack_parity_supported']}`",
        f"- offline action replay supported: `{report['offline_action_replay_supported']}`",
        f"- AWS receive-time transport supported: `{report['aws_receive_time_transport_supported']}`",
        f"- unmasked action-effective rate: `{report['unmasked_action_effective_rate']}`",
        f"- binding blockers: `{binding_blockers}`",
        "",
        "The predecessor's 61%-70% rate is retained only as a mechanical timing-change diagnostic.",
        "It is not an unmasked quote-action rate.",
    ]
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    manifest = {
        "schema_version": "volatility_time_add_rearm_live_stack_parity_manifest.v1",
        "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        "artifacts": [
            {"path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size}
            for path in (bbo_manifest_path, lineage_path, parity_path, markdown_path)
        ],
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "output_dir": str(output),
                "decision": report["decision"],
                "binding_blockers": binding_blockers,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
