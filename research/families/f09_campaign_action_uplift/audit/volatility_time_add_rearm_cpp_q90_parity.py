#!/usr/bin/env python3
"""Development-only native-book and BUY q90 Python/C++ parity evidence."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from research.families.f09_campaign_action_uplift.audit import (
    volatility_time_add_rearm_full_path_preflight as full_path,
)


ROOT = Path(__file__).resolve().parents[4]
SCHEMA_VERSION = "volatility_time_add_rearm_cpp_q90_parity.v1"
FAMILY_ID = "volatility_time_add_rearm_cpp_q90_parity_v1"
ARMS = ("control_wall_time", "candidate_variance_time")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_identity(path: Path, expected_sha256: str, label: str) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"{label} is missing: {path}")
    actual = sha256_file(path)
    if actual != str(expected_sha256):
        raise ValueError(
            f"{label} hash mismatch: expected {expected_sha256}, found {actual}"
        )


def _load_spec(path: Path) -> dict[str, Any]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unexpected C++ q90 parity spec schema")
    permissions = spec.get("permissions") or {}
    forbidden = (
        "reward_or_pnl_read",
        "markout_read",
        "validation_read",
        "sealed_holdout_read",
        "randomized_action_identity_created",
        "action_experiment_authorized",
        "live_deployment_authorized",
    )
    enabled = [key for key in forbidden if bool(permissions.get(key, False))]
    if enabled:
        raise ValueError(
            "C++ q90 parity must remain mechanics-only: " + ", ".join(enabled)
        )
    days = [str(day) for day in spec["panels"]["parity_days"]]
    development = set(str(day) for day in spec["panels"]["development_days"])
    if not days or not set(days).issubset(development):
        raise ValueError("parity days must be a non-empty Development subset")
    if tuple(spec["replay_contract"]["arms"]) != ARMS:
        raise ValueError("C++ q90 parity requires both frozen full-path arms")
    predecessor_spec = Path(
        spec["predecessor_identity"]["spec"]["path"]
    ).expanduser()
    spec["full_path_contract"] = json.loads(
        predecessor_spec.read_text(encoding="utf-8")
    )
    return spec


def _read_junit(path: Path) -> dict[str, Any]:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    names: set[str] = set()
    for suite in suites:
        for key in totals:
            totals[key] += int(suite.attrib.get(key, 0) or 0)
        names.update(
            str(case.attrib.get("name", ""))
            for case in suite.findall("testcase")
        )
    return {
        **totals,
        "test_names": sorted(names),
        "passed": bool(
            totals["tests"] > 0
            and totals["failures"] == 0
            and totals["errors"] == 0
        ),
    }


def _validate_identities(
    spec: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    implementation = spec["implementation_identity"]
    require_identity(Path(__file__).resolve(), implementation["evaluator_sha256"], "evaluator")
    for relative, expected in implementation["source_sha256"].items():
        require_identity(ROOT / relative, expected, relative)
    for section, label in (
        (spec["predecessor_identity"]["spec"], "full-path predecessor spec"),
        (spec["predecessor_identity"]["report"], "full-path predecessor report"),
        (spec["buy_q90_identity"]["model"], "BUY q90 model"),
        (spec["buy_q90_identity"]["policy"], "BUY q90 policy"),
        (spec["native_module_identity"], "native module"),
        (spec["test_identity"]["junit_xml"], "parity contract JUnit"),
    ):
        require_identity(Path(section["path"]), section["sha256"], label)
    junit = _read_junit(Path(spec["test_identity"]["junit_xml"]["path"]))
    missing = sorted(
        set(spec["test_identity"]["required_test_names"])
        - set(junit["test_names"])
    )
    if not junit["passed"] or missing:
        raise ValueError(f"C++ q90 contract tests are incomplete: {missing}")
    contract = spec["full_path_contract"]
    for section, label in (
        (contract["operational_config_identity"], "operational config"),
        (contract["execution_trade_identity"]["manifest"], "trade manifest"),
        (
            contract["execution_trade_identity"]["quality_report"],
            "trade quality report",
        ),
        (contract["source_identity"]["normalized_l2_manifest"], "normalized L2 manifest"),
        (contract["source_identity"]["normalized_l2_quality"], "normalized L2 quality"),
        (contract["source_identity"]["queue_calibration"], "queue calibration"),
        (contract["source_identity"]["p3_artifact"], "P3 artifact"),
        (contract["latency_identity"]["samples"], "latency samples"),
        (contract["panels"]["source_split"], "source split"),
    ):
        require_identity(Path(section["path"]), section["sha256"], label)
    source_contract = copy.deepcopy(contract)
    source_contract["panels"]["development_days"] = list(
        spec["panels"]["parity_days"]
    )
    market_manifest = full_path.build_market_source_manifest(source_contract)
    actual_manifest_sha256 = full_path.canonical_sha256(market_manifest)
    expected_manifest_sha256 = str(
        spec["source_identity"]["market_source_manifest_canonical_sha256"]
    )
    if actual_manifest_sha256 != expected_manifest_sha256:
        raise ValueError(
            "C++ q90 parity market source identity changed: expected "
            f"{expected_manifest_sha256}, found {actual_manifest_sha256}"
        )
    return junit, market_manifest


def _parity_result(result: Mapping[str, Any]) -> dict[str, Any]:
    """Read only mechanics/parity fields from the authoritative replay."""

    identity = dict(result.get("dynamic_fill_hazard_cpp_identity") or {})
    return {
        "parity_passed": bool(
            result.get("dynamic_fill_hazard_cpp_parity_passed", False)
        ),
        "mismatch_count": int(
            result.get("dynamic_fill_hazard_cpp_mismatch_count", 0) or 0
        ),
        "mismatch_by_stage": dict(
            result.get("dynamic_fill_hazard_cpp_mismatch_by_stage") or {}
        ),
        "book_event_count": int(
            result.get("dynamic_fill_hazard_cpp_book_event_count", 0) or 0
        ),
        "activation_count": int(
            result.get("dynamic_fill_hazard_cpp_activation_count", 0) or 0
        ),
        "evaluation_call_count": int(
            result.get("dynamic_fill_hazard_cpp_evaluation_count", 0) or 0
        ),
        "lifecycle_call_count": int(
            result.get("dynamic_fill_hazard_cpp_lifecycle_count", 0) or 0
        ),
        "cancel_request_count": int(
            result.get("dynamic_fill_hazard_cancel_request_count", 0) or 0
        ),
        "cancel_ack_count": int(
            result.get("dynamic_fill_hazard_cancel_ack_count", 0) or 0
        ),
        "pre_ack_fill_count": int(
            result.get("dynamic_fill_hazard_pre_ack_fill_count", 0) or 0
        ),
        "recovery_count": int(
            result.get("dynamic_fill_hazard_recovery_count", 0) or 0
        ),
        "reentry_count": int(
            result.get("dynamic_fill_hazard_reentry_count", 0) or 0
        ),
        "cpp_counters": dict(
            result.get("dynamic_fill_hazard_cpp_counters") or {}
        ),
        "cpp_sequence_stats": dict(
            result.get("dynamic_fill_hazard_cpp_sequence_stats") or {}
        ),
        "native_module_sha256": str(
            identity.get("native_module_sha256", "")
        ),
        "model_file_sha256": str(identity.get("model_file_sha256", "")),
        "policy_file_sha256": str(identity.get("policy_file_sha256", "")),
        "abi_version": str(identity.get("abi_version", "")),
        "scope": str(identity.get("scope", "")),
        "full_cpp_tick_replay_authority": bool(
            result.get(
                "dynamic_fill_hazard_full_cpp_tick_replay_authority",
                True,
            )
        ),
    }


def run_day(spec: Mapping[str, Any], day: str) -> dict[str, Any]:
    started = time.monotonic()
    params = full_path._configure_params(spec["full_path_contract"], day)
    params.update(
        {
            "dynamic_fill_hazard_cpp_parity_enabled": True,
            "dynamic_fill_hazard_cpp_parity_strict": True,
            "dynamic_fill_hazard_cpp_parity_trace_max": int(
                spec["replay_contract"]["mismatch_trace_max"]
            ),
        }
    )
    window = full_path._load_window(spec["full_path_contract"], day, params)
    variance = full_path._variance_time_data(spec["full_path_contract"], day)
    rows: list[dict[str, Any]] = []
    for arm in ARMS:
        candidate = arm == "candidate_variance_time"
        result = full_path._run_arm(
            spec["full_path_contract"],
            day,
            window,
            params,
            candidate=candidate,
            variance_data=variance if candidate else None,
        )
        rows.append({"day": day, "arm": arm, **_parity_result(result)})
    return {
        "day": day,
        "runtime_s": float(time.monotonic() - started),
        "rows": rows,
    }


def _decision(rows: pd.DataFrame, spec: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    gates = spec["gates"]
    actual_arms = set(rows["arm"].astype(str)) if not rows.empty else set()
    totals = {
        key: int(pd.to_numeric(rows[key], errors="coerce").fillna(0).sum())
        for key in (
            "book_event_count",
            "activation_count",
            "evaluation_call_count",
            "lifecycle_call_count",
            "cancel_request_count",
            "cancel_ack_count",
            "pre_ack_fill_count",
            "recovery_count",
            "reentry_count",
            "mismatch_count",
        )
    }
    passed = bool(
        len(rows) == len(spec["panels"]["parity_days"]) * len(ARMS)
        and actual_arms == set(ARMS)
        and rows["parity_passed"].astype(bool).all()
        and not rows["full_cpp_tick_replay_authority"].astype(bool).any()
        and totals["mismatch_count"] == 0
        and totals["book_event_count"]
        >= int(gates["minimum_native_book_events"])
        and totals["activation_count"] >= int(gates["minimum_activations"])
        and totals["evaluation_call_count"]
        >= int(gates["minimum_evaluation_calls"])
        and totals["cancel_request_count"]
        >= int(gates["minimum_cancel_requests"])
        and totals["cancel_ack_count"] >= int(gates["minimum_cancel_acks"])
        and totals["recovery_count"] >= int(gates["minimum_recoveries"])
        and totals["reentry_count"] >= int(gates["minimum_reentries"])
    )
    decision = (
        "cpp_q90_native_parity_pass_register_randomized_replay_identity"
        if passed
        else "cpp_q90_native_parity_failed_keep_action_identity_blocked"
    )
    return decision, {
        "parity_gate_passed": passed,
        "totals": totals,
        "actual_arms": sorted(actual_arms),
        "historical_development_pre_ack_fill_count": int(
            spec["mechanism_coverage"]["historical_development_pre_ack_fill_count"]
        ),
        "pre_ack_fill_covered_by_synthetic_contract_test": bool(
            spec["mechanism_coverage"]["pre_ack_fill_synthetic_test"]
        ),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.expanduser().resolve()
    output = args.output_dir.expanduser().resolve()
    spec = _load_spec(spec_path)
    junit, market_source_manifest = _validate_identities(spec)
    if output.exists() and not args.resume:
        raise FileExistsError(f"output directory already exists: {output}")
    output.mkdir(parents=True, exist_ok=True)
    checkpoint_dir = output / "day_checkpoints"
    checkpoint_dir.mkdir(exist_ok=True)
    spec_sha256 = sha256_file(spec_path)
    market_manifest_path = output / "market_source_manifest.json"
    market_manifest_path.write_text(
        json.dumps(market_source_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    results: list[dict[str, Any]] = []
    pending: list[str] = []
    for day in spec["panels"]["parity_days"]:
        checkpoint = checkpoint_dir / f"{day}.json"
        if args.resume and checkpoint.is_file():
            payload = json.loads(checkpoint.read_text(encoding="utf-8"))
            if payload.get("spec_sha256") != spec_sha256:
                raise ValueError(f"checkpoint spec identity mismatch: {checkpoint}")
            results.append(payload["result"])
        else:
            pending.append(str(day))

    workers = max(1, min(int(args.workers), len(pending) or 1))
    if workers == 1:
        iterator = ((day, run_day(spec, day)) for day in pending)
        for day, result in iterator:
            results.append(result)
            (checkpoint_dir / f"{day}.json").write_text(
                json.dumps(
                    {"spec_sha256": spec_sha256, "result": result},
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))
    else:
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(run_day, spec, day): day for day in pending}
            for future in as_completed(futures):
                day = futures[future]
                result = future.result()
                results.append(result)
                (checkpoint_dir / f"{day}.json").write_text(
                    json.dumps(
                        {"spec_sha256": spec_sha256, "result": result},
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                print(json.dumps({"completed_day": day, "runtime_s": result["runtime_s"]}))

    results.sort(key=lambda value: str(value["day"]))
    rows = pd.DataFrame(
        [row for result in results for row in result.get("rows", ())]
    ).sort_values(["day", "arm"], kind="stable")
    decision, gates = _decision(rows, spec)
    daily_path = output / "daily_arm_parity.csv"
    report_path = output / "report.json"
    manifest_path = output / "manifest.json"
    rows.to_csv(daily_path, index=False)
    report = {
        "schema_version": SCHEMA_VERSION,
        "family_id": FAMILY_ID,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "spec_path": str(spec_path),
        "spec_sha256": spec_sha256,
        "decision": decision,
        "gates": gates,
        "parity_days": list(spec["panels"]["parity_days"]),
        "development_days": list(spec["panels"]["development_days"]),
        "mechanism_coverage": dict(spec["mechanism_coverage"]),
        "test_identity": junit,
        "permissions": dict(spec["permissions"]),
        "scope": {
            "native_book_and_buy_q90_parity_kernel_only": True,
            "full_cpp_tick_replay_authority": False,
            "randomized_action_identity_created": False,
            "action_or_live_authorization": False,
        },
        "artifacts": {
            "daily_arm_parity": str(daily_path),
            "market_source_manifest": str(market_manifest_path),
        },
    }
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    manifest = {
        "schema_version": "volatility_time_add_rearm_cpp_q90_parity_manifest.v1",
        "family_id": FAMILY_ID,
        "spec": {"path": str(spec_path), "sha256": spec_sha256},
        "report": {"path": str(report_path), "sha256": sha256_file(report_path)},
        "daily_arm_parity": {
            "path": str(daily_path),
            "sha256": sha256_file(daily_path),
        },
        "market_source_manifest": {
            "path": str(market_manifest_path),
            "sha256": sha256_file(market_manifest_path),
            "canonical_sha256": full_path.canonical_sha256(
                market_source_manifest
            ),
        },
        "permissions": dict(spec["permissions"]),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"decision": decision, "report": str(report_path)}))
    return 0 if gates["parity_gate_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
