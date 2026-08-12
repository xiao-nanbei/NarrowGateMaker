#!/usr/bin/env python3
"""Read-only cache execution successor for the frozen carryover-safe v2 smoke."""

from __future__ import annotations

import argparse
import json
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import patch

from research.families.f09_campaign_action_uplift.audit import (
    ranked_toxicity_guard_full_path_mechanics as legacy,
)
from research.families.f09_campaign_action_uplift.audit.ranked_toxicity_guard_authoritative_replay_v2 import (
    RankedToxicityGuardAuthoritativeReplayV2,
)

SCHEMA_VERSION = "ranked_toxicity_guard_carryover_safe_execution.v2.1"
SIDES = ("BUY", "SELL")


class CacheWriteContractViolation(RuntimeError):
    """Raised if the successor attempts to materialize any replay cache."""


class SmokeParityContractViolation(RuntimeError):
    """Raised when the successor differs from the frozen v2 smoke."""


def _identity(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "entry_count": 0, "sha256": legacy.canonical_sha256([])}
    entries: list[dict[str, Any]] = []
    for item in sorted(path.rglob("*")):
        stat = item.stat()
        entries.append(
            {
                "path": str(item.relative_to(path)),
                "kind": "dir" if item.is_dir() else "file",
                "size": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
        )
    return {
        "exists": True,
        "entry_count": len(entries),
        "sha256": legacy.canonical_sha256(entries),
    }


def _load_predecessor(spec: Mapping[str, Any]) -> dict[str, Any]:
    identity = spec["predecessor_v2"]
    path = legacy._require_identity(identity, "frozen carryover-safe v2 spec")
    predecessor = json.loads(path.read_text(encoding="utf-8"))
    if legacy.canonical_spec_sha256(predecessor) != str(
        identity["canonical_sha256"]
    ):
        raise ValueError("frozen carryover-safe v2 canonical identity mismatch")
    if predecessor.get("family_id") != "causal_v12_ranked_toxicity_exposure_guard_carryover_safe_v2":
        raise ValueError("unexpected carryover-safe predecessor")
    return predecessor


def _validate_cache_contract(spec: Mapping[str, Any]) -> dict[str, Any]:
    from models import data_windows
    from models.replay_cache_dag import (
        REPLAY_WINDOW_CACHE_GRAPH_V2,
        REPLAY_WINDOW_CACHE_GRAPH_V2_IDENTITY,
    )

    contract = spec["cache_execution_contract"]
    if int(data_windows.WINDOW_CACHE_VERSION) != 13:
        raise ValueError("legacy monolithic read compatibility is no longer v13")
    if str(REPLAY_WINDOW_CACHE_GRAPH_V2_IDENTITY) != str(
        contract["replay_cache_dag_v2_identity_sha256"]
    ):
        raise ValueError("replay-cache DAG v2 identity mismatch")
    nodes = {node.name: node for node in REPLAY_WINDOW_CACHE_GRAPH_V2.nodes}
    window_node = nodes.get("window_data")
    if window_node is None or window_node.materialization != "ephemeral":
        raise ValueError("DAG v2 WindowData must remain ephemeral")
    if nodes["action_dependent_replay_state"].materialization != "forbidden":
        raise ValueError("action-dependent replay cache must remain forbidden")
    if contract.get("legacy_v13_read") != "read_only_compatible":
        raise ValueError("v13 compatibility must be explicitly read-only")
    if contract.get("legacy_component_v1_read") != "read_only_compatible":
        raise ValueError("component-v1 compatibility must be explicitly read-only")
    for flag in (
        "window_cache_write_enabled",
        "legacy_monolithic_window_cache_write_enabled",
        "legacy_component_v1_write_enabled",
        "market_context_day_v2_write_enabled",
        "model_overlay_day_write_enabled",
        "native_book_hour_write_enabled",
    ):
        if bool(contract.get(flag)):
            raise ValueError(f"cache write flag must be false: {flag}")
    return {
        "legacy_window_cache_version": int(data_windows.WINDOW_CACHE_VERSION),
        "replay_cache_dag_v2_identity_sha256": str(
            REPLAY_WINDOW_CACHE_GRAPH_V2_IDENTITY
        ),
        "window_data_materialization": window_node.materialization,
        "action_dependent_replay_state_materialization": nodes[
            "action_dependent_replay_state"
        ].materialization,
    }


def load_spec(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    spec = json.loads(path.read_text(encoding="utf-8"))
    if spec.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported carryover-safe execution successor")
    if legacy.canonical_spec_sha256(spec) != spec.get(
        "canonical_spec_identity_sha256"
    ):
        raise ValueError("v2.1 execution successor canonical hash mismatch")
    if legacy.sha256_file(Path(__file__).resolve()) != spec.get(
        "implementation_sha256"
    ):
        raise ValueError("v2.1 execution successor implementation hash mismatch")
    predecessor = _load_predecessor(spec)
    for label, identity in spec.get("artifact_identities", {}).items():
        legacy._require_identity(identity, label)
    for label, identity in spec.get("implementation_identities", {}).items():
        legacy._require_identity(identity, label)
    _validate_cache_contract(spec)
    permissions = spec.get("permissions") or {}
    for forbidden in (
        "formal_40_day_mechanics_run",
        "mechanics_results_read",
        "economic_outcomes_read",
        "validation_read",
        "sealed_holdout_read",
        "action_experiment_authorized",
        "live_deployment_authorized",
    ):
        if bool(permissions.get(forbidden)):
            raise ValueError(f"v2.1 execution successor cannot grant {forbidden}")
    return spec, predecessor


def _configure_read_only_params(
    predecessor: Mapping[str, Any], day: str
) -> dict[str, Any]:
    params = legacy._configure_params(predecessor, day)
    params.update(
        {
            "window_cache_write_enabled": False,
            "legacy_monolithic_window_cache_write_enabled": False,
            "legacy_component_v1_write_enabled": False,
        }
    )
    return params


def _read_only_native_cache(**kwargs: Any):
    from models.native_exchange_book_cache import (
        NativeBookHourCacheArtifact,
        _read_valid_manifest,
        native_book_hour_cache_paths,
    )

    data_path, manifest_path, _, digest = native_book_hour_cache_paths(
        Path(kwargs["cache_root"]), dict(kwargs["identity"])
    )
    if bool(kwargs.get("refresh", False)):
        raise CacheWriteContractViolation("native cache refresh is forbidden")
    manifest = _read_valid_manifest(
        data_path=data_path,
        manifest_path=manifest_path,
        identity_sha256=digest,
    )
    if manifest is None:
        raise FileNotFoundError("read-only native cache miss")
    return NativeBookHourCacheArtifact(
        data_path=data_path,
        manifest_path=manifest_path,
        identity_sha256=digest,
        event_count=int(manifest["event_count"]),
        level_count=int(manifest["level_count"]),
        cache_hit=True,
    )


def _simulate_read_only(
    spec: Mapping[str, Any],
    predecessor: Mapping[str, Any],
    day: str,
    binding: RankedToxicityGuardAuthoritativeReplayV2,
) -> tuple[dict[str, Any], dict[str, Any]]:
    from models import backtest_tick as bt
    from models import data_windows
    from models import exchange_book_replay

    params = _configure_read_only_params(predecessor, day)
    cache_root = Path(str(predecessor["storage"]["window_cache_dir"])).expanduser().resolve()
    read_paths: list[str] = []
    component_v2_hits: list[str] = []
    component_v1_hits: list[str] = []
    original_v13_loader = data_windows._load_cached_window
    original_component_v1_loader = data_windows._load_component
    original_market_loader = data_windows.load_market_context
    original_overlay_loader = data_windows.load_model_overlay

    def record_v13(path: Path):
        value = original_v13_loader(path)
        if value is not None:
            read_paths.append(str(Path(path).resolve()))
        return value

    def record_market(*args: Any, **kwargs: Any):
        value = original_market_loader(*args, **kwargs)
        if value is not None:
            component_v2_hits.append("market_context_day_v2")
        return value

    def record_component_v1(path: Path, expected_type: type[Any]):
        value = original_component_v1_loader(path, expected_type)
        if value is not None:
            component_v1_hits.append(str(Path(path).resolve()))
        return value

    def record_overlay(*args: Any, **kwargs: Any):
        value = original_overlay_loader(*args, **kwargs)
        if value is not None:
            component_v2_hits.append("model_overlay_day")
        return value

    def reject_write(*_: Any, **__: Any) -> None:
        raise CacheWriteContractViolation("replay cache write attempted")

    before_window = _identity(cache_root)
    with ExitStack() as stack:
        stack.enter_context(patch.object(data_windows, "_load_cached_window", record_v13))
        stack.enter_context(patch.object(data_windows, "_load_component", record_component_v1))
        stack.enter_context(patch.object(data_windows, "load_market_context", record_market))
        stack.enter_context(patch.object(data_windows, "load_model_overlay", record_overlay))
        for name in (
            "_write_cached_window",
            "_write_component",
            "write_market_context",
            "write_model_overlay",
        ):
            stack.enter_context(patch.object(data_windows, name, reject_write))
        stack.enter_context(
            patch.object(
                exchange_book_replay,
                "ensure_native_book_hour_cache",
                _read_only_native_cache,
            )
        )
        window = legacy._load_window(predecessor, day, params)
        if not isinstance(window, data_windows.WindowData):
            raise TypeError("authoritative loader did not return WindowData")
        tape = legacy._exchange_tape(predecessor, day, params)
        before_native = _identity(tape.cache_dir)
        result = bt._simulate_tick_with_engine(
            "python",
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            params,
            ml_data=window.ml_data,
            bbo_data=window.bbo_data,
            l2_data=window.l2_data,
            var_ti=window.var_ti,
            var_retsq=window.var_retsq,
            reference_event_tapes=window.reference_event_tapes,
            campaign_repair_data=window.campaign_repair_data,
            campaign_repair_model=window.campaign_repair_model,
            historical_global_flow_data=window.historical_global_flow_data,
            exchange_book_event_tape=tape,
            ranked_toxicity_guard_binding=binding,
        )
        after_native = _identity(tape.cache_dir)
        native_stats = tape.cache_stats()
    after_window = _identity(cache_root)
    if before_window != after_window:
        raise CacheWriteContractViolation("window-cache tree changed during smoke")
    if before_native != after_native:
        raise CacheWriteContractViolation("native-book cache tree changed during smoke")
    audit = result.get("ranked_toxicity_guard_binding_audit")
    if not isinstance(audit, Mapping):
        raise RuntimeError("authoritative replay omitted guard binding audit")
    cache_audit = {
        "write_attempt_count": 0,
        "window_cache_tree_unchanged": True,
        "native_book_cache_tree_unchanged": True,
        "legacy_v13_read_paths": sorted(set(read_paths)),
        "legacy_v13_read_used": bool(read_paths),
        "legacy_component_v1_read_paths": sorted(set(component_v1_hits)),
        "legacy_component_v1_read_used": bool(component_v1_hits),
        "component_v2_read_hits": sorted(component_v2_hits),
        "native_book_cache_stats": native_stats,
        "window_data_runtime_type": type(window).__name__,
        "window_data_published": False,
    }
    if spec["cache_execution_contract"]["smoke_requires_legacy_v13_read"] and not read_paths:
        raise CacheWriteContractViolation("smoke did not exercise legacy v13 read compatibility")
    return dict(audit), cache_audit


def _observed_counts(audit: Mapping[str, Any]) -> dict[str, Any]:
    observed: dict[str, Any] = {
        "baseline": {
            "rows": int(audit["baseline_shadow"]["rows"]),
            "consumed": int(audit["baseline_shadow"]["consumed"]),
            "unconsumed": int(audit["baseline_shadow"]["unconsumed"]),
            "complete": bool(audit["baseline_shadow"]["complete"]),
        },
        "candidate_campaign_terminal_count": int(
            audit["candidate_campaign_terminal_count"]
        ),
    }
    for side in SIDES:
        side_audit = audit["adapters"][side]
        observed[side] = {
            "assignment_episodes": int(side_audit["assignment_count"]),
            "completed_episodes": int(
                side_audit["completed_assignment_episode_count"]
            ),
            "censored_episodes": int(
                side_audit["censored_assignment_episode_count"]
            ),
            "carryover_transitions": int(side_audit["carryover_transition_count"]),
            "active_order_role_transition_to_exposure_count": int(
                side_audit["active_order_role_transition_to_exposure_count"]
            ),
            "cross_arm_order_ownership_count": int(
                side_audit["cross_arm_order_ownership_count"]
            ),
            "forced_washout_cancel_count": int(
                side_audit["forced_washout_cancel_count"]
            ),
            "order_owner_mismatch_count": int(
                side_audit["order_owner_mismatch_count"]
            ),
            "execution_complete": bool(side_audit["execution_complete"]),
            "zero_tolerance_passed": bool(side_audit["zero_tolerance_passed"]),
            "carryover_contract_valid": bool(
                side_audit["carryover_contract_valid"]
            ),
        }
    return observed


def assert_v2_smoke_parity(
    observed: Mapping[str, Any], expected: Mapping[str, Any]
) -> None:
    if dict(observed) != dict(expected):
        raise SmokeParityContractViolation(
            "v2.1 smoke differs from frozen v2: "
            + json.dumps(
                {"observed": observed, "expected": expected},
                sort_keys=True,
                separators=(",", ":"),
            )
        )


def run_smoke(
    spec_path: Path, *, output_root: Path, report_path: Path
) -> dict[str, Any]:
    spec, predecessor = load_spec(spec_path.resolve())
    day = str(spec["smoke_contract"]["utc_day"])
    output_root = output_root.expanduser().resolve()
    if output_root.exists():
        raise FileExistsError(f"smoke output already exists: {output_root}")
    baseline = legacy._require_identity(
        spec["smoke_contract"]["baseline_manifest"], "v2 baseline manifest"
    )
    threshold = float(spec["smoke_contract"]["threshold"])
    source_sha256 = str(spec["smoke_contract"]["threshold_source_sha256"])
    binding = RankedToxicityGuardAuthoritativeReplayV2(
        baseline_manifest_path=baseline,
        output_root=output_root,
        frozen_model_sha256=str(predecessor["model_identity"]["bundle_meta_sha256"]),
        threshold_schedule={
            side: {day: (threshold, source_sha256)} for side in SIDES
        },
        sides=SIDES,
        chunk_rows=int(predecessor["replay_contract"]["journal_chunk_rows"]),
    )
    audit, cache_audit = _simulate_read_only(
        spec, predecessor, day, binding
    )
    observed = _observed_counts(audit)
    assert_v2_smoke_parity(observed, spec["smoke_contract"]["expected_v2_counts"])
    manifests: dict[str, Any] = {}
    for side in SIDES:
        path = output_root / side.lower() / "manifest.json"
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not bool(payload.get("closed")):
            raise RuntimeError(f"{side} journal did not close atomically")
        manifests[side] = {
            "path": str(path),
            "sha256": legacy.sha256_file(path),
            "row_count": int(payload["row_count"]),
            "part_count": int(payload["part_count"]),
            "closed": True,
        }
    report = {
        "schema_version": f"{SCHEMA_VERSION}.smoke_report",
        "family_id": spec["family_id"],
        "spec_path": str(spec_path.resolve()),
        "spec_canonical_sha256": spec["canonical_spec_identity_sha256"],
        "utc_day": day,
        "status": "authoritative_smoke_passed" if observed == spec["smoke_contract"]["expected_v2_counts"] else "failed",
        "v2_counts_exact_match": True,
        "observed_counts": observed,
        "cache_audit": cache_audit,
        "journal_manifests": manifests,
        "mechanics_results_read": False,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "formal_40_day_mechanics_run": False,
        "action_experiment_authorized": False,
        "live_deployment_authorized": False,
    }
    report["report_identity_sha256"] = legacy.canonical_sha256(report)
    legacy._atomic_json(report_path.expanduser().resolve(), report)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = run_smoke(
        args.spec,
        output_root=args.output_root,
        report_path=args.report,
    )
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
