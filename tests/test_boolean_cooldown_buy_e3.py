from __future__ import annotations

import hashlib
import json
import os
from itertools import product
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_deployment_gate_v1 as deployment_gate,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_buy_e3_parity_v1 as parity,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    CausalMultichannelEmaState,
    CausalWindowObservation,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    LEARNER_IDENTITY,
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    TriLiteral,
)
from strategy import boolean_cooldown_buy_e3 as subject


def _canonical_sha(value) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _write_json(path: Path, payload: dict) -> str:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="ascii")
    path.chmod(0o600)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact(tmp_path: Path) -> tuple[subject.LiveBuyE3CooldownPolicy, dict[str, Path]]:
    ordering = "predicate::test::mid_ordering"
    campaign_age = subject.DIRECT_CAMPAIGN_AGE
    policy_body = {
        "identity": LEARNER_IDENTITY,
        "side": "BUY",
        "ordered_first_match_rules": [
            {
                "action": "FIXED_2048S",
                "clauses": [
                    {
                        "literals": [
                            {"predicate": campaign_age, "negated": False},
                            {"predicate": ordering, "negated": False},
                        ]
                    }
                ],
            },
            {
                "action": "FIXED_79S",
                "clauses": [
                    {
                        "literals": [
                            {"predicate": campaign_age, "negated": True},
                            {"predicate": ordering, "negated": False},
                        ]
                    }
                ],
            },
        ],
        "default_action": subject.CONTROL_ACTION,
        "permissions": {"action_authorized": False, "live_authorized": False},
    }
    bundle = {
        "schema_version": subject.OWNER_BUNDLE_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "side": "BUY",
        "selected_candidate": subject.SELECTED_CANDIDATE,
        "selected_profile": subject.SELECTED_PROFILE,
        "predicate_columns": sorted((campaign_age, ordering)),
        "definitions": [
            {
                "name": ordering,
                "block": "M1",
                "clock_group": "book",
                "kind": "preserved_tri",
                "source_field": ("tri::mid_usdc_per_btc__h0p5s__h1s::positive_ordering"),
                "threshold": None,
                "quantile": None,
                "category": None,
            }
        ],
        "direct_predicates": [
            {
                "name": campaign_age,
                "kind": "campaign_age_gt_baseline_duration",
                "source_field": "campaign_age_s",
                "clock_group": "context",
            }
        ],
        "ema_half_lives_s": list(subject.EMA_HALF_LIVES_S),
        "ema_pairs_s": [list(pair) for pair in subject.EMA_PAIRS_S],
        "ema_pair_count": 45,
        "normalization_source": {"reference_days_are_2025": True},
        "uses_trade_predicates": False,
        "uses_depth_predicates": False,
        "uses_m2_incremental_features": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    bundle["canonical_sha256"] = _canonical_sha(bundle)
    bundle_path = tmp_path / "predicate_bundle.json"
    bundle_sha = _write_json(bundle_path, bundle)

    policy = {
        "schema_version": subject.OWNER_POLICY_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "owner_refit_frozen_not_self_confirmed",
        "side": "BUY",
        "selected_candidate": subject.SELECTED_CANDIDATE,
        "selected_profile": subject.SELECTED_PROFILE,
        "policy": policy_body,
        "predicate_bundle_file_sha256": bundle_sha,
        "evidence_boundary": {
            "research_supported": False,
            "owner_risk_accepted": True,
        },
    }
    policy["canonical_sha256"] = _canonical_sha(policy)
    policy_path = tmp_path / "policy.json"
    policy_sha = _write_json(policy_path, policy)

    manifest = {
        "schema_version": subject.OWNER_MANIFEST_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "exact_buy_e3_artifact_frozen",
        "policy_file_sha256": policy_sha,
        "predicate_bundle_file_sha256": bundle_sha,
        "duration_vocabulary": list(subject.BUY_ACTIONS),
        "default_action": subject.CONTROL_ACTION,
        "research_supported": False,
        "owner_risk_accepted": True,
    }
    artifact_sha = _canonical_sha(manifest)
    manifest["artifact_sha256"] = artifact_sha
    manifest_path = tmp_path / "artifact_manifest.json"
    manifest_sha = _write_json(manifest_path, manifest)

    runtime = subject.LiveBuyE3CooldownPolicy.from_files(
        artifact_manifest_path=manifest_path,
        artifact_manifest_sha256=manifest_sha,
        expected_artifact_sha256=artifact_sha,
        policy_path=policy_path,
        policy_sha256=policy_sha,
        predicate_bundle_path=bundle_path,
        predicate_bundle_sha256=bundle_sha,
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    return runtime, {
        "manifest": manifest_path,
        "policy": policy_path,
        "bundle": bundle_path,
    }


def _loaded_artifact(tmp_path: Path) -> parity.LoadedExactArtifact:
    _runtime, paths = _artifact(tmp_path)
    manifest = json.loads(paths["manifest"].read_text(encoding="ascii"))
    return parity.load_exact_artifact(
        artifact_manifest_path=paths["manifest"],
        artifact_manifest_file_sha256=hashlib.sha256(paths["manifest"].read_bytes()).hexdigest(),
        expected_artifact_sha256=manifest["artifact_sha256"],
        policy_path=paths["policy"],
        policy_file_sha256=hashlib.sha256(paths["policy"].read_bytes()).hexdigest(),
        predicate_bundle_path=paths["bundle"],
        predicate_bundle_file_sha256=hashlib.sha256(paths["bundle"].read_bytes()).hexdigest(),
    )


def _health_receipt(tmp_path: Path) -> Path:
    payload = {
        "schema_version": deployment_gate.HEALTH_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "pre_enable_live_health_window_complete",
        "host": {
            "logical_host": "<current-live-host>",
            "logical_cpu_count": 2,
            "mem_total_mib": 1_909.0,
            "mem_available_mib": 1_100.0,
            "swap_total_mib": 0.0,
            "load_1m": 0.4,
        },
        "process": {
            "process_count": 1,
            "rss_mib": 320.0,
            "rss_high_water_mib": 330.0,
            "cpu_percent_one_core_scale": 25.0,
        },
        "runtime": {
            "buy_e3_enabled": False,
            "sell_owner_enabled": True,
            "health_window_s": 60.0,
            "actual_execution_book_callback_count": 60,
            "actual_execution_book_callback_rate_hz": 1.0,
            "counter_deltas": {
                key: 0
                for key in deployment_gate._ZERO_DELTA_KEYS  # noqa: SLF001
            },
            "fatal_pattern_counts": {
                key: 0
                for key in deployment_gate._FATAL_PATTERNS  # noqa: SLF001
            },
        },
        "repository": {
            "commit": "a" * 40,
            "annotated_tags_at_head": ["owner-buy-e3-test"],
            "worktree_clean": True,
        },
        "live_config_file_sha256": "b" * 64,
        "economic_values_persisted": False,
        "hypothetical_actions_scored": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    payload["canonical_health_receipt_sha256"] = deployment_gate._document_sha256(  # noqa: SLF001
        payload, "canonical_health_receipt_sha256"
    )
    path = tmp_path / "host-health.json"
    _write_json(path, payload)
    return path


def test_compiled_boolean_evaluator_matches_research_semantics(tmp_path: Path) -> None:
    runtime, _ = _artifact(tmp_path)
    ordering = "predicate::test::mid_ordering"
    campaign_age = subject.DIRECT_CAMPAIGN_AGE
    research = BooleanCooldownPolicy(
        side="BUY",
        rules=(
            BooleanRule(
                action="FIXED_2048S",
                clauses=(
                    AndClause(tuple(sorted((TriLiteral(campaign_age), TriLiteral(ordering))))),
                ),
            ),
            BooleanRule(
                action="FIXED_79S",
                clauses=(
                    AndClause(
                        tuple(
                            sorted(
                                (
                                    TriLiteral(campaign_age, negated=True),
                                    TriLiteral(ordering),
                                )
                            )
                        )
                    ),
                ),
            ),
        ),
    )
    for values in product((-1, 0, 1), repeat=2):
        row = dict(zip(runtime.evaluator.predicate_columns, values, strict=True))
        expected = str(research.choose(pd.DataFrame([row]))[0])
        observed = runtime.evaluator.evaluate(
            predicate_values=row,
            baseline_duration_ms=170_000,
        )
        assert observed[0] == expected
        assert observed[1] == (
            170_000
            if expected == subject.CONTROL_ACTION
            else int(expected.removeprefix("FIXED_").removesuffix("S")) * 1_000
        )


def test_full_mid_ema_state_matches_offline_projector() -> None:
    live = subject._FullMidEmaState()  # noqa: SLF001
    offline = CausalMultichannelEmaState(
        block="R0",
        warmup_admitted=True,
        warmup_identity="unit-test",
    )
    values = (100.0, 101.0, 99.0, 102.0, 98.0, 103.0)
    for offset, value in enumerate(values, start=1):
        right = offset * subject.BASE_WINDOW_WIDTH_NS
        live.update(ts_ns=right, value=value)
        offline.update(
            CausalWindowObservation(
                left_ts_ns=right - subject.BASE_WINDOW_WIDTH_NS,
                right_ts_ns=right,
                feature_ready_ts_ns=right,
                market_generation=offset,
                depth_generation=offset,
                values={"mid_usdc_per_btc": value},
            )
        )
    decision = len(values) * subject.BASE_WINDOW_WIDTH_NS
    live_row = live.feature_row(decision_ts_ns=decision)
    offline_row = offline.channel_feature_row(
        channel_name="mid_usdc_per_btc",
        side="BUY",
        decision_ts_ns=decision,
    )
    assert set(live_row) == set(offline_row)
    for key, expected in offline_row.items():
        observed = live_row[key]
        if isinstance(expected, float):
            assert observed == expected
        else:
            assert observed == expected


def test_warmup_unobserved_and_hash_drift_fail_closed(tmp_path: Path) -> None:
    runtime, paths = _artifact(tmp_path)
    before = runtime.evaluate(
        side="BUY",
        baseline_duration_ms=170_000,
        campaign_age_s=200.0,
        decision_ts_ns=1,
        snapshot_id="before-warmup",
    )
    assert before.action_id == subject.CONTROL_ACTION
    assert before.fallback_reason == "no_completed_receive_time_window"

    paths["policy"].write_text("{}\n", encoding="ascii")
    drift = runtime.evaluate(
        side="BUY",
        baseline_duration_ms=170_000,
        campaign_age_s=200.0,
        decision_ts_ns=2,
        snapshot_id="hash-drift",
    )
    assert drift.action_id == subject.CONTROL_ACTION
    assert drift.support_valid is False
    assert drift.fallback_reason == "runtime_artifact_file_hash_drift"


def _runtime_reload_kwargs(paths: dict[str, Path]) -> dict[str, object]:
    manifest = json.loads(paths["manifest"].read_text(encoding="ascii"))
    return {
        "artifact_manifest_path": paths["manifest"],
        "artifact_manifest_sha256": hashlib.sha256(paths["manifest"].read_bytes()).hexdigest(),
        "expected_artifact_sha256": manifest["artifact_sha256"],
        "policy_path": paths["policy"],
        "policy_sha256": hashlib.sha256(paths["policy"].read_bytes()).hexdigest(),
        "predicate_bundle_path": paths["bundle"],
        "predicate_bundle_sha256": hashlib.sha256(paths["bundle"].read_bytes()).hexdigest(),
        "warmup_s": 2048.0,
        "max_feature_age_s": 1.0,
    }


def test_artifact_load_rejects_non_private_mode(tmp_path: Path) -> None:
    _runtime, paths = _artifact(tmp_path)
    kwargs = _runtime_reload_kwargs(paths)
    paths["policy"].chmod(0o640)

    with pytest.raises(ValueError, match="mode_not_private"):
        subject.LiveBuyE3CooldownPolicy.from_files(**kwargs)


def test_artifact_load_rejects_additional_hard_link(tmp_path: Path) -> None:
    _runtime, paths = _artifact(tmp_path)
    kwargs = _runtime_reload_kwargs(paths)
    os.link(paths["bundle"], paths["bundle"].with_suffix(".linked"))

    with pytest.raises(ValueError, match="link_count_mismatch"):
        subject.LiveBuyE3CooldownPolicy.from_files(**kwargs)


def test_artifact_load_rejects_path_swap_between_lstat_and_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _runtime, paths = _artifact(tmp_path)
    kwargs = _runtime_reload_kwargs(paths)
    policy_path = paths["policy"]
    original_open = subject.os.open
    swapped = False

    def swap_then_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal swapped
        if Path(path) == policy_path and not swapped:
            original_bytes = policy_path.read_bytes()
            policy_path.replace(policy_path.with_suffix(".original"))
            policy_path.write_bytes(original_bytes)
            policy_path.chmod(0o600)
            swapped = True
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(subject.os, "open", swap_then_open)
    with pytest.raises(ValueError, match="identity_changed_during_open"):
        subject.LiveBuyE3CooldownPolicy.from_files(**kwargs)


def test_invalid_baseline_is_rejected(tmp_path: Path) -> None:
    runtime, _paths = _artifact(tmp_path)

    with pytest.raises(ValueError, match="baseline_duration_ms_must_be_positive"):
        runtime.evaluate(
            side="BUY",
            baseline_duration_ms=0,
            campaign_age_s=0.0,
            decision_ts_ns=1,
            snapshot_id="invalid-baseline",
        )


def test_gap_and_out_of_order_reset_receive_time_state() -> None:
    windows = subject.ReceiveTimeFullMidEmaWindows(
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    event = {
        "bids": ((99.0, 1.0),),
        "asks": ((101.0, 1.0),),
        "market_generation": 1,
        "depth_generation": 1,
    }
    windows.observe_depth(receive_ts_ns=100_000_001, **event)
    windows.observe_depth(receive_ts_ns=400_000_001, **event)
    assert windows.audit()["gap_windows"] == 2
    assert windows.audit()["gap_resets"] == 1
    assert windows.audit()["resets"] == 1
    windows.observe_depth(receive_ts_ns=300_000_001, **event)
    audit = windows.audit()
    assert audit["out_of_order_updates"] == 1
    assert audit["warmup_time_admitted"] == 0
    assert audit["resets"] == 2
    windows.observe_depth(receive_ts_ns=1_600_000_001, **event)
    audit = windows.audit()
    assert audit["gap_resets"] == 1
    assert audit["resets"] == 2


def test_research_compiled_parity_writes_bound_receipt(tmp_path: Path) -> None:
    artifact = _loaded_artifact(tmp_path)
    path = tmp_path / "research_compiled.json"
    receipt = parity.run_research_compiled_parity(artifact, output_path=path)
    assert receipt["evidence"]["logical_vector_count"] == 9
    assert receipt["evidence"]["mismatch_count"] == 0
    assert path.stat().st_mode & 0o777 == 0o600
    assert (
        parity.validate_parity_receipt(
            path,
            expected_layer=parity.RESEARCH_COMPILED_LAYER,
            expected_artifact_sha256=artifact.artifact_sha256,
        )
        == receipt
    )


def test_development_snapshot_parity_covers_buy_and_sell(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact = _loaded_artifact(tmp_path)
    ordering = "predicate::test::mid_ordering"
    campaign_age = subject.DIRECT_CAMPAIGN_AGE
    index = pd.Index(("buy-opportunity", "sell-opportunity"))
    replay_inputs = pd.DataFrame(
        {
            "side": ("BUY", "SELL"),
            "decision_ts_ns": (1_000_000_000, 2_000_000_000),
            "baseline_duration_ms": (170_000, 85_000),
        },
        index=index,
    )
    boolean_features = pd.DataFrame(
        {
            campaign_age: (1, 0),
            ordering: (1, 0),
        },
        index=index,
        dtype="int8",
    )
    panel = SimpleNamespace(
        metadata=pd.DataFrame(
            {
                "side": ("BUY", "SELL"),
                "baseline_duration_ms": (170_000, 85_000),
                "campaign_age_s": (200.0, 0.0),
            },
            index=index,
        ),
        boolean_features=boolean_features,
        continuous_features=pd.DataFrame(index=index),
        exact_owner_actions=pd.Series(("CONTROL_85N", "FIXED_166S"), index=index, dtype="object"),
    )
    mechanics = SimpleNamespace(
        replay_inputs=replay_inputs,
        panel=panel,
        mechanics_receipt_sha256="a" * 64,
    )
    source_bundle = SimpleNamespace(file_sha256="b" * 64)

    def materialize(**kwargs):
        row = kwargs["feature_row"]
        return {
            campaign_age: int(float(row["campaign_age_s"]) * 1_000 > 170_000),
            ordering: int(row[ordering]),
        }

    monkeypatch.setattr(
        parity.predicate_view,
        "materialize_snapshot_predicates",
        materialize,
    )
    receipt = parity.run_development_snapshot_parity(
        artifact,
        mechanics=mechanics,
        source_predicate_bundle=source_bundle,
        output_path=tmp_path / "development_snapshot.json",
        expected_opportunity_count=2,
    )
    assert receipt["evidence"]["buy_snapshot_count"] == 1
    assert receipt["evidence"]["sell_snapshot_count"] == 1
    assert receipt["evidence"]["action_duration_mismatch_count"] == 0


def test_streaming_offline_parity_covers_full_warmup(tmp_path: Path) -> None:
    artifact = _loaded_artifact(tmp_path)
    receipt = parity.run_streaming_offline_parity(
        artifact,
        output_path=tmp_path / "streaming_offline.json",
    )
    assert receipt["evidence"]["completed_window_count"] == 20_481
    assert receipt["evidence"]["ema_half_life_count"] == 10
    assert receipt["evidence"]["ema_pair_count"] == 45
    assert receipt["evidence"]["feature_mismatch_count"] == 0


def test_sell_owner_54_case_receipt_is_bound_to_buy_artifact(tmp_path: Path, monkeypatch) -> None:
    artifact = _loaded_artifact(tmp_path)
    monkeypatch.setattr(
        parity.successor,
        "audit_exact_owner_artifact_parity",
        lambda **_kwargs: parity.successor.ExactOwnerArtifactParity(
            policy_sha256="1" * 64,
            predicate_bundle_sha256="2" * 64,
            predicate_columns=("a", "b", "c"),
            sell_tri_state_cases=27,
            buy_tri_state_cases=27,
            mismatch_count=0,
            documented_semantics_equal=True,
            runtime_binding_valid=True,
        ),
    )
    path = tmp_path / "sell-54-case.json"
    receipt = parity.run_sell_owner_54_case_unchanged(
        artifact,
        sell_policy_path=tmp_path / "sell-policy.json",
        sell_predicate_bundle_path=tmp_path / "sell-bundle.json",
        output_path=path,
    )
    assert receipt["artifact_sha256"] == artifact.artifact_sha256
    assert receipt["evidence"]["sell_tri_state_cases"] == 27
    assert receipt["evidence"]["buy_tri_state_cases"] == 27
    assert (
        parity.validate_parity_receipt(
            path,
            expected_layer=parity.SELL_OWNER_54_CASE_LAYER,
            expected_artifact_sha256=artifact.artifact_sha256,
        )
        == receipt
    )


def test_repeated_policy_lockstep_receipts_resume_without_rerun(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifact = _loaded_artifact(tmp_path)
    index = pd.Index(("row-a", "row-b"))
    replay_inputs = pd.DataFrame(
        {
            "utc_day": ("2026-01-01", "2026-01-02"),
            "day_input_sha256": ("1" * 64, "2" * 64),
        },
        index=index,
    )
    mechanics = SimpleNamespace(
        selected_days=("2026-01-01", "2026-01-02"),
        replay_inputs=replay_inputs,
        mechanics_receipt_sha256="a" * 64,
    )
    source_bundle = SimpleNamespace(file_sha256="b" * 64)
    monkeypatch.setattr(
        parity.replay_adapter,
        "_resolve_execution_options",
        lambda _rows: SimpleNamespace(binding={"test": True}),
    )
    calls: list[str] = []

    def run_day(**kwargs):
        calls.append(kwargs["utc_day"])
        return {
            "summary_signature_sha256": "3" * 64,
            "campaign_frame_sha256": "4" * 64,
            "fill_frame_sha256": "5" * 64,
            "decision_frame_sha256": "6" * 64,
            "decision_count": 1,
            "campaign_count": 1,
            "fill_count": 1,
            "mismatch_count": 0,
        }

    monkeypatch.setattr(parity, "_run_lockstep_day", run_day)
    final_path = tmp_path / "lockstep-final.json"
    receipt = parity.run_repeated_policy_lockstep_parity(
        artifact,
        mechanics=mechanics,
        source_predicate_bundle=source_bundle,
        learning_algorithm_artifact_sha256="c" * 64,
        day_receipt_dir=tmp_path / "days",
        output_path=final_path,
        expected_day_count=2,
    )
    assert calls == ["2026-01-01", "2026-01-02"]
    assert receipt["evidence"]["day_count"] == 2
    assert receipt["economic_values_materialized_by_replay"] is True
    assert (
        parity.validate_parity_receipt(
            final_path,
            expected_layer=parity.REPEATED_POLICY_LOCKSTEP_LAYER,
            expected_artifact_sha256=artifact.artifact_sha256,
        )
        == receipt
    )

    final_path.unlink()
    monkeypatch.setattr(
        parity,
        "_run_lockstep_day",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected rerun")),
    )
    resumed = parity.run_repeated_policy_lockstep_parity(
        artifact,
        mechanics=mechanics,
        source_predicate_bundle=source_bundle,
        learning_algorithm_artifact_sha256="c" * 64,
        day_receipt_dir=tmp_path / "days",
        output_path=final_path,
        expected_day_count=2,
    )
    assert resumed == receipt


def test_host_benchmark_binds_actual_rate_and_lifecycle(tmp_path: Path) -> None:
    _runtime, paths = _artifact(tmp_path)
    manifest = json.loads(paths["manifest"].read_text(encoding="ascii"))
    output = tmp_path / "host-benchmark.json"
    receipt = deployment_gate.run_host_benchmark(
        artifact_manifest_path=paths["manifest"],
        artifact_manifest_file_sha256=hashlib.sha256(paths["manifest"].read_bytes()).hexdigest(),
        expected_artifact_sha256=manifest["artifact_sha256"],
        policy_path=paths["policy"],
        policy_file_sha256=hashlib.sha256(paths["policy"].read_bytes()).hexdigest(),
        predicate_bundle_path=paths["bundle"],
        predicate_bundle_file_sha256=hashlib.sha256(paths["bundle"].read_bytes()).hexdigest(),
        health_receipt_path=_health_receipt(tmp_path),
        output_path=output,
        paced_duration_s=2.0,
    )
    assert receipt["callback_benchmark"]["target_to_observed_ratio"] >= 2.0
    assert receipt["lifecycle_checks"]["cold_restart_fell_back_to_b0"] is True
    assert receipt["lifecycle_checks"]["artifact_hash_drift_fell_back_to_b0"] is True
    assert output.stat().st_mode & 0o777 == 0o600


def test_deployment_gate_preserves_failed_receipt(tmp_path: Path) -> None:
    health_path = _health_receipt(tmp_path)
    benchmark = {
        "schema_version": deployment_gate.BENCHMARK_SCHEMA,
        "identity": subject.OWNER_IDENTITY,
        "status": "exact_artifact_host_benchmark_complete",
        "artifact_sha256": "c" * 64,
        "health_receipt_sha256": json.loads(health_path.read_text(encoding="ascii"))[
            "canonical_health_receipt_sha256"
        ],
        "host": {"logical_cpu_count": 2, "max_rss_mib": 100.0},
        "callback_benchmark": {
            "observed_live_rate_hz": 1.0,
            "target_rate_hz": 2.0,
            "target_to_observed_ratio": 2.0,
            "achieved_rate_hz": 2.0,
            "latency_p99_us": 20.0,
            "cpu_percent_total_host_scale": 10.0,
        },
        "decision_benchmark": {"latency_p99_us": 20_000.0},
        "lifecycle_checks": {
            "cold_restart_fell_back_to_b0": True,
            "full_warmup_completed": True,
            "selected_state_identified_after_warmup": True,
            "short_gap_unobserved_windows": 2,
            "out_of_order_updates_ignored": 1,
            "stale_gap_resets": 1,
            "artifact_hash_drift_fell_back_to_b0": True,
        },
        "economic_values_persisted": False,
        "hypothetical_live_actions_scored": False,
    }
    benchmark["canonical_benchmark_receipt_sha256"] = deployment_gate._document_sha256(  # noqa: SLF001
        benchmark, "canonical_benchmark_receipt_sha256"
    )
    benchmark_path = tmp_path / "benchmark-failing.json"
    _write_json(benchmark_path, benchmark)
    gate_path = tmp_path / "gate-failing.json"
    with pytest.raises(deployment_gate.BuyE3DeploymentGateError, match="failed"):
        deployment_gate.build_deployment_gate_receipt(
            health_receipt_path=health_path,
            benchmark_receipt_path=benchmark_path,
            expected_artifact_sha256="c" * 64,
            expected_execution_commit="a" * 40,
            expected_execution_tag="owner-buy-e3-test",
            output_path=gate_path,
        )
    failed = json.loads(gate_path.read_text(encoding="ascii"))
    assert failed["status"] == "deployment_gate_failed"
    assert failed["activation_allowed"] is False
    assert failed["checks"]["decision_p99_at_most_10ms"] is False
