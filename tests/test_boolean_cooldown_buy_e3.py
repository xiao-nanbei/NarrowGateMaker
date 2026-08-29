from __future__ import annotations

import hashlib
import json
import os
from itertools import product
from pathlib import Path

import pandas as pd
import pytest

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


def test_short_gap_and_out_of_order_preserve_receive_time_state() -> None:
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
    assert windows.audit()["gap_resets"] == 0
    assert windows.audit()["resets"] == 0
    windows.observe_depth(receive_ts_ns=300_000_001, **event)
    audit = windows.audit()
    assert audit["out_of_order_updates"] == 1
    assert audit["warmup_time_admitted"] == 0
    assert audit["resets"] == 0
    windows.observe_depth(receive_ts_ns=1_600_000_001, **event)
    audit = windows.audit()
    assert audit["gap_resets"] == 1
    assert audit["resets"] == 1


def test_sparse_receive_time_windows_match_offline_projector() -> None:
    windows = subject.ReceiveTimeFullMidEmaWindows(
        warmup_s=2048.0,
        max_feature_age_s=1.0,
    )
    offline = CausalMultichannelEmaState(
        block="R0",
        warmup_admitted=True,
        warmup_identity="sparse-live-parity",
    )
    width = subject.BASE_WINDOW_WIDTH_NS
    previous_index: int | None = None
    previous_mid: float | None = None
    generation = 0
    final_index = 20_493
    for index in range(final_index + 1):
        if index % 4 == 2:
            continue
        receive_ts_ns = index * width + 1
        mid = 60_000.0 + 0.001 * index
        windows.observe_depth(
            receive_ts_ns=receive_ts_ns,
            bids=((mid - 0.5, 1.0),),
            asks=((mid + 0.5, 1.0),),
            market_generation=index + 1,
            depth_generation=index + 1,
        )
        if previous_index is not None:
            generation += 1
            offline.update(
                CausalWindowObservation(
                    left_ts_ns=previous_index * width,
                    right_ts_ns=(previous_index + 1) * width,
                    feature_ready_ts_ns=receive_ts_ns,
                    market_generation=generation,
                    depth_generation=generation,
                    values={"mid_usdc_per_btc": previous_mid},
                )
            )
            for gap_index in range(previous_index + 1, index):
                generation += 1
                offline.update(
                    CausalWindowObservation(
                        left_ts_ns=gap_index * width,
                        right_ts_ns=(gap_index + 1) * width,
                        feature_ready_ts_ns=receive_ts_ns,
                        market_generation=generation,
                        depth_generation=generation,
                        values={"mid_usdc_per_btc": None},
                        source_gap=True,
                    )
                )
        previous_index = index
        previous_mid = mid

    decision_ts_ns = final_index * width + 1
    live_row, reason, _ready, _age_ms = windows.feature_row(
        decision_ts_ns=decision_ts_ns
    )
    assert reason is None
    assert live_row is not None
    offline_row = offline.channel_feature_row(
        channel_name="mid_usdc_per_btc",
        side="BUY",
        decision_ts_ns=decision_ts_ns,
    )
    assert set(live_row) == set(offline_row)
    for name, expected in offline_row.items():
        observed = live_row[name]
        if isinstance(expected, float):
            assert observed == pytest.approx(expected, rel=0.0, abs=1e-12)
        else:
            assert observed == expected
    audit = windows.audit()
    assert audit["warmup_time_admitted"] == 1
    assert audit["gap_windows"] > 0
    assert audit["gap_resets"] == 0
    assert audit["resets"] == 0
