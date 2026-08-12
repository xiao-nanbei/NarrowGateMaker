from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from models.replay.narrowgate_continuous_tick_adapter import (
    AdapterArmBinding,
    AuthoritativeReplayEpoch,
)
from models.replay.replay_state_checkpoint import (
    ContinuousReplayState,
    EconomicCampaignState,
)
from models.replay.restart_aware_continuous_ab import canonical_sha256, sha256_file
from models.replay.restart_aware_continuous_execution import ContinuousOperation
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_2 as f03_framework,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_full_path_v1 as daily_full_path,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_restart_aware_v1 as subject,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    CausalWindowObservation,
)

DAY_MS = 86_400_000
START = 1_767_225_600_000
MIDNIGHT = START + DAY_MS
DAY1 = "2026-01-01"
DAY2 = "2026-01-02"
H64 = "a" * 64


def _operation(
    sequence: int,
    kind: str,
    day: str,
    start: int,
    end: int,
    *,
    gap_id: str = "",
    warmup_start: int | None = None,
) -> ContinuousOperation:
    return ContinuousOperation(
        sequence=sequence,
        operation_id=f"op-{sequence:02d}-{kind}",
        kind=kind,
        day=day,
        start_ts_ms=start,
        end_ts_ms=end,
        source_day=day,
        gap_id=gap_id,
        warmup_lookback_start_ts_ms=warmup_start,
        exact_queue_authority=False,
        exact_lifecycle_authority=False,
        continuous_economic_sensitivity_authority=True,
        source_identity_sha256=canonical_sha256({"day": day}),
        source_artifact_manifest_sha256=H64,
        restart_timeline_sha256="b" * 64,
        random_seed=20260812 + sequence,
        random_path_sha256=canonical_sha256({"operation": sequence}),
    )


def _operations() -> tuple[ContinuousOperation, ...]:
    return (
        _operation(1, "online", DAY1, MIDNIGHT - 5_000, MIDNIGHT),
        _operation(2, "utc_accounting", DAY1, MIDNIGHT, MIDNIGHT),
        _operation(3, "online", DAY2, MIDNIGHT, MIDNIGHT + 2_000),
        _operation(
            4,
            "cancel_drain",
            DAY2,
            MIDNIGHT + 2_000,
            MIDNIGHT + 3_000,
            gap_id="G1",
        ),
        _operation(
            5,
            "offline_gap",
            DAY2,
            MIDNIGHT + 3_000,
            MIDNIGHT + 5_000,
            gap_id="G1",
        ),
        _operation(
            6,
            "warmup_resume",
            DAY2,
            MIDNIGHT + 5_000,
            MIDNIGHT + 5_000,
            gap_id="G1",
            warmup_start=MIDNIGHT + 5_000 - 300_000,
        ),
        _operation(7, "online", DAY2, MIDNIGHT + 5_000, MIDNIGHT + 10_000),
        _operation(8, "panel_terminal", DAY2, MIDNIGHT + 10_000, MIDNIGHT + 10_000),
    )


def _write_framework(path: Path) -> Path:
    operations = _operations()
    payload: dict[str, Any] = {
        "schema_version": f03_framework.PLAN_SCHEMA_VERSION,
        "identity": "test-owner-restart-framework",
        "continuous_plan": {
            "same_restart_manifest_both_arms": True,
            "arm_economic_state_isolated": True,
            "utc_midnight_policy": "accounting_only_no_flatten_no_state_reset",
            "gap_policy": "clear_orders_queue_then_past_only_warmup",
            "restart_timeline_sha256": "b" * 64,
        },
        "comparison": {
            "same_restart_schedule": True,
            "arm_mutable_state_isolated": True,
            "utc_midnight_state_reset": False,
            "daily_forced_flat": False,
        },
        "operations": [asdict(row) for row in operations],
        "operation_tape_sha256": canonical_sha256([asdict(row) for row in operations]),
        "blockers": ["irrelevant_f03_policy_artifacts_unbound"],
        "execution_eligible": False,
        "economic_outcomes_read": False,
        "promotion_authorized": False,
        "action_authorized": False,
        "live_authorized": False,
    }
    payload["plan_identity_sha256"] = canonical_sha256(payload)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    (path.parent / f03_framework.PLAN_SUCCESS).write_text(
        sha256_file(path) + "\n",
        encoding="ascii",
    )
    return path


def _write_daily_panel(root: Path, *, policy_sha256: str) -> Path:
    root.mkdir(parents=True)
    report = {
        "schema_version": f"{daily_full_path.SCHEMA_VERSION}.report",
        "identity": daily_full_path.IDENTITY,
        "status": "owner_repeated_policy_historical_full_path_economics_complete",
        "panel": {
            "days": 50,
            "prefix_days": 40,
            "added_days": 10,
            "daily_fresh_start": True,
            "continuous_replay": False,
        },
        "permissions": {
            "research_supported": False,
            "strict_native_queue_authority": False,
            "receive_time_transport_authority": False,
            "continuous_replay_authority": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    report_path = root / "report.json"
    report_path.write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
    manifest = {
        "schema_version": f"{daily_full_path.SCHEMA_VERSION}.panel_manifest",
        "identity": daily_full_path.IDENTITY,
        "policy": {"sha256": policy_sha256},
        "files": [
            {
                "relative_path": "report.json",
                "sha256": sha256_file(report_path),
                "bytes": report_path.stat().st_size,
            }
        ],
        "permissions": report["permissions"],
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
    (root / daily_full_path.PANEL_SUCCESS).write_text(
        sha256_file(manifest_path) + "\n",
        encoding="ascii",
    )
    return root


class _Evaluator:
    _binding_error = None

    def __init__(self, policy_sha256: str) -> None:
        self.policy_sha256 = policy_sha256

    def audit(self) -> dict[str, int]:
        return {"evaluations": 0}


def _runtime_loader(path: Path, *, expected_policy_sha256: str) -> _Evaluator:
    assert sha256_file(path) == expected_policy_sha256
    return _Evaluator(expected_policy_sha256)


def _preflight(tmp_path: Path, *, admit_daily: bool) -> dict[str, Any]:
    policy = tmp_path / "policy.json"
    policy.write_text('{"policy":"owner"}\n', encoding="ascii")
    policy_sha = sha256_file(policy)
    daily = tmp_path / "daily-panel"
    if admit_daily:
        _write_daily_panel(daily, policy_sha256=policy_sha)
    return subject.prepare_preflight(
        daily_panel=daily,
        framework_plan=_write_framework(tmp_path / "framework" / "execution-plan.json"),
        policy_path=policy,
        expected_policy_sha256=policy_sha,
        runtime_loader=_runtime_loader,
    )


def test_preflight_blocks_until_daily_50d_full_path_is_admitted(tmp_path: Path) -> None:
    payload = _preflight(tmp_path, admit_daily=False)

    assert payload["mechanics_execution_eligible"] is False
    assert payload["blockers"] == [
        "daily_fresh_start_50d_full_path_panel_not_admitted"
    ]
    assert payload["permissions"]["continuous_economic_result_authorized"] is False
    assert payload["permissions"]["receive_time_transport_authority"] is False
    with pytest.raises(subject.OwnerRestartAwareError, match="mechanics blocked"):
        subject.validate_preflight(payload, require_eligible=True)


def test_admitted_preflight_keeps_midnight_inside_one_online_epoch(tmp_path: Path) -> None:
    payload = _preflight(tmp_path, admit_daily=True)

    subject.validate_preflight(payload, require_eligible=True)
    assert payload["mechanics_execution_eligible"] is True
    assert payload["framework"]["utc_accounting_boundary_count"] == 1
    assert payload["framework"]["utc_boundaries_embedded_inside_epochs"] is True
    assert payload["framework"]["minimum_restart_warmup_ms"] == 300_000
    midnight = payload["state_contract"]["utc_midnight"]
    assert midnight["cooldown_preserved"] is True
    assert midnight["ema_preserved"] is True
    maintenance = payload["state_contract"]["planned_maintenance"]
    assert maintenance["inventory_flattened"] is False
    assert maintenance["process_local_cooldown_and_ema_reset"] is True


def test_planned_restart_preserves_economics_but_clears_runtime_state() -> None:
    campaign = EconomicCampaignState(
        campaign_id="campaign-1",
        side="SHORT",
        start_ts_ms=START,
        start_equity_usdc=0.0,
        peak_abs_inventory_btc=0.002,
    )
    state = ContinuousReplayState(
        arm_id="control",
        checkpoint_ts_ms=START,
        cash_usdc=100.0,
        position_btc=-0.001,
        average_entry_price=100_000.0,
        cumulative_realized_pnl_usdc=0.0,
        cumulative_fees_usdc=0.0,
        equity_anchor_usdc=0.0,
        last_mark_price=100_000.0,
        cumulative_pnl_usdc=0.0,
        economic_campaign=campaign,
    )

    restarted = state.for_planned_restart(START + 1_000)

    assert restarted.cash_usdc == state.cash_usdc
    assert restarted.position_btc == state.position_btc
    assert restarted.average_entry_price == state.average_entry_price
    assert restarted.economic_campaign == campaign
    assert restarted.orders_terminal is True
    assert "fill_cooldown" in restarted.runtime_reset_fields
    assert "markout_ema" in restarted.runtime_reset_fields
    assert "signal_runtime" in restarted.runtime_reset_fields


class _MissingFeatureProvider:
    def load_epoch(self, epoch: AuthoritativeReplayEpoch) -> None:
        del epoch
        return None


class _SupportedFeatureProvider:
    def __init__(self, feature_input: subject.OwnerEpochFeatureInput) -> None:
        self.feature_input = feature_input

    def load_epoch(
        self, epoch: AuthoritativeReplayEpoch
    ) -> subject.OwnerEpochFeatureInput:
        del epoch
        return self.feature_input


class _Emitter:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

    def audit(self) -> SimpleNamespace:
        return SimpleNamespace(
            feature_block="M2",
            windows_consumed=1,
            snapshots_emitted=1,
            fallback_snapshots=0,
            last_window_right_ts_ns=1,
            last_feature_ready_ts_ns=1,
            warmup_admitted=True,
            economic_outcomes_read=False,
        )


def _epoch() -> AuthoritativeReplayEpoch:
    return AuthoritativeReplayEpoch(
        epoch_id="epoch-0001",
        start_ts_ms=START + 300_000,
        quote_stop_ts_ms=START + 600_000,
        end_ts_ms=START + 601_000,
        warmup_lookback_start_ts_ms=START,
        gap_id="G1",
        gap_end_ts_ms=START + 700_000,
        utc_boundaries_ts_ms=(),
        source_days=(DAY1,),
        random_seed=1,
        random_path_sha256=H64,
        terminal=False,
    )


def test_missing_m2_is_exact_control_runtime_fallback(tmp_path: Path) -> None:
    policy = tmp_path / "policy.json"
    policy.write_text("{}", encoding="ascii")
    binding = subject.bind_epoch_runtime(
        arm=subject.CANDIDATE_ARM,
        epoch=_epoch(),
        params={"gamma": 0.1},
        feature_provider=_MissingFeatureProvider(),
        policy_path=policy,
        expected_policy_sha256=sha256_file(policy),
        runtime_loader=_runtime_loader,
        emitter_factory=_Emitter,
    )

    assert binding.mode == "missing_m2_control_fallback"
    assert binding.params == {"gamma": 0.1}
    assert "cooldown_duration_policy_evaluator" not in binding.params
    assert binding.receipt()["candidate_effective_policy"] == "control"
    assert binding.receipt()["missing_m2_control_fallback"] is True


def test_supported_m2_binds_owner_evaluator_and_streaming_emitter(tmp_path: Path) -> None:
    policy = tmp_path / "policy.json"
    policy.write_text("{}", encoding="ascii")
    policy_sha = sha256_file(policy)
    feature_input = subject.OwnerEpochFeatureInput(
        observation_factory=lambda: iter(()),
        warmup_cutoff_ts_ns=1_000_000_000,
        warmup_identity="warmup",
        identity_hashes={
            name: H64
            for name in (
                "config_sha256",
                "code_sha256",
                "model_sha256",
                "p3_sha256",
                "feature_dag_sha256",
                "execution_abi_sha256",
                "baseline_identity_sha256",
            )
        },
        source_cursor_prefixes={
            "market": "market",
            "depth": "depth",
            "trade": "trade",
        },
        support_binding={"supported": True},
    )

    binding = subject.bind_epoch_runtime(
        arm=subject.CANDIDATE_ARM,
        epoch=_epoch(),
        params={"gamma": 0.1},
        feature_provider=_SupportedFeatureProvider(feature_input),
        policy_path=policy,
        expected_policy_sha256=policy_sha,
        runtime_loader=_runtime_loader,
        emitter_factory=_Emitter,
    )

    assert binding.mode == "owner_policy"
    assert isinstance(binding.params["cooldown_v2_snapshot_emitter"], _Emitter)
    assert isinstance(binding.params["cooldown_duration_policy_evaluator"], _Evaluator)
    assert binding.receipt()["candidate_effective_policy"] == "owner_boolean_cooldown"


def test_daily_cache_epoch_provider_relabels_restart_local_warmup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    epoch = _epoch()
    day_root = tmp_path / DAY1
    day_root.mkdir(parents=True)
    (day_root / "manifest.json").write_text("{}", encoding="ascii")
    before = CausalWindowObservation(
        left_ts_ns=(epoch.start_ts_ms - 200) * 1_000_000,
        right_ts_ns=(epoch.start_ts_ms - 100) * 1_000_000,
        feature_ready_ts_ns=(epoch.start_ts_ms - 50) * 1_000_000,
        market_generation=1,
        depth_generation=1,
        values={},
        warmup_admitted=True,
    )
    after = replace_observation(
        before,
        left_ts_ns=(epoch.start_ts_ms + 100) * 1_000_000,
        right_ts_ns=(epoch.start_ts_ms + 200) * 1_000_000,
        feature_ready_ts_ns=(epoch.start_ts_ms + 250) * 1_000_000,
        market_generation=2,
        depth_generation=2,
    )
    cache = SimpleNamespace(
        day_root=day_root,
        manifest={
            "exact_queue_policy_eligible": False,
            "action_authorized": False,
            "live_policy_authorized": False,
            "source_binding_sha256": H64,
            "first_left_ts_ns": epoch.warmup_lookback_start_ts_ms * 1_000_000,
            "last_right_ts_ns": epoch.end_ts_ms * 1_000_000,
        },
        observations=lambda: iter((before, after)),
        observations_between=lambda **kwargs: iter((before, after)),
    )
    monkeypatch.setattr(subject, "open_admitted_observation_cache", lambda *args, **kwargs: cache)
    provider = subject.DailyObservationCacheEpochProvider(
        tmp_path,
        identity_hashes={
            name: H64
            for name in (
                "config_sha256",
                "code_sha256",
                "model_sha256",
                "p3_sha256",
                "feature_dag_sha256",
                "execution_abi_sha256",
                "baseline_identity_sha256",
            )
        },
    )

    feature_input = provider.load_epoch(epoch)

    assert feature_input is not None
    rows = tuple(feature_input.observation_factory())
    assert [row.warmup_admitted for row in rows] == [False, True]
    assert feature_input.support_binding["receive_time_transport_authority"] is False


def test_daily_cache_epoch_provider_uses_target_cache_for_d_minus_one_warmup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    epoch = AuthoritativeReplayEpoch(
        epoch_id="epoch-d-minus-one",
        start_ts_ms=START + 1_000,
        quote_stop_ts_ms=START + 5_000,
        end_ts_ms=START + 6_000,
        warmup_lookback_start_ts_ms=START - 300_000,
        gap_id="G1",
        gap_end_ts_ms=START + 7_000,
        utc_boundaries_ts_ms=(),
        source_days=(DAY1,),
        random_seed=1,
        random_path_sha256=H64,
        terminal=False,
    )
    day_root = tmp_path / DAY1
    day_root.mkdir(parents=True)
    (day_root / "manifest.json").write_text("{}", encoding="ascii")
    warmup_row = CausalWindowObservation(
        left_ts_ns=(START - 200_000) * 1_000_000,
        right_ts_ns=(START - 199_900) * 1_000_000,
        feature_ready_ts_ns=(START - 199_900) * 1_000_000,
        market_generation=1,
        depth_generation=1,
        values={},
        warmup_admitted=True,
    )
    cache = SimpleNamespace(
        day_root=day_root,
        manifest={
            "exact_queue_policy_eligible": False,
            "action_authorized": False,
            "live_policy_authorized": False,
            "source_binding_sha256": H64,
            "first_left_ts_ns": (START - DAY_MS) * 1_000_000,
            "last_right_ts_ns": (START + DAY_MS) * 1_000_000,
        },
        observations=lambda: iter((warmup_row,)),
        observations_between=lambda **kwargs: iter((warmup_row,)),
    )
    monkeypatch.setattr(
        subject,
        "open_admitted_observation_cache",
        lambda *args, **kwargs: cache,
    )
    provider = subject.DailyObservationCacheEpochProvider(
        tmp_path,
        identity_hashes={
            name: H64
            for name in (
                "config_sha256",
                "code_sha256",
                "model_sha256",
                "p3_sha256",
                "feature_dag_sha256",
                "execution_abi_sha256",
                "baseline_identity_sha256",
            )
        },
    )

    feature_input = provider.load_epoch(epoch)

    assert feature_input is not None
    rows = tuple(feature_input.observation_factory())
    assert len(rows) == 1
    assert rows[0].feature_ready_ts_ns == warmup_row.feature_ready_ts_ns
    assert rows[0].warmup_admitted is False
    assert feature_input.support_binding["days"] == [DAY1]
    assert feature_input.support_binding["coverage_calendar_days"] == [
        "2025-12-31",
        DAY1,
    ]


def replace_observation(
    row: CausalWindowObservation, **changes: Any
) -> CausalWindowObservation:
    payload = asdict(row)
    payload.update(changes)
    return CausalWindowObservation(**payload)


def test_adapter_rejects_nonidentical_initial_economic_state(tmp_path: Path) -> None:
    preflight = _preflight(tmp_path, admit_daily=True)
    params = {
        "ml_enabled": True,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }
    bindings = {
        arm: AdapterArmBinding(arm, params, H64, 10_000) for arm in subject.ARMS
    }

    def initial(arm: str, cash: float) -> ContinuousReplayState:
        return ContinuousReplayState(
            arm_id=arm,
            checkpoint_ts_ms=MIDNIGHT - 5_000,
            cash_usdc=cash,
            position_btc=0.0,
            average_entry_price=0.0,
            cumulative_realized_pnl_usdc=0.0,
            cumulative_fees_usdc=0.0,
            equity_anchor_usdc=cash,
            last_mark_price=100.0,
            cumulative_pnl_usdc=0.0,
        )

    with pytest.raises(subject.OwnerRestartAwareError, match="common economics"):
        subject.OwnerBooleanCooldownRestartAwareAdapter(
            preflight=preflight,
            plan_identity_sha256=H64,
            operations=_operations(),
            arm_bindings=bindings,
            shared_input_provider=SimpleNamespace(load_day=lambda **kwargs: None),
            initial_states={
                subject.CONTROL_ARM: initial(subject.CONTROL_ARM, 0.0),
                subject.CANDIDATE_ARM: initial(subject.CANDIDATE_ARM, 1.0),
            },
            feature_provider=_MissingFeatureProvider(),
            output_root=tmp_path / "output",
            panel_cancel_drain_ms=1_000,
            runtime_identity_sha256=H64,
        )


def test_adapter_binds_policy_and_keeps_midnight_inside_epoch(tmp_path: Path) -> None:
    preflight = _preflight(tmp_path, admit_daily=True)
    policy_path = Path(preflight["owner_policy"]["path"])
    policy_sha = str(preflight["owner_policy"]["sha256"])
    params = {
        "ml_enabled": True,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }
    bindings = {
        subject.CONTROL_ARM: AdapterArmBinding(
            subject.CONTROL_ARM,
            params,
            H64,
            10_000,
        ),
        subject.CANDIDATE_ARM: AdapterArmBinding(
            subject.CANDIDATE_ARM,
            params,
            policy_sha,
            10_000,
        ),
    }

    def initial(arm: str) -> ContinuousReplayState:
        return ContinuousReplayState(
            arm_id=arm,
            checkpoint_ts_ms=MIDNIGHT - 5_000,
            cash_usdc=0.0,
            position_btc=0.0,
            average_entry_price=0.0,
            cumulative_realized_pnl_usdc=0.0,
            cumulative_fees_usdc=0.0,
            equity_anchor_usdc=0.0,
            last_mark_price=100.0,
            cumulative_pnl_usdc=0.0,
        )

    adapter = subject.OwnerBooleanCooldownRestartAwareAdapter(
        preflight=preflight,
        operations=_operations(),
        arm_bindings=bindings,
        shared_input_provider=SimpleNamespace(load_day=lambda **kwargs: None),
        initial_states={arm: initial(arm) for arm in subject.ARMS},
        feature_provider=_MissingFeatureProvider(),
        output_root=tmp_path / "output",
        panel_cancel_drain_ms=1_000,
        policy_path=policy_path,
        expected_policy_sha256=policy_sha,
        runtime_identity_sha256=H64,
    )

    assert len(adapter.owner_plan_identity_sha256) == 64
    assert adapter.epochs[0].start_ts_ms < MIDNIGHT < adapter.epochs[0].quote_stop_ts_ms
    assert adapter.epochs[0].utc_boundaries_ts_ms == (MIDNIGHT,)
