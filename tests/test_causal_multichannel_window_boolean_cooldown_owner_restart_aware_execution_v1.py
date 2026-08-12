from __future__ import annotations

import json
import pickle
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from models.replay.narrowgate_continuous_tick_adapter import ReplayDayInput
from models.replay.restart_aware_continuous_ab import canonical_sha256, sha256_file
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_owner_restart_aware_execution_v1 as subject,
)

H64 = "a" * 64


def _write_canonical(
    path: Path,
    payload: dict[str, Any],
    *,
    hash_field: str,
) -> dict[str, Any]:
    row = dict(payload)
    row[hash_field] = canonical_sha256(row)
    subject._atomic_json(path, row)
    subject._atomic_text(path.with_suffix(".success"), sha256_file(path) + "\n")
    return row


def test_preflight_requires_explicit_owner_continuation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = {
        "preflight_identity_sha256": H64,
        "blockers": [],
        "framework": {
            "operation_tape_sha256": "b" * 64,
            "path": "/tmp/framework.json",
        },
        "permissions": {
            "strict_queue_authority": False,
            "receive_time_transport_authority": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    monkeypatch.setattr(subject.owner_abi, "prepare_preflight", lambda **kwargs: owner)
    monkeypatch.setattr(
        subject,
        "_inspect_f03_control_plan",
        lambda path: (
            {
                "control_policy_bound": True,
                "operation_tape_sha256": "b" * 64,
                "control_policy": {"days": {}},
            },
            [],
        ),
    )
    monkeypatch.setattr(subject, "_required_warmup_context_days", lambda **kwargs: ())

    blocked = subject.preflight()
    allowed = subject.preflight(owner_continue_after_daily=True)

    assert blocked["execution_eligible"] is False
    assert blocked["blockers"] == [
        "owner_continuation_after_daily_not_precommitted"
    ]
    assert allowed["execution_eligible"] is True
    assert allowed["owner_continuation"]["mode"] == "explicit_owner_cli_precommit"
    assert allowed["economic_outcomes_read"] is False
    assert allowed["permissions"]["action_authorized"] is False


def test_f03_shared_provider_demotes_exact_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = ReplayDayInput(
        day="2026-01-01",
        window=SimpleNamespace(ml_data=None),
        ml_data=(object(),),
        market_window_sha256=H64,
        overlay_identity_sha256="b" * 64,
        source_identity_sha256="c" * 64,
        source_profile="native",
        exact_queue_authority=True,
        exact_lifecycle_authority=True,
    )

    class FakeProvider:
        def __init__(self, policies: Any) -> None:
            assert set(policies) == {subject.CONTROL_ARM}

        def load_day(self, *, arm_id: str, day: str) -> ReplayDayInput:
            assert arm_id == subject.CONTROL_ARM
            assert day == row.day
            return row

    monkeypatch.setattr(subject.f03_binding, "_BoundInputProvider", FakeProvider)
    provider = subject.SharedF03ControlInputProvider({"policy": "control"})

    observed = provider.load_day(day=row.day)

    assert observed.source_profile == "native"
    assert observed.exact_queue_authority is False
    assert observed.exact_lifecycle_authority is False


def test_f03_shared_provider_loads_warmup_context_without_scoring_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = "2026-04-16"
    data_path = tmp_path / "context.pkl"
    with data_path.open("wb") as handle:
        pickle.dump(
            {
                "window": SimpleNamespace(
                    ml_data=None,
                    trades=SimpleNamespace(),
                ),
                "ml_data": ([1, 2], [0.1, 0.2]),
            },
            handle,
        )
    binding = {
        "day": day,
        "kind": "warmup_context_only",
        "identity_sha256": "b" * 64,
        "source_identity_sha256": "c" * 64,
        "source_profile": "native",
        "data": {
            "path": str(data_path),
            "sha256": sha256_file(data_path),
            "size_bytes": data_path.stat().st_size,
        },
    }

    class FakeProvider:
        def __init__(self, policies: Any) -> None:
            assert policies

        def load_day(self, *, arm_id: str, day: str) -> ReplayDayInput:
            raise AssertionError("warmup-only context must not use scored-day provider")

    monkeypatch.setattr(subject.f03_binding, "_BoundInputProvider", FakeProvider)
    monkeypatch.setattr(
        subject,
        "_validate_warmup_context_binding",
        lambda row: dict(row),
    )
    provider = subject.SharedF03ControlInputProvider(
        {"policy": "control"},
        {day: binding},
    )

    observed = provider.load_day(day=day)

    assert observed.day == day
    assert observed.ml_data[0] == [1, 2]
    assert observed.exact_queue_authority is False
    assert observed.exact_lifecycle_authority is False


def test_build_adapter_routes_warmup_identity_to_runtime_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_tape_sha256 = "b" * 64
    preflight_identity_sha256 = "c" * 64
    warmup_identity_sha256 = "d" * 64
    preflight_calls: list[dict[str, Any]] = []
    adapter_calls: list[dict[str, Any]] = []
    owner_preflight = {
        "preflight_identity_sha256": preflight_identity_sha256,
        "framework": {"operation_tape_sha256": operation_tape_sha256},
    }

    def fake_prepare_preflight(**kwargs: Any) -> dict[str, Any]:
        preflight_calls.append(dict(kwargs))
        return owner_preflight

    def fake_adapter(**kwargs: Any) -> object:
        adapter_calls.append(dict(kwargs))
        return object()

    control_policy = {
        "policy_identity_sha256": "e" * 64,
        "cadence_ms": 10_000,
        "operational_config": {"path": str(tmp_path / "config.yaml")},
        "initial_state": {"path": str(tmp_path / "initial.json")},
    }
    plan = {
        "daily_panel": {"path": str(tmp_path / "daily.json")},
        "framework": {"path": str(tmp_path / "framework.json")},
        "owner_policy": {"path": str(tmp_path / "policy.json"), "sha256": "f" * 64},
        "preflight": {
            "owner_restart_adapter_preflight": {
                "preflight_identity_sha256": preflight_identity_sha256,
            }
        },
        "f03_shared_control_binding": {
            "path": str(tmp_path / "f03-plan.json"),
            "plan_identity_sha256": "1" * 64,
            "operation_tape_sha256": operation_tape_sha256,
            "control_policy": control_policy,
        },
        "warmup_context_days": {
            "2026-04-16": {"identity_sha256": warmup_identity_sha256}
        },
        "owner_feature_identity_hashes": {"code_sha256": H64},
        "observation_cache_root": str(tmp_path / "observations"),
        "execution_output_root": str(tmp_path / "execution"),
        "adapter_plan_identity_sha256": "2" * 64,
    }
    operation = SimpleNamespace(kind="cancel_drain", start_ts_ms=1, end_ts_ms=2)

    monkeypatch.setattr(
        subject,
        "_inspect_f03_control_plan",
        lambda path: ({"plan_identity_sha256": "1" * 64}, []),
    )
    monkeypatch.setattr(subject.owner_abi, "prepare_preflight", fake_prepare_preflight)
    monkeypatch.setattr(subject.owner_abi, "validate_preflight", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        subject.owner_abi,
        "_load_framework_binding",
        lambda path: {
            "operations": (operation,),
            "operation_tape_sha256": operation_tape_sha256,
        },
    )
    monkeypatch.setattr(subject.native_runner, "_load_formal_base_params", lambda path: {})
    monkeypatch.setattr(
        subject,
        "AdapterArmBinding",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )
    monkeypatch.setattr(
        subject.f03_binding,
        "_initial_state",
        lambda path, arm: SimpleNamespace(arm_id=arm),
    )
    monkeypatch.setattr(
        subject,
        "SharedF03ControlInputProvider",
        lambda *args, **kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        subject.owner_abi,
        "DailyObservationCacheEpochProvider",
        lambda *args, **kwargs: SimpleNamespace(),
    )
    monkeypatch.setattr(
        subject.owner_abi,
        "OwnerBooleanCooldownRestartAwareAdapter",
        fake_adapter,
    )

    subject._build_adapter_from_plan(plan)

    assert len(preflight_calls) == 1
    assert "warmup_context_identities" not in preflight_calls[0]
    assert adapter_calls[0]["warmup_context_identities"] == {
        "2026-04-16": warmup_identity_sha256
    }
    assert adapter_calls[0]["runtime_identity_sha256"] == H64


def _ledger_state(
    *,
    arm: str,
    terminal_pnl: float,
    day_values: tuple[float, float],
    campaign_values: tuple[float, float],
) -> dict[str, Any]:
    state = {
        "arm_id": arm,
        "cash_usdc": terminal_pnl,
        "position_btc": 0.0,
        "last_mark_price": 100.0,
        "equity_anchor_usdc": 0.0,
        "cumulative_pnl_usdc": terminal_pnl,
        "economic_campaign": None,
    }
    daily = [
        {
            "day": day,
            "start_equity_usdc": float(index),
            "end_equity_usdc": float(index) + value,
            "pnl_usdc": value,
            "start_inventory_btc": 0.001 * index,
            "end_inventory_btc": 0.001 * (index + 1),
            "end_mark_price": 100.0,
        }
        for index, (day, value) in enumerate(
            zip(("2026-01-01", "2026-01-02"), day_values, strict=True)
        )
    ]
    campaigns = [
        {
            "campaign_id": f"{arm}-campaign-{index}",
            "side": "LONG" if index == 1 else "SHORT",
            "start_ts_ms": index * 86_400_000,
            "end_ts_ms": index * 86_400_000 + 1,
            "start_equity_usdc": 0.0,
            "end_equity_usdc": value,
            "value_usdc": value,
            "peak_abs_inventory_btc": 0.002 * index,
        }
        for index, value in enumerate(campaign_values, start=1)
    ]
    return {
        "state": state,
        "day_start_equity_usdc": terminal_pnl,
        "day_start_inventory_btc": 0.0,
        "day_start": "2026-01-03",
        "daily_slices": daily,
        "closed_campaigns": campaigns,
        "gap_carries": [
            {
                "gap_id": f"{arm}-gap",
                "start_ts_ms": 1_000,
                "end_ts_ms": 2_000,
                "position_btc": 0.001,
                "start_mark_price": 100.0,
                "end_mark_price": 101.0,
                "pnl_usdc": 0.001,
            }
        ],
    }


def _write_checkpoint(
    root: Path,
    *,
    arm: str,
    epoch_id: str,
    previous: str,
    ledger: dict[str, Any],
) -> dict[str, Any]:
    path = root / "checkpoints" / arm / f"{epoch_id}.json"
    payload = {
        "schema_version": "test.checkpoint",
        "plan_identity_sha256": "d" * 64,
        "arm_id": arm,
        "epoch_id": epoch_id,
        "state": ledger["state"],
        "state_sha256": canonical_sha256(ledger["state"]),
        "ledger_state": ledger,
        "ledger_state_sha256": canonical_sha256(ledger),
        "engine_state": {},
        "engine_state_sha256": canonical_sha256({}),
        "mechanics_sha256": H64,
        "previous_checkpoint_sha256": previous,
        "economic_outcomes_read": False,
        "promotion_authorized": False,
    }
    checkpoint = _write_canonical(path, payload, hash_field="checkpoint_sha256")
    return {
        "path": str(path),
        "file_sha256": sha256_file(path),
        "checkpoint_sha256": checkpoint["checkpoint_sha256"],
    }


def _write_execution(root: Path) -> None:
    previous = {arm: "" for arm in subject.ARMS}
    for ordinal in (1, 2):
        epoch_id = f"epoch-{ordinal:04d}"
        refs: dict[str, Any] = {}
        for arm in subject.ARMS:
            if arm == subject.CONTROL_ARM:
                ledger = _ledger_state(
                    arm=arm,
                    terminal_pnl=float(ordinal),
                    day_values=(0.5, 0.5) if ordinal == 2 else (0.5, 0.0),
                    campaign_values=(0.4, 0.6),
                )
            else:
                ledger = _ledger_state(
                    arm=arm,
                    terminal_pnl=float(2 * ordinal),
                    day_values=(1.0, 1.0) if ordinal == 2 else (1.0, 0.0),
                    campaign_values=(0.8, 1.2),
                )
            refs[arm] = _write_checkpoint(
                root,
                arm=arm,
                epoch_id=epoch_id,
                previous=previous[arm],
                ledger=ledger,
            )
            previous[arm] = refs[arm]["checkpoint_sha256"]
        receipt_path = root / "receipts" / f"{epoch_id}.json"
        payload = {
            "schema_version": "test.receipt",
            "plan_identity_sha256": "d" * 64,
            "epoch": {"epoch_id": epoch_id},
            "arms": {
                subject.CONTROL_ARM: {
                    "fill_count": 10,
                    "random_path_sha256": H64,
                    "owner_runtime": {"mode": "control", "policy_audit": {}},
                },
                subject.CANDIDATE_ARM: {
                    "fill_count": 9,
                    "random_path_sha256": H64,
                    "owner_runtime": {
                        "mode": "owner_policy",
                        "policy_audit": {"evaluations": 3},
                    },
                },
            },
            "checkpoints": refs,
            "same_random_path": True,
            "economic_outcomes_read": False,
            "promotion_authorized": False,
        }
        _write_canonical(receipt_path, payload, hash_field="receipt_sha256")


def test_paired_finalizer_verifies_checkpoint_chain_and_admits_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution = tmp_path / "execution"
    _write_execution(execution)
    plan_path = tmp_path / "execution-plan.json"
    plan_path.write_text("{}\n", encoding="ascii")
    plan = {
        "plan_identity_sha256": "e" * 64,
        "preflight": {
            "owner_restart_adapter_preflight": {"preflight_identity_sha256": "f" * 64}
        },
        "framework": {"authoritative_epoch_count": 2},
        "adapter_plan_identity_sha256": "d" * 64,
        "execution_output_root": str(execution),
        "final_output_root": str(tmp_path / "final"),
        "owner_continuation": {
            "authorized": True,
            "continuation_identity_sha256": H64,
        },
        "permissions": {
            "strict_queue_authority": False,
            "receive_time_transport_authority": False,
            "research_supported": False,
            "action_authorized": False,
            "live_authorized": False,
        },
    }
    monkeypatch.setattr(subject, "validate_execution_plan", lambda *args, **kwargs: plan)

    report = subject.finalize(plan_path, bootstrap_draws=100, bootstrap_seed=7)

    assert report["economics"]["paired"]["terminal_mtm_pnl_delta_usdc"] == 2.0
    assert report["economics"]["paired"]["closed_campaign_value_delta_usdc"] == 1.0
    assert report["economics"]["paired"]["fill_retention"] == pytest.approx(0.9)
    assert report["economics"]["arms"][subject.CANDIDATE_ARM][
        "max_abs_inventory_btc"
    ] == pytest.approx(0.004)
    final = tmp_path / "final"
    assert (final / subject.FINAL_SUCCESS).read_text(encoding="ascii").strip() == sha256_file(
        final / "manifest.json"
    )
    manifest = json.loads((final / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["receipts"]) == 2
    assert manifest["permissions"]["live_authorized"] is False


def test_finalizer_refuses_missing_epoch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution = tmp_path / "execution"
    _write_execution(execution)
    (execution / "receipts" / "epoch-0002.success").unlink()
    plan = {
        "preflight": {
            "owner_restart_adapter_preflight": {"preflight_identity_sha256": "f" * 64}
        },
        "framework": {"authoritative_epoch_count": 2},
        "adapter_plan_identity_sha256": "d" * 64,
        "execution_output_root": str(execution),
    }
    monkeypatch.setattr(subject, "validate_execution_plan", lambda *args, **kwargs: plan)

    with pytest.raises(subject.OwnerContinuousExecutionError, match="atomic marker"):
        subject._load_complete_execution(plan)
