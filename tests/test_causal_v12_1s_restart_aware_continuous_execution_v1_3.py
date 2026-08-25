from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("narrowgate_cpp")

from models import backtest_tick as bt
from models.replay.narrowgate_continuous_tick_adapter import (
    AdapterArmBinding,
    NarrowGateContinuousAdapterError,
    NarrowGateContinuousTickReplayAdapter,
    ReplayDayInput,
    compile_authoritative_epochs,
)
from models.replay.replay_state_checkpoint import ContinuousReplayState
from models.replay.restart_aware_continuous_ab import canonical_sha256
from models.replay.restart_aware_continuous_execution import ContinuousOperation
from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_restart_aware_continuous_execution_v1_3 as subject,
)

DAY_MS = 86_400_000
DAY1 = "2026-01-01"
DAY2 = "2026-01-02"
START = int(np.datetime64(DAY1, "ms").astype(np.int64))
MIDNIGHT = START + DAY_MS
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
        operation_id=f"test-op-{sequence:02d}-{kind}",
        kind=kind,
        day=day,
        start_ts_ms=start,
        end_ts_ms=end,
        source_day=day,
        gap_id=gap_id,
        warmup_lookback_start_ts_ms=warmup_start,
        exact_queue_authority=kind == "online" and day == DAY1,
        exact_lifecycle_authority=kind in {"online", "cancel_drain"} and day == DAY1,
        continuous_economic_sensitivity_authority=True,
        source_identity_sha256=canonical_sha256({"day": day}),
        source_artifact_manifest_sha256=H64,
        restart_timeline_sha256="b" * 64,
        random_seed=20260805 + sequence,
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
            warmup_start=MIDNIGHT + 4_000,
        ),
        _operation(7, "online", DAY2, MIDNIGHT + 5_000, MIDNIGHT + 10_000),
        _operation(8, "panel_terminal", DAY2, MIDNIGHT + 10_000, MIDNIGHT + 10_000),
    )


def _ml_data(ts: np.ndarray) -> tuple[object, ...]:
    ready = ts[::10].copy()
    if not len(ready):
        ready = ts[:1].copy()
    zero = np.zeros(len(ready), dtype=np.float64)
    return (
        ready,
        np.full(len(ready), 0.5, dtype=np.float64),
        zero.copy(),
        zero.copy(),
        np.full(len(ready), 0.5, dtype=np.float64),
        np.full(len(ready), 0.5, dtype=np.float64),
        *([zero.copy()] * len(bt.XMARKET_REPLAY_FEATURE_COLUMNS)),
        {},
    )


def _day_input(day: str, start: int, end: int, *, provider: bool) -> ReplayDayInput:
    ts = np.arange(start, end, 100, dtype=np.int64)
    price = np.full(len(ts), 100.0, dtype=np.float64)
    qty = np.zeros(len(ts), dtype=np.float64)
    maker = np.zeros(len(ts), dtype=np.uint8)
    # Taker sells produce real BUY maker fills.  One lies inside the first
    # maintenance cancel/ACK race at quote_stop+200ms.
    for event_ts in (MIDNIGHT - 3_800, MIDNIGHT + 2_200):
        hit = np.flatnonzero(ts == event_ts)
        if hit.size:
            price[int(hit[0])] = 99.0
            qty[int(hit[0])] = 0.001
            maker[int(hit[0])] = 1
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": price,
            "quantity": qty,
            "is_buyer_maker": maker,
            "_is_execution_trade": np.ones(len(ts), dtype=np.bool_),
        }
    )
    var_ts = ts[::10].copy()
    window = SimpleNamespace(
        trades=trades,
        var_ts_ms=var_ts,
        var_ssq=np.full(len(var_ts), 0.01, dtype=np.float64),
        var_ti=np.ones(len(var_ts), dtype=np.float64),
        var_retsq=np.zeros(len(var_ts), dtype=np.float64),
        bbo_data=None,
        l2_data=None,
        ml_data=None,
    )
    profile = "provider_normalized" if provider else "native"
    return ReplayDayInput(
        day=day,
        window=window,
        ml_data=_ml_data(ts),
        market_window_sha256=canonical_sha256({"window": day}),
        overlay_identity_sha256=canonical_sha256({"overlay": day}),
        source_identity_sha256=canonical_sha256({"source": day}),
        source_profile=profile,
        exact_queue_authority=not provider,
        exact_lifecycle_authority=not provider,
    )


class _Provider:
    def __init__(self) -> None:
        self.rows = {
            DAY1: _day_input(DAY1, MIDNIGHT - 6_000, MIDNIGHT, provider=False),
            DAY2: _day_input(DAY2, MIDNIGHT, MIDNIGHT + 11_000, provider=True),
        }

    def load_day(self, *, arm_id: str, day: str) -> ReplayDayInput:
        del arm_id
        return self.rows[day]


def _params() -> dict[str, object]:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "p3_kappa_eff_override": 1.0,
        "maker_fee": 0.0,
        "max_inventory": 0.01,
        "order_size": 0.001,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": True,
        "dynamic_cap_enabled": False,
        "max_spread_bps": 100.0,
        "spread_cap_mode": "compress",
        "max_exec_book_age_s": 0.0,
        "ml_enabled": True,
        "adverse_guard_enabled": True,
        "adverse_toxicity_threshold": 2.0,
        "new_order_latency_ms": 0,
        "cancel_order_latency_ms": 500,
        "latency_sampler_version": "keyed_splitmix64_v1",
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_live_enabled": False,
        "trace_quotes_max": 1_000,
        "trace_fills_max": 1_000,
        "collect_curves": False,
    }


def _initial(arm: str) -> ContinuousReplayState:
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
        orders_terminal=True,
        feature_warmup_ready=True,
        quoting_enabled=False,
    )


def _adapter(root: Path) -> NarrowGateContinuousTickReplayAdapter:
    bindings = {
        "control": AdapterArmBinding(
            "control", _params(), canonical_sha256({"policy": "control"}), 10_000
        ),
        "candidate": AdapterArmBinding(
            "candidate", _params(), canonical_sha256({"policy": "candidate"}), 1_000
        ),
    }
    return NarrowGateContinuousTickReplayAdapter(
        plan_identity_sha256=canonical_sha256({"plan": "integration"}),
        operations=_operations(),
        arm_bindings=bindings,
        input_provider=_Provider(),
        initial_states={arm: _initial(arm) for arm in bindings},
        output_root=root,
        panel_cancel_drain_ms=1_000,
    )


def test_epoch_compiler_keeps_midnight_inside_real_engine_epoch() -> None:
    epochs = compile_authoritative_epochs(_operations(), panel_cancel_drain_ms=1_000)
    assert len(epochs) == 2
    assert epochs[0].start_ts_ms < MIDNIGHT < epochs[0].quote_stop_ts_ms
    assert epochs[0].utc_boundaries_ts_ms == (MIDNIGHT,)
    assert epochs[0].gap_end_ts_ms == MIDNIGHT + 5_000
    assert epochs[1].warmup_lookback_start_ts_ms == MIDNIGHT + 4_000


def test_epoch_compiler_carries_across_restart_without_online_segment() -> None:
    operations = (
        _operation(1, "online", DAY1, START + 1_000, START + 2_000),
        _operation(
            2,
            "cancel_drain",
            DAY1,
            START + 2_000,
            START + 3_000,
            gap_id="G1",
        ),
        _operation(
            3,
            "offline_gap",
            DAY1,
            START + 3_000,
            START + 4_000,
            gap_id="G1",
        ),
        _operation(
            4,
            "warmup_resume",
            DAY1,
            START + 4_000,
            START + 4_000,
            gap_id="G1",
            warmup_start=START + 3_000,
        ),
        _operation(
            5,
            "cancel_drain",
            DAY1,
            START + 4_000,
            START + 5_000,
            gap_id="G2",
        ),
        _operation(
            6,
            "offline_gap",
            DAY1,
            START + 5_000,
            START + 7_000,
            gap_id="G2",
        ),
        _operation(
            7,
            "warmup_resume",
            DAY1,
            START + 7_000,
            START + 7_000,
            gap_id="G2",
            warmup_start=START + 6_000,
        ),
        _operation(8, "online", DAY1, START + 7_000, START + 9_000),
        _operation(9, "panel_terminal", DAY1, START + 9_000, START + 9_000),
    )

    epochs = compile_authoritative_epochs(operations, panel_cancel_drain_ms=1_000)

    assert len(epochs) == 2
    assert epochs[0].gap_end_ts_ms == epochs[1].start_ts_ms == START + 7_000
    assert epochs[0].gap_id == "G1+G2"


def test_epoch_compiler_rejects_uncovered_inter_epoch_time() -> None:
    operations = (
        _operation(1, "online", DAY1, START + 1_000, START + 2_000),
        _operation(
            2,
            "cancel_drain",
            DAY1,
            START + 2_000,
            START + 3_000,
            gap_id="G1",
        ),
        _operation(
            3,
            "offline_gap",
            DAY1,
            START + 3_000,
            START + 4_000,
            gap_id="G1",
        ),
        _operation(
            4,
            "warmup_resume",
            DAY1,
            START + 4_000,
            START + 4_000,
            gap_id="G1",
            warmup_start=START + 3_000,
        ),
        _operation(5, "online", DAY1, START + 5_000, START + 7_000),
        _operation(6, "panel_terminal", DAY1, START + 7_000, START + 7_000),
    )

    with pytest.raises(
        NarrowGateContinuousAdapterError,
        match="does not cover the inter-epoch maintenance bridge",
    ):
        compile_authoritative_epochs(operations, panel_cancel_drain_ms=1_000)


def test_real_tick_replay_crosses_midnight_gap_and_resumes_deterministically(
    tmp_path: Path,
) -> None:
    partial_root = tmp_path / "partial"
    first = _adapter(partial_root).run(max_epochs=1)
    assert first["epochs_completed_this_call"] == 1
    admitted_control = partial_root / "checkpoints" / "control" / "epoch-0001.json"
    orphan = partial_root / "checkpoints" / "control" / "epoch-9999.json"
    orphan.write_bytes(admitted_control.read_bytes())
    orphan.with_suffix(".success").write_text("unadmitted\n", encoding="ascii")
    resumed = _adapter(partial_root).run()
    assert resumed["last_completed_epoch"] == 2

    clean_root = tmp_path / "clean"
    clean = _adapter(clean_root).run()
    assert clean["last_completed_epoch"] == 2
    assert resumed["checkpoint_sha256"] == clean["checkpoint_sha256"]

    first_receipt = json.loads(
        (partial_root / "receipts" / "epoch-0001.json").read_text(encoding="utf-8")
    )
    assert first_receipt["same_random_path"] is True
    assert all(row["actual_tick_replay_used"] for row in first_receipt["arms"].values())
    assert all(row["quote_count"] > 0 for row in first_receipt["arms"].values())
    assert all(row["fill_count"] > 0 for row in first_receipt["arms"].values())
    assert all(row["terminal_fill_count"] > 0 for row in first_receipt["arms"].values())
    assert all(row["cancel_request_count"] > 0 for row in first_receipt["arms"].values())
    assert all(row["cancel_ack_count"] > 0 for row in first_receipt["arms"].values())
    assert all(
        row["offline_gap"]["market_event_trading_enabled"] is False
        and row["offline_gap"]["inventory_unchanged"] is True
        for row in first_receipt["arms"].values()
    )
    provider_rows = [
        authority
        for row in first_receipt["arms"].values()
        for authority in row["source_authority"]
        if authority["source_profile"] == "provider_normalized"
    ]
    assert provider_rows
    assert all(
        not row["exact_queue_authority"]
        and not row["exact_lifecycle_authority"]
        and not row["q90_authority"]
        for row in provider_rows
    )

    second_receipt = json.loads(
        (partial_root / "receipts" / "epoch-0002.json").read_text(encoding="utf-8")
    )
    assert all(
        row["warmup"]["required"]
        and row["warmup"]["market_event_count"] > 0
        and row["warmup"]["quoting_enabled"] is False
        for row in second_receipt["arms"].values()
    )
    for arm in ("control", "candidate"):
        checkpoint = json.loads(
            (partial_root / "checkpoints" / arm / "epoch-0001.json").read_text(
                encoding="utf-8"
            )
        )
        assert checkpoint["state"]["position_btc"] != 0.0
        assert checkpoint["engine_state"]["active_orders"] == []
        assert checkpoint["engine_state"]["queue_positions"] == []
        assert checkpoint["engine_state"]["feature_model_held_state"][
            "cleared_by_production_restart"
        ]


def test_v13_plan_is_outcome_blind_and_cli_adapter_is_not_injection_only() -> None:
    plan = subject.validate_execution_plan(subject.DEFAULT_OUTPUT_ROOT / subject.PLAN_FILENAME)
    assert plan["execution_eligible"] is False
    assert plan["economic_outcomes_read"] is False
    assert plan["authoritative_epoch_count"] > 0
    assert "narrowgate_cpp_extension" in plan["runtime_artifacts"]
    assert "shared_continuous_adapter" in plan["runtime_artifacts"]
    with pytest.raises(subject.F03ContinuousExecutionV13Error, match="actual run blocked"):
        subject.build_concrete_adapter(plan)


def test_arm_binding_rejects_old_operational_actions() -> None:
    params = _params()
    params["buy_fill_selection_live_enabled"] = True
    with pytest.raises(NarrowGateContinuousAdapterError, match="BUY fill selector"):
        AdapterArmBinding("control", params, H64, 10_000).validate()
