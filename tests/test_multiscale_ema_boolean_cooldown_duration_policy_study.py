from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from models.tick_data_types import HistoricalBBOData, HistoricalL2Data
from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_boolean_cooldown_duration_policy_study as study,
)


def _contract() -> dict[str, object]:
    def actions(first: int, second: int) -> list[dict[str, object]]:
        return [
            {
                "policy_id": "CONTROL_85N",
                "duration_s": None,
                "duration_semantics": "current control",
            },
            {
                "policy_id": f"FIXED_{first}S",
                "duration_s": first,
                "duration_semantics": "fixed total duration",
            },
            {
                "policy_id": f"FIXED_{second}S",
                "duration_s": second,
                "duration_semantics": "fixed total duration",
            },
        ]

    return {
        "duration_source": {
            "candidate_actions": {
                "BUY": actions(10, 20),
                "SELL": actions(30, 40),
            }
        }
    }


def _opportunities() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "opportunity_id": ["a", "b", "c", "d"],
            "diagnostic_order_sha256": ["04", "01", "03", "02"],
            "side": ["BUY", "BUY", "SELL", "SELL"],
            "role_at_fill": ["opener", "add", "opener", "add"],
        }
    )


def test_formal_task_plan_is_full_cartesian_not_sampled() -> None:
    tasks = study._build_tasks(_opportunities(), _contract(), limit=None)
    assert len(tasks) == 12
    assert {row[0]["opportunity_id"] for row in tasks} == {"a", "b", "c", "d"}
    by_opportunity: dict[str, set[str]] = {}
    for opportunity, action, _ in tasks:
        by_opportunity.setdefault(opportunity["opportunity_id"], set()).add(action.policy_id)
    assert by_opportunity["a"] == {"CONTROL_85N", "FIXED_10S", "FIXED_20S"}
    assert by_opportunity["c"] == {"CONTROL_85N", "FIXED_30S", "FIXED_40S"}
    fixed = next(action for _, action, _ in tasks if action.policy_id == "FIXED_10S")
    assert fixed.fixed_duration_s == 10
    assert fixed.fixed_duration_ms == 10_000
    assert fixed.payload()["fixed_duration_ms"] == 10_000


def test_limit_is_diagnostic_only_and_formal_finalize_rejects_it() -> None:
    tasks = study._build_tasks(_opportunities(), _contract(), limit=1)
    assert len(tasks) == 3
    assert {task[0]["opportunity_id"] for task in tasks} == {"b"}
    with pytest.raises(study.StudyError, match="limited diagnostic"):
        study._validate_formal_run_manifest(
            {
                "identity": study.IDENTITY,
                "utc_day": "2026-01-01",
                "scope": "diagnostic-limit-1",
                "limited_diagnostic": True,
                "formal_full_opportunity_coverage": False,
            },
            day="2026-01-01",
        )


def test_mechanics_projection_does_not_read_economic_values() -> None:
    forbidden = {
        "assignment_to_washout_value_usdc",
        "censor_time_mid_mark_usdc",
        "censor_time_executable_mark_usdc",
    }
    assert forbidden.isdisjoint(study.MECHANICS_COLUMNS)


def _replay_params() -> dict[str, object]:
    return {
        "gamma": 0.01,
        "kappa": 1.0,
        "order_size": 0.001,
        "max_inventory": 0.01,
        "requote_interval": 1.0,
        "rq_min": 1.0,
        "rq_max": 1.0,
        "requote_clock": "fixed",
        "maker_fee": 0.0,
        "taker_fee": 0.0,
        "tick_size": 0.1,
        "lot_size": 0.001,
        "queue_base": 0.0,
        "queue_decay": 0.0,
        "maker_fill_prob": 1.0,
        "use_bar_pricing": False,
        "replay_event_clock": "merged",
        "replay_clock_interval_ms": 100,
        "max_exec_book_age_s": 0.0,
        "collect_curves": False,
        "position_timeout": 0.0,
        "markout_ema_span_fills": 0,
        "fill_cooldown": 1.0,
        "fill_cooldown_reducing": 0.0,
        "fill_cooldown_apply_reducing": False,
        "fill_cooldown_consecutive_reset_policy": "opposite_fill_only",
        "fill_cooldown_clock_mode": "wall_time",
        "replay_initial_state_mode": "fresh_start",
        "trace_cooldown_duration_opportunities_max": 100,
        "trace_fills_max": 100,
        "dynamic_fill_hazard_action_enabled": False,
        "buy_fill_selection_live_enabled": False,
    }


def _synthetic_window() -> SimpleNamespace:
    base_ms = 1_700_000_000_000
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [base_ms + offset for offset in range(0, 9_000, 1_000)],
                dtype=np.int64,
            ),
            "price": np.asarray(
                [100.0, 96.6, 96.6, 90.0, 90.0, 90.0, 80.0, 80.0, 80.0],
                dtype=np.float64,
            ),
            "quantity": np.asarray(
                [0.0, 0.001, 0.0, 0.001, 0.0, 0.0, 0.001, 0.0, 0.0],
                dtype=np.float64,
            ),
            "is_buyer_maker": np.asarray(
                [False, True, False, True, False, False, True, False, False],
                dtype=np.uint8,
            ),
        }
    )
    ts_ms = trades["transact_time"].to_numpy(dtype=np.int64)
    best_bid = np.full(len(trades), 99.9, dtype=np.float64)
    best_ask = np.full(len(trades), 100.1, dtype=np.float64)
    quantity = np.full(len(trades), 10.0, dtype=np.float64)
    bbo = HistoricalBBOData(ts_ms, best_bid, best_ask, quantity, quantity)
    l2 = HistoricalL2Data(
        ts_ms,
        best_bid[:, None],
        quantity[:, None],
        best_ask[:, None],
        quantity[:, None],
    )
    return SimpleNamespace(
        trades=trades,
        var_ts_ms=np.empty(0, dtype=np.int64),
        var_ssq=np.empty(0, dtype=np.float64),
        bbo_data=bbo,
        l2_data=l2,
    )


def _shared(window: SimpleNamespace) -> dict[str, object]:
    return {"bbo_data": window.bbo_data, "l2_data": window.l2_data}


def _ema_contract() -> dict[str, object]:
    payload = study._load_json(study.OUTCOME_BLIND_INPUTS)
    return {"atomic_predicates": payload["atomic_predicates"]}


def _ema_window(
    *,
    bbo_mid: list[float],
    l2_mid: list[float] | None = None,
) -> SimpleNamespace:
    timestamps = np.arange(1_000, 1_000 * (len(bbo_mid) + 1), 1_000, dtype=np.int64)
    bbo_mid_array = np.asarray(bbo_mid, dtype=np.float64)
    l2_mid_array = np.asarray(l2_mid if l2_mid is not None else bbo_mid, dtype=np.float64)
    quantity = np.full(len(timestamps), 10.0, dtype=np.float64)
    bbo = HistoricalBBOData(
        timestamps,
        bbo_mid_array - 0.05,
        bbo_mid_array + 0.05,
        quantity,
        quantity,
        source="synthetic_native_bbo",
    )
    l2 = HistoricalL2Data(
        timestamps,
        (l2_mid_array - 0.05)[:, None],
        quantity[:, None],
        (l2_mid_array + 0.05)[:, None],
        quantity[:, None],
        source="deliberately_different_l2",
    )
    return SimpleNamespace(
        bbo_data=bbo,
        l2_data=l2,
        var_ts_ms=timestamps,
        var_ssq=np.full(len(timestamps), 0.01, dtype=np.float64),
    )


def _ema_opportunity(*, bbo_index: int, canonical_mid: float, side: str = "BUY") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "opportunity_id": ["ema-opportunity"],
            "side": [side],
            "fill_visible_ts_ms": [4_500],
            "decision_visible_bbo_index": [bbo_index],
            "decision_visible_l2_index": [0],
            "canonical_mid": [canonical_mid],
        }
    )


def test_boolean_ema_uses_exact_decision_visible_bbo_identity_not_l2() -> None:
    window = _ema_window(
        bbo_mid=[100.0, 101.0, 99.0, 102.0],
        l2_mid=[200.0, 201.0, 199.0, 202.0],
    )
    output = study._attach_boolean_ema_state(
        _ema_opportunity(bbo_index=2, canonical_mid=99.0),
        window,
        _ema_contract(),
        tick_size=0.1,
    )
    row = output.iloc[0]
    assert row["ema_surface_canonical_mid"] == 99.0
    assert row["ema_source_bbo_index"] == 2
    assert row["ema_source_bbo_ready_ts_ms"] == 3_000
    assert row["ema_surface_feature_ready_ts_ns"] == 3_000_000_000
    assert row["ema_source_bbo_stream"] == "synthetic_native_bbo"
    assert bool(row["ema_snapshot_mid_exact_match"])
    assert row["ema_state_price_source"] == "decision_visible_native_bbo_mid"

    with pytest.raises(study.StudyError, match="exactly match"):
        study._attach_boolean_ema_state(
            _ema_opportunity(bbo_index=2, canonical_mid=99.01),
            window,
            _ema_contract(),
            tick_size=0.1,
        )


def test_boolean_ema_no_cross_uses_frozen_sentinel_and_cross_predicates_fail_closed() -> None:
    window = _ema_window(bbo_mid=[100.0, 100.0, 100.0, 100.0])
    output = study._attach_boolean_ema_state(
        _ema_opportunity(bbo_index=3, canonical_mid=100.0),
        window,
        _ema_contract(),
        tick_size=0.1,
    )
    row = output.iloc[0]
    pair_prefixes = {
        study.ema_contract.pair_prefix(fast, slow)
        for fast, slow in study.ema_contract.ema_pairs()
    }
    for prefix in pair_prefixes:
        assert row[f"{prefix}_cross_missing"] == 1
        assert (
            row[f"{prefix}_cross_age_s"]
            == study.ema_contract.CROSS_AGE_MISSING_SENTINEL_S
        )
        assert not bool(row[f"predicate::{prefix}:last_cross_favorable"])
        assert not bool(row[f"predicate::{prefix}:cross_age_le_fast"])
        assert not bool(row[f"predicate::{prefix}:cross_age_le_slow"])


def test_execution_dependencies_bind_frozen_specs_without_amendment_cycle(monkeypatch) -> None:
    fake_cpp = SimpleNamespace(__file__=study.bt.__file__)
    monkeypatch.setattr(study.bt, "_load_cpp_tick_replay", lambda: fake_cpp)
    bindings = {row["role"]: row for row in study._dependency_bindings()}
    assert bindings["frozen_study_spec_json"]["path"] == str(study.FROZEN_SPEC_JSON.resolve())
    assert bindings["frozen_study_spec_md"]["path"] == str(study.FROZEN_SPEC_MD.resolve())
    assert bindings["frozen_study_spec_json"]["sha256"] == (
        "9f8c5abce4817b029d943648a46ab115d6ce7ac7f758b1326a48d75fa446e8ce"
    )
    assert bindings["frozen_study_spec_md"]["sha256"] == (
        "c2aca7b2742de0631ebf9f99fdcad55cdcd055024fdb13cf34afc0811a9a624f"
    )
    assert not any("amendment" in role for role in bindings)


def test_single_synthetic_fill_runs_fixed_duration_full_path_fork() -> None:
    window = _synthetic_window()
    baseline = study.bt._simulate_tick_with_engine(
        "python",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        _replay_params(),
        **_shared(window),
    )
    target = baseline["_cooldown_duration_opportunity_trace"][0]
    target["opportunity_id"] = "synthetic"
    action = study.DurationAction(
        policy_id="FIXED_2S",
        engine_action="FIXED_DURATION_MS",
        fixed_duration_s=2,
        duration_semantics="synthetic fixed duration",
    )
    trace, elapsed = study._run_duration_arm(
        target,
        action,
        window=window,
        base=_replay_params(),
        shared=_shared(window),
        engine="python",
    )
    assert elapsed >= 0.0
    assert trace["action"] == "FIXED_DURATION_MS"
    assert trace["applied_duration_ms"] == pytest.approx(2_000.0)
    assert trace["baseline_duration_ms"] == pytest.approx(target["baseline_duration_ms"])
    assert trace["assignment_state_sha256"]
    assert trace["right_censored"] in {True, False}
    assert trace["censor_marks_are_terminal_bounds"] is False
    if trace["right_censored"]:
        assert trace["assignment_to_washout_value_usdc"] is None
        assert trace["censor_time_mid_mark_usdc"] is not None
        assert trace["censor_time_executable_mark_usdc"] is not None
    else:
        assert trace["assignment_to_washout_value_usdc"] is not None
        assert trace["censor_time_mid_mark_usdc"] is None
        assert trace["censor_time_executable_mark_usdc"] is None


def test_single_synthetic_cpp_fork_matches_frozen_python_parity() -> None:
    window = _synthetic_window()
    controls = {
        engine: study.bt._simulate_tick_with_engine(
            engine,
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            _replay_params(),
            **_shared(window),
        )
        for engine in ("cpp", "python")
    }
    for field in (
        "pnl",
        "terminal_mtm_pnl",
        "final_inventory",
        "fills_total",
        "buy_fill_qty",
        "sell_fill_qty",
        "n_requotes",
    ):
        assert controls["cpp"][field] == pytest.approx(controls["python"][field])
    cpp_opportunities = controls["cpp"]["_cooldown_duration_opportunity_trace"]
    python_opportunities = controls["python"]["_cooldown_duration_opportunity_trace"]
    assert len(cpp_opportunities) == len(python_opportunities)
    for cpp_row, python_row in zip(cpp_opportunities, python_opportunities, strict=True):
        assert study._assert_cpp_python_opportunity_trace_parity(cpp_row, python_row)
    target = cpp_opportunities[0]
    target["opportunity_id"] = "synthetic-cpp"
    action = study.DurationAction(
        policy_id="FIXED_2S",
        engine_action="FIXED_DURATION_MS",
        fixed_duration_s=2,
        duration_semantics="synthetic fixed duration",
    )
    cpp_trace, _ = study._run_duration_arm(
        target,
        action,
        window=window,
        base=_replay_params(),
        shared=_shared(window),
        engine="cpp",
    )
    python_trace, _ = study._run_duration_arm(
        target,
        action,
        window=window,
        base=_replay_params(),
        shared=_shared(window),
        engine="python",
    )
    assert study._assert_cpp_python_trace_parity(cpp_trace, python_trace)


def test_control_fork_requires_exact_pre_quarantine_fill_prefix() -> None:
    window = _synthetic_window()
    baseline = study.bt._simulate_tick_with_engine(
        "cpp",
        window.trades,
        window.var_ts_ms,
        window.var_ssq,
        _replay_params(),
        **_shared(window),
    )
    target = dict(baseline["_cooldown_duration_opportunity_trace"][0])
    target["opportunity_id"] = "synthetic-control-prefix"
    action = study.DurationAction(
        policy_id="CONTROL_85N",
        engine_action="CONTROL_85N",
        fixed_duration_s=None,
        duration_semantics="synthetic authoritative control",
    )
    with pytest.raises(study.StudyError, match="lacks authoritative control fill path"):
        study._run_duration_arm(
            target,
            action,
            window=window,
            base=_replay_params(),
            shared=_shared(window),
            engine="cpp",
        )

    trace, _ = study._run_duration_arm(
        target,
        action,
        window=window,
        base=_replay_params(),
        shared=_shared(window),
        engine="cpp",
        authoritative_control_fills=baseline["_fill_trace"],
    )
    assert trace["control_prefix_parity_match"] is True
    assert trace["control_prefix_fill_count"] >= 1

    tampered = [dict(row) for row in baseline["_fill_trace"]]
    tampered[0]["quote_px"] += 0.1
    with pytest.raises(
        study.StudyError,
        match="first_difference_index=0",
    ):
        study._run_duration_arm(
            target,
            action,
            window=window,
            base=_replay_params(),
            shared=_shared(window),
            engine="cpp",
            authoritative_control_fills=tampered,
        )

    candidate_trace, _ = study._run_duration_arm(
        target,
        action,
        window=window,
        base=_replay_params(),
        shared=_shared(window),
        engine="cpp",
        require_control_prefix_parity=False,
    )
    assert candidate_trace["control_prefix_parity_match"] is None
    assert candidate_trace["control_prefix_fill_count"] is None


def test_cpp_python_opportunity_parity_uses_field_level_tolerances() -> None:
    window = _synthetic_window()
    controls = {
        engine: study.bt._simulate_tick_with_engine(
            engine,
            window.trades,
            window.var_ts_ms,
            window.var_ssq,
            _replay_params(),
            **_shared(window),
        )
        for engine in ("cpp", "python")
    }
    cpp_row = dict(controls["cpp"]["_cooldown_duration_opportunity_trace"][0])
    python_row = dict(controls["python"]["_cooldown_duration_opportunity_trace"][0])

    python_row["assignment_equity_usdc"] = cpp_row["assignment_equity_usdc"] + 3e-18
    python_row["canonical_mid"] = cpp_row["canonical_mid"] + 5e-10
    assert study._assert_cpp_python_opportunity_trace_parity(cpp_row, python_row)

    python_row["assignment_equity_usdc"] = cpp_row["assignment_equity_usdc"] + 2e-12
    with pytest.raises(study.StudyError, match="assignment_equity_usdc"):
        study._assert_cpp_python_opportunity_trace_parity(cpp_row, python_row)

    python_row = dict(cpp_row)
    python_row["canonical_mid"] = cpp_row["canonical_mid"] + 2e-9
    with pytest.raises(study.StudyError, match="canonical_mid"):
        study._assert_cpp_python_opportunity_trace_parity(cpp_row, python_row)

    python_row = dict(cpp_row)
    python_row["campaign_id"] = cpp_row["campaign_id"] + 1
    with pytest.raises(study.StudyError, match="campaign_id"):
        study._assert_cpp_python_opportunity_trace_parity(cpp_row, python_row)


def _joint_arm_rows(*, censor_last: bool) -> pd.DataFrame:
    rows = []
    for index, policy_id in enumerate(("CONTROL_85N", "FIXED_10S", "FIXED_20S")):
        censored = censor_last and index == 2
        rows.append(
            {
                "opportunity_id": "joint-a",
                "campaign_side_id": "2026-01-01:1:BUY",
                "assignment_ts_ns": 1_000_000_000,
                "side": "BUY",
                "task_id": f"task-{index}",
                "duration_policy_id": policy_id,
                "assignment_state_sha256": "shared",
                "arm_washout_complete": not censored,
                "right_censored": censored,
                "arm_end_ts_ms": 1_000 + index,
                "assignment_to_washout_value_usdc": None if censored else 0.1 + index,
                "censor_time_mid_mark_usdc": -0.2 if censored else None,
                "censor_time_executable_mark_usdc": -0.3 if censored else None,
                "censor_marks_are_terminal_bounds": False,
            }
        )
    return pd.DataFrame(rows)


def test_joint_outcome_censors_all_arms_when_one_arm_is_right_censored() -> None:
    annotated = study._annotate_joint_outcomes(
        _joint_arm_rows(censor_last=True),
        _contract(),
    )
    assert annotated["joint_action_count"].eq(3).all()
    assert annotated["joint_censored"].all()
    assert not annotated["joint_washout_complete"].any()
    assert not annotated["training_label_eligible"].any()
    assert annotated["washout_ts_ns"].eq(1_002_000_000).all()
    assert not annotated["washout_ts_is_joint_economic_washout"].any()
    assert annotated["joint_censor_reason"].eq("one_or_more_arms_right_censored").all()


def test_joint_outcome_requires_every_arm_washout() -> None:
    annotated = study._annotate_joint_outcomes(
        _joint_arm_rows(censor_last=False),
        _contract(),
    )
    assert not annotated["joint_censored"].any()
    assert annotated["joint_washout_complete"].all()
    assert annotated["training_label_eligible"].all()
    assert annotated["washout_ts_ns"].eq(1_002_000_000).all()
    assert annotated["washout_ts_is_joint_economic_washout"].all()


def test_economic_summary_excludes_joint_censored_opportunity_as_one_unit(tmp_path) -> None:
    rows = []
    for opportunity_id, censored in (("eligible", False), ("censored", True)):
        for index, policy_id in enumerate(("CONTROL_85N", "FIXED_10S", "FIXED_20S")):
            rows.append(
                {
                    "opportunity_id": opportunity_id,
                    "utc_day": "2026-01-01",
                    "side": "BUY",
                    "role_at_fill": "add",
                    "duration_policy_id": policy_id,
                    "joint_washout_complete": not censored,
                    "joint_censored": censored,
                    "training_label_eligible": not censored,
                    "assignment_to_washout_value_usdc": (
                        None if censored else float(index) * 0.1
                    ),
                    "post_assignment_buy_fill_count": index,
                    "post_assignment_sell_fill_count": 0,
                    "inventory_time_btc_s": float(index),
                    "mae_usdc": float(index),
                    "max_abs_inventory_btc": float(index) * 0.001,
                }
            )
    data_path = tmp_path / "arms.parquet"
    pd.DataFrame(rows).to_parquet(data_path, index=False)
    report = study._economic_summary(
        output=tmp_path,
        contract=_contract(),
        parts=({"arm_trace_path": str(data_path)},),
    )
    assert report["materialized_opportunities"] == 2
    assert report["training_label_opportunities"] == 1
    assert report["whole_opportunity_censor_exclusions"] == 1
    assert {row["paired_opportunities"] for row in report["cell_reports"]} == {1}


def test_python_parity_requires_explicit_limited_scope() -> None:
    with pytest.raises(study.StudyError, match="explicit diagnostic --limit"):
        study._scope_name(None, python_parity=True)


def test_single_action_label_replay_stops_at_daily_fresh_start_boundary(monkeypatch) -> None:
    sentinel_window = object()
    sentinel_params = {"fill_cooldown": 85.0}
    sentinel_shared = {"ml_data": object()}
    sentinel_audit = {"projection": {"identity": "control"}}
    monkeypatch.setattr(
        study,
        "_load_target_day",
        lambda day: (sentinel_window, sentinel_params, sentinel_shared, sentinel_audit),
    )
    window, params, shared, audit = study._load_arm_replay("2026-01-01")
    assert window is sentinel_window
    assert params is sentinel_params
    assert shared is sentinel_shared
    assert audit["continuation_day"] is None
    assert audit["continuation_source_bound"] is False
    assert audit["daily_fresh_start_boundary"] is True
    assert "next_day_stitch" in audit["UTC_midnight_behavior"]


def test_chunk_checkpoint_is_hash_validated(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "task_id": ["task-a", "task-b"],
            "replay_engine": ["cpp", "cpp"],
            "python_parity_checked": [False, False],
            "python_parity_match": [None, None],
            "arm_wall_seconds": [0.1, 0.2],
        }
    )
    data_path, manifest_path = study._chunk_paths(tmp_path, "diagnostic-limit-1", "2026-01-01", 0)
    study._atomic_parquet(data_path, frame)
    study._atomic_json(
        manifest_path,
        {
            "identity": study.IDENTITY,
            "scope": "diagnostic-limit-1",
            "utc_day": "2026-01-01",
            "chunk_index": 0,
            "execution_identity_sha256": "identity",
            "task_set_sha256": study._canonical_sha256(["task-a", "task-b"]),
            "formal_replay_engine": "cpp",
            "python_parity": False,
            "data_sha256": study._sha256_file(data_path),
        },
    )
    loaded = study._load_chunk(
        output=tmp_path,
        scope="diagnostic-limit-1",
        day="2026-01-01",
        chunk_index=0,
        expected_task_ids=["task-a", "task-b"],
        execution_identity_sha256="identity",
    )
    assert list(loaded["task_id"]) == ["task-a", "task-b"]
