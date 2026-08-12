from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from models import backtest_tick as bt
from research.families.f10_live_replay_attribution.audit import (
    first_add_decision_to_terminal_contract as contract,
)
from tests.test_lineage_randomized_outcome_contract_v2 import _replay_params

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "first_add_decision_to_terminal_loss_diagnostic_v1_spec_20260729.json"
)
BASE_MS = int(pd.Timestamp("2026-04-20", tz="UTC").value // 1_000_000)


def test_native_replay_emits_exact_first_add_decision_to_terminal_trace() -> None:
    params = _replay_params()
    params.update(
        {
            "fill_cooldown": 0.0,
            "fill_cooldown_clock_mode": "wall_time",
            "variance_time_lineage_randomized_enabled": False,
            "trace_variance_time_lineage_max": 0,
            "trace_first_add_decision_to_terminal_max": 100,
            "first_add_trace_quality_grade": "A",
        }
    )
    trades = pd.DataFrame(
        {
            "transact_time": np.asarray(
                [BASE_MS, BASE_MS + 1_000, BASE_MS + 2_000, BASE_MS + 3_000],
                dtype=np.int64,
            ),
            "price": np.asarray([100.0, 96.6, 90.0, 100.0]),
            "quantity": np.asarray([0.0, 0.001, 0.001, 0.001]),
            "is_buyer_maker": np.asarray(
                [False, True, True, False], dtype=np.uint8
            ),
        }
    )

    result = bt._simulate_tick_with_engine(
        "python",
        trades,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
    )
    trace = pd.DataFrame(result["_first_add_decision_to_terminal_trace"])
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    validated = contract.validate_native_trace(trace, spec)

    assert len(validated) == 1
    row = validated.iloc[0]
    assert row["inventory_role"] == "add"
    assert row["decision_ts_ms"] < row["fill_ts_ms"]
    assert row["fill_ts_ms"] <= row["campaign_terminal_ts_ms"]
    assert row["pre_decision_campaign_pnl_usdc"] == pytest.approx(
        row["decision_equity_usdc"] - row["campaign_start_equity_usdc"]
    )
    assert row["exposure_increasing_fill_count_so_far"] == 1
    assert row["reducing_fill_count_so_far"] == 0
    assert row["parent_aggtrade_flow_imbalance"] == pytest.approx(-1.0)
    assert row[contract.PRIMARY_ESTIMAND] == pytest.approx(
        row["campaign_terminal_equity_usdc"] - row["decision_equity_usdc"]
    )
    assert result["_first_add_decision_to_terminal_trace_audit"] == {
        "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
        "selected_campaign_count": 1,
        "emitted_row_count": 1,
        "unique_campaign_count": 1,
        "exact_join_count": 1,
        "feature_clock_violation_count": 0,
        "open_record_count": 0,
        "coverage_complete": True,
    }


def test_native_trace_order_ids_are_day_local_not_global() -> None:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    primary_days = spec["panels"]["development_primary_grade_a_days"][:2]
    rows = []
    for campaign_id, day in enumerate(primary_days, start=1):
        decision_ts = int(pd.Timestamp(day, tz="UTC").value // 1_000_000)
        rows.append(
            {
                "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
                "day": day,
                "quality_grade": "A",
                "campaign_id": campaign_id,
                "decision_id": f"decision-{day}",
                "decision_ts_ms": decision_ts,
                "order_id": 7,
                "order_submit_ts_ms": decision_ts,
                "fill_ts_ms": decision_ts + 1,
                "campaign_terminal_ts_ms": decision_ts + 2,
                "side": "BUY",
                "inventory_role": "add",
                "exact_decision_order_fill_join": 1,
                "decision_visible_feature_ready_ts_max_ms": decision_ts,
                "decision_equity_usdc": 10.0,
                "campaign_terminal_equity_usdc": 10.1,
                contract.PRIMARY_ESTIMAND: 0.1,
            }
        )

    validated = contract.validate_native_trace(pd.DataFrame(rows), spec)

    assert len(validated) == 2
