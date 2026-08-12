from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from data_paths import relocate_marketdata_path
from models import backtest_tick as bt
from models.tick_data_types import HistoricalL2Data
from research.families.f10_live_replay_attribution.audit import (
    first_opener_decision_to_terminal_contract as contract,
)
from tests.test_lineage_randomized_outcome_contract_v2 import _replay_params

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = (
    ROOT
    / "research"
    / "families"
    / "f10_live_replay_attribution"
    / "docs"
    / "sell_first_fill_conditional_value_feasibility_v3_spec_20260730.json"
)
BASE_MS = int(pd.Timestamp("2026-04-20", tz="UTC").value // 1_000_000)
IDENTITY_FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "frozen_identity"


def _spec() -> dict:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    spec["status"] = "frozen_before_v3_native_development_outcome_read"
    quality = spec["quality_identity"]
    quality["path"] = str(relocate_marketdata_path(quality["path"]))
    identity_fixtures = {
        "normalized_l2_manifest_identity": (
            IDENTITY_FIXTURE_ROOT / "normalized_l2_manifest.json"
        ),
        "normalized_l2_daily_quality_identity": (
            IDENTITY_FIXTURE_ROOT / "normalized_l2_daily_quality.csv"
        ),
    }
    for key, fixture_path in identity_fixtures.items():
        identity = quality[key]
        identity["path"] = str(fixture_path.resolve())
        identity["sha256"] = contract.sha256_file(fixture_path)
    implementation = spec["implementation_identity"]
    implementation["contract_module_sha256"] = contract.sha256_file(
        Path(contract.__file__).resolve()
    )
    implementation["contract_test_sha256"] = contract.sha256_file(
        Path(__file__).resolve()
    )
    implementation["evaluator_module_sha256"] = contract.sha256_file(
        ROOT
        / "research"
        / "families"
        / "f05_fill_quality_quote_ev"
        / "audit"
        / "sell_first_fill_conditional_value.py"
    )
    implementation["evaluator_test_sha256"] = contract.sha256_file(
        ROOT / "tests" / "test_sell_first_fill_conditional_value.py"
    )
    spec["canonical_spec_sha256"] = contract.canonical_spec_sha256(spec)
    return spec


def _run(prices: list[float], buyer_maker: list[bool]) -> dict:
    params = _replay_params()
    params.update(
        {
            "fill_cooldown": 0.0,
            "fill_cooldown_clock_mode": "wall_time",
            "variance_time_lineage_randomized_enabled": False,
            "trace_variance_time_lineage_max": 0,
            "trace_first_opener_decision_to_terminal_max": 100,
            "first_opener_trace_quality_grade": "A",
            "first_opener_trace_schema_version": contract.TRACE_SCHEMA_VERSION,
        }
    )
    ts = np.arange(
        BASE_MS,
        BASE_MS + len(prices) * 1_000,
        1_000,
        dtype=np.int64,
    )
    trades = pd.DataFrame(
        {
            "transact_time": ts,
            "price": np.asarray(prices, dtype=np.float64),
            "quantity": np.asarray([0.0] + [0.001] * (len(prices) - 1)),
            "is_buyer_maker": np.asarray(buyer_maker, dtype=np.uint8),
        }
    )
    levels = 20
    offsets = np.arange(1, levels + 1, dtype=np.float64) * 0.1
    price_array = np.asarray(prices, dtype=np.float64)
    l2 = HistoricalL2Data(
        ts_ms=ts,
        bid_px=price_array[:, None] - offsets[None, :],
        bid_qty=np.tile(np.linspace(1.0, 2.0, levels), (len(ts), 1)),
        ask_px=price_array[:, None] + offsets[None, :],
        ask_qty=np.tile(np.linspace(1.5, 0.5, levels), (len(ts), 1)),
    )
    return bt._simulate_tick_with_engine(
        "python",
        trades,
        np.empty(0, dtype=np.int64),
        np.empty(0, dtype=np.float64),
        params,
        l2_data=l2,
    )


@pytest.mark.parametrize(
    ("prices", "buyer_maker", "expected_side"),
    (
        ([100.0, 96.6, 90.0, 100.0], [False, True, True, False], "BUY"),
        ([100.0, 103.4, 110.0, 100.0], [False, False, False, True], "SELL"),
    ),
)
def test_native_replay_emits_exact_first_opener_trace(
    prices: list[float],
    buyer_maker: list[bool],
    expected_side: str,
) -> None:
    result = _run(prices, buyer_maker)
    trace = pd.DataFrame(result["_first_opener_decision_to_terminal_trace"])
    validated = contract.validate_native_trace(trace, _spec())

    assert len(validated) == 1
    row = validated.iloc[0]
    assert row["side"] == expected_side
    assert row["inventory_role"] == "opener"
    assert row["inventory_btc"] == pytest.approx(0.0)
    assert row["decision_ts_ms"] == row["order_submit_ts_ms"]
    assert row["order_activation_ts_ms"] <= row["fill_ts_ms"]
    assert row["fill_ts_ms"] <= row["campaign_terminal_ts_ms"]
    assert row[contract.PRIMARY_ESTIMAND] == pytest.approx(
        row["campaign_terminal_equity_usdc"] - row["decision_equity_usdc"]
    )
    assert row["decision_l2_source_ts_ms"] <= row[
        "decision_visible_l2_cutoff_ts_ms"
    ]
    assert row["l2_feature_ready_ts_ms"] == (
        row["decision_ts_ms"]
        - (
            row["decision_visible_l2_cutoff_ts_ms"]
            - row["decision_l2_source_ts_ms"]
        )
    )
    assert row["decision_mid_usdc_per_btc"] == pytest.approx(
        0.5
        * (
            row["decision_best_bid_usdc_per_btc"]
            + row["decision_best_ask_usdc_per_btc"]
        )
    )
    assert row["decision_bbo_source_kind"] == "l2_fallback"
    assert row["queue_ahead_decision_authority"] == (
        "exchange_time_diagnostic_only"
    )
    assert result["_first_opener_decision_to_terminal_trace_audit"] == {
        "trace_schema_version": contract.TRACE_SCHEMA_VERSION,
        "campaign_count": 1,
        "eligible_true_opener_campaign_count": 1,
        "true_opener_campaign_coverage": 1.0,
        "unsupported_nonopener_open_campaign_count": 0,
        "unsupported_nonopener_open_campaigns": [],
        "selected_campaign_count": 1,
        "emitted_row_count": 1,
        "unique_campaign_count": 1,
        "exact_join_count": 1,
        "feature_clock_violation_count": 0,
        "open_record_count": 0,
        "coverage_complete": True,
    }


def test_first_opener_contract_keeps_order_age_out_of_model_features() -> None:
    spec = _spec()
    contract.validate_spec(spec)

    broken = copy.deepcopy(spec)
    broken["decision_visible_features"]["model_features"].append(
        "order_age_to_fill_ms"
    )
    broken["canonical_spec_sha256"] = contract.canonical_spec_sha256(broken)

    with pytest.raises(ValueError, match="post-decision feature"):
        contract.validate_spec(broken)


def test_first_opener_trace_rejects_future_ready_features() -> None:
    result = _run(
        [100.0, 96.6, 90.0, 100.0],
        [False, True, True, False],
    )
    trace = pd.DataFrame(result["_first_opener_decision_to_terminal_trace"])
    trace.loc[0, "decision_visible_feature_ready_ts_max_ms"] = (
        int(trace.loc[0, "decision_ts_ms"]) + 1
    )

    with pytest.raises(ValueError, match="non-causal"):
        contract.validate_native_trace(trace, _spec())


def test_first_opener_trace_rejects_future_source_asof() -> None:
    result = _run(
        [100.0, 96.6, 90.0, 100.0],
        [False, True, True, False],
    )
    trace = pd.DataFrame(result["_first_opener_decision_to_terminal_trace"])
    trace.loc[0, "decision_l2_source_ts_ms"] = (
        int(trace.loc[0, "decision_visible_l2_cutoff_ts_ms"]) + 1
    )

    with pytest.raises(ValueError, match="non-causal"):
        contract.validate_native_trace(trace, _spec())


def test_first_opener_trace_rejects_self_declared_book_ready_time() -> None:
    result = _run(
        [100.0, 96.6, 90.0, 100.0],
        [False, True, True, False],
    )
    trace = pd.DataFrame(result["_first_opener_decision_to_terminal_trace"])
    trace.loc[0, "l2_feature_ready_ts_ms"] = int(
        trace.loc[0, "l2_feature_ready_ts_ms"]
    ) - 1

    with pytest.raises(ValueError, match="non-causal"):
        contract.validate_native_trace(trace, _spec())


def test_first_opener_v3_excludes_child_trade_flow_from_model() -> None:
    spec = _spec()

    assert "individual_trade_taker_flow_imbalance_5s" not in (
        spec["decision_visible_features"]["model_features"]
    )
    assert "individual_trade_taker_flow_imbalance_5s" not in (
        contract.REQUIRED_TRACE_COLUMNS
    )
