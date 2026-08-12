from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import causal_v12_1s_full_schema as full
from research.families.f03_causal_13_head.audit import causal_v12_1s_schema as schema

BASE_TS_MS = 1_780_000_000_000
DESIGN_PATH = Path(
    "research/families/f03_causal_13_head/docs/causal_v12_1s_full_schema_v1_design_20260805.json"
)
SOURCE_MANIFEST_PATH = Path(
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_feature_source_manifest_v1_20260805.json"
)


def _bars(count: int, *, price_offset: float = 0.0) -> list[base.OneSecondBar]:
    result: list[base.OneSecondBar] = []
    for index in range(count):
        start = BASE_TS_MS + index * 1_000
        close = 60_000.0 + price_offset + index * 0.2 + (0.05 if index % 4 else 0.0)
        result.append(
            base.OneSecondBar(
                start_ts_ms=start,
                finalized_ts_ms=start + 1_000,
                open=close - 0.05,
                high=close + 0.2,
                low=close - 0.2,
                close=close,
                volume=1.0 + index * 0.001,
                buy_volume=0.55 + index * 0.0006,
                sell_volume=0.45 + index * 0.0004,
                trade_count=10 + index % 4,
                buy_count=6,
                sell_count=4,
                buy_quote_qty=30_000.0 + index,
                sell_quote_qty=29_500.0 + index,
                max_same_side_run=2 + index % 3,
                buy_price_high=close + 0.1,
                buy_price_low=close - 0.1,
                sell_price_high=close + 0.1,
                sell_price_low=close - 0.1,
            )
        )
    return result


def _l2(cutoff: int, *, ready_offset_ms: int = 0) -> full.ExecutionL2Observation:
    values = {
        name: float(index + 1) / 10.0 for index, name in enumerate(schema.EXECUTION_L2_FEATURES)
    }
    return full.ExecutionL2Observation(
        bucket_start_ts_ms=cutoff - 1_000,
        feature_ready_ts_ms=cutoff + ready_offset_ms,
        values=values,
    )


def _metrics(cutoff: int) -> list[full.MetricObservation]:
    return [
        full.MetricObservation(
            source_ts_ms=cutoff - 299_000 + index * 60_000,
            feature_ready_ts_ms=cutoff - 298_900 + index * 60_000,
            sum_open_interest=10_000.0 + index * 100.0,
            toptrader_ls_ratio=1.1 + index * 0.01,
            crowd_ls_ratio=0.9 + index * 0.01,
            taker_ls_ratio=1.0 + index * 0.01,
        )
        for index in range(5)
    ]


def _full_row(count: int = 401) -> full.FullFeatureRow:
    cutoff = BASE_TS_MS + (count - 1) * 1_000
    return full.generate_full_feature_row(
        _bars(count),
        cutoff_exclusive_ms=cutoff,
        decision_ts_ms=cutoff + 25,
        execution_l2=(_l2(cutoff),),
        metrics=_metrics(cutoff),
        reference_bars=_bars(count, price_offset=500.0),
    )


def test_full_schema_matches_frozen_v12_training_abi() -> None:
    schema.validate_trainable_schema()
    full.validate_full_feature_dag()

    assert len(schema.TRAINABLE_FEATURE_ORDER) == 173
    assert tuple(spec.name for spec in full.FULL_FEATURE_SPECS) == (schema.TRAINABLE_FEATURE_ORDER)
    assert schema.feature_order_sha256() == (
        "5a6947850dfabefbf4e36bdbe986e39c96324e3714efb16d3410a4443ea1b797"
    )
    assert set(schema.EXECUTION_L2_FEATURES) <= set(schema.TRAINABLE_FEATURE_ORDER)
    assert set(schema.METRIC_FEATURES) <= set(schema.TRAINABLE_FEATURE_ORDER)
    assert set(schema.CALENDAR_FEATURES) <= set(schema.TRAINABLE_FEATURE_ORDER)
    assert set(schema.CROSS_MARKET_FEATURES) <= set(schema.TRAINABLE_FEATURE_ORDER)


def test_all_13_heads_retain_original_label_estimands() -> None:
    payload = schema.head_linkage_payload()

    assert payload["head_count"] == 13
    assert payload["label_estimand_changed"] is False
    assert payload["label_contract_identity"] == ("causal_v12_13_head_label_contract_v3_preserved")
    assert [row["head"] for row in payload["heads"]] == [
        "dir_10s",
        "ret_10s",
        "vol_10s",
        "dir_30s",
        "ret_30s",
        "vol_30s",
        "dir_60s",
        "ret_60s",
        "vol_60s",
        "tox_bid_5s",
        "tox_ask_5s",
        "tox_bid_10s",
        "tox_ask_10s",
    ]


def test_full_row_has_exact_schema_order_and_causal_ready_time() -> None:
    row = _full_row()

    assert tuple(row.values) == schema.TRAINABLE_FEATURE_ORDER
    assert row.feature_order == schema.TRAINABLE_FEATURE_ORDER
    assert row.feature_ready_ts_ms <= row.cutoff_exclusive_ms
    assert row.feature_ready_ts_ms <= row.decision_ts_ms
    assert row.values["l2_spread_bps"].lag_state == "ready"
    assert row.values["oi_log"].lag_state == "ready"
    assert row.values["cal_utc_hour"].lag_state == "ready"
    assert row.values["cv_ref_perp_available"].value == 1.0


def test_next_cutoff_source_data_cannot_change_previous_row() -> None:
    bars = _bars(402)
    cutoff = BASE_TS_MS + 400_000
    kwargs = {
        "cutoff_exclusive_ms": cutoff,
        "execution_l2": (_l2(cutoff),),
        "metrics": _metrics(cutoff),
        "reference_bars": _bars(402, price_offset=500.0),
    }
    before = full.generate_full_feature_row(bars, **kwargs)

    future = replace(
        bars[400],
        close=90_000.0,
        high=90_001.0,
        low=59_999.0,
        finalized_ts_ms=cutoff + 1_000,
    )
    after = full.generate_full_feature_row([*bars[:400], future, bars[401]], **kwargs)

    assert before.values == after.values
    assert before.fingerprint_sha256 == after.fingerprint_sha256


def test_local_1s_gap_fails_closed_instead_of_compressing_time() -> None:
    bars = _bars(401)
    del bars[200]
    cutoff = BASE_TS_MS + 400_000

    with pytest.raises(base.FeatureContractError, match="1s gap|missing or late"):
        full.generate_full_feature_row(bars, cutoff_exclusive_ms=cutoff)


def test_late_execution_l2_is_unsupported_not_retroactively_visible() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    row = full.generate_full_feature_row(
        bars,
        cutoff_exclusive_ms=cutoff,
        execution_l2=(_l2(cutoff, ready_offset_ms=1),),
    )

    for name in schema.EXECUTION_L2_FEATURES:
        assert row.values[name].value is None
        assert row.values[name].lag_state == "execution_l2_late_at_cutoff"


def test_execution_l2_does_not_carry_an_older_1s_bucket() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    older = replace(_l2(cutoff), bucket_start_ts_ms=cutoff - 2_000)
    row = full.generate_full_feature_row(
        bars,
        cutoff_exclusive_ms=cutoff,
        execution_l2=(older,),
    )

    assert row.values["l2_spread_bps"].value is None
    assert row.values["l2_spread_bps"].lag_state == ("execution_l2_exact_bucket_missing_no_carry")


def test_metrics_use_past_only_ready_observation() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    visible = _metrics(cutoff)
    future = full.MetricObservation(
        source_ts_ms=cutoff - 100,
        feature_ready_ts_ms=cutoff + 1,
        sum_open_interest=999_999.0,
        toptrader_ls_ratio=9.0,
        crowd_ls_ratio=9.0,
        taker_ls_ratio=9.0,
    )

    before = full.generate_full_feature_row(bars, cutoff_exclusive_ms=cutoff, metrics=visible)
    after = full.generate_full_feature_row(
        bars, cutoff_exclusive_ms=cutoff, metrics=[*visible, future]
    )

    for name in schema.METRIC_FEATURES:
        assert before.values[name] == after.values[name]


def test_missing_cross_market_source_is_explicitly_unsupported() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    row = full.generate_full_feature_row(bars, cutoff_exclusive_ms=cutoff)

    for name in schema.CROSS_MARKET_FEATURES:
        assert row.values[name].value is None
        assert row.values[name].lag_state == ("cross_market_missing_or_stale_no_forward_fill")


def test_duplicate_auxiliary_source_clocks_fail_closed() -> None:
    cutoff = BASE_TS_MS + 400_000
    observation = _l2(cutoff)
    with pytest.raises(base.FeatureContractError, match="duplicate execution L2"):
        full.generate_full_feature_row(
            _bars(401),
            cutoff_exclusive_ms=cutoff,
            execution_l2=(observation, observation),
        )


def test_schema_reordering_is_rejected() -> None:
    bad = list(full.FULL_FEATURE_SPECS)
    bad[0], bad[1] = bad[1], bad[0]

    with pytest.raises(base.FeatureContractError, match="order differs"):
        full.validate_full_feature_dag(tuple(bad))


def test_fingerprint_is_stable_for_input_container_order() -> None:
    count = 401
    cutoff = BASE_TS_MS + 400_000
    metrics = _metrics(cutoff)
    ordered = full.generate_full_feature_row(
        _bars(count),
        cutoff_exclusive_ms=cutoff,
        execution_l2=(_l2(cutoff),),
        metrics=metrics,
        reference_bars=_bars(count, price_offset=500.0),
    )
    reversed_inputs = full.generate_full_feature_row(
        list(reversed(_bars(count))),
        cutoff_exclusive_ms=cutoff,
        execution_l2=(_l2(cutoff),),
        metrics=list(reversed(metrics)),
        reference_bars=list(reversed(_bars(count, price_offset=500.0))),
    )

    assert ordered.values == reversed_inputs.values
    assert ordered.fingerprint_sha256 == reversed_inputs.fingerprint_sha256


def test_fingerprint_changes_when_a_visible_raw_value_changes() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    before = full.generate_full_feature_row(bars, cutoff_exclusive_ms=cutoff)
    changed = list(bars)
    changed[399] = replace(
        changed[399],
        close=changed[399].close + 1.0,
        high=changed[399].high + 1.0,
    )
    after = full.generate_full_feature_row(changed, cutoff_exclusive_ms=cutoff)

    assert before.fingerprint_sha256 != after.fingerprint_sha256


def test_catchup_emits_each_cutoff_once_and_is_atomic_on_failure() -> None:
    bars = _bars(15)
    generator = full.CausalV12FullSchema1sGenerator(last_emitted_cutoff_ms=BASE_TS_MS + 10_000)
    rows = generator.emit_through(bars, completed_exclusive_ms=BASE_TS_MS + 14_000)
    assert [row.cutoff_exclusive_ms for row in rows] == [
        BASE_TS_MS + 11_000,
        BASE_TS_MS + 12_000,
        BASE_TS_MS + 13_000,
        BASE_TS_MS + 14_000,
    ]

    broken = _bars(17)
    del broken[15]
    with pytest.raises(base.FeatureContractError, match="1s gap|missing or late"):
        generator.emit_through(broken, completed_exclusive_ms=BASE_TS_MS + 16_000)
    assert generator.last_emitted_cutoff_ms == BASE_TS_MS + 14_000


def test_frozen_design_and_source_manifest_bind_generated_contracts() -> None:
    design = json.loads(DESIGN_PATH.read_text(encoding="utf-8"))
    source_manifest = json.loads(SOURCE_MANIFEST_PATH.read_text(encoding="utf-8"))

    assert design["feature_schema"]["feature_count"] == 173
    assert design["feature_schema"]["feature_order_sha256"] == (schema.feature_order_sha256())
    assert design["feature_schema"]["full_feature_contract_sha256"] == (
        full.full_feature_contract_fingerprint()
    )
    assert design["label_contract"]["head_linkage_sha256"] == (
        schema.canonical_sha256(schema.head_linkage_payload())
    )
    assert source_manifest["source_manifest_sha256"] == (
        schema.canonical_sha256(schema.source_manifest_payload())
    )
    assert [row["name"] for row in source_manifest["sources"]] == [
        contract.name for contract in schema.SOURCE_CONTRACTS
    ]
    assert [row["feature_count"] for row in source_manifest["sources"]] == [
        len(contract.feature_names) for contract in schema.SOURCE_CONTRACTS
    ]
    assert sum(row["feature_count"] for row in source_manifest["sources"]) == 173
    assert source_manifest["ten_second_feature_rows_accepted_as_input"] is False
    assert all(value is False for value in design["authority_boundaries"].values())
