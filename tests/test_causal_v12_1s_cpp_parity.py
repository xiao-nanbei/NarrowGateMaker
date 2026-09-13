from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import numpy as np
import pytest

cpp = pytest.importorskip("narrowgate_cpp")

from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_full_schema as full,
)
from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_schema as schema,
)

BASE_TS_MS = 1_780_000_000_000


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


def _bars_before(
    cutoff: int, count: int = 401, *, price_offset: float = 0.0
) -> list[base.OneSecondBar]:
    shift = cutoff - count * 1_000 - BASE_TS_MS
    return [
        replace(
            item,
            start_ts_ms=item.start_ts_ms + shift,
            finalized_ts_ms=item.finalized_ts_ms + shift,
        )
        for item in _bars(count, price_offset=price_offset)
    ]


def _l2(cutoff: int, *, ready_offset_ms: int = 0) -> full.ExecutionL2Observation:
    values = {
        name: float(index + 1) / 10.0
        for index, name in enumerate(schema.EXECUTION_L2_FEATURES)
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


def _cpp_bar(item: base.OneSecondBar):  # type: ignore[no-untyped-def]
    result = cpp.F03CausalV12OneSecondBar()
    for name in (
        "start_ts_ms",
        "finalized_ts_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "buy_volume",
        "sell_volume",
        "trade_count",
        "buy_count",
        "sell_count",
        "buy_quote_qty",
        "sell_quote_qty",
        "max_same_side_run",
        "buy_price_high",
        "buy_price_low",
        "sell_price_high",
        "sell_price_low",
    ):
        setattr(result, name, getattr(item, name))
    return result


def _cpp_l2(item: full.ExecutionL2Observation):  # type: ignore[no-untyped-def]
    result = cpp.F03CausalV12ExecutionL2Observation()
    result.bucket_start_ts_ms = item.bucket_start_ts_ms
    result.feature_ready_ts_ms = item.feature_ready_ts_ms
    result.values = [item.values[name] for name in schema.EXECUTION_L2_FEATURES]
    return result


def _cpp_metric(item: full.MetricObservation):  # type: ignore[no-untyped-def]
    result = cpp.F03CausalV12MetricObservation()
    for name in (
        "source_ts_ms",
        "feature_ready_ts_ms",
        "sum_open_interest",
        "toptrader_ls_ratio",
        "crowd_ls_ratio",
        "taker_ls_ratio",
    ):
        setattr(result, name, getattr(item, name))
    return result


def _cpp_row(
    bars: list[base.OneSecondBar],
    *,
    cutoff: int,
    execution_l2: tuple[full.ExecutionL2Observation, ...] = (),
    metrics: tuple[full.MetricObservation, ...] = (),
    reference_bars: list[base.OneSecondBar] | None = None,
):
    return cpp.compute_f03_causal_v12_1s_features(
        [_cpp_bar(item) for item in bars],
        cutoff,
        cutoff + 25,
        [_cpp_l2(item) for item in execution_l2],
        [_cpp_metric(item) for item in metrics],
        [_cpp_bar(item) for item in (reference_bars or [])],
    )


def _assert_row_parity(py_row: full.FullFeatureRow, cpp_row) -> None:  # type: ignore[no-untyped-def]
    assert tuple(cpp.F03_CAUSAL_V12_1S_FEATURE_NAMES) == schema.TRAINABLE_FEATURE_ORDER
    assert cpp.F03_CAUSAL_V12_1S_FEATURE_ORDER_SHA256 == schema.feature_order_sha256()
    assert cpp_row["cutoff_exclusive_ms"] == py_row.cutoff_exclusive_ms
    assert cpp_row["decision_ts_ms"] == py_row.decision_ts_ms
    assert cpp_row["feature_ready_ts_ms"] == py_row.feature_ready_ts_ms
    for index, name in enumerate(schema.TRAINABLE_FEATURE_ORDER):
        expected = py_row.values[name]
        actual_valid = bool(cpp_row["valid"][index])
        assert actual_valid is (expected.value is not None), name
        if expected.value is None:
            assert np.isnan(cpp_row["values"][index]), name
        else:
            assert cpp_row["values"][index] == pytest.approx(
                expected.value, rel=2e-12, abs=2e-12
            ), name
        assert cpp_row["source_latest_ts_ms"][index] == (
            -1 if expected.source_latest_ts_ms is None else expected.source_latest_ts_ms
        ), name
        assert cpp_row["feature_ready_ts_ms_by_feature"][index] == (
            -1 if expected.feature_ready_ts_ms is None else expected.feature_ready_ts_ms
        ), name
        assert cpp_row["observation_count"][index] == expected.observation_count, name
        assert cpp_row["lag_state"][index] == expected.lag_state, name


def test_cpp_matches_all_173_python_fields_and_dynamic_states() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    l2 = (_l2(cutoff),)
    metrics = tuple(_metrics(cutoff))
    reference = _bars(401, price_offset=500.0)
    py_row = full.generate_full_feature_row(
        bars,
        cutoff_exclusive_ms=cutoff,
        decision_ts_ms=cutoff + 25,
        execution_l2=l2,
        metrics=metrics,
        reference_bars=reference,
    )
    _assert_row_parity(
        py_row,
        _cpp_row(
            bars,
            cutoff=cutoff,
            execution_l2=l2,
            metrics=metrics,
            reference_bars=reference,
        ),
    )


def test_long_lag_state_windows_transition_to_ready_with_field_parity() -> None:
    count = 21_601
    cutoff = BASE_TS_MS + count * 1_000
    bars = _bars(count)
    py_row = full.generate_full_feature_row(
        bars, cutoff_exclusive_ms=cutoff, decision_ts_ms=cutoff + 25
    )
    cpp_row = _cpp_row(bars, cutoff=cutoff)
    _assert_row_parity(py_row, cpp_row)
    for name in ("vol_regime_6h", "vol_regime_24h"):
        assert py_row.values[name].lag_state == "ready"
    assert py_row.values["vol_regime_zscore"].lag_state == "warmup_insufficient"


def test_cutoff_minus_one_visible_cutoff_source_invisible() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    visible = full.MetricObservation(
        source_ts_ms=cutoff - 1,
        feature_ready_ts_ms=cutoff - 1,
        sum_open_interest=12_345.0,
        toptrader_ls_ratio=1.2,
        crowd_ls_ratio=1.1,
        taker_ls_ratio=1.0,
    )
    at_cutoff = replace(
        visible,
        source_ts_ms=cutoff,
        feature_ready_ts_ms=cutoff,
        sum_open_interest=999_999.0,
    )
    py_row = full.generate_full_feature_row(
        bars,
        cutoff_exclusive_ms=cutoff,
        decision_ts_ms=cutoff + 25,
        metrics=(visible, at_cutoff),
    )
    cpp_row = _cpp_row(bars, cutoff=cutoff, metrics=(visible, at_cutoff))
    _assert_row_parity(py_row, cpp_row)
    oi_index = schema.TRAINABLE_FEATURE_ORDER.index("oi_log")
    assert cpp_row["values"][oi_index] == pytest.approx(np.log(12_345.0))


def test_next_second_perturbation_cannot_change_previous_cpp_row() -> None:
    bars = _bars(402)
    cutoff = BASE_TS_MS + 400_000
    before = _cpp_row(bars, cutoff=cutoff)
    future = replace(
        bars[400],
        close=90_000.0,
        high=90_001.0,
        low=59_999.0,
        finalized_ts_ms=cutoff + 1_000,
    )
    after = _cpp_row([*bars[:400], future, bars[401]], cutoff=cutoff)

    np.testing.assert_array_equal(before["valid"], after["valid"])
    np.testing.assert_allclose(before["values"], after["values"], equal_nan=True)
    np.testing.assert_array_equal(
        before["source_latest_ts_ms"], after["source_latest_ts_ms"]
    )
    assert before["lag_state"] == after["lag_state"]


def test_lag_states_match_for_late_l2_stale_metrics_and_missing_reference() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    late_l2 = (_l2(cutoff, ready_offset_ms=1),)
    stale = full.MetricObservation(
        source_ts_ms=cutoff - 301_000,
        feature_ready_ts_ms=cutoff - 300_999,
        sum_open_interest=10_000.0,
        toptrader_ls_ratio=1.0,
        crowd_ls_ratio=1.0,
        taker_ls_ratio=1.0,
    )
    py_row = full.generate_full_feature_row(
        bars,
        cutoff_exclusive_ms=cutoff,
        decision_ts_ms=cutoff + 25,
        execution_l2=late_l2,
        metrics=(stale,),
    )
    _assert_row_parity(
        py_row,
        _cpp_row(bars, cutoff=cutoff, execution_l2=late_l2, metrics=(stale,)),
    )


@pytest.mark.parametrize(
    "instant",
    [
        datetime(2025, 3, 9, 6, 59, tzinfo=UTC),
        datetime(2025, 3, 9, 7, 0, tzinfo=UTC),
        datetime(2025, 11, 2, 5, 59, tzinfo=UTC),
        datetime(2025, 11, 2, 6, 0, tzinfo=UTC),
        datetime(2026, 2, 16, 12, 0, tzinfo=UTC),
        datetime(2026, 7, 3, 14, 30, tzinfo=UTC),
    ],
)
def test_calendar_and_dst_fields_match_at_supported_boundaries(instant: datetime) -> None:
    cutoff = int(instant.timestamp() * 1_000)
    bars = _bars_before(cutoff)
    py_row = full.generate_full_feature_row(
        bars, cutoff_exclusive_ms=cutoff, decision_ts_ms=cutoff + 25
    )
    _assert_row_parity(py_row, _cpp_row(bars, cutoff=cutoff))


def test_multisecond_gap_fails_closed_in_python_and_cpp() -> None:
    bars = _bars(401)
    del bars[199:202]
    cutoff = BASE_TS_MS + 400_000
    with pytest.raises(base.FeatureContractError, match="1s gap"):
        full.generate_full_feature_row(bars, cutoff_exclusive_ms=cutoff)
    with pytest.raises(ValueError, match="1s gap"):
        _cpp_row(bars, cutoff=cutoff)


def test_explicit_contiguous_flat_gap_bars_are_raw_1s_inputs_not_10s_feature_fill() -> None:
    bars = _bars(401)
    prior = bars[198]
    for index in range(199, 202):
        bars[index] = replace(
            bars[index],
            open=prior.close,
            high=prior.close,
            low=prior.close,
            close=prior.close,
            volume=0.0,
            buy_volume=0.0,
            sell_volume=0.0,
            trade_count=0,
            buy_count=0,
            sell_count=0,
            buy_quote_qty=0.0,
            sell_quote_qty=0.0,
            max_same_side_run=0,
            buy_price_high=0.0,
            buy_price_low=0.0,
            sell_price_high=0.0,
            sell_price_low=0.0,
        )
    cutoff = BASE_TS_MS + 400_000
    py_row = full.generate_full_feature_row(
        bars, cutoff_exclusive_ms=cutoff, decision_ts_ms=cutoff + 25
    )
    _assert_row_parity(py_row, _cpp_row(bars, cutoff=cutoff))
