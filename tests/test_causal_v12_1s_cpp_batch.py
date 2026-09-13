from __future__ import annotations

import copy
from dataclasses import replace

import numpy as np
import pytest

cpp = pytest.importorskip("narrowgate_cpp")

from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_cpp_batch as batch,
)
from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_feature_generator as base,
)
from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_full_schema as full,
)
from research.families.f03_causal_13_head.audit import (  # noqa: E402
    causal_v12_1s_schema as schema,
)

BASE_TS_MS = 1_754_000_000_000


def _bars(count: int, *, price_offset: float = 0.0) -> tuple[base.OneSecondBar, ...]:
    rows = []
    for index in range(count):
        start = BASE_TS_MS + index * 1_000
        close = 60_000.0 + price_offset + 0.2 * index + 0.03 * (index % 7)
        if 150 <= index <= 152:
            close = rows[-1].close
            volume = buy_volume = sell_volume = 0.0
            trade_count = buy_count = sell_count = 0
        else:
            volume = 1.0 + index * 0.001
            buy_volume = 0.55 + index * 0.0006
            sell_volume = 0.45 + index * 0.0004
            trade_count = 10 + index % 4
            buy_count = 6
            sell_count = 4
        rows.append(
            base.OneSecondBar(
                start_ts_ms=start,
                finalized_ts_ms=start + 1_000,
                open=close - 0.05,
                high=close + 0.2,
                low=close - 0.2,
                close=close,
                volume=volume,
                buy_volume=buy_volume,
                sell_volume=sell_volume,
                trade_count=trade_count,
                buy_count=buy_count,
                sell_count=sell_count,
                buy_quote_qty=buy_volume * close,
                sell_quote_qty=sell_volume * close,
                max_same_side_run=0 if trade_count == 0 else 2 + index % 3,
                buy_price_high=0.0 if buy_count == 0 else close + 0.1,
                buy_price_low=0.0 if buy_count == 0 else close - 0.1,
                sell_price_high=0.0 if sell_count == 0 else close + 0.1,
                sell_price_low=0.0 if sell_count == 0 else close - 0.1,
            )
        )
    return tuple(rows)


def _l2(cutoff: int) -> full.ExecutionL2Observation:
    return full.ExecutionL2Observation(
        bucket_start_ts_ms=cutoff - 1_000,
        feature_ready_ts_ms=cutoff,
        values={name: 0.1 + index for index, name in enumerate(schema.EXECUTION_L2_FEATURES)},
    )


def _metrics(cutoff: int) -> tuple[full.MetricObservation, ...]:
    return tuple(
        full.MetricObservation(
            source_ts_ms=cutoff - (6 - index) * 300_000,
            feature_ready_ts_ms=cutoff - (6 - index) * 300_000,
            sum_open_interest=10_000.0 + index,
            toptrader_ls_ratio=1.1 + index * 0.01,
            crowd_ls_ratio=0.9 + index * 0.01,
            taker_ls_ratio=1.0 + index * 0.01,
        )
        for index in range(6)
        if cutoff - (6 - index) * 300_000 > 0
    )


def _assert_batch_row(py_row: full.FullFeatureRow, output: object, row: int) -> None:
    result = output
    vocabulary = tuple(cpp.F03_CAUSAL_V12_1S_LAG_STATE_VOCABULARY)
    assert result["cutoff_exclusive_ms"][row] == py_row.cutoff_exclusive_ms
    assert result["decision_ts_ms"][row] == py_row.decision_ts_ms
    assert result["feature_ready_ts_ms"][row] == py_row.feature_ready_ts_ms
    for column, name in enumerate(schema.TRAINABLE_FEATURE_ORDER):
        expected = py_row.values[name]
        is_valid = bool(result["valid"][row, column])
        assert is_valid is (expected.value is not None), name
        if expected.value is None:
            assert np.isnan(result["values"][row, column]), name
        else:
            assert result["values"][row, column] == pytest.approx(
                expected.value, rel=2e-12, abs=2e-12
            ), name
        assert result["source_latest_ts_ms"][row, column] == (
            -1 if expected.source_latest_ts_ms is None else expected.source_latest_ts_ms
        ), name
        assert result["feature_ready_ts_ms_by_feature"][row, column] == (
            -1 if expected.feature_ready_ts_ms is None else expected.feature_ready_ts_ms
        ), name
        assert result["observation_count"][row, column] == expected.observation_count, name
        assert vocabulary[int(result["lag_state_code"][row, column])] == expected.lag_state, name
    fingerprint = batch.feature_row_fingerprint(
        cutoff_exclusive_ms=py_row.cutoff_exclusive_ms,
        values=result["values"][row],
        valid=result["valid"][row],
        source_latest_ts_ms=result["source_latest_ts_ms"][row],
        feature_ready_ts_ms=result["feature_ready_ts_ms_by_feature"][row],
        observation_count=result["observation_count"][row],
        lag_state_code=result["lag_state_code"][row],
        lag_state_vocabulary=vocabulary,
    )
    assert len(fingerprint) == 64


def test_batch_matches_python_all_173_fields_across_cutoffs_and_flat_gap() -> None:
    bars = _bars(901)
    reference = _bars(901, price_offset=500.0)
    cutoffs = tuple(BASE_TS_MS + index * 1_000 for index in (200, 400, 900))
    l2 = tuple(_l2(cutoff) for cutoff in cutoffs)
    metrics = _metrics(cutoffs[-1])
    engine = batch.create_engine(
        cpp,
        local_bars=bars,
        execution_l2=l2,
        metrics=metrics,
        reference_bars=reference,
    )
    output = batch.compute_batch(engine, cutoffs)

    for row, cutoff in enumerate(cutoffs):
        py_row = full.generate_full_feature_row(
            bars,
            cutoff_exclusive_ms=cutoff,
            execution_l2=l2,
            metrics=metrics,
            reference_bars=reference,
        )
        _assert_batch_row(py_row, output, row)


def test_cutoff_bar_and_next_second_perturbation_are_strictly_invisible() -> None:
    cutoff = BASE_TS_MS + 400_000
    original = list(_bars(402))
    perturbed = copy.deepcopy(original)
    perturbed[400] = replace(
        perturbed[400],
        open=perturbed[400].open + 500.0,
        high=perturbed[400].high + 500.0,
        low=perturbed[400].low + 500.0,
        close=perturbed[400].close + 500.0,
    )
    first = batch.create_engine(
        cpp,
        local_bars=original,
        execution_l2=(),
        metrics=(),
        reference_bars=(),
    )
    second = batch.create_engine(
        cpp,
        local_bars=perturbed,
        execution_l2=(),
        metrics=(),
        reference_bars=(),
    )

    left = batch.compute_batch(first, (cutoff,))
    right = batch.compute_batch(second, (cutoff,))

    np.testing.assert_array_equal(left["valid"], right["valid"])
    np.testing.assert_array_equal(left["values"], right["values"])
    np.testing.assert_array_equal(left["source_latest_ts_ms"], right["source_latest_ts_ms"])


def test_missing_sources_preserve_lag_state_codes() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    engine = batch.create_engine(
        cpp,
        local_bars=bars,
        execution_l2=(),
        metrics=(),
        reference_bars=(),
    )
    output = batch.compute_batch(engine, (cutoff,))
    py_row = full.generate_full_feature_row(bars, cutoff_exclusive_ms=cutoff)
    _assert_batch_row(py_row, output, 0)


def test_long_warmup_regime_and_ewm_match_python() -> None:
    bars = _bars(108_001)
    cutoff = BASE_TS_MS + 108_000_000
    engine = batch.create_engine(
        cpp,
        local_bars=bars,
        execution_l2=(),
        metrics=(),
        reference_bars=(),
    )
    output = batch.compute_batch(engine, (cutoff,))
    py_row = full.generate_full_feature_row(bars, cutoff_exclusive_ms=cutoff)

    _assert_batch_row(py_row, output, 0)
    for name in ("tick_ewm_3s", "tick_ewm_10s", "vol_regime_6h", "vol_regime_24h"):
        assert py_row.values[name].value is not None
    assert py_row.values["vol_regime_zscore"].observation_count >= 24


def test_batch_rejects_a_physical_multisecond_hole() -> None:
    bars = list(_bars(401))
    del bars[150:153]
    with pytest.raises(ValueError, match="dense causal 1s source"):
        batch.create_engine(
            cpp,
            local_bars=bars,
            execution_l2=(),
            metrics=(),
            reference_bars=(),
        )
