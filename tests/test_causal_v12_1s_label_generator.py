from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.families.f03_causal_13_head.audit import (
    causal_v12_1s_label_generator as labels,
)
from research.families.f03_causal_13_head.audit.causal_v12_1s_schema import (
    TRAINABLE_FEATURE_ORDER,
)
from research.families.f03_causal_13_head.audit.causal_v12_1s_training_contract import (
    HEAD_MAXIMUM_FUTURE_DEPENDENCY_S,
)

TARGET_DAY = "2025-08-01"


@pytest.fixture(scope="module")
def daily_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    count = labels.SECONDS_PER_DAY
    start = pd.Timestamp(TARGET_DAY, tz="UTC")
    decision_ms = (
        start.value // 1_000_000
        + np.arange(count, dtype=np.int64) * labels.DECISION_CADENCE_MS
    )
    values = np.zeros((count, len(TRAINABLE_FEATURE_ORDER)), dtype=np.float64)
    panel = pd.DataFrame(values, columns=TRAINABLE_FEATURE_ORDER)
    panel["close"] = 65_000.0
    panel.insert(0, "feature_ready_ts_ms", decision_ms)
    panel.insert(0, "decision_ts_ms", decision_ms)
    panel.insert(0, "cutoff_exclusive_ms", decision_ms)
    panel["feature_row_fingerprint_sha256"] = "a" * 64

    index = pd.date_range(start, periods=count, freq="1s")
    bars = pd.DataFrame(
        {
            "close": np.full(count, 65_000.0),
            "high": np.full(count, 65_000.1),
            "low": np.full(count, 64_999.9),
        },
        index=index,
    )
    return panel, bars


def _quote_params() -> dict[str, float | bool]:
    return {
        "gamma": 1.0,
        "kappa_ratio": 1.0,
        "p3_kappa_eff": 100.0,
        "quote_horizon_s": 1.0,
        "liq_baseline": 0.0,
        "gamma_liq_scale_min": 0.5,
        "gamma_liq_scale_max": 3.0,
        "vol_baseline": 0.0,
        "vol_power": 1.0,
        "gamma_scale_min": 0.5,
        "gamma_scale_max": 2.0,
        "p3_delta_star": 0.0,
        "tick_size": 0.1,
        "maker_fee": 0.0,
        "max_spread_bps": 0.0,
        "dynamic_cap_enabled": False,
        "dynamic_cap_base_bps": 0.0,
        "dynamic_cap_var_baseline": 0.0,
        "dynamic_cap_alpha": 0.5,
        "dynamic_cap_min_mult": 1.0,
        "dynamic_cap_max_mult": 2.0,
    }


def test_exact_1s_decision_origin_and_head_specific_censoring(
    monkeypatch: pytest.MonkeyPatch,
    daily_inputs: tuple[pd.DataFrame, pd.DataFrame],
) -> None:
    panel, bars = daily_inputs
    captured_origins: list[np.ndarray] = []

    def fake_triplet(*args):
        quote_time_ns = np.asarray(args[5], dtype=np.int64)
        captured_origins.append(quote_time_ns.copy())
        one = np.ones(quote_time_ns.size, dtype=np.float64)
        return one.copy(), one.copy(), one.copy()

    def fake_toxicity(*args):
        quote_time_ns = np.asarray(args[4], dtype=np.int64)
        captured_origins.append(quote_time_ns.copy())
        one = np.ones(quote_time_ns.size, dtype=np.float64)
        return one.copy(), one.copy()

    monkeypatch.setattr(labels.legacy_labels, "_compute_label_triplet", fake_triplet)
    monkeypatch.setattr(labels.legacy_labels, "_compute_toxicity_pair", fake_toxicity)

    result = labels.generate_daily_1s_labels(
        panel,
        bars,
        target_utc_day=TARGET_DAY,
        quote_params=_quote_params(),
    )

    expected_origin = panel["decision_ts_ms"].to_numpy(dtype=np.int64) * 1_000_000
    assert len(captured_origins) == 5
    assert all(np.array_equal(origin, expected_origin) for origin in captured_origins)
    assert result.attrs["legacy_resample_offset_applied"] is False
    assert "close" not in result.columns
    assert not (set(TRAINABLE_FEATURE_ORDER) & set(result.columns))
    assert set(result["feature_row_fingerprint_sha256"]) == {"a" * 64}
    assert np.all(panel["close"].to_numpy() == 65_000.0)

    base_weight = labels.inherited_base_sample_weights(
        pd.to_datetime(panel["decision_ts_ms"], unit="ms", utc=True)
    )
    for head, dependency_s in HEAD_MAXIMUM_FUTURE_DEPENDENCY_S.items():
        valid = result[f"label_valid__{head}"].to_numpy(dtype=bool)
        weight = result[f"sample_weight__{head}"].to_numpy(dtype=np.float64)
        assert valid.sum() == labels.SECONDS_PER_DAY - dependency_s
        assert np.all(weight[~valid] == 0.0)
        assert np.all(weight[valid] > 0.0)
        assert np.isclose(weight.sum(), base_weight[valid].sum(), rtol=1e-12)

    assert result["label_valid__ret_60s"].sum() == labels.SECONDS_PER_DAY - 120
    assert result["label_valid__vol_60s"].sum() == labels.SECONDS_PER_DAY - 60


def test_rejects_shifted_daily_decision_grid(
    daily_inputs: tuple[pd.DataFrame, pd.DataFrame],
) -> None:
    panel, bars = daily_inputs
    shifted = panel.copy(deep=False)
    shifted = shifted.copy()
    shifted.loc[0, "decision_ts_ms"] += labels.DECISION_CADENCE_MS
    with pytest.raises(labels.LabelGenerationError, match="exactly one canonical decision"):
        labels.generate_daily_1s_labels(
            shifted,
            bars,
            target_utc_day=TARGET_DAY,
            quote_params=_quote_params(),
        )


def test_rejects_preexisting_label_namespace(
    daily_inputs: tuple[pd.DataFrame, pd.DataFrame],
) -> None:
    panel, bars = daily_inputs
    contaminated = panel.copy(deep=False)
    contaminated = contaminated.assign(label_ret_10s=0.0)
    with pytest.raises(labels.LabelGenerationError, match="already contains label"):
        labels.generate_daily_1s_labels(
            contaminated,
            bars,
            target_utc_day=TARGET_DAY,
            quote_params=_quote_params(),
        )


def test_rejects_invalid_feature_row_fingerprint(
    daily_inputs: tuple[pd.DataFrame, pd.DataFrame],
) -> None:
    panel, bars = daily_inputs
    contaminated = panel.copy(deep=False)
    contaminated = contaminated.copy()
    contaminated.loc[0, "feature_row_fingerprint_sha256"] = "nan"
    with pytest.raises(labels.LabelGenerationError, match="lowercase SHA256"):
        labels.generate_daily_1s_labels(
            contaminated,
            bars,
            target_utc_day=TARGET_DAY,
            quote_params=_quote_params(),
        )


def test_two_second_observed_bar_gap_breaks_future_support() -> None:
    start = pd.Timestamp(TARGET_DAY, tz="UTC")
    decision_index = pd.date_range(start, periods=15, freq="1s")
    decision_ns = decision_index.as_unit("ns").asi8
    bars_index = decision_index.delete(2)
    bars_ns = bars_index.as_unit("ns").asi8

    mask = labels._bar_future_support_masks(
        decision_ns,
        bars_index,
        bars_ns,
    )[10]

    assert not mask[0]
    assert not mask[2]
    assert mask[3]
