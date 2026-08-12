from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

import research.families.f03_causal_13_head.audit.causal_v12_1s_feature_generator as generator

BASE_TS_MS = 1_800_000_000_000
DESIGN_PATH = Path(
    "research/families/f03_causal_13_head/docs/"
    "causal_v12_1s_feature_generator_v1_design_20260804.json"
)


def _bars(count: int, *, start_ts_ms: int = BASE_TS_MS, price_offset: float = 0.0):
    result = []
    for index in range(count):
        start = start_ts_ms + index * 1_000
        close = 60_000.0 + price_offset + index * 0.25 + (0.1 if index % 3 == 0 else 0.0)
        result.append(
            generator.OneSecondBar(
                start_ts_ms=start,
                finalized_ts_ms=start + 1_000,
                open=close - 0.05,
                high=close + 0.2,
                low=close - 0.2,
                close=close,
                volume=1.0 + index * 0.01,
                buy_volume=0.55 + index * 0.006,
                sell_volume=0.45 + index * 0.004,
                trade_count=10 + index % 4,
                buy_count=6,
                sell_count=4,
                buy_quote_qty=30_000.0 + index * 10.0,
                sell_quote_qty=29_000.0 + index * 9.0,
            )
        )
    return result


def test_node_contract_is_acyclic_complete_and_label_free() -> None:
    generator.validate_feature_dag()
    payload = generator.feature_contract_payload()

    assert payload["feature_dag_id"] == "live_1s_signal_cutoff.v1"
    assert payload["feature_namespace"] == "feature"
    assert len(payload["nodes"]) == len(generator.FEATURE_SPECS) == 44
    assert len({node["name"] for node in payload["nodes"]}) == 44
    for node in payload["nodes"]:
        assert node["unit"]
        assert node["cadence_ms"] == 1_000
        assert node["lookback_ms"] > 0
        assert node["source"]
        assert node["availability_clock"] == "finalized_1s_bar_time_ms"
        assert node["lag_state_rule"]
        assert node["namespace"] == "feature"
        assert not node["name"].startswith(("label_", "target_", "future_"))


def test_design_binds_the_canonical_feature_contract_and_no_authority() -> None:
    design = json.loads(DESIGN_PATH.read_text(encoding="utf-8"))

    assert design["implementation"]["feature_contract_sha256"] == (
        generator.feature_contract_fingerprint()
    )
    assert design["node_contract"]["prototype_node_count"] == len(generator.FEATURE_SPECS)
    assert all(value is False for value in design["authority_boundaries"].values())


def test_multiscale_feature_basis_is_preserved() -> None:
    names = set(generator.FEATURE_SPEC_BY_NAME)
    required = {
        "tick_mom_3s",
        "tick_mom_5s",
        "tick_mom_10s",
        "taker_quote_imbalance_5s",
        "taker_quote_imbalance_10s",
        "taker_quote_imbalance_30s",
        "taker_quote_imbalance_60s",
        "volatility_5s",
        "volatility_30s",
        "volatility_60s",
        "volatility_300s",
        "cv_ref_perp_ret_10s",
        "cv_ref_perp_ret_30s",
        "cv_ref_perp_ret_60s",
        "vol_regime_6h",
        "vol_regime_24h",
    }
    assert required <= names


def test_next_1s_bar_cannot_change_previous_cutoff_features() -> None:
    bars = _bars(401)
    cutoff = BASE_TS_MS + 400_000
    before = generator.generate_feature_row(bars, cutoff_exclusive_ms=cutoff)

    future = replace(
        bars[400],
        close=99_999.0,
        high=100_000.0,
        low=59_999.0,
        finalized_ts_ms=cutoff + 1_000,
    )
    after = generator.generate_feature_row(
        [*bars[:400], future],
        cutoff_exclusive_ms=cutoff,
    )

    assert before.values == after.values
    assert before.fingerprint_sha256 == after.fingerprint_sha256


def test_cutoff_minus_one_second_is_visible_but_cutoff_is_not() -> None:
    bars = _bars(12)
    cutoff = BASE_TS_MS + 11_000
    row = generator.generate_feature_row(bars, cutoff_exclusive_ms=cutoff)

    assert row.values["close"].source_latest_ts_ms == cutoff - 1_000
    assert row.values["close"].value == bars[10].close
    assert row.values["close"].value != bars[11].close


def test_feature_ready_never_exceeds_cutoff_or_decision() -> None:
    cutoff = BASE_TS_MS + 70_000
    row = generator.generate_feature_row(
        _bars(71),
        cutoff_exclusive_ms=cutoff,
        decision_ts_ms=cutoff + 250,
    )

    assert row.feature_ready_ts_ms == cutoff
    assert row.feature_ready_ts_ms <= row.cutoff_exclusive_ms
    assert row.feature_ready_ts_ms <= row.decision_ts_ms
    assert all(
        value.feature_ready_ts_ms is None or value.feature_ready_ts_ms <= row.decision_ts_ms
        for value in row.values.values()
    )


def test_bar_finalized_after_cutoff_is_not_retroactively_visible() -> None:
    bars = _bars(20)
    cutoff = BASE_TS_MS + 20_000
    bars[-1] = replace(bars[-1], finalized_ts_ms=cutoff + 1)

    with pytest.raises(generator.FeatureContractError, match="missing or late"):
        generator.generate_feature_row(bars, cutoff_exclusive_ms=cutoff)


def test_gap_fails_closed_and_does_not_compress_window_time() -> None:
    bars = _bars(20)
    del bars[7]

    with pytest.raises(generator.FeatureContractError, match="1s gap"):
        generator.generate_feature_row(
            bars,
            cutoff_exclusive_ms=BASE_TS_MS + 20_000,
        )


def test_duplicate_source_clock_fails_closed() -> None:
    bars = _bars(20)
    bars.insert(5, bars[5])

    with pytest.raises(generator.FeatureContractError, match="duplicate 1s source clock"):
        generator.generate_feature_row(
            bars,
            cutoff_exclusive_ms=BASE_TS_MS + 20_000,
        )


def test_generator_catches_up_every_bucket_once() -> None:
    bars = _bars(15)
    state = generator.Causal1sFeatureGenerator(last_emitted_cutoff_ms=BASE_TS_MS + 10_000)

    rows = state.emit_through(
        bars,
        completed_exclusive_ms=BASE_TS_MS + 14_000,
    )

    assert [row.cutoff_exclusive_ms for row in rows] == [
        BASE_TS_MS + 11_000,
        BASE_TS_MS + 12_000,
        BASE_TS_MS + 13_000,
        BASE_TS_MS + 14_000,
    ]
    assert (
        state.emit_through(
            bars,
            completed_exclusive_ms=BASE_TS_MS + 14_000,
        )
        == ()
    )


def test_failed_catchup_does_not_advance_cursor() -> None:
    bars = _bars(15)
    del bars[12]
    state = generator.Causal1sFeatureGenerator(last_emitted_cutoff_ms=BASE_TS_MS + 10_000)

    with pytest.raises(generator.FeatureContractError, match="1s gap|missing or late"):
        state.emit_through(
            bars,
            completed_exclusive_ms=BASE_TS_MS + 14_000,
        )

    assert state.last_emitted_cutoff_ms == BASE_TS_MS + 10_000


def test_backward_completed_clock_fails_closed() -> None:
    state = generator.Causal1sFeatureGenerator(last_emitted_cutoff_ms=BASE_TS_MS + 10_000)
    with pytest.raises(generator.FeatureContractError, match="moved backwards"):
        state.emit_through(
            _bars(12),
            completed_exclusive_ms=BASE_TS_MS + 9_000,
        )


def test_canonical_fingerprint_is_order_independent() -> None:
    bars = _bars(70)
    cutoff = BASE_TS_MS + 70_000

    ordered = generator.generate_feature_row(bars, cutoff_exclusive_ms=cutoff)
    reversed_input = generator.generate_feature_row(
        list(reversed(bars)),
        cutoff_exclusive_ms=cutoff,
    )

    assert ordered.values == reversed_input.values
    assert ordered.fingerprint_sha256 == reversed_input.fingerprint_sha256


def test_canonical_fingerprint_changes_with_visible_input() -> None:
    bars = _bars(70)
    cutoff = BASE_TS_MS + 70_000
    original = generator.generate_feature_row(bars, cutoff_exclusive_ms=cutoff)
    modified = list(bars)
    modified[-1] = replace(
        modified[-1],
        close=modified[-1].close + 1.0,
        high=modified[-1].high + 1.0,
    )

    changed = generator.generate_feature_row(modified, cutoff_exclusive_ms=cutoff)
    assert original.fingerprint_sha256 != changed.fingerprint_sha256


def test_reference_basis_uses_raw_cutoff_view_without_forward_fill() -> None:
    local = _bars(70)
    reference = _bars(70, price_offset=500.0)
    cutoff = BASE_TS_MS + 70_000

    supported = generator.generate_feature_row(
        local,
        cutoff_exclusive_ms=cutoff,
        reference_bars=reference,
    )
    unsupported = generator.generate_feature_row(local, cutoff_exclusive_ms=cutoff)

    assert supported.values["cv_ref_perp_ret_60s"].lag_state == ("ready_contiguous_full_window")
    assert supported.values["cv_ref_perp_ret_60s"].value is not None
    assert unsupported.values["cv_ref_perp_ret_60s"].value is None
    assert unsupported.values["cv_ref_perp_ret_60s"].lag_state == (
        "source_unavailable_no_forward_fill"
    )


def test_precomputed_feature_rows_are_not_valid_generator_inputs() -> None:
    with pytest.raises(generator.FeatureContractError, match="raw OneSecondBar"):
        generator.generate_feature_row(  # type: ignore[arg-type]
            [{"close": 60_000.0, "volatility_5s": 0.1}],
            cutoff_exclusive_ms=BASE_TS_MS + 1_000,
        )


def test_label_dependency_is_rejected_by_dag_validator() -> None:
    bad = generator.FeatureNodeSpec(
        name="bad_feature",
        dependencies=("label.future_return",),
        unit="ratio",
        cadence_ms=1_000,
        lookback_ms=1_000,
        source="test",
        source_clock="exchange_bucket_start_ms",
        availability_clock="finalized_1s_bar_time_ms",
        lag_state_rule="test",
        minimum_observations=1,
        stateful=False,
    )
    with pytest.raises(generator.FeatureContractError, match="forbidden outcome namespace"):
        generator.validate_feature_dag((*generator.FEATURE_SPECS, bad))
