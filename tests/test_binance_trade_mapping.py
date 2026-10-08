import json

import pandas as pd
import pytest

from research.families.f08_side_taker_lifecycle.audit.binance_trade_mapping import (
    SOURCE_CONTRACT_ID,
    build_individual_aggtrade_mapping,
)


def _individual() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [100, 101, 102],
            "price": [60_000.0, 60_000.0, 60_000.1],
            "qty": [0.1, 0.2, 0.4],
            "time": [1_000, 1_000, 1_001],
            "is_buyer_maker": [True, True, False],
        }
    )


def _aggregate() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "agg_trade_id": [7, 8],
            "price": [60_000.0, 60_000.1],
            "quantity": [0.3, 0.5],
            "normal_quantity": [0.3, 0.4],
            "first_trade_id": [100, 102],
            "last_trade_id": [101, 102],
            "transact_time": [1_000, 1_001],
            "is_buyer_maker": [True, False],
            "feature_ready_ts_ns": [1_005_000_000, 1_006_000_000],
        }
    )


def test_exact_mapping_uses_aggregate_visibility_clock() -> None:
    mapped, summary = build_individual_aggtrade_mapping(
        _individual(),
        _aggregate(),
    )

    assert summary["status"] == "passed"
    assert summary["source_contract_id"] == SOURCE_CONTRACT_ID
    assert summary["quantity_identity_counts"] == {
        "q_and_nq": 1,
        "nq_normal_only": 1,
    }
    assert mapped["agg_trade_id"].tolist() == [7, 7, 8]
    assert mapped["feature_ready_ts_ns"].tolist() == [
        1_005_000_000,
        1_005_000_000,
        1_006_000_000,
    ]
    assert mapped["exchange_ts_ms"].tolist() == [1_000, 1_000, 1_001]


def test_mapping_fails_closed_on_missing_trade_id() -> None:
    individual = _individual().query("id != 101")

    with pytest.raises(ValueError, match="strict individual↔aggTrade mapping"):
        build_individual_aggtrade_mapping(individual, _aggregate())


def test_mapping_fails_closed_on_side_mismatch() -> None:
    individual = _individual()
    individual.loc[individual["id"] == 101, "is_buyer_maker"] = False

    with pytest.raises(ValueError, match="strict individual↔aggTrade mapping"):
        build_individual_aggtrade_mapping(individual, _aggregate())


def test_historical_aggregate_timestamp_uses_last_child_visibility_floor() -> None:
    individual = _individual().iloc[:2].copy()
    individual.loc[individual.index[1], "time"] = 1_005
    aggregate = _aggregate().iloc[:1].drop(
        columns=["normal_quantity", "feature_ready_ts_ns"]
    )

    mapped, summary = build_individual_aggtrade_mapping(
        individual,
        aggregate,
        feature_ready_latency_ms=7.0,
        feature_ready_latency_profile_id="provider_neutral.test.v1",
    )

    assert mapped["feature_ready_ts_ns"].eq(1_012_000_000).all()
    assert set(mapped["feature_ready_source"]) == {
        "last_child_plus_frozen_latency"
    }
    assert summary["policy_feature_timing_eligible"] is True


def test_internal_trade_id_gap_is_excluded_from_exact_queue_outcomes() -> None:
    individual = pd.DataFrame(
        {
            "id": [100, 102],
            "price": [60_000.0, 60_000.0],
            "qty": [0.2, 0.3],
            "time": [1_000, 1_001],
            "is_buyer_maker": [True, True],
        }
    )
    aggregate = pd.DataFrame(
        {
            "agg_trade_id": [7],
            "price": [60_000.0],
            "quantity": [0.5],
            "first_trade_id": [100],
            "last_trade_id": [102],
            "transact_time": [1_000],
            "is_buyer_maker": [True],
        }
    )

    mapped, summary = build_individual_aggtrade_mapping(individual, aggregate)

    assert mapped["queue_outcome_exact"].eq(False).all()
    assert summary["nonexact_trade_id_range_aggregate_rows"] == 1
    assert summary["queue_outcome_day_strict_eligible"] is False
    with pytest.raises(ValueError, match="trade_ids_contiguous"):
        build_individual_aggtrade_mapping(
            individual,
            aggregate,
            require_exact_trade_id_coverage=True,
        )


def test_diagnostic_unmapped_children_preserve_full_denominator() -> None:
    individual = pd.concat([
        _individual(),
        pd.DataFrame([{"id": 105, "price": 60_001.0, "qty": 0.2,
                       "time": 1_003, "is_buyer_maker": False}]),
    ], ignore_index=True)
    with pytest.raises(ValueError, match="outside aggTrade ranges"):
        build_individual_aggtrade_mapping(individual, _aggregate())
    mapped, summary = build_individual_aggtrade_mapping(
        individual, _aggregate(), raise_on_findings=False,
    )
    assert summary["status"] == "findings"
    assert summary["individual_rows"] == 4
    assert summary["mapped_individual_rows"] == len(mapped) == 3
    assert summary["unmapped_individual_rows"] == 1
    assert summary["unmapped_individual_id_examples"] == [105]
    assert summary["actual_matched_aggregate_rows"] == 2
    assert summary["valid_aggregate_rows"] == 2
    assert summary["queue_outcome_day_strict_eligible"] is False
    assert summary["policy_feature_timing_eligible"] is False


def test_diagnostic_zero_child_parent_is_not_a_matched_parent() -> None:
    aggregate = _aggregate()
    extra = aggregate.iloc[[-1]].copy()
    extra["agg_trade_id"] = 9
    extra["first_trade_id"] = extra["last_trade_id"] = 200
    aggregate = pd.concat([aggregate, extra], ignore_index=True)
    with pytest.raises(ValueError, match="strict individual↔aggTrade mapping"):
        build_individual_aggtrade_mapping(_individual(), aggregate)
    _, summary = build_individual_aggtrade_mapping(
        _individual(), aggregate, raise_on_findings=False,
    )
    assert summary["status"] == "findings"
    assert summary["aggregate_rows"] == summary["mapped_aggregate_rows"] == 3
    assert summary["actual_matched_aggregate_rows"] == 2
    assert summary["zero_child_aggregate_rows"] == 1
    assert summary["partial_child_aggregate_rows"] == 0
    assert summary["reason_denominator_aggregate_rows"] == 3
    assert summary["reason_unobserved_aggregate_rows"] == 1
    assert summary["reason_counts"]["quantity_match"] == 1
    example = summary["aggregate_finding_examples"][0]
    assert example["agg_trade_id"] == 9
    assert example["mapped_individual_count"] == 0
    assert example["first_child_exchange_ts_ms"] is None
    assert example["last_child_exchange_ts_ms"] is None
    assert example["aggregate_timestamp_span_offset_ms"] is None
    assert example["individual_quantity"] is None
    assert example["child_price_min"] is None
    assert example["aggregate_quantity"] == 0.5
    json.dumps(summary, allow_nan=False)


@pytest.mark.parametrize("fault,reason", [
    ("missing_child", "quantity_match"),
    ("price", "price_match"),
    ("side", "aggressor_side_match"),
    ("quantity", "quantity_match"),
    ("timestamp", "aggregate_timestamp_in_child_span"),
    ("early_ready", "feature_ready_causal"),
])
def test_diagnostic_parent_findings_keep_denominators_and_default_rejection(fault, reason):
    individual, aggregate = _individual(), _aggregate()
    if fault == "missing_child":
        individual = individual.query("id != 101")
    elif fault == "price":
        individual.loc[1, "price"] = 60_001.0
    elif fault == "side":
        individual.loc[1, "is_buyer_maker"] = False
    elif fault == "quantity":
        individual.loc[1, "qty"] = 0.8
    elif fault == "timestamp":
        aggregate.loc[0, "transact_time"] = 999
    elif fault == "early_ready":
        aggregate.loc[0, "feature_ready_ts_ns"] = 999_000_000
    with pytest.raises(ValueError, match="strict individual↔aggTrade mapping"):
        build_individual_aggtrade_mapping(individual, aggregate)
    mapped, summary = build_individual_aggtrade_mapping(
        individual, aggregate, raise_on_findings=False,
    )
    assert summary["status"] == "findings"
    assert summary["individual_rows"] == len(individual) == len(mapped)
    assert summary["unmapped_individual_rows"] == 0
    assert summary["aggregate_rows"] == summary["actual_matched_aggregate_rows"] == 2
    assert summary["valid_aggregate_rows"] == summary["invalid_aggregate_rows"] == 1
    assert summary["partial_child_aggregate_rows"] == int(fault == "missing_child")
    assert summary["reason_counts"][reason] == 1
    assert summary["reason_denominator_aggregate_rows"] == 2
    assert summary["aggregate_finding_examples"][0]["agg_trade_id"] == 7


@pytest.mark.parametrize("explicit_ready", [False, True])
def test_diagnostic_all_unmatched_is_serializable_and_not_successful(explicit_ready) -> None:
    individual = _individual()
    individual["id"] += 1_000
    aggregate = _aggregate()
    if not explicit_ready:
        aggregate = aggregate.drop(columns=["feature_ready_ts_ns"])
    mapped, summary = build_individual_aggtrade_mapping(
        individual, aggregate, raise_on_findings=False,
    )
    assert mapped.empty
    assert summary["status"] == "findings"
    assert summary["individual_rows"] == summary["unmapped_individual_rows"] == 3
    assert summary["actual_matched_aggregate_rows"] == summary["valid_aggregate_rows"] == 0
    assert summary["zero_child_aggregate_rows"] == summary["invalid_aggregate_rows"] == 2
    assert summary["queue_outcome_day_strict_eligible"] is False
    assert summary["aggregate_timestamp_span_offset_ms_min"] is None
    assert summary["aggregate_timestamp_span_offset_ms_max"] is None
    json.dumps(summary, allow_nan=False)


@pytest.mark.parametrize("fault", ["individual_duplicate", "aggregate_duplicate", "overlap", "schema"])
def test_diagnostic_mode_still_rejects_unconstructable_mapping(fault):
    individual, aggregate = _individual(), _aggregate()
    if fault == "individual_duplicate":
        individual = pd.concat([individual, individual.iloc[[0]]], ignore_index=True)
    elif fault == "aggregate_duplicate":
        aggregate = pd.concat([aggregate, aggregate.iloc[[0]]], ignore_index=True)
    elif fault == "overlap":
        aggregate.loc[1, "first_trade_id"] = 101
    else:
        individual = individual.drop(columns=["id"])
    with pytest.raises(ValueError):
        build_individual_aggtrade_mapping(individual, aggregate, raise_on_findings=False)


def test_order_reversals_and_parent_gaps_are_diagnostics_not_new_default_gates():
    individual, aggregate = _individual(), _aggregate()
    individual.loc[2, "id"] = 104
    aggregate.loc[1, ["first_trade_id", "last_trade_id"]] = 104
    mapped, summary = build_individual_aggtrade_mapping(
        individual.iloc[::-1], aggregate.iloc[::-1],
    )
    assert summary["status"] == "passed"
    assert mapped["trade_id"].tolist() == [100, 101, 104]
    assert summary["source_order_diagnostics"]["individual"]["id_reversals"] == 2
    assert summary["source_order_diagnostics"]["individual"]["exchange_time_reversals"] == 1
    assert summary["source_order_diagnostics"]["aggregate"]["id_reversals"] == 1
    for name in ("individual", "aggregate_parent_ranges"):
        assert summary["id_gap_diagnostics"][name] == {
            "interval_count": 1, "unrepresented_id_count": 2,
            "examples": [{"first_id": 102, "last_id": 103, "count": 2}],
        }


def test_diagnostic_nonexact_contract_remains_separate_from_mapping_findings():
    individual = _individual().iloc[:2].copy()
    individual.loc[1, "id"] = 102
    aggregate = _aggregate().iloc[:1].copy()
    aggregate["last_trade_id"] = 102
    _, summary = build_individual_aggtrade_mapping(
        individual, aggregate, raise_on_findings=False,
    )
    assert summary["status"] == "passed"
    assert summary["nonexact_trade_id_range_aggregate_rows"] == 1
    assert summary["partial_child_aggregate_rows"] == 1
    assert summary["queue_outcome_day_strict_eligible"] is False
    _, strict_summary = build_individual_aggtrade_mapping(
        individual, aggregate, require_exact_trade_id_coverage=True, raise_on_findings=False,
    )
    assert strict_summary["status"] == "findings"


def test_diagnostic_examples_are_bounded_without_truncating_counts():
    individual = _individual().iloc[[0]].copy()
    individual["id"] = 1_000
    aggregate = pd.concat([_aggregate().iloc[[0]]] * 12, ignore_index=True)
    aggregate["agg_trade_id"] = range(12)
    aggregate["first_trade_id"] = range(100, 124, 2)
    aggregate["last_trade_id"] = aggregate["first_trade_id"] + 1
    _, summary = build_individual_aggtrade_mapping(
        individual, aggregate, raise_on_findings=False,
    )
    assert summary["zero_child_aggregate_rows"] == 12
    assert summary["invalid_aggregate_rows"] == 12
    assert len(summary["aggregate_finding_examples"]) == 8
    assert summary["reason_counts"]["quantity_match"] == 12


def test_diagnostic_success_matches_default_and_legacy_partial_coverage_stays_opt_in():
    default_mapped, default_summary = build_individual_aggtrade_mapping(_individual(), _aggregate())
    diagnostic_mapped, diagnostic_summary = build_individual_aggtrade_mapping(
        _individual(), _aggregate(), raise_on_findings=False,
    )
    pd.testing.assert_frame_equal(default_mapped, diagnostic_mapped)
    assert diagnostic_summary == default_summary
    individual = _individual()
    extra = individual.iloc[[0]].copy()
    extra["id"] = 999
    individual = pd.concat([individual, extra], ignore_index=True)
    _, legacy = build_individual_aggtrade_mapping(
        individual, _aggregate(), require_full_individual_coverage=False,
    )
    assert legacy["status"] == "passed"
    assert legacy["unmapped_individual_rows"] == 1
    _, diagnostic = build_individual_aggtrade_mapping(
        individual, _aggregate(), require_full_individual_coverage=False, raise_on_findings=False,
    )
    assert diagnostic["status"] == "findings"


@pytest.mark.parametrize("parent_time,offset", [(998, -2), (1008, 3)])
def test_timestamp_finding_examples_keep_signed_span_and_actual_values(parent_time, offset):
    individual = _individual().iloc[:2].copy()
    individual.loc[1, "time"] = 1_005
    aggregate = _aggregate().iloc[:1].copy()
    aggregate.loc[0, "transact_time"] = parent_time
    _, summary = build_individual_aggtrade_mapping(
        individual, aggregate, raise_on_findings=False,
    )
    example = summary["aggregate_finding_examples"][0]
    assert example["aggregate_exchange_ts_ms"] == parent_time
    assert example["first_child_exchange_ts_ms"] == 1_000
    assert example["last_child_exchange_ts_ms"] == 1_005
    assert example["aggregate_timestamp_span_offset_ms"] == offset
    assert example["aggregate_quantity"] == pytest.approx(0.3)
    assert example["individual_quantity"] == pytest.approx(0.3)
    assert example["aggregate_normal_quantity"] == pytest.approx(0.3)
    assert example["aggregate_price"] == 60_000.0
    assert example["child_price_min"] == example["child_price_max"] == 60_000.0
    assert summary["reason_counts"]["price_match"] == 0
    assert summary["reason_counts"]["quantity_match"] == 0
    assert summary["aggregate_timestamp_span_offset_ms_min"] == offset
    assert summary["aggregate_timestamp_span_offset_ms_max"] == offset


def test_span_offset_extrema_cover_all_parents_not_only_first_eight_examples():
    individual = pd.DataFrame({
        "id": range(100, 110), "price": [60_000.0] * 10, "qty": [0.1] * 10,
        "time": range(1_000, 1_010), "is_buyer_maker": [True] * 10,
    })
    aggregate = pd.DataFrame({
        "agg_trade_id": range(10), "price": [60_000.0] * 10, "quantity": [0.1] * 10,
        "first_trade_id": range(100, 110), "last_trade_id": range(100, 110),
        "transact_time": [995, *range(1_002, 1_010), 1_108],
        "is_buyer_maker": [True] * 10,
    })
    _, summary = build_individual_aggtrade_mapping(
        individual, aggregate, raise_on_findings=False,
    )
    assert summary["status"] == "findings"
    assert len(summary["aggregate_finding_examples"]) == 8
    assert summary["reason_counts"]["aggregate_timestamp_in_child_span"] == 10
    assert summary["aggregate_timestamp_span_offset_ms_min"] == -5
    assert summary["aggregate_timestamp_span_offset_ms_max"] == 99
    assert max(r["aggregate_timestamp_span_offset_ms"] for r in summary["aggregate_finding_examples"]) == 1


@pytest.mark.parametrize("source", ["individual", "aggregate"])
@pytest.mark.parametrize("field", ["price", "quantity"])
@pytest.mark.parametrize("value", [None, float("nan"), "inf", "-inf", 0, -0.1])
def test_nonfinite_or_nonpositive_source_values_cannot_match(source, field, value):
    individual, aggregate = _individual(), _aggregate()
    frame = individual if source == "individual" else aggregate
    column = "qty" if source == "individual" and field == "quantity" else field
    frame[column] = frame[column].astype(object)
    frame.loc[0, column] = value
    for raise_on_findings in (True, False):
        with pytest.raises(ValueError, match=f"{source} {field} must be finite and positive"):
            build_individual_aggtrade_mapping(
                individual, aggregate, raise_on_findings=raise_on_findings,
            )


@pytest.mark.parametrize("column", ["normal_quantity", "nq"])
@pytest.mark.parametrize("value", [None, float("nan"), "inf", "-inf", -0.1])
def test_supplied_normal_quantity_must_be_finite_nonnegative(column, value):
    aggregate = _aggregate().rename(columns={"normal_quantity": column})
    aggregate[column] = aggregate[column].astype(object)
    aggregate.loc[0, column] = value
    with pytest.raises(ValueError, match="normal_quantity must be finite and nonnegative"):
        build_individual_aggtrade_mapping(_individual(), aggregate, raise_on_findings=False)


def test_optional_normal_quantity_absent_or_zero_does_not_invent_normal_support():
    individual, aggregate = _individual().iloc[:2], _aggregate().iloc[:1]
    for frame in (aggregate.drop(columns=["normal_quantity"]), aggregate.assign(normal_quantity=0.0)):
        _, summary = build_individual_aggtrade_mapping(individual, frame)
        assert summary["status"] == "passed"
        assert summary["quantity_identity_counts"] == {"q_total": 1}
