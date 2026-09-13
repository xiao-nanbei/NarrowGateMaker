import pytest

from features.feature_dag import (
    CROSS_VENUE_FAIR_PRICE_GRAPH,
    Q90_CAUSAL_GRAPH,
    RUNTIME_FEATURE_GRAPH_IDENTITIES,
    TEN_SECOND_CAUSAL_GRAPH,
    FeatureGraph,
    FeatureSpec,
    validate_feature_graphs,
)


def _node(
    name: str,
    dependencies: tuple[str, ...] = (),
    *,
    namespace: str = "feature",
) -> FeatureSpec:
    return FeatureSpec(
        name=name,
        dependencies=dependencies,
        unit="dimensionless",
        source_clock="exchange_time_ms",
        availability_clock="feature_ready_time",
        cadence="event",
        lookback="none",
        stateful=False,
        invalid_policy="fail_closed",
        namespace=namespace,
    )


def test_runtime_feature_graphs_are_valid_and_hashed() -> None:
    identities = validate_feature_graphs()
    assert identities == RUNTIME_FEATURE_GRAPH_IDENTITIES
    assert set(identities) == {
        "live_10s_signal_cutoff.v1",
        "buy_q90_visibility_lifecycle_path_score.v3",
        "cross_venue_causal_fair_price.v1",
    }
    assert all(len(value) == 64 for value in identities.values())


def test_ten_second_graph_encodes_strict_cutoff_before_prediction() -> None:
    order = TEN_SECOND_CAUSAL_GRAPH.validate()
    assert order.index("feature_cutoff_view") < order.index(
        "signal_10s_feature_vector"
    )
    assert order.index("signal_10s_feature_vector") < order.index(
        "signal_13_head_prediction"
    )
    cutoff = TEN_SECOND_CAUSAL_GRAPH.by_name()["feature_cutoff_view"]
    assert cutoff.invalid_policy == "strict_exclusive_cutoff"
    assert cutoff.cpp_impl.endswith("compute_at_cutoff")


def test_q90_graph_keeps_visibility_before_path_and_score() -> None:
    order = Q90_CAUSAL_GRAPH.validate()
    assert order.index("q90_visible_book_state") < order.index(
        "q90_active_order_depth_path"
    )
    assert order.index("q90_active_order_depth_path") < order.index(
        "q90_model_feature_vector"
    )
    assert order.index("q90_model_feature_vector") < order.index(
        "q90_adverse_fill_score"
    )
    path = Q90_CAUSAL_GRAPH.by_name()["q90_active_order_depth_path"]
    assert path.invalid_policy == "invalid_outside_real_fill_risk_set"
    exchange_exposure = Q90_CAUSAL_GRAPH.by_name()[
        "q90_quantity_weighted_exchange_exposure"
    ]
    visible_exposure = Q90_CAUSAL_GRAPH.by_name()[
        "q90_quantity_weighted_visible_exposure"
    ]
    assert exchange_exposure.unit == visible_exposure.unit == "BTC*s"
    assert exchange_exposure.source_clock == "exchange_time_ns"
    assert visible_exposure.source_clock == "visibility_time_ns"
    assert exchange_exposure.invalid_policy == (
        "null_on_missing_or_regressed_exchange_clock"
    )
    assert visible_exposure.invalid_policy == (
        "zero_increment_outside_fill_risk_set"
    )


def test_cross_venue_graph_keeps_visibility_and_past_state_before_shadow() -> None:
    order = CROSS_VENUE_FAIR_PRICE_GRAPH.validate()
    assert order.index("external_bbo_receive_state") < order.index(
        "venue_basis_adjusted_price"
    )
    assert order.index("prior_basis_and_gain_state") < order.index(
        "causal_fair_center_shift"
    )
    assert order.index("causal_fair_center_shift") < order.index(
        "fair_center_quote_shadow"
    )
    shadow = CROSS_VENUE_FAIR_PRICE_GRAPH.by_name()["fair_center_quote_shadow"]
    assert shadow.invalid_policy == "baseline_fallback_and_no_order_action"


def test_graph_rejects_missing_dependency_and_cycle() -> None:
    with pytest.raises(ValueError, match="missing dependencies"):
        FeatureGraph("missing.v1", (_node("feature", ("missing",)),)).validate()

    cyclic = FeatureGraph(
        "cycle.v1",
        (
            _node("first", ("second",)),
            _node("second", ("first",)),
        ),
    )
    with pytest.raises(ValueError, match="contains a cycle"):
        cyclic.validate()


def test_feature_and_decision_nodes_cannot_depend_on_labels() -> None:
    graph = FeatureGraph(
        "leak.v1",
        (
            _node("future_label", namespace="label"),
            _node("leaking_feature", ("future_label",)),
        ),
    )
    with pytest.raises(ValueError, match="cannot depend on label"):
        graph.validate()
