from __future__ import annotations

from features.feature_dag import (
    P3_REACH_TIME_CONDITIONED_GRAPH,
    P3_REACH_TIME_GRAPH,
)


def test_reach_time_v2_graph_is_causal_and_keeps_v1_identity() -> None:
    assert P3_REACH_TIME_GRAPH.graph_id == "p3_aggressive_reach_time_surface.v1"
    assert P3_REACH_TIME_CONDITIONED_GRAPH.graph_id == (
        "p3_aggressive_reach_time_surface.v2"
    )
    order = P3_REACH_TIME_CONDITIONED_GRAPH.validate()
    assert order[-1] == "p3_rt_reach_cdf"
    nodes = P3_REACH_TIME_CONDITIONED_GRAPH.by_name()
    assert nodes["p3_rt_first_passage_label"].namespace == "label"
    assert nodes["p3_rt_hazard_feature_vector"].namespace == "feature"
    assert "p3_rt_first_passage_label" not in nodes[
        "p3_rt_hazard_feature_vector"
    ].dependencies
    assert nodes["p3_rt_risk_interval"].unit == "100ms_interval_upper_endpoint"
    assert "30s_administrative_censor" in nodes["p3_rt_risk_interval"].lookback


def test_source_identity_does_not_enter_tradable_feature_vector() -> None:
    node = P3_REACH_TIME_CONDITIONED_GRAPH.by_name()[
        "p3_rt_hazard_feature_vector"
    ]
    assert "source_identity_and_year_forbidden" in node.invalid_policy
