"""Static contracts for NarrowGate's causal feature subgraphs.

The runtime remains hand-optimized Python/C++.  These declarations validate
the graph shape and causal metadata at startup; they are not a dynamic feature
executor on the latency-sensitive path.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from typing import Literal

NodeNamespace = Literal["source", "feature", "decision", "label"]


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    dependencies: tuple[str, ...]
    unit: str
    source_clock: str
    availability_clock: str
    cadence: str
    lookback: str
    stateful: bool
    invalid_policy: str
    namespace: NodeNamespace = "feature"
    python_impl: str = ""
    cpp_impl: str = ""
    lagged_state: bool = False

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("feature node name cannot be empty")
        for field_name in (
            "unit",
            "source_clock",
            "availability_clock",
            "cadence",
            "lookback",
            "invalid_policy",
        ):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"feature node {self.name} lacks {field_name}")
        if self.namespace not in {"source", "feature", "decision", "label"}:
            raise ValueError(
                f"feature node {self.name} has invalid namespace {self.namespace}"
            )
        if self.lagged_state and self.namespace != "source":
            raise ValueError(
                f"lagged node {self.name} must be an explicit source boundary"
            )


@dataclass(frozen=True)
class FeatureGraph:
    graph_id: str
    nodes: tuple[FeatureSpec, ...]

    def by_name(self) -> dict[str, FeatureSpec]:
        return {node.name: node for node in self.nodes}

    def validate(self) -> tuple[str, ...]:
        if not self.graph_id.strip():
            raise ValueError("feature graph id cannot be empty")
        names = [node.name for node in self.nodes]
        if len(names) != len(set(names)):
            duplicates = sorted({name for name in names if names.count(name) > 1})
            raise ValueError(
                f"feature graph {self.graph_id} has duplicate nodes: {duplicates}"
            )

        nodes = self.by_name()
        for node in self.nodes:
            missing = sorted(set(node.dependencies).difference(nodes))
            if missing:
                raise ValueError(
                    f"feature node {node.name} has missing dependencies: {missing}"
                )
            for dependency_name in node.dependencies:
                dependency = nodes[dependency_name]
                if (
                    node.namespace in {"feature", "decision"}
                    and dependency.namespace == "label"
                ):
                    raise ValueError(
                        f"{node.namespace} node {node.name} cannot depend on "
                        f"label node {dependency.name}"
                    )

        visiting: set[str] = set()
        visited: set[str] = set()
        order: list[str] = []

        def visit(name: str, lineage: tuple[str, ...]) -> None:
            if name in visited:
                return
            if name in visiting:
                cycle = " -> ".join((*lineage, name))
                raise ValueError(
                    f"feature graph {self.graph_id} contains a cycle: {cycle}"
                )
            visiting.add(name)
            for dependency_name in nodes[name].dependencies:
                visit(dependency_name, (*lineage, name))
            visiting.remove(name)
            visited.add(name)
            order.append(name)

        for node in self.nodes:
            visit(node.name, ())
        return tuple(order)

    def manifest(self) -> dict[str, object]:
        order = self.validate()
        return {
            "schema_version": "narrowgate_feature_dag.v1",
            "graph_id": self.graph_id,
            "topological_order": list(order),
            "nodes": [asdict(self.by_name()[name]) for name in order],
        }

    def sha256(self) -> str:
        payload = json.dumps(
            self.manifest(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


TEN_SECOND_CAUSAL_GRAPH = FeatureGraph(
    graph_id="live_10s_signal_cutoff.v1",
    nodes=(
        FeatureSpec(
            name="agg_trade_event",
            dependencies=(),
            unit="market_trade_event",
            source_clock="exchange_time_ms",
            availability_clock="receive_time_ns",
            cadence="event",
            lookback="none",
            stateful=False,
            invalid_policy="reject_invalid_trade",
            namespace="source",
            python_impl="strategy.signal.SignalEngine.on_agg_trade",
            cpp_impl="narrowgate_cpp::TradeBarAggregator::update",
        ),
        FeatureSpec(
            name="finalized_1s_bar",
            dependencies=("agg_trade_event",),
            unit="ohlcv_bar_1s",
            source_clock="exchange_time_ms",
            availability_clock="finalized_bar_time",
            cadence="1s",
            lookback="current_1s_bucket",
            stateful=True,
            invalid_policy="causal_flat_bar_for_completed_trade_gap",
            python_impl="strategy.signal.SignalEngine._finalize_bar",
            cpp_impl="narrowgate_cpp::TradeBarAggregator",
        ),
        FeatureSpec(
            name="prior_10s_feature_state",
            dependencies=(),
            unit="feature_state",
            source_clock="exchange_time_ms",
            availability_clock="feature_ready_time",
            cadence="10s",
            lookback="up_to_7d",
            stateful=True,
            invalid_policy="past_only_or_default",
            namespace="source",
            python_impl="strategy.signal.SignalEngine._feat_history",
            cpp_impl="narrowgate_cpp::SignalFeatureEngine::push_history",
            lagged_state=True,
        ),
        FeatureSpec(
            name="completed_10s_bar",
            dependencies=("finalized_1s_bar",),
            unit="ohlcv_bar_10s",
            source_clock="exchange_time_ms",
            availability_clock="finalized_bar_time",
            cadence="10s",
            lookback="10_exact_1s_bars",
            stateful=False,
            invalid_policy="fail_on_incomplete_1s_grid",
            python_impl="strategy.signal.SignalEngine._aggregate_bars",
        ),
        FeatureSpec(
            name="feature_cutoff_view",
            dependencies=("finalized_1s_bar", "completed_10s_bar"),
            unit="causal_bar_view",
            source_clock="exchange_time_ms",
            availability_clock="finalized_bar_time",
            cadence="10s",
            lookback="bars_strictly_before_cutoff",
            stateful=False,
            invalid_policy="strict_exclusive_cutoff",
            python_impl="strategy.signal.FeatureCutoff.visible_bars",
            cpp_impl="narrowgate_cpp::SignalFeatureEngine::compute_at_cutoff",
        ),
        FeatureSpec(
            name="signal_10s_feature_vector",
            dependencies=(
                "completed_10s_bar",
                "feature_cutoff_view",
                "prior_10s_feature_state",
            ),
            unit="typed_feature_vector",
            source_clock="exchange_time_ms",
            availability_clock="feature_ready_time",
            cadence="10s",
            lookback="node_specific_up_to_7d",
            stateful=True,
            invalid_policy="fail_closed_for_ml_contract",
            python_impl="strategy.signal.SignalEngine._compute_features",
            cpp_impl="narrowgate_cpp::SignalFeatureEngine::compute_at_cutoff",
        ),
        FeatureSpec(
            name="signal_13_head_prediction",
            dependencies=("signal_10s_feature_vector",),
            unit="model_head_vector",
            source_clock="exchange_time_ms",
            availability_clock="decision_time",
            cadence="10s",
            lookback="current_feature_vector",
            stateful=True,
            invalid_policy="ml_fail_closed",
            namespace="decision",
            python_impl="strategy.signal.SignalEngine._predict",
        ),
    ),
)


Q90_CAUSAL_GRAPH = FeatureGraph(
    graph_id="buy_q90_visibility_lifecycle_path_score.v3",
    nodes=(
        FeatureSpec(
            name="q90_exchange_book_event",
            dependencies=(),
            unit="native_book_event",
            source_clock="exchange_time_ns",
            availability_clock="receive_time_ns",
            cadence="book_event",
            lookback="native_sequence",
            stateful=True,
            invalid_policy="invalidate_on_sequence_gap",
            namespace="source",
            python_impl="live.orderbook.binance_usdm.BinanceUsdMDeepBook",
            cpp_impl="narrowgate_cpp::NativeBookState",
        ),
        FeatureSpec(
            name="q90_active_order_state",
            dependencies=(),
            unit="exchange_order_lifecycle_and_remaining_quantity",
            source_clock="exchange_time_ns",
            availability_clock="receive_time_ns",
            cadence="order_event",
            lookback="active_order_lifecycle",
            stateful=True,
            invalid_policy="terminal_order_leaves_fill_risk_set",
            namespace="source",
            python_impl="execution.order_lifecycle.QuantityWeightedOrderLifecycle",
        ),
        FeatureSpec(
            name="q90_quantity_weighted_exchange_exposure",
            dependencies=("q90_active_order_state",),
            unit="BTC*s",
            source_clock="exchange_time_ns",
            availability_clock="receive_time_ns",
            cadence="order_event",
            lookback="activation_to_exchange_terminal",
            stateful=True,
            invalid_policy="null_on_missing_or_regressed_exchange_clock",
            python_impl="execution.order_lifecycle.QuantityWeightedOrderLifecycle",
        ),
        FeatureSpec(
            name="q90_quantity_weighted_visible_exposure",
            dependencies=("q90_active_order_state",),
            unit="BTC*s",
            source_clock="visibility_time_ns",
            availability_clock="visibility_time_ns",
            cadence="order_event",
            lookback="visible_activation_to_visible_exchange_terminal",
            stateful=True,
            invalid_policy="zero_increment_outside_fill_risk_set",
            python_impl="execution.order_lifecycle.QuantityWeightedOrderLifecycle",
        ),
        FeatureSpec(
            name="q90_visible_book_state",
            dependencies=("q90_exchange_book_event",),
            unit="visible_native_book_state",
            source_clock="exchange_time_ns",
            availability_clock="feature_ready_time_ns",
            cadence="100ms_edge",
            lookback="latest_causally_visible_generation",
            stateful=True,
            invalid_policy="invalid_until_snapshot_sequence_recovers",
            python_impl="live.orderbook.binance_usdm.BinanceUsdMDeepBook",
            cpp_impl="narrowgate_cpp::NativeBookState",
        ),
        FeatureSpec(
            name="q90_active_order_depth_path",
            dependencies=("q90_visible_book_state", "q90_active_order_state"),
            unit="active_order_queue_path",
            source_clock="exchange_time_ns",
            availability_clock="feature_ready_time_ns",
            cadence="100ms_edge",
            lookback="activation_to_terminal",
            stateful=True,
            invalid_policy="invalid_outside_real_fill_risk_set",
            python_impl="execution.active_order_depth_path.ActiveOrderDepthPathTracker",
            cpp_impl="narrowgate_cpp::DynamicFillHazardRuntime",
        ),
        FeatureSpec(
            name="q90_model_feature_vector",
            dependencies=("q90_visible_book_state", "q90_active_order_depth_path"),
            unit="typed_feature_vector",
            source_clock="exchange_time_ns",
            availability_clock="feature_ready_time_ns",
            cadence="100ms_edge",
            lookback="active_order_path_to_decision",
            stateful=True,
            invalid_policy="feature_ready_must_not_exceed_decision_time",
            python_impl="strategy.dynamic_fill_hazard_model.build_dynamic_fill_hazard_features",
            cpp_impl="narrowgate_cpp::DynamicFillHazardRuntime",
        ),
        FeatureSpec(
            name="q90_adverse_fill_score",
            dependencies=("q90_model_feature_vector",),
            unit="dimensionless_score",
            source_clock="exchange_time_ns",
            availability_clock="decision_time_ns",
            cadence="100ms_edge",
            lookback="current_feature_vector",
            stateful=False,
            invalid_policy="hold_without_action_on_invalid_observation",
            namespace="decision",
            python_impl="strategy.dynamic_fill_hazard_model.DynamicFillHazardActionPolicy.score",
            cpp_impl="narrowgate_cpp::DynamicFillHazardRuntime",
        ),
    ),
)


CROSS_VENUE_FAIR_PRICE_GRAPH = FeatureGraph(
    graph_id="cross_venue_causal_fair_price.v1",
    nodes=(
        FeatureSpec(
            name="external_bbo_receive_state",
            dependencies=(),
            unit="venue_spot_perp_bbo",
            source_clock="exchange_time_ns",
            availability_clock="feature_ready_time_ns",
            cadence="book_event",
            lookback="latest_visible_per_venue_market",
            stateful=True,
            invalid_policy="require_feature_ready_not_after_decision",
            namespace="source",
            python_impl="strategy.signal.SignalEngine.on_book_ticker",
        ),
        FeatureSpec(
            name="stablecoin_anchor_bbo",
            dependencies=(),
            unit="USDT_per_USDC",
            source_clock="exchange_time_ns",
            availability_clock="feature_ready_time_ns",
            cadence="book_event",
            lookback="latest_visible_anchor",
            stateful=True,
            invalid_policy="invalidate_when_stale_or_future",
            namespace="source",
            python_impl="strategy.signal.SignalEngine.on_book_ticker",
        ),
        FeatureSpec(
            name="local_execution_mid",
            dependencies=(),
            unit="USDC_per_BTC",
            source_clock="exchange_time_ns",
            availability_clock="decision_time_ns",
            cadence="quote_decision",
            lookback="current_execution_bbo",
            stateful=False,
            invalid_policy="require_positive_non_crossed_bbo",
            namespace="source",
            python_impl="strategy.maker_engine.MakerEngine._requote",
        ),
        FeatureSpec(
            name="prior_basis_and_gain_state",
            dependencies=(),
            unit="past_only_EW_state",
            source_clock="feature_ready_time_ns",
            availability_clock="decision_time_ns",
            cadence="new_consensus_identity",
            lookback="causal_exponential_history",
            stateful=True,
            invalid_policy="warmup_without_quote_action",
            namespace="source",
            python_impl="strategy.cross_venue_fair_price.CrossVenueFairPriceEstimator",
            lagged_state=True,
        ),
        FeatureSpec(
            name="venue_basis_adjusted_price",
            dependencies=(
                "external_bbo_receive_state",
                "stablecoin_anchor_bbo",
                "local_execution_mid",
                "prior_basis_and_gain_state",
            ),
            unit="USDC_per_BTC",
            source_clock="exchange_time_ns",
            availability_clock="decision_time_ns",
            cadence="quote_decision",
            lookback="current_visible_sources_plus_prior_basis",
            stateful=False,
            invalid_policy="drop_invalid_or_stale_source",
            python_impl="strategy.cross_venue_fair_price.CrossVenueFairPriceEstimator.observe",
        ),
        FeatureSpec(
            name="weighted_median_fair_price",
            dependencies=("venue_basis_adjusted_price",),
            unit="USDC_per_BTC",
            source_clock="exchange_time_ns",
            availability_clock="decision_time_ns",
            cadence="quote_decision",
            lookback="at_least_two_visible_venues",
            stateful=False,
            invalid_policy="invalidate_on_support_or_dispersion_failure",
            python_impl="strategy.cross_venue_fair_price.weighted_median",
        ),
        FeatureSpec(
            name="causal_fair_center_shift",
            dependencies=(
                "weighted_median_fair_price",
                "local_execution_mid",
                "prior_basis_and_gain_state",
            ),
            unit="USDC_per_BTC",
            source_clock="exchange_time_ns",
            availability_clock="decision_time_ns",
            cadence="quote_decision",
            lookback="current_consensus_plus_prior_gain_state",
            stateful=True,
            invalid_policy="zero_shift_on_invalid_state",
            namespace="decision",
            python_impl="strategy.cross_venue_fair_price.CrossVenueFairPriceEstimator.observe",
        ),
        FeatureSpec(
            name="fair_center_quote_shadow",
            dependencies=("causal_fair_center_shift",),
            unit="shadow_quote_pair_USDC_per_BTC",
            source_clock="exchange_time_ns",
            availability_clock="decision_time_ns",
            cadence="quote_decision",
            lookback="current_baseline_pair",
            stateful=False,
            invalid_policy="baseline_fallback_and_no_order_action",
            namespace="decision",
            python_impl="strategy.cross_venue_fair_price.project_fair_center_shadow",
        ),
    ),
)


P3_TOUCH_CONDITIONAL_GRAPH = FeatureGraph(
    graph_id="p3_touch_volatility_conditioned.v4",
    nodes=(
        FeatureSpec(
            name="p3_causal_bbo",
            dependencies=(),
            unit="USDC_per_BTC_and_BTC",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="normalized_100ms_boundary_ms",
            cadence="100ms_state",
            lookback="latest_state_not_after_window_start",
            stateful=True,
            invalid_policy="reject_crossed_nonpositive_or_stale_bbo",
            namespace="source",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_official_aggressive_trade",
            dependencies=(),
            unit="Binance_BTCUSDC_aggTrade",
            source_clock="exchange_time_ms",
            availability_clock="historical_label_only",
            cadence="trade_event",
            lookback="current_non_overlapping_10s_target_window",
            stateful=False,
            invalid_policy="reject_invalid_price_or_timestamp",
            namespace="label",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_window_start_bbo",
            dependencies=("p3_causal_bbo",),
            unit="USDC_per_BTC",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="window_start_ms",
            cadence="10s",
            lookback="latest_state_not_after_window_start",
            stateful=False,
            invalid_policy="require_book_age_at_most_5000ms",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_fast_price_variance",
            dependencies=("p3_causal_bbo", "p3_window_start_bbo"),
            unit="(USDC_per_BTC)^2_per_s",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="window_start_ms",
            cadence="10s",
            lookback="10_strictly_causal_1s_mid_differences",
            stateful=False,
            invalid_policy="exclude_window_without_complete_causal_lookback",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_slow_price_variance",
            dependencies=("p3_causal_bbo", "p3_window_start_bbo"),
            unit="(USDC_per_BTC)^2_per_s",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="window_start_ms",
            cadence="10s",
            lookback="60_strictly_causal_1s_mid_differences",
            stateful=False,
            invalid_policy="exclude_window_without_complete_causal_lookback",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_raw_distance",
            dependencies=("p3_window_start_bbo",),
            unit="USDC_per_BTC",
            source_clock="decision_parameter",
            availability_clock="window_start_ms",
            cadence="distance_query",
            lookback="none",
            stateful=False,
            invalid_policy="require_frozen_distance_grid_support",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_volatility_conditioned.build_model_matrix"
            ),
        ),
        FeatureSpec(
            name="p3_volatility_normalized_distance",
            dependencies=("p3_raw_distance", "p3_slow_price_variance"),
            unit="dimensionless",
            source_clock="decision_parameter",
            availability_clock="window_start_ms",
            cadence="distance_query",
            lookback="10s_touch_horizon",
            stateful=False,
            invalid_policy="floor_variance_at_frozen_numerical_epsilon",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_volatility_conditioned.build_model_matrix"
            ),
        ),
        FeatureSpec(
            name="p3_causal_regime",
            dependencies=(
                "p3_fast_price_variance",
                "p3_slow_price_variance",
            ),
            unit="categorical_volatility_regime",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="window_start_ms",
            cadence="10s",
            lookback="fast_to_slow_variance_ratio",
            stateful=False,
            invalid_policy="use_frozen_outcome_blind_ratio_boundaries",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_volatility_conditioned.regime_code"
            ),
        ),
        FeatureSpec(
            name="p3_conditional_feature_vector",
            dependencies=(
                "p3_window_start_bbo",
                "p3_fast_price_variance",
                "p3_slow_price_variance",
                "p3_raw_distance",
                "p3_volatility_normalized_distance",
                "p3_causal_regime",
            ),
            unit="typed_feature_vector",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="window_start_ms",
            cadence="distance_query",
            lookback="current_causal_10s_window_state",
            stateful=False,
            invalid_policy="feature_ready_must_not_exceed_window_start",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_volatility_conditioned.build_model_matrix"
            ),
        ),
        FeatureSpec(
            name="p3_touch_label",
            dependencies=(
                "p3_official_aggressive_trade",
                "p3_window_start_bbo",
            ),
            unit="binary_touch_by_distance_within_10s",
            source_clock="exchange_time_ms",
            availability_clock="window_end_ms",
            cadence="distance_query",
            lookback="current_non_overlapping_10s_target_window",
            stateful=False,
            invalid_policy="exclude_window_without_valid_start_bbo",
            namespace="label",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_conditional_touch_probability",
            dependencies=("p3_conditional_feature_vector",),
            unit="probability",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="window_start_ms",
            cadence="distance_query",
            lookback="current_causal_10s_window_state",
            stateful=False,
            invalid_policy="research_prediction_unavailable_on_invalid_context",
            namespace="decision",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_volatility_conditioned.ConditionalTouchModel.predict"
            ),
        ),
    ),
)


P3_REACH_TIME_GRAPH = FeatureGraph(
    graph_id="p3_aggressive_reach_time_surface.v1",
    nodes=(
        FeatureSpec(
            name="p3_reach_time_causal_bbo",
            dependencies=(),
            unit="integer_price_ticks",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="decision_origin_ms",
            cadence="explicit_causal_decision_origin",
            lookback="latest_state_not_after_decision_origin",
            stateful=True,
            invalid_policy="reject_crossed_nonpositive_or_stale_bbo",
            namespace="source",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_reach_time_official_aggressive_trade",
            dependencies=(),
            unit="Binance_BTCUSDC_aggTrade_integer_price_ticks",
            source_clock="exchange_time_ms",
            availability_clock="historical_label_only",
            cadence="trade_event",
            lookback="strict_future_to_30s_administrative_censor",
            stateful=False,
            invalid_policy="reject_invalid_price_timestamp_or_side",
            namespace="label",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_surface.build_cumulative_reach_ticks"
            ),
        ),
        FeatureSpec(
            name="p3_reach_time_decision_bbo",
            dependencies=("p3_reach_time_causal_bbo",),
            unit="integer_price_ticks",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="decision_origin_ms",
            cadence="explicit_causal_decision_origin",
            lookback="latest_state_not_after_decision_origin",
            stateful=False,
            invalid_policy="require_frozen_book_freshness_contract",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_reach_time_distance_query",
            dependencies=("p3_reach_time_decision_bbo",),
            unit="integer_price_ticks",
            source_clock="decision_parameter",
            availability_clock="decision_origin_ms",
            cadence="distance_query",
            lookback="none",
            stateful=False,
            invalid_policy="unsupported_outside_frozen_distance_support",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_surface.reach_indicator_at_horizon"
            ),
        ),
        FeatureSpec(
            name="p3_reach_time_side_query",
            dependencies=(),
            unit="BUY_or_SELL",
            source_clock="decision_parameter",
            availability_clock="decision_origin_ms",
            cadence="side_query",
            lookback="none",
            stateful=False,
            invalid_policy="reject_noncanonical_side_or_side_pooling",
            namespace="source",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_surface.build_cumulative_reach_ticks"
            ),
        ),
        FeatureSpec(
            name="p3_reach_time_horizon_query",
            dependencies=(),
            unit="milliseconds",
            source_clock="decision_parameter",
            availability_clock="decision_origin_ms",
            cadence="100ms_query_grid",
            lookback="none",
            stateful=False,
            invalid_policy="unsupported_off_grid_or_beyond_30s_censor",
            namespace="source",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_surface.ReachTimeGridSpec"
            ),
        ),
        FeatureSpec(
            name="p3_reach_time_causal_context",
            dependencies=("p3_reach_time_decision_bbo",),
            unit="typed_feature_vector",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="decision_origin_ms",
            cadence="explicit_causal_decision_origin",
            lookback="strictly_past_context_only",
            stateful=False,
            invalid_policy="feature_ready_must_not_exceed_decision_origin",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_touch_window_context.extract_window_context"
            ),
        ),
        FeatureSpec(
            name="p3_reach_time_first_passage_label",
            dependencies=(
                "p3_reach_time_official_aggressive_trade",
                "p3_reach_time_decision_bbo",
                "p3_reach_time_distance_query",
                "p3_reach_time_side_query",
                "p3_reach_time_horizon_query",
            ),
            unit="interval_censored_binary_reach_by_side_distance_and_time",
            source_clock="exchange_time_ms",
            availability_clock="label_endpoint_ms",
            cadence="100ms_label_grid",
            lookback="strict_future_to_30s_administrative_censor",
            stateful=False,
            invalid_policy="explicit_invalid_or_right_censored",
            namespace="label",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_surface.build_cumulative_reach_ticks"
            ),
        ),
        FeatureSpec(
            name="p3_reach_time_probability_surface",
            dependencies=(
                "p3_reach_time_causal_context",
                "p3_reach_time_distance_query",
                "p3_reach_time_side_query",
                "p3_reach_time_horizon_query",
            ),
            unit="probability",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="decision_origin_ms",
            cadence="side_distance_time_query",
            lookback="current_causal_context_only",
            stateful=False,
            invalid_policy="research_prediction_unavailable_on_invalid_context",
            namespace="decision",
            python_impl="unimplemented_model_successor",
        ),
    ),
)


P3_REACH_TIME_CONDITIONED_GRAPH = FeatureGraph(
    graph_id="p3_aggressive_reach_time_surface.v2",
    nodes=(
        FeatureSpec(
            name="p3_rt_source_bbo",
            dependencies=(),
            unit="integer_price_ticks",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="feature_ready_time_ms",
            cadence="100ms_state",
            lookback="latest_state_not_after_canonical_origin",
            stateful=True,
            invalid_policy="reject_crossed_nonpositive_stale_or_off_tick_bbo",
            namespace="source",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_context.extract_reach_time_context"
            ),
        ),
        FeatureSpec(
            name="p3_rt_official_aggressive_trade",
            dependencies=(),
            unit="Binance_BTCUSDC_aggTrade_integer_price_ticks",
            source_clock="exchange_trade_time_ms",
            availability_clock="historical_label_only",
            cadence="trade_event",
            lookback="strict_future_to_30s_administrative_censor",
            stateful=False,
            invalid_policy="reject_invalid_price_timestamp_side_or_off_tick_trade",
            namespace="label",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_cache.build_reach_label_surface"
            ),
        ),
        FeatureSpec(
            name="p3_rt_canonical_origin",
            dependencies=("p3_rt_source_bbo",),
            unit="utc_timestamp_ms",
            source_clock="utc_exchange_day",
            availability_clock="canonical_origin_ms",
            cadence="10s_origin_sampling",
            lookback="60s_past_only_and_30s_same_day_future_support",
            stateful=False,
            invalid_policy="exclude_incomplete_warmup_or_right_censor_support",
            namespace="source",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_context.canonical_origins_ms"
            ),
        ),
        FeatureSpec(
            name="p3_rt_causal_context",
            dependencies=("p3_rt_source_bbo", "p3_rt_canonical_origin"),
            unit="typed_causal_context_vector",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="canonical_origin_ms",
            cadence="10s_origin_sampling",
            lookback="strictly_past_10s_and_60s_bbo_basis",
            stateful=False,
            invalid_policy="feature_ready_must_not_exceed_origin",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_context.extract_reach_time_context"
            ),
        ),
        FeatureSpec(
            name="p3_rt_distance_query",
            dependencies=(),
            unit="integer_price_ticks",
            source_clock="research_query_parameter",
            availability_clock="canonical_origin_ms",
            cadence="deterministic_outcome_blind_query",
            lookback="none",
            stateful=False,
            invalid_policy="unsupported_outside_frozen_distance_support",
            namespace="source",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_conditioned_hazard"
            ),
        ),
        FeatureSpec(
            name="p3_rt_risk_interval",
            dependencies=(),
            unit="100ms_interval_upper_endpoint",
            source_clock="research_query_parameter",
            availability_clock="canonical_origin_ms",
            cadence="100ms_label_grid",
            lookback="0_to_30s_administrative_censor",
            stateful=False,
            invalid_policy="reject_duplicate_skipped_or_beyond_censor_interval",
            namespace="source",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_conditioned_hazard"
            ),
        ),
        FeatureSpec(
            name="p3_rt_first_passage_label",
            dependencies=(
                "p3_rt_official_aggressive_trade",
                "p3_rt_canonical_origin",
                "p3_rt_distance_query",
                "p3_rt_risk_interval",
            ),
            unit="at_risk_and_first_reach_event",
            source_clock="exchange_trade_time_ms",
            availability_clock="label_interval_upper_endpoint_ms",
            cadence="100ms_label_grid",
            lookback="strict_future_first_passage_only",
            stateful=False,
            invalid_policy="explicit_right_censor_or_invalid_source_row",
            namespace="label",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_cache.build_reach_label_surface"
            ),
        ),
        FeatureSpec(
            name="p3_rt_hazard_feature_vector",
            dependencies=(
                "p3_rt_causal_context",
                "p3_rt_distance_query",
                "p3_rt_risk_interval",
            ),
            unit="typed_feature_vector",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="canonical_origin_ms",
            cadence="sampled_at_risk_interval",
            lookback="current_origin_context_only",
            stateful=False,
            invalid_policy="source_identity_and_year_forbidden_from_tradable_vector",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_conditioned_hazard"
            ),
        ),
        FeatureSpec(
            name="p3_rt_interval_hazard",
            dependencies=("p3_rt_hazard_feature_vector",),
            unit="conditional_first_passage_probability_per_100ms",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="canonical_origin_ms",
            cadence="100ms_query_grid",
            lookback="current_origin_context_and_query",
            stateful=False,
            invalid_policy="prediction_unavailable_outside_frozen_support",
            namespace="decision",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_conditioned_hazard"
            ),
        ),
        FeatureSpec(
            name="p3_rt_reach_cdf",
            dependencies=("p3_rt_interval_hazard",),
            unit="first_passage_cumulative_probability",
            source_clock="source_specific_exchange_or_provider_local_time_ms",
            availability_clock="canonical_origin_ms",
            cadence="side_distance_time_query",
            lookback="ordered_100ms_hazards_to_query_time",
            stateful=False,
            invalid_policy="fail_closed_on_probability_or_monotonicity_violation",
            namespace="decision",
            python_impl=(
                "research.families.f02_empirical_p3_touch.audit."
                "p3_reach_time_conditioned_hazard"
            ),
        ),
    ),
)


RUNTIME_FEATURE_GRAPHS = (
    TEN_SECOND_CAUSAL_GRAPH,
    Q90_CAUSAL_GRAPH,
    CROSS_VENUE_FAIR_PRICE_GRAPH,
)

RESEARCH_FEATURE_GRAPHS = (
    P3_TOUCH_CONDITIONAL_GRAPH,
    P3_REACH_TIME_GRAPH,
    P3_REACH_TIME_CONDITIONED_GRAPH,
)


def validate_feature_graphs(
    graphs: Iterable[FeatureGraph] = RUNTIME_FEATURE_GRAPHS,
) -> dict[str, str]:
    """Validate runtime graph contracts and return their identity hashes."""

    identities: dict[str, str] = {}
    for graph in graphs:
        graph.validate()
        if graph.graph_id in identities:
            raise ValueError(f"duplicate feature graph id {graph.graph_id}")
        identities[graph.graph_id] = graph.sha256()
    return identities


RUNTIME_FEATURE_GRAPH_IDENTITIES = validate_feature_graphs()
RESEARCH_FEATURE_GRAPH_IDENTITIES = validate_feature_graphs(
    RESEARCH_FEATURE_GRAPHS
)
