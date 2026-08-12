"""Static materialization contracts for reusable replay-window inputs.

This graph describes cache boundaries, not execution policy. Persistent nodes
must be reproducible before any strategy action can change the replay path.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Literal

Materialization = Literal["source", "persistent", "ephemeral", "forbidden"]


@dataclass(frozen=True)
class ReplayCacheNodeSpec:
    name: str
    dependencies: tuple[str, ...]
    materialization: Materialization
    artifact_unit: str
    source_clock: str
    visibility_clock: str
    cache_namespace: str = ""
    identity_fields: tuple[str, ...] = ()
    strategy_dependent: bool = False
    implementation_status: str = "declared"

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("replay cache node name cannot be empty")
        if self.materialization not in {
            "source",
            "persistent",
            "ephemeral",
            "forbidden",
        }:
            raise ValueError(f"replay cache node {self.name} has invalid materialization")
        for field_name in (
            "artifact_unit",
            "source_clock",
            "visibility_clock",
            "implementation_status",
        ):
            if not str(getattr(self, field_name)).strip():
                raise ValueError(f"replay cache node {self.name} lacks {field_name}")
        if self.materialization == "persistent":
            if self.strategy_dependent:
                raise ValueError(f"strategy-dependent node {self.name} cannot be persistent")
            if not self.cache_namespace.strip() or not self.identity_fields:
                raise ValueError(f"persistent node {self.name} lacks cache identity")
        elif self.cache_namespace or self.identity_fields:
            raise ValueError(f"non-persistent node {self.name} cannot declare a cache key")
        if self.materialization == "forbidden" and not self.strategy_dependent:
            raise ValueError(f"forbidden cache node {self.name} must be strategy-dependent")


@dataclass(frozen=True)
class ReplayCacheGraph:
    graph_id: str
    nodes: tuple[ReplayCacheNodeSpec, ...]

    def validate(self) -> tuple[str, ...]:
        names = [node.name for node in self.nodes]
        if len(names) != len(set(names)):
            raise ValueError(f"duplicate replay cache nodes in {self.graph_id}")
        by_name = {node.name: node for node in self.nodes}
        for node in self.nodes:
            missing = sorted(set(node.dependencies).difference(by_name))
            if missing:
                raise ValueError(
                    f"replay cache node {node.name} has missing dependencies: {missing}"
                )
        visiting: set[str] = set()
        visited: set[str] = set()
        order: list[str] = []

        def visit(name: str) -> None:
            if name in visited:
                return
            if name in visiting:
                raise ValueError(f"replay cache graph {self.graph_id} has a cycle")
            visiting.add(name)
            for dependency in by_name[name].dependencies:
                visit(dependency)
            visiting.remove(name)
            visited.add(name)
            order.append(name)

        for name in names:
            visit(name)

        strategy_ancestry: dict[str, bool] = {}
        for name in order:
            node = by_name[name]
            strategy_ancestry[name] = node.strategy_dependent or any(
                strategy_ancestry[dependency] for dependency in node.dependencies
            )
            if node.materialization == "persistent" and any(
                strategy_ancestry[dependency] for dependency in node.dependencies
            ):
                raise ValueError(f"persistent node {node.name} has strategy-dependent ancestry")
        return tuple(order)

    def manifest(self) -> dict[str, object]:
        order = self.validate()
        by_name = {node.name: node for node in self.nodes}
        return {
            "schema_version": "narrowgate.replay_cache_dag.v1",
            "graph_id": self.graph_id,
            "topological_order": list(order),
            "nodes": [asdict(by_name[name]) for name in order],
        }

    def sha256(self) -> str:
        payload = json.dumps(
            self.manifest(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        return hashlib.sha256(payload).hexdigest()


REPLAY_WINDOW_CACHE_GRAPH = ReplayCacheGraph(
    graph_id="tick_replay_window_materialization.v1",
    nodes=(
        ReplayCacheNodeSpec(
            name="execution_trade_day_source",
            dependencies=(),
            materialization="source",
            artifact_unit="individual_or_aggregate_trade_rows",
            source_clock="exchange_time",
            visibility_clock="source_file_complete",
        ),
        ReplayCacheNodeSpec(
            name="normalized_book_day_source",
            dependencies=(),
            materialization="source",
            artifact_unit="normalized_bbo_l2_100ms_day",
            source_clock="exchange_time",
            visibility_clock="causal_snapshot_time",
        ),
        ReplayCacheNodeSpec(
            name="native_orderbook_hour_source",
            dependencies=(),
            materialization="source",
            artifact_unit="cryptohft_snapshot_delta_level_rows",
            source_clock="exchange_time",
            visibility_clock="provider_receive_time",
        ),
        ReplayCacheNodeSpec(
            name="native_orderbook_logical_hour",
            dependencies=("native_orderbook_hour_source",),
            materialization="persistent",
            artifact_unit="strategy_independent_logical_book_messages",
            source_clock="exchange_time",
            visibility_clock="provider_receive_time",
            cache_namespace="native_exchange_book_hour_v1",
            identity_fields=(
                "source_path",
                "source_size",
                "source_mtime_ns",
                "symbol",
                "exchange",
                "tick_size",
                "parser_identity_sha256",
                "event_schema_version",
            ),
            implementation_status="implemented",
        ),
        ReplayCacheNodeSpec(
            name="rolling_market_context_day",
            dependencies=(
                "execution_trade_day_source",
                "normalized_book_day_source",
            ),
            materialization="persistent",
            artifact_unit="variance_bbo_l2_context_arrays",
            source_clock="exchange_time",
            visibility_clock="causal_context_cutoff",
            cache_namespace="window_component_market_context_v1",
            identity_fields=(
                "source_signatures",
                "warmup_days",
                "book_dataset_identity",
                "transform_identity",
            ),
            implementation_status="implemented_components_v1_with_v13_read_compatibility",
        ),
        ReplayCacheNodeSpec(
            name="causal_feature_day",
            dependencies=("rolling_market_context_day",),
            materialization="persistent",
            artifact_unit="causal_feature_block",
            source_clock="exchange_time",
            visibility_clock="feature_ready_time",
            cache_namespace="causal_feature_day_v1",
            identity_fields=(
                "feature_dag_sha256",
                "source_signatures",
                "cutoff_contract",
                "feature_semantics_version",
            ),
            implementation_status="existing_frozen_bundles",
        ),
        ReplayCacheNodeSpec(
            name="model_prediction_day",
            dependencies=("causal_feature_day",),
            materialization="persistent",
            artifact_unit="model_head_predictions",
            source_clock="exchange_time",
            visibility_clock="feature_ready_time",
            cache_namespace="model_prediction_day_v1",
            identity_fields=(
                "feature_artifact_sha256",
                "model_bundle_sha256",
                "inference_semantics_version",
            ),
            implementation_status="implemented_components_v1",
        ),
        ReplayCacheNodeSpec(
            name="window_assembly",
            dependencies=(
                "rolling_market_context_day",
                "native_orderbook_logical_hour",
                "model_prediction_day",
            ),
            materialization="ephemeral",
            artifact_unit="in_memory_window_view",
            source_clock="mixed_declared_clocks",
            visibility_clock="decision_scheduler",
            implementation_status="ephemeral_components_v1_legacy_v13_read_only",
        ),
        ReplayCacheNodeSpec(
            name="strategy_order_lifecycle",
            dependencies=("window_assembly",),
            materialization="forbidden",
            artifact_unit="orders_cancel_ack_queue_fill_path",
            source_clock="exchange_and_action_time",
            visibility_clock="decision_scheduler",
            strategy_dependent=True,
            implementation_status="must_replay_per_arm",
        ),
        ReplayCacheNodeSpec(
            name="inventory_campaign_outcome",
            dependencies=("strategy_order_lifecycle",),
            materialization="forbidden",
            artifact_unit="inventory_campaign_reward_path",
            source_clock="action_path_time",
            visibility_clock="decision_scheduler",
            strategy_dependent=True,
            implementation_status="must_replay_per_arm",
        ),
    ),
)


# Keep the v1 graph and identity stable because existing native-hour artifacts
# bind it byte-for-byte. New component artifacts bind this v2 graph instead of
# invalidating the already reusable native layer.
REPLAY_WINDOW_CACHE_GRAPH_V2 = ReplayCacheGraph(
    graph_id="tick_replay_window_materialization.v2",
    nodes=(
        ReplayCacheNodeSpec(
            name="execution_trade_day_source",
            dependencies=(),
            materialization="source",
            artifact_unit="execution_trade_rows",
            source_clock="exchange_time",
            visibility_clock="source_file_complete",
        ),
        ReplayCacheNodeSpec(
            name="normalized_book_day_source",
            dependencies=(),
            materialization="source",
            artifact_unit="normalized_bbo_l2_100ms_day",
            source_clock="exchange_time",
            visibility_clock="causal_snapshot_time",
        ),
        ReplayCacheNodeSpec(
            name="native_orderbook_hour_source",
            dependencies=(),
            materialization="source",
            artifact_unit="snapshot_delta_level_rows",
            source_clock="exchange_time",
            visibility_clock="provider_receive_time",
        ),
        ReplayCacheNodeSpec(
            name="feature_day_source",
            dependencies=(),
            materialization="source",
            artifact_unit="causal_feature_rows",
            source_clock="exchange_time",
            visibility_clock="feature_ready_time",
        ),
        ReplayCacheNodeSpec(
            name="model_bundle_source",
            dependencies=(),
            materialization="source",
            artifact_unit="model_bundle",
            source_clock="artifact_build_time",
            visibility_clock="replay_start",
        ),
        ReplayCacheNodeSpec(
            name="native_book_hour",
            dependencies=("native_orderbook_hour_source",),
            materialization="persistent",
            artifact_unit="logical_book_messages_hour",
            source_clock="exchange_time",
            visibility_clock="provider_receive_time",
            cache_namespace="native_exchange_book_hour_v1",
            identity_fields=(
                "source_signature",
                "symbol",
                "exchange",
                "tick_size",
                "parser_identity_sha256",
                "event_schema_version",
            ),
            implementation_status="implemented_v1_identity_preserved",
        ),
        ReplayCacheNodeSpec(
            name="market_context_day_v2",
            dependencies=(
                "execution_trade_day_source",
                "normalized_book_day_source",
            ),
            materialization="persistent",
            artifact_unit="directory_trades_rolling_arrays_source_refs",
            source_clock="exchange_time",
            visibility_clock="causal_context_cutoff",
            cache_namespace="market_context_day_v2",
            identity_fields=(
                "source_references",
                "warmup_days",
                "book_dataset_identity",
                "transform_identity_sha256",
                "schema_sha256",
            ),
            implementation_status="implemented",
        ),
        ReplayCacheNodeSpec(
            name="model_overlay_day",
            dependencies=(
                "market_context_day_v2",
                "feature_day_source",
                "model_bundle_source",
            ),
            materialization="persistent",
            artifact_unit="compressed_model_prediction_arrays",
            source_clock="exchange_time",
            visibility_clock="feature_ready_time",
            cache_namespace="model_overlay_day_v1",
            identity_fields=(
                "market_context_identity_sha256",
                "feature_source_identity",
                "model_bundle_identity",
                "inference_semantics",
                "schema_sha256",
            ),
            implementation_status="implemented",
        ),
        ReplayCacheNodeSpec(
            name="window_data",
            dependencies=(
                "native_book_hour",
                "market_context_day_v2",
                "model_overlay_day",
            ),
            materialization="ephemeral",
            artifact_unit="in_memory_window_data",
            source_clock="declared_component_clocks",
            visibility_clock="decision_scheduler",
            implementation_status="implemented_no_v2_persistence",
        ),
        ReplayCacheNodeSpec(
            name="action_dependent_replay_state",
            dependencies=("window_data",),
            materialization="forbidden",
            artifact_unit="orders_queue_fills_inventory_campaign_pnl",
            source_clock="replay_event_clock",
            visibility_clock="decision_time",
            strategy_dependent=True,
            implementation_status="shared_cache_forbidden",
        ),
    ),
)


REPLAY_WINDOW_CACHE_GRAPH_IDENTITY = REPLAY_WINDOW_CACHE_GRAPH.sha256()
REPLAY_WINDOW_CACHE_GRAPH_V2_IDENTITY = REPLAY_WINDOW_CACHE_GRAPH_V2.sha256()
