from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType, SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from data_paths import immutable_backtest_v12_config_path
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_panel_builder_v1 as builder,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_v1 as offline,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_features import (
    CHANNELS_BY_BLOCK,
    CausalWindowObservation,
)

SEQUENTIAL_V2_FIELDS_BY_ROLE = {
    "metadata": frozenset(
        {
            "campaign_cluster_id",
            "observation_end_ts_ns",
        }
    ),
    "replay_inputs": frozenset(
        {
            "assignment_equity_usdc",
            "assignment_to_common_washout_required",
            "campaign_id",
            "d_plus_1_context_receipt_sha256",
            "d_plus_1_feature_identity_sha256",
            "d_plus_1_market_identity_sha256",
            "d_plus_1_native_observation_sha256",
            "d_plus_1_new_target_assignments_allowed",
            "d_plus_1_utc_day",
            "day_input_sha256",
            "day_replay_workers",
            "exposure_fill_ordinal",
            "latency_identity_sha256",
            "market_window_identity_sha256",
            "model_overlay_identity_sha256",
            "order_id",
            "portable_day_cache_root",
            "portable_replay_binding_path",
            "portable_replay_binding_sha256",
            "queue_random_identity_sha256",
            "target_day_end_terminalized",
        }
    ),
}


@dataclass
class _ObservationCache:
    rows: tuple[CausalWindowObservation, ...]

    def observations(self):
        return iter(self.rows)

    def observations_between(self, *, start_feature_ready_ts_ns, end_feature_ready_ts_ns):
        return iter(
            row
            for row in self.rows
            if start_feature_ready_ts_ns <= row.feature_ready_ts_ns < end_feature_ready_ts_ns
        )


class _SyntheticAdapter:
    identity = builder.CANONICAL_ADAPTER_IDENTITY

    def __init__(self, feature_dag_sha256: str, *, invalid_result: bool = False) -> None:
        self.feature_dag_sha256 = feature_dag_sha256
        self.invalid_result = invalid_result

    def identity_hashes(self, request):
        del request
        return {
            "config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
            "code_sha256": "1" * 64,
            "model_sha256": "2" * 64,
            "p3_sha256": "3" * 64,
            "feature_dag_sha256": self.feature_dag_sha256,
            "execution_abi_sha256": "4" * 64,
            "baseline_identity_sha256": offline.ACTIVE_OWNER_POLICY_SHA256,
        }

    def run_day(self, request, *, emitter, evaluator):
        day_start_ns = int(
            datetime.fromisoformat(request.utc_day).replace(tzinfo=UTC).timestamp() * 1_000_000_000
        )
        fill_ns = day_start_ns + 100_000_000
        fill_ms = fill_ns // 1_000_000
        order_id = 0
        exposure_fill_ordinal = 1
        snapshot = emitter.capture_exposure_fill(
            assignment_id=(
                f"cooldown-v2:BTCUSDC:{fill_ms}:SELL:{order_id}:{exposure_fill_ordinal}"
            ),
            fill_event_id=f"fill:{order_id}:1:{fill_ms}:{exposure_fill_ordinal}",
            client_order_id=f"replay-order-{order_id}",
            lineage_id="cooldown-sell-lineage",
            lineage_revision=2,
            partial_fill_ordinal=1,
            partial_fill_qty_btc=0.001,
            fill_exchange_ts_ns=fill_ns,
            fill_visible_ts_ns=fill_ns,
            m0_context={
                "assignment_ts_ns": fill_ns,
                "fill_visible_ts_ns": fill_ns,
                "side": "SELL",
                "role_at_fill": "add",
                "inventory_before_fill_btc": -0.001,
                "inventory_after_fill_btc": -0.002,
                "fill_qty_btc": 0.001,
                "order_qty_btc": 0.001,
                "cumulative_filled_qty_before_btc": 0.0,
                "cumulative_filled_qty_after_btc": 0.001,
                "remaining_order_qty_after_btc": 0.0,
                "partial_fill_ordinal": 1,
                "fill_is_partial": False,
                "order_age_s": 4.0,
                "queue_ahead_before_fill_btc": None,
                "queue_state_before_fill": "unknown",
                "target_price_tick": 100,
                "target_price_displayed_qty_btc": None,
                "target_price_displayed_qty_status": "unknown",
                "target_price_displayed_qty_known": False,
                "target_price_displayed_qty_is_queue_ahead": False,
                "consecutive_units_after": 2.0,
                "baseline_duration_ms": 170_000.0,
                "campaign_age_s": 120.0,
                "campaign_add_count": 1,
                "campaign_mae_to_date_usdc": -0.01,
                "campaign_inventory_time_to_date_btc_s": 0.3,
                "last_same_side_fill_age_s": 90.0,
                "last_opposite_side_fill_age_s": None,
                "cooldown_remaining_ms": 0.0,
                "cooldown_blocker_active": False,
                "cooldown_lineage_revision_before": 1,
                "cooldown_deadline_owner": "none",
            },
        )
        evaluator.evaluate(snapshot, 170_000.0)
        result = {
            "schema_version": builder.ADAPTER_RESULT_SCHEMA,
            "identity": builder.CANONICAL_ADAPTER_IDENTITY,
            "utc_day": request.utc_day,
            "replay_engine": "python",
            "queue_identity": builder.QUEUE_IDENTITY,
            "same_millisecond_ambiguity_policy": "censor",
            "exposure_fill_scope": "exposure_increasing_only",
            "current_owner_b0_executed": True,
            "candidate_actions_generated": False,
            "economic_outcomes_read": False,
            "labels_read": False,
            "snapshots_emitted": 1,
            "market_window_identity_sha256": "5" * 64,
            "model_overlay_identity_sha256": "6" * 64,
            "latency_identity_sha256": "7" * 64,
            "queue_random_identity_sha256": "8" * 64,
            "replay_input_receipt_sha256": "7" * 64,
            "assignment_mechanics": {
                str(snapshot.snapshot_id): {
                    "campaign_id": 3,
                    "order_id": order_id,
                    "exposure_fill_ordinal": exposure_fill_ordinal,
                    "assignment_equity_usdc": 42.5,
                }
            },
        }
        if self.invalid_result:
            result["candidate_actions_generated"] = True
        return result


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_feature_manifest(root: Path, days: tuple[str, ...]) -> tuple[Path, str]:
    root.mkdir(parents=True)
    rows = []
    digest = hashlib.sha256()
    for day in days:
        path = root / f"features_{day}.parquet"
        pq.write_table(pa.table({"open": [100.0], "sample_weight": [1.0]}), path)
        row = {
            "day": day,
            "file": path.name,
            "size_bytes": path.stat().st_size,
            "sha256": _sha(path),
        }
        rows.append(row)
        digest.update(f"{day}\0{path.name}\0{path.stat().st_size}\0{_sha(path)}\n".encode())
    feature_dag_sha256 = "8" * 64
    manifest = {
        "labels_materialized": False,
        "exact_queue_policy_eligible": False,
        "formal_eligible": False,
        "config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        "feature_dag_sha256": feature_dag_sha256,
        "derived_datasets": [],
        "split": {"inference": list(days)},
        "daily_file_count": len(rows),
        "daily_manifest_sha256": digest.hexdigest(),
        "daily_files": rows,
    }
    path = root / "causal_feature_manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path, feature_dag_sha256


def _observations(day: str) -> tuple[CausalWindowObservation, ...]:
    start = int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1e9)
    values = MappingProxyType({spec.name: 100.0 for spec in CHANNELS_BY_BLOCK["M2"]})
    return (
        CausalWindowObservation(
            left_ts_ns=start,
            right_ts_ns=start + 100_000_000,
            feature_ready_ts_ns=start + 100_000_000,
            market_generation=1,
            depth_generation=1,
            values=values,
            warmup_admitted=True,
        ),
    )


def test_cache_stitch_drops_boundary_overlap_and_rebases_local_generations() -> None:
    boundary = 1_000_000_000
    values = MappingProxyType({spec.name: 100.0 for spec in CHANNELS_BY_BLOCK["M2"]})

    def row(right_ns: int, generation: int) -> CausalWindowObservation:
        return CausalWindowObservation(
            left_ts_ns=right_ns - 100_000_000,
            right_ts_ns=right_ns,
            feature_ready_ts_ns=right_ns,
            market_generation=generation,
            depth_generation=generation,
            values=values,
            warmup_admitted=True,
        )

    observed = tuple(
        builder._stitch_observation_caches(
            (row(boundary - 100_000_000, 7), row(boundary, 8)),
            (row(boundary, 4), row(boundary + 100_000_000, 5)),
        )
    )
    assert [item.right_ts_ns for item in observed] == [
        boundary - 100_000_000,
        boundary,
        boundary + 100_000_000,
    ]
    assert [item.market_generation for item in observed] == [7, 8, 9]
    assert [item.depth_generation for item in observed] == [7, 8, 9]


def _inputs_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    continuation_assignment_eligible: bool = False,
    observation_schema_version: str | None = None,
    observation_context_override: tuple[str, ...] | None = None,
):
    tmp_path.mkdir(parents=True, exist_ok=True)
    days = ("2026-06-27", "2026-06-28")
    observation_context_days = (*days, "2026-06-29")
    replay_context_days = ("2026-06-26", *observation_context_days)
    monkeypatch.setattr(offline, "REQUIRED_DAYS", 2)
    layout = offline.OfflineSourceLayout(
        project_data_root=tmp_path,
        marketdata_root=tmp_path / "market",
        raw_orderbook_root=tmp_path / "market/raw",
        normalized_roots=(tmp_path / "normalized",),
        aggtrades_root=tmp_path / "agg",
        individual_trades_root=tmp_path / "trades",
        sequence_audit_paths=(tmp_path / "sequence.json",),
    )
    source_path = tmp_path / "source.json"
    source_path.write_text("{}", encoding="ascii")
    receipt_files = {}
    target_receipts = []
    for index, day in enumerate(replay_context_days):
        receipt_files[day] = {"canonical_sha256": f"{index + 1:x}" * 64}
    for index, day in enumerate(days):
        target_receipts.append(
            {
                "utc_day": day,
                "day_receipt_sha256": f"{index + 3:x}" * 64,
                "context_days": {
                    "D_minus_1": "2026-06-26" if index == 0 else days[index - 1],
                    "D": day,
                    "D_plus_1": observation_context_days[index + 1],
                },
            }
        )
    target_receipts.append(
        {
            "utc_day": "2026-07-01",
            "day_receipt_sha256": "9" * 64,
            "context_days": {
                "D_minus_1": "2026-06-30",
                "D": "2026-07-01",
                "D_plus_1": "2026-07-02",
            },
        }
    )
    source = {
        "panel_role": builder.PANEL_ROLE,
        "queue_identity": builder.QUEUE_IDENTITY,
        "selected_days": list(days),
        "canonical_manifest_sha256": "9" * 64,
        "selection_sha256": "a" * 64,
        "target_day_receipts": target_receipts,
        "source_day_receipt_files": receipt_files,
    }
    monkeypatch.setattr(
        offline,
        "validate_canonical_manifest",
        lambda *args, **kwargs: source,
    )
    view_root = tmp_path / "book-view"
    files = []
    for day in replay_context_days:
        for kind in ("bbo", "l2"):
            path = view_root / kind / f"BTCUSDC-{kind}-{day}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table({"timestamp": [1]}), path)
            files.append({"day": day, "kind": kind, "sha256": _sha(path)})
    (view_root / "manifest.json").write_text("{}", encoding="ascii")
    book_view = {
        "selected_target_days": list(days),
        "context_days": list(replay_context_days),
        "source_manifest": {"canonical_sha256": "9" * 64},
        "same_millisecond_ambiguity_policy": "censor",
        "canonical_manifest_sha256": "b" * 64,
        "files": files,
    }
    monkeypatch.setattr(
        builder.mechanics,
        "validate_book_view",
        lambda *args, **kwargs: book_view,
    )
    observation_root = tmp_path / "observations"
    observation_root.mkdir()
    observation_manifest_path = observation_root / "batch.json"
    observation_manifest_path.write_text("{}", encoding="ascii")
    observation_rows = []
    for day in observation_context_days:
        is_target = day in days
        observation_rows.append(
            {
                "utc_day": day,
                "observation_role": "selected_target" if is_target else "continuation_only",
                "target_assignment_eligible": (
                    True if is_target else continuation_assignment_eligible
                ),
                "observation_receipt_sha256": "1" * 64,
                "cache_manifest_file_sha256": "c" * 64,
                "cache_parquet_sha256": "d" * 64,
                "cache_observation_sha256": "e" * 64,
                "source_binding_sha256": "f" * 64,
            }
        )
    observations = {
        "schema_version": (observation_schema_version or builder.observation_batch.SCHEMA_VERSION),
        "selected_target_days": list(days),
        "selected_target_day_count": len(days),
        "observation_context_days": list(observation_context_override or observation_context_days),
        "observation_context_day_count": len(
            observation_context_override or observation_context_days
        ),
        "continuation_only_days": ["2026-06-29"],
        "continuation_days_create_target_assignments": False,
        "source_manifest": {"canonical_manifest_sha256": "9" * 64},
        "canonical_manifest_sha256": "1" * 64,
        "days": observation_rows,
        "permissions": {
            "economic_outcomes_read": False,
            "labels_read": False,
            "actions_read": False,
        },
    }
    monkeypatch.setattr(
        builder.observation_batch,
        "validate_batch_manifest",
        lambda *args, **kwargs: observations,
    )
    monkeypatch.setattr(
        builder.observation_batch,
        "_observation_context_days",
        lambda *args, **kwargs: (
            observation_context_days,
            ("2026-06-29",),
        ),
    )
    monkeypatch.setattr(
        builder,
        "open_admitted_observation_cache",
        lambda root, day, deep: _ObservationCache(_observations(day)),
    )
    feature_manifest, feature_dag_sha256 = _write_feature_manifest(
        tmp_path / "features", replay_context_days
    )
    root = Path(__file__).resolve().parents[1]
    owner = builder.OwnerArtifactPaths(
        policy=root / "models/private/f05_boolean_cooldown_owner_v1/policy.json",
        predicate_bundle=root
        / "models/private/f05_boolean_cooldown_owner_v1/predicate_bundle.json",
        private_config=immutable_backtest_v12_config_path(root=root),
    )
    inputs = builder.validate_inputs(
        source_manifest_path=source_path,
        book_view_root=view_root,
        native_observation_manifest_path=observation_manifest_path,
        native_observation_root=observation_root,
        features_manifest_path=feature_manifest,
        owner_artifacts=owner,
        layout=layout,
    )
    return inputs, feature_dag_sha256


def test_contract_keeps_thirty_day_identity_and_default_adapter_is_available() -> None:
    assert len(offline.PRIMARY_TARGET_DAYS) == 30
    preflight = builder.adapter_preflight()
    assert preflight["status"] == "canonical_b0_mechanics_adapter_available"
    assert preflight["economic_outcomes_read"] is False


def test_owner_artifact_validation_reports_missing_file_separately(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing.json"
    paths = builder.OwnerArtifactPaths(
        policy=missing,
        predicate_bundle=missing,
        private_config=missing,
    )

    with pytest.raises(
        builder.OfflinePanelBuilderError,
        match="exact current owner policy file is missing",
    ):
        builder._validate_owner_artifacts(paths)


def test_owner_artifact_validation_reports_hash_drift_separately(
    tmp_path: Path,
) -> None:
    drifted = tmp_path / "drifted.json"
    drifted.write_text("{}\n", encoding="utf-8")
    paths = builder.OwnerArtifactPaths(
        policy=drifted,
        predicate_bundle=drifted,
        private_config=drifted,
    )

    with pytest.raises(
        builder.OfflinePanelBuilderError,
        match="exact current owner policy hash drifted",
    ):
        builder._validate_owner_artifacts(paths)


def test_builds_atomic_outcome_blind_rows_with_exact_owner_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs, feature_dag_sha256 = _inputs_fixture(tmp_path, monkeypatch)
    output = tmp_path / "panel-rows"
    result = builder.build_selected_days(
        inputs,
        output_root=output,
        adapter=_SyntheticAdapter(feature_dag_sha256),
    )

    assert result["selected_days"] == list(inputs.selected_days)
    assert all(value is False for value in result["permissions"].values())
    first = output / inputs.selected_days[0]
    day_manifest = builder.validate_day(first, expected_input_binding=inputs.input_binding_sha256)
    assert day_manifest["opportunity_count"] == 1
    assert day_manifest["replay_context_days"] == [
        "2026-06-26",
        "2026-06-27",
        "2026-06-28",
    ]
    assert day_manifest["continuation_context_day"] == "2026-06-28"
    assert day_manifest["new_target_assignments_from_continuation_day"] == 0
    assert set(day_manifest["files"]) == set(builder.PANEL_ROLES)
    metadata = pq.read_table(first / "metadata.parquet").to_pydict()
    owner = pq.read_table(first / "exact_owner_actions.parquet").to_pydict()
    replay = pq.read_table(first / "replay_inputs.parquet").to_pydict()
    boolean = pq.read_table(first / "boolean_features.parquet")
    continuous = pq.read_table(first / "continuous_features.parquet")
    role_columns = {
        role: set(pq.read_schema(first / f"{role}.parquet").names) for role in builder.PANEL_ROLES
    }
    assert sum(len(fields) for fields in SEQUENTIAL_V2_FIELDS_BY_ROLE.values()) == 23
    for expected_role, fields in SEQUENTIAL_V2_FIELDS_BY_ROLE.items():
        assert fields <= role_columns[expected_role]
        for other_role, columns in role_columns.items():
            if other_role != expected_role:
                assert fields.isdisjoint(columns)
    opportunity_id = metadata["opportunity_id"][0]
    assert opportunity_id.startswith("f05-offline-")
    assert owner["exact_owner_action"][0] in builder.OWNER_ACTIONS
    assert replay["candidate_actions_generated"] == [False]
    assert replay["economic_outcomes_read"] == [False]
    assert replay["source_manifest_canonical_sha256"] == ["9" * 64]
    assert replay["target_day_receipt_sha256"] == ["3" * 64]
    assert replay["continuation_day"] == ["2026-06-28"]
    assert replay["continuation_use_role"] == ["continuation_context_for_target"]
    assert replay["continuation_creates_target_assignments"] == [False]
    assert any(name.startswith("tri::") for name in boolean.schema.names)
    assert any(name.startswith("value::") for name in continuous.schema.names)
    assert "campaign_mae_to_date_usdc" in metadata
    assert not any("pnl" in name.lower() for name in metadata)

    second_output = tmp_path / "panel-rows-repeat"
    builder.build_selected_days(
        inputs,
        output_root=second_output,
        adapter=_SyntheticAdapter(feature_dag_sha256),
    )
    repeated = pq.read_table(
        second_output / inputs.selected_days[0] / "metadata.parquet",
        columns=["opportunity_id"],
    )["opportunity_id"].to_pylist()
    assert repeated == [opportunity_id]


@pytest.mark.parametrize(
    "observed_ids",
    (
        ("opportunity-1", "replacement-opportunity"),
        ("opportunity-2", "opportunity-1"),
    ),
)
def test_predecessor_opportunity_id_drift_is_rejected(
    tmp_path: Path,
    observed_ids: tuple[str, str],
) -> None:
    day = offline.PRIMARY_TARGET_DAYS[0]
    predecessor = (
        tmp_path
        / "cache"
        / "replay_dag"
        / "f05_full_multiscale_successor_offline_panel_builder_v1"
        / day
    )
    predecessor.mkdir(parents=True)
    pq.write_table(
        pa.table({"opportunity_id": ["opportunity-1", "opportunity-2"]}),
        predecessor / "metadata.parquet",
    )
    inputs = SimpleNamespace(
        selected_days=offline.PRIMARY_TARGET_DAYS,
        project_data_root=tmp_path,
    )

    with pytest.raises(
        builder.OfflinePanelBuilderError,
        match="opportunity IDs drifted from the immutable predecessor",
    ):
        builder._predecessor_day_opportunity_sha256(
            inputs,
            utc_day=day,
            observed_ids=observed_ids,
        )


def test_formal_panel_rejects_predecessor_denominator_drift(tmp_path: Path) -> None:
    panel_root = tmp_path / "v2" / "panel"
    panel_root.mkdir(parents=True)
    selected_days = offline.PRIMARY_TARGET_DAYS
    opportunity_ids = [
        f"opportunity-{index:04d}" for index in range(builder.FORMAL_OPPORTUNITY_COUNT - 1)
    ]
    table = pa.table(
        {
            "utc_day": [selected_days[0]] * len(opportunity_ids),
            "opportunity_id": opportunity_ids,
        }
    )
    for role in builder.PANEL_ROLES:
        pq.write_table(table, panel_root / f"{role}.parquet")
    inputs = SimpleNamespace(
        selected_days=selected_days,
        observation_context_days=selected_days,
        continuation_only_days=(),
        replay_context_days=selected_days,
        input_binding_sha256="a" * 64,
        project_data_root=tmp_path,
    )
    manifest = builder._merged_panel_manifest(
        inputs=inputs,
        day_manifests=(),
        panel_root=panel_root,
    )
    (panel_root / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True),
        encoding="ascii",
    )

    with pytest.raises(
        builder.OfflinePanelBuilderError,
        match="opportunity denominator is not 3,516",
    ):
        builder._validate_merged_panel(
            panel_root,
            inputs=inputs,
            day_manifests=(),
        )


def test_adapter_cannot_generate_candidate_actions_or_leave_partial_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs, feature_dag_sha256 = _inputs_fixture(tmp_path, monkeypatch)
    output = tmp_path / "rejected"
    with pytest.raises(builder.OfflinePanelBuilderError, match="contract drifted"):
        builder.materialize_day(
            inputs,
            inputs.selected_days[0],
            output_root=output,
            adapter=_SyntheticAdapter(feature_dag_sha256, invalid_result=True),
        )
    assert not (output / inputs.selected_days[0]).exists()
    assert not list(output.glob(".*.staging-*"))


def test_post_publish_validation_failure_removes_day_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs, feature_dag_sha256 = _inputs_fixture(tmp_path, monkeypatch)
    output = tmp_path / "post-publish-rejected"
    day = inputs.selected_days[0]

    def reject(*args, **kwargs):
        del args, kwargs
        raise builder.OfflinePanelBuilderError("forced post-publish rejection")

    monkeypatch.setattr(builder, "validate_day", reject)
    with pytest.raises(builder.OfflinePanelBuilderError, match="post-publish"):
        builder.materialize_day(
            inputs,
            day,
            output_root=output,
            adapter=_SyntheticAdapter(feature_dag_sha256),
        )
    assert not (output / day).exists()


def test_features_only_manifest_with_label_column_fails_before_replay(
    tmp_path: Path,
) -> None:
    day = "2026-06-27"
    root = tmp_path / "features"
    root.mkdir()
    feature = root / f"features_{day}.parquet"
    pq.write_table(pa.table({"open": [1.0], "label_ret_10s": [0.0]}), feature)
    digest = hashlib.sha256(
        f"{day}\0{feature.name}\0{feature.stat().st_size}\0{_sha(feature)}\n".encode()
    ).hexdigest()
    manifest = {
        "labels_materialized": False,
        "config_sha256": offline.ACTIVE_PRIVATE_CONFIG_SHA256,
        "feature_dag_sha256": "8" * 64,
        "derived_datasets": [],
        "split": {"inference": [day]},
        "daily_file_count": 1,
        "daily_manifest_sha256": digest,
        "daily_files": [
            {
                "day": day,
                "file": feature.name,
                "size_bytes": feature.stat().st_size,
                "sha256": _sha(feature),
            }
        ],
    }
    path = root / "causal_feature_manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(builder.OfflinePanelBuilderError, match="contains labels"):
        builder._validate_features_only_manifest(path, required_context_days=(day,))


def test_observation_v2_rejects_assignment_eligible_continuation_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(
        builder.OfflinePanelBuilderError,
        match="role/assignment contract drifted: 2026-06-29",
    ):
        _inputs_fixture(
            tmp_path,
            monkeypatch,
            continuation_assignment_eligible=True,
        )


def test_observation_manifest_requires_exact_v2_schema_and_context_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(builder.OfflinePanelBuilderError, match="source/day identity drifted"):
        _inputs_fixture(
            tmp_path / "v1",
            monkeypatch,
            observation_schema_version=(f"{builder.observation_batch.IDENTITY}.manifest.v1"),
        )
    with pytest.raises(builder.OfflinePanelBuilderError, match="source/day identity drifted"):
        _inputs_fixture(
            tmp_path / "missing-continuation",
            monkeypatch,
            observation_context_override=("2026-06-27", "2026-06-28"),
        )


def test_cli_has_no_day_override_and_bounds_workers() -> None:
    assert builder.parse_args(["build"]).workers == 1
    assert builder.parse_args(["build", "--workers", "8"]).workers == 8
    with pytest.raises(SystemExit):
        builder.parse_args(["build", "--workers", "9"])
    with pytest.raises(SystemExit):
        builder.parse_args(["build", "--days", "2026-06-27"])


def test_interrupted_build_reuses_valid_day_and_publishes_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs, feature_dag_sha256 = _inputs_fixture(tmp_path, monkeypatch)
    output = tmp_path / "resumable-panel"
    synthetic = _SyntheticAdapter(feature_dag_sha256)
    builder.materialize_day(
        inputs,
        inputs.selected_days[0],
        output_root=output,
        adapter=synthetic,
    )
    result = builder.build_selected_days(
        inputs,
        output_root=output,
        adapter=synthetic,
        workers=1,
    )
    assert result["reused_day_count"] == 1
    assert result["newly_materialized_day_count"] == 1
    progress = json.loads((output / "progress.json").read_text(encoding="ascii"))
    assert progress["status"] == "complete"
    assert progress["total_days"] == len(inputs.selected_days)
    assert progress["observation_context_day_count"] == 3
    assert progress["continuation_only_day_count"] == 1
    assert progress["workers"] == 1
    assert progress["reused_days"] == [inputs.selected_days[0]]
    assert progress["completed_days"] == [inputs.selected_days[1]]
    assert progress["pending_days"] == []
    assert progress["running_days"] == []
    assert progress["failed_days"] == {}
    assert (output / "panel" / "manifest.json").is_file()
    assert builder.validate_panel(output, inputs=inputs) == result
