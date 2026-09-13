from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_label_panel import (
    FORMAL_DAY_SCHEMA_VERSION,
    PANEL_IDENTITY,
    LabelPanelError,
    assemble_day_label_panel,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    ARM_RESULT_SCHEMA_VERSION,
    OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_shared_prefix import (
    SCHEMA_VERSION as PREFIX_SCHEMA_VERSION,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_snapshot import (
    SNAPSHOT_SCHEMA_VERSION,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_strict_checkpoint import (
    BUY_ARMS,
)

IDENTITY = "causal_multichannel_window_boolean_cooldown_duration_v2"


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _fixture(
    tmp_path: Path,
    *,
    censored_arm: str | None,
    queue_missing_arm: str | None = None,
) -> Path:
    snapshot_id = "cooldown-v2-" + "a" * 64
    fill_visible_ts_ms = 1_000
    fill_exchange_ts_ms = 999
    source_bundle_sha256 = "b" * 64
    source_contract_sha256 = "d" * 64
    identity_hashes = {
        "baseline_identity_sha256": "1" * 64,
        "config_sha256": "2" * 64,
        "code_sha256": "3" * 64,
        "model_sha256": "4" * 64,
        "p3_sha256": "5" * 64,
        "feature_dag_sha256": "6" * 64,
        "execution_abi_sha256": "7" * 64,
    }
    m0 = {
        "assignment_ts_ns": fill_visible_ts_ms * 1_000_000,
        "fill_visible_ts_ns": fill_visible_ts_ms * 1_000_000,
        "side": "BUY",
        "role_at_fill": "opener",
        "fill_qty_btc": 0.001,
        "partial_fill_ordinal": 1,
        "baseline_duration_ms": 85_000.0,
    }
    feature = {
        **m0,
        "feature_block": "R0",
        "tri::mid::positive_ordering": 1,
    }
    snapshot_payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "snapshot_id": snapshot_id,
        "assignment_id": "assignment-1",
        "fill_event_id": "fill-1",
        "client_order_id": "replay-order-9",
        "lineage_id": "lineage-1",
        "lineage_revision": 1,
        "partial_fill_ordinal": 1,
        "partial_fill_qty_btc": 0.001,
        "visibility_profile": "historical_exchange_event_visibility_exploratory",
        "receive_time_transport_eligible": False,
        "clocks": {
            "assignment": {"ts_ns": fill_visible_ts_ms * 1_000_000},
            "fill_exchange": {"ts_ns": fill_exchange_ts_ms * 1_000_000},
            "fill_receive": {"ts_ns": None},
            "fill_visible": {"ts_ns": fill_visible_ts_ms * 1_000_000},
            "feature_ready": {"ts_ns": fill_visible_ts_ms * 1_000_000},
        },
        "sources": {"market": {"generation": 1}},
        "source_bundle_sha256": source_bundle_sha256,
        "identity_hashes": identity_hashes,
        "m0_context": m0,
        "feature_block": "R0",
        "feature_row": feature,
        "field_validity": {"m0.side": {"valid": True}},
        "policy_input_valid": True,
        "fallback_policy_id": None,
        "fallback_reason": None,
        "economic_outcomes_read": False,
    }
    snapshot_payload_json = json.dumps(
        snapshot_payload,
        sort_keys=True,
        separators=(",", ":"),
    )
    snapshots_path = tmp_path / "assignment_snapshots.parquet"
    pd.DataFrame(
        [
            {
                "snapshot_id": snapshot_id,
                "assignment_id": "assignment-1",
                "fill_event_id": "fill-1",
                "client_order_id": "replay-order-9",
                "lineage_id": "lineage-1",
                "lineage_revision": 1,
                "partial_fill_ordinal": 1,
                "partial_fill_qty_btc": 0.001,
                "visibility_profile": snapshot_payload["visibility_profile"],
                "receive_time_transport_eligible": False,
                "source_bundle_sha256": source_bundle_sha256,
                "feature_block": "R0",
                "m0_context_json": json.dumps(m0),
                "feature_row_json": json.dumps(feature),
                "snapshot_payload_json": snapshot_payload_json,
                "snapshot_payload_sha256": _canonical_sha256(snapshot_payload),
                "policy_input_valid": True,
                "fallback_policy_id": None,
                "fallback_reason": None,
                "economic_outcomes_read": False,
            }
        ]
    ).to_parquet(snapshots_path, index=False)

    opportunity_contract = {
        "schema_version": PREFIX_SCHEMA_VERSION,
        "identity": IDENTITY,
        "target_day": "2026-04-17",
        "source_contract_sha256": source_contract_sha256,
        "execution_identity_hashes": identity_hashes,
        "opportunity": {
            "exposure_fill_ordinal": 1,
            "partial_fill_ordinal": 1,
            "fill_visible_ts_ms": fill_visible_ts_ms,
            "fill_exchange_ts_ms": fill_exchange_ts_ms,
            "side": "BUY",
            "role_at_fill": "opener",
            "order_id": 9,
            "campaign_id": 7,
            "fill_qty_btc": 0.001,
            "baseline_duration_ms": 85_000.0,
            "cooldown_v2_snapshot_id": snapshot_id,
            "cooldown_v2_source_bundle_sha256": source_bundle_sha256,
            "exchange_book_queue_mode": "strict",
            "exchange_book_queue_scope": (
                "strategy_independent_native_snapshot_delta_exchange_time_v1"
            ),
            "strict_counter_baseline": {},
            "exchange_book_queue_missing_trace_cursor": 0,
            "exchange_book_queue_missing_count_at_assignment": 0,
        },
        "checkpoint_semantics": "posix_fork_copy_on_write_at_fill_callback",
        "portable_restore_authority": False,
        "economic_outcomes_read_before_fork": False,
    }
    opportunity_id = _canonical_sha256(opportunity_contract)
    opportunity_root = tmp_path / "labels" / opportunity_id
    opportunity_root.mkdir(parents=True)
    arm_rows = []
    for arm in BUY_ARMS:
        right_censored = arm == censored_arm
        trace = {
            "schema_version": "multiscale_ema_boolean_cooldown_duration_fork_trace.v2",
            "action": "CONTROL_85N" if arm == "CONTROL_85N" else "FIXED_DURATION_MS",
            "side": "BUY",
            "campaign_id": 7,
            "assignment_ts_ms": 1_000,
            "baseline_duration_ms": 85_000.0,
            "applied_duration_ms": 85_000.0,
            "arm_washout_complete": not right_censored,
            "terminal_ts_ms": 2_000,
            "terminal_reason": "flat" if not right_censored else "data_boundary_right_censored",
            "right_censored": right_censored,
            "assignment_to_washout_value_usdc": (
                None if right_censored or arm == queue_missing_arm else 1.0
            ),
            "censor_time_mid_mark_usdc": 0.5 if right_censored else None,
            "censor_time_executable_mark_usdc": 0.4 if right_censored else None,
            "censor_marks_are_terminal_bounds": False,
            "accounting_residual_usdc": 0.0,
            "second_assignment_count": 0,
            "reducing_quote_change_count": 0,
            "inventory_time_btc_s": 0.2,
            "mae_usdc": 0.1,
            "max_abs_inventory_btc": 0.001,
        }
        payload = {
            "schema_version": ARM_RESULT_SCHEMA_VERSION,
            "identity": IDENTITY,
            "opportunity_identity_sha256": opportunity_id,
            "arm_id": arm,
            "action": trace["action"],
            "fixed_duration_ms": 0.0,
            "fork_trace": trace,
            "prefix_execution_contract": {
                "exchange_book_queue_missing_count": 0,
                "exchange_book_queue_missing_count_at_assignment": 0,
                "exchange_book_queue_missing_trace_cursor": 0,
            },
            "strict_execution_contract": {
                "exchange_book_queue_missing_count": (
                    1 if arm == queue_missing_arm else 0
                ),
                "exchange_book_queue_missing_trace": (
                    [
                        {
                            "order_id": 9,
                            "side": "BUY",
                            "price": 90_000.0,
                            "price_tick": 900_000,
                            "activate_ts_ms": 1_100,
                            "status": "missing",
                            "reason": "outside_snapshot_range",
                            "asof_exchange_ts_ns": 1_099_000_000,
                            "segment_id": 3,
                            "snapshot_min_tick": 899_990,
                            "snapshot_max_tick": 899_999,
                        }
                    ]
                    if arm == queue_missing_arm
                    else []
                ),
                "strict_native_label_eligible": arm != queue_missing_arm,
                "strict_native_label_unsupported_reasons": (
                    ["exchange_book_queue_missing_count"]
                    if arm == queue_missing_arm
                    else []
                ),
                "economic_point_label_status": (
                    "unsupported_redacted"
                    if arm == queue_missing_arm
                    else "eligible"
                ),
            },
        }
        payload["canonical_result_sha256"] = _canonical_sha256(payload)
        arm_path = opportunity_root / f"arm-{arm}.json"
        _write_json(arm_path, payload)
        arm_rows.append(
            {"arm_id": arm, "path": arm_path.name, "sha256": _sha256(arm_path)}
        )
    opportunity_manifest = {
        "schema_version": OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
        "identity": IDENTITY,
        "opportunity_identity_sha256": opportunity_id,
        "opportunity_contract": opportunity_contract,
        "arms": arm_rows,
    }
    opportunity_manifest_path = opportunity_root / "manifest.json"
    _write_json(opportunity_manifest_path, opportunity_manifest)
    day_manifest = {
        "schema_version": FORMAL_DAY_SCHEMA_VERSION,
        "target_day": "2026-04-17",
        "feature_block": "R0",
        "panel_role": "prefix40_development",
        "strict_native_queue": {"missing_trace_unbounded": True},
        "source_contract": {
            "canonical_identity_sha256": source_contract_sha256,
        },
        "assignment_snapshots": {
            "path": str(snapshots_path),
            "sha256": _sha256(snapshots_path),
        },
        "one_shot_label_manifests": [
            {
                "path": str(opportunity_manifest_path),
                "size_bytes": opportunity_manifest_path.stat().st_size,
                "sha256": _sha256(opportunity_manifest_path),
            }
        ],
    }
    day_manifest_path = tmp_path / "day-manifest.json"
    _write_json(day_manifest_path, day_manifest)
    return day_manifest_path


def test_label_panel_admits_exact_eight_arm_complete_opportunity(
    tmp_path: Path,
) -> None:
    manifest = assemble_day_label_panel(
        _fixture(tmp_path, censored_arm=None),
        destination=tmp_path / "admitted",
    )

    assert manifest["opportunities"]["rows"] == 1
    assert manifest["schema_version"] == PANEL_IDENTITY
    assert manifest["opportunities"]["joint_strict_rows"] == 1
    assert manifest["labels"]["rows"] == 8
    assert manifest["labels"]["strict_rows"] == 8
    assert manifest["complete_case_filter_applied"] is False


def test_label_panel_retains_censored_arm_and_marks_joint_ineligible(
    tmp_path: Path,
) -> None:
    manifest = assemble_day_label_panel(
        _fixture(tmp_path, censored_arm="FIXED_2048S"),
        destination=tmp_path / "admitted",
    )

    assert manifest["opportunities"]["joint_strict_rows"] == 0
    assert manifest["labels"]["strict_rows"] == 7
    assert manifest["labels"]["right_censored_rows"] == 1
    labels = pd.read_parquet(manifest["labels"]["path"])
    censored = labels.loc[labels["duration_policy_id"] == "FIXED_2048S"].iloc[0]
    assert bool(censored["right_censored"])
    assert not bool(censored["strict_native_label"])
    assert pd.isna(censored["terminal_value_usdc"])


def test_label_panel_retains_queue_missing_arm_without_point_label(
    tmp_path: Path,
) -> None:
    manifest = assemble_day_label_panel(
        _fixture(
            tmp_path,
            censored_arm=None,
            queue_missing_arm="FIXED_2048S",
        ),
        destination=tmp_path / "admitted",
    )

    assert manifest["opportunities"]["joint_strict_rows"] == 0
    assert manifest["opportunities"]["unsupported_opportunity_rows"] == 1
    assert manifest["labels"]["rows"] == 8
    assert manifest["labels"]["strict_rows"] == 7
    assert manifest["labels"]["unsupported_rows"] == 1
    labels = pd.read_parquet(manifest["labels"]["path"])
    missing = labels.loc[labels["duration_policy_id"] == "FIXED_2048S"].iloc[0]
    assert not bool(missing["strict_native_label"])
    assert pd.isna(missing["terminal_value_usdc"])
    assert missing["treatment_exchange_book_queue_missing_count"] == 1
    assert "outside_snapshot_range" in missing[
        "treatment_exchange_book_queue_missing_trace_json"
    ]


def test_label_panel_rejects_queue_missing_trace_count_drift(
    tmp_path: Path,
) -> None:
    day_manifest_path = _fixture(
        tmp_path,
        censored_arm=None,
        queue_missing_arm="FIXED_2048S",
    )
    day_manifest = json.loads(day_manifest_path.read_text(encoding="utf-8"))
    opportunity_path = Path(day_manifest["one_shot_label_manifests"][0]["path"])
    opportunity = json.loads(opportunity_path.read_text(encoding="utf-8"))
    arm_row = next(row for row in opportunity["arms"] if row["arm_id"] == "FIXED_2048S")
    arm_path = opportunity_path.parent / arm_row["path"]
    arm = json.loads(arm_path.read_text(encoding="utf-8"))
    arm["strict_execution_contract"]["exchange_book_queue_missing_trace"] = []
    arm.pop("canonical_result_sha256")
    arm["canonical_result_sha256"] = _canonical_sha256(arm)
    _write_json(arm_path, arm)
    arm_row["sha256"] = _sha256(arm_path)
    _write_json(opportunity_path, opportunity)
    _rewrite_day_bound_file(
        day_manifest_path,
        section="one_shot_label_manifests",
        path=opportunity_path,
    )

    with pytest.raises(LabelPanelError, match="trace count drifted"):
        assemble_day_label_panel(
            day_manifest_path,
            destination=tmp_path / "admitted",
        )


def _rewrite_day_bound_file(
    day_manifest_path: Path,
    *,
    section: str,
    path: Path,
) -> None:
    day_manifest = json.loads(day_manifest_path.read_text(encoding="utf-8"))
    if section == "assignment_snapshots":
        day_manifest[section]["sha256"] = _sha256(path)
    else:
        day_manifest[section][0]["sha256"] = _sha256(path)
    _write_json(day_manifest_path, day_manifest)


def test_label_panel_recomputes_opportunity_contract_identity(
    tmp_path: Path,
) -> None:
    day_manifest_path = _fixture(tmp_path, censored_arm=None)
    day_manifest = json.loads(day_manifest_path.read_text(encoding="utf-8"))
    manifest_path = Path(day_manifest["one_shot_label_manifests"][0]["path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["opportunity_contract"]["opportunity"]["role_at_fill"] = "add"
    _write_json(manifest_path, manifest)
    _rewrite_day_bound_file(
        day_manifest_path,
        section="one_shot_label_manifests",
        path=manifest_path,
    )

    with pytest.raises(
        LabelPanelError,
        match="opportunity contract canonical identity drifted",
    ):
        assemble_day_label_panel(
            day_manifest_path,
            destination=tmp_path / "admitted",
        )


def test_label_panel_cross_checks_snapshot_source_bundle(
    tmp_path: Path,
) -> None:
    day_manifest_path = _fixture(tmp_path, censored_arm=None)
    day_manifest = json.loads(day_manifest_path.read_text(encoding="utf-8"))
    manifest_path = Path(day_manifest["one_shot_label_manifests"][0]["path"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    contract = manifest["opportunity_contract"]
    contract["opportunity"]["cooldown_v2_source_bundle_sha256"] = "e" * 64
    manifest["opportunity_identity_sha256"] = _canonical_sha256(contract)
    _write_json(manifest_path, manifest)
    _rewrite_day_bound_file(
        day_manifest_path,
        section="one_shot_label_manifests",
        path=manifest_path,
    )

    with pytest.raises(
        LabelPanelError,
        match="opportunity/snapshot source bundle drifted",
    ):
        assemble_day_label_panel(
            day_manifest_path,
            destination=tmp_path / "admitted",
        )


def test_label_panel_rejects_snapshot_payload_hash_drift(
    tmp_path: Path,
) -> None:
    day_manifest_path = _fixture(tmp_path, censored_arm=None)
    day_manifest = json.loads(day_manifest_path.read_text(encoding="utf-8"))
    snapshots_path = Path(day_manifest["assignment_snapshots"]["path"])
    snapshots = pd.read_parquet(snapshots_path)
    snapshots.loc[0, "snapshot_payload_sha256"] = "f" * 64
    snapshots.to_parquet(snapshots_path, index=False)
    _rewrite_day_bound_file(
        day_manifest_path,
        section="assignment_snapshots",
        path=snapshots_path,
    )

    with pytest.raises(LabelPanelError, match="snapshot payload SHA256 drifted"):
        assemble_day_label_panel(
            day_manifest_path,
            destination=tmp_path / "admitted",
        )


def test_label_panel_prevents_joined_state_identity_override(
    tmp_path: Path,
) -> None:
    day_manifest_path = _fixture(tmp_path, censored_arm=None)
    day_manifest = json.loads(day_manifest_path.read_text(encoding="utf-8"))
    snapshots_path = Path(day_manifest["assignment_snapshots"]["path"])
    snapshots = pd.read_parquet(snapshots_path)
    payload = json.loads(str(snapshots.loc[0, "snapshot_payload_json"]))
    m0 = dict(payload["m0_context"])
    m0["side"] = "SELL"
    payload["m0_context"] = m0
    snapshots.loc[0, "m0_context_json"] = json.dumps(m0)
    snapshots.loc[0, "snapshot_payload_json"] = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
    )
    snapshots.loc[0, "snapshot_payload_sha256"] = _canonical_sha256(payload)
    snapshots.to_parquet(snapshots_path, index=False)
    _rewrite_day_bound_file(
        day_manifest_path,
        section="assignment_snapshots",
        path=snapshots_path,
    )

    with pytest.raises(
        LabelPanelError,
        match="snapshot/feature identity disagrees on 'side'",
    ):
        assemble_day_label_panel(
            day_manifest_path,
            destination=tmp_path / "admitted",
        )
