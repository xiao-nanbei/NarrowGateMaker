from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_study as study,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_nested_oof import (
    AndClause,
    BooleanCooldownPolicy,
    BooleanRule,
    SearchConfig,
    TriLiteral,
    duration_vocabulary,
)
from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    fit_predicate_artifact,
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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, sort_keys=True, allow_nan=False),
        encoding="ascii",
    )


def _reseal_admission(output: Path, *, report_changed: bool) -> None:
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    if report_changed:
        manifest["artifacts"]["report"]["sha256"] = _sha256(output / "report.json")
    _write_json(manifest_path, manifest)
    manifest_sha256 = _sha256(manifest_path)
    (output / "manifest.sha256").write_text(
        f"{manifest_sha256}  manifest.json\n", encoding="ascii"
    )
    _write_json(output / "_SUCCESS", {"manifest_sha256": manifest_sha256})


def _m0_row(*, day: str, side: str, role: str, ordinal: int) -> dict[str, object]:
    fill_visible_ts_ns = (1_100 + ordinal) * 1_000_000
    return {
        "utc_day": day,
        "assignment_ts_ns": fill_visible_ts_ns,
        "fill_visible_ts_ns": fill_visible_ts_ns,
        "side": side,
        "role_at_fill": role,
        "inventory_before_fill_btc": 0.0 if role == "opener" else 0.001,
        "inventory_after_fill_btc": 0.001 if role == "opener" else 0.002,
        "fill_qty_btc": 0.001,
        "order_qty_btc": 0.001,
        "cumulative_filled_qty_before_btc": 0.0,
        "cumulative_filled_qty_after_btc": 0.001,
        "remaining_order_qty_after_btc": 0.0,
        "partial_fill_ordinal": 1,
        "queue_state_before_fill": "exact",
        "queue_ahead_before_fill_btc": 0.002,
        "target_price_displayed_qty_status": "exact",
        "target_price_displayed_qty_btc": 0.003,
        "target_price_displayed_qty_known": True,
        "target_price_displayed_qty_is_queue_ahead": False,
        "target_price_tick": 100_000 + ordinal,
        "fill_is_partial": role == "add",
        "order_age_s": 1.0 + ordinal,
        "cooldown_blocker_active": False,
        "consecutive_units_after": float(ordinal + 1),
        "baseline_duration_ms": float(85_000 * (ordinal + 1)),
        "campaign_age_s": float(ordinal * 10),
        "campaign_add_count": 0 if role == "opener" else 1,
        "campaign_mae_to_date_usdc": 0.01 * ordinal,
        "campaign_inventory_time_to_date_btc_s": 0.1 * ordinal,
        "last_same_side_fill_age_s": None if role == "opener" else 5.0,
        "last_opposite_side_fill_age_s": None,
        "cooldown_remaining_ms": 0.0,
        "cooldown_lineage_revision_before": ordinal,
        "cooldown_deadline_owner": "none",
    }


def _feature_row(*, day: str, side: str, role: str, ordinal: int) -> dict[str, object]:
    return {
        **_m0_row(day=day, side=side, role=role, ordinal=ordinal),
        "tri::mid_usdc_per_btc__h1s__h2s::positive_ordering": 1,
        "value::mid_usdc_per_btc::ema::h1s": 100.0 + ordinal,
        "tri::signed_flow_imbalance__h1s__h2s::positive_ordering": 1,
        "value::signed_flow_imbalance::ema::h1s": 1.0 + ordinal,
    }


_SOURCE_BUNDLE_SHA256 = "b" * 64
_SOURCE_CONTRACT_SHA256 = "d" * 64
_IDENTITY_HASHES = {
    "baseline_identity_sha256": "1" * 64,
    "config_sha256": "2" * 64,
    "code_sha256": "3" * 64,
    "model_sha256": "4" * 64,
    "p3_sha256": "5" * 64,
    "feature_dag_sha256": "6" * 64,
    "execution_abi_sha256": "7" * 64,
}


def _snapshot_record(
    *,
    snapshot_id: str,
    assignment_id: str,
    fill_event_id: str,
    lineage_id: str,
    order_id: int,
    m0: dict[str, object],
    feature: dict[str, object],
) -> dict[str, object]:
    fill_visible_ts_ns = int(m0["fill_visible_ts_ns"])
    payload = {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "identity": IDENTITY,
        "snapshot_id": snapshot_id,
        "assignment_id": assignment_id,
        "fill_event_id": fill_event_id,
        "client_order_id": f"replay-order-{order_id}",
        "lineage_id": lineage_id,
        "lineage_revision": 1,
        "partial_fill_ordinal": int(m0["partial_fill_ordinal"]),
        "partial_fill_qty_btc": float(m0["fill_qty_btc"]),
        "visibility_profile": "historical_exchange_event_visibility_exploratory",
        "receive_time_transport_eligible": False,
        "clocks": {
            "assignment": {"ts_ns": fill_visible_ts_ns},
            "fill_exchange": {"ts_ns": fill_visible_ts_ns - 1_000_000},
            "fill_receive": {"ts_ns": None},
            "fill_visible": {"ts_ns": fill_visible_ts_ns},
            "feature_ready": {"ts_ns": fill_visible_ts_ns},
        },
        "sources": {"market": {"generation": 1}},
        "source_bundle_sha256": _SOURCE_BUNDLE_SHA256,
        "identity_hashes": _IDENTITY_HASHES,
        "m0_context": m0,
        "feature_block": "M2",
        "feature_row": feature,
        "field_validity": {"m0.side": {"valid": True}},
        "policy_input_valid": True,
        "fallback_policy_id": None,
        "fallback_reason": None,
        "economic_outcomes_read": False,
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return {
        "snapshot_id": snapshot_id,
        "assignment_id": assignment_id,
        "fill_event_id": fill_event_id,
        "client_order_id": f"replay-order-{order_id}",
        "lineage_id": lineage_id,
        "lineage_revision": 1,
        "partial_fill_ordinal": int(m0["partial_fill_ordinal"]),
        "partial_fill_qty_btc": float(m0["fill_qty_btc"]),
        "visibility_profile": payload["visibility_profile"],
        "receive_time_transport_eligible": False,
        "source_bundle_sha256": _SOURCE_BUNDLE_SHA256,
        "feature_block": "M2",
        "m0_context_json": json.dumps(m0),
        "feature_row_json": json.dumps(feature),
        "snapshot_payload_json": payload_json,
        "snapshot_payload_sha256": _canonical_sha256(payload),
        "policy_input_valid": True,
        "fallback_policy_id": None,
        "fallback_reason": None,
        "economic_outcomes_read": False,
    }


def _arm_payload(
    *,
    opportunity_id: str,
    side: str,
    arm: str,
    campaign_id: int,
    right_censored: bool,
    assignment_ts_ms: int,
    baseline_duration_ms: float,
) -> dict[str, object]:
    vocabulary = duration_vocabulary(side)
    action_index = vocabulary.index(arm)
    terminal_value = 0.0 if action_index == 0 else (1.0 if action_index == 1 else -1.0)
    trace = {
        "schema_version": "multiscale_ema_boolean_cooldown_duration_fork_trace.v2",
        "action": "CONTROL_85N" if arm == "CONTROL_85N" else "FIXED_DURATION_MS",
        "side": side,
        "campaign_id": campaign_id,
        "assignment_ts_ms": assignment_ts_ms,
        "baseline_duration_ms": baseline_duration_ms,
        "applied_duration_ms": 85_000.0,
        "arm_washout_complete": not right_censored,
        "terminal_ts_ms": 2_000,
        "terminal_reason": ("data_boundary_right_censored" if right_censored else "flat"),
        "right_censored": right_censored,
        "assignment_to_washout_value_usdc": (None if right_censored else terminal_value),
        "censor_time_mid_mark_usdc": 0.2 if right_censored else None,
        "censor_time_executable_mark_usdc": 0.1 if right_censored else None,
        "censor_marks_are_terminal_bounds": False,
        "accounting_residual_usdc": 0.0,
        "second_assignment_count": 0,
        "reducing_quote_change_count": 0,
        "inventory_time_btc_s": 1.0,
        "mae_usdc": 0.1,
        "max_abs_inventory_btc": 0.002,
    }
    payload: dict[str, object] = {
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
            "exchange_book_queue_missing_count": 0,
            "exchange_book_queue_missing_trace": [],
            "strict_native_label_eligible": True,
            "strict_native_label_unsupported_reasons": [],
            "economic_point_label_status": "eligible",
        },
    }
    payload["canonical_result_sha256"] = _canonical_sha256(payload)
    return payload


def _day_source(
    root: Path,
    *,
    day: str,
    day_index: int,
    panel_role: str,
) -> tuple[Path, list[dict[str, object]]]:
    day_root = root / day
    snapshots: list[dict[str, object]] = []
    opportunity_rows: list[dict[str, object]] = []
    feature_rows: list[dict[str, object]] = []
    campaign_id = day_index * 100
    for side_index, side in enumerate(("BUY", "SELL")):
        for role_index, role in enumerate(("opener", "add")):
            ordinal = day_index + role_index + side_index
            snapshot_id = (
                "cooldown-v2-" + hashlib.sha256(f"{day}:{side}:{role}".encode("ascii")).hexdigest()
            )
            m0 = _m0_row(day=day, side=side, role=role, ordinal=ordinal)
            feature = _feature_row(day=day, side=side, role=role, ordinal=ordinal)
            order_id = day_index * 100 + side_index * 10 + role_index
            snapshots.append(
                _snapshot_record(
                    snapshot_id=snapshot_id,
                    assignment_id=f"assignment-{day}-{side}-{role}",
                    fill_event_id=f"fill-{day}-{side}-{role}",
                    lineage_id=f"lineage-{day}-{side}-{role}",
                    order_id=order_id,
                    m0=m0,
                    feature=feature,
                )
            )
            feature_rows.append(feature)
            opportunity_contract = {
                "schema_version": PREFIX_SCHEMA_VERSION,
                "identity": IDENTITY,
                "target_day": day,
                "source_contract_sha256": _SOURCE_CONTRACT_SHA256,
                "execution_identity_hashes": _IDENTITY_HASHES,
                "opportunity": {
                    "exposure_fill_ordinal": ordinal + 1,
                    "partial_fill_ordinal": 1,
                    "fill_visible_ts_ms": int(m0["fill_visible_ts_ns"]) // 1_000_000,
                    "fill_exchange_ts_ms": (
                        int(m0["fill_visible_ts_ns"]) // 1_000_000 - 1
                    ),
                    "side": side,
                    "role_at_fill": role,
                    "order_id": order_id,
                    "campaign_id": campaign_id,
                    "fill_qty_btc": float(m0["fill_qty_btc"]),
                    "baseline_duration_ms": float(m0["baseline_duration_ms"]),
                    "cooldown_v2_snapshot_id": snapshot_id,
                    "cooldown_v2_source_bundle_sha256": _SOURCE_BUNDLE_SHA256,
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
            opportunity_root = day_root / "labels" / opportunity_id
            opportunity_root.mkdir(parents=True)
            arm_rows = []
            for arm in duration_vocabulary(side):
                right_censored = bool(
                    day_index == 1
                    and side == "BUY"
                    and role == "add"
                    and arm == duration_vocabulary(side)[-1]
                )
                arm_path = opportunity_root / f"arm-{arm}.json"
                _write_json(
                    arm_path,
                    _arm_payload(
                        opportunity_id=opportunity_id,
                        side=side,
                        arm=arm,
                        campaign_id=campaign_id,
                        right_censored=right_censored,
                        assignment_ts_ms=(
                            int(m0["fill_visible_ts_ns"]) // 1_000_000
                        ),
                        baseline_duration_ms=float(m0["baseline_duration_ms"]),
                    ),
                )
                arm_rows.append({"arm_id": arm, "path": arm_path.name, "sha256": _sha256(arm_path)})
            opportunity_manifest = {
                "schema_version": OPPORTUNITY_MANIFEST_SCHEMA_VERSION,
                "identity": IDENTITY,
                "opportunity_identity_sha256": opportunity_id,
                "opportunity_contract": opportunity_contract,
                "arms": arm_rows,
            }
            opportunity_path = opportunity_root / "manifest.json"
            _write_json(opportunity_path, opportunity_manifest)
            opportunity_rows.append(
                {
                    "path": str(opportunity_path),
                    "size_bytes": opportunity_path.stat().st_size,
                    "sha256": _sha256(opportunity_path),
                }
            )
            campaign_id += 1

    if day_index == 0:
        m0 = _m0_row(day=day, side="BUY", role="add", ordinal=99)
        feature = _feature_row(day=day, side="BUY", role="add", ordinal=99)
        snapshots.append(
            _snapshot_record(
                snapshot_id="cooldown-v2-" + "f" * 64,
                assignment_id="unlabeled-assignment",
                fill_event_id="unlabeled-fill",
                lineage_id="unlabeled-lineage",
                order_id=999,
                m0=m0,
                feature=feature,
            )
        )
        feature_rows.append(feature)

    snapshots_path = day_root / "assignment_snapshots.parquet"
    pd.DataFrame(snapshots).to_parquet(snapshots_path, index=False)
    day_manifest = {
        "schema_version": (
            f"{IDENTITY}.strict_native_one_shot_labels.v1.day.v2"
        ),
        "target_day": day,
        "feature_block": "M2",
        "panel_role": panel_role,
        "strict_native_queue": {"missing_trace_unbounded": True},
        "source_contract": {
            "canonical_identity_sha256": _SOURCE_CONTRACT_SHA256,
        },
        "assignment_snapshots": {
            "path": str(snapshots_path),
            "sha256": _sha256(snapshots_path),
        },
        "one_shot_label_manifests": opportunity_rows,
    }
    day_manifest_path = day_root / "day-manifest.json"
    _write_json(day_manifest_path, day_manifest)
    return day_manifest_path, feature_rows


def _write_artifact(path: Path, artifact: object) -> dict[str, str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(artifact.to_json(), encoding="ascii")  # type: ignore[attr-defined]
    return {"path": str(path), "sha256": _sha256(path)}


def _market_reference(*, side: str, channel: str) -> pd.DataFrame:
    rows = []
    for index, day in enumerate(("2025-08-01", "2025-08-02")):
        context = {
            "role_at_fill": "opener" if index == 0 else "add",
            "queue_state_before_fill": "exact",
            "target_price_displayed_qty_status": "exact",
            "target_price_displayed_qty_known": True,
            "fill_is_partial": False,
            "cooldown_blocker_active": False,
            "cooldown_deadline_owner": "none",
        }
        if channel == "book":
            row = {
                "utc_day": day,
                "side": side,
                **context,
                "tri::mid_usdc_per_btc__h1s__h2s::positive_ordering": 1,
                "value::mid_usdc_per_btc::ema::h1s": 90.0 + index,
            }
        else:
            row = {
                "utc_day": day,
                "side": side,
                **context,
                "tri::signed_flow_imbalance__h1s__h2s::positive_ordering": 1,
                "value::signed_flow_imbalance::ema::h1s": 0.5 + index,
            }
        rows.append(row)
    return pd.DataFrame(rows)


def _fixture(tmp_path: Path) -> tuple[Path, Path, study.StudyConfig]:
    days = tuple(f"2026-01-{value:02d}" for value in range(1, 9))
    prefix_days = days[:6]
    added_days = days[6:]
    day_rows = []
    for index, day in enumerate(days):
        role = "prefix40_development" if day in prefix_days else "added10_late_diagnostic"
        day_path, _ = _day_source(
            tmp_path / "strict",
            day=day,
            day_index=index,
            panel_role=role,
        )
        day_rows.append(
            {
                "day": day,
                "manifest_path": str(day_path),
                "manifest_size_bytes": day_path.stat().st_size,
                "manifest_sha256": _sha256(day_path),
            }
        )
    formal = {
        "schema_version": study.FORMAL_PANEL_SCHEMA,
        "identity": study.RUNNER_IDENTITY,
        "formal_full_support_run": False,
        "run_kind": "engineering_test_fixture",
        "ordered_days": list(days),
        "day_count": len(days),
        "prefix40_full_support_count": len(prefix_days),
        "added10_full_support_count": len(added_days),
        "day_manifests": day_rows,
    }
    formal_path = tmp_path / "formal-panel.json"
    _write_json(formal_path, formal)

    config = study.StudyConfig(
        outer_folds=2,
        outer_minimum_train_days=3,
        search=SearchConfig(
            max_literals_per_clause=1,
            max_clauses_per_rule=1,
            max_rules_per_policy=1,
            max_clause_candidates=8,
            max_rule_candidates=8,
            max_policy_candidates=14,
            inner_folds=1,
            inner_minimum_train_days=2,
            minimum_action_opportunities=1,
            minimum_action_campaigns=1,
            minimum_action_days=1,
        ),
        minimum_deployment_campaigns=1,
        minimum_deployment_days=1,
        engineering_allow_nonformal_panel=True,
    )
    artifact_root = tmp_path / "artifacts"
    book: dict[str, dict[str, str]] = {}
    trade: dict[str, dict[str, str]] = {}
    for side in ("BUY", "SELL"):
        for channel, destination in (("book", book), ("trade", trade)):
            reference = _market_reference(side=side, channel=channel)
            artifact = fit_predicate_artifact(
                reference,
                side=side,
                source_role="outcome_blind_2025_single_channel",
                reference_identity_sha256=("a" if channel == "book" else "b") * 64,
                reference_days=("2025-08-01", "2025-08-02"),
                source_clock_identity=(
                    "tardis_provider_local_receive_clock_v1"
                    if channel == "book"
                    else "binance_exchange_aggtrade_clock_v1"
                ),
            )
            destination[side] = _write_artifact(artifact_root / f"{channel}-{side}.json", artifact)

    bundle = {
        "schema_version": study.PREDICATE_BUNDLE_SCHEMA,
        "identity": IDENTITY,
        "book": book,
        "trade": trade,
        "m0_artifacts": [],
        "cross_clock_clause_authorized": False,
        "cross_clock_clause_scope": "2025_reference_rows_only",
        "strict_2026_target_snapshot": {
            "book_trade_predicates_may_be_combined_by_study": True,
        },
    }
    bundle["canonical_sha256"] = _canonical_sha256(bundle)
    bundle_path = tmp_path / "predicate-bundle.json"
    _write_json(bundle_path, bundle)
    return formal_path, bundle_path, config


def test_formal_day_denominators_are_partitioned_without_reduced_support_pooling() -> None:
    sources = tuple(
        study.DaySource(
            day=(pd.Timestamp("2026-01-01") + pd.Timedelta(days=index)).strftime("%Y-%m-%d"),
            panel_role="prefix40",
            manifest_path=Path(f"/prefix-{index}.json"),
            manifest_sha256="a" * 64,
        )
        for index in range(33)
    ) + tuple(
        study.DaySource(
            day=f"2026-02-{index + 1:02d}",
            panel_role="added10",
            manifest_path=Path(f"/added-{index}.json"),
            manifest_sha256="b" * 64,
        )
        for index in range(8)
    )

    report = study._day_denominator_report(  # noqa: SLF001
        sources=sources,
        formal_full_support_run=True,
    )

    assert report["formal_full_support_contract_satisfied"] is True
    assert report["nominal_mechanics_denominator"]["pooled_days"] == 50
    assert report["exact_label_economic_denominator"]["pooled_days"] == 41
    assert report["reduced_support_diagnostic_denominator"]["pooled_days"] == 9
    assert (
        report["reduced_support_diagnostic_denominator"][
            "pooled_into_exact_label_economics"
        ]
        is False
    )
    bindings = report["scope_bindings"]
    assert bindings["prefix_exact_label_economic"]["exact_label_economic"][
        "observed_manifest_day_count"
    ] == 33
    assert bindings["added_exact_label_economic"]["exact_label_economic"][
        "observed_manifest_day_count"
    ] == 8
    assert bindings["pooled_exact_label_economic"]["exact_label_economic"][
        "observed_manifest_day_count"
    ] == 41


def test_multiday_study_runs_real_admission_nested_oof_and_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    formal, bundle, config = _fixture(tmp_path)
    output = tmp_path / "admitted-study"

    preflight = study.preflight_study(
        formal_panel_manifest=formal,
        predicate_bundle=bundle,
        config=config,
    )
    assert preflight["economic_outcomes_read"] is False
    assert preflight["formal_panel_manifest"]["observed_day_count"] == 8

    manifest = study.run_study(
        formal_panel_manifest=formal,
        predicate_bundle=bundle,
        output=output,
        config=config,
    )
    assert manifest["exact_label_manifest_day_count"] == 8
    assert manifest["nominal_mechanics_day_count"] == 50
    assert manifest["formal_exact_label_day_count"] == 41
    assert manifest["reduced_support_diagnostic_day_count"] == 9
    assert manifest["economic_statistics_denominator"] == "exact_label_economic_only"
    assert manifest["reduced_support_pooled_into_economic_statistics"] is False
    assert manifest["reduced_support_economic_labels_manufactured"] is False
    assert "formal_panel_is_exactly_50_days" not in manifest
    assert manifest["outer_fold_support_audited"] is True
    assert manifest["combined_action_support_audited"] is True
    assert manifest["permissions"]["validation_read"] is False
    assert manifest["permissions"]["sealed_holdout_read"] is False
    assert manifest["permissions"]["action_authorized"] is False
    assert manifest["permissions"]["live_authorized"] is False
    assert manifest["continuous_state_comparator_completed"] is True
    assert manifest["continuous_state_comparator_may_replace_boolean_policy"] is False

    report = json.loads((output / "report.json").read_text(encoding="ascii"))
    day_denominators = report["day_denominators"]
    assert day_denominators["nominal_mechanics_denominator"] == {
        "added_days": 10,
        "economic_statistics_bound": False,
        "pooled_days": 50,
        "prefix_days": 40,
    }
    assert day_denominators["exact_label_economic_denominator"] == {
        "added_days": 8,
        "economic_statistics_bound": True,
        "observed_manifest_days": 8,
        "only_source_for_reported_economic_statistics": True,
        "pooled_days": 41,
        "prefix_days": 33,
    }
    assert day_denominators["reduced_support_diagnostic_denominator"] == {
        "added_days": 2,
        "economic_labels_manufactured": False,
        "economic_statistics_bound": False,
        "pooled_days": 9,
        "pooled_into_exact_label_economics": False,
        "prefix_days": 7,
    }
    assert day_denominators["formal_full_support_contract_satisfied"] is False
    assert report["formal_panel"]["exact_label_manifest_day_count"] == 8
    assert report["formal_panel"]["contains_reduced_support_days"] is False
    assert report["legacy_panel_scopes_field_emitted"] is False
    assert "panel_scopes" not in report
    assert report["continuous_state_comparator"]["status"] == "completed"
    assert (
        report["continuous_state_comparator"]["model_family"]
        == "raw_state_multioutput_regression_tree_diagnostic"
    )
    assert report["continuous_state_comparator"]["may_replace_boolean_policy"] is False
    denominator = report["denominator_audit"]
    assert denominator["status_counts"]["unlabeled"] == 1
    assert denominator["status_counts"]["right_censored_or_incomplete"] == 1
    assert denominator["complete_case_filter_applied"] is False
    assert denominator["denominator_rows_retained"] is True
    assert denominator["point_label_common_support_filter_applied"] is True
    assert denominator["unsupported_opportunity_sensitivity_implemented"] is False
    assert denominator["partial_identification_unresolved"] is True
    assert (
        denominator["research_supported_promotion_blocked_by_partial_identification"]
        is True
    )
    results = report["exact_label_economic_results"]
    assert results["added_exact_label_economic"]["sides"]["BUY"]["status"] == ("not_run")
    assert (
        results["pooled_exact_label_economic"]["sides"]["BUY"]["feature_blocks"]["R0"][
            "exact_label_economic_calendar_day_count"
        ]
        == 8
    )
    pooled_r0 = results["pooled_exact_label_economic"]["sides"]["BUY"][
        "feature_blocks"
    ]["R0"]
    assert pooled_r0["economic_scope_identity"] == "pooled_exact_label_economic"
    assert pooled_r0["denominator_binding"]["nominal_mechanics"]["day_count"] == 50
    assert pooled_r0["denominator_binding"]["exact_label_economic"]["formal_day_count"] == 41
    assert (
        pooled_r0["denominator_binding"]["reduced_support_diagnostic"][
            "pooled_into_exact_label_economics"
        ]
        is False
    )
    assert "nominal_pooled50_is_exactly_50_days" not in pooled_r0
    assert pooled_r0["final_candidate_eligible"] is False
    assert (
        "r0_reproduction_not_final_candidate_eligible"
        in pooled_r0["deployment_gate_after_outer_oof"]["reasons"]
    )
    assert (
        pooled_r0["statistical_deployment_gate_after_outer_oof"]["action_authorized"]
        is False
    )
    assert pooled_r0["deployment_gate_after_outer_oof"]["action_authorized"] is False

    prefix = results["prefix_exact_label_economic"]
    buy = prefix["sides"]["BUY"]["feature_blocks"]["M2"]
    sell = prefix["sides"]["SELL"]["feature_blocks"]["M2"]
    assert buy["action_rate"] > 0.0
    assert sell["action_rate"] > 0.0
    assert "partial_identification_unresolved" in buy[
        "deployment_gate_after_outer_oof"
    ]["reasons"]
    assert buy["deployment_gate_after_outer_oof"]["decision"] == "abstain"
    assert buy["candidate_ids"] != sell["candidate_ids"]
    assert all(not fold["candidate_replaced_by_baseline_before_outer_oof"] for fold in buy["folds"])
    assert all("outer_support" in fold for fold in buy["folds"])
    assert buy["combined_action_support"]["action_opportunities"] > 0
    assert (
        buy["statistical_deployment_gate_after_outer_oof"]["combined_support"]
        == buy["combined_action_support"]
    )
    assert buy["opener_add_support"]["opener"]["action_opportunities"] > 0
    assert buy["opener_add_support"]["add"]["action_opportunities"] > 0
    continuous_m2 = prefix["sides"]["BUY"][
        "continuous_state_comparator_feature_blocks"
    ]["M2"]
    for role in ("opener", "add"):
        role_audit = continuous_m2["opener_add_support"][role]
        assert "action_campaigns" in role_audit
        assert "action_days" in role_audit
        assert isinstance(role_audit["campaign_day_clustered_uplift_interval"], dict)
        assert isinstance(role_audit["tail_diagnostics"], dict)
    family = prefix["sides"]["BUY"]["feature_family_selection"]
    assert family["comparison_count"] == 2
    assert set(family["comparisons"]) == {"M1_minus_M0", "M2_minus_M1"}
    assert family["selection_contract"].startswith("hierarchical_M0")
    assert family["continuous_state_comparator_status"] == "completed"
    assert family["continuous_state_comparator_may_replace_boolean_policy"] is False
    assert family["unified_policy_freeze_eligible"] is False
    assert "continuous_state_comparator_not_run" not in family[
        "unified_policy_freeze_blockers"
    ]
    assert family["action_authorized"] is False
    assert family["live_authorized"] is False

    oof = pd.read_parquet(output / "outer_oof_rows.parquet")
    assert set(oof["side"]) == {"BUY", "SELL"}
    assert oof["evaluation_stage"].eq("outer_oof").all()
    assert oof["economic_denominator_identity"].eq("exact_label_economic").all()
    assert oof["economic_scope_identity"].str.endswith("_exact_label_economic").all()
    assert oof["panel_scope_is_deprecated_nominal_alias"].eq(True).all()  # noqa: E712
    assert oof["selected_nonbaseline"].any()
    continuous_oof = pd.read_parquet(output / "continuous_outer_oof_rows.parquet")
    assert continuous_oof["evaluation_stage"].eq("continuous_outer_oof").all()
    assert continuous_oof["model_family"].eq(
        "raw_state_multioutput_regression_tree_diagnostic"
    ).all()
    pairing = [
        "economic_scope_identity",
        "side",
        "feature_block",
        "opportunity_id",
    ]
    assert set(map(tuple, continuous_oof[pairing].to_numpy())) == set(
        map(tuple, oof[pairing].to_numpy())
    )
    assert study.validate_study_output(output) == manifest
    shutil.rmtree(output.parent / f".{output.name}.work")
    assert study.validate_study_output(output) == manifest

    def _must_not_reassemble(*args: object, **kwargs: object) -> object:
        raise AssertionError("resume should validate the admitted output")

    monkeypatch.setattr(study, "assemble_day_label_panel", _must_not_reassemble)
    assert (
        study.run_study(
            formal_panel_manifest=formal,
            predicate_bundle=bundle,
            output=output,
            config=config,
        )
        == manifest
    )

    drifted = study.StudyConfig(
        outer_folds=config.outer_folds,
        outer_minimum_train_days=config.outer_minimum_train_days,
        search=config.search,
        economic_epsilon_usdc=0.001,
        minimum_deployment_campaigns=1,
        minimum_deployment_days=1,
        engineering_allow_nonformal_panel=True,
    )
    with pytest.raises(study.CooldownStudyError, match="binding differs"):
        study.run_study(
            formal_panel_manifest=formal,
            predicate_bundle=bundle,
            output=output,
            config=drifted,
        )


def test_strict_target_view_allows_book_and_trade_in_same_clause() -> None:
    policy = BooleanCooldownPolicy(
        side="BUY",
        rules=(
            BooleanRule(
                action=duration_vocabulary("BUY")[1],
                clauses=(
                    AndClause(
                        (
                            TriLiteral("book_predicate"),
                            TriLiteral("trade_predicate"),
                        )
                    ),
                ),
            ),
        ),
    )
    assert study._policy_respects_clock_groups(  # noqa: SLF001
        policy,
        {"book_predicate": "book", "trade_predicate": "trade"},
    )


def test_candidate_universe_can_express_cross_channel_and() -> None:
    config = SearchConfig(
        max_literals_per_clause=2,
        max_clauses_per_rule=1,
        max_rules_per_policy=1,
        max_clause_candidates=8,
        max_rule_candidates=8,
        max_policy_candidates=14,
        minimum_action_opportunities=1,
        minimum_action_campaigns=1,
        minimum_action_days=1,
    )
    candidates = study._bounded_clock_safe_candidates(  # noqa: SLF001
        side="BUY",
        predicate_columns=("book_predicate", "trade_predicate"),
        predicate_groups={"book_predicate": "book", "trade_predicate": "trade"},
        config=config,
    )
    assert any(
        {literal.predicate for literal in clause.literals}
        == {"book_predicate", "trade_predicate"}
        for policy in candidates
        for rule in policy.rules
        for clause in rule.clauses
    )

    with pytest.raises(study.CooldownStudyError, match="mapping is incomplete"):
        study._bounded_clock_safe_candidates(  # noqa: SLF001
            side="BUY",
            predicate_columns=("book_predicate", "trade_predicate"),
            predicate_groups={"book_predicate": "book"},
            config=config,
        )
    with pytest.raises(study.CooldownStudyError, match="invalid values"):
        study._bounded_clock_safe_candidates(  # noqa: SLF001
            side="BUY",
            predicate_columns=("book_predicate", "trade_predicate"),
            predicate_groups={
                "book_predicate": "book",
                "trade_predicate": "receive",
            },
            config=config,
        )


def test_engineering_panel_is_never_accepted_as_formal(tmp_path: Path) -> None:
    formal, bundle, _ = _fixture(tmp_path)
    with pytest.raises(study.CooldownStudyError, match="formal full-support"):
        study.preflight_study(
            formal_panel_manifest=formal,
            predicate_bundle=bundle,
        )


def test_fold_calendar_is_not_compressed_to_exact_label_days() -> None:
    days = tuple(f"2026-01-{value:02d}" for value in range(1, 9))
    sources = tuple(
        study.DaySource(
            day=day,
            panel_role="prefix40" if index < 6 else "added10",
            manifest_path=Path(f"/{day}.json"),
            manifest_sha256="a" * 64,
        )
        for index, day in enumerate(days)
    )
    rows = [
        {"utc_day": day, "side": side, "panel_role": ("prefix40" if day in days[:6] else "added10")}
        for day in days
        if day != days[1]
        for side in ("BUY", "SELL")
    ]
    config = study.StudyConfig(
        outer_folds=2,
        outer_minimum_train_days=3,
        search=SearchConfig(inner_folds=1, inner_minimum_train_days=2),
        engineering_allow_nonformal_panel=True,
    )
    plans = study._build_fold_plans(  # noqa: SLF001
        sources=sources,
        economic=pd.DataFrame(rows),
        config=config,
    )
    pooled = plans[("pooled50", "BUY")]
    assert pooled.observed_days == days
    assert days[1] in pooled.excluded_days
    assert any(days[1] in fold.train_days for fold in pooled.outer_folds)


@pytest.mark.parametrize("field", ("validation_read", "sealed_holdout_read"))
def test_validate_rejects_report_protected_evidence_reads(
    tmp_path: Path, field: str
) -> None:
    formal, bundle, config = _fixture(tmp_path)
    output = tmp_path / "admitted-study"
    study.run_study(
        formal_panel_manifest=formal,
        predicate_bundle=bundle,
        output=output,
        config=config,
    )
    report_path = output / "report.json"
    report = json.loads(report_path.read_text(encoding="ascii"))
    report[field] = True
    report["permissions"][field] = True
    _write_json(report_path, report)
    _reseal_admission(output, report_changed=True)
    with pytest.raises(study.CooldownStudyError, match="research-only|protected"):
        study.validate_study_output(output)


@pytest.mark.parametrize("field", ("validation_read", "sealed_holdout_read"))
def test_validate_rejects_manifest_protected_evidence_reads(
    tmp_path: Path, field: str
) -> None:
    formal, bundle, config = _fixture(tmp_path)
    output = tmp_path / "admitted-study"
    study.run_study(
        formal_panel_manifest=formal,
        predicate_bundle=bundle,
        output=output,
        config=config,
    )
    manifest_path = output / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="ascii"))
    manifest["permissions"][field] = True
    _write_json(manifest_path, manifest)
    _reseal_admission(output, report_changed=False)
    with pytest.raises(study.CooldownStudyError, match="research-only"):
        study.validate_study_output(output)


def test_validate_recomputes_fold_support_and_continuous_role_contract(
    tmp_path: Path,
) -> None:
    formal, bundle, config = _fixture(tmp_path)
    output = tmp_path / "admitted-study"
    study.run_study(
        formal_panel_manifest=formal,
        predicate_bundle=bundle,
        output=output,
        config=config,
    )
    report_path = output / "report.json"
    original = json.loads(report_path.read_text(encoding="ascii"))
    prefix_buy = original["exact_label_economic_results"][
        "prefix_exact_label_economic"
    ]["sides"]["BUY"]

    broken_fold = json.loads(json.dumps(original))
    del broken_fold["exact_label_economic_results"][
        "prefix_exact_label_economic"
    ]["sides"]["BUY"]["feature_blocks"]["M2"]["folds"][0]["outer_support"]
    _write_json(report_path, broken_fold)
    _reseal_admission(output, report_changed=True)
    with pytest.raises(study.CooldownStudyError, match="outer support is missing"):
        study.validate_study_output(output)

    broken_continuous = json.loads(json.dumps(original))
    del broken_continuous["exact_label_economic_results"][
        "prefix_exact_label_economic"
    ]["sides"]["BUY"]["continuous_state_comparator_feature_blocks"]["M2"][
        "opener_add_support"
    ]["opener"]["action_days"]
    _write_json(report_path, broken_continuous)
    _reseal_admission(output, report_changed=True)
    with pytest.raises(study.CooldownStudyError, match="audit fields are incomplete"):
        study.validate_study_output(output)

    assert prefix_buy["feature_blocks"]["M2"]["folds"]


def test_validate_detects_atomic_output_hash_drift(tmp_path: Path) -> None:
    formal, bundle, config = _fixture(tmp_path)
    output = tmp_path / "admitted-study"
    study.run_study(
        formal_panel_manifest=formal,
        predicate_bundle=bundle,
        output=output,
        config=config,
    )
    report_path = output / "report.json"
    report_path.write_text("{}\n", encoding="ascii")
    with pytest.raises(study.CooldownStudyError, match="report hash drifted"):
        study.validate_study_output(output)
