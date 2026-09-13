from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_features as feature_engine,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_panel as panel,
)

BASE_NS = 1_700_000_000_000_000_000
WINDOW_NS = feature_engine.BASE_WINDOW_WIDTH_NS


def _census(day: str = "2026-01-02") -> pd.DataFrame:
    rows = []
    for ordinal, (offset_ns, role, before, after, units) in enumerate(
        (
            (150_000_000, "opener", 0.0, 0.001, 1.0),
            (250_000_000, "add", 0.001, 0.002, 2.0),
        ),
        start=1,
    ):
        visible_ns = BASE_NS + offset_ns
        visible_ms = visible_ns // 1_000_000
        row = {
            "schema_version": "multiscale_ema_boolean_cooldown_duration_opportunity.v1",
            "fill_clock_semantics": (
                "native_exchange_event_revealed_at_replay_event_clock_"
                "no_live_receive_time_claim"
            ),
            "live_receive_time_authority": False,
            "exposure_fill_ordinal": ordinal,
            "fill_visible_ts_ms": visible_ms,
            "fill_exchange_ts_ms": visible_ms,
            "side": "BUY",
            "role_at_fill": role,
            "order_id": 100 + ordinal,
            "campaign_id": 7,
            "inventory_before_fill_btc": before,
            "inventory_after_fill_btc": after,
            "fill_qty_btc": 0.001,
            "unit_qty_btc": 0.001,
            "consecutive_units_before": units - 1.0,
            "consecutive_units_after": units,
            "prior_deadline_ts_ms": visible_ms - 1_000,
            "baseline_duration_ms": 85_000.0 * units,
            "baseline_deadline_ts_ms": visible_ms + int(85_000 * units),
            "canonical_mid": 100.05,
            "best_bid": 100.0,
            "best_ask": 100.1,
            "decision_visible_bbo_index": ordinal,
            "decision_visible_l2_index": ordinal,
            "market_event_index": ordinal,
            "utc_day": day,
            "campaign_side_id": f"{day}:7:BUY",
            "assignment_ts_ns": visible_ns,
            "opportunity_id": f"opportunity-{ordinal}",
            "source_profile": "native_formal_lifecycle",
            "formal_lifecycle_replay_eligible": True,
            "exact_queue_policy_eligible": False,
            "queue_path_semantics": (
                "native_l2_exact_level_replay_model_without_exchange_queue_authority"
            ),
        }
        row.update(
            {
                name: bool((ordinal + index) % 2)
                for index, name in enumerate(panel.IMMUTABLE_R0_PREDICATE_COLUMNS)
            }
        )
        row.update(
            {
                name: float(ordinal) + (index + 1) / 10_000.0
                for index, name in enumerate(panel.IMMUTABLE_R0_CONTINUOUS_COLUMNS)
            }
        )
        rows.append(row)
    return pd.DataFrame(rows, columns=panel.CENSUS_SAFE_PROJECTION_COLUMNS)


def _values(
    block: str,
    overrides: dict[str, float] | None = None,
) -> dict[str, float]:
    output: dict[str, float] = {}
    for index, spec in enumerate(feature_engine.CHANNELS_BY_BLOCK[block], start=1):
        if spec.name == "mid_usdc_per_btc":
            output[spec.name] = 100.0 + 0.01 * index
        elif spec.name == "spread_bps":
            output[spec.name] = 10.0
        elif "age_s" in spec.name:
            output[spec.name] = 1.0
        elif "imbalance" in spec.name or "deviation" in spec.name:
            output[spec.name] = 0.01
        else:
            output[spec.name] = 1.0 + 0.01 * index
    output.update(overrides or {})
    return output


def _observations(
    block: str = "M2",
    overrides: dict[str, float] | None = None,
):
    for generation, right_offset in enumerate((100_000_000, 200_000_000), start=1):
        yield feature_engine.CausalWindowObservation(
            left_ts_ns=BASE_NS + right_offset - WINDOW_NS,
            right_ts_ns=BASE_NS + right_offset,
            feature_ready_ts_ns=BASE_NS + right_offset,
            market_generation=generation,
            depth_generation=generation,
            values=_values(block, overrides),
            warmup_admitted=True,
        )


def _m0_enrichment(census: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ordinal, census_row in enumerate(census.to_dict("records"), start=1):
        rows.append(
            {
                "opportunity_id": census_row["opportunity_id"],
                "assignment_ts_ns": int(census_row["assignment_ts_ns"]),
                "fill_visible_ts_ns": int(census_row["fill_visible_ts_ms"]) * 1_000_000,
                "side": "BUY",
                "role_at_fill": census_row["role_at_fill"],
                "inventory_before_fill_btc": census_row["inventory_before_fill_btc"],
                "inventory_after_fill_btc": census_row["inventory_after_fill_btc"],
                "fill_qty_btc": 0.001,
                "order_qty_btc": 0.001,
                "cumulative_filled_qty_before_btc": 0.0,
                "cumulative_filled_qty_after_btc": 0.001,
                "remaining_order_qty_after_btc": 0.0,
                "partial_fill_ordinal": 1,
                "fill_is_partial": False,
                "order_age_s": 1.0,
                "queue_ahead_before_fill_btc": None,
                "queue_state_before_fill": "unknown",
                "target_price_tick": 1_000,
                "target_price_displayed_qty_btc": None,
                "target_price_displayed_qty_status": "unknown",
                "target_price_displayed_qty_known": False,
                "target_price_displayed_qty_is_queue_ahead": False,
                "consecutive_units_after": census_row["consecutive_units_after"],
                "baseline_duration_ms": census_row["baseline_duration_ms"],
                "campaign_age_s": float(ordinal - 1),
                "campaign_add_count": max(0, ordinal - 1),
                "campaign_mae_to_date_usdc": -0.01 * (ordinal - 1),
                "campaign_inventory_time_to_date_btc_s": 0.1 * (ordinal - 1),
                "last_same_side_fill_age_s": None if ordinal == 1 else 1.0,
                "last_opposite_side_fill_age_s": None,
                "cooldown_remaining_ms": 0.0,
                "cooldown_blocker_active": False,
                "cooldown_lineage_revision_before": 0,
                "cooldown_deadline_owner": "none",
            }
        )
    return pd.DataFrame(rows)


def _binding(name: str) -> dict[str, object]:
    payload: dict[str, object] = {
        "identity": name,
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
    }
    payload["binding_sha256"] = panel.canonical_sha256(payload)
    return payload


def _source_binding(*, m2_supported: bool = True) -> dict[str, object]:
    normalized_sha = "a" * 64
    raw_sha = "b" * 64 if m2_supported else None
    return {
        "identity": "test-source-split",
        "source_split_semantics": {
            "schema_version": panel.SOURCE_SPLIT_SCHEMA_VERSION,
            "r0_source_identity": panel.R0_SOURCE_IDENTITY,
            "m1_source_identity": panel.M1_SOURCE_IDENTITY,
            "m1_supported": True,
            "raw_m2_used_for_m1": False,
            "normalized_m1_source_binding_sha256": normalized_sha,
            "m2_source_identity": panel.M2_SOURCE_IDENTITY,
            "m2_supported": m2_supported,
            "raw_m2_source_opened": m2_supported,
            "raw_m2_source_binding_sha256": raw_sha,
        },
        "economic_outcomes_read": False,
    }


def test_builder_requires_explicit_full_m0_or_reduced_opt_in() -> None:
    with pytest.raises(panel.ModeledFeaturePanelError, match="full M0 enrichment"):
        panel.build_feature_frames(
            _census(),
            m1_observations=_observations("M1"),
            m1_warmup_identity="normalized-warmup",
            m2_observations=_observations("M2"),
            m2_warmup_identity="raw-warmup",
            m2_day_supported=True,
        )


def test_reduced_m0_keeps_missing_fields_unobserved_not_zero() -> None:
    frames, audit = panel.build_feature_frames(
        _census(),
        m1_observations=_observations("M1"),
        m1_warmup_identity="normalized-warmup",
        m2_observations=_observations("M2"),
        m2_warmup_identity="raw-warmup",
        allow_reduced_m0=True,
        m2_day_supported=True,
    )

    assert audit.full_m0_support is False
    assert audit.support_identity == panel.REDUCED_M0_SUPPORT_IDENTITY
    assert frames["R0"]["support_valid"].tolist() == [True, True]
    for block in ("M0", "M1", "M2"):
        assert frames[block]["support_valid"].tolist() == [False, False]
        assert frames[block]["order_qty_btc"].isna().all()
        assert frames[block]["campaign_age_s"].isna().all()
        assert frames[block]["campaign_mae_to_date_usdc"].isna().all()
        assert frames[block]["queue_ahead_before_fill_btc"].isna().all()
        assert frames[block]["m0_missing_value_semantics"].eq(
            "null_means_UNOBSERVED_not_zero"
        ).all()
        assert frames[block]["predicate::m0::role_is_add"].tolist() == [0, 1]
        assert frames[block]["predicate::m0::fill_is_partial"].eq(-1).all()
    assert frames["M2"]["exact_queue_policy_eligible"].eq(False).all()
    assert frames["M2"]["owner_modeled_queue"].eq(True).all()


def test_full_m0_is_joined_by_opportunity_id_not_row_order() -> None:
    census = _census()
    enrichment = _m0_enrichment(census).iloc[::-1].reset_index(drop=True)
    frames, audit = panel.build_feature_frames(
        census,
        m1_observations=_observations("M1"),
        m1_warmup_identity="normalized-warmup",
        m2_observations=_observations("M2"),
        m2_warmup_identity="raw-warmup",
        m0_enrichment=enrichment,
        m2_day_supported=True,
    )

    assert audit.full_m0_support is True
    assert frames["M2"]["support_valid"].tolist() == [True, True]
    assert frames["M2"]["campaign_age_s"].tolist() == [0.0, 1.0]
    assert frames["M2"]["role_at_fill"].tolist() == ["opener", "add"]
    assert frames["M2"]["baseline_duration_ms"].tolist() == [85_000.0, 170_000.0]
    assert frames["M2"]["fill_qty_btc"].tolist() == [0.001, 0.001]
    assert frames["M2"]["feature_ready_ts_ns"].tolist() == [
        BASE_NS + 100_000_000,
        BASE_NS + 200_000_000,
    ]
    assert frames["M2"]["predicate::m0::role_is_add"].tolist() == [0, 1]
    assert frames["M2"]["predicate::m0::campaign_has_prior_add"].tolist() == [0, 1]
    assert frames["M2"]["predicate::m0::consecutive_units_ge_2"].tolist() == [0, 1]
    assert frames["M2"]["predicate::m0::same_fill_age_le_control_duration"].tolist() == [
        -1,
        1,
    ]


def test_immutable_v1_r0_is_copied_exactly_into_cumulative_blocks() -> None:
    census = _census()
    frames, _ = panel.build_feature_frames(
        census,
        m1_observations=_observations("M1"),
        m1_warmup_identity="normalized-warmup",
        m2_observations=_observations("M2"),
        m2_warmup_identity="raw-warmup",
        m0_enrichment=_m0_enrichment(census),
        m2_day_supported=True,
    )

    immutable = list(panel.IMMUTABLE_R0_COLUMNS)
    for block in ("R0", "M1", "M2"):
        pd.testing.assert_frame_equal(
            frames[block].loc[:, immutable],
            census.loc[:, immutable],
            check_exact=True,
        )
    assert len(panel.IMMUTABLE_R0_PREDICATE_COLUMNS) == 360
    assert len(panel.IMMUTABLE_R0_CONTINUOUS_COLUMNS) == 291


def test_normalized_m1_and_raw_m2_sources_are_not_cross_coupled() -> None:
    census = _census()
    frames, audit = panel.build_feature_frames(
        census,
        m1_observations=_observations(
            "M1",
            {
                "mid_usdc_per_btc": 101.0,
                "spread_bps": 11.0,
            },
        ),
        m1_warmup_identity="normalized-warmup",
        m2_observations=_observations(
            "M2",
            {
                "mid_usdc_per_btc": 999.0,
                "spread_bps": 99.0,
                "aggressive_buy_qty_btc_per_s": 7.0,
            },
        ),
        m2_warmup_identity="raw-warmup",
        m0_enrichment=_m0_enrichment(census),
        m2_day_supported=True,
    )

    mid_ema = "value::mid_usdc_per_btc::ema::h0p5s"
    spread_ema = "value::spread_bps::ema::h0p5s"
    raw_trade_ema = "value::aggressive_buy_qty_btc_per_s::ema::h0p5s"
    assert frames["M1"][mid_ema].tolist() == [101.0, 101.0]
    assert frames["M2"][mid_ema].tolist() == [101.0, 101.0]
    assert frames["M2"][spread_ema].tolist() == [11.0, 11.0]
    assert frames["M2"][raw_trade_ema].tolist() == [7.0, 7.0]
    assert frames["M2"]["normalized_m1_market_generation"].tolist() == [1, 2]
    assert frames["M2"]["raw_m2_market_generation"].tolist() == [1, 2]
    assert audit.normalized_m1_observation_count == 2
    assert audit.raw_m2_observation_count == 2


def test_frozen_m2_excluded_day_preserves_rows_and_only_marks_m2_unsupported() -> None:
    census = _census("2026-04-20")
    frames, audit = panel.build_feature_frames(
        census,
        m1_observations=_observations("M1"),
        m1_warmup_identity="normalized-M1",
        m0_enrichment=_m0_enrichment(census),
    )

    assert audit.m2_day_supported is False
    assert {block: len(frame) for block, frame in frames.items()} == {
        "R0": 2,
        "M0": 2,
        "M1": 2,
        "M2": 2,
    }
    assert frames["R0"]["support_valid"].all()
    assert frames["M0"]["support_valid"].all()
    assert frames["M1"]["support_valid"].all()
    assert not frames["M2"]["support_valid"].any()
    assert not frames["M2"]["m2_source_support_valid"].any()
    assert frames["M2"]["m2_source_support_reason"].eq(
        "frozen_prefix40_raw_M2_excluded_day"
    ).all()
    m2_only_observed = "channel::aggressive_buy_qty_btc_per_s::observed"
    assert frames["M2"][m2_only_observed].eq(0).all()
    m2_only_tri = (
        "tri::aggressive_buy_qty_btc_per_s__h0p5s__h1s::positive_ordering"
    )
    assert frames["M2"][m2_only_tri].eq(
        int(feature_engine.TriState.UNOBSERVED)
    ).all()


def test_enrichment_coverage_and_census_identity_fail_closed() -> None:
    census = _census()
    incomplete = _m0_enrichment(census).iloc[:1].copy()
    with pytest.raises(panel.ModeledFeaturePanelError, match="coverage drifted"):
        panel.validate_m0_enrichment(census, incomplete)

    enrichment = _m0_enrichment(census)
    enrichment.loc[0, "assignment_ts_ns"] += 1
    with pytest.raises(panel.ModeledFeaturePanelError, match="disagrees with census"):
        panel.validate_m0_enrichment(census, enrichment)


def test_enrichment_rejects_economic_columns_before_schema_use() -> None:
    census = _census()
    enrichment = _m0_enrichment(census).assign(terminal_value_usdc=123.0)

    with pytest.raises(
        panel.ModeledFeaturePanelError,
        match="prohibited arm/economic columns",
    ):
        panel.validate_m0_enrichment(census, enrichment)


def test_m0_provider_projects_safe_columns_and_binds_manifest(tmp_path: Path) -> None:
    census = _census()
    day_root = tmp_path / "days" / "2026-01-02"
    day_root.mkdir(parents=True)
    data_path = day_root / "m0_context.parquet"
    provider = _m0_enrichment(census).assign(
        snapshot_id=["snapshot-1", "snapshot-2"],
        arm_outcomes_read=False,
        exact_queue_policy_eligible=False,
    )
    pq.write_table(pa.Table.from_pandas(provider, preserve_index=False), data_path)
    manifest = {
        "identity": panel.EXPECTED_M0_PROVIDER_IDENTITY,
        "status": panel.EXPECTED_M0_PROVIDER_STATUS,
        "utc_day": "2026-01-02",
        "data_sha256": panel.sha256_file(data_path),
        "row_count": len(provider),
        "execution_identity_sha256": "1" * 64,
        "source_census_data_sha256": "2" * 64,
        "source_census_manifest_sha256": "3" * 64,
        "m0_columns": list(feature_engine.M0_REQUIRED_FIELDS),
        "economic_outcomes_read": False,
        "arm_outcomes_read": False,
        "duration_treatment_applied": False,
        "exact_queue_policy_eligible": False,
    }
    manifest_path = day_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    resolved_data, resolved_manifest = panel.resolve_m0_enrichment_day(
        tmp_path,
        "2026-01-02",
    )
    loaded, binding = panel.load_m0_enrichment(
        resolved_data,
        opportunities=census,
        manifest_path=resolved_manifest,
        census_binding={
            "utc_day": "2026-01-02",
            "data_sha256": "2" * 64,
            "manifest_sha256": "3" * 64,
        },
    )

    assert resolved_manifest == manifest_path
    assert set(loaded.columns) == {"opportunity_id", *feature_engine.M0_REQUIRED_FIELDS}
    assert "snapshot_id" in binding["provider_schema_columns"]
    assert "snapshot_id" not in binding["columns_read"]
    assert binding["arm_economic_labels_read"] is False
    assert binding["execution_identity_sha256"] == "1" * 64


def test_frozen_support_partition_is_complete_and_cannot_be_overridden() -> None:
    assert len(panel.PREFIX40_DAYS) == 40
    assert len(panel.M2_COMMON_SUPPORT_DAYS) == 33
    assert len(panel.M2_EXCLUDED_DAYS) == 7
    assert set(panel.M2_COMMON_SUPPORT_DAYS).isdisjoint(panel.M2_EXCLUDED_DAYS)
    assert set(panel.M2_COMMON_SUPPORT_DAYS) | panel.M2_EXCLUDED_DAYS == set(
        panel.PREFIX40_DAYS
    )

    census = _census("2026-04-20")
    with pytest.raises(panel.ModeledFeaturePanelError, match="support override"):
        panel.build_feature_frames(
            census,
            m1_observations=_observations("M1"),
            m1_warmup_identity="normalized-M1",
            m2_observations=_observations("M2"),
            m2_warmup_identity="raw-M2",
            m0_enrichment=_m0_enrichment(census),
            m2_day_supported=True,
        )


def test_census_loader_projects_only_allowlisted_columns(tmp_path: Path) -> None:
    day = "2026-01-02"
    day_root = tmp_path / day
    day_root.mkdir()
    census = _census(day)
    stored = census.assign(terminal_pnl_usdc=[123.0, 456.0])
    data_path = day_root / "opportunities.parquet"
    pq.write_table(pa.Table.from_pandas(stored, preserve_index=False), data_path)
    manifest = {
        "identity": "multiscale_ema_boolean_cooldown_duration_policy_v1",
        "utc_day": day,
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "data_sha256": panel.sha256_file(data_path),
        "book_source_contract": {"exact_queue_policy_eligible": False},
    }
    (day_root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    loaded, binding = panel.load_census_day(day, census_root=tmp_path)

    assert tuple(loaded.columns) == panel.CENSUS_SAFE_PROJECTION_COLUMNS
    assert "terminal_pnl_usdc" not in loaded
    assert binding["columns_read"] == list(panel.CENSUS_SAFE_PROJECTION_COLUMNS)
    assert binding["immutable_r0_predicate_count"] == 360
    assert binding["immutable_r0_continuous_count"] == 291
    assert binding["arm_economic_labels_read"] is False


def test_normalized_window_binding_records_partial_d_minus_1_warmup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from research.families.f03_causal_13_head.audit import (
        causal_v12_1s_native_40day_full_path_ml_ab as f03_full_path,
    )

    day = panel.PREFIX40_DAYS[0]
    target_start_ns = int(pd.Timestamp(day, tz="UTC").value)
    source_start_ns = target_start_ns - 86_400_000_000_000 + 120_000_000_000
    target_end_ns = target_start_ns + 86_400_000_000_000
    window_path = tmp_path / "window.pkl"
    window_path.write_bytes(b"bound-window")
    payload = {
        "ordered_utc_days": list(panel.PREFIX40_DAYS),
        "days": [
            {
                "utc_day": day,
                "daily_source_identity_sha256": "d" * 64,
                "window": {
                    "path": str(window_path),
                    "sha256": panel.sha256_file(window_path),
                    "size_bytes": window_path.stat().st_size,
                },
            }
        ],
    }
    plan_payload = {
        "identity": panel.EXPECTED_NORMALIZED_PLAN_IDENTITY,
        "identity_payload": payload,
        "plan_identity_sha256": panel.canonical_sha256(payload),
        "day_count": 40,
        "economic_outcomes_read": False,
        "development_pnl_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    plan_path = tmp_path / "execution-plan.json"
    plan_path.write_text(json.dumps(plan_payload), encoding="utf-8")
    (tmp_path / "_PLAN_SUCCESS").write_text(
        panel.sha256_file(plan_path) + "\n",
        encoding="ascii",
    )
    fake_window = SimpleNamespace(
        bbo_data=SimpleNamespace(
            ts_ms=np.asarray(
                [source_start_ns // 1_000_000, (target_end_ns - 100_000_000) // 1_000_000]
            ),
            source="historical_bbo",
        ),
        l2_data=SimpleNamespace(
            ts_ms=np.asarray(
                [source_start_ns // 1_000_000, (target_end_ns - 100_000_000) // 1_000_000]
            ),
            source="historical_l2",
        ),
        book_source_authority="native_formal_lifecycle",
        formal_lifecycle_replay_eligible=True,
    )
    monkeypatch.setattr(f03_full_path, "_load_bound_window", lambda _path: fake_window)
    before_target = feature_engine.CausalWindowObservation(
        left_ts_ns=target_start_ns - 200_000_000,
        right_ts_ns=target_start_ns - 100_000_000,
        feature_ready_ts_ns=target_start_ns - 100_000_000,
        market_generation=1,
        depth_generation=1,
        values=_values("M1"),
    )
    at_target = feature_engine.CausalWindowObservation(
        left_ts_ns=target_start_ns - 100_000_000,
        right_ts_ns=target_start_ns,
        feature_ready_ts_ns=target_start_ns,
        market_generation=2,
        depth_generation=2,
        values=_values("M1"),
    )
    monkeypatch.setattr(
        panel.normalized_windows,
        "stream_causal_windows",
        lambda **_kwargs: iter((before_target, at_target)),
    )

    stream, _audit, binding = panel._load_normalized_m1_source(
        day,
        execution_plan_path=plan_path,
    )
    observations = list(stream)

    assert [row.warmup_admitted for row in observations] == [False, True]
    assert binding["window_path"] == str(window_path)
    assert binding["window_sha256"] == panel.sha256_file(window_path)
    assert binding["exact_full_24h_warmup"] is False
    assert binding["available_d_minus_1_warmup_span_ns"] == (
        86_400_000_000_000 - 120_000_000_000
    )
    assert binding["warmup_admitted_at_target_day_start"] is True


def test_atomic_day_admission_binds_all_blocks_without_label_paths(
    tmp_path: Path,
) -> None:
    census = _census()
    frames, audit = panel.build_feature_frames(
        census,
        m1_observations=_observations("M1"),
        m1_warmup_identity="normalized-warmup",
        m2_observations=_observations("M2"),
        m2_warmup_identity="raw-warmup",
        m0_enrichment=_m0_enrichment(census),
        m2_day_supported=True,
    )
    manifest = panel.admit_feature_day(
        day="2026-01-02",
        frames=frames,
        audit=audit,
        output_root=tmp_path,
        census_binding=_binding("census"),
        m0_binding=_binding("m0"),
        source_binding=_source_binding(),
    )

    final = tmp_path / "2026-01-02"
    assert final.is_dir()
    assert (final / "manifest.json").is_file()
    assert (final / panel.DAY_SUCCESS).read_text(encoding="ascii").strip() == manifest[
        "canonical_manifest_sha256"
    ]
    assert set(manifest["blocks"]) == set(panel.FEATURE_BLOCKS)
    assert manifest["arm_label_paths_opened"] == []
    assert manifest["economic_outcomes_read"] is False
    assert manifest["exact_queue_policy_eligible"] is False
    assert manifest["source_split_semantics"] == _source_binding()[
        "source_split_semantics"
    ]
    assert set(manifest["input_identity"]["code_bindings"]) == {
        "builder_sha256",
        "feature_engine_sha256",
        "native_feature_engine_sha256",
        "batch_builder_sha256",
    }
    assert not list(tmp_path.glob(".*.staging-*"))
    with pytest.raises(panel.ModeledFeaturePanelError, match="already exists"):
        panel.admit_feature_day(
            day="2026-01-02",
            frames=frames,
            audit=audit,
            output_root=tmp_path,
            census_binding=_binding("census"),
            m0_binding=_binding("m0"),
            source_binding=_source_binding(),
        )
