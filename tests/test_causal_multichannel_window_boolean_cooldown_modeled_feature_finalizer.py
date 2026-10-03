from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_finalizer as finalizer,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_modeled_feature_panel as panel,
)

SOURCE_CENSUS_EXECUTION = "1" * 64
M0_EXECUTION = "2" * 64
NORMALIZED_M1_BINDING = "3" * 64
RAW_M2_BINDING = "4" * 64


def test_source_split_constants_are_shared_with_day_builder() -> None:
    assert finalizer.SOURCE_SPLIT_SCHEMA_VERSION == panel.SOURCE_SPLIT_SCHEMA_VERSION
    assert finalizer.EXPECTED_R0_SOURCE_IDENTITY == panel.R0_SOURCE_IDENTITY
    assert finalizer.EXPECTED_M1_SOURCE_IDENTITY == panel.M1_SOURCE_IDENTITY
    assert finalizer.EXPECTED_M2_SOURCE_IDENTITY == panel.M2_SOURCE_IDENTITY


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, sort_keys=True), encoding="ascii")


def _with_binding_sha256(payload: dict[str, object]) -> dict[str, object]:
    bound = dict(payload)
    bound["binding_sha256"] = finalizer._canonical_sha256(bound)
    return bound


def _write_provenance(
    day_root: Path,
    day: str,
    *,
    m0_execution: str,
) -> tuple[dict[str, object], dict[str, object]]:
    provenance_root = day_root / "provenance"
    provenance_root.mkdir()

    census_data = provenance_root / "opportunities.parquet"
    census_data.write_bytes(b"outcome-blind-census")
    census_manifest = {
        "identity": finalizer.EXPECTED_SOURCE_CENSUS_IDENTITY,
        "utc_day": day,
        "execution_identity_sha256": SOURCE_CENSUS_EXECUTION,
        "data_sha256": finalizer._sha256(census_data),
        "economic_outcomes_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
    }
    census_manifest_path = provenance_root / "census_manifest.json"
    _write_json(census_manifest_path, census_manifest)
    census_binding = _with_binding_sha256(
        {
            "identity": finalizer.EXPECTED_SOURCE_CENSUS_IDENTITY,
            "utc_day": day,
            "manifest_path": str(census_manifest_path),
            "manifest_sha256": finalizer._sha256(census_manifest_path),
            "data_path": str(census_data),
            "data_sha256": finalizer._sha256(census_data),
            "economic_outcomes_read": False,
            "arm_economic_labels_read": False,
            "exact_queue_policy_eligible": False,
        }
    )

    m0_data = provenance_root / "m0_context.parquet"
    m0_data.write_bytes(b"outcome-blind-m0")
    m0_manifest = {
        "identity": panel.EXPECTED_M0_PROVIDER_IDENTITY,
        "status": panel.EXPECTED_M0_PROVIDER_STATUS,
        "utc_day": day,
        "execution_identity_sha256": m0_execution,
        "data_sha256": finalizer._sha256(m0_data),
        "row_count": 1,
        "source_census_data_sha256": census_binding["data_sha256"],
        "source_census_manifest_sha256": census_binding["manifest_sha256"],
        "economic_outcomes_read": False,
        "arm_outcomes_read": False,
        "duration_treatment_applied": False,
        "exact_queue_policy_eligible": False,
    }
    m0_manifest_path = provenance_root / "m0_manifest.json"
    _write_json(m0_manifest_path, m0_manifest)
    m0_binding = _with_binding_sha256(
        {
            "mode": "full_explicit_M0_enrichment",
            "path": str(m0_data),
            "sha256": finalizer._sha256(m0_data),
            "manifest": {
                "path": str(m0_manifest_path),
                "sha256": finalizer._sha256(m0_manifest_path),
                "identity": panel.EXPECTED_M0_PROVIDER_IDENTITY,
                "status": panel.EXPECTED_M0_PROVIDER_STATUS,
                "utc_day": day,
                "execution_identity_sha256": m0_execution,
                "source_census_data_sha256": census_binding["data_sha256"],
                "source_census_manifest_sha256": census_binding[
                    "manifest_sha256"
                ],
            },
            "provider_identity": panel.EXPECTED_M0_PROVIDER_IDENTITY,
            "execution_identity_sha256": m0_execution,
            "economic_outcomes_read": False,
            "arm_economic_labels_read": False,
        }
    )
    return census_binding, m0_binding


def _write_day(
    root: Path,
    day: str,
    *,
    m2_supported: bool,
    index: int,
    m0_execution: str = M0_EXECUTION,
    historical_predicate_count: int = 360,
    m1_source_identity: str = finalizer.EXPECTED_M1_SOURCE_IDENTITY,
) -> None:
    day_root = root / day
    day_root.mkdir(parents=True)
    historical_predicates = {
        f"predicate::ema_pair_fixture_{position:03d}": [position % 2]
        for position in range(historical_predicate_count)
    }
    frame = pd.DataFrame(
        {
            "utc_day": [day],
            "opportunity_id": [f"op-{index}"],
            "economic_outcomes_read": [False],
            **historical_predicates,
            "ema_causal_volatility_bps": [1.0 + index],
            "ema_rel_mid_bps_h0p5s": [0.1 + index],
            "ema_slope_bps_per_s_h0p5s": [0.01 + index],
            "ema_pair_h0p5s_h1s_cross_age_s": [2.0 + index],
            "ema_pair_h0p5s_h1s_arrangement_persistence_s": [3.0 + index],
            "ema_pair_h0p5s_h1s_favorable_distance_bps": [0.2 + index],
            "ema_pair_h0p5s_h1s_abs_distance_bps": [0.2 + index],
            "ema_pair_h0p5s_h1s_volatility_normalized": [0.3 + index],
            "ema_pair_h0p5s_h1s_favorable_distance_velocity_bps_per_s": [
                0.04 + index
            ],
            "ema_pair_favorable_fraction": [0.5],
            "predicate::m0::role_is_add": [index % 2],
            "predicate::m0::fill_is_partial": [0],
            "tri::mid_usdc_per_btc__h0p5s__h1s::positive_ordering": [1],
            "tri::spread_bps__h0p5s__h1s::positive_ordering": [0],
            "tri::aggressive_buy_qty_btc_per_s__h0p5s__h1s::positive_ordering": [
                1 if m2_supported else -1
            ],
            "inventory_after_fill_btc": [0.001 * (index + 1)],
            "baseline_duration_ms": [85_000.0 * (index + 1)],
            "value::mid_usdc_per_btc::ema::h0p5s": [60_000.0 + index],
            "value::spread_bps::ema::h0p5s": [1.0 + index],
            "value::aggressive_buy_qty_btc_per_s::ema::h0p5s": [
                2.0 + index if m2_supported else float("nan")
            ],
        }
    )
    blocks: dict[str, dict[str, object]] = {}
    for block in panel.FEATURE_BLOCKS:
        path = day_root / f"{block}.parquet"
        frame.to_parquet(path, index=False)
        blocks[block] = {
            "path": path.name,
            "sha256": finalizer._sha256(path),
            "row_count": 1,
        }
    census_binding, m0_binding = _write_provenance(
        day_root,
        day,
        m0_execution=m0_execution,
    )
    source_split = {
        "schema_version": finalizer.SOURCE_SPLIT_SCHEMA_VERSION,
        "r0_source_identity": finalizer.EXPECTED_R0_SOURCE_IDENTITY,
        "m1_source_identity": m1_source_identity,
        "m1_supported": True,
        "normalized_m1_source_binding_sha256": NORMALIZED_M1_BINDING,
        "raw_m2_used_for_m1": False,
        "m2_source_identity": finalizer.EXPECTED_M2_SOURCE_IDENTITY,
        "m2_supported": m2_supported,
        "raw_m2_source_opened": m2_supported,
        "raw_m2_source_binding_sha256": RAW_M2_BINDING if m2_supported else None,
    }
    manifest = {
        "identity": panel.IDENTITY,
        "utc_day": day,
        "full_m0_support": True,
        "owner_modeled_queue": True,
        "exact_queue_policy_eligible": False,
        "economic_outcomes_read": False,
        "arm_economic_labels_read": False,
        "validation_read": False,
        "sealed_holdout_read": False,
        "m2_day_supported": m2_supported,
        "opportunity_count": 1,
        "blocks": blocks,
        "census_binding": census_binding,
        "m0_binding": m0_binding,
        "source_split_semantics": source_split,
    }
    manifest["canonical_manifest_sha256"] = finalizer._canonical_sha256(manifest)
    _write_json(day_root / "manifest.json", manifest)
    (day_root / panel.DAY_SUCCESS).write_text(
        f"{manifest['canonical_manifest_sha256']}\n", encoding="ascii"
    )


def _configure_two_days(monkeypatch: pytest.MonkeyPatch) -> tuple[str, str]:
    days = ("2026-01-01", "2026-01-02")
    monkeypatch.setattr(panel, "PREFIX40_DAYS", days)
    monkeypatch.setattr(panel, "M2_COMMON_SUPPORT_DAYS", (days[0],))
    monkeypatch.setattr(panel, "M2_EXCLUDED_DAYS", frozenset({days[1]}))
    monkeypatch.setattr(finalizer, "EXPECTED_OPPORTUNITIES", 2)
    monkeypatch.setattr(
        finalizer,
        "M0_CONTINUOUS_CANDIDATES",
        ("inventory_after_fill_btc", "baseline_duration_ms"),
    )
    return days


def test_finalize_binds_provenance_historical_r0_and_source_split(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _configure_two_days(monkeypatch)
    _write_day(tmp_path, days[0], m2_supported=True, index=0)
    _write_day(tmp_path, days[1], m2_supported=False, index=1)

    result = finalizer.finalize(tmp_path)

    assert [item["relative_path"] for item in result["files"]] == [
        "2026-01-01/M2.parquet",
        "2026-01-02/M2.parquet",
    ]
    assert result["m0_execution_identity_sha256"] == M0_EXECUTION
    assert (
        result["source_census_execution_identity_sha256"]
        == SOURCE_CENSUS_EXECUTION
    )
    assert result["label_feature_execution_identity_same"] is False
    assert result["source_split_semantics"] == {
        "schema_version": finalizer.SOURCE_SPLIT_SCHEMA_VERSION,
        "R0": finalizer.EXPECTED_R0_SOURCE_IDENTITY,
        "M1": finalizer.EXPECTED_M1_SOURCE_IDENTITY,
        "M1_support": "prefix40_all_days",
        "M2": finalizer.EXPECTED_M2_SOURCE_IDENTITY,
        "M2_support": "frozen_prefix33_only",
        "raw_M2_used_for_M1": False,
    }
    blocks = result["feature_schema"]["feature_blocks"]
    assert len(blocks["R0"]["boolean_predicates"]) == 360
    assert all(
        name.startswith(finalizer.HISTORICAL_R0_PREDICATE_PREFIX)
        for name in blocks["R0"]["boolean_predicates"]
    )
    assert "ema_causal_volatility_bps" in blocks["R0"]["continuous_features"]
    assert "predicate::m0::role_is_add" in blocks["M0"]["boolean_predicates"]
    assert set(blocks["R0"]["boolean_predicates"]).issubset(
        blocks["M1"]["boolean_predicates"]
    )
    assert set(blocks["M1"]["boolean_predicates"]).issubset(
        blocks["M2"]["boolean_predicates"]
    )
    assert "value::aggressive_buy_qty_btc_per_s::ema::h0p5s" in blocks["M2"][
        "continuous_features"
    ]
    assert finalizer.validate(tmp_path) == result


def test_finalize_rejects_mixed_m0_execution_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _configure_two_days(monkeypatch)
    _write_day(tmp_path, days[0], m2_supported=True, index=0)
    _write_day(
        tmp_path,
        days[1],
        m2_supported=False,
        index=1,
        m0_execution="5" * 64,
    )

    with pytest.raises(
        finalizer.FeatureFinalizerError,
        match="M0 execution identity is not shared",
    ):
        finalizer.finalize(tmp_path)


def test_finalize_rejects_incomplete_historical_r0_universe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _configure_two_days(monkeypatch)
    _write_day(
        tmp_path,
        days[0],
        m2_supported=True,
        index=0,
        historical_predicate_count=359,
    )
    _write_day(
        tmp_path,
        days[1],
        m2_supported=False,
        index=1,
        historical_predicate_count=359,
    )

    with pytest.raises(finalizer.FeatureFinalizerError, match="feature schema lacks"):
        finalizer.finalize(tmp_path)


def test_finalize_rejects_raw_m2_as_m1_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _configure_two_days(monkeypatch)
    _write_day(
        tmp_path,
        days[0],
        m2_supported=True,
        index=0,
        m1_source_identity=finalizer.EXPECTED_M2_SOURCE_IDENTITY,
    )
    _write_day(tmp_path, days[1], m2_supported=False, index=1)

    with pytest.raises(finalizer.FeatureFinalizerError, match="source split drifted"):
        finalizer.finalize(tmp_path)


def test_validate_rejects_tampered_selected_table(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    days = _configure_two_days(monkeypatch)
    _write_day(tmp_path, days[0], m2_supported=True, index=0)
    _write_day(tmp_path, days[1], m2_supported=False, index=1)
    finalizer.finalize(tmp_path)
    with (tmp_path / days[0] / "M2.parquet").open("ab") as handle:
        handle.write(b"tamper")

    with pytest.raises(finalizer.FeatureFinalizerError):
        finalizer.validate(tmp_path)
