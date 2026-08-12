from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit.causal_multichannel_window_boolean_cooldown_predicates import (
    PredicateArtifact,
    PredicateContractError,
    fit_predicate_artifact,
    transform_predicates,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "snapshot_id": ["a", "b", "c", "d"],
            "utc_day": [
                "2025-08-01",
                "2025-08-01",
                "2025-08-02",
                "2025-08-02",
            ],
            "side": ["BUY", "BUY", "BUY", "BUY"],
            "role_at_fill": ["opener", "add", "add", "opener"],
            "queue_state_before_fill": ["exact", "known_zero", "unknown", "exact"],
            "target_price_displayed_qty_status": [
                "exact",
                "known_zero",
                "unknown",
                "exact",
            ],
            "target_price_displayed_qty_known": [True, True, False, True],
            "fill_is_partial": [True, False, False, True],
            "cooldown_blocker_active": [False, True, False, True],
            "cooldown_deadline_owner": [
                "none",
                "existing_same_side_lineage",
                "none",
                "existing_same_side_lineage",
            ],
            "inventory_before_fill_btc": [0.0, -0.001, 0.001, 0.0],
            "consecutive_units_after": [1.0, 2.0, 3.0, 4.0],
            "baseline_duration_ms": [85_000.0, 170_000.0, 255_000.0, 340_000.0],
            "value::mid_usdc_per_btc::ema::h1s": [100.0, 101.0, 102.0, 103.0],
            "tri::mid_usdc_per_btc__h1s__h2s::positive_ordering": [1, 0, -1, 1],
            "value::signed_flow_imbalance::ema::h1s": [-1.0, 0.0, 1.0, 2.0],
            "tri::signed_flow_imbalance__h1s__h2s::positive_ordering": [0, 1, 1, -1],
        }
    )


def _fit(frame: pd.DataFrame | None = None) -> PredicateArtifact:
    return fit_predicate_artifact(
        _frame() if frame is None else frame,
        side="BUY",
        source_role="outcome_blind_2025_single_channel",
        reference_identity_sha256="a" * 64,
        reference_days=("2025-08-01", "2025-08-02"),
        source_clock_identity={
            "book": "tardis_provider_local_receive_clock_v1",
            "trade": "binance_exchange_aggtrade_clock_v1",
        },
        quantiles=(0.25, 0.5, 0.75),
    )


@pytest.mark.parametrize(
    "column",
    [
        "terminal_usdc",
        "daily_pnl",
        "reward_hint",
        "training_label",
        "operational_outcome",
        "maker_markout_10s",
    ],
)
def test_rejects_any_leakage_column(column: str) -> None:
    frame = _frame().assign(**{column: 0.0})
    with pytest.raises(PredicateContractError, match="prohibited"):
        _fit(frame)


def test_preserves_tri_state_and_models_missing_continuous_as_unobserved() -> None:
    reference = _frame()
    artifact = _fit(reference)
    target = reference.copy()
    target.loc[0, "consecutive_units_after"] = np.nan
    result = transform_predicates(target, artifact)

    existing = "tri::mid_usdc_per_btc__h1s__h2s::positive_ordering"
    assert result.columns[existing].tolist() == [1, 0, -1, 1]
    median = "tri::quantile::consecutive_units_after::ge::q5000"
    assert result.columns[median].tolist() == [-1, 0, 1, 1]
    assert set(np.unique(result.columns.to_numpy())) <= {-1, 0, 1}


def test_categorical_m0_predicates_are_deterministic_and_unknown_is_explicit() -> None:
    reference = _frame()
    artifact = _fit(reference)
    result = artifact.transform(reference)

    assert result.columns["tri::m0::side::buy"].tolist() == [1, 1, 1, 1]
    assert result.columns["tri::m0::side::sell"].tolist() == [0, 0, 0, 0]
    assert result.columns["tri::m0::role::add"].tolist() == [0, 1, 1, 0]
    assert result.columns["tri::m0::queue::known_zero"].tolist() == [0, 1, 0, 0]
    assert result.columns["tri::m0::queue::unknown"].tolist() == [0, 0, 1, 0]
    assert result.columns["tri::m0::fill::partial"].tolist() == [1, 0, 0, 1]
    assert result.columns["tri::m0::fill::full"].tolist() == [0, 1, 1, 0]
    assert result.columns["tri::m0::cooldown_blocker::active"].tolist() == [0, 1, 0, 1]
    assert result.columns["tri::m0::cooldown_owner::none"].tolist() == [1, 0, 1, 0]
    assert result.columns[
        "tri::m0::cooldown_owner::existing_same_side_lineage"
    ].tolist() == [0, 1, 0, 1]

    missing = reference.copy()
    missing.loc[0, "queue_state_before_fill"] = None
    missing_result = artifact.transform(missing)
    queue_columns = [
        "tri::m0::queue::exact",
        "tri::m0::queue::known_zero",
        "tri::m0::queue::unknown",
    ]
    assert missing_result.columns.loc[0, queue_columns].tolist() == [-1, -1, -1]

    drifted = reference.copy()
    drifted.loc[0, "queue_state_before_fill"] = "estimated"
    with pytest.raises(PredicateContractError, match="domain drifted"):
        artifact.transform(drifted)

    raw_owner = reference.copy()
    raw_owner.loc[1, "cooldown_deadline_owner"] = "buy-lineage-7"
    with pytest.raises(PredicateContractError, match="domain drifted"):
        artifact.transform(raw_owner)


def test_2025_clock_separation_forbids_cross_channel_clause_and_joint_field() -> None:
    artifact = _fit()
    book = "tri::mid_usdc_per_btc__h1s__h2s::positive_ordering"
    trade = "tri::signed_flow_imbalance__h1s__h2s::positive_ordering"
    artifact.validate_clause([book])
    artifact.validate_clause([trade])
    with pytest.raises(PredicateContractError, match="cannot combine"):
        artifact.validate_clause([book, trade])

    joint = _frame().rename(
        columns={
            "value::signed_flow_imbalance::ema::h1s": (
                "value::book_trade_signed_flow_depth::ema::h1s"
            )
        }
    )
    with pytest.raises(PredicateContractError, match="unknown block|combines"):
        _fit(joint)

    book_only = _frame().drop(
        columns=[
            "value::signed_flow_imbalance::ema::h1s",
            "tri::signed_flow_imbalance__h1s__h2s::positive_ordering",
        ]
    )
    single_clock = fit_predicate_artifact(
        book_only,
        side="BUY",
        source_role="outcome_blind_2025_single_channel",
        reference_identity_sha256="b" * 64,
        reference_days=("2025-08-01", "2025-08-02"),
        source_clock_identity="tardis_provider_local_receive_clock_v1",
    )
    assert single_clock.source_clock_identity == {
        "shared": "tardis_provider_local_receive_clock_v1"
    }

    with pytest.raises(PredicateContractError, match="separate clock identities"):
        fit_predicate_artifact(
            _frame(),
            side="BUY",
            source_role="outcome_blind_2025_single_channel",
            reference_identity_sha256="c" * 64,
            reference_days=("2025-08-01", "2025-08-02"),
            source_clock_identity="mixed_clock_is_prohibited",
        )


def test_transform_never_refits_thresholds_on_target_frame() -> None:
    reference = _frame()
    artifact = _fit(reference)
    median_name = "tri::quantile::value::mid_usdc_per_btc::ema::h1s::ge::q5000"
    definition = next(item for item in artifact.definitions if item.name == median_name)
    assert definition.threshold == pytest.approx(101.5)

    target = reference.copy()
    target["value::mid_usdc_per_btc::ema::h1s"] = [10_000.0] * len(target)
    transformed = artifact.transform(target)
    assert transformed.columns[median_name].tolist() == [1, 1, 1, 1]
    assert next(
        item for item in artifact.definitions if item.name == median_name
    ).threshold == pytest.approx(101.5)


def test_explicit_reference_days_must_match_reference_frame_when_present() -> None:
    frame = _frame().assign(utc_day=["2025-08-01"] * 4)
    with pytest.raises(PredicateContractError, match="reference_days do not match"):
        _fit(frame)


def test_schema_and_expected_hash_drift_fail_closed() -> None:
    reference = _frame()
    artifact = _fit(reference)
    with pytest.raises(PredicateContractError, match="schema drifted"):
        artifact.transform(reference.drop(columns=["snapshot_id"]))
    with pytest.raises(PredicateContractError, match="expected predicate artifact"):
        artifact.transform(reference, expected_artifact_sha256="0" * 64)


def test_canonical_roundtrip_and_block_mapping() -> None:
    artifact = _fit()
    restored = PredicateArtifact.from_json(artifact.to_json())
    assert restored == artifact
    assert restored.canonical_sha256 == artifact.canonical_sha256
    with pytest.raises(TypeError):
        restored.source_clock_identity["book"] = "drifted"  # type: ignore[index]
    result = restored.transform(_frame(), expected_artifact_sha256=restored.canonical_sha256)
    assert set(result.block_mapping) == {"R0", "M0", "M1", "M2"}
    assert set(result.block_mapping["M0"]) <= set(result.block_mapping["M1"])
    assert set(result.block_mapping["M1"]) <= set(result.block_mapping["M2"])
    assert any("signed_flow" in name for name in result.block_mapping["M2"])
    assert not any("signed_flow" in name for name in result.block_mapping["M1"])

    tampered = json.loads(artifact.to_json())
    tampered["definitions"][0]["block"] = "M2"
    with pytest.raises(PredicateContractError, match="SHA256 drifted"):
        PredicateArtifact.from_dict(tampered)


def test_inner_chronological_role_accepts_shared_clock_but_still_rejects_leakage() -> None:
    frame = _frame().assign(
        utc_day=[
            "2026-04-17",
            "2026-04-17",
            "2026-04-18",
            "2026-04-18",
        ]
    )
    artifact = fit_predicate_artifact(
        frame,
        side="BUY",
        source_role="inner_chronological_development",
        reference_identity_sha256="d" * 64,
        reference_days=("2026-04-17", "2026-04-18"),
        source_clock_identity="historical_exchange_event_visibility_v1",
    )
    assert artifact.clock_separated_2025 is False
    artifact.validate_clause(
        [
            "tri::mid_usdc_per_btc__h1s__h2s::positive_ordering",
            "tri::signed_flow_imbalance__h1s__h2s::positive_ordering",
        ]
    )


def test_reference_and_target_are_strictly_side_specific() -> None:
    mixed = _frame().copy()
    mixed.loc[0, "side"] = "SELL"
    with pytest.raises(PredicateContractError, match="one explicit side"):
        _fit(mixed)

    artifact = _fit()
    target = _frame().copy()
    target["side"] = "SELL"
    with pytest.raises(PredicateContractError, match="target side drifted"):
        artifact.transform(target)
