from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_offline_predicate_view_v1 as view,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_full_multiscale_successor_v1 as successor,
)
from research.families.f05_fill_quality_quote_ev.audit import (
    causal_multichannel_window_boolean_cooldown_predicates as predicates,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact(*, side: str, group: str) -> predicates.PredicateArtifact:
    field = f"{group}_signal"
    definition = predicates.PredicateDefinition(
        name=f"tri::quantile::{field}::ge::q5000",
        kind="quantile_ge",
        source_field=field,
        block="M2",
        clock_group=group,
        threshold=0.5,
        quantile=0.5,
    )
    return predicates.PredicateArtifact(
        schema_version=predicates.ARTIFACT_SCHEMA,
        identity=predicates.IDENTITY,
        side=side,
        source_role="outcome_blind_2025_single_channel",
        reference_identity_sha256="a" * 64,
        reference_days=("2025-01-01",),
        source_clock_identity={group: f"synthetic_{group}_clock"},
        clock_separated_2025=True,
        quantiles=(0.5,),
        input_schema=tuple(sorted((("side", "text"), (field, "numeric")))),
        definitions=(definition,),
    )


def _write_bundle(tmp_path: Path) -> tuple[Path, str]:
    entries: dict[str, dict[str, dict[str, str]]] = {}
    for group in ("book", "trade"):
        entries[group] = {}
        for side in ("BUY", "SELL"):
            artifact = _artifact(side=side, group=group)
            filename = f"{group}_{side.lower()}.json"
            path = tmp_path / filename
            path.write_text(artifact.to_json() + "\n", encoding="utf-8")
            entries[group][side] = {"path": filename, "sha256": _sha256(path)}
    body = {
        "schema_version": view._BUNDLE_SCHEMA,
        "identity": view._BUNDLE_IDENTITY,
        "m0_artifacts": [],
        "cross_clock_clause_authorized": False,
        "strict_2026_target_snapshot": {
            "book_trade_predicates_may_be_combined_by_study": True
        },
        **entries,
    }
    body["canonical_sha256"] = view._canonical_sha256(body)
    bundle_path = tmp_path / "predicate_bundle.json"
    bundle_path.write_text(
        json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return bundle_path, _sha256(bundle_path)


def _source_frames() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    index = pd.Index(["buy-row", "sell-row"], name="opportunity_id")
    metadata = pd.DataFrame(
        {
            "side": ["BUY", "SELL"],
            "campaign_age_s": [90.0, 40.0],
            "baseline_duration_ms": [85_000, 85_000],
        },
        index=index,
    )
    primitive = pd.DataFrame(
        {view._MID_OBSERVED: pd.Series([1, 1], index=index, dtype="int8")},
        index=index,
    )
    continuous = pd.DataFrame(
        {
            view._SHORT_CROSS_AGE: [8.0, 24.0],
            view._LONG_CROSS_AGE: [20.0, 12.0],
            "book_signal": [0.75, 0.25],
            "trade_signal": [0.10, 0.90],
        },
        index=index,
    )
    return metadata, primitive, continuous


def test_panel_and_snapshot_use_the_same_frozen_predicate_semantics(
    tmp_path: Path,
) -> None:
    bundle_path, bundle_sha = _write_bundle(tmp_path)
    bundle = view.load_frozen_predicate_bundle(
        bundle_path, expected_file_sha256=bundle_sha
    )
    metadata, primitive, continuous = _source_frames()

    expanded, receipt = view.materialize_panel_predicates(
        metadata=metadata,
        primitive_boolean=primitive,
        continuous=continuous,
        bundle=bundle,
    )

    selected = [
        "tri::quantile::book_signal::ge::q5000",
        "tri::quantile::trade_signal::ge::q5000",
        successor.CURRENT_SHORT_CROSS,
        successor.CURRENT_LONG_CROSS,
        successor.CURRENT_CAMPAIGN_AGE,
    ]
    for opportunity_id, decision_ts_ns in (
        ("buy-row", 1_767_225_600_000_000_000),
        ("sell-row", 1_767_225_700_000_000_000),
    ):
        feature_row = {
            **metadata.loc[opportunity_id].to_dict(),
            **primitive.loc[opportunity_id].to_dict(),
            **continuous.loc[opportunity_id].to_dict(),
            "decision_ts_ns": decision_ts_ns,
        }
        snapshot = view.materialize_snapshot_predicates(
            predicate_names=selected,
            feature_row=feature_row,
            side=str(feature_row["side"]),
            baseline_duration_ms=int(feature_row["baseline_duration_ms"]),
            bundle=bundle,
        )
        assert snapshot == {
            name: int(expanded.loc[opportunity_id, name]) for name in selected
        }
    assert receipt["rows"] == 2
    assert receipt["side_rows"] == {"BUY": 1, "SELL": 1}
    assert receipt["economic_outcomes_read"] is False
    assert receipt["bundle"]["reference_days_are_2025"] is True


def test_snapshot_uses_bound_utc_day_without_requiring_decision_timestamp(
    tmp_path: Path,
) -> None:
    bundle_path, bundle_sha = _write_bundle(tmp_path)
    bundle = view.load_frozen_predicate_bundle(
        bundle_path, expected_file_sha256=bundle_sha
    )

    snapshot = view.materialize_snapshot_predicates(
        predicate_names=["tri::quantile::book_signal::ge::q5000"],
        feature_row={
            "utc_day": "2026-01-01",
            "book_signal": 0.9,
            "side": "BUY",
        },
        side="BUY",
        baseline_duration_ms=85_000,
        bundle=bundle,
    )

    assert snapshot == {"tri::quantile::book_signal::ge::q5000": 1}


def test_bundle_and_artifact_hash_drift_fail_closed(tmp_path: Path) -> None:
    bundle_path, bundle_sha = _write_bundle(tmp_path)
    with pytest.raises(view.OfflinePredicateViewError, match="bundle file SHA256"):
        view.load_frozen_predicate_bundle(
            bundle_path, expected_file_sha256="f" * 64
        )

    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    artifact_path = tmp_path / bundle["book"]["BUY"]["path"]
    artifact_path.write_text(
        artifact_path.read_text(encoding="utf-8") + " ", encoding="utf-8"
    )
    with pytest.raises(view.OfflinePredicateViewError, match="artifact SHA256"):
        view.load_frozen_predicate_bundle(
            bundle_path, expected_file_sha256=bundle_sha
        )


@pytest.mark.parametrize("bad_value", ["bad", float("inf")])
def test_snapshot_predicate_rejects_nonmissing_malformed_values(
    tmp_path: Path,
    bad_value: object,
) -> None:
    bundle_path, bundle_sha = _write_bundle(tmp_path)
    bundle = view.load_frozen_predicate_bundle(
        bundle_path, expected_file_sha256=bundle_sha
    )
    feature_row = {
        "decision_ts_ns": 1_767_225_600_000_000_000,
        "book_signal": bad_value,
        "trade_signal": 0.1,
        "side": "BUY",
    }
    with pytest.raises(view.OfflinePredicateViewError):
        view.materialize_snapshot_predicates(
            predicate_names=["tri::quantile::book_signal::ge::q5000"],
            feature_row=feature_row,
            side="BUY",
            baseline_duration_ms=85_000,
            bundle=bundle,
        )
