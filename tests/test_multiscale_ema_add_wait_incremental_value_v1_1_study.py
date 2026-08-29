from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_v1_1_study as study,
)


def _release_trace_row(
    event: int,
    ts_ms: int,
    *,
    side: str,
    source_index: int,
    mid_tick_x2: int,
    release_event: int,
    release_ts_ms: int,
    release_source_index: int,
    release_mid_tick_x2: int,
) -> dict[str, object]:
    release = study.MarketGeneration(
        bbo_index=release_source_index,
        l2_index=release_source_index,
        trade_index=release_source_index,
        feature_ready_index=release_source_index,
        prediction_index=release_source_index,
        snapshot_mid_tick_x2=release_mid_tick_x2,
    )
    return {
        "canonical_external_decision": 1,
        "market_readiness": 1,
        "market_event_generation": event,
        "ts_ms": ts_ms,
        "side": side,
        "decision_visible_bbo_index": source_index,
        "decision_visible_l2_index": source_index,
        "decision_visible_trade_index": source_index,
        "feature_ready_generation_index": source_index,
        "prediction_generation_index": source_index,
        "quote_snapshot_mid_tick_x2": mid_tick_x2,
        "external_epoch_support_valid": 1,
        "external_epoch_support_reason": "supported",
        "release_ts_ms": release_ts_ms,
        "release_market_event_generation": release_event,
        "release_market_generation_identity": release.identity,
        "release_bbo_index": release_source_index,
        "release_l2_index": release_source_index,
        "release_trade_index": release_source_index,
        "release_feature_ready_index": release_source_index,
        "release_prediction_index": release_source_index,
        "release_snapshot_mid_tick_x2": release_mid_tick_x2,
    }


def test_artifact_paths_are_scoped_to_requested_output(tmp_path: Path) -> None:
    paths = study._artifact_paths(tmp_path)
    assert set(paths) == {
        "panel_manifest",
        "selected_panel",
        "label_panel",
        "report",
    }
    assert all(path.parent == tmp_path for path in paths.values())


def test_output_path_escape_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(study.StudyError, match="escaped"):
        study._require_output_path(
            tmp_path.parent / "foreign" / "artifact.json",
            output=tmp_path,
            role="test artifact",
        )


def test_frozen_spec_uses_private_source_identity_not_projection_hash() -> None:
    assert study._spec_sha256() == (
        "b59f9f5a3c9cbdd1fa714abe6ddf8ef23e19654374c354a6840e6f943a7c6908"
    )
    assert study._spec_sha256() != study._sha256_file(study.SPEC)


def test_execution_amendment_uses_source_identity_and_portable_config_locator() -> None:
    assert study._execution_amendment_sha256() == (
        "4c2dd8a19640e058856bc2eb27e408fa431935349a3a2fab0e474643f389269f"
    )
    assert study._execution_amendment_sha256() != study._sha256_file(study.EXECUTION_AMENDMENT)
    public_amendment = study._load_json(study.EXECUTION_AMENDMENT)
    by_role = {row["role"]: row for row in public_amendment["artifacts"]}
    assert by_role["operational_config"]["path"] == "${NARROWGATE_LIVE_CONFIG}"


def test_missing_frozen_operational_pointer_bytes_fail_closed() -> None:
    with pytest.raises(
        study.StudyError,
        match="frozen operational baseline pointer exact bytes are missing",
    ):
        study._spec_and_plan()


def test_projected_denominator_validates_against_frozen_source_identity() -> None:
    public_spec = study._load_json(study.SPEC)
    row = public_spec["source_contract"]["denominator_source_spec"]
    if not os.environ.get("NARROWGATE_PRIVATE_RESEARCH_ROOT"):
        with pytest.raises(RuntimeError, match="requires private configuration"):
            study._resolve_bound_path(row["path"])
        return
    path = study._resolve_bound_path(row["path"])
    study._validate_source_identity(path, row["sha256"], role="test denominator")


def test_transform_includes_frozen_cooldown_categorical_columns() -> None:
    train = pd.DataFrame(
        {
            "value": [1.0, 2.0, 3.0, 4.0],
            "cooldown_phase": [
                "COOLDOWN_ACTIVE",
                "COOLDOWN_EXPIRED",
                "COOLDOWN_ACTIVE",
                "COOLDOWN_EXPIRED",
            ],
        }
    )
    test = train.iloc[:2].copy()
    weights = np.full(len(train), 0.25, dtype=np.float64)
    x_train, x_test = study._transform(train, test, ("value",), weights)

    # One scaled continuous feature, one missing indicator, then the two
    # frozen categorical columns in active/expired order.
    assert x_train.shape == (4, 4)
    assert x_test.shape == (2, 4)
    assert x_train[:, -2:].tolist() == [
        [1.0, 0.0],
        [0.0, 1.0],
        [1.0, 0.0],
        [0.0, 1.0],
    ]


def test_transform_rejects_unknown_cooldown_phase() -> None:
    train = pd.DataFrame(
        {
            "value": [1.0, 2.0],
            "cooldown_phase": ["COOLDOWN_ACTIVE", "UNKNOWN"],
        }
    )
    with pytest.raises(study.StudyError, match="unknown cooldown phases"):
        study._transform(
            train,
            train.iloc[:1],
            ("value",),
            np.ones(len(train), dtype=np.float64),
        )


def test_external_release_uses_authoritative_raw_event_successor() -> None:
    trace = pd.DataFrame(
        [
            _release_trace_row(
                10,
                1_000,
                side="BUY",
                source_index=1,
                mid_tick_x2=1_000,
                release_event=11,
                release_ts_ms=1_000,
                release_source_index=2,
                release_mid_tick_x2=999,
            )
        ]
    )
    target = trace.iloc[[0]].copy()
    target["market_generation_identity"] = [study._generation_from_row(target.iloc[0]).identity]
    result = study._attach_external_release(target, trace).iloc[0]
    assert result["external_epoch_support_valid"] == 1
    assert result["release_market_event_generation"] == 11
    assert result["release_ts_ms"] == 1_000
    assert result["release_snapshot_mid_tick_x2"] == 999


def test_external_release_rejects_unchanged_market_content() -> None:
    trace = pd.DataFrame(
        [
            _release_trace_row(
                10,
                1_000,
                side="BUY",
                source_index=1,
                mid_tick_x2=1_000,
                release_event=11,
                release_ts_ms=1_025,
                release_source_index=1,
                release_mid_tick_x2=1_000,
            )
        ]
    )
    target = trace.iloc[[0]].copy()
    target["market_generation_identity"] = [study._generation_from_row(target.iloc[0]).identity]
    with pytest.raises(study.StudyError, match="did not advance"):
        study._attach_external_release(target, trace)


def test_external_release_rejects_identity_drift() -> None:
    trace = pd.DataFrame(
        [
            _release_trace_row(
                10,
                1_000,
                side="SELL",
                source_index=1,
                mid_tick_x2=1_000,
                release_event=11,
                release_ts_ms=1_025,
                release_source_index=2,
                release_mid_tick_x2=999,
            )
        ]
    )
    trace.loc[0, "release_market_generation_identity"] = "0" * 64
    target = trace.copy()
    target["market_generation_identity"] = [study._generation_from_row(target.iloc[0]).identity]
    with pytest.raises(study.StudyError, match="identity drifted"):
        study._attach_external_release(target, trace)
