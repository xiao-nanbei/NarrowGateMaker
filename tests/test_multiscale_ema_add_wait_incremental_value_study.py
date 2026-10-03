from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from research.families.f05_fill_quality_quote_ev.audit import (
    multiscale_ema_add_wait_incremental_value_study as study,
)


def _clock_row(
    event: int,
    ts_ms: int,
    *,
    side: str,
    source_index: int,
    mid_tick_x2: int,
) -> dict[str, object]:
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


def test_external_release_is_frozen_from_next_changed_market_content() -> None:
    rows = [
        _clock_row(10, 1_000, side="BUY", source_index=1, mid_tick_x2=1000),
        _clock_row(10, 1_000, side="SELL", source_index=1, mid_tick_x2=1000),
        _clock_row(11, 2_000, side="BUY", source_index=1, mid_tick_x2=1000),
        _clock_row(11, 2_000, side="SELL", source_index=1, mid_tick_x2=1000),
        _clock_row(12, 3_000, side="BUY", source_index=2, mid_tick_x2=999),
        _clock_row(12, 3_000, side="SELL", source_index=2, mid_tick_x2=999),
    ]
    trace = pd.DataFrame(rows)
    target = trace.iloc[[0]].copy()
    target["market_generation_identity"] = [
        study._generation_from_row(target.iloc[0]).identity
    ]
    result = study._attach_external_release(target, trace).iloc[0]
    assert result["external_epoch_support_valid"] == 1
    assert result["release_market_event_generation"] == 12
    assert result["release_ts_ms"] == 3_000
    assert result["release_snapshot_mid_tick_x2"] == 999


def test_external_clock_rejects_side_dependent_generation() -> None:
    rows = [
        _clock_row(10, 1_000, side="BUY", source_index=1, mid_tick_x2=1000),
        _clock_row(10, 1_000, side="SELL", source_index=2, mid_tick_x2=1000),
    ]
    trace = pd.DataFrame(rows)
    target = trace.iloc[[0]].copy()
    target["market_generation_identity"] = [
        study._generation_from_row(target.iloc[0]).identity
    ]
    with pytest.raises(study.StudyError, match="side-dependent"):
        study._attach_external_release(target, trace)
